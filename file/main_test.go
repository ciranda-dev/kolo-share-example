package main

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	koloblobs "github.com/ciranda-dev/kolo-blobs"
	kololog "github.com/ciranda-dev/kolo-log"
)

var bin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "kolo-share-test-")
	if err != nil {
		panic(err)
	}
	bin = filepath.Join(dir, "file")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		os.RemoveAll(dir)
		panic(string(out))
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// sender runs `file send name` and returns its share code, its stderr and a func that stops it; the test end stops it too.
func sender(t *testing.T, name string) (string, *bytes.Buffer, func()) {
	t.Helper()
	cmd := exec.Command(bin, "send", name)
	stderr := &bytes.Buffer{}
	r, w := io.Pipe()
	cmd.Stdout, cmd.Stderr = w, stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		w.Close()
		done <- err
	}()
	codes := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), codePrefix) {
				codes <- sc.Text()
			}
		}
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cmd.Process.Signal(syscall.SIGINT)
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("sender: %v\n%s", err, stderr)
				}
			case <-time.After(30 * time.Second):
				cmd.Process.Kill()
				t.Errorf("sender did not stop on Ctrl-C\n%s", stderr)
			}
		})
	}
	t.Cleanup(stop)
	select {
	case c := <-codes:
		return c, stderr, stop
	case <-time.After(30 * time.Second):
		t.Fatalf("no share code\n%s", stderr)
		return "", nil, nil
	}
}

// runGet runs `file get code out` and returns its stderr and exit code.
func runGet(t *testing.T, code, out string) (string, int) {
	t.Helper()
	return run(t, "", "get", code, out)
}

// run runs the program with args and stdin and returns its output and exit code.
func run(t *testing.T, stdin string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Stdin = strings.NewReader(stdin)
	b, err := cmd.CombinedOutput()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return string(b), ee.ExitCode()
		}
		t.Fatal(err)
	}
	return string(b), 0
}

var sumLine = regexp.MustCompile(`sha256 ([0-9a-f]{64})`)

func write(t *testing.T, size int) (string, []byte) {
	t.Helper()
	b := make([]byte, size)
	rand.Read(b)
	name := filepath.Join(t.TempDir(), "photo.jpg")
	if err := os.WriteFile(name, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return name, b
}

func TestShareFile(t *testing.T) {
	for _, size := range []int{0, 1, 3_000_000} {
		name, data := write(t, size)
		code, sendErr, stop := sender(t, name)
		out := filepath.Join(t.TempDir(), "copy.jpg")
		msg, exit := runGet(t, code, out)
		if exit != 0 {
			t.Fatalf("size %d: get exit %d\n%s", size, exit, msg)
		}
		got, err := os.ReadFile(out)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("size %d: saved file differs (%v)", size, err)
		}
		stop()
		a, b := sumLine.FindStringSubmatch(sendErr.String()), sumLine.FindStringSubmatch(msg)
		if a == nil || b == nil || a[1] != b[1] {
			t.Fatalf("size %d: sha256 lines differ\nsender:\n%s\nreceiver:\n%s", size, sendErr, msg)
		}
		if !strings.Contains(msg, "every chunk checked against the blob ID") {
			t.Fatalf("size %d: no verification line\n%s", size, msg)
		}
	}
}

func TestSavedFileIsPrivate(t *testing.T) {
	name, _ := write(t, 1000)
	code, _, _ := sender(t, name)
	out := filepath.Join(t.TempDir(), "copy.jpg")
	if msg, exit := runGet(t, code, out); exit != 0 {
		t.Fatalf("exit %d\n%s", exit, msg)
	}
	if info, err := os.Stat(out); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("saved file: %v, %v", info, err)
	}
}

func TestCodeFromStdin(t *testing.T) {
	name, data := write(t, 1000)
	code, _, _ := sender(t, name)
	out := filepath.Join(t.TempDir(), "copy.jpg")
	if msg, exit := run(t, code+"\n", "get", "-", out); exit != 0 {
		t.Fatalf("exit %d\n%s", exit, msg)
	}
	if got, err := os.ReadFile(out); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("saved file differs (%v)", err)
	}
	if msg, exit := run(t, "", "get", "-", out+"2"); exit != 1 || !strings.Contains(msg, "no share code on standard input") {
		t.Fatalf("empty stdin: exit %d\n%s", exit, msg)
	}
}

func TestMaxBytes(t *testing.T) {
	for _, c := range []struct {
		size, limit int
		pulled      bool // the limit is met by the size check after the download, not during it
	}{
		{100_000, 50_000, true},
		{200_000_000, 5_000_000, false},
	} {
		if c.size > 1_000_000 && testing.Short() {
			continue
		}
		name, _ := write(t, c.size)
		code, _, _ := sender(t, name)
		out := filepath.Join(t.TempDir(), "copy.jpg")
		msg, exit := run(t, "", "-max-bytes", fmt.Sprint(c.limit), "get", code, out)
		if exit != 1 || !strings.Contains(msg, "-max-bytes") || strings.Contains(msg, "saved") {
			t.Fatalf("size %d: exit %d\n%s", c.size, exit, msg)
		}
		if got := strings.Contains(msg, "received"); got != c.pulled {
			t.Fatalf("size %d: download finished = %v, want %v\n%s", c.size, got, c.pulled, msg)
		}
		if _, err := os.Stat(out); !os.IsNotExist(err) {
			t.Fatalf("size %d: output left behind: %v", c.size, err)
		}
	}
	if msg, exit := run(t, "", "-max-bytes", "0", "get", "x", "y"); exit != 2 {
		t.Fatalf("-max-bytes 0: exit %d\n%s", exit, msg)
	}
}

func TestCodeAddress(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:7450", "224.0.0.1:7450", "255.255.255.255:7450", "127.0.0.1:0"} {
		c := shareCode{addr: netip.MustParseAddrPort(addr), sender: make(ed25519.PublicKey, 32)}
		if _, err := parseCode(c.String()); err == nil || !strings.Contains(err.Error(), "nobody can listen") {
			t.Errorf("%s: %v", addr, err)
		}
	}
	if _, err := parseCode(shareCode{addr: netip.MustParseAddrPort("127.0.0.1:7450"), sender: make(ed25519.PublicKey, 32)}.String()); err != nil {
		t.Fatal(err)
	}
}

func TestBadCode(t *testing.T) {
	out := filepath.Join(t.TempDir(), "copy.jpg")
	for _, code := range []string{"", "hello", codePrefix + "abc", "kolo-folder:" + strings.Repeat("A", 136)} {
		msg, exit := runGet(t, code, out)
		if exit != 1 || !strings.Contains(msg, "not a file share code") {
			t.Fatalf("code %q: exit %d\n%s", code, exit, msg)
		}
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("output written for a bad code: %v", err)
	}
}

// edit decodes a share code, changes byte i and encodes it again.
func edit(t *testing.T, code string, i int) string {
	t.Helper()
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(code, codePrefix))
	if err != nil {
		t.Fatal(err)
	}
	b[i] ^= 1
	return codePrefix + base64.RawURLEncoding.EncodeToString(b)
}

func TestWrongCode(t *testing.T) {
	name, _ := write(t, 100_000)
	code, _, _ := sender(t, name)
	const seed, key = 6 + 32 + kololog.LogIDSize, codeSize - koloblobs.KeySize
	for _, c := range []struct {
		at   int
		want string
	}{
		{key, "does not open this file"},
		{seed, "does not share this file with this code"},
	} {
		out := filepath.Join(t.TempDir(), "copy.jpg")
		msg, exit := runGet(t, edit(t, code, c.at), out)
		if exit != 1 || !strings.Contains(msg, c.want) {
			t.Fatalf("byte %d changed: exit %d, want %q\n%s", c.at, exit, c.want, msg)
		}
		if _, err := os.Stat(out); !os.IsNotExist(err) {
			t.Fatalf("byte %d changed: output left behind: %v", c.at, err)
		}
	}
}

func TestSenderGone(t *testing.T) {
	name, _ := write(t, 1000)
	code, _, stop := sender(t, name)
	stop()
	msg, exit := runGet(t, code, filepath.Join(t.TempDir(), "copy.jpg"))
	if exit != 1 || !strings.Contains(msg, "cannot reach the sender") {
		t.Fatalf("exit %d\n%s", exit, msg)
	}
}

func TestOutputExists(t *testing.T) {
	name, _ := write(t, 1000)
	code, _, _ := sender(t, name)
	out := filepath.Join(t.TempDir(), "copy.jpg")
	if err := os.WriteFile(out, []byte("keep me"), 0o600); err != nil {
		t.Fatal(err)
	}
	msg, exit := runGet(t, code, out)
	if exit != 1 || !strings.Contains(msg, "already exists") {
		t.Fatalf("exit %d\n%s", exit, msg)
	}
	if b, _ := os.ReadFile(out); string(b) != "keep me" {
		t.Fatalf("existing file changed: %q", b)
	}
}

func TestSendErrors(t *testing.T) {
	dir := t.TempDir()
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"send", filepath.Join(dir, "missing")}, "no such file"},
		{[]string{"send", dir}, "is not a regular file"},
		{[]string{"-listen", "localhost:0", "send", "main.go"}, "needs an IPv4 address"},
	} {
		b, err := exec.Command(bin, c.args...).CombinedOutput()
		if err == nil || !strings.Contains(string(b), c.want) {
			t.Fatalf("%v: %v\n%s", c.args, err, b)
		}
	}
}

func FuzzParseCode(f *testing.F) {
	f.Add("")
	f.Add(codePrefix + strings.Repeat("A", 222))
	f.Fuzz(func(t *testing.T, s string) {
		c, err := parseCode(s)
		if err != nil {
			return
		}
		again, err := parseCode(c.String())
		if err != nil || again.String() != c.String() {
			t.Fatalf("round trip of %q: %v", s, err)
		}
	})
}
