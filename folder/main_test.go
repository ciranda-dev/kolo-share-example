package main

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"io"
	"io/fs"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var bin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "kolo-share-test-")
	if err != nil {
		panic(err)
	}
	bin = filepath.Join(dir, "folder")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		os.RemoveAll(dir)
		panic(string(out))
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// buffer is a bytes.Buffer safe to read while a process writes to it.
type buffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *buffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type proc struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stderr *buffer
	done   chan struct{} // closed when the process has exited
	err    error         // its exit error, set before done is closed
}

// start runs the program; it is stopped with Ctrl-C at the end of the test.
func start(t *testing.T, stdout io.Writer, args ...string) *proc {
	t.Helper()
	p := &proc{cmd: exec.Command(bin, args...), stderr: &buffer{}, done: make(chan struct{})}
	p.cmd.Stdout = stdout
	p.cmd.Stderr = p.stderr
	var err error
	if p.stdin, err = p.cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		p.err = p.cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() { p.stop(t) })
	return p
}

func (p *proc) stop(t *testing.T) {
	p.cmd.Process.Signal(syscall.SIGINT)
	select {
	case <-p.done:
	case <-time.After(30 * time.Second):
		p.cmd.Process.Signal(syscall.SIGQUIT) // Go prints every goroutine's stack
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			p.cmd.Process.Kill()
		}
		t.Errorf("%v did not stop on Ctrl-C\n%s", p.cmd.Args, p.stderr)
	}
}

// sender starts `folder send dir` and returns it with its share code.
func sender(t *testing.T, dir string) (*proc, string) {
	t.Helper()
	r, w := io.Pipe()
	p := start(t, w, "send", dir)
	codes := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), codePrefix) {
				codes <- sc.Text()
			}
		}
	}()
	select {
	case c := <-codes:
		return p, c
	case <-time.After(30 * time.Second):
		t.Fatalf("no share code\n%s", p.stderr)
		return nil, ""
	}
}

// published waits for the sender's k-th "published version N" line and returns N.
func published(t *testing.T, p *proc, k int) string {
	t.Helper()
	const prefix = "published version "
	deadline := time.Now().Add(30 * time.Second)
	for {
		var got []string
		for _, line := range strings.Split(p.stderr.String(), "\n") {
			if i := strings.Index(line, prefix); i >= 0 {
				got = append(got, line[i+len(prefix):])
			}
		}
		if len(got) >= k {
			return got[k-1]
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %d published versions in\n%s", k, p.stderr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitFor waits until the process's stderr contains s.
func waitFor(t *testing.T, p *proc, s string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(p.stderr.String(), s) {
		if time.Now().After(deadline) {
			t.Fatalf("no %q in\n%s", s, p.stderr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// tree returns every file under dir with its content, by slash path.
func tree(t *testing.T, dir string) map[string]string {
	t.Helper()
	m := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		rel, _ := filepath.Rel(dir, p)
		m[filepath.ToSlash(rel)] = string(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func same(t *testing.T, a, b string) {
	t.Helper()
	x, y := tree(t, a), tree(t, b)
	if len(x) != len(y) {
		t.Fatalf("%s has %d files, %s has %d", a, len(x), b, len(y))
	}
	for k, v := range x {
		if y[k] != v {
			t.Fatalf("%s differs between %s and %s", k, a, b)
		}
	}
}

func mustWrite(t *testing.T, name, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestShareFolder(t *testing.T) {
	src := filepath.Join(t.TempDir(), "shared")
	mustWrite(t, filepath.Join(src, "todo.txt"), "buy bread")
	mustWrite(t, filepath.Join(src, "docs", "readme.txt"), "first draft")
	mustWrite(t, filepath.Join(src, "docs", "big.bin"), strings.Repeat("kolo", 1_000_000))
	if err := os.Mkdir(filepath.Join(src, "empty"), 0o700); err != nil {
		t.Fatal(err)
	}
	snd, code := sender(t, src)
	waitFor(t, snd, "version 6 holds")

	out := filepath.Join(t.TempDir(), "copy")
	rcv := start(t, io.Discard, "-every", "200ms", "get", code, out)
	waitFor(t, rcv, "saved version 6 in")
	same(t, src, filepath.Join(out, "v6"))
	if _, err := os.Stat(filepath.Join(out, "v6", "empty")); err != nil {
		t.Fatalf("empty folder not saved: %v", err)
	}

	io.WriteString(snd.stdin, "\n")
	waitFor(t, snd, "nothing changed")

	mustWrite(t, filepath.Join(src, "todo.txt"), "buy bread and milk")
	mustWrite(t, filepath.Join(src, "plans.txt"), "plans")
	if err := os.RemoveAll(filepath.Join(src, "docs")); err != nil {
		t.Fatal(err)
	}
	io.WriteString(snd.stdin, "\n")
	n := published(t, snd, 1)
	waitFor(t, rcv, "  added plans.txt")
	waitFor(t, rcv, "  changed todo.txt")
	waitFor(t, rcv, "  deleted a file or folder")
	waitFor(t, rcv, "saved version "+n+" in")
	same(t, src, filepath.Join(out, "v"+n))
	if got := tree(t, filepath.Join(out, "v6"))["todo.txt"]; got != "buy bread" {
		t.Fatalf("v6/todo.txt = %q", got)
	}

	// A folder with a file in it becomes a file of the same name, and a file becomes a folder.
	mustWrite(t, filepath.Join(src, "box", "inside.txt"), "in a box")
	io.WriteString(snd.stdin, "\n")
	published(t, snd, 2)
	if err := os.RemoveAll(filepath.Join(src, "box")); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(src, "box"), "now a file")
	if err := os.Remove(filepath.Join(src, "plans.txt")); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(src, "plans.txt", "week.txt"), "now a folder")
	io.WriteString(snd.stdin, "\n")
	n = published(t, snd, 3)
	waitFor(t, rcv, "saved version "+n+" in")
	same(t, src, filepath.Join(out, "v"+n))

	snd.stop(t)
	select {
	case <-rcv.done:
		if rcv.err == nil || !strings.Contains(rcv.stderr.String(), "lost the sender") {
			t.Fatalf("receiver after the sender stopped: %v\n%s", rcv.err, rcv.stderr)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("receiver still running after the sender stopped\n%s", rcv.stderr)
	}
}

func TestSavedFolderIsPrivate(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "a.txt"), "a")
	snd, code := sender(t, src)
	waitFor(t, snd, "version 2 holds")
	out := filepath.Join(t.TempDir(), "parent", "copy")
	rcv := start(t, io.Discard, "-every", "200ms", "get", code, out)
	waitFor(t, rcv, "saved version 2 in")
	if info, err := os.Stat(out); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("output folder: %v, %v", info, err)
	}
}

func TestCodeFromStdin(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "a.txt"), "a")
	snd, code := sender(t, src)
	waitFor(t, snd, "version 2 holds")
	out := filepath.Join(t.TempDir(), "copy")
	rcv := start(t, io.Discard, "-every", "200ms", "get", "-", out)
	io.WriteString(rcv.stdin, code+"\n")
	waitFor(t, rcv, "saved version 2 in")
	same(t, src, filepath.Join(out, "v2"))

	cmd := exec.Command(bin, "get", "-", filepath.Join(t.TempDir(), "copy"))
	cmd.Stdin = strings.NewReader("")
	if b, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(b), "no share code on standard input") {
		t.Fatalf("empty stdin: %v\n%s", err, b)
	}
}

func TestMaxBytes(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "a.bin"), strings.Repeat("a", 600_000))
	snd, code := sender(t, src)
	waitFor(t, snd, "version 2 holds")

	out := filepath.Join(t.TempDir(), "small")
	b, err := exec.Command(bin, "-max-bytes", "100000", "get", code, out).CombinedOutput()
	if err == nil || !strings.Contains(string(b), "-max-bytes") {
		t.Fatalf("%v\n%s", err, b)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("output left behind: %v", err)
	}

	// The first copy fits, a second copy of the same size does not.
	out = filepath.Join(t.TempDir(), "copy")
	rcv := start(t, io.Discard, "-every", "200ms", "-max-bytes", "1000000", "get", code, out)
	waitFor(t, rcv, "saved version 2 in")
	mustWrite(t, filepath.Join(src, "b.txt"), "b")
	io.WriteString(snd.stdin, "\n")
	select {
	case <-rcv.done:
		if rcv.err == nil || !strings.Contains(rcv.stderr.String(), "-max-bytes") {
			t.Fatalf("receiver: %v\n%s", rcv.err, rcv.stderr)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("receiver still running\n%s", rcv.stderr)
	}
	if got := tree(t, out); len(got) != 1 || got["v2/a.bin"] == "" {
		t.Fatalf("saved %d files, want only v2/a.bin", len(got))
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

func TestFolderErrors(t *testing.T) {
	dir := t.TempDir()
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"get", "kolo-file:abc", filepath.Join(dir, "out")}, "not a folder share code"},
		{[]string{"send", filepath.Join(dir, "missing")}, "no such file"},
		{[]string{"-listen", "localhost:0", "send", dir}, "needs an IPv4 address"},
		{[]string{"send", "main.go"}, "is not a folder"},
		{[]string{"-every", "0", "get", "kolo-folder:abc", filepath.Join(dir, "out")}, "usage"},
		{[]string{"-max-bytes", "0", "get", "kolo-folder:abc", filepath.Join(dir, "out")}, "usage"},
	} {
		b, err := exec.Command(bin, c.args...).CombinedOutput()
		if err == nil || !strings.Contains(string(b), c.want) {
			t.Fatalf("%v: %v\n%s", c.args, err, b)
		}
	}
}

func TestOutputExists(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "a.txt"), "a")
	_, code := sender(t, src)
	out := t.TempDir()
	mustWrite(t, filepath.Join(out, "v2", "a.txt"), "keep me")
	b, err := exec.Command(bin, "get", code, out).CombinedOutput()
	if err == nil || !strings.Contains(string(b), "already exists") {
		t.Fatalf("%v\n%s", err, b)
	}
	if got := tree(t, out)["v2/a.txt"]; got != "keep me" {
		t.Fatalf("existing file changed: %q", got)
	}
}

func FuzzParseCode(f *testing.F) {
	f.Add("")
	f.Add(codePrefix + strings.Repeat("A", 136))
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
