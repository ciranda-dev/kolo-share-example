// Command file shares one file between two peers with kolo-blobs.
//
//	file send FILE      stores FILE as a blob, prints a share code and serves the blob
//	file get CODE OUT   fetches the blob with the code, checks every chunk and saves it as OUT
package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	koloblobs "github.com/ciranda-dev/kolo-blobs"
	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolostore "github.com/ciranda-dev/kolo-store"
	kolosync "github.com/ciranda-dev/kolo-sync"
)

const dialTimeout = 10 * time.Second

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "IPv4 IP:PORT that send listens on; the share code carries it")
	maxBytes := flag.Int64("max-bytes", 1<<30, "most bytes get stores for a file")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: file [-listen IP:PORT] send FILE\n       file [-max-bytes N] get CODE OUT   (CODE - reads the code from standard input)")
		flag.PrintDefaults()
	}
	flag.Parse()
	if *maxBytes <= 0 {
		flag.Usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	log.SetFlags(log.Ltime)

	var err error
	switch args := flag.Args(); {
	case len(args) == 2 && args[0] == "send":
		err = send(ctx, *listen, args[1])
	case len(args) == 3 && args[0] == "get":
		err = get(ctx, args[1], args[2], *maxBytes)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

// send stores the file as a blob in a new log and serves that log to whoever holds the share code.
func send(ctx context.Context, listen, name string) error {
	addr, err := netip.ParseAddrPort(listen)
	if err != nil || !addr.Addr().Is4() || addr.Addr().IsUnspecified() {
		return fmt.Errorf("-listen needs an IPv4 address the receiver can reach, such as 127.0.0.1:0, not %q", listen)
	}
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	if info, err := f.Stat(); err != nil {
		return err
	} else if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", name)
	}

	st, _, cleanup, err := openStore()
	if err != nil {
		return err
	}
	defer cleanup()
	me, err := koloid.Generate()
	if err != nil {
		return err
	}
	l, err := st.Create(me)
	if err != nil {
		return err
	}
	key := koloblobs.NewKey()
	sum := sha256.New()
	id, err := koloblobs.New(koloblobs.NewLogBackend(l)).Write(ctx, key, io.TeeReader(f, sum))
	if err != nil {
		return fmt.Errorf("store %s: %w", name, err)
	}
	log.Printf("stored %s as blob %s in %d log entries", name, id.String()[:16], l.Len())
	log.Printf("sha256 %x", sum.Sum(nil))

	// The receiver's identity comes from a seed in the share code, so only the code's holder gets the log.
	var seed [32]byte
	rand.Read(seed[:])
	reader, err := koloid.FromSeed(seed[:])
	if err != nil {
		return err
	}
	srv := kolosync.NewServer(func(peer, _ ed25519.PublicKey, _ kololog.LogID) kolosync.Access {
		if peer.Equal(reader.PublicKey()) {
			return kolosync.Keys
		}
		return kolosync.NoAccess
	})
	if err := srv.Offer(l); err != nil {
		return err
	}
	ln, err := kolosecure.Listen(ctx, addr.String(), me, nil)
	if err != nil {
		return err
	}
	defer ln.Close()
	if addr, err = netip.ParseAddrPort(ln.Addr().String()); err != nil { // the port the system chose
		return err
	}

	c := shareCode{addr: addr, sender: me.PublicKey(), log: l.ID(), seed: seed, blob: id, key: key}
	fmt.Printf("share code:\n%s\n", c)
	log.Print("waiting for the receiver, press Ctrl-C to stop")
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(ctx) // ends the connections below whenever send returns
	defer cancel()
	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return err
		}
		log.Printf("peer %x connected", conn.RemotePublicKey()[:4])
		wg.Go(func() {
			defer conn.Close()
			srv.Serve(ctx, conn)
			log.Printf("peer %x left", conn.RemotePublicKey()[:4])
		})
	}
}

// get copies the sender's log, then reads the blob from it and saves it, checking every chunk on the way.
// It stops with errTooBig when the copy would hold more than maxBytes.
func get(ctx context.Context, code, out string, maxBytes int64) error {
	code, err := readCode(code)
	if err != nil {
		return err
	}
	c, err := parseCode(code)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(out); err == nil {
		return fmt.Errorf("%s already exists; choose a new name", out)
	}
	me, err := koloid.FromSeed(c.seed[:])
	if err != nil {
		return err
	}
	st, dir, cleanup, err := openStore()
	if err != nil {
		return err
	}
	defer cleanup()

	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	conn, err := kolosecure.Dial(dctx, c.addr.String(), me, c.sender, nil)
	if err != nil {
		return fmt.Errorf("cannot reach the sender at %s: %w", c.addr, err)
	}
	defer conn.Close()
	header, err := kolosync.FetchHeader(ctx, conn, c.sender, c.log)
	if errors.Is(err, kolosync.ErrNotFound) {
		return fmt.Errorf("the sender does not share this file with this code: %w", err)
	} else if err != nil {
		return fmt.Errorf("ask the sender for its log: %w", err)
	}
	rep, err := st.OpenReplica(c.sender, c.log, header)
	if err != nil {
		return err
	}
	log.Print("downloading")
	lctx, stopLimit := limit(ctx, dir, maxBytes)
	defer stopLimit()
	if err := kolosync.Pull(lctx, conn, rep); err != nil {
		return fmt.Errorf("download: %w", tooBig(lctx, err))
	}
	log.Printf("received %d log entries, each checked against the sender's signed head", rep.Len())

	r, err := koloblobs.New(koloblobs.NewLogBackend(rep)).Open(ctx, c.blob, c.key)
	if err != nil {
		return explain(err)
	}
	defer r.Close()
	if r.Size() > maxBytes {
		return errTooBig
	}
	f, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // never replaces a file
	if err != nil {
		return err
	}
	sum := sha256.New()
	_, err = io.Copy(io.MultiWriter(f, sum), r)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(out) // no half-written or unverified file is left behind
		return explain(err)
	}
	log.Printf("saved %s: %d bytes, every chunk checked against the blob ID", out, r.Size())
	log.Printf("sha256 %x", sum.Sum(nil))
	return nil
}

// explain turns the errors of kolo-blobs into a sentence a user can act on.
func explain(err error) error {
	switch {
	case errors.Is(err, koloblobs.ErrWrongKey):
		return fmt.Errorf("the key in the share code does not open this file: %w", err)
	case errors.Is(err, koloblobs.ErrCorrupt):
		return fmt.Errorf("the file failed verification, nothing was saved: %w", err)
	case errors.Is(err, koloblobs.ErrNotFound):
		return fmt.Errorf("the sender's log does not hold this file: %w", err)
	}
	return err
}

// openStore opens a kolo-store in a new temporary folder, removed by the returned function.
func openStore() (*kolostore.Store, string, func(), error) {
	dir, err := os.MkdirTemp("", "kolo-share-")
	if err != nil {
		return nil, "", nil, err
	}
	db, err := kolostore.OpenPebble(dir)
	if err != nil {
		os.RemoveAll(dir)
		return nil, "", nil, err
	}
	st := kolostore.New(db)
	return st, dir, func() {
		st.Close()
		os.RemoveAll(dir)
	}, nil
}

// errTooBig ends a get that would store more than -max-bytes.
var errTooBig = errors.New("the sender offers more than -max-bytes allows; only get codes from people you trust")

// limit returns a context that ends with errTooBig once the folder dir holds more than max bytes.
func limit(ctx context.Context, dir string, max int64) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(ctx)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
			}
			var n int64
			filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return nil
				}
				if info, err := d.Info(); err == nil {
					n += info.Size()
				}
				return nil
			})
			if n > max {
				cancel(errTooBig)
				return
			}
		}
	}()
	return ctx, func() { cancel(nil) }
}

// tooBig returns errTooBig when ctx ended because of limit, and err otherwise.
func tooBig(ctx context.Context, err error) error {
	if errors.Is(context.Cause(ctx), errTooBig) {
		return errTooBig
	}
	return err
}

// readCode returns code, or the first line of standard input when code is "-", so that it stays out of ps and the shell history.
func readCode(code string) (string, error) {
	if code != "-" {
		return code, nil
	}
	line, _ := bufio.NewReader(io.LimitReader(os.Stdin, 4096)).ReadString('\n')
	if line == "" {
		return "", errors.New("no share code on standard input")
	}
	return line, nil
}

// checkAddr refuses the addresses nobody can listen on, so that a share code cannot make get send packets to them.
func checkAddr(a netip.AddrPort) bool {
	ip := a.Addr()
	return a.Port() != 0 && !ip.IsUnspecified() && !ip.IsMulticast() && ip != netip.AddrFrom4([4]byte{255, 255, 255, 255})
}

// shareCode is all the receiver needs: where the sender listens, its public key, the log holding the blob,
// the seed of the receiver's identity, and the blob's ID and key. Whoever has it can read the file.
type shareCode struct {
	addr   netip.AddrPort
	sender ed25519.PublicKey
	log    kololog.LogID
	seed   [32]byte
	blob   koloblobs.ID
	key    koloblobs.Key
}

const (
	codePrefix = "kolo-file:"
	codeSize   = 6 + 32 + kololog.LogIDSize + 32 + koloblobs.HashSize + koloblobs.KeySize
)

func (c shareCode) String() string {
	ip := c.addr.Addr().As4()
	b := binary.BigEndian.AppendUint16(ip[:], c.addr.Port())
	b = append(b, c.sender...)
	b = append(b, c.log[:]...)
	b = append(b, c.seed[:]...)
	b = append(b, c.blob[:]...)
	b = append(b, c.key[:]...)
	return codePrefix + base64.RawURLEncoding.EncodeToString(b)
}

func parseCode(s string) (shareCode, error) {
	rest, ok := strings.CutPrefix(strings.TrimSpace(s), codePrefix)
	b, err := base64.RawURLEncoding.DecodeString(rest)
	if !ok || err != nil || len(b) != codeSize {
		return shareCode{}, fmt.Errorf("this is not a file share code; copy the whole line, it starts with %q", codePrefix)
	}
	var c shareCode
	c.addr = netip.AddrPortFrom(netip.AddrFrom4([4]byte(b[:4])), binary.BigEndian.Uint16(b[4:6]))
	if !checkAddr(c.addr) {
		return shareCode{}, fmt.Errorf("the share code names %s, an address nobody can listen on", c.addr)
	}
	b = b[6:]
	c.sender = ed25519.PublicKey(b[:32])
	b = b[32:]
	b = b[copy(c.log[:], b):]
	b = b[copy(c.seed[:], b):]
	b = b[copy(c.blob[:], b):]
	copy(c.key[:], b)
	return c, nil
}
