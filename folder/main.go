// Command folder shares a folder between two peers with kolo-files and keeps the copy up to date.
//
//	folder send DIR    puts DIR in a volume, prints a share code and publishes changes each time you press Enter
//	folder get CODE OUT   saves the folder into OUT/v<version>, then each new version the sender publishes
package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
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
	kolofiles "github.com/ciranda-dev/kolo-files"
	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolostore "github.com/ciranda-dev/kolo-store"
	kolosync "github.com/ciranda-dev/kolo-sync"
)

const dialTimeout = 10 * time.Second

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "IPv4 IP:PORT that send listens on; the share code carries it")
	every := flag.Duration("every", 2*time.Second, "how often get asks the sender for a new version")
	maxBytes := flag.Int64("max-bytes", 1<<30, "most bytes get stores, and writes into OUT in all")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: folder [-listen IP:PORT] send DIR\n       folder [-every 2s] [-max-bytes N] get CODE OUT   (CODE - reads the code from standard input)")
		flag.PrintDefaults()
	}
	flag.Parse()
	if *every <= 0 || *maxBytes <= 0 {
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
		err = get(ctx, args[1], args[2], *every, *maxBytes)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

// send keeps dir in a volume, serves its two logs to whoever holds the share code and adds a version on Enter.
func send(ctx context.Context, listen, dir string) error {
	addr, err := netip.ParseAddrPort(listen)
	if err != nil || !addr.Addr().Is4() || addr.Addr().IsUnspecified() {
		return fmt.Errorf("-listen needs an IPv4 address the receiver can reach, such as 127.0.0.1:0, not %q", listen)
	}
	if info, err := os.Lstat(dir); err != nil {
		return err
	} else if !info.IsDir() {
		return fmt.Errorf("%s is not a folder", dir)
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
	meta, err := st.Create(me)
	if err != nil {
		return err
	}
	content, err := st.Create(me)
	if err != nil {
		return err
	}
	v, err := kolofiles.Create(meta, content)
	if err != nil {
		return err
	}
	defer v.Close()
	seen := map[string]stamp{}
	if err := update(ctx, v, dir, seen); err != nil {
		return err
	}
	log.Printf("version %d holds %s", v.Len(), dir)

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
	for _, l := range []*kolostore.Log{meta, content} {
		if err := srv.Offer(l); err != nil {
			return err
		}
	}
	ln, err := kolosecure.Listen(ctx, addr.String(), me, nil)
	if err != nil {
		return err
	}
	defer ln.Close()
	if addr, err = netip.ParseAddrPort(ln.Addr().String()); err != nil { // the port the system chose
		return err
	}

	c := shareCode{addr: addr, sender: me.PublicKey(), meta: v.ID(), seed: seed}
	fmt.Printf("share code:\n%s\n", c)
	log.Print("change files in the folder, then press Enter to publish them; Ctrl-C stops")
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(ctx) // ends the goroutines below whenever send returns
	defer cancel()
	wg.Go(func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			log.Printf("peer %x connected", conn.RemotePublicKey()[:4])
			wg.Go(func() {
				defer conn.Close()
				srv.Serve(ctx, conn)
				log.Printf("peer %x left", conn.RemotePublicKey()[:4])
			})
		}
	})
	enter := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
			enter <- struct{}{}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-enter:
			before := v.Len()
			if err := update(ctx, v, dir, seen); err != nil {
				return err
			}
			if v.Len() == before {
				log.Print("nothing changed")
			} else {
				log.Printf("published version %d", v.Len())
			}
		}
	}
}

// stamp is what update compares to find a changed file: its size and modification time.
type stamp struct {
	dir  bool
	size int64
	mod  time.Time
}

// update makes the volume match dir: it adds new folders and files, writes changed files and deletes what is gone.
// seen holds the stamps of the last update and is brought up to date.
func update(ctx context.Context, v *kolofiles.Volume, dir string, seen map[string]stamp) error {
	now := map[string]stamp{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) && p != dir {
			return nil // removed while we walked: the next update sees it gone
		}
		if err != nil || p == dir {
			return err
		}
		if !d.IsDir() && !d.Type().IsRegular() {
			return nil // links and devices are not shared
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		info, err := d.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		} else if err != nil {
			return err
		}
		s := stamp{dir: true}
		if !d.IsDir() {
			s = stamp{size: info.Size(), mod: info.ModTime()}
		}
		old, ok := seen[name]
		if ok && old.dir != s.dir { // a file became a folder or the other way round
			if err := v.Delete(name); err != nil {
				return err
			}
			ok = false
		}
		switch {
		case ok && (s.dir || old == s):
		case s.dir:
			err = v.Mkdir(name)
		default:
			var f *os.File
			if f, err = os.Open(p); err == nil {
				err = v.WriteFile(ctx, name, f)
				f.Close()
			}
		}
		if err != nil {
			log.Printf("skipped %s: %v", name, err)
			if ok {
				now[name] = old // keep the published version; the next update tries again
			}
			if s.dir {
				return fs.SkipDir
			}
			return nil
		}
		now[name] = s
		return nil
	})
	if err != nil {
		return err
	}
	for name := range seen {
		if _, ok := now[name]; !ok {
			// A folder's delete takes everything below it, so a later delete of a child finds nothing,
			// or finds a file where its folder was.
			if err := v.Delete(name); err != nil && !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, kolofiles.ErrNotDir) {
				return err
			}
		}
	}
	clear(seen)
	for name, s := range now {
		seen[name] = s
	}
	return nil
}

// get copies the sender's two logs, saves the folder, then pulls again every few seconds and saves each new version.
// It stops with errTooBig when the copy, or all the versions saved in out together, would hold more than maxBytes.
func get(ctx context.Context, code, out string, every time.Duration, maxBytes int64) error {
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

	lctx, stopLimit := limit(ctx, dir, maxBytes)
	defer stopLimit()
	meta, err := replica(lctx, st, conn, c.sender, c.meta)
	if err != nil {
		return err
	}
	contentID, err := kolofiles.ContentLog(meta)
	if err != nil {
		return err
	}
	content, err := replica(lctx, st, conn, c.sender, contentID)
	if err != nil {
		return err
	}
	v, err := kolofiles.OpenReader(meta, koloblobs.NewLogBackend(content), nil)
	if err != nil {
		return err
	}
	defer v.Close()

	if err := os.MkdirAll(out, 0o700); err != nil { // the folders inside are private too
		return err
	}
	var saved uint64
	var written int64
	defer func() {
		if saved == 0 {
			os.Remove(out) // leaves nothing behind when no version was saved
		}
	}()
	for {
		if n := v.Len(); n > saved {
			if saved > 0 {
				changes, err := v.Diff(saved, n)
				if err != nil {
					return err
				}
				printChanges(changes)
			}
			size, err := treeSize(v)
			if err != nil {
				return err
			}
			if written += size; written > maxBytes {
				return errTooBig
			}
			dst := filepath.Join(out, fmt.Sprintf("v%d", n))
			if err := os.CopyFS(dst, v); err != nil {
				os.RemoveAll(dst) // no half-saved version is left behind
				return fmt.Errorf("save version %d: %w", n, err)
			}
			log.Printf("saved version %d in %s, every file checked against its blob ID", n, dst)
			saved = n
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(every):
		}
		// The metadata first: content pulled after it holds every object its ops name.
		if err := kolosync.Pull(lctx, conn, meta); err != nil {
			return fmt.Errorf("lost the sender: %w", tooBig(lctx, err))
		}
		if err := kolosync.Pull(lctx, conn, content); err != nil {
			return fmt.Errorf("lost the sender: %w", tooBig(lctx, err))
		}
		if err := v.Refresh(); err != nil {
			return err
		}
	}
}

// treeSize adds up the sizes of the files in fsys.
func treeSize(fsys fs.FS) (n int64, err error) {
	err = fs.WalkDir(fsys, ".", func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err == nil {
			n += info.Size()
		}
		return err
	})
	return n, err
}

// replica opens a local replica of the sender's log logID and pulls it.
func replica(ctx context.Context, st *kolostore.Store, conn *kolosecure.Conn, owner ed25519.PublicKey, logID kololog.LogID) (*kolostore.Replica, error) {
	header, err := kolosync.FetchHeader(ctx, conn, owner, logID)
	if errors.Is(err, kolosync.ErrNotFound) {
		return nil, fmt.Errorf("the sender does not share this folder with this code: %w", err)
	} else if err != nil {
		return nil, fmt.Errorf("ask the sender for its log: %w", err)
	}
	rep, err := st.OpenReplica(owner, logID, header)
	if err != nil {
		return nil, err
	}
	if err := kolosync.Pull(ctx, conn, rep); err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}
	return rep, nil
}

// printChanges prints what Diff found between two versions.
func printChanges(changes []kolofiles.Change) {
	for _, c := range changes {
		switch {
		case c.Deleted:
			log.Print("  deleted a file or folder (its names are gone with it)")
		case c.Old == "":
			log.Printf("  added %s", c.New)
		case c.Old != c.New && c.Modified:
			log.Printf("  moved %s to %s and changed it", c.Old, c.New)
		case c.Old != c.New:
			log.Printf("  moved %s to %s", c.Old, c.New)
		default:
			log.Printf("  changed %s", c.New)
		}
	}
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

// shareCode is all the receiver needs: where the sender listens, its public key, the volume's metadata log and
// the seed of the receiver's identity. The file keys travel inside the metadata log. Whoever has it can read the folder.
type shareCode struct {
	addr   netip.AddrPort
	sender ed25519.PublicKey
	meta   kololog.LogID
	seed   [32]byte
}

const (
	codePrefix = "kolo-folder:"
	codeSize   = 6 + 32 + kololog.LogIDSize + 32
)

func (c shareCode) String() string {
	ip := c.addr.Addr().As4()
	b := binary.BigEndian.AppendUint16(ip[:], c.addr.Port())
	b = append(b, c.sender...)
	b = append(b, c.meta[:]...)
	b = append(b, c.seed[:]...)
	return codePrefix + base64.RawURLEncoding.EncodeToString(b)
}

func parseCode(s string) (shareCode, error) {
	rest, ok := strings.CutPrefix(strings.TrimSpace(s), codePrefix)
	b, err := base64.RawURLEncoding.DecodeString(rest)
	if !ok || err != nil || len(b) != codeSize {
		return shareCode{}, fmt.Errorf("this is not a folder share code; copy the whole line, it starts with %q", codePrefix)
	}
	var c shareCode
	c.addr = netip.AddrPortFrom(netip.AddrFrom4([4]byte(b[:4])), binary.BigEndian.Uint16(b[4:6]))
	if !checkAddr(c.addr) {
		return shareCode{}, fmt.Errorf("the share code names %s, an address nobody can listen on", c.addr)
	}
	b = b[6:]
	c.sender = ed25519.PublicKey(b[:32])
	b = b[32:]
	b = b[copy(c.meta[:], b):]
	copy(c.seed[:], b)
	return c, nil
}
