# kolo-share-example

[![status: beta](https://img.shields.io/badge/status-beta-orange)](CHANGELOG.md)
[![license: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
[![go: 1.26+](https://img.shields.io/badge/go-1.26%2B-00ADD8)](go.mod)

Share a file or a folder between two terminals with
[kolo-blobs](https://github.com/ciranda-dev/kolo-blobs) and
[kolo-files](https://github.com/ciranda-dev/kolo-files).

This repository is the finished code of the tutorial "Share files between two
terminals" in the Kolo docs. It has two small programs, one per library:

- `file` shares one file with kolo-blobs: the sender prints a share code, the
  receiver uses it to fetch the file, checks every chunk and saves it.
- `folder` shares a folder with kolo-files: the receiver saves every version
  the sender publishes, and prints what changed between them.

Requirements: Go 1.26 or newer. The versions of kolo-blobs, kolo-files and
kolo-sync it uses are pinned in `go.mod`.

## Share a file

In a first terminal, share a file:

```sh
go run ./file send photo.jpg
```

It stores the file, prints its SHA-256 and a share code, and waits:

```text
20:43:56 stored photo.jpg as blob 7573860ce1c67fe8 in 6 log entries
20:43:56 sha256 cec861d1c16a906271b9d62340c559b4203a591c54217e24e6a2fb4b58358763
share code:
kolo-file:fwAAAeWwzXkT8Yqjotld...
20:43:56 waiting for the receiver, press Ctrl-C to stop
```

In a second terminal, paste the whole code (in quotes) and a name for the copy:

```sh
go run ./file get 'kolo-file:fwAAAeWwzXkT8Yqjotld...' copy.jpg
```

```text
20:43:56 downloading
20:43:56 received 6 log entries, each checked against the sender's signed head
20:43:56 saved copy.jpg: 5000000 bytes, every chunk checked against the blob ID
20:43:56 sha256 cec861d1c16a906271b9d62340c559b4203a591c54217e24e6a2fb4b58358763
```

The two SHA-256 lines match. `get` never replaces an existing file, creates
the copy readable by you only, and removes the partial file when it fails or is
interrupted.

A share code is an address plus a secret: whoever has it can read what it
shares, and a receiver connects to whatever address a code names. Only use
codes from people you trust, and send them over a channel you trust. The code
on the command line shows in `ps` and in the shell history; pass `-` instead
and `get` reads the code from standard input:

```sh
echo 'kolo-file:fwAAAeWwzXkT8Yqjotld...' | go run ./file get - copy.jpg
```

`get` stops when what it downloads, or what it writes, would be more than
`-max-bytes` (1 GiB by default):

```sh
go run ./file -max-bytes 100000000 get 'kolo-file:...' copy.jpg
```

## Share a folder

In a first terminal:

```sh
go run ./folder send shared
```

In a second terminal, with the code it prints:

```sh
go run ./folder get 'kolo-folder:...' copy
```

The receiver saves the folder in `copy/v5` (the version number); `copy` must
not exist yet, and it and the folders in it are readable by you only. Change,
add or delete files in `shared`, then press Enter in the first terminal: the
sender publishes a new version, and within two seconds the receiver prints
what changed and saves it in a new folder next to the old one:

```text
20:45:20   added plans.txt
20:45:20   changed todo.txt
20:45:20 saved version 10 in copy/v10, every file checked against its blob ID
```

Every version is a full copy, so `get` stops when the copies it wrote in `copy`
add up to more than `-max-bytes` (1 GiB by default), or when what it downloads
is more than that. The code can also be read from standard input with `-`, as for `file`.

## One file or a folder

| | `file` (kolo-blobs) | `folder` (kolo-files) |
|---|---|---|
| What is shared | the bytes of one file | a tree of files and folders, with names |
| What the code carries | the blob's ID and its key | the metadata log's ID; the file keys travel inside that log |
| Changes | a new file is a new code | each change is a new version of the same volume |
| History | none | every version stays readable; `Snapshot` and `Diff` |
| On the wire | one kolo-store log, pulled with kolo-sync | two logs (metadata and content), pulled with kolo-sync |

kolo-files keeps every file's content as a kolo-blobs blob, so the folder
program checks every file the same way the file program does.

## How it works

- Each sender has a new kolo-id identity per run, keeps its logs in a
  kolo-store in a new temporary folder, and removes that folder when it stops
  on Ctrl-C, `kill` or a closed terminal.
- The share code holds the sender's IPv4 address and port, its public key,
  the ID of the log to copy and a random seed. The receiver turns the seed into
  its identity; the sender gives kolo-sync access only to that identity, so a
  peer without the code gets nothing. Whoever has the code can read what it
  shares, so send it over a channel you trust.
- The receiver connects with kolo-secure, which checks the sender's key,
  copies the log with kolo-sync (every entry is checked against the sender's
  signed head), then reads the file through kolo-blobs, which checks every
  chunk against the blob ID before returning its bytes.
- `-listen` sets the address the sender listens on, `127.0.0.1:0` by default,
  which keeps everything on one computer. The share code carries that address,
  so it must be an IPv4 address the receiver can reach.

## Between two computers

The two programs connect directly: the share code carries the sender's IPv4
address and port, and the receiver dials it. They do not use the DHT or the
swarm, so there is no bootstrap to configure.

- On one computer it works as it is (`-listen` defaults to `127.0.0.1:0`).
- On the same local network, the sender passes `-listen` with the IPv4 address
  of its network card and a port, and the receiver reaches it:

  ```sh
  go run ./file -listen 192.168.1.20:7450 send photo.jpg
  ```

  The address must be IPv4 and not `0.0.0.0`; otherwise `send` stops with
  `-listen needs an IPv4 address the receiver can reach`.
- Between two different networks it works only when the sender has a public
  IPv4 address the receiver can reach, with the UDP port open in its firewall.
- Two computers that are both behind home routers will not connect: these
  programs do not punch through NAT and use no relay.

For that case use the Kolo apps (chat, share, meet, board) or the
[kolo-swarm](https://github.com/ciranda-dev/kolo-swarm) library, which find
peers through the public entry nodes by default.

## Tests

```sh
go test ./...
```

The tests build each program and run a sender and a receiver as two
processes on 127.0.0.1, in temporary folders: a file of 0 bytes, 1 byte and
3 MB, a folder through several versions (including a folder that becomes a
file and a file that becomes a folder), and the errors (a bad code, a code with
a wrong key or a wrong reader or an address nobody listens on, a sender that
stopped, an output that exists, a sender that offers more than `-max-bytes`).
The test of the download limit sends a file of 200 MB and is skipped with
`go test -short`.
`FuzzParseCode` in each package checks that no input makes the share code
parser panic.

## License

Licensed under either of the [Apache License, Version 2.0](LICENSE) or the [MIT license](LICENSE-MIT), at your option.
