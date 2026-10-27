// Command raftlockd runs one RaftLock node over HTTP.
//
//	go run ./cmd/raftlockd --id n1 --listen :7001 --peers n2=:7002,n3=:7003 --data-dir /tmp/rl/n1
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
	"github.com/hithisisyeshwanth/RaftLock/internal/rpc"
	"github.com/hithisisyeshwanth/RaftLock/internal/storage"
)

type options struct {
	ID      raft.NodeID
	Listen  string
	Peers   map[raft.NodeID]string
	DataDir string
}

func parseFlags(args []string) (options, error) {
	var o options
	var id, peers string
	fs := flag.NewFlagSet("raftlockd", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // the caller prints the returned error
	fs.StringVar(&id, "id", "", "this node's id, e.g. n1")
	fs.StringVar(&o.Listen, "listen", "", "address to accept peer messages on, e.g. :7001")
	fs.StringVar(&peers, "peers", "", "the other nodes, as id=addr,id=addr")
	fs.StringVar(&o.DataDir, "data-dir", "", "directory for this node's durable state")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	for _, f := range []struct{ name, v string }{{"id", id}, {"listen", o.Listen}, {"peers", peers}, {"data-dir", o.DataDir}} {
		if f.v == "" {
			return o, fmt.Errorf("--%s is required", f.name)
		}
	}
	o.ID = raft.NodeID(id)
	o.Peers = map[raft.NodeID]string{}
	for _, p := range strings.Split(peers, ",") {
		pid, addr, ok := strings.Cut(p, "=")
		if !ok || pid == "" || addr == "" {
			return o, fmt.Errorf("--peers: %q is not id=addr", p)
		}
		if raft.NodeID(pid) == o.ID {
			return o, fmt.Errorf("--peers: %s is this node's own id", pid)
		}
		if _, dup := o.Peers[raft.NodeID(pid)]; dup {
			return o, fmt.Errorf("--peers: %s listed twice", pid)
		}
		o.Peers[raft.NodeID(pid)] = addr
	}
	return o, nil
}

func main() {
	o, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "raftlockd:", err)
		os.Exit(2)
	}
	if err := run(o); err != nil {
		log.Fatal(err)
	}
}

func run(o options) error {
	log.SetPrefix(string(o.ID) + " ")
	if err := os.MkdirAll(o.DataDir, 0o755); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", o.Listen)
	if err != nil {
		return err
	}
	r, err := rpc.Start(rpc.Config{
		ID:       o.ID,
		Peers:    o.Peers,
		Listener: ln,
		Storage:  storage.NewFileStorage(filepath.Join(o.DataDir, "state")),
		Raft:     raft.DefaultConfig(),
		Seed:     time.Now().UnixNano(),
	})
	if err != nil {
		ln.Close()
		return err
	}
	log.Printf("listening on %s, peers %v, data in %s", ln.Addr(), o.Peers, o.DataDir)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	watch := time.NewTicker(50 * time.Millisecond)
	defer watch.Stop()
	var last raft.Snapshot
	for {
		select {
		case <-stop:
			log.Print("shutting down")
			return r.Close()
		case <-watch.C:
			// ponytail: polls the role instead of tracing every event; misses a change that reverts within 50 ms.
			s, err := r.Snapshot()
			if errors.Is(err, rpc.ErrClosed) {
				return err
			}
			if s.Role != last.Role || s.Term != last.Term {
				if s.Role == raft.Leader {
					log.Printf("became leader in term %d", s.Term)
				} else {
					log.Printf("%s in term %d", s.Role, s.Term)
				}
			}
			last = s
		}
	}
}
