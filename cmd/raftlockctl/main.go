// Command raftlockctl acquires, releases and inspects locks on a RaftLock cluster.
//
//	go run ./cmd/raftlockctl --servers :7001,:7002,:7003 --client c1 acquire jobs --ttl 5000
//	go run ./cmd/raftlockctl --servers :7001,:7002,:7003 --client c1 release jobs --token 7
//	go run ./cmd/raftlockctl --servers :7001,:7002,:7003 --client c1 status jobs
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/hithisisyeshwanth/RaftLock/internal/lock"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
	"github.com/hithisisyeshwanth/RaftLock/internal/rpc"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is one CLI invocation. It exits 0 on success, 1 when the cluster refuses, 2 on a bad command line.
func run(args []string, stdout, stderr io.Writer) int {
	servers, client, cmd, err := parse(args)
	if err != nil {
		fmt.Fprintln(stderr, "raftlockctl:", err)
		return 2
	}

	ck := lock.NewClerk(client, servers, rpc.NewClientTransport())
	// Every run is a new Clerk for the same client; the clock keeps its seqs above earlier runs'.
	ck.SetSeq(uint64(time.Now().UnixNano()))
	call := ck.Do(cmd)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !call.Done() {
		<-tick.C
		ck.Tick()
	}

	res := call.Result()
	switch {
	case cmd.Kind == lock.Status && res.Holder == "":
		fmt.Fprintf(stdout, "%s is free\n", cmd.Lock)
	case cmd.Kind == lock.Status:
		fmt.Fprintf(stdout, "%s held by %s token=%d\n", cmd.Lock, res.Holder, res.Token)
	case !res.OK:
		fmt.Fprintf(stderr, "%s: %s\n", cmd.Kind, res.Err)
		return 1
	case cmd.Kind == lock.Acquire:
		fmt.Fprintf(stdout, "acquired %s token=%d\n", cmd.Lock, res.Token)
	default:
		fmt.Fprintf(stdout, "released %s\n", cmd.Lock)
	}
	return 0
}

func parse(args []string) ([]raft.NodeID, string, lock.Command, error) {
	var cmd lock.Command
	var servers, client string
	fs := flag.NewFlagSet("raftlockctl", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // run prints the returned error
	fs.StringVar(&servers, "servers", "", "server addresses, comma separated, e.g. :7001,:7002,:7003")
	fs.StringVar(&client, "client", "", "this client's id; release must come from the client that acquired")
	if err := fs.Parse(args); err != nil {
		return nil, "", cmd, err
	}
	if servers == "" {
		return nil, "", cmd, errors.New("--servers is required")
	}
	if client == "" {
		return nil, "", cmd, errors.New("--client is required")
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return nil, "", cmd, errors.New("missing command: acquire, release or status")
	}
	switch rest[0] {
	case "acquire":
		cmd.Kind = lock.Acquire
	case "release":
		cmd.Kind = lock.Release
	case "status":
		cmd.Kind = lock.Status
	default:
		return nil, "", cmd, fmt.Errorf("unknown command %q: want acquire, release or status", rest[0])
	}
	if len(rest) < 2 || strings.HasPrefix(rest[1], "-") {
		return nil, "", cmd, fmt.Errorf("%s: missing lock name", rest[0])
	}
	cmd.Lock = rest[1]

	sub := flag.NewFlagSet(rest[0], flag.ContinueOnError)
	sub.SetOutput(io.Discard)
	var token uint64
	switch cmd.Kind {
	case lock.Acquire:
		sub.IntVar(&cmd.TTL, "ttl", 0, "milliseconds the lock may be held before it expires")
	case lock.Release:
		sub.Uint64Var(&token, "token", 0, "the fencing token from acquire")
	}
	if err := sub.Parse(rest[2:]); err != nil {
		return nil, "", cmd, fmt.Errorf("%s: %w", rest[0], err)
	}
	if sub.NArg() > 0 {
		return nil, "", cmd, fmt.Errorf("%s: unexpected argument %q", rest[0], sub.Arg(0))
	}
	if cmd.Kind == lock.Release {
		if token == 0 {
			return nil, "", cmd, errors.New("release: --token is required")
		}
		cmd.Token = raft.Index(token)
	}

	var ids []raft.NodeID
	for _, s := range strings.Split(servers, ",") {
		ids = append(ids, raft.NodeID(s))
	}
	return ids, client, cmd, nil
}
