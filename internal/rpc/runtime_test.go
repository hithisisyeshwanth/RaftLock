package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hithisisyeshwanth/RaftLock/internal/lock"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// These tests run the real thing: three servers in this process, each on its own localhost port,
// talking HTTP/JSON, with real 1 ms ticks. Run with -race: the runtime's promise is that one
// goroutine owns each node, so concurrent callers must never touch it directly.

var ids = []raft.NodeID{"n1", "n2", "n3"}

// startCluster starts three runtimes on fresh localhost ports and stops them when the test ends.
func startCluster(t *testing.T) map[raft.NodeID]*Runtime {
	t.Helper()
	lns := map[raft.NodeID]net.Listener{}
	addrs := map[raft.NodeID]string{}
	for _, id := range ids {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[id], addrs[id] = ln, ln.Addr().String()
	}
	rts := map[raft.NodeID]*Runtime{}
	for i, id := range ids {
		peers := map[raft.NodeID]string{}
		for _, p := range ids {
			if p != id {
				peers[p] = addrs[p]
			}
		}
		rt, err := Start(Config{
			ID:       id,
			Peers:    peers,
			Listener: lns[id],
			Storage:  raft.NewMemStorage(),
			Raft:     raft.DefaultConfig(),
			Seed:     int64(i + 1),
		})
		if err != nil {
			t.Fatalf("Start(%s): %v", id, err)
		}
		rts[id] = rt
		t.Cleanup(func() { rt.Close() })
	}
	return rts
}

// waitLeader polls until exactly one live runtime reports itself leader of the highest term.
func waitLeader(t *testing.T, rts map[raft.NodeID]*Runtime, within time.Duration) raft.NodeID {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var leaders []raft.NodeID
		for id, rt := range rts {
			if s, err := rt.Snapshot(); err == nil && s.Role == raft.Leader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no single leader within %v", within)
	return ""
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

func acquire(lockName string) lock.Command { return lock.Command{Kind: lock.Acquire, Lock: lockName} }

func TestRuntimeElectsLeader(t *testing.T) {
	rts := startCluster(t)
	leader := waitLeader(t, rts, 5*time.Second)

	// Everyone agrees on the leader within a few heartbeats.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		agreed := true
		for _, rt := range rts {
			if s, err := rt.Snapshot(); err != nil || s.LeaderID != leader {
				agreed = false
			}
		}
		if agreed {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("not every node names %s as leader", leader)
}

// A Submit on the leader returns once the command is committed and applied; a follower redirects.
func TestRuntimeSubmit(t *testing.T) {
	rts := startCluster(t)
	leader := waitLeader(t, rts, 5*time.Second)

	call, err := rts[leader].Submit(ctx(t), "c1", 1, acquire("jobs"))
	if err != nil {
		t.Fatalf("Submit on the leader: %v", err)
	}
	if call.Err() != nil || !call.Result().OK || call.Result().Token == 0 {
		t.Fatalf("Submit on the leader = %+v, %v; want a grant with a token", call.Result(), call.Err())
	}

	for _, id := range ids {
		if id == leader {
			continue
		}
		f, err := rts[id].Submit(ctx(t), "c2", 1, acquire("jobs"))
		if err != nil {
			t.Fatalf("Submit on follower %s: %v", id, err)
		}
		if !errors.Is(f.Err(), lock.ErrNotLeader) || f.Hint() != leader {
			t.Errorf("Submit on follower %s = err %v, hint %q; want ErrNotLeader hinting %s", id, f.Err(), f.Hint(), leader)
		}
	}

	// The command reaches every node's state machine over the real network.
	want, _ := rts[leader].Dump()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		same := true
		for _, rt := range rts {
			if d, err := rt.Dump(); err != nil || d != want {
				same = false
			}
		}
		if same {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("followers did not reach the leader's state %s", want)
}

// Many goroutines submit at once. Only the runtime's own goroutine may touch the node, so with
// -race any unsynchronised access fails this test.
func TestRuntimeConcurrentSubmits(t *testing.T) {
	rts := startCluster(t)
	leader := waitLeader(t, rts, 5*time.Second)

	var wg sync.WaitGroup
	errs := make(chan error, 30)
	for c := 1; c <= 5; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			client := fmt.Sprintf("c%d", c)
			for seq := uint64(1); seq <= 6; seq++ {
				call, err := rts[leader].Submit(ctx(t), client, seq, acquire(fmt.Sprintf("%s-%d", client, seq)))
				if err != nil || call.Err() != nil || !call.Result().OK {
					errs <- fmt.Errorf("%s seq %d: %v %v %+v", client, seq, err, call.Err(), call.Result())
					return
				}
			}
		}(c)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	// Reading state from other goroutines while the node runs is safe too.
	for _, rt := range rts {
		if _, err := rt.Snapshot(); err != nil {
			t.Error(err)
		}
	}
}

// Closing the leader is a crash as far as the others are concerned: they elect a new leader and
// keep serving.
func TestRuntimeLeaderFailover(t *testing.T) {
	rts := startCluster(t)
	old := waitLeader(t, rts, 5*time.Second)
	if err := rts[old].Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	delete(rts, old)

	leader := waitLeader(t, rts, 5*time.Second)
	call, err := rts[leader].Submit(ctx(t), "c1", 1, acquire("jobs"))
	if err != nil || call.Err() != nil || !call.Result().OK {
		t.Errorf("Submit on the new leader %s = %+v, %v, %v; want a grant", leader, call.Result(), err, call.Err())
	}
}

// Close stops the loop and the HTTP server; it is safe to call twice, and later calls fail cleanly.
func TestRuntimeClose(t *testing.T) {
	rts := startCluster(t)
	rt := rts["n1"]
	if err := rt.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := rt.Close(); err != nil {
		t.Errorf("second Close: %v, want nil", err)
	}
	if _, err := rt.Submit(ctx(t), "c1", 1, acquire("jobs")); !errors.Is(err, ErrClosed) {
		t.Errorf("Submit after Close = %v, want ErrClosed", err)
	}
	if _, err := rt.Snapshot(); !errors.Is(err, ErrClosed) {
		t.Errorf("Snapshot after Close = %v, want ErrClosed", err)
	}
}
