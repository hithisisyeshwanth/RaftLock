package lock_test

import (
	"errors"
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/lock"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
	"github.com/hithisisyeshwanth/RaftLock/internal/sim"
)

// The lock server runs on every node: it wraps a Raft node, applies committed entries to its
// LockSM, and answers Submit once the command it proposed has been applied. These tests run real
// servers in the simulation harness, so all the safety checkers watch every tick.

var three = []raft.NodeID{"n1", "n2", "n3"}

func newServer(cfg raft.Config, id raft.NodeID, peers []raft.NodeID, st raft.Storage, tr raft.Tracer, to raft.TimeoutSource, onApply func(raft.Entry)) (sim.Peer, error) {
	return lock.NewServer(cfg, id, peers, st, tr, to, onApply)
}

// lockCluster is three lock servers with an elected leader that every follower has heard from.
func lockCluster(t *testing.T, seed int64) (*sim.Cluster, raft.NodeID) {
	t.Helper()
	c := sim.NewClusterWith(3, seed, newServer)
	if err := c.RunUntil(func() bool { _, ok := c.Leader(); return ok }, 1000); err != nil {
		t.Fatal(err)
	}
	run(t, c, 60)
	id, _ := c.Leader()
	return c, id
}

func server(t *testing.T, c *sim.Cluster, id raft.NodeID) *lock.Server {
	t.Helper()
	s, ok := c.Peer(id).(*lock.Server)
	if !ok {
		t.Fatalf("peer %s is %T, want a live *lock.Server", id, c.Peer(id))
	}
	return s
}

func run(t *testing.T, c *sim.Cluster, ticks int) {
	t.Helper()
	for i := 0; i < ticks; i++ {
		if err := c.Step(); err != nil {
			t.Fatal(err)
		}
	}
}

func submit(c *sim.Cluster, s *lock.Server, client string, seq uint64, cmd lock.Command) *lock.Call {
	call, msgs := s.Submit(client, seq, cmd)
	c.Send(msgs)
	return call
}

func waitDone(t *testing.T, c *sim.Cluster, call *lock.Call, max int) {
	t.Helper()
	if err := c.RunUntil(call.Done, max); err != nil {
		t.Fatalf("Submit not answered: %v", err)
	}
}

func acquire(lockName string, ttl int) lock.Command {
	return lock.Command{Kind: lock.Acquire, Lock: lockName, TTL: ttl}
}

// Submit on the leader answers only after the command is committed and applied, with its result.
func TestSubmitOnLeaderWaitsForApply(t *testing.T) {
	c, leader := lockCluster(t, 1)
	s := server(t, c, leader)

	call := submit(c, s, "c1", 1, acquire("jobs", 0))
	if call.Done() {
		t.Fatal("Submit answered before anything was replicated")
	}
	waitDone(t, c, call, 500)

	if call.Err() != nil || !call.Result().OK || call.Result().Token == 0 {
		t.Fatalf("Submit = %+v, %v; want a grant with a token", call.Result(), call.Err())
	}
	if snap, _ := c.Snapshot(leader); snap.LastApplied < call.Result().Token {
		t.Errorf("answered with token %d while the leader has applied only up to %d", call.Result().Token, snap.LastApplied)
	}
	if s.Waiting() != 0 {
		t.Errorf("Waiting() = %d after the answer, want 0", s.Waiting())
	}
}

// A follower cannot propose; it points the client at the leader instead.
func TestSubmitOnFollowerRedirects(t *testing.T) {
	c, leader := lockCluster(t, 1)
	for _, id := range three {
		if id == leader {
			continue
		}
		before, _ := c.Snapshot(leader)
		call := submit(c, server(t, c, id), "c1", 1, acquire("jobs", 0))

		if !call.Done() || !errors.Is(call.Err(), lock.ErrNotLeader) || call.Hint() != leader {
			t.Errorf("Submit on follower %s = done %v, err %v, hint %q; want ErrNotLeader at once, hinting %s",
				id, call.Done(), call.Err(), call.Hint(), leader)
		}
		run(t, c, 100)
		if after, _ := c.Snapshot(leader); len(after.Log) != len(before.Log) {
			t.Errorf("a follower's Submit added %d entries to the leader's log, want none", len(after.Log)-len(before.Log))
		}
	}
}

// The leader is cut off right after accepting the command, so its entry never commits. The others
// elect a new leader, whose no-op takes that index. When the old leader rejoins, the index holds
// an entry of a different term, so the command can never be applied there: ErrLeaderChanged.
func TestSubmitLeaderChangedBeforeCommit(t *testing.T) {
	c, old := lockCluster(t, 1)
	s := server(t, c, old)
	s.Timeout = 100000
	var others []raft.NodeID
	for _, id := range three {
		if id != old {
			others = append(others, id)
		}
	}
	c.Partition([]raft.NodeID{old}, others)

	call := submit(c, s, "c1", 1, acquire("jobs", 0))
	if err := c.RunUntil(func() bool { id, ok := c.Leader(); return ok && id != old }, 1000); err != nil {
		t.Fatal(err)
	}
	run(t, c, 100)
	c.Heal()
	waitDone(t, c, call, 1000)

	if !errors.Is(call.Err(), lock.ErrLeaderChanged) {
		t.Errorf("Submit = %+v, %v; want ErrLeaderChanged", call.Result(), call.Err())
	}
	if s.Waiting() != 0 {
		t.Errorf("Waiting() = %d, want 0", s.Waiting())
	}
}

// A leader that can never commit (cut off from everyone) gives up after its timeout and forgets the waiter.
func TestSubmitTimesOut(t *testing.T) {
	c, leader := lockCluster(t, 1)
	s := server(t, c, leader)
	s.Timeout = 200
	var others []raft.NodeID
	for _, id := range three {
		if id != leader {
			others = append(others, id)
		}
	}
	c.Partition([]raft.NodeID{leader}, others)

	call := submit(c, s, "c1", 1, acquire("jobs", 0))
	run(t, c, 199)
	if call.Done() {
		t.Fatalf("answered after 199 ticks with %v, want still waiting until the 200-tick timeout", call.Err())
	}
	run(t, c, 1)

	if !call.Done() || !errors.Is(call.Err(), lock.ErrTimeout) {
		t.Errorf("after 200 ticks: done %v, err %v; want ErrTimeout", call.Done(), call.Err())
	}
	if s.Waiting() != 0 {
		t.Errorf("Waiting() = %d after the timeout, want 0 (the waiter is cleaned up)", s.Waiting())
	}
}

// The client gives up on a crashed leader and sends the same (clientID, seq) to the new one. Whether
// or not the first copy was committed, the command takes effect once: the token is the index of the
// first committed copy, and every server agrees.
func TestSubmitRetryOnNewLeaderAppliesOnce(t *testing.T) {
	for _, wait := range []int{0, 3, 30} { // crash before, around, and after replication
		c, old := lockCluster(t, 1)
		submit(c, server(t, c, old), "c1", 1, acquire("jobs", 0))
		run(t, c, wait)
		c.Crash(old)
		if err := c.RunUntil(func() bool { _, ok := c.Leader(); return ok }, 1000); err != nil {
			t.Fatal(err)
		}
		leader, _ := c.Leader()

		call := submit(c, server(t, c, leader), "c1", 1, acquire("jobs", 0))
		waitDone(t, c, call, 1000)
		if call.Err() != nil || !call.Result().OK {
			t.Fatalf("wait %d: retry = %+v, %v; want a grant", wait, call.Result(), call.Err())
		}

		snap, _ := c.Snapshot(leader)
		var first raft.Index
		for _, e := range snap.Log {
			if cmd, err := lock.Decode(e.Cmd); err == nil && cmd.ClientID == "c1" && cmd.Seq == 1 {
				first = e.Index
				break
			}
		}
		if call.Result().Token != first {
			t.Errorf("wait %d: retry token = %d, want %d, the first committed copy (later copies are deduplicated)", wait, call.Result().Token, first)
		}
		run(t, c, 100)
		want := server(t, c, leader).Dump()
		for _, id := range three {
			if id != old && server(t, c, id).Dump() != want {
				t.Errorf("wait %d: %s state %s differs from the leader's %s", wait, id, server(t, c, id).Dump(), want)
			}
		}
	}
}

// With servers in place, T5.4's leases run for real: a lock taken with a TTL is expired through the
// log, on every server, without its holder doing anything.
func TestSubmitTTLExpiresThroughTheLog(t *testing.T) {
	c, leader := lockCluster(t, 1)
	call := submit(c, server(t, c, leader), "c1", 1, acquire("jobs", 100))
	waitDone(t, c, call, 500)
	if !call.Result().OK {
		t.Fatalf("acquire = %+v, %v", call.Result(), call.Err())
	}

	run(t, c, 300) // 100-tick TTL, plus time to propose, commit and apply the Expire
	taken := submit(c, server(t, c, leader), "c2", 1, acquire("jobs", 0))
	waitDone(t, c, taken, 500)
	if !taken.Result().OK {
		t.Errorf("c2's acquire after the TTL = %+v, %v; want the lock, freed by an Expire entry", taken.Result(), taken.Err())
	}
}
