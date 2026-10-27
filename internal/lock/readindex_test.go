package lock_test

import (
	"errors"
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/lock"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
	"github.com/hithisisyeshwanth/RaftLock/internal/sim"
)

// ReadIndex (section 8) answers a read without a log entry. The leader may answer only once
//  1. it has committed an entry from its current term (its no-op), so its commitIndex is up to date;
//  2. it records readIndex = commitIndex;
//  3. a majority acknowledges a round of messages sent after the read arrived, so it was still the
//     leader when the read began;
//  4. it has applied up to readIndex.
// It lives in the lock server, not the Raft core.

func read(c *sim.Cluster, s *lock.Server, lockName string) *lock.Call {
	call, msgs := s.Read(lockName)
	c.Send(msgs)
	return call
}

// A read after a completed write sees it, and costs no log entry.
func TestReadIndexSeesCompletedWriteWithoutTheLog(t *testing.T) {
	c, leader := lockCluster(t, 1)
	s := server(t, c, leader)
	w := submit(c, s, "c1", 1, acquire("jobs", 0))
	waitDone(t, c, w, 500)
	before, _ := c.Snapshot(leader)

	r := read(c, s, "jobs")
	waitDone(t, c, r, 500)

	if r.Err() != nil || !r.Result().OK || r.Result().Holder != "c1" || r.Result().Token != w.Result().Token {
		t.Errorf("read = %+v, %v; want holder c1 with token %d", r.Result(), r.Err(), w.Result().Token)
	}
	if after, _ := c.Snapshot(leader); len(after.Log) != len(before.Log) {
		t.Errorf("a ReadIndex read added %d log entries, want none", len(after.Log)-len(before.Log))
	}
}

// Being leader is not proof enough on its own: the answer waits for a majority to confirm it.
func TestReadIndexWaitsForMajorityConfirmation(t *testing.T) {
	c, leader := lockCluster(t, 1)
	r := read(c, server(t, c, leader), "jobs")
	if r.Done() {
		t.Fatal("the leader answered at once, before any follower confirmed its leadership")
	}
	waitDone(t, c, r, 500)
	if r.Err() != nil || !r.Result().OK {
		t.Errorf("read = %+v, %v; want an answer once confirmed", r.Result(), r.Err())
	}
}

func TestReadIndexFollowerRedirects(t *testing.T) {
	c, leader := lockCluster(t, 1)
	r := read(c, server(t, c, others(leader)[0]), "jobs")
	if !r.Done() || !errors.Is(r.Err(), lock.ErrNotLeader) || r.Hint() != leader {
		t.Errorf("read on a follower: done %v, err %v, hint %q; want ErrNotLeader at once, hinting %s", r.Done(), r.Err(), r.Hint(), leader)
	}
}

// The cut-off old leader still believes it leads, while the majority side moves the lock to c2.
// It cannot get its leadership confirmed, so it never answers from its stale table.
func TestReadIndexPartitionedOldLeaderCannotServe(t *testing.T) {
	c, old := lockCluster(t, 1)
	first := submit(c, server(t, c, old), "c1", 1, acquire("jobs", 0))
	waitDone(t, c, first, 500)
	run(t, c, 60)

	c.Partition([]raft.NodeID{old}, others(old))
	if err := c.RunUntil(func() bool { id, ok := c.Leader(); return ok && id != old }, 1000); err != nil {
		t.Fatal(err)
	}
	now, _ := c.Leader()
	waitDone(t, c, submit(c, server(t, c, now), "c1", 2, release("jobs", first.Result().Token)), 500)
	taken := submit(c, server(t, c, now), "c2", 1, acquire("jobs", 0))
	waitDone(t, c, taken, 500)

	s := server(t, c, old)
	s.Timeout = 300
	stale := read(c, s, "jobs")
	waitDone(t, c, stale, 1000)
	if stale.Err() == nil {
		t.Fatalf("the cut-off old leader answered with %+v; it must not serve reads", stale.Result())
	}

	fresh := read(c, server(t, c, now), "jobs")
	waitDone(t, c, fresh, 500)
	if fresh.Result().Holder != "c2" || fresh.Result().Token != taken.Result().Token {
		t.Errorf("read on the new leader = %+v, want holder c2 with token %d", fresh.Result(), taken.Result().Token)
	}
}

// A brand-new leader's commitIndex may lag the entries the old leader committed, until something
// from its own term commits. Normally that is the no-op; with the no-op disabled the read must
// wait for the first client write of the term instead.
func TestReadIndexWaitsForCurrentTermCommit(t *testing.T) {
	c := sim.NewClusterWith(3, 1, newServer)
	cfg := raft.DefaultConfig()
	cfg.DisableNoop = true
	c.SetConfig(cfg)
	for _, id := range three {
		c.Crash(id)
		if err := c.Restart(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.RunUntil(func() bool { _, ok := c.Leader(); return ok }, 1000); err != nil {
		t.Fatal(err)
	}
	run(t, c, 60)
	leader, _ := c.Leader()
	s := server(t, c, leader)

	r := read(c, s, "jobs")
	run(t, c, 300) // plenty of time for confirmation, but nothing from this term is committed
	if r.Done() {
		t.Fatalf("answered %+v, %v before any entry of the leader's term committed", r.Result(), r.Err())
	}

	w := submit(c, s, "c1", 1, acquire("jobs", 0))
	waitDone(t, c, w, 500)
	waitDone(t, c, r, 500)
	if r.Err() != nil || !r.Result().OK {
		t.Errorf("read after the first commit of the term = %+v, %v; want an answer", r.Result(), r.Err())
	}
}

// With the no-op on (the default), a read right after an election is answered once the no-op commits.
func TestReadIndexRightAfterElection(t *testing.T) {
	c := sim.NewClusterWith(3, 1, newServer)
	if err := c.RunUntil(func() bool { _, ok := c.Leader(); return ok }, 1000); err != nil {
		t.Fatal(err)
	}
	leader, _ := c.Leader()
	r := read(c, server(t, c, leader), "jobs")
	waitDone(t, c, r, 500)

	snap, _ := c.Snapshot(leader)
	if r.Err() != nil || snap.CommitIndex == 0 || snap.Log[snap.CommitIndex-1].Term != snap.Term {
		t.Errorf("read = %+v, %v with commitIndex %d; want an answer only after the term's no-op committed", r.Result(), r.Err(), snap.CommitIndex)
	}
}
