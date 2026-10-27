package sim

import (
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/lock"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

func srv(t *testing.T, c *Cluster, id raft.NodeID) *lock.Server {
	t.Helper()
	s, ok := c.Peer(id).(*lock.Server)
	if !ok {
		t.Fatalf("%s is not a live lock server", id)
	}
	return s
}

// commit submits cmd on the current leader and waits for its answer.
func commit(t *testing.T, c *Cluster, client string, seq uint64, cmd lock.Command) lock.Result {
	t.Helper()
	if err := c.RunUntil(hasLeader(c), 2000); err != nil {
		t.Fatal(err)
	}
	id, _ := c.Leader()
	call, msgs := srv(t, c, id).Submit(client, seq, cmd)
	c.Send(msgs)
	if err := c.RunUntil(call.Done, 1000); err != nil {
		t.Fatal(err)
	}
	if call.Err() != nil {
		t.Fatalf("%s seq %d: %v", client, seq, call.Err())
	}
	return call.Result()
}

func acq(lockName string) lock.Command { return lock.Command{Kind: lock.Acquire, Lock: lockName} }

// wantRestartedFromDisk checks what a restart must bring back, and what it must not: durable term,
// vote and log come from storage; commitIndex, lastApplied and the state machine start empty.
func wantRestartedFromDisk(t *testing.T, c *Cluster, id raft.NodeID) {
	t.Helper()
	h := c.Storage(id).History()
	saved := h[len(h)-1]
	got, live := c.Snapshot(id)
	if !live {
		t.Fatalf("%s is not running after Restart", id)
	}
	if got.Term != saved.CurrentTerm || got.VotedFor != saved.VotedFor || !sameLog(got.Log, saved.Log) {
		t.Errorf("%s restarted with term %d, vote %q, %d entries; want the saved %d, %q, %d entries",
			id, got.Term, got.VotedFor, len(got.Log), saved.CurrentTerm, saved.VotedFor, len(saved.Log))
	}
	if got.CommitIndex != 0 || got.LastApplied != 0 {
		t.Errorf("%s restarted with commitIndex %d, lastApplied %d; want 0, 0 (volatile)", id, got.CommitIndex, got.LastApplied)
	}
	if empty := lock.NewLockSM().Dump(); srv(t, c, id).Dump() != empty {
		t.Errorf("%s restarted with state %s, want an empty state machine until it replays", id, srv(t, c, id).Dump())
	}
}

// caughtUp reports whether every node is live, committed and applied as far as the leader, and
// holds the leader's state machine and dedup table.
func caughtUp(c *Cluster) func() bool {
	return func() bool {
		id, ok := c.Leader()
		if !ok {
			return false
		}
		l, _ := c.Snapshot(id)
		if l.CommitIndex == 0 || int(l.CommitIndex) != len(l.Log) {
			return false // not even the leader has committed everything it holds yet
		}
		want := c.Peer(id).(*lock.Server).Dump()
		for _, n := range three {
			s, live := c.Snapshot(n)
			if !live || s.CommitIndex != l.CommitIndex || s.LastApplied != l.CommitIndex {
				return false
			}
			if c.Peer(n).(*lock.Server).Dump() != want {
				return false
			}
		}
		return true
	}
}

// A follower crashes while entries are flowing to it and misses some; on restart it reloads its
// log, relearns the commit index from the leader, and replays everything into a fresh state machine.
func TestCrashRestartFollowerMidReplication(t *testing.T) {
	c := NewClusterWith(3, 1, lockServer)
	commit(t, c, "c1", 1, acq("a"))
	leader, _ := c.Leader()
	f := others3(leader)[0]

	call, msgs := srv(t, c, leader).Submit("c2", 1, acq("b"))
	c.Send(msgs)
	mustStep(t, c, 2) // the entry is on its way
	c.Crash(f)
	if err := c.RunUntil(call.Done, 1000); err != nil {
		t.Fatal(err)
	}
	commit(t, c, "c3", 1, acq("c")) // committed while f is down

	if err := c.Restart(f); err != nil {
		t.Fatal(err)
	}
	wantRestartedFromDisk(t, c, f)
	if err := c.RunUntil(caughtUp(c), 1000); err != nil {
		t.Fatalf("%s did not catch up after restarting: %v", f, err)
	}
}

// The leader commits and applies an entry, then crashes before any follower hears the new commit
// index. The entry is on a majority, so the new leader must keep it and commit it, and the client's
// answer stays true. The old leader then restarts and catches up.
func TestCrashRestartLeaderAfterCommitBeforeFollowersLearn(t *testing.T) {
	c := NewClusterWith(3, 1, lockServer)
	commit(t, c, "c1", 1, acq("a"))
	mustStep(t, c, 100)
	old, _ := c.Leader()

	call, msgs := srv(t, c, old).Submit("c2", 1, acq("jobs"))
	c.Send(msgs)
	for i := 0; ; i++ {
		if i == 500 {
			t.Fatal("the leader never committed the entry")
		}
		mustStep(t, c, 1)
		if s, _ := c.Snapshot(old); call.Done() && s.CommitIndex >= call.Result().Token {
			break
		}
	}
	idx := call.Result().Token
	for _, f := range others3(old) {
		if s, _ := c.Snapshot(f); s.CommitIndex >= idx {
			t.Fatalf("setup: %s already knows index %d is committed; the crash would come too late", f, idx)
		}
	}
	c.Crash(old)

	if err := c.RunUntil(func() bool { id, ok := c.Leader(); return ok && id != old }, 2000); err != nil {
		t.Fatal(err)
	}
	if err := c.RunUntil(func() bool {
		for _, f := range others3(old) {
			if s, _ := c.Snapshot(f); s.LastApplied < idx {
				return false
			}
		}
		return true
	}, 1000); err != nil {
		t.Fatalf("the survivors never applied the committed index %d: %v", idx, err)
	}
	status := commit(t, c, "r1", 1, lock.Command{Kind: lock.Status, Lock: "jobs"})
	if status.Holder != "c2" || status.Token != idx {
		t.Errorf("after the leader change, jobs = %+v; want c2's acknowledged grant at %d", status, idx)
	}

	if err := c.Restart(old); err != nil {
		t.Fatal(err)
	}
	wantRestartedFromDisk(t, c, old)
	if err := c.RunUntil(caughtUp(c), 1000); err != nil {
		t.Fatalf("the old leader did not catch up: %v", err)
	}
}

// Every node crashes at once and restarts. Nothing volatile survives anywhere, yet the cluster
// elects a leader, relearns what was committed, and rebuilds the same lock table and dedup table.
func TestCrashRestartWholeCluster(t *testing.T) {
	c := NewClusterWith(3, 1, lockServer)
	first := commit(t, c, "c1", 1, acq("a"))
	commit(t, c, "c2", 1, acq("b"))
	release := lock.Command{Kind: lock.Release, Lock: "a", Token: first.Token}
	released := commit(t, c, "c1", 2, release)
	if err := c.RunUntil(caughtUp(c), 500); err != nil {
		t.Fatal(err)
	}
	leader, _ := c.Leader()
	before := srv(t, c, leader).Dump()

	for _, id := range three {
		c.Crash(id)
	}
	for _, id := range three {
		if err := c.Restart(id); err != nil {
			t.Fatal(err)
		}
		wantRestartedFromDisk(t, c, id)
	}

	if err := c.RunUntil(caughtUp(c), 3000); err != nil {
		t.Fatalf("the cluster did not recover: %v", err)
	}
	now, _ := c.Leader()
	if got := srv(t, c, now).Dump(); got != before {
		t.Errorf("state after replay:\n%s\nwant the state before the crash:\n%s", got, before)
	}

	// The dedup table came back too: c1's release, retried, gets its original answer. Without the
	// table it would be applied again and refused, since the lock is no longer held.
	again := commit(t, c, "c1", 2, release)
	if again != released {
		t.Errorf("retried c1 seq 2 = %+v, want the original answer %+v from the rebuilt dedup table", again, released)
	}
	if got := srv(t, c, now).Dump(); got != before {
		t.Errorf("the retry changed the state:\n%s\nwant\n%s", got, before)
	}
}

func others3(id raft.NodeID) []raft.NodeID {
	var out []raft.NodeID
	for _, n := range three {
		if n != id {
			out = append(out, n)
		}
	}
	return out
}
