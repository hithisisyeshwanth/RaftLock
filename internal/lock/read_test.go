package lock_test

import (
	"errors"
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/lock"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

func status(lockName string) lock.Command { return lock.Command{Kind: lock.Status, Lock: lockName} }

func release(lockName string, token raft.Index) lock.Command {
	return lock.Command{Kind: lock.Release, Lock: lockName, Token: token}
}

func encode(t *testing.T, c lock.Command) []byte {
	t.Helper()
	b, err := lock.Encode(c)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestReadThroughLogStatusRoundTrips(t *testing.T) {
	want := lock.Command{Kind: lock.Status, Lock: "jobs", ClientID: "c1", Seq: 4}
	got, err := lock.Decode(encode(t, want))
	if err != nil || got != want {
		t.Errorf("round trip = %+v, %v; want %+v", got, err, want)
	}
}

// Status reports the holder and fencing token, and applying it changes nothing.
func TestReadThroughLogStatusAppliesWithoutChangingState(t *testing.T) {
	sm := lock.NewLockSM()
	sm.Apply(5, encode(t, lock.Command{Kind: lock.Acquire, Lock: "jobs", ClientID: "c1", Seq: 1}))
	before := sm.Dump()

	held := sm.Apply(6, encode(t, status("jobs")))
	free := sm.Apply(7, encode(t, status("other")))

	if !held.OK || held.Holder != "c1" || held.Token != 5 {
		t.Errorf("status of a held lock = %+v, want OK, holder c1, token 5", held)
	}
	if !free.OK || free.Holder != "" || free.Token != 0 {
		t.Errorf("status of a free lock = %+v, want OK with no holder and no token", free)
	}
	if after := sm.Dump(); after != before {
		t.Errorf("status changed the state:\nbefore %s\nafter  %s", before, after)
	}
}

// A read is a log entry like any write: it costs one entry and is answered only once applied.
func TestReadThroughLogUsesTheLog(t *testing.T) {
	c, leader := lockCluster(t, 1)
	before, _ := c.Snapshot(leader)

	call := submit(c, server(t, c, leader), "r1", 1, status("jobs"))
	if call.Done() {
		t.Fatal("status was answered before going through the log")
	}
	waitDone(t, c, call, 500)

	after, _ := c.Snapshot(leader)
	if len(after.Log) != len(before.Log)+1 {
		t.Errorf("log grew by %d entries for one read, want 1", len(after.Log)-len(before.Log))
	}
}

// Linearizable: once a write has completed, any later read sees it, whichever client reads.
func TestReadThroughLogSeesCompletedWrites(t *testing.T) {
	c, _ := lockCluster(t, 1)
	writer := lock.NewClerk("c1", three, &simTransport{c: c})
	reader := lock.NewClerk("r1", three, &simTransport{c: c})

	got := writer.Do(acquire("jobs", 0))
	drive(t, c, writer, got, 500)
	read := reader.Do(status("jobs"))
	drive(t, c, reader, read, 500)
	if !read.Result().OK || read.Result().Holder != "c1" || read.Result().Token != got.Result().Token {
		t.Fatalf("read after acquire = %+v, want holder c1 with token %d", read.Result(), got.Result().Token)
	}

	rel := writer.Do(release("jobs", got.Result().Token))
	drive(t, c, writer, rel, 500)
	read = reader.Do(status("jobs"))
	drive(t, c, reader, read, 500)
	if !read.Result().OK || read.Result().Holder != "" {
		t.Errorf("read after release = %+v, want the lock free", read.Result())
	}
}

// The old leader is cut off but still thinks it leads. The majority side elects a new leader and
// the lock changes hands there. A read sent to the old leader must not answer from its stale
// state: its entry can never commit, so the read fails instead of returning c1 as the holder.
func TestReadThroughLogDeposedLeaderCannotAnswer(t *testing.T) {
	c, old := lockCluster(t, 1)
	first := submit(c, server(t, c, old), "c1", 1, acquire("jobs", 0))
	waitDone(t, c, first, 500)
	run(t, c, 60) // followers learn the commit

	c.Partition([]raft.NodeID{old}, others(old))
	if err := c.RunUntil(func() bool { id, ok := c.Leader(); return ok && id != old }, 1000); err != nil {
		t.Fatal(err)
	}
	now, _ := c.Leader()
	rel := submit(c, server(t, c, now), "c1", 2, release("jobs", first.Result().Token))
	waitDone(t, c, rel, 500)
	taken := submit(c, server(t, c, now), "c2", 1, acquire("jobs", 0))
	waitDone(t, c, taken, 500)
	if !rel.Result().OK || !taken.Result().OK {
		t.Fatalf("on the new leader: release %+v, acquire %+v; want both to succeed", rel.Result(), taken.Result())
	}

	s := server(t, c, old)
	s.Timeout = 300
	stale := submit(c, s, "r1", 1, status("jobs"))
	waitDone(t, c, stale, 1000)
	if stale.Err() == nil {
		t.Fatalf("the cut-off old leader answered a read with %+v; it must not answer from stale state", stale.Result())
	}
	if !errors.Is(stale.Err(), lock.ErrTimeout) && !errors.Is(stale.Err(), lock.ErrLeaderChanged) {
		t.Errorf("stale read failed with %v, want ErrTimeout or ErrLeaderChanged", stale.Err())
	}

	c.Heal()
	read := submit(c, server(t, c, now), "r1", 2, status("jobs"))
	waitDone(t, c, read, 500)
	if read.Result().Holder != "c2" || read.Result().Token != taken.Result().Token {
		t.Errorf("read on the new leader after healing = %+v, want holder c2 with token %d", read.Result(), taken.Result().Token)
	}
}
