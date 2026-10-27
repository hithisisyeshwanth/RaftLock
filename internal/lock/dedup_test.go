package lock

import (
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// acquireCmd and releaseCmd build commands with an explicit seq, so a test can replay an old one as a retry would.
func acquireCmd(client string, seq uint64, lock string) Command {
	return Command{Kind: Acquire, Lock: lock, ClientID: client, Seq: seq}
}

func releaseCmd(client string, seq uint64, lock string, token raft.Index) Command {
	return Command{Kind: Release, Lock: lock, ClientID: client, Seq: seq, Token: token}
}

// Section 8: a client may send the same command again after a timeout, and both copies can end up
// in the log. The state machine applies each (clientID, seq) once and answers a repeat with the
// result it gave the first time.
func TestDedupRetryReturnsCachedResult(t *testing.T) {
	t.Run("granted acquire", func(t *testing.T) {
		sm := NewLockSM()
		first := apply(sm, 5, acquireCmd("c1", 1, "jobs"))
		before := sm.Dump()

		retry := apply(sm, 7, acquireCmd("c1", 1, "jobs"))

		if retry != first {
			t.Errorf("retry = %+v, want the cached first result %+v", retry, first)
		}
		if after := sm.Dump(); after != before {
			t.Errorf("the retry changed the state:\nbefore %s\nafter  %s", before, after)
		}
	})
	t.Run("release applied once, not refused as a double release", func(t *testing.T) {
		sm := NewLockSM()
		apply(sm, 5, acquireCmd("c1", 1, "jobs"))
		first := apply(sm, 6, releaseCmd("c1", 2, "jobs", 5))

		retry := apply(sm, 7, releaseCmd("c1", 2, "jobs", 5))

		if !first.OK || retry != first {
			t.Errorf("release then its retry = %+v then %+v, want the same successful result twice", first, retry)
		}
	})
	t.Run("denied acquire stays denied even after the lock frees", func(t *testing.T) {
		sm := NewLockSM()
		apply(sm, 5, acquireCmd("c1", 1, "jobs"))
		denied := apply(sm, 6, acquireCmd("c2", 1, "jobs"))
		apply(sm, 7, releaseCmd("c1", 2, "jobs", 5))
		before := sm.Dump()

		retry := apply(sm, 8, acquireCmd("c2", 1, "jobs")) // c2's timed-out request, delivered again

		if retry != denied {
			t.Errorf("retry = %+v, want the cached denial %+v (the command already ran once)", retry, denied)
		}
		if after := sm.Dump(); after != before {
			t.Errorf("the retry changed the state:\nbefore %s\nafter  %s", before, after)
		}
	})
}

// The dangerous case: c1 acquires (seq 1) and releases (seq 2), then a delayed copy of its acquire
// arrives. Applying it again would hand c1 a lock it believes it gave up, and nobody would release it.
func TestDedupDelayedAcquireAfterReleaseDoesNotReacquire(t *testing.T) {
	sm := NewLockSM()
	apply(sm, 5, acquireCmd("c1", 1, "jobs"))
	apply(sm, 6, releaseCmd("c1", 2, "jobs", 5))
	before := sm.Dump()

	r := apply(sm, 7, acquireCmd("c1", 1, "jobs"))

	if r.OK && r.Token == 7 {
		t.Errorf("delayed acquire = %+v: it granted the lock again at index 7", r)
	}
	if after := sm.Dump(); after != before {
		t.Errorf("the delayed acquire changed the state:\nbefore %s\nafter  %s", before, after)
	}
	if got := apply(sm, 8, acquireCmd("c2", 1, "jobs")); !got.OK || got.Token != 8 {
		t.Errorf("c2's acquire = %+v, want OK with token 8 (the lock must still be free)", got)
	}
}

// A seq below the client's latest is an old request the client has stopped waiting for: ignore it.
func TestDedupLowerSeqIgnored(t *testing.T) {
	sm := NewLockSM()
	apply(sm, 5, acquireCmd("c1", 1, "jobs"))
	apply(sm, 6, acquireCmd("c1", 5, "other")) // c1 is now at seq 5
	before := sm.Dump()

	r := apply(sm, 7, releaseCmd("c1", 3, "jobs", 5)) // seq 3: older than 5

	if r.OK {
		t.Errorf("release with an old seq = %+v, want it ignored, not applied", r)
	}
	if after := sm.Dump(); after != before {
		t.Errorf("an old seq changed the state:\nbefore %s\nafter  %s", before, after)
	}
}

// Each client has its own sequence numbers.
func TestDedupClientsAreIndependent(t *testing.T) {
	sm := NewLockSM()
	if r := apply(sm, 5, acquireCmd("c1", 1, "a")); !r.OK {
		t.Fatalf("c1 seq 1 = %+v", r)
	}
	if r := apply(sm, 6, acquireCmd("c2", 1, "b")); !r.OK || r.Token != 6 {
		t.Errorf("c2 seq 1 = %+v, want its own grant at 6, not c1's cached result", r)
	}
}

// The dedup table is replicated state: two replicas whose lock tables match but whose clients'
// histories differ are not in the same state, and Dump must show that.
func TestDedupTableIsPartOfDump(t *testing.T) {
	used := NewLockSM()
	apply(used, 5, acquireCmd("c1", 1, "jobs"))
	apply(used, 6, releaseCmd("c1", 2, "jobs", 5))
	fresh := NewLockSM()

	if used.Dump() == fresh.Dump() {
		t.Errorf("Dump is %s for both, but only one has seen c1's commands", used.Dump())
	}
}
