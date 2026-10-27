package lock

import (
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

func expireCmd(lock string, token raft.Index) Command {
	return Command{Kind: Expire, Lock: lock, Token: token}
}

// ---- The state machine side: Expire is an ordinary log entry. It carries the token it means to
// expire, and only that grant can be expired; the state machine never looks at a clock.

func TestExpiryFreesLockWhenTokenMatches(t *testing.T) {
	sm := NewLockSM()
	apply(sm, 5, acquireCmd("c1", 1, "jobs"))

	if r := apply(sm, 9, expireCmd("jobs", 5)); !r.OK {
		t.Fatalf("Expire{jobs, 5} for the current grant = %+v, want OK", r)
	}
	if r := apply(sm, 10, acquireCmd("c2", 1, "jobs")); !r.OK || r.Token != 10 {
		t.Errorf("acquire after expiry = %+v, want OK with token 10", r)
	}
}

// The leader proposed Expire for token 5, but before it committed, c1 released and c2 acquired
// (token 7). The late Expire is about a grant that no longer exists and must not take c2's lock.
func TestExpiryStaleTokenIgnored(t *testing.T) {
	sm := NewLockSM()
	apply(sm, 5, acquireCmd("c1", 1, "jobs"))
	apply(sm, 6, releaseCmd("c1", 2, "jobs", 5))
	apply(sm, 7, acquireCmd("c2", 1, "jobs"))
	before := lockTable(sm)

	if r := apply(sm, 8, expireCmd("jobs", 5)); r.OK {
		t.Errorf("stale Expire{jobs, 5} = %+v, want ignored", r)
	}
	if after := lockTable(sm); after != before {
		t.Errorf("a stale Expire changed the locks:\nbefore %s\nafter  %s", before, after)
	}
}

func TestExpiryOfFreeLockIgnored(t *testing.T) {
	sm := NewLockSM()
	if r := apply(sm, 3, expireCmd("jobs", 2)); r.OK {
		t.Errorf("Expire of a lock nobody holds = %+v, want ignored", r)
	}
	if got := lockTable(sm); got != "{}" {
		t.Errorf("locks = %s, want none", got)
	}
}

// Expire comes from the leader, not a client, so a second copy is not a client retry: it simply
// finds the grant already gone.
func TestExpiryDuplicateIsHarmless(t *testing.T) {
	sm := NewLockSM()
	apply(sm, 5, acquireCmd("c1", 1, "jobs"))
	apply(sm, 9, expireCmd("jobs", 5))
	apply(sm, 10, acquireCmd("c2", 1, "jobs"))

	if r := apply(sm, 11, expireCmd("jobs", 5)); r.OK {
		t.Errorf("second Expire{jobs, 5} = %+v, want ignored", r)
	}
	if r := apply(sm, 12, acquireCmd("c3", 1, "jobs")); r.OK {
		t.Errorf("c3 got the lock = %+v; the duplicate Expire must not have freed c2's grant", r)
	}
}

// ---- The leader side: Leases watches applied commands on every node, but only a leader's timers
// run, counted in ticks. When a lease runs out the leader returns an Expire to propose.

// grant applies an Acquire to sm and tells l about it, as the lock server does on every node.
func grant(t *testing.T, sm *LockSM, l *Leases, index raft.Index, client, lock string, ttl int) Result {
	t.Helper()
	c := Command{Kind: Acquire, Lock: lock, ClientID: client, Seq: 1, TTL: ttl}
	r := apply(sm, index, c)
	l.Applied(c, r)
	return r
}

// tickUntil ticks l up to max times and returns how many ticks passed before it
// returned something, and what.
func tickUntil(l *Leases, leader bool, max int) (int, []Command) {
	for i := 1; i <= max; i++ {
		if out := l.Tick(leader); len(out) > 0 {
			return i, out
		}
	}
	return 0, nil
}

func TestExpiryLeaderProposesAfterTTL(t *testing.T) {
	sm, l := NewLockSM(), NewLeases()
	l.Tick(true) // already leader
	grant(t, sm, l, 5, "c1", "jobs", 100)

	at, out := tickUntil(l, true, 1000)

	if at != 100 {
		t.Fatalf("Expire proposed after %d ticks, want 100 (the TTL)", at)
	}
	if len(out) != 1 || out[0] != expireCmd("jobs", 5) {
		t.Errorf("proposals = %+v, want exactly Expire{jobs, 5}", out)
	}
}

// Proposing is not applying: until the Expire commits, the leader must not flood the log with copies.
func TestExpiryProposedOnce(t *testing.T) {
	sm, l := NewLockSM(), NewLeases()
	l.Tick(true)
	grant(t, sm, l, 5, "c1", "jobs", 100)
	tickUntil(l, true, 1000)

	for i := 0; i < 50; i++ {
		if out := l.Tick(true); len(out) != 0 {
			t.Fatalf("tick %d after proposing: proposed %+v again", i+1, out)
		}
	}
}

func TestExpiryReleasedOrExpiredLockNotProposed(t *testing.T) {
	t.Run("released before its TTL", func(t *testing.T) {
		sm, l := NewLockSM(), NewLeases()
		l.Tick(true)
		grant(t, sm, l, 5, "c1", "jobs", 100)
		rel := releaseCmd("c1", 2, "jobs", 5)
		l.Applied(rel, apply(sm, 6, rel))

		if at, out := tickUntil(l, true, 500); at != 0 {
			t.Errorf("proposed %+v after %d ticks for a released lock, want nothing", out, at)
		}
	})
	t.Run("expired once already", func(t *testing.T) {
		sm, l := NewLockSM(), NewLeases()
		l.Tick(true)
		grant(t, sm, l, 5, "c1", "jobs", 100)
		_, out := tickUntil(l, true, 1000)
		l.Applied(out[0], apply(sm, 9, out[0]))

		if at, again := tickUntil(l, true, 500); at != 0 {
			t.Errorf("proposed %+v again after the Expire was applied", again)
		}
	})
	t.Run("denied acquire starts no lease", func(t *testing.T) {
		sm, l := NewLockSM(), NewLeases()
		l.Tick(true)
		grant(t, sm, l, 5, "c1", "jobs", 1000)
		grant(t, sm, l, 6, "c2", "jobs", 10) // denied: c1 holds it

		if at, out := tickUntil(l, true, 500); at != 0 {
			t.Errorf("proposed %+v after %d ticks; the denied acquire's short TTL must not count", out, at)
		}
	})
}

// A lock taken without a TTL has no lease and is never expired.
func TestExpiryNoTTLNeverExpires(t *testing.T) {
	sm, l := NewLockSM(), NewLeases()
	l.Tick(true)
	grant(t, sm, l, 5, "c1", "jobs", 0)

	if at, out := tickUntil(l, true, 5000); at != 0 {
		t.Errorf("proposed %+v after %d ticks for a lock with no TTL", out, at)
	}
}

// Followers track leases too, so a new leader knows every lock, but their timers do not run:
// only the leader decides expiry.
func TestExpiryFollowerNeverProposes(t *testing.T) {
	sm, l := NewLockSM(), NewLeases()
	grant(t, sm, l, 5, "c1", "jobs", 100)

	if at, out := tickUntil(l, false, 1000); at != 0 {
		t.Errorf("a follower proposed %+v after %d ticks", out, at)
	}
}

// A new leader cannot know how long its predecessor had been counting, so it restarts every lease
// from its own election. That can only give holders more time than they were promised, never less.
func TestExpiryNewLeaderRestartsTimers(t *testing.T) {
	t.Run("follower becomes leader", func(t *testing.T) {
		sm, l := NewLockSM(), NewLeases()
		grant(t, sm, l, 5, "c1", "jobs", 100)
		for i := 0; i < 80; i++ {
			l.Tick(false) // 80 ticks pass under the old leader
		}

		at, out := tickUntil(l, true, 1000)

		if at != 100 {
			t.Errorf("new leader proposed Expire %d ticks after its election, want 100 (the full TTL again)", at)
		}
		if len(out) != 1 || out[0] != expireCmd("jobs", 5) {
			t.Errorf("proposals = %+v, want Expire{jobs, 5}", out)
		}
	})
	t.Run("leader loses and regains leadership", func(t *testing.T) {
		sm, l := NewLockSM(), NewLeases()
		l.Tick(true)
		grant(t, sm, l, 5, "c1", "jobs", 100)
		for i := 0; i < 90; i++ {
			l.Tick(true)
		}
		l.Tick(false) // deposed

		at, _ := tickUntil(l, true, 1000)

		if at != 100 {
			t.Errorf("re-elected leader proposed Expire %d ticks later, want 100", at)
		}
	})
	t.Run("a pending proposal is made again by the next leader", func(t *testing.T) {
		sm, l := NewLockSM(), NewLeases()
		l.Tick(true)
		grant(t, sm, l, 5, "c1", "jobs", 100)
		tickUntil(l, true, 1000) // proposed, but the leader is deposed before it commits
		l.Tick(false)

		if at, out := tickUntil(l, true, 1000); at != 100 || len(out) != 1 {
			t.Errorf("next leader proposed %+v after %d ticks, want Expire again after 100", out, at)
		}
	})
}
