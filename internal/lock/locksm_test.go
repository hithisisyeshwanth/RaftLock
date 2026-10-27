package lock

import (
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

var _ StateMachine = (*LockSM)(nil)

// Every client numbers its commands 1, 2, 3, ... so these tests never look like retries to the
// deduplication T5.3 adds.
type client struct {
	id  string
	seq uint64
}

func (c *client) acquire(sm *LockSM, index raft.Index, lock string) Result {
	c.seq++
	return apply(sm, index, Command{Kind: Acquire, Lock: lock, ClientID: c.id, Seq: c.seq})
}

func (c *client) release(sm *LockSM, index raft.Index, lock string, token raft.Index) Result {
	c.seq++
	return apply(sm, index, Command{Kind: Release, Lock: lock, ClientID: c.id, Seq: c.seq, Token: token})
}

func apply(sm *LockSM, index raft.Index, cmd Command) Result {
	b, err := Encode(cmd)
	if err != nil {
		panic(err)
	}
	return sm.Apply(index, b)
}

func TestLockSMAcquireFreeGrantsWithIndexAsToken(t *testing.T) {
	sm := NewLockSM()
	c1 := &client{id: "c1"}

	r := c1.acquire(sm, 5, "jobs")

	if !r.OK || r.Token != 5 {
		t.Errorf("acquire of a free lock at index 5 = %+v, want OK with token 5", r)
	}
}

// No wait queue: a held lock is simply refused, naming who holds it, and the client retries.
func TestLockSMAcquireHeldByAnotherIsDenied(t *testing.T) {
	sm := NewLockSM()
	c1, c2 := &client{id: "c1"}, &client{id: "c2"}
	c1.acquire(sm, 5, "jobs")

	r := c2.acquire(sm, 6, "jobs")

	if r.OK || r.Holder != "c1" {
		t.Errorf("acquire of a lock c1 holds = %+v, want denied naming c1", r)
	}
	if again := c1.acquire(sm, 7, "jobs"); !again.OK || again.Token != 5 {
		t.Errorf("after the denial c1 still holds it with token 5? got %+v", again)
	}
}

// Asking again for a lock you already hold succeeds and keeps the original token.
func TestLockSMReacquireByHolderKeepsToken(t *testing.T) {
	sm := NewLockSM()
	c1 := &client{id: "c1"}
	c1.acquire(sm, 5, "jobs")

	r := c1.acquire(sm, 8, "jobs")

	if !r.OK || r.Token != 5 {
		t.Errorf("re-acquire by the holder at index 8 = %+v, want OK with the original token 5", r)
	}
}

func TestLockSMReleaseByHolderFreesLock(t *testing.T) {
	sm := NewLockSM()
	c1, c2 := &client{id: "c1"}, &client{id: "c2"}
	c1.acquire(sm, 5, "jobs")

	if r := c1.release(sm, 6, "jobs", 5); !r.OK {
		t.Fatalf("release by the holder with its token = %+v, want OK", r)
	}
	if r := c2.acquire(sm, 7, "jobs"); !r.OK || r.Token != 7 {
		t.Errorf("acquire after release = %+v, want OK with token 7", r)
	}
}

func TestLockSMReleaseRejected(t *testing.T) {
	tests := []struct {
		name  string
		setup func(sm *LockSM, c1, c2 *client)
		by    string // "c1" or "c2"
		token raft.Index
	}{
		{"by a client that does not hold it", func(sm *LockSM, c1, c2 *client) { c1.acquire(sm, 5, "jobs") }, "c2", 5},
		{"by the holder with a wrong token", func(sm *LockSM, c1, c2 *client) { c1.acquire(sm, 5, "jobs") }, "c1", 4},
		{"twice by the holder", func(sm *LockSM, c1, c2 *client) {
			c1.acquire(sm, 5, "jobs")
			c1.release(sm, 6, "jobs", 5)
		}, "c1", 5},
		{"of a lock never acquired", func(sm *LockSM, c1, c2 *client) {}, "c1", 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := NewLockSM()
			c1, c2 := &client{id: "c1"}, &client{id: "c2"}
			tt.setup(sm, c1, c2)
			before := sm.Dump()

			by := map[string]*client{"c1": c1, "c2": c2}[tt.by]
			if r := by.release(sm, 9, "jobs", tt.token); r.OK {
				t.Errorf("release = %+v, want rejected", r)
			}
			if after := sm.Dump(); after != before {
				t.Errorf("a rejected release changed the lock table:\nbefore %s\nafter  %s", before, after)
			}
		})
	}
}

// Fencing tokens are log indexes, so every new grant carries a larger token than any before it,
// on any lock: a resource that remembers the highest token it has seen can refuse a stale holder.
func TestLockSMTokensStrictlyIncreaseAcrossGrants(t *testing.T) {
	sm := NewLockSM()
	c1, c2 := &client{id: "c1"}, &client{id: "c2"}
	var tokens []raft.Index
	grant := func(r Result) {
		if !r.OK {
			t.Fatalf("expected a grant, got %+v", r)
		}
		tokens = append(tokens, r.Token)
	}

	grant(c1.acquire(sm, 3, "jobs"))
	grant(c2.acquire(sm, 4, "queue")) // a different lock is independent
	c1.release(sm, 6, "jobs", 3)
	grant(c2.acquire(sm, 9, "jobs"))
	c2.release(sm, 10, "jobs", 9)
	grant(c1.acquire(sm, 12, "jobs"))

	for i := 1; i < len(tokens); i++ {
		if tokens[i] <= tokens[i-1] {
			t.Errorf("grant tokens %v do not strictly increase", tokens)
		}
	}
}

// Applying the same commands at the same indexes to two fresh state machines gives the same
// state, so replicas never diverge; the dump must show it, whatever order maps iterate in.
func TestLockSMIdenticalSequencesGiveIdenticalDumps(t *testing.T) {
	run := func() *LockSM {
		sm := NewLockSM()
		c1, c2, c3 := &client{id: "c1"}, &client{id: "c2"}, &client{id: "c3"}
		c1.acquire(sm, 1, "a")
		c2.acquire(sm, 2, "b")
		c3.acquire(sm, 3, "c")
		c2.acquire(sm, 4, "a") // denied
		c1.release(sm, 5, "a", 1)
		c3.acquire(sm, 6, "a")
		c2.release(sm, 7, "b", 99) // rejected
		return sm
	}
	a, b := run().Dump(), run().Dump()
	if a != b {
		t.Errorf("same sequence, different dumps:\n%s\n%s", a, b)
	}

	other := NewLockSM()
	(&client{id: "c1"}).acquire(other, 1, "a")
	if other.Dump() == a {
		t.Error("a different sequence gave the same dump; Dump does not reflect the lock table")
	}
}

// From T5.1: a command that does not decode yields an error result, never a panic, and changes nothing.
func TestLockSMBadCommandIsAnErrorResult(t *testing.T) {
	sm := NewLockSM()
	(&client{id: "c1"}).acquire(sm, 1, "jobs")
	before := sm.Dump()

	for _, b := range [][]byte{[]byte(`{"Kind":"teleport","Lock":"jobs"}`), []byte("garbage"), nil} {
		func() {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("Apply(%q) panicked: %v", b, p)
				}
			}()
			if r := sm.Apply(2, b); r.OK || r.Err == "" {
				t.Errorf("Apply(%q) = %+v, want an error result", b, r)
			}
		}()
	}
	if after := sm.Dump(); after != before {
		t.Errorf("a bad command changed the lock table:\nbefore %s\nafter  %s", before, after)
	}
}
