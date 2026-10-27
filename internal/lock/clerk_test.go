package lock_test

import (
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/lock"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
	"github.com/hithisisyeshwanth/RaftLock/internal/sim"
)

// attempt is one request the clerk sent.
type attempt struct {
	tick   int
	server raft.NodeID
	client string
	seq    uint64
}

// simTransport delivers a clerk's requests straight to servers in the harness and records them.
// Client traffic does not go through the simulated network, so a partitioned leader is still
// reachable by clients, as a real one behind a partial network failure would be.
type simTransport struct {
	c        *sim.Cluster
	attempts []attempt
}

func (tr *simTransport) Submit(server raft.NodeID, client string, seq uint64, cmd lock.Command) (*lock.Call, error) {
	tr.attempts = append(tr.attempts, attempt{tr.c.Now(), server, client, seq})
	s, ok := tr.c.Peer(server).(*lock.Server)
	if !ok {
		return nil, errUnreachable
	}
	call, msgs := s.Submit(client, seq, cmd)
	tr.c.Send(msgs)
	return call, nil
}

type unreachable struct{}

func (unreachable) Error() string { return "server unreachable" }

var errUnreachable error = unreachable{}

// drive ticks the clerk and the cluster together until call is done.
func drive(t *testing.T, c *sim.Cluster, ck *lock.Clerk, call *lock.Call, max int) {
	t.Helper()
	for i := 0; !call.Done(); i++ {
		if i == max {
			t.Fatalf("clerk request not done after %d ticks", max)
		}
		ck.Tick()
		if err := c.Step(); err != nil {
			t.Fatal(err)
		}
	}
}

func others(leader raft.NodeID) []raft.NodeID {
	var out []raft.NodeID
	for _, id := range three {
		if id != leader {
			out = append(out, id)
		}
	}
	return out
}

// The clerk remembers who answered last, so its next request goes straight there.
func TestClerkCachesLeader(t *testing.T) {
	c, leader := lockCluster(t, 1)
	tr := &simTransport{c: c}
	ck := lock.NewClerk("c1", three, tr)

	drive(t, c, ck, ck.Do(acquire("a", 0)), 500)
	if ck.Leader() != leader {
		t.Fatalf("Leader() = %q after a success, want %s", ck.Leader(), leader)
	}
	tr.attempts = nil
	drive(t, c, ck, ck.Do(acquire("b", 0)), 500)

	if len(tr.attempts) != 1 || tr.attempts[0].server != leader {
		t.Errorf("second request attempts = %+v, want exactly one, sent to the cached leader %s", tr.attempts, leader)
	}
}

// A follower's hint is followed directly, instead of walking the server list.
func TestClerkFollowsHint(t *testing.T) {
	c, leader := lockCluster(t, 1)
	f := others(leader)
	tr := &simTransport{c: c}
	ck := lock.NewClerk("c1", []raft.NodeID{f[0], f[1], leader}, tr) // round robin would try f[1] next

	drive(t, c, ck, ck.Do(acquire("a", 0)), 500)

	if len(tr.attempts) != 2 || tr.attempts[0].server != f[0] || tr.attempts[1].server != leader {
		t.Errorf("attempts = %+v, want %s then the hinted leader %s", tr.attempts, f[0], leader)
	}
}

// With the cached leader gone and no hint, the clerk tries the next server in its list, and
// keeps going until one of them leads.
func TestClerkFallsBackRoundRobin(t *testing.T) {
	c, leader := lockCluster(t, 1)
	tr := &simTransport{c: c}
	ck := lock.NewClerk("c1", three, tr)
	drive(t, c, ck, ck.Do(acquire("a", 0)), 500)
	c.Crash(leader)
	tr.attempts = nil

	drive(t, c, ck, ck.Do(acquire("b", 0)), 3000)

	if len(tr.attempts) < 2 || tr.attempts[0].server != leader {
		t.Fatalf("attempts = %+v, want the cached leader first, then others", tr.attempts)
	}
	next := three[0]
	for i, id := range three {
		if id == leader {
			next = three[(i+1)%len(three)]
		}
	}
	if tr.attempts[1].server != next {
		t.Errorf("after %s failed the clerk tried %s, want %s, the next in its list", leader, tr.attempts[1].server, next)
	}
	if got, _ := c.Leader(); ck.Leader() != got {
		t.Errorf("Leader() = %q, want the new leader %s", ck.Leader(), got)
	}
}

// With nothing reachable, retries slow down instead of hammering the servers every tick.
func TestClerkBacksOff(t *testing.T) {
	c, _ := lockCluster(t, 1)
	for _, id := range three {
		c.Crash(id)
	}
	tr := &simTransport{c: c}
	ck := lock.NewClerk("c1", three, tr)
	ck.Do(acquire("a", 0))
	for i := 0; i < 2000; i++ {
		ck.Tick()
		if err := c.Step(); err != nil {
			t.Fatal(err)
		}
	}

	if n := len(tr.attempts); n < 3 || n > 50 {
		t.Fatalf("%d attempts in 2000 ticks with every server down, want a few retries, not one per tick", n)
	}
	var gaps []int
	for i := 1; i < len(tr.attempts); i++ {
		gaps = append(gaps, tr.attempts[i].tick-tr.attempts[i-1].tick)
	}
	for i := 1; i < len(gaps); i++ {
		if gaps[i] < gaps[i-1] {
			t.Errorf("gaps between attempts %v shrink at %d; backoff must not decrease", gaps, i)
		}
	}
	if gaps[len(gaps)-1] <= gaps[0] {
		t.Errorf("gaps between attempts %v never grow", gaps)
	}
}

// Every retry of one request carries the same (clientID, seq), so the state machine can recognise
// it; seq moves on only once a request has succeeded, however many tries that took.
func TestClerkStableSeqAcrossRetries(t *testing.T) {
	c, leader := lockCluster(t, 1)
	tr := &simTransport{c: c}
	ck := lock.NewClerk("c1", three, tr)

	var perRequest [][]attempt
	do := func(cmd lock.Command, max int) {
		from := len(tr.attempts)
		drive(t, c, ck, ck.Do(cmd), max)
		perRequest = append(perRequest, tr.attempts[from:])
	}
	do(acquire("a", 0), 500)
	c.Crash(leader) // forces retries for the next request
	do(acquire("b", 0), 3000)
	do(acquire("c", 0), 500)

	for i, tries := range perRequest {
		for _, a := range tries {
			if a.client != "c1" || a.seq != uint64(i+1) {
				t.Errorf("request %d sent (%q, seq %d), want (c1, seq %d) on every try", i+1, a.client, a.seq, i+1)
			}
		}
	}
	if len(perRequest[1]) < 2 {
		t.Errorf("request 2 took %d tries, want retries after its leader crashed", len(perRequest[1]))
	}
}

// The leader accepts the request and is then lost. The clerk retries elsewhere with the same seq;
// whether or not the first copy committed, the lock is granted once.
func TestClerkLeaderChangeMidRequestHasOneEffect(t *testing.T) {
	for _, wait := range []int{0, 3, 30} {
		c, leader := lockCluster(t, 1)
		tr := &simTransport{c: c}
		ck := lock.NewClerk("c1", three, tr)
		ck.AttemptTimeout = 300
		drive(t, c, ck, ck.Do(acquire("warmup", 0)), 500) // caches the leader

		call := ck.Do(acquire("jobs", 0))
		for i := 0; i < wait; i++ {
			ck.Tick()
			if err := c.Step(); err != nil {
				t.Fatal(err)
			}
		}
		c.Crash(leader)
		drive(t, c, ck, call, 5000)

		if call.Err() != nil || !call.Result().OK {
			t.Fatalf("wait %d: request = %+v, %v; want a grant", wait, call.Result(), call.Err())
		}
		newLeader, _ := c.Leader()
		snap, _ := c.Snapshot(newLeader)
		var first raft.Index
		for _, e := range snap.Log {
			if cmd, err := lock.Decode(e.Cmd); err == nil && cmd.ClientID == "c1" && cmd.Lock == "jobs" {
				first = e.Index
				break
			}
		}
		if call.Result().Token != first {
			t.Errorf("wait %d: token = %d, want %d, the first committed copy", wait, call.Result().Token, first)
		}
	}
}

// After a partition heals, a duplicate of an already-answered request reaches the leader again.
// It is answered from the dedup table and changes nothing.
func TestClerkDuplicateAfterHealIsDeduplicated(t *testing.T) {
	c, leader := lockCluster(t, 1)
	tr := &simTransport{c: c}
	ck := lock.NewClerk("c1", three, tr)
	ck.AttemptTimeout = 300
	drive(t, c, ck, ck.Do(acquire("warmup", 0)), 500)

	c.Partition([]raft.NodeID{leader}, others(leader))
	call := ck.Do(acquire("jobs", 0))
	drive(t, c, ck, call, 5000) // times out on the cut-off leader, then succeeds on the majority side
	c.Heal()
	for i := 0; i < 300; i++ {
		if err := c.Step(); err != nil {
			t.Fatal(err)
		}
	}
	now, _ := c.Leader()
	before := server(t, c, now).Dump()

	last := tr.attempts[len(tr.attempts)-1]
	dup := submit(c, server(t, c, now), last.client, last.seq, acquire("jobs", 0))
	waitDone(t, c, dup, 500)

	if dup.Result() != call.Result() {
		t.Errorf("duplicate = %+v, want the original answer %+v", dup.Result(), call.Result())
	}
	if after := server(t, c, now).Dump(); after != before {
		t.Errorf("the duplicate changed the state:\nbefore %s\nafter  %s", before, after)
	}
	for _, id := range three {
		if server(t, c, id).Dump() != server(t, c, now).Dump() {
			t.Errorf("after healing, %s state differs from the leader's", id)
		}
	}
}
