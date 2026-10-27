package sim

import (
	"fmt"
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/lock"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

func lockServer(cfg raft.Config, id raft.NodeID, peers []raft.NodeID, st raft.Storage, tr raft.Tracer, to raft.TimeoutSource, onApply func(raft.Entry)) (Peer, error) {
	s, err := lock.NewServer(cfg, id, peers, st, tr, to, onApply)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// clerkTransport delivers clerk requests straight to servers, bypassing the simulated network.
type clerkTransport struct{ c *Cluster }

func (tr clerkTransport) Submit(server raft.NodeID, client string, seq uint64, cmd lock.Command) (*lock.Call, error) {
	s, ok := tr.c.Peer(server).(*lock.Server)
	if !ok {
		return nil, fmt.Errorf("%s is down", server)
	}
	call, msgs := s.Submit(client, seq, cmd)
	tr.c.Send(msgs)
	return call, nil
}

// resource is what the lock protects, e.g. a storage service. It remembers the highest fencing
// token it has seen and refuses anything older (Kleppmann, "How to do distributed locking").
type resource struct {
	highest  raft.Index
	accepted []write
}

type write struct {
	client string
	token  raft.Index
}

func (r *resource) Write(client string, token raft.Index) bool {
	if token < r.highest {
		return false
	}
	r.highest = token
	r.accepted = append(r.accepted, write{client, token})
	return true
}

// contender acquires the lock, uses the resource for a while, releases, and starts again.
type contender struct {
	id      string
	ck      *lock.Clerk
	call    *lock.Call
	holding bool
	token   raft.Index
	heldFor int
}

// The spec's scenario: three clients fight over one lock while 10 percent of messages are dropped
// and the leader crashes and later restarts. Every grant must go to one client only, grants must
// carry ever larger tokens, and the resource must only ever see tokens that never go backwards.
func TestMutualExclusionUnderFaults(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		t.Run(fmt.Sprintf("seed %d", seed), func(t *testing.T) {
			c := NewClusterWith(3, seed, lockServer)
			if err := c.RunUntil(hasLeader(c), 1000); err != nil {
				t.Fatal(err)
			}
			c.SetDrop(0.1)
			res := &resource{}
			var cs []*contender
			for i := 1; i <= 3; i++ {
				ck := lock.NewClerk(fmt.Sprintf("c%d", i), three, clerkTransport{c})
				ck.AttemptTimeout = 400
				cs = append(cs, &contender{id: fmt.Sprintf("c%d", i), ck: ck})
			}

			holderOf := map[raft.Index]string{} // every grant any client was told about
			var grants []raft.Index             // in the order clients learned of them
			var crashed raft.NodeID
			for tick := 1; tick <= 4000; tick++ {
				switch tick {
				case 800:
					crashed, _ = c.Leader()
					c.Crash(crashed)
				case 1400:
					if err := c.Restart(crashed); err != nil {
						t.Fatal(err)
					}
				}
				for _, ct := range cs {
					ct.ck.Tick()
					switch {
					case ct.call == nil:
						ct.call = ct.ck.Do(lock.Command{Kind: lock.Acquire, Lock: "jobs", TTL: 300})
					case !ct.call.Done():
					case !ct.holding && ct.call.Result().OK:
						tok := ct.call.Result().Token
						if prev, ok := holderOf[tok]; ok && prev != ct.id {
							t.Fatalf("token %d was granted to both %s and %s", tok, prev, ct.id)
						}
						holderOf[tok] = ct.id
						grants = append(grants, tok)
						ct.holding, ct.token, ct.heldFor = true, tok, 0
						ct.call = &lock.Call{} // never done: we are holding, not waiting
					case !ct.holding: // denied: try again
						ct.call = nil
					default: // the release finished
						ct.holding, ct.call = false, nil
					}
					if ct.holding && !ct.call.Done() && ct.heldFor < 40 {
						ct.heldFor++
						if ct.heldFor%10 == 0 {
							res.Write(ct.id, ct.token)
						}
						if ct.heldFor == 40 {
							ct.call = ct.ck.Do(lock.Command{Kind: lock.Release, Lock: "jobs", Token: ct.token})
						}
					}
				}
				if err := c.Step(); err != nil {
					t.Fatal(err)
				}
			}

			if len(grants) < 5 {
				t.Fatalf("only %d grants in 4000 ticks; the run shows too little", len(grants))
			}

			// Mutual exclusion, checked against the committed log rather than the state machine:
			// before the lock goes to a different client, the previous grant must have been
			// released by its holder or expired, with its own token.
			c.SetDrop(0)
			mustStep(t, c, 300)
			leader, _ := c.Leader()
			snap, _ := c.Snapshot(leader)
			var curClient string
			var curToken raft.Index
			for _, e := range snap.Log[:snap.CommitIndex] {
				cmd, err := lock.Decode(e.Cmd)
				if e.Noop || err != nil {
					continue
				}
				if client, granted := holderOf[e.Index]; granted {
					if curClient != "" && curClient != client {
						t.Errorf("index %d granted the lock to %s while %s still held token %d", e.Index, client, curClient, curToken)
					}
					curClient, curToken = client, e.Index
				}
				ends := (cmd.Kind == lock.Release && cmd.ClientID == curClient) || cmd.Kind == lock.Expire
				if ends && cmd.Token == curToken {
					curClient, curToken = "", 0
				}
			}
			seen := map[raft.Index]bool{}
			for i, tok := range grants {
				if seen[tok] {
					continue // the same grant reported again to a retrying client
				}
				seen[tok] = true
				for _, earlier := range grants[:i] {
					if !seen[earlier] {
						continue
					}
					if earlier > tok && holderOf[earlier] != holderOf[tok] {
						t.Errorf("a new grant %d to %s came after grant %d to %s: tokens must increase with each grant", tok, holderOf[tok], earlier, holderOf[earlier])
					}
				}
			}
			for i := 1; i < len(res.accepted); i++ {
				if res.accepted[i].token < res.accepted[i-1].token {
					t.Errorf("the resource accepted token %d after %d", res.accepted[i].token, res.accepted[i-1].token)
				}
			}
		})
	}
}

// Fencing in one deterministic story. c1 takes the lock with a 100-tick TTL, writes, and then
// stalls (think of a long GC pause) without releasing. The leader expires its grant through the
// log, c2 takes the lock with a higher token and writes. When c1 wakes up it still believes it
// holds the lock; the lock service cannot stop it, but the resource refuses its old token.
func TestMutualExclusionFencingRejectsStaleHolder(t *testing.T) {
	c := NewClusterWith(3, 1, lockServer)
	if err := c.RunUntil(hasLeader(c), 1000); err != nil {
		t.Fatal(err)
	}
	res := &resource{}
	a := lock.NewClerk("c1", three, clerkTransport{c})
	b := lock.NewClerk("c2", three, clerkTransport{c})
	drive := func(ck *lock.Clerk, call *lock.Call) lock.Result {
		for i := 0; !call.Done(); i++ {
			if i == 2000 {
				t.Fatal("request not answered")
			}
			ck.Tick()
			if err := c.Step(); err != nil {
				t.Fatal(err)
			}
		}
		return call.Result()
	}

	ga := drive(a, a.Do(lock.Command{Kind: lock.Acquire, Lock: "jobs", TTL: 100}))
	if !ga.OK || !res.Write("c1", ga.Token) {
		t.Fatalf("c1: grant %+v, first write refused", ga)
	}

	// c1 stalls. c2 keeps asking until the expiry frees the lock.
	var gb lock.Result
	for i := 0; ; i++ {
		if i == 20 {
			t.Fatal("c2 never got the lock; c1's grant was not expired")
		}
		gb = drive(b, b.Do(lock.Command{Kind: lock.Acquire, Lock: "jobs"}))
		if gb.OK {
			break
		}
		for k := 0; k < 50; k++ {
			if err := c.Step(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if gb.Token <= ga.Token {
		t.Fatalf("c2's token %d is not above c1's %d", gb.Token, ga.Token)
	}
	if !res.Write("c2", gb.Token) {
		t.Fatalf("the resource refused c2's current token %d", gb.Token)
	}

	if res.Write("c1", ga.Token) {
		t.Errorf("the resource accepted c1's stale token %d after c2's %d", ga.Token, gb.Token)
	}
	if got := res.highest; got != gb.Token {
		t.Errorf("resource's highest token = %d, want c2's %d", got, gb.Token)
	}
}
