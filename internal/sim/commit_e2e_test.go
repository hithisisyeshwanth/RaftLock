package sim

import (
	"fmt"
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// proposal is a command a leader accepted at (index, term). It counts as acknowledged once that
// same leader reports it committed: commitIndex >= index with its own entry there still from term.
type proposal struct {
	leader raft.NodeID
	index  raft.Index
	term   raft.Term
	cmd    string
}

func ackedNow(c *Cluster, p proposal) bool {
	s, live := c.Snapshot(p.leader)
	return live && s.CommitIndex >= p.index && int(p.index) <= len(s.Log) && s.Log[p.index-1].Term == p.term
}

// sameApplied reports whether all three nodes are live and have applied identical, non-empty sequences.
func sameApplied(c *Cluster) bool {
	first := c.applied["n1"]
	if len(first) == 0 {
		return false
	}
	for _, id := range three {
		if _, live := c.Snapshot(id); !live || !sameLog(c.applied[id], first) {
			return false
		}
	}
	return true
}

// End to end on three nodes: a stream of 30 commands with 20 percent of messages dropped and the
// leader crashed halfway, then restarted. Every node must end up applying the same sequence, every
// command a leader acknowledged as committed must be in it at its index, and the checkers
// (run by the harness on every tick) must never fire.
func TestCommitEndToEnd(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c, _ := electedCluster(t, seed)
		c.SetDrop(0.2)

		var pending, acked []proposal
		step := func() {
			mustStep(t, c, 1)
			kept := pending[:0]
			for _, p := range pending {
				if ackedNow(c, p) {
					acked = append(acked, p)
				} else {
					kept = append(kept, p)
				}
			}
			pending = kept
		}

		var crashed raft.NodeID
		for i := 1; i <= 30; i++ {
			if i == 16 {
				crashed, _ = c.Leader()
				c.Crash(crashed)
			}
			for k := 0; ; k++ { // wait for some node to lead
				if _, ok := c.Leader(); ok {
					break
				}
				if k == 2000 {
					t.Fatalf("command %d: no leader for 2000 ticks", i)
				}
				step()
			}
			leader, _ := c.Leader()
			cmd := fmt.Sprintf("c%d", i)
			if res, err := c.Propose(leader, []byte(cmd)); err == nil && res.IsLeader {
				pending = append(pending, proposal{leader, res.Index, res.Term, cmd})
			}
			for k := 0; k < 10; k++ {
				step()
			}
		}
		if crashed == "" {
			t.Fatal("no leader was crashed")
		}
		if err := c.Restart(crashed); err != nil {
			t.Fatal(err)
		}
		c.SetDrop(0)

		for k := 0; !sameApplied(c); k++ {
			if k == 3000 {
				for _, id := range three {
					t.Logf("%s applied %d entries", id, len(c.applied[id]))
				}
				t.Fatal("the three nodes did not converge on one applied sequence within 3000 ticks")
			}
			step()
		}

		if len(acked) < 15 {
			t.Fatalf("only %d of 30 commands were acknowledged; the run is too lossy to show anything", len(acked))
		}
		for _, a := range acked {
			for _, id := range three {
				found := false
				for _, e := range c.applied[id] {
					if e.Index == a.index {
						found = string(e.Cmd) == a.cmd && e.Term == a.term
						break
					}
				}
				if !found {
					t.Errorf("%s: acknowledged %q at index %d (term %d) is missing from its applied sequence", id, a.cmd, a.index, a.term)
				}
			}
		}
	})
}
