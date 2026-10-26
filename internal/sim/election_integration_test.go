package sim

import (
	"fmt"
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

var three = []raft.NodeID{"n1", "n2", "n3"}

// forSeeds runs fn as a subtest for each seed 1..50.
func forSeeds(t *testing.T, fn func(t *testing.T, seed int64)) {
	for seed := int64(1); seed <= 50; seed++ {
		t.Run(fmt.Sprintf("seed %d", seed), func(t *testing.T) { fn(t, seed) })
	}
}

func leaderCount(c *Cluster) int {
	n := 0
	for _, id := range three {
		if s, live := c.Snapshot(id); live && s.Role == raft.Leader {
			n++
		}
	}
	return n
}

func term(c *Cluster, id raft.NodeID) raft.Term {
	s, _ := c.Snapshot(id)
	return s.Term
}

// electFirstLeader runs a fresh three-node cluster until it has a leader, failing after 1000 ticks.
func electFirstLeader(t *testing.T, seed int64) (*Cluster, raft.NodeID, raft.Term) {
	t.Helper()
	c := NewCluster(3, seed)
	if err := c.RunUntil(hasLeader(c), 1000); err != nil {
		t.Fatalf("no leader within 1000 ticks: %v", err)
	}
	id, _ := c.Leader()
	return c, id, term(c, id)
}

func TestElectionExactlyOneLeaderWithin1000Ticks(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c, _, _ := electFirstLeader(t, seed)
		if got := leaderCount(c); got != 1 {
			t.Errorf("%d nodes think they are leader, want exactly 1", got)
		}
	})
}

// With heartbeats every 50 ticks and election timeouts of 150 to 300, a healthy leader is never
// challenged: its term never changes and no node starts a later election. A follower may still be
// in an earlier term for a few ticks, until the new leader's first heartbeat reaches it.
func TestElectionLeaderStableFor2000Ticks(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c, leader, lt := electFirstLeader(t, seed)
		for i := 0; i < 2000; i++ {
			mustStep(t, c, 1)
			if id, _ := c.Leader(); id != leader || term(c, leader) != lt {
				t.Fatalf("tick %d: leader is %s in term %d, want %s to stay leader in term %d", c.Now(), id, term(c, id), leader, lt)
			}
			for _, id := range three {
				if got := term(c, id); got > lt {
					t.Fatalf("tick %d: %s reached term %d, want no election after term %d", c.Now(), id, got, lt)
				}
			}
		}
	})
}

func TestElectionKilledLeaderIsReplacedInHigherTerm(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c, old, oldTerm := electFirstLeader(t, seed)
		mustStep(t, c, 100)
		c.Crash(old)

		newLeader := func() bool {
			id, ok := c.Leader()
			return ok && id != old && term(c, id) > oldTerm
		}
		if err := c.RunUntil(newLeader, 1000); err != nil {
			t.Fatalf("no new leader in a term above %d after %s crashed: %v", oldTerm, old, err)
		}
	})
}

// The old leader restarts from disk in its old term, hears the new leader, and follows it.
func TestElectionRestartedOldLeaderFollows(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c, old, oldTerm := electFirstLeader(t, seed)
		mustStep(t, c, 100)
		c.Crash(old)
		if err := c.RunUntil(func() bool { id, ok := c.Leader(); return ok && id != old }, 1000); err != nil {
			t.Fatal(err)
		}
		mustStep(t, c, 100)
		current, _ := c.Leader()
		currentTerm := term(c, current)

		if err := c.Restart(old); err != nil {
			t.Fatal(err)
		}
		if got := term(c, old); got != oldTerm {
			t.Errorf("restarted %s is in term %d, want its saved term %d", old, got, oldTerm)
		}
		follows := func() bool {
			s, _ := c.Snapshot(old)
			return s.Role == raft.Follower && s.Term == currentTerm && s.LeaderID == current
		}
		if err := c.RunUntil(follows, 500); err != nil {
			s, _ := c.Snapshot(old)
			t.Fatalf("restarted %s did not follow %s in term %d: now %v in term %d: %v", old, current, currentTerm, s.Role, s.Term, err)
		}
		if id, _ := c.Leader(); id != current || term(c, current) != currentTerm {
			t.Errorf("the restart disturbed the leader: now %s in term %d, want %s in term %d", id, term(c, id), current, currentTerm)
		}
	})
}

// Two live nodes of three with the same fixed timeout time out together forever: each votes
// for itself, so neither can get the other's vote. Random timeouts break the tie.
func TestElectionSplitVoteResolvedByRandomTimeouts(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := NewCluster(3, seed)
		c.Crash("n3")
		for _, id := range []raft.NodeID{"n1", "n2"} {
			c.Crash(id)
			c.SetTimeouts(id, raft.NewFixedTimeouts(200))
			if err := c.Restart(id); err != nil {
				t.Fatal(err)
			}
		}

		mustStep(t, c, 2000)
		if id, ok := c.Leader(); ok {
			t.Fatalf("%s became leader although both nodes always time out together", id)
		}
		if t1, t2 := term(c, "n1"), term(c, "n2"); t1 < 5 || t1 != t2 {
			t.Fatalf("terms after 2000 ticks of split votes = %d, %d; want equal and at least 5", t1, t2)
		}

		for i, id := range []raft.NodeID{"n1", "n2"} {
			c.Crash(id)
			c.SetTimeouts(id, raft.NewSeededTimeouts(seed*10+int64(i), 150, 300))
			if err := c.Restart(id); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.RunUntil(hasLeader(c), 2000); err != nil {
			t.Errorf("random timeouts did not resolve the split vote: %v", err)
		}
	})
}

// Without PreVote (out of scope), an isolated node keeps starting elections, so its term keeps
// rising. When it rejoins, that higher term forces the healthy leader to step down and a new
// election to run. This test documents that disruption; it is expected, not a bug.
func TestElectionIsolatedNodeForcesReelectionAfterHeal(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c, leader, lt := electFirstLeader(t, seed)
		var isolated raft.NodeID
		var others []raft.NodeID
		for _, id := range three {
			if id != leader && isolated == "" {
				isolated = id
			} else {
				others = append(others, id)
			}
		}
		c.Partition([]raft.NodeID{isolated}, others)

		mustStep(t, c, 1500)
		mid := term(c, isolated)
		mustStep(t, c, 1500)
		late := term(c, isolated)
		if !(lt < mid && mid < late) {
			t.Fatalf("isolated %s's term went %d -> %d -> %d, want it to keep rising", isolated, lt, mid, late)
		}
		if id, _ := c.Leader(); id != leader || term(c, leader) != lt {
			t.Fatalf("the majority side lost its leader during the partition: now %s in term %d", id, term(c, id))
		}

		c.Heal()
		reelected := func() bool {
			id, ok := c.Leader()
			if !ok || term(c, id) < late {
				return false
			}
			for _, other := range three {
				if term(c, other) != term(c, id) {
					return false
				}
			}
			return true
		}
		if err := c.RunUntil(reelected, 3000); err != nil {
			t.Fatalf("after Heal no leader emerged in a term of at least %d: %v", late, err)
		}
	})
}
