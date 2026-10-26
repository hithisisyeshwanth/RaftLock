package sim

import (
	"fmt"
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/check"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// Clusters name their nodes n1, n2, ..., nN.

func mustStep(t *testing.T, c *Cluster, k int) {
	t.Helper()
	for i := 0; i < k; i++ {
		if err := c.Step(); err != nil {
			t.Fatalf("Step at tick %d: %v", c.Now(), err)
		}
	}
}

func hasLeader(c *Cluster) func() bool {
	return func() bool { _, ok := c.Leader(); return ok }
}

func TestClusterRunsWithoutPanic(t *testing.T) {
	for _, n := range []int{1, 3, 5} {
		c := NewCluster(n, 1)
		mustStep(t, c, 100)
		if c.Now() != 100 {
			t.Errorf("%d nodes: Now() = %d after 100 steps, want 100", n, c.Now())
		}
	}
}

func TestClusterElectsALeader(t *testing.T) {
	c := NewCluster(3, 1)
	if err := c.RunUntil(hasLeader(c), 2000); err != nil {
		t.Fatalf("RunUntil(leader) = %v", err)
	}
	id, _ := c.Leader()
	snap, ok := c.Snapshot(id)
	if !ok || snap.Role != raft.Leader {
		t.Errorf("Leader() = %s, but its snapshot is %+v (live %v)", id, snap, ok)
	}
}

func TestClusterRunUntilTimesOut(t *testing.T) {
	c := NewCluster(3, 1)
	if err := c.RunUntil(func() bool { return false }, 50); err == nil {
		t.Error("RunUntil with a condition that never holds returned nil, want a timeout error")
	}
	if c.Now() != 50 {
		t.Errorf("Now() = %d after a 50-tick RunUntil, want 50", c.Now())
	}
}

// Every random choice comes from the seed, so the same seed replays the same run.
func TestClusterSameSeedSameRun(t *testing.T) {
	trace := func(seed int64) []string {
		c := NewCluster(3, seed)
		var out []string
		for i := 0; i < 600; i++ {
			if err := c.Step(); err != nil {
				t.Fatalf("seed %d, tick %d: %v", seed, c.Now(), err)
			}
			for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
				s, _ := c.Snapshot(id)
				out = append(out, fmt.Sprintf("%s %v %d", id, s.Role, s.Term))
			}
		}
		return out
	}
	a, b := trace(7), trace(7)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("two runs with seed 7 differ at entry %d: %q vs %q", i, a[i], b[i])
		}
	}
}

// A crashed node is neither ticked nor sent anything, so it cannot change its durable state.
func TestClusterCrashedNodeReceivesNothing(t *testing.T) {
	c := NewCluster(3, 1)
	c.Crash("n3")

	if err := c.RunUntil(hasLeader(c), 2000); err != nil {
		t.Fatalf("the two live nodes did not elect a leader: %v", err)
	}
	mustStep(t, c, 500) // the leader heartbeats n3 all along

	if id, _ := c.Leader(); id == "n3" {
		t.Fatal("a crashed node is the leader")
	}
	if _, live := c.Snapshot("n3"); live {
		t.Error("Snapshot(n3) reports a live node while n3 is crashed")
	}
	if got := c.Storage("n3").SaveCount(); got != 0 {
		t.Errorf("n3's storage was written %d times while it was crashed, want 0", got)
	}
}

// Restart builds a new node from the crashed node's storage: durable state comes back,
// volatile state starts over.
func TestClusterRestartRebuildsFromStorage(t *testing.T) {
	c := NewCluster(3, 1)
	if err := c.RunUntil(hasLeader(c), 2000); err != nil {
		t.Fatal(err)
	}
	mustStep(t, c, 50)
	leader, _ := c.Leader()
	c.Crash(leader)

	history := c.Storage(leader).History()
	saved := history[len(history)-1]

	if err := c.Restart(leader); err != nil {
		t.Fatalf("Restart(%s) = %v", leader, err)
	}
	got, live := c.Snapshot(leader)
	if !live {
		t.Fatalf("Snapshot(%s) reports a crashed node after Restart", leader)
	}
	if got.Term != saved.CurrentTerm || got.VotedFor != saved.VotedFor || len(got.Log) != len(saved.Log) {
		t.Errorf("restarted term/votedFor/log length = %d/%q/%d, want the saved %d/%q/%d",
			got.Term, got.VotedFor, len(got.Log), saved.CurrentTerm, saved.VotedFor, len(saved.Log))
	}
	if got.Role != raft.Follower || got.CommitIndex != 0 || got.LastApplied != 0 {
		t.Errorf("restarted role/commitIndex/lastApplied = %v/%d/%d, want Follower/0/0", got.Role, got.CommitIndex, got.LastApplied)
	}
	mustStep(t, c, 500) // and the cluster carries on with it, checkers quiet
}

// The checkers run on every step, so a bad state fails the run on the step it appears.
func TestClusterInjectedViolationFails(t *testing.T) {
	tests := []struct {
		name   string
		tamper func([]check.Snapshot)
	}{
		{"two leaders in one term", func(snaps []check.Snapshot) {
			for i := range snaps {
				snaps[i].Role, snaps[i].Term = raft.Leader, 99
			}
		}},
		{"logs disagree at one index and term", func(snaps []check.Snapshot) {
			for i := range snaps {
				snaps[i].Log = []raft.Entry{{Term: 1, Index: 1, Cmd: []byte{byte('a' + i)}}}
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCluster(3, 1)
			mustStep(t, c, 20)

			c.Tamper = tt.tamper
			if err := c.Step(); err == nil {
				t.Error("Step with a tampered snapshot returned nil, want a checker violation")
			}
		})
	}
	t.Run("RunUntil stops on the violation", func(t *testing.T) {
		c := NewCluster(3, 1)
		c.Tamper = tests[0].tamper
		if err := c.RunUntil(func() bool { return false }, 100); err == nil || c.Now() != 1 {
			t.Errorf("RunUntil = %v at tick %d, want a violation on tick 1", err, c.Now())
		}
	})
}

func TestClusterPartitionAndHeal(t *testing.T) {
	c := NewCluster(3, 1)
	c.Partition([]raft.NodeID{"n1"}, []raft.NodeID{"n2", "n3"})

	for i := 0; i < 2000; i++ {
		mustStep(t, c, 1)
		if s, _ := c.Snapshot("n1"); s.Role == raft.Leader {
			t.Fatalf("tick %d: isolated n1 became leader", c.Now())
		}
	}
	if _, ok := c.Leader(); !ok {
		t.Fatal("the majority side n2, n3 has no leader after 2000 ticks")
	}

	c.Heal()
	sameTerm := func() bool {
		id, ok := c.Leader()
		if !ok {
			return false
		}
		l, _ := c.Snapshot(id)
		for _, other := range []raft.NodeID{"n1", "n2", "n3"} {
			if s, _ := c.Snapshot(other); s.Term != l.Term {
				return false
			}
		}
		return true
	}
	if err := c.RunUntil(sameTerm, 3000); err != nil {
		t.Errorf("after Heal the three nodes did not settle on one leader's term: %v", err)
	}
}
