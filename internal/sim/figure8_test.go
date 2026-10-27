package sim

import (
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// Paper Figure 8 with S1..S5 as n1..n5. Index 1 (term 1) is everywhere; the story is index 2.
//
//	(a) S1 leads term 2 and replicates index 2 (term 2) to S2 only.
//	(b) S1 crashes. S5 is elected in term 3 by S3, S4, S5 and writes its own index 2 (term 3).
//	(c) S5 crashes. S1 restarts, wins term 4, and copies its index 2 (term 2) to a majority,
//	    then appends index 3 (term 4). Index 2 is on a majority but MUST NOT count as committed.
//	(d) S1 crashes. S5 wins term 5 (its last term 3 beats the majority's 2) and overwrites
//	    index 2 everywhere. Safe only because (c) never committed it.
//	(e) Had S1 instead replicated its term-4 entry before crashing, index 2 would commit with
//	    it, and S5 could never win: the committed prefix survives.
//
// Stages (a) and (b) are written straight into storage; (c) to (e) run on the harness.
// One node at a time gets a short election timeout so the winner of each election is fixed;
// everyone else waits far longer than any test runs.

const (
	fast = 150
	slow = 100000
)

var five = []raft.NodeID{"n1", "n2", "n3", "n4", "n5"}

func ent(index raft.Index, term raft.Term, cmd string) raft.Entry {
	return raft.Entry{Term: term, Index: index, Cmd: []byte(cmd)}
}

// figure8 returns a five-node cluster in the state right after stage (b), every node crashed.
func figure8(t *testing.T, disableNoop bool) *Cluster {
	t.Helper()
	c := NewCluster(5, 1)
	for _, id := range five {
		c.Crash(id)
	}
	cfg := raft.DefaultConfig()
	cfg.DisableNoop = disableNoop
	c.SetConfig(cfg)

	stored := map[raft.NodeID]raft.PersistentState{
		"n1": {CurrentTerm: 2, VotedFor: "n1", Log: []raft.Entry{ent(1, 1, "a"), ent(2, 2, "b")}}, // (a): leader of term 2
		"n2": {CurrentTerm: 2, VotedFor: "n1", Log: []raft.Entry{ent(1, 1, "a"), ent(2, 2, "b")}}, // (a): got index 2
		"n3": {CurrentTerm: 3, VotedFor: "n5", Log: []raft.Entry{ent(1, 1, "a")}},                 // (b): voted S5
		"n4": {CurrentTerm: 3, VotedFor: "n5", Log: []raft.Entry{ent(1, 1, "a")}},                 // (b): voted S5
		"n5": {CurrentTerm: 3, VotedFor: "n5", Log: []raft.Entry{ent(1, 1, "a"), ent(2, 3, "c")}}, // (b): leader of term 3
	}
	for _, id := range five {
		if err := c.Storage(id).Save(stored[id]); err != nil {
			t.Fatal(err)
		}
		c.SetTimeouts(id, raft.NewFixedTimeouts(slow))
	}
	return c
}

func entryAt(c *Cluster, id raft.NodeID, i raft.Index) (raft.Entry, bool) {
	s, live := c.Snapshot(id)
	if !live || int(i) > len(s.Log) {
		return raft.Entry{}, false
	}
	return s.Log[i-1], true
}

// holding counts the nodes, live or crashed, whose log holds index i with term term.
func holding(c *Cluster, i raft.Index, term raft.Term) int {
	n := 0
	for _, id := range five {
		var log []raft.Entry
		if s, live := c.Snapshot(id); live {
			log = s.Log
		} else if h := c.Storage(id).History(); len(h) > 0 {
			log = h[len(h)-1].Log
		}
		if int(i) <= len(log) && log[i-1].Term == term {
			n++
		}
	}
	return n
}

// step runs one tick and reports whether any live node now counts index 2 from term 2 as committed.
func stepAndCheckIndex2(t *testing.T, c *Cluster) (term2Committed bool) {
	t.Helper()
	mustStep(t, c, 1)
	for _, id := range five {
		s, live := c.Snapshot(id)
		if e, ok := entryAt(c, id, 2); live && ok && e.Term == 2 && s.CommitIndex >= 2 {
			return true
		}
	}
	return false
}

func runUntil(t *testing.T, c *Cluster, what string, max int, cond func() bool) {
	t.Helper()
	if err := c.RunUntil(cond, max); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

func leads(c *Cluster, id raft.NodeID, term raft.Term) func() bool {
	return func() bool {
		s, live := c.Snapshot(id)
		return live && s.Role == raft.Leader && s.Term == term
	}
}

// stageC restarts S1..S4 with S4 cut off, lets S1 win term 4, and waits until S1 has copied its
// term-2 index 2 to S2 and S3. It fails if index 2 is ever counted as committed meanwhile.
func stageC(t *testing.T, c *Cluster) {
	t.Helper()
	c.Partition([]raft.NodeID{"n1", "n2", "n3", "n5"}, []raft.NodeID{"n4"})
	c.SetTimeouts("n1", raft.NewFixedTimeouts(fast))
	for _, id := range []raft.NodeID{"n1", "n2", "n3", "n4"} {
		if err := c.Restart(id); err != nil {
			t.Fatal(err)
		}
	}
	runUntil(t, c, "(c) S1 wins term 4", 1000, leads(c, "n1", 4))
}

func TestFigure8(t *testing.T) {
	t.Run("no-op disabled: (c) majority without commit, then (d) S5 overwrites it", func(t *testing.T) {
		c := figure8(t, true)
		stageC(t, c)

		for i := 0; holding(c, 2, 2) < 3; i++ {
			if i == 300 {
				t.Fatalf("(c) index 2 (term 2) reached only %d nodes", holding(c, 2, 2))
			}
			if stepAndCheckIndex2(t, c) {
				t.Fatal("(c) index 2 from term 2 was committed by counting replicas")
			}
		}
		for i := 0; i < 200; i++ { // stays on a majority for several heartbeats, still uncommitted
			if stepAndCheckIndex2(t, c) {
				t.Fatalf("(c) index 2 (term 2) on %d of 5 nodes was committed: Figure 8's mistake", holding(c, 2, 2))
			}
		}
		if s, _ := c.Snapshot("n1"); s.CommitIndex != 0 {
			t.Fatalf("(c) S1 commitIndex = %d, want 0: no entry from term 4 exists yet", s.CommitIndex)
		}

		if res, err := c.Propose("n1", []byte("d")); err != nil || res.Index != 3 || res.Term != 4 {
			t.Fatalf("(c) S1 Propose = %+v, %v; want index 3 in term 4", res, err)
		}
		c.Crash("n1") // before its next heartbeat: index 3 exists only on S1

		c.SetTimeouts("n5", raft.NewFixedTimeouts(fast))
		if err := c.Restart("n5"); err != nil {
			t.Fatal(err)
		}
		runUntil(t, c, "(d) S5 wins term 5", 1000, leads(c, "n5", 5))
		overwritten := func() bool {
			for _, id := range []raft.NodeID{"n2", "n3"} {
				if e, ok := entryAt(c, id, 2); !ok || e.Term != 3 {
					return false
				}
			}
			return true
		}
		runUntil(t, c, "(d) S5 overwrites index 2 on S2 and S3", 500, overwritten)
		if n := holding(c, 2, 3); n < 3 {
			t.Errorf("(d) index 2 from term 3 is on %d nodes, want a majority", n)
		}
	})

	t.Run("no-op disabled: (e) a term-4 entry on a majority commits index 2, and S5 cannot win", func(t *testing.T) {
		c := figure8(t, true)
		stageC(t, c)
		if res, err := c.Propose("n1", []byte("d")); err != nil || res.Index != 3 || res.Term != 4 {
			t.Fatalf("(e) S1 Propose = %+v, %v; want index 3 in term 4", res, err)
		}
		runUntil(t, c, "(e) S1 commits index 3", 500, func() bool {
			s, _ := c.Snapshot("n1")
			return s.CommitIndex >= 3
		})
		c.Crash("n1")

		c.SetTimeouts("n5", raft.NewFixedTimeouts(fast))
		if err := c.Restart("n5"); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2000; i++ {
			mustStep(t, c, 1)
			if s, _ := c.Snapshot("n5"); s.Role == raft.Leader {
				t.Fatalf("(e) S5 became leader in term %d at tick %d, which would erase the committed index 2", s.Term, c.Now())
			}
		}
		for _, id := range []raft.NodeID{"n2", "n3"} {
			if e, ok := entryAt(c, id, 2); !ok || e.Term != 2 {
				t.Errorf("(e) %s index 2 = %+v, want the committed term-2 entry", id, e)
			}
		}
	})

	t.Run("no-op enabled: index 2 commits only through the term-4 no-op", func(t *testing.T) {
		c := figure8(t, false)
		stageC(t, c)

		if e, ok := entryAt(c, "n1", 3); !ok || !e.Noop || e.Term != 4 {
			t.Fatalf("S1 index 3 = %+v, want its term-4 no-op", e)
		}
		for i := 0; ; i++ {
			s, _ := c.Snapshot("n1")
			if s.CommitIndex == 1 || s.CommitIndex == 2 {
				t.Fatalf("S1 commitIndex = %d: an old-term entry committed on its own, without the no-op", s.CommitIndex)
			}
			if s.CommitIndex >= 3 {
				break
			}
			if i == 500 {
				t.Fatal("S1 never committed its no-op")
			}
			mustStep(t, c, 1)
		}

		c.Crash("n1")
		c.SetTimeouts("n5", raft.NewFixedTimeouts(fast))
		if err := c.Restart("n5"); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2000; i++ {
			mustStep(t, c, 1)
			if s, _ := c.Snapshot("n5"); s.Role == raft.Leader {
				t.Fatalf("S5 became leader in term %d, which would erase the committed index 2", s.Term)
			}
		}
	})
}
