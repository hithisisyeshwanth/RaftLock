package check

import (
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// Each case breaks exactly one of the five Figure 3 properties, so All must be running that checker.
func TestAllCatchesEachProperty(t *testing.T) {
	const (
		F = raft.Follower
		L = raft.Leader
	)
	runStepCases(t, func() observer { return NewAll() }, []stepCase{
		{
			name: "Election Safety: two leaders in one term",
			steps: [][]Snapshot{
				{state("n1", 1, L, 0), state("n2", 1, L, 0), state("n3", 1, F, 0)},
			},
			wantErrAt: 0,
		},
		{
			name: "Log Matching: same index and term, different command",
			steps: [][]Snapshot{
				{state("n1", 1, F, 0, ent{1, "a"}), state("n2", 1, F, 0, ent{1, "X"})},
			},
			wantErrAt: 0,
		},
		{
			name: "State Machine Safety: a gap in applied indexes",
			steps: [][]Snapshot{
				{{ID: "n1", Term: 1, Role: F, Applied: []raft.Entry{appliedEntry(1, 1, "a"), appliedEntry(3, 1, "c")}}},
			},
			wantErrAt: 0,
		},
		{
			name: "Leader Append-Only: a leader's log shrinks",
			steps: [][]Snapshot{
				{state("n1", 1, L, 0, ent{1, "a"}, ent{1, "b"})},
				{state("n1", 1, L, 0, ent{1, "a"})},
			},
			wantErrAt: 1,
		},
		{
			name: "Leader Completeness: a later leader lacks a committed entry",
			steps: [][]Snapshot{
				{state("n1", 2, F, 2, ent{1, "a"}, ent{2, "b"})},
				{state("n2", 3, L, 0, ent{1, "a"})},
			},
			wantErrAt: 1,
		},
	})
}

func TestAllPassesACleanHistory(t *testing.T) {
	const (
		F = raft.Follower
		L = raft.Leader
	)
	runStepCases(t, func() observer { return NewAll() }, []stepCase{
		{
			name: "election, replication, commit, apply, then a new leader",
			steps: [][]Snapshot{
				{state("n1", 1, L, 0), state("n2", 1, F, 0), state("n3", 1, F, 0)},
				{state("n1", 1, L, 0, ent{1, "a"}), state("n2", 1, F, 0), state("n3", 1, F, 0)},
				{state("n1", 1, L, 1, ent{1, "a"}), state("n2", 1, F, 0, ent{1, "a"}), state("n3", 1, F, 0)},
				{
					{ID: "n1", Term: 1, Role: L, CommitIndex: 1, Log: mkLog(ent{1, "a"}), Applied: []raft.Entry{appliedEntry(1, 1, "a")}},
					state("n2", 1, F, 1, ent{1, "a"}),
					state("n3", 1, F, 0, ent{1, "a"}),
				},
				{state("n1", 2, F, 1, ent{1, "a"}), state("n2", 2, L, 1, ent{1, "a"}), state("n3", 2, F, 1, ent{1, "a"})},
			},
			wantErrAt: -1,
		},
	})
}

// All must carry state from one call to the next in every checker it runs.
func TestAllKeepsHistoryAcrossCalls(t *testing.T) {
	a := NewAll()
	if err := a.Observe([]Snapshot{state("n1", 2, raft.Leader, 0)}); err != nil {
		t.Fatalf("Observe() = %v", err)
	}
	if err := a.Observe([]Snapshot{state("n2", 2, raft.Leader, 0)}); err == nil {
		t.Error("two different leaders in term 2 across two calls were not flagged")
	}
}

// A fresh All has no history, so one run's violations don't leak into another.
func TestAllFreshHasNoHistory(t *testing.T) {
	first := NewAll()
	first.Observe([]Snapshot{state("n1", 2, raft.Leader, 0)})
	second := NewAll()
	if err := second.Observe([]Snapshot{state("n2", 2, raft.Leader, 0)}); err != nil {
		t.Errorf("a new All flagged a leader only another All had seen: %v", err)
	}
}
