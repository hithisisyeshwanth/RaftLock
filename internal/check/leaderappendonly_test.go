package check

import (
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

func TestLeaderAppendOnly(t *testing.T) {
	const (
		F = raft.Follower
		L = raft.Leader
	)
	runStepCases(t, func() observer { return NewLeaderAppendOnly() }, []stepCase{
		// A leader's log may only grow within its term.
		{
			name:      "no snapshots",
			steps:     [][]Snapshot{{}},
			wantErrAt: -1,
		},
		{
			name: "leader log unchanged",
			steps: [][]Snapshot{
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"})},
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"})},
			},
			wantErrAt: -1,
		},
		{
			name: "leader log grows by appending",
			steps: [][]Snapshot{
				{state("n1", 2, L, 0, ent{1, "a"})},
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"})},
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"}, ent{2, "c"})},
			},
			wantErrAt: -1,
		},
		{
			name: "leader starts with an empty log and appends",
			steps: [][]Snapshot{
				{state("n1", 1, L, 0)},
				{state("n1", 1, L, 0, ent{1, "a"})},
			},
			wantErrAt: -1,
		},
		{
			name: "followers may shrink and rewrite their logs freely",
			steps: [][]Snapshot{
				{state("n2", 2, F, 0, ent{1, "a"}, ent{1, "b"}, ent{1, "c"})},
				{state("n2", 2, F, 0, ent{1, "a"})},
				{state("n2", 3, F, 0, ent{1, "a"}, ent{3, "z"})},
			},
			wantErrAt: -1,
		},
		{
			name: "a different leader in a later term may have a shorter log",
			steps: [][]Snapshot{
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"}, ent{2, "c"})},
				{state("n2", 3, L, 0, ent{1, "a"}, ent{2, "b"})},
			},
			wantErrAt: -1,
		},
		{
			name: "the same node leads again in a later term with a truncated log",
			steps: [][]Snapshot{
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"}, ent{2, "c"})},
				{state("n1", 2, F, 0, ent{1, "a"})}, // stepped down and was truncated by the new leader
				{state("n1", 4, L, 0, ent{1, "a"})}, // re-elected in term 4
			},
			wantErrAt: -1,
		},
		{
			name: "two leaders are tracked separately",
			steps: [][]Snapshot{
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"}), state("n2", 3, L, 0, ent{1, "a"})},
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"}, ent{2, "c"}), state("n2", 3, L, 0, ent{1, "a"}, ent{3, "d"})},
			},
			wantErrAt: -1,
		},

		// Violations.
		{
			name: "leader log shrinks within its term",
			steps: [][]Snapshot{
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"}, ent{2, "c"})},
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"})},
			},
			wantErrAt: 1,
		},
		{
			name: "leader log is emptied within its term",
			steps: [][]Snapshot{
				{state("n1", 2, L, 0, ent{1, "a"})},
				{state("n1", 2, L, 0)},
			},
			wantErrAt: 1,
		},
		{
			name: "leader overwrites an entry in place",
			steps: [][]Snapshot{
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"}, ent{2, "c"})},
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "X"}, ent{2, "c"})},
			},
			wantErrAt: 1,
		},
		{
			name: "leader overwrites an entry's term",
			steps: [][]Snapshot{
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"})},
				{state("n1", 2, L, 0, ent{1, "a"}, ent{3, "b"})},
			},
			wantErrAt: 1,
		},
		{
			name: "leader replaces its tail and appends more",
			steps: [][]Snapshot{
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"}, ent{2, "c"})},
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"}, ent{2, "Q"}, ent{2, "d"})},
			},
			wantErrAt: 1,
		},
		{
			name: "violation is caught after the leader was missing from a step",
			steps: [][]Snapshot{
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"})},
				{state("n2", 2, F, 0)},
				{state("n1", 2, L, 0, ent{1, "a"})},
			},
			wantErrAt: 2,
		},
		{
			name: "one of two leaders breaks append-only",
			steps: [][]Snapshot{
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"}), state("n2", 3, L, 0, ent{1, "a"}, ent{3, "c"})},
				{state("n1", 2, L, 0, ent{1, "a"}, ent{2, "b"}), state("n2", 3, L, 0, ent{1, "a"})},
			},
			wantErrAt: 1,
		},
	})
}

func TestLeaderAppendOnlyKeepsItsOwnCopy(t *testing.T) {
	c := NewLeaderAppendOnly()
	first := state("n1", 2, raft.Leader, 0, ent{1, "a"}, ent{2, "b"})
	if err := c.Observe([]Snapshot{first}); err != nil {
		t.Fatalf("Observe() = %v", err)
	}
	// The caller reuses its buffer; the checker must still remember what the leader's log held.
	first.Log[1].Cmd[0] = 'X'
	if err := c.Observe([]Snapshot{state("n1", 2, raft.Leader, 0, ent{1, "a"}, ent{2, "b"})}); err != nil {
		t.Errorf("mutating a slice passed to an earlier Observe changed the checker's history: %v", err)
	}
}
