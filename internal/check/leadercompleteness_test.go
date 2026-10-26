package check

import (
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// An entry counts as committed in the term of the node that first reports it committed
// (index <= CommitIndex); every leader of a higher term must hold it.
func TestLeaderCompleteness(t *testing.T) {
	const (
		F = raft.Follower
		C = raft.Candidate
		L = raft.Leader
	)
	runStepCases(t, func() observer { return NewLeaderCompleteness() }, []stepCase{
		{
			name:      "no snapshots",
			steps:     [][]Snapshot{{}},
			wantErrAt: -1,
		},
		{
			name: "nothing committed, empty leader log",
			steps: [][]Snapshot{
				{state("n1", 1, F, 0), state("n2", 2, L, 0)},
			},
			wantErrAt: -1,
		},
		{
			name: "higher-term leader holds every committed entry",
			steps: [][]Snapshot{
				{state("n1", 2, L, 2, ent{1, "a"}, ent{2, "b"}), state("n2", 2, F, 2, ent{1, "a"}, ent{2, "b"})},
				{state("n2", 3, L, 2, ent{1, "a"}, ent{2, "b"}, ent{3, "c"})},
			},
			wantErrAt: -1,
		},
		{
			name: "higher-term leader may lack uncommitted entries",
			steps: [][]Snapshot{
				{state("n1", 2, L, 1, ent{1, "a"}, ent{2, "b"}, ent{2, "c"})},
				{state("n2", 3, L, 1, ent{1, "a"})},
			},
			wantErrAt: -1,
		},
		{
			name: "a commit reported by a follower binds later leaders too",
			steps: [][]Snapshot{
				{state("n2", 2, F, 2, ent{1, "a"}, ent{2, "b"})},
				{state("n3", 3, L, 0, ent{1, "a"}, ent{2, "b"})},
			},
			wantErrAt: -1,
		},
		{
			name: "a stale leader of a lower term is not held to a later commit",
			steps: [][]Snapshot{
				{state("n1", 3, F, 2, ent{1, "a"}, ent{3, "b"})},
				{state("n2", 2, L, 0, ent{1, "a"})}, // deposed leader still thinking it leads term 2
			},
			wantErrAt: -1,
		},
		{
			name: "a leader of the same term is not checked here",
			steps: [][]Snapshot{
				{state("n1", 2, F, 2, ent{1, "a"}, ent{2, "b"}), state("n2", 2, L, 0, ent{1, "a"})},
			},
			wantErrAt: -1,
		},
		{
			name: "candidates are never checked",
			steps: [][]Snapshot{
				{state("n1", 2, L, 2, ent{1, "a"}, ent{2, "b"})},
				{state("n2", 3, C, 0)},
			},
			wantErrAt: -1,
		},

		// Violations.
		{
			name: "higher-term leader has a shorter log than the committed prefix",
			steps: [][]Snapshot{
				{state("n1", 2, L, 2, ent{1, "a"}, ent{2, "b"})},
				{state("n2", 3, L, 0, ent{1, "a"})},
			},
			wantErrAt: 1,
		},
		{
			name: "higher-term leader has an empty log",
			steps: [][]Snapshot{
				{state("n1", 2, L, 1, ent{1, "a"})},
				{state("n2", 3, L, 0)},
			},
			wantErrAt: 1,
		},
		{
			name: "higher-term leader has a different command at a committed index",
			steps: [][]Snapshot{
				{state("n1", 2, L, 2, ent{1, "a"}, ent{2, "b"})},
				{state("n2", 3, L, 0, ent{1, "a"}, ent{2, "X"})},
			},
			wantErrAt: 1,
		},
		{
			name: "higher-term leader has a different term at a committed index",
			steps: [][]Snapshot{
				{state("n1", 2, L, 2, ent{1, "a"}, ent{2, "b"})},
				{state("n2", 4, L, 0, ent{1, "a"}, ent{3, "b"})},
			},
			wantErrAt: 1,
		},
		{
			name: "the committed entry was reported by a node that has since left the snapshot set",
			steps: [][]Snapshot{
				{state("n1", 2, F, 2, ent{1, "a"}, ent{2, "b"})},
				{state("n2", 3, L, 0, ent{1, "a"})}, // n1 is gone, but index 2 was already committed
			},
			wantErrAt: 1,
		},
		{
			name: "commit and the offending leader appear in the same step",
			steps: [][]Snapshot{
				{state("n2", 3, L, 0, ent{1, "a"}), state("n1", 2, F, 2, ent{1, "a"}, ent{2, "b"})},
			},
			wantErrAt: 0,
		},
		{
			name: "the violation shows up several terms later",
			steps: [][]Snapshot{
				{state("n1", 2, L, 3, ent{1, "a"}, ent{2, "b"}, ent{2, "c"})},
				{state("n2", 3, L, 3, ent{1, "a"}, ent{2, "b"}, ent{2, "c"})},
				{state("n3", 5, L, 0, ent{1, "a"}, ent{2, "b"})}, // lost index 3, committed back in term 2
			},
			wantErrAt: 2,
		},
	})
}

func TestLeaderCompletenessKeepsItsOwnCopy(t *testing.T) {
	c := NewLeaderCompleteness()
	committer := state("n1", 2, raft.Follower, 2, ent{1, "a"}, ent{2, "b"})
	if err := c.Observe([]Snapshot{committer}); err != nil {
		t.Fatalf("Observe() = %v", err)
	}
	// The caller reuses its buffer; the committed entry must still be remembered as "b".
	committer.Log[1].Cmd[0] = 'X'
	if err := c.Observe([]Snapshot{state("n2", 3, raft.Leader, 0, ent{1, "a"}, ent{2, "b"})}); err != nil {
		t.Errorf("mutating a slice passed to an earlier Observe changed the checker's history: %v", err)
	}
}
