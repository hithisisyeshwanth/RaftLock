package check

import (
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

func snap(id raft.NodeID, term raft.Term, role raft.Role) Snapshot {
	return Snapshot{ID: id, Term: term, Role: role}
}

// TestElectionSafety feeds each case one Observe call per step, in order,
// and expects the first error at step wantErrAt (-1 for none).
func TestElectionSafety(t *testing.T) {
	const (
		F = raft.Follower
		C = raft.Candidate
		L = raft.Leader
	)
	tests := []struct {
		name      string
		steps     [][]Snapshot
		wantErrAt int
	}{
		{
			name:      "no snapshots",
			steps:     [][]Snapshot{{}},
			wantErrAt: -1,
		},
		{
			name: "same leader seen again in the same term",
			steps: [][]Snapshot{
				{snap("n1", 2, L), snap("n2", 2, F), snap("n3", 2, F)},
				{snap("n1", 2, L), snap("n2", 2, F), snap("n3", 2, F)},
				{snap("n1", 2, L), snap("n2", 2, F), snap("n3", 2, F)},
			},
			wantErrAt: -1,
		},
		{
			name: "different leaders in different terms",
			steps: [][]Snapshot{
				{snap("n1", 1, L), snap("n2", 1, F), snap("n3", 1, F)},
				{snap("n1", 2, F), snap("n2", 2, L), snap("n3", 2, F)},
				{snap("n1", 3, F), snap("n2", 3, F), snap("n3", 3, L)},
			},
			wantErrAt: -1,
		},
		{
			name: "a leader steps down and another wins a later term",
			steps: [][]Snapshot{
				{snap("n1", 1, L), snap("n2", 1, F)},
				{snap("n1", 2, F), snap("n2", 2, C)},
				{snap("n1", 2, F), snap("n2", 2, L)}, // term 2 has only ever had n2 as leader
			},
			wantErrAt: -1,
		},
		{
			name: "candidates and followers in the leader's term do not count",
			steps: [][]Snapshot{
				{snap("n1", 2, L), snap("n2", 2, C), snap("n3", 2, C)},
				{snap("n1", 2, L), snap("n2", 2, F), snap("n3", 2, C)},
			},
			wantErrAt: -1,
		},
		{
			name: "candidates in a term never make it a violation",
			steps: [][]Snapshot{
				{snap("n1", 3, C), snap("n2", 3, C), snap("n3", 3, C)},
			},
			wantErrAt: -1,
		},
		{
			name: "two leaders in the same term in one snapshot set",
			steps: [][]Snapshot{
				{snap("n1", 2, L), snap("n2", 2, L), snap("n3", 2, F)},
			},
			wantErrAt: 0,
		},
		{
			name: "two leaders in the same term at different times",
			steps: [][]Snapshot{
				{snap("n1", 2, L), snap("n2", 2, F), snap("n3", 2, F)},
				{snap("n1", 2, L), snap("n2", 2, F), snap("n3", 2, F)},
				// n1 has gone away from the snapshot set, but the checker must remember it led term 2
				{snap("n2", 2, L), snap("n3", 2, F)},
			},
			wantErrAt: 2,
		},
		{
			name: "hand-built bad history: violation appears at step 3",
			steps: [][]Snapshot{
				{snap("n1", 1, C), snap("n2", 1, F), snap("n3", 1, F)},
				{snap("n1", 1, L), snap("n2", 1, F), snap("n3", 1, F)},
				{snap("n1", 2, F), snap("n2", 2, C), snap("n3", 2, F)},
				{snap("n1", 2, F), snap("n2", 2, L), snap("n3", 2, L)},
			},
			wantErrAt: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewElectionSafety()
			for i, step := range tt.steps {
				err := e.Observe(step)
				switch {
				case i == tt.wantErrAt && err == nil:
					t.Fatalf("step %d: Observe returned nil, want a violation", i)
				case i != tt.wantErrAt && err != nil:
					t.Fatalf("step %d: Observe returned %v, want nil", i, err)
				}
				if i == tt.wantErrAt {
					return
				}
			}
		})
	}
}

// Each checker starts with no history, so a violation in one run is invisible to a fresh one.
func TestElectionSafetyFreshCheckerHasNoHistory(t *testing.T) {
	first := NewElectionSafety()
	if err := first.Observe([]Snapshot{snap("n1", 2, Leader)}); err != nil {
		t.Fatalf("Observe() = %v", err)
	}
	second := NewElectionSafety()
	if err := second.Observe([]Snapshot{snap("n2", 2, Leader)}); err != nil {
		t.Errorf("a new checker flagged a leader seen only by another checker: %v", err)
	}
}
