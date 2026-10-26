package check

import (
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

type observer interface {
	Observe([]Snapshot) error
}

// state builds a snapshot with a role, term, log and commit index; mkLog assigns the log indexes 1, 2, 3, ...
func state(id raft.NodeID, term raft.Term, role raft.Role, commit raft.Index, ents ...ent) Snapshot {
	return Snapshot{ID: id, Term: term, Role: role, CommitIndex: commit, Log: mkLog(ents...)}
}

// stepCase is a sequence of Observe calls, one per step, whose first error must come at step wantErrAt (-1 for none).
type stepCase struct {
	name      string
	steps     [][]Snapshot
	wantErrAt int
}

func runStepCases(t *testing.T, newChecker func() observer, tests []stepCase) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newChecker()
			for i, step := range tt.steps {
				err := c.Observe(step)
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
