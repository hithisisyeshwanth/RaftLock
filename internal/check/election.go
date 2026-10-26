// Package check holds the Figure 3 safety checkers, run against node snapshots after every simulation step.
package check

import (
	"fmt"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// Snapshot is one node's state at a point in time. Later checkers add fields (commitIndex, applied).
type Snapshot struct {
	ID   raft.NodeID
	Term raft.Term
	Role raft.Role
	Log  []raft.Entry // real entries from index 1, no sentinel
}

// ElectionSafety checks that at most one leader is ever seen per term (Figure 3).
type ElectionSafety struct {
	leaders map[raft.Term]raft.NodeID
}

func NewElectionSafety() *ElectionSafety {
	return &ElectionSafety{leaders: map[raft.Term]raft.NodeID{}}
}

// Observe remembers every leader across calls, so a leader that has since left the
// snapshot set still conflicts with a later leader of the same term.
func (e *ElectionSafety) Observe(snaps []Snapshot) error {
	for _, s := range snaps {
		if s.Role != raft.Leader {
			continue
		}
		if prev, ok := e.leaders[s.Term]; ok && prev != s.ID {
			return fmt.Errorf("election safety violated: %s and %s are both leader in term %d", prev, s.ID, s.Term)
		}
		e.leaders[s.Term] = s.ID
	}
	return nil
}
