package check

import (
	"fmt"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// StateMachineSafety checks Figure 3's State Machine Safety: no two nodes ever apply
// different entries at the same index. It also checks that each node applies indexes
// 1, 2, 3, ... exactly once and in order since its last boot.
type StateMachineSafety struct {
	applied map[raft.Index]raft.Entry // first entry seen applied at each index, by any node
}

func NewStateMachineSafety() *StateMachineSafety {
	return &StateMachineSafety{applied: map[raft.Index]raft.Entry{}}
}

func (c *StateMachineSafety) Observe(snaps []Snapshot) error {
	for _, s := range snaps {
		for i, e := range s.Applied {
			// Applied lists every apply since boot, so position i must hold index i+1:
			// anything else is a gap, a reorder, or a repeat.
			if want := raft.Index(i + 1); e.Index != want {
				return fmt.Errorf("state machine safety violated: %s applied index %d at position %d, want index %d", s.ID, e.Index, i+1, want)
			}
			if prev, ok := c.applied[e.Index]; ok {
				if !prev.Equal(e) {
					return fmt.Errorf("state machine safety violated: %s applied term %d %q at index %d, but term %d %q was applied there before", s.ID, e.Term, e.Cmd, e.Index, prev.Term, prev.Cmd)
				}
				continue
			}
			// Copy Cmd so a caller reusing its buffer cannot rewrite history.
			e.Cmd = append([]byte(nil), e.Cmd...)
			c.applied[e.Index] = e
		}
	}
	return nil
}
