package check

import (
	"fmt"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// StateMachineSafety checks Figure 3's State Machine Safety: no two nodes ever apply
// different entries at the same index. It also checks that each node applies indexes
// 1, 2, 3, ... exactly once and in order since its last boot, skipping only leader no-ops in
// its own log, which are committed but never handed to the state machine.
type StateMachineSafety struct {
	applied map[raft.Index]raft.Entry // first entry seen applied at each index, by any node
}

func NewStateMachineSafety() *StateMachineSafety {
	return &StateMachineSafety{applied: map[raft.Index]raft.Entry{}}
}

func (c *StateMachineSafety) Observe(snaps []Snapshot) error {
	for _, s := range snaps {
		want := raft.Index(1) // the next index this node must apply
		for i, e := range s.Applied {
			for want < e.Index && isNoop(s.Log, want) {
				want++
			}
			// Applied lists every apply since boot, so anything other than the next index is a
			// gap, a reorder, or a repeat.
			if e.Index != want {
				return fmt.Errorf("state machine safety violated: %s applied index %d at position %d, want index %d", s.ID, e.Index, i+1, want)
			}
			want++
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

// isNoop reports whether log (entries from index 1, no sentinel) holds a no-op at index.
func isNoop(log []raft.Entry, index raft.Index) bool {
	return index >= 1 && int(index) <= len(log) && log[index-1].Noop
}
