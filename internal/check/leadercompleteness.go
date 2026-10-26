package check

import (
	"fmt"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

type commitRecord struct {
	entry raft.Entry
	term  raft.Term // term of the node that first reported the entry committed
}

// LeaderCompleteness checks Figure 3's Leader Completeness: an entry committed in a term
// is present in the log of every leader of a higher term.
type LeaderCompleteness struct {
	committed []commitRecord // committed[i] is the entry committed at index i+1
}

func NewLeaderCompleteness() *LeaderCompleteness {
	return &LeaderCompleteness{}
}

func (c *LeaderCompleteness) Observe(snaps []Snapshot) error {
	// Record every commit before checking leaders, so a commit and a bad leader in one step still conflict.
	for _, s := range snaps {
		upTo := min(int(s.CommitIndex), len(s.Log))
		for i := len(c.committed); i < upTo; i++ {
			c.committed = append(c.committed, commitRecord{entry: cloneEntries(s.Log[i : i+1])[0], term: s.Term})
		}
	}
	for _, s := range snaps {
		if s.Role != raft.Leader {
			continue
		}
		for i, rec := range c.committed {
			if s.Term <= rec.term {
				continue
			}
			if i >= len(s.Log) || !s.Log[i].Equal(rec.entry) {
				return fmt.Errorf("leader completeness violated: %s, leader of term %d, lacks index %d committed in term %d", s.ID, s.Term, i+1, rec.term)
			}
		}
	}
	return nil
}
