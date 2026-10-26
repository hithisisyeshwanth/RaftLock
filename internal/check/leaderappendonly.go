package check

import (
	"fmt"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

type leaderTerm struct {
	id   raft.NodeID
	term raft.Term
}

// LeaderAppendOnly checks Figure 3's Leader Append-Only: a leader never overwrites or
// deletes entries in its log within its term, so its earlier log stays a prefix of its later one.
type LeaderAppendOnly struct {
	last map[leaderTerm][]raft.Entry // the log each leader had when last seen leading that term
}

func NewLeaderAppendOnly() *LeaderAppendOnly {
	return &LeaderAppendOnly{last: map[leaderTerm][]raft.Entry{}}
}

func (c *LeaderAppendOnly) Observe(snaps []Snapshot) error {
	for _, s := range snaps {
		if s.Role != raft.Leader {
			continue
		}
		k := leaderTerm{s.ID, s.Term}
		prev := c.last[k]
		if len(s.Log) < len(prev) {
			return fmt.Errorf("leader append-only violated: %s shrank its log from %d to %d entries in term %d", s.ID, len(prev), len(s.Log), s.Term)
		}
		for i, e := range prev {
			if !e.Equal(s.Log[i]) {
				return fmt.Errorf("leader append-only violated: %s overwrote index %d in term %d", s.ID, e.Index, s.Term)
			}
		}
		c.last[k] = cloneEntries(s.Log)
	}
	return nil
}

// cloneEntries deep-copies so a caller reusing its buffers cannot rewrite a checker's history.
func cloneEntries(entries []raft.Entry) []raft.Entry {
	out := make([]raft.Entry, len(entries))
	for i, e := range entries {
		e.Cmd = append([]byte(nil), e.Cmd...)
		out[i] = e
	}
	return out
}
