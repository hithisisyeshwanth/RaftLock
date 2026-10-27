package raft

import "testing"

// Figure 2, AppendEntries rule 5 (slide 7): if leaderCommit > commitIndex, set
// commitIndex = min(leaderCommit, index of last new entry). The last new entry is
// prevLogIndex + len(entries): the end of what this message proved, not the end of the log,
// which may still hold stale entries the leader has not yet overwritten.
func TestFollowerCommit(t *testing.T) {
	ae := func(prev Index, prevTerm Term, commit Index, entries ...Entry) AppendEntriesArgs {
		return AppendEntriesArgs{Term: 5, PrevLogIndex: prev, PrevLogTerm: prevTerm, LeaderCommit: commit, Entries: entries}
	}
	tests := []struct {
		name  string
		steps []AppendEntriesArgs // delivered in order to followerAt5: log 1:(t1) 2:(t1) 3:(t2) 4:(t2)
		want  Index
	}{
		// min(leaderCommit, last new entry).
		{"new entry, leaderCommit far ahead", []AppendEntriesArgs{ae(4, 2, 10, en(5, 5, "e"))}, 5},
		{"new entries, leaderCommit behind them", []AppendEntriesArgs{ae(4, 2, 4, en(5, 5, "e"), en(6, 5, "f"))}, 4},
		{"heartbeat at the end", []AppendEntriesArgs{ae(4, 2, 3)}, 3},
		{"heartbeat at the end, leaderCommit far ahead", []AppendEntriesArgs{ae(4, 2, 10)}, 4},
		{"stale shorter message: last new entry is 2, not the log's 4", []AppendEntriesArgs{
			ae(0, 0, 10, en(1, 1, "a"), en(2, 1, "b"))}, 2},

		// Monotonic.
		{"lower leaderCommit later", []AppendEntriesArgs{ae(4, 2, 3), ae(4, 2, 1)}, 3},
		{"later message proves less of the log", []AppendEntriesArgs{ae(4, 2, 4), ae(2, 1, 10)}, 4},
		{"leaderCommit 0", []AppendEntriesArgs{ae(4, 2, 2), ae(4, 2, 0)}, 2},

		// A failed AppendEntries never moves commitIndex.
		{"prev past the end", []AppendEntriesArgs{ae(6, 2, 10)}, 0},
		{"term mismatch at prev", []AppendEntriesArgs{ae(4, 3, 10)}, 0},
		{"failure after a commit", []AppendEntriesArgs{ae(4, 2, 2), ae(4, 3, 10)}, 2},
		{"stale term", []AppendEntriesArgs{{Term: 4, PrevLogIndex: 4, PrevLogTerm: 2, LeaderCommit: 10}}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, _, _ := followerAt5(t)
			for _, args := range tt.steps {
				appendEntries(n, args)
			}
			if got := n.Snapshot().CommitIndex; got != tt.want {
				t.Errorf("commitIndex = %d, want %d", got, tt.want)
			}
		})
	}
}

// The spec's key case. The follower's log has 5 entries, but 4 and 5 are stale (term 3, from a
// leader that lost). The new leader's heartbeat proves only entries 1 to 3 match, so although
// leaderCommit is 10 and the log is 5 long, the follower may commit only up to 3.
func TestFollowerCommitStaleTailHeartbeat(t *testing.T) {
	st := NewMemStorage()
	st.Save(PersistentState{CurrentTerm: 5, Log: []Entry{
		en(1, 1, "a"), en(2, 1, "b"), en(3, 2, "c"), en(4, 3, "stale"), en(5, 3, "stale"),
	}})
	n := newTestNode(t, st)

	reply := aeReply(t, appendEntries(n, AppendEntriesArgs{Term: 5, PrevLogIndex: 3, PrevLogTerm: 2, LeaderCommit: 10}), "n2")

	if !reply.Success {
		t.Fatalf("reply = %+v, want success (entry 3 matches)", reply)
	}
	if got := n.Snapshot().CommitIndex; got != 3 {
		t.Errorf("commitIndex = %d, want 3 (not 5: entries 4 and 5 were never confirmed by this leader)", got)
	}
}

// commitIndex is volatile: learning it from the leader writes nothing.
func TestFollowerCommitNotPersisted(t *testing.T) {
	n, st, saves := followerAt5(t)
	appendEntries(n, AppendEntriesArgs{Term: 5, PrevLogIndex: 4, PrevLogTerm: 2, LeaderCommit: 4})
	if got := n.Snapshot().CommitIndex; got != 4 {
		t.Fatalf("setup: commitIndex = %d, want 4", got)
	}
	if st.SaveCount() != saves {
		t.Errorf("SaveCount() = %d, want %d (a heartbeat that only moves commitIndex saves nothing)", st.SaveCount(), saves)
	}
}
