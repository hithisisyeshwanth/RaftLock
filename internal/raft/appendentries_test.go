package raft

import "testing"

// followerLog is the log every test here starts from: 1:(t1,a) 2:(t1,b) 3:(t2,c) 4:(t2,d).
func followerLog() []Entry {
	return []Entry{en(1, 1, "a"), en(2, 1, "b"), en(3, 2, "c"), en(4, 2, "d")}
}

// followerAt5 is n1, a follower in term 5 holding followerLog.
func followerAt5(t *testing.T) (*Node, *MemStorage, int) {
	t.Helper()
	st := NewMemStorage()
	if err := st.Save(PersistentState{CurrentTerm: 5, Log: followerLog()}); err != nil {
		t.Fatal(err)
	}
	return newTestNode(t, st), st, st.SaveCount()
}

func appendEntries(n *Node, args AppendEntriesArgs) []Message {
	if args.LeaderID == "" {
		args.LeaderID = "n2"
	}
	return n.Step(Message{From: args.LeaderID, To: n.id, Body: args})
}

// Figure 2, AppendEntries rule 1, now with entries attached: nothing from a stale leader is taken.
func TestAppendEntries_StaleTermRejectedWithEntries(t *testing.T) {
	n, st, saves := followerAt5(t)

	reply := aeReply(t, appendEntries(n, AppendEntriesArgs{Term: 4, PrevLogIndex: 4, PrevLogTerm: 2, Entries: []Entry{en(5, 4, "e")}}), "n2")

	if reply.Success || reply.Term != 5 {
		t.Errorf("reply = %+v, want term 5, not successful", reply)
	}
	if got := n.Snapshot().Log; !entriesEqual(got, followerLog()) {
		t.Errorf("log = %+v, want it unchanged", got)
	}
	if st.SaveCount() != saves {
		t.Errorf("SaveCount() = %d, want %d", st.SaveCount(), saves)
	}
}

// Figure 2, AppendEntries rule 2: reply false if the log has no entry at prevLogIndex with
// prevLogTerm. It applies to heartbeats too, since the leader uses the answer to find nextIndex.
func TestAppendEntries_FailedCheck(t *testing.T) {
	tests := []struct {
		name     string
		prev     Index
		prevTerm Term
		entries  []Entry
	}{
		{"prev past the end, with entries", 6, 2, []Entry{en(7, 5, "g")}},
		{"prev past the end, heartbeat", 5, 2, nil},
		{"term mismatch at prev, with entries", 4, 3, []Entry{en(5, 5, "e")}},
		{"term mismatch at prev, heartbeat", 2, 2, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, st, saves := followerAt5(t)

			reply := aeReply(t, appendEntries(n, AppendEntriesArgs{Term: 5, PrevLogIndex: tt.prev, PrevLogTerm: tt.prevTerm, Entries: tt.entries}), "n2")

			if reply.Success {
				t.Error("Success = true, want false (the consistency check fails)")
			}
			if reply.Term != 5 || reply.PrevLogIndex != tt.prev {
				t.Errorf("reply Term/PrevLogIndex = %d/%d, want 5/%d (echoed)", reply.Term, reply.PrevLogIndex, tt.prev)
			}
			got := n.Snapshot()
			if !entriesEqual(got.Log, followerLog()) {
				t.Errorf("log = %+v, want it unchanged", got.Log)
			}
			if got.LeaderID != "n2" || got.Role != Follower {
				t.Errorf("leader/role = %q/%v, want n2/Follower (it is still this term's leader)", got.LeaderID, got.Role)
			}
			if st.SaveCount() != saves {
				t.Errorf("SaveCount() = %d, want %d (nothing durable changed)", st.SaveCount(), saves)
			}
		})
	}
}

// A passing check merges the entries (T3.4) and answers with MatchIndex = prev + len(entries):
// the highest index this message proves the follower holds, not the length of its log.
func TestAppendEntries_SuccessfulMerge(t *testing.T) {
	tests := []struct {
		name      string
		prev      Index
		prevTerm  Term
		entries   []Entry
		wantLog   []Entry
		wantMatch Index
	}{
		{"append after the end", 4, 2, []Entry{en(5, 5, "e"), en(6, 5, "f")},
			append(followerLog(), en(5, 5, "e"), en(6, 5, "f")), 6},
		{"conflict truncates and replaces", 2, 1, []Entry{en(3, 5, "X")},
			[]Entry{en(1, 1, "a"), en(2, 1, "b"), en(3, 5, "X")}, 3},
		{"stale shorter message keeps the tail", 0, 0, []Entry{en(1, 1, "a"), en(2, 1, "b")},
			followerLog(), 2},
		{"heartbeat at the end", 4, 2, nil, followerLog(), 4},
		{"heartbeat in the middle", 2, 1, nil, followerLog(), 2},
		{"heartbeat at the sentinel", 0, 0, nil, followerLog(), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, _, _ := followerAt5(t)

			reply := aeReply(t, appendEntries(n, AppendEntriesArgs{Term: 5, PrevLogIndex: tt.prev, PrevLogTerm: tt.prevTerm, Entries: tt.entries}), "n2")

			want := AppendEntriesReply{Term: 5, Success: true, MatchIndex: tt.wantMatch, PrevLogIndex: tt.prev}
			if reply != want {
				t.Errorf("reply = %+v, want %+v", reply, want)
			}
			if got := n.Snapshot().Log; !entriesEqual(got, tt.wantLog) {
				t.Errorf("log = %+v\nwant  %+v", got, tt.wantLog)
			}
		})
	}
}

// CLAUDE.md rule 2: a changed log is on disk before the reply is returned.
func TestAppendEntries_PersistBeforeReply(t *testing.T) {
	t.Run("changed log is saved", func(t *testing.T) {
		n, st, saves := followerAt5(t)
		appendEntries(n, AppendEntriesArgs{Term: 5, PrevLogIndex: 2, PrevLogTerm: 1, Entries: []Entry{en(3, 5, "X")}})

		if st.SaveCount() != saves+1 {
			t.Errorf("SaveCount() = %d, want %d", st.SaveCount(), saves+1)
		}
		if s := lastSaved(t, st); !entriesEqual(s.Log, n.Snapshot().Log) || s.CurrentTerm != 5 {
			t.Errorf("saved term %d, log %+v; want term 5 and the merged log", s.CurrentTerm, s.Log)
		}
	})
	t.Run("unchanged log is not saved", func(t *testing.T) {
		n, st, saves := followerAt5(t)
		appendEntries(n, AppendEntriesArgs{Term: 5, PrevLogIndex: 0, PrevLogTerm: 0, Entries: followerLog()})
		appendEntries(n, AppendEntriesArgs{Term: 5, PrevLogIndex: 4, PrevLogTerm: 2})
		if st.SaveCount() != saves {
			t.Errorf("SaveCount() = %d after a duplicate and a heartbeat, want %d", st.SaveCount(), saves)
		}
	})
	t.Run("higher term and new entries are both saved", func(t *testing.T) {
		n, st, _ := followerAt5(t)
		reply := aeReply(t, appendEntries(n, AppendEntriesArgs{Term: 7, PrevLogIndex: 4, PrevLogTerm: 2, Entries: []Entry{en(5, 7, "e")}}), "n2")
		if !reply.Success || reply.Term != 7 || reply.MatchIndex != 5 {
			t.Errorf("reply = %+v, want term 7, success, match 5", reply)
		}
		if s := lastSaved(t, st); s.CurrentTerm != 7 || len(s.Log) != 5 {
			t.Errorf("saved term %d with %d entries, want term 7 with 5", s.CurrentTerm, len(s.Log))
		}
	})
	t.Run("failed save sends nothing and keeps the old log", func(t *testing.T) {
		n, _ := spyNode(t, PersistentState{CurrentTerm: 5, Log: followerLog()}, Follower, 1)
		out := appendEntries(n, AppendEntriesArgs{Term: 5, PrevLogIndex: 2, PrevLogTerm: 1, Entries: []Entry{en(3, 5, "X")}})
		if len(out) != 0 {
			t.Errorf("Step() = %+v after a failed save, want no messages", out)
		}
		if got := n.Snapshot().Log; !entriesEqual(got, followerLog()) {
			t.Errorf("log = %+v after a failed save, want the old log (memory must match disk)", got)
		}
	})
}

// Replies are one-way and may arrive late or twice; the echoed PrevLogIndex lets the leader match
// a reply to the request it answers.
func TestAppendEntries_ReplyFields(t *testing.T) {
	n, _, _ := followerAt5(t)
	out := appendEntries(n, AppendEntriesArgs{Term: 5, LeaderID: "n3", PrevLogIndex: 3, PrevLogTerm: 2, Entries: []Entry{en(4, 2, "d"), en(5, 5, "e")}})

	reply := aeReply(t, out, "n3")
	want := AppendEntriesReply{Term: 5, Success: true, MatchIndex: 5, PrevLogIndex: 3}
	if reply != want {
		t.Errorf("reply = %+v, want %+v", reply, want)
	}
}
