package raft

import "testing"

// nodeWithLogTerms builds follower n1 at term 5 with no vote cast and a log whose entry i has term terms[i-1].
func nodeWithLogTerms(t *testing.T, terms ...Term) (*Node, *MemStorage, int) {
	t.Helper()
	log := make([]Entry, len(terms))
	for i, term := range terms {
		log[i] = Entry{Term: term, Index: Index(i + 1), Cmd: []byte{byte('a' + i)}}
	}
	st := NewMemStorage()
	if err := st.Save(PersistentState{CurrentTerm: 5, Log: log}); err != nil {
		t.Fatal(err)
	}
	return newTestNode(t, st), st, st.SaveCount()
}

// vote delivers args from args.CandidateID and returns the single RequestVoteReply.
func vote(t *testing.T, n *Node, args RequestVoteArgs) RequestVoteReply {
	t.Helper()
	out := n.Step(Message{From: args.CandidateID, To: n.id, Body: args})
	if len(out) != 1 {
		t.Fatalf("Step(RequestVote) returned %d messages, want 1: %+v", len(out), out)
	}
	reply, ok := out[0].Body.(RequestVoteReply)
	if !ok {
		t.Fatalf("reply body is %T, want RequestVoteReply", out[0].Body)
	}
	return reply
}

// Section 5.4.1: the candidate's log is at least as up to date as the voter's if its last
// term is higher, or its last term is equal and its log is at least as long.
func TestRequestVote_LogCheck(t *testing.T) {
	tests := []struct {
		name         string
		voterLog     []Term // terms of the voter's entries 1, 2, 3, ...
		lastLogIndex Index  // the candidate's
		lastLogTerm  Term   // the candidate's
		want         bool
	}{
		// Last term decides first.
		{"candidate last term higher, log shorter", []Term{1, 2, 3}, 1, 4, true},
		{"candidate last term higher, log same length", []Term{1, 2, 3}, 3, 4, true},
		{"candidate last term higher, log longer", []Term{1, 2, 3}, 10, 4, true},
		{"candidate last term lower, log longer", []Term{1, 2, 3}, 10, 2, false},
		{"candidate last term lower, log same length", []Term{1, 2, 3}, 3, 2, false},
		{"candidate last term lower, log shorter", []Term{1, 2, 3}, 1, 1, false},

		// Same last term: length decides.
		{"same last term, candidate log longer", []Term{1, 2, 3}, 4, 3, true},
		{"same last term, candidate log equal", []Term{1, 2, 3}, 3, 3, true},
		{"same last term, candidate log shorter", []Term{1, 2, 3}, 2, 3, false},
		{"same last term, voter has many entries in it", []Term{1, 3, 3, 3}, 3, 3, false},

		// Empty logs: last index 0 and last term 0.
		{"both logs empty", nil, 0, 0, true},
		{"voter empty, candidate has entries", nil, 2, 1, true},
		{"candidate empty, voter has entries", []Term{1}, 0, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, st, saves := nodeWithLogTerms(t, tt.voterLog...)

			reply := vote(t, n, RequestVoteArgs{Term: 5, CandidateID: "n3", LastLogIndex: tt.lastLogIndex, LastLogTerm: tt.lastLogTerm})

			if reply.VoteGranted != tt.want {
				t.Errorf("VoteGranted = %v, want %v", reply.VoteGranted, tt.want)
			}
			got := n.Snapshot().VotedFor
			if tt.want && got != "n3" {
				t.Errorf("VotedFor = %q after a grant, want n3", got)
			}
			if !tt.want {
				if got != "" {
					t.Errorf("VotedFor = %q after a denial, want it still empty", got)
				}
				if st.SaveCount() != saves {
					t.Errorf("SaveCount() = %d, want %d (a denied vote must not write anything)", st.SaveCount(), saves)
				}
			}
		})
	}
}

// A higher term is still adopted when the vote is denied for a stale log, and the
// node is left with no vote cast in the new term, free to vote for an up-to-date candidate.
func TestRequestVote_LogCheckDeniedStillAdoptsTerm(t *testing.T) {
	n, st, _ := nodeWithLogTerms(t, 1, 2, 3)

	reply := vote(t, n, RequestVoteArgs{Term: 7, CandidateID: "n3", LastLogIndex: 10, LastLogTerm: 2})

	if reply.VoteGranted {
		t.Error("VoteGranted = true for a candidate with a stale log")
	}
	if reply.Term != 7 {
		t.Errorf("reply Term = %d, want 7", reply.Term)
	}
	got := n.Snapshot()
	if got.Term != 7 || got.VotedFor != "" {
		t.Errorf("state = term %d, votedFor %q; want 7, \"\"", got.Term, got.VotedFor)
	}
	if s := lastSaved(t, st); s.CurrentTerm != 7 || s.VotedFor != "" {
		t.Errorf("saved term/votedFor = %d/%q, want 7/\"\"", s.CurrentTerm, s.VotedFor)
	}

	if !vote(t, n, RequestVoteArgs{Term: 7, CandidateID: "n4", LastLogIndex: 3, LastLogTerm: 3}).VoteGranted {
		t.Error("an up-to-date candidate was denied after a stale one in the same term")
	}
}
