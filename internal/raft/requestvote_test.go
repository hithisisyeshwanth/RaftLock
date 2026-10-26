package raft

import "testing"

// emptyNodeAt builds node n1 with an empty log, so the up-to-date check (T2.5) can never be what decides a vote.
func emptyNodeAt(t *testing.T, term Term, votedFor NodeID, role Role) (*Node, *MemStorage, int) {
	t.Helper()
	st := NewMemStorage()
	if err := st.Save(PersistentState{CurrentTerm: term, VotedFor: votedFor}); err != nil {
		t.Fatal(err)
	}
	n := newTestNode(t, st)
	n.role = role
	return n, st, st.SaveCount()
}

// askVote delivers a RequestVote from candidate for term, with an empty log, and returns the single reply.
func askVote(t *testing.T, n *Node, candidate NodeID, term Term) RequestVoteReply {
	t.Helper()
	out := n.Step(Message{
		From: candidate,
		To:   n.id,
		Body: RequestVoteArgs{Term: term, CandidateID: candidate},
	})
	if len(out) != 1 {
		t.Fatalf("Step(RequestVote) returned %d messages, want exactly 1 reply: %+v", len(out), out)
	}
	if out[0].From != n.id || out[0].To != candidate {
		t.Errorf("reply is from %q to %q, want from %q to %q", out[0].From, out[0].To, n.id, candidate)
	}
	reply, ok := out[0].Body.(RequestVoteReply)
	if !ok {
		t.Fatalf("reply body is %T, want RequestVoteReply", out[0].Body)
	}
	return reply
}

// Figure 2, RequestVote rule 1: reply false if term < currentTerm.
func TestRequestVote_TermLowerIsRejected(t *testing.T) {
	tests := []struct {
		name string
		role Role
		term Term // the candidate's term; the node is at term 5
	}{
		{"follower, one lower", Follower, 4},
		{"candidate, one lower", Candidate, 4},
		{"leader, one lower", Leader, 4},
		{"follower, far lower", Follower, 1},
		{"follower, term 0", Follower, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, st, saves := emptyNodeAt(t, 5, "n2", tt.role)

			reply := askVote(t, n, "n3", tt.term)

			if reply.VoteGranted {
				t.Error("VoteGranted = true for a stale term, want false")
			}
			if reply.Term != 5 {
				t.Errorf("reply Term = %d, want the node's current term 5", reply.Term)
			}

			got := n.Snapshot()
			if got.Term != 5 || got.VotedFor != "n2" || got.Role != tt.role {
				t.Errorf("state changed to term %d, votedFor %q, role %v; want 5, \"n2\", %v",
					got.Term, got.VotedFor, got.Role, tt.role)
			}
			if st.SaveCount() != saves {
				t.Errorf("SaveCount() = %d, want %d (a stale request must not write anything)", st.SaveCount(), saves)
			}
		})
	}
}

// The adopted term must be in the reply, so the handler adopts before it builds the reply.
// Whether the vote is then granted is T2.4's question, so it is not asserted here.
func TestRequestVote_TermHigherIsAdoptedFirst(t *testing.T) {
	tests := []struct {
		name string
		role Role
		from Term
		to   Term
	}{
		{"follower, one higher", Follower, 5, 6},
		{"candidate, one higher", Candidate, 5, 6},
		{"leader, one higher", Leader, 5, 6},
		{"follower, far higher", Follower, 5, 40},
		{"fresh node, term 0 to 1", Follower, 0, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, st, saves := emptyNodeAt(t, tt.from, "n2", tt.role)

			reply := askVote(t, n, "n3", tt.to)

			if reply.Term != tt.to {
				t.Errorf("reply Term = %d, want the adopted term %d", reply.Term, tt.to)
			}
			got := n.Snapshot()
			if got.Term != tt.to {
				t.Errorf("node Term = %d, want %d", got.Term, tt.to)
			}
			if got.Role != Follower {
				t.Errorf("Role = %v, want Follower (a higher term always steps a node down)", got.Role)
			}
			if st.SaveCount() <= saves {
				t.Fatalf("SaveCount() = %d, want more than %d (the new term must be saved)", st.SaveCount(), saves)
			}
			if last := st.History()[st.SaveCount()-1]; last.CurrentTerm != tt.to {
				t.Errorf("last saved term = %d, want %d", last.CurrentTerm, tt.to)
			}
		})
	}
}

// An equal term is neither stale nor newer: the node's term stays, and the reply carries it.
func TestRequestVote_TermEqualKeepsTerm(t *testing.T) {
	for _, role := range []Role{Follower, Candidate, Leader} {
		t.Run(role.String(), func(t *testing.T) {
			n, _, _ := emptyNodeAt(t, 5, "", role)

			reply := askVote(t, n, "n3", 5)

			if reply.Term != 5 {
				t.Errorf("reply Term = %d, want 5", reply.Term)
			}
			if got := n.Snapshot().Term; got != 5 {
				t.Errorf("node Term = %d, want 5", got)
			}
		})
	}
}

func TestRequestVote_ReplyGoesToTheSender(t *testing.T) {
	n, _, _ := emptyNodeAt(t, 5, "", Follower)
	out := n.Step(Message{From: "n3", To: "n1", Body: RequestVoteArgs{Term: 4, CandidateID: "n3"}})
	if len(out) != 1 || out[0].To != "n3" || out[0].From != "n1" {
		t.Errorf("Step() = %+v, want one reply from n1 to n3", out)
	}
}
