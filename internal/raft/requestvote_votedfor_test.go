package raft

import "testing"

// lastSaved is the most recent state the node wrote to storage.
func lastSaved(t *testing.T, st *MemStorage) PersistentState {
	t.Helper()
	h := st.History()
	if len(h) == 0 {
		t.Fatal("nothing was ever saved")
	}
	return h[len(h)-1]
}

// Figure 2, RequestVote rule 2: grant only if votedFor is null or the candidate, so a
// node casts at most one vote per term. Logs are empty throughout, so the log check (T2.5) never decides.
func TestRequestVote_VotedForEmptyGrants(t *testing.T) {
	n, st, saves := emptyNodeAt(t, 5, "", Follower)

	reply := askVote(t, n, "n3", 5)

	if !reply.VoteGranted {
		t.Error("VoteGranted = false with no vote cast this term, want true")
	}
	if reply.Term != 5 {
		t.Errorf("reply Term = %d, want 5", reply.Term)
	}
	got := n.Snapshot()
	if got.VotedFor != "n3" || got.Term != 5 || got.Role != Follower {
		t.Errorf("state = term %d, votedFor %q, role %v; want 5, \"n3\", Follower", got.Term, got.VotedFor, got.Role)
	}
	if st.SaveCount() != saves+1 {
		t.Errorf("SaveCount() = %d, want %d (the vote must be saved)", st.SaveCount(), saves+1)
	}
	if s := lastSaved(t, st); s.CurrentTerm != 5 || s.VotedFor != "n3" {
		t.Errorf("saved term/votedFor = %d/%q, want 5/\"n3\"", s.CurrentTerm, s.VotedFor)
	}
}

func TestRequestVote_VotedForAnotherDenies(t *testing.T) {
	tests := []struct {
		name string
		role Role
		vote NodeID // who the node voted for in term 5
	}{
		{"follower voted for another candidate", Follower, "n2"},
		{"candidate voted for itself", Candidate, "n1"},
		{"leader voted for itself", Leader, "n1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, st, saves := emptyNodeAt(t, 5, tt.vote, tt.role)

			reply := askVote(t, n, "n3", 5)

			if reply.VoteGranted {
				t.Error("VoteGranted = true after voting for someone else this term, want false")
			}
			if reply.Term != 5 {
				t.Errorf("reply Term = %d, want 5", reply.Term)
			}
			got := n.Snapshot()
			if got.VotedFor != tt.vote || got.Role != tt.role {
				t.Errorf("state = votedFor %q, role %v; want %q, %v", got.VotedFor, got.Role, tt.vote, tt.role)
			}
			if st.SaveCount() != saves {
				t.Errorf("SaveCount() = %d, want %d (a denied vote must not write anything)", st.SaveCount(), saves)
			}
		})
	}
}

// A candidate's RequestVote can be delivered twice; the second one must get the same answer.
func TestRequestVote_VotedForDuplicateRequestRegrants(t *testing.T) {
	t.Run("repeat of a request already granted", func(t *testing.T) {
		n, _, _ := emptyNodeAt(t, 5, "", Follower)
		for i := 1; i <= 3; i++ {
			if reply := askVote(t, n, "n3", 5); !reply.VoteGranted {
				t.Fatalf("request #%d from n3: VoteGranted = false, want true", i)
			}
		}
		if got := n.Snapshot().VotedFor; got != "n3" {
			t.Errorf("VotedFor = %q, want n3", got)
		}
	})
	t.Run("duplicate request from the candidate already voted for", func(t *testing.T) {
		n, _, _ := emptyNodeAt(t, 5, "n2", Follower)
		if reply := askVote(t, n, "n2", 5); !reply.VoteGranted {
			t.Error("a duplicate request from the candidate already voted for: VoteGranted = false, want true")
		}
		if got := n.Snapshot().VotedFor; got != "n2" {
			t.Errorf("VotedFor = %q, want n2", got)
		}
	})
	t.Run("another candidate is still denied after the grant", func(t *testing.T) {
		n, _, _ := emptyNodeAt(t, 5, "", Follower)
		askVote(t, n, "n3", 5)
		if reply := askVote(t, n, "n4", 5); reply.VoteGranted {
			t.Error("n4 got a vote in a term where the node already voted for n3")
		}
		if reply := askVote(t, n, "n3", 5); !reply.VoteGranted {
			t.Error("n3's repeat request was denied after n4 was refused")
		}
		if got := n.Snapshot().VotedFor; got != "n3" {
			t.Errorf("VotedFor = %q, want n3", got)
		}
	})
}

// A new term starts with no vote cast, whoever the node voted for before.
func TestRequestVote_VotedForNewTermResets(t *testing.T) {
	tests := []struct {
		name      string
		role      Role
		oldVote   NodeID
		candidate NodeID
	}{
		{"follower, a different candidate", Follower, "n2", "n3"},
		{"follower, the candidate it voted for last term", Follower, "n2", "n2"},
		{"candidate that voted for itself", Candidate, "n1", "n3"},
		{"leader that voted for itself", Leader, "n1", "n3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, st, _ := emptyNodeAt(t, 5, tt.oldVote, tt.role)

			reply := askVote(t, n, tt.candidate, 6)

			if !reply.VoteGranted {
				t.Error("VoteGranted = false in a new term where no vote was cast, want true")
			}
			if reply.Term != 6 {
				t.Errorf("reply Term = %d, want 6", reply.Term)
			}
			got := n.Snapshot()
			if got.Term != 6 || got.VotedFor != tt.candidate || got.Role != Follower {
				t.Errorf("state = term %d, votedFor %q, role %v; want 6, %q, Follower", got.Term, got.VotedFor, got.Role, tt.candidate)
			}
			if s := lastSaved(t, st); s.CurrentTerm != 6 || s.VotedFor != tt.candidate {
				t.Errorf("saved term/votedFor = %d/%q, want 6/%q", s.CurrentTerm, s.VotedFor, tt.candidate)
			}
		})
	}
}

func TestRequestVote_VotedForNewTermAllowsOneVoteThere(t *testing.T) {
	n, _, _ := emptyNodeAt(t, 5, "n2", Follower)
	if !askVote(t, n, "n3", 6).VoteGranted {
		t.Fatal("first request in term 6 was denied")
	}
	if askVote(t, n, "n4", 6).VoteGranted {
		t.Error("a second candidate got a vote in term 6")
	}
	if !askVote(t, n, "n4", 7).VoteGranted {
		t.Error("n4 was denied in term 7, which is a fresh term")
	}
}

// The vote is saved, so a node that crashes right after granting cannot vote again in that term.
func TestRequestVote_VotedForSurvivesRestart(t *testing.T) {
	n, st, _ := emptyNodeAt(t, 5, "", Follower)
	if !askVote(t, n, "n3", 5).VoteGranted {
		t.Fatal("vote was not granted")
	}

	restarted := newTestNode(t, st)

	if got := restarted.Snapshot(); got.Term != 5 || got.VotedFor != "n3" {
		t.Fatalf("restarted state = term %d, votedFor %q; want 5, \"n3\"", got.Term, got.VotedFor)
	}
	if askVote(t, restarted, "n4", 5).VoteGranted {
		t.Error("restarted node voted twice in term 5")
	}
	if !askVote(t, restarted, "n3", 5).VoteGranted {
		t.Error("restarted node refused the candidate it had voted for")
	}
}
