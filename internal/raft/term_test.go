package raft

import "testing"

// nodeAt builds a node whose durable state is (term, votedFor, a three-entry log) and whose role is role.
// It returns the storage so a test can count the saves made after construction.
func nodeAt(t *testing.T, term Term, votedFor NodeID, role Role) (*Node, *MemStorage, int) {
	t.Helper()
	st := NewMemStorage()
	err := st.Save(PersistentState{
		CurrentTerm: term,
		VotedFor:    votedFor,
		Log: []Entry{
			{Term: 1, Index: 1, Cmd: []byte("a")},
			{Term: 2, Index: 2, Cmd: []byte("b")},
			{Term: 2, Index: 3, Cmd: []byte("c")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	n := newTestNode(t, st)
	n.role = role // the only way to start a core test as Candidate or Leader before elections exist
	return n, st, st.SaveCount()
}

func TestAdoptTermHigher(t *testing.T) {
	tests := []struct {
		name string
		role Role
		from Term
		to   Term
	}{
		{"follower, one higher", Follower, 5, 6},
		{"candidate, one higher", Candidate, 5, 6},
		{"leader, one higher", Leader, 5, 6},
		{"follower, far higher", Follower, 5, 100},
		{"leader, far higher", Leader, 5, 100},
		{"fresh node, term 0 to 1", Follower, 0, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, st, saves := nodeAt(t, tt.from, "n2", tt.role)

			if !n.adoptTermIfHigher(tt.to) {
				t.Fatalf("adoptTermIfHigher(%d) at term %d = false, want true", tt.to, tt.from)
			}

			got := n.Snapshot()
			if got.Term != tt.to {
				t.Errorf("Term = %d, want %d", got.Term, tt.to)
			}
			if got.VotedFor != "" {
				t.Errorf("VotedFor = %q, want it cleared", got.VotedFor)
			}
			if got.Role != Follower {
				t.Errorf("Role = %v, want Follower", got.Role)
			}

			if st.SaveCount() != saves+1 {
				t.Fatalf("SaveCount() = %d, want %d (exactly one save)", st.SaveCount(), saves+1)
			}
			saved := st.History()[saves]
			if saved.CurrentTerm != tt.to || saved.VotedFor != "" {
				t.Errorf("saved term/vote = %d/%q, want %d/\"\"", saved.CurrentTerm, saved.VotedFor, tt.to)
			}
			if !entriesEqual(saved.Log, got.Log) || len(saved.Log) != 3 {
				t.Errorf("saved log = %+v, want the node's full log %+v", saved.Log, got.Log)
			}
		})
	}
}

func TestAdoptTermNotHigher(t *testing.T) {
	tests := []struct {
		name string
		role Role
		term Term // the term the message carries; the node is at term 5
	}{
		{"follower, equal", Follower, 5},
		{"candidate, equal", Candidate, 5},
		{"leader, equal", Leader, 5},
		{"follower, one lower", Follower, 4},
		{"leader, one lower", Leader, 4},
		{"candidate, far lower", Candidate, 1},
		{"leader, term 0", Leader, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, st, saves := nodeAt(t, 5, "n2", tt.role)
			before := n.Snapshot()

			if n.adoptTermIfHigher(tt.term) {
				t.Fatalf("adoptTermIfHigher(%d) at term 5 = true, want false", tt.term)
			}

			after := n.Snapshot()
			if after.Term != 5 || after.VotedFor != "n2" || after.Role != tt.role {
				t.Errorf("state changed: term %d, votedFor %q, role %v; want 5, \"n2\", %v",
					after.Term, after.VotedFor, after.Role, tt.role)
			}
			if !entriesEqual(after.Log, before.Log) {
				t.Errorf("log changed: %+v, want %+v", after.Log, before.Log)
			}
			if st.SaveCount() != saves {
				t.Errorf("SaveCount() = %d, want %d (nothing may be persisted when the term is not higher)", st.SaveCount(), saves)
			}
		})
	}
}

func TestAdoptTermLeavesVolatileStateAlone(t *testing.T) {
	n, _, _ := nodeAt(t, 5, "n2", Leader)
	n.commitIndex = 2
	n.lastApplied = 1

	n.adoptTermIfHigher(9)

	got := n.Snapshot()
	if got.CommitIndex != 2 || got.LastApplied != 1 {
		t.Errorf("CommitIndex/LastApplied = %d/%d after a step-down, want 2/1 (a term change must not touch them)", got.CommitIndex, got.LastApplied)
	}
}

// The same higher term seen again (a second message from the new term) must not be saved twice.
func TestAdoptTermRepeatedIsPersistedOnce(t *testing.T) {
	n, st, saves := nodeAt(t, 5, "n2", Leader)

	if !n.adoptTermIfHigher(8) {
		t.Fatal("first adoptTermIfHigher(8) = false, want true")
	}
	if n.adoptTermIfHigher(8) {
		t.Error("second adoptTermIfHigher(8) = true, want false")
	}
	if st.SaveCount() != saves+1 {
		t.Errorf("SaveCount() = %d, want %d (one save for the one change)", st.SaveCount(), saves+1)
	}
}

func TestAdoptTermVoteInNewTermAfterReset(t *testing.T) {
	n, _, _ := nodeAt(t, 5, "n2", Follower)

	n.adoptTermIfHigher(6)
	n.votedFor = "n3" // a vote cast in term 6
	if n.adoptTermIfHigher(6) {
		t.Fatal("adoptTermIfHigher(6) at term 6 = true, want false")
	}
	if got := n.Snapshot().VotedFor; got != "n3" {
		t.Errorf("VotedFor = %q, want n3 (an equal term must keep the vote)", got)
	}

	n.adoptTermIfHigher(7)
	if got := n.Snapshot().VotedFor; got != "" {
		t.Errorf("VotedFor = %q after moving to term 7, want it cleared", got)
	}
}
