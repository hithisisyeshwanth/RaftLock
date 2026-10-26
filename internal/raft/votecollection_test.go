package raft

import "testing"

// candidateNode builds n1 at term 5 with the given peers and lets its election timer run out,
// so it is a real candidate in term 6 that has voted for itself.
func candidateNode(t *testing.T, peers ...NodeID) (*Node, *MemStorage) {
	t.Helper()
	st := NewMemStorage()
	if err := st.Save(PersistentState{CurrentTerm: 5}); err != nil {
		t.Fatal(err)
	}
	n, err := NewNode(DefaultConfig(), "n1", peers, st, nil, NewFixedTimeouts(150), func(Entry) {})
	if err != nil {
		t.Fatal(err)
	}
	tick(n, 150)
	if got := n.Snapshot(); got.Role != Candidate || got.Term != 6 {
		t.Fatalf("setup: role %v, term %d; want Candidate in term 6", got.Role, got.Term)
	}
	return n, st
}

func voteReply(n *Node, from NodeID, term Term, granted bool) []Message {
	return n.Step(Message{From: from, To: n.id, Body: RequestVoteReply{Term: term, VoteGranted: granted}})
}

func wantRole(t *testing.T, n *Node, role Role, term Term) {
	t.Helper()
	if got := n.Snapshot(); got.Role != role || got.Term != term {
		t.Fatalf("role %v, term %d; want %v, %d", got.Role, got.Term, role, term)
	}
}

// Slide 3: votes are a set of node IDs, including the candidate's own, and a majority of the cluster wins.
func TestVoteCollection_SelfPlusOneOfThreeWins(t *testing.T) {
	n, _ := candidateNode(t, "n2", "n3")
	voteReply(n, "n2", 6, true)
	wantRole(t, n, Leader, 6)
}

func TestVoteCollection_DenialDoesNotCount(t *testing.T) {
	n, _ := candidateNode(t, "n2", "n3")

	if out := voteReply(n, "n2", 6, false); len(out) != 0 {
		t.Errorf("a denial produced messages %+v, want none", out)
	}
	wantRole(t, n, Candidate, 6)
	voteReply(n, "n3", 6, false)
	wantRole(t, n, Candidate, 6)
}

func TestVoteCollection_GrantAfterDenialWins(t *testing.T) {
	n, _ := candidateNode(t, "n2", "n3")
	voteReply(n, "n2", 6, false)
	voteReply(n, "n3", 6, true)
	wantRole(t, n, Leader, 6)
}

// With five nodes a majority is three: self plus two distinct grants.
func TestVoteCollection_FiveNodeMajority(t *testing.T) {
	t.Run("two grants are not enough, the third wins", func(t *testing.T) {
		n, _ := candidateNode(t, "n2", "n3", "n4", "n5")
		voteReply(n, "n2", 6, true)
		wantRole(t, n, Candidate, 6)
		voteReply(n, "n3", 6, true)
		wantRole(t, n, Leader, 6)
	})
	t.Run("denials in between do not count", func(t *testing.T) {
		n, _ := candidateNode(t, "n2", "n3", "n4", "n5")
		voteReply(n, "n2", 6, true)
		voteReply(n, "n3", 6, false)
		voteReply(n, "n4", 6, false)
		wantRole(t, n, Candidate, 6)
		voteReply(n, "n5", 6, true)
		wantRole(t, n, Leader, 6)
	})
}

// A duplicated reply from one node must not count as a second vote.
func TestVoteCollection_DuplicateGrantCountsOnce(t *testing.T) {
	n, _ := candidateNode(t, "n2", "n3", "n4", "n5")
	voteReply(n, "n2", 6, true)
	voteReply(n, "n2", 6, true)
	voteReply(n, "n2", 6, true)
	wantRole(t, n, Candidate, 6)

	voteReply(n, "n3", 6, true)
	wantRole(t, n, Leader, 6)
}

// A vote from someone outside the cluster must never help form a majority.
func TestVoteCollection_GrantFromNonPeerIgnored(t *testing.T) {
	n, _ := candidateNode(t, "n2", "n3")
	voteReply(n, "zz", 6, true)
	voteReply(n, "n1", 6, true) // a reply claiming to come from the candidate itself
	wantRole(t, n, Candidate, 6)
}

func TestVoteCollection_StaleTermIgnored(t *testing.T) {
	t.Run("grant from an earlier term", func(t *testing.T) {
		n, _ := candidateNode(t, "n2", "n3")
		if out := voteReply(n, "n2", 5, true); len(out) != 0 {
			t.Errorf("a stale reply produced messages %+v, want none", out)
		}
		wantRole(t, n, Candidate, 6)
	})
	t.Run("votes from the previous election do not carry into the next", func(t *testing.T) {
		n, _ := candidateNode(t, "n2", "n3", "n4", "n5")
		voteReply(n, "n2", 6, true)
		tick(n, 150) // times out again: candidate in term 7
		wantRole(t, n, Candidate, 7)

		voteReply(n, "n3", 7, true) // self + n3 is two of five; n2's vote was for term 6
		wantRole(t, n, Candidate, 7)
		voteReply(n, "n2", 6, true) // a late, duplicated term-6 grant
		wantRole(t, n, Candidate, 7)
		voteReply(n, "n4", 7, true)
		wantRole(t, n, Leader, 7)
	})
}

// Figure 2, all servers: a reply carrying a higher term turns the candidate into a follower in that term.
func TestVoteCollection_HigherTermStepsDown(t *testing.T) {
	n, st := candidateNode(t, "n2", "n3")
	saves := st.SaveCount()

	if out := voteReply(n, "n2", 9, false); len(out) != 0 {
		t.Errorf("step-down produced messages %+v, want none", out)
	}

	got := n.Snapshot()
	if got.Role != Follower || got.Term != 9 || got.VotedFor != "" {
		t.Fatalf("state = role %v, term %d, votedFor %q; want Follower, 9, \"\"", got.Role, got.Term, got.VotedFor)
	}
	if st.SaveCount() != saves+1 {
		t.Errorf("SaveCount() = %d, want %d (the new term is saved)", st.SaveCount(), saves+1)
	}

	voteReply(n, "n3", 6, true) // its old election's grant arrives late
	wantRole(t, n, Follower, 9)
}

// Only a candidate collects votes.
func TestVoteCollection_IgnoredByFollowerAndLeader(t *testing.T) {
	t.Run("follower", func(t *testing.T) {
		n, _, _ := emptyNodeAt(t, 6, "", Follower)
		if out := voteReply(n, "n2", 6, true); len(out) != 0 {
			t.Errorf("follower produced messages %+v, want none", out)
		}
		voteReply(n, "n3", 6, true)
		wantRole(t, n, Follower, 6)
	})
	t.Run("leader", func(t *testing.T) {
		n, _ := candidateNode(t, "n2", "n3", "n4", "n5")
		voteReply(n, "n2", 6, true)
		voteReply(n, "n3", 6, true)
		wantRole(t, n, Leader, 6)

		for _, r := range []struct {
			from    NodeID
			granted bool
		}{{"n4", true}, {"n5", false}, {"n2", true}} {
			if out := voteReply(n, r.from, 6, r.granted); len(out) != 0 {
				t.Errorf("leader produced messages %+v for a late vote reply, want none", out)
			}
		}
		wantRole(t, n, Leader, 6)
	})
}
