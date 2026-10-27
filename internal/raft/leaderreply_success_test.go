package raft

import "testing"

func aeSuccess(n *Node, from NodeID, term Term, match, prev Index) []Message {
	return n.Step(Message{From: from, To: n.id, Body: AppendEntriesReply{Term: term, Success: true, MatchIndex: match, PrevLogIndex: prev}})
}

func wantProgress(t *testing.T, n *Node, peer NodeID, match, next Index) {
	t.Helper()
	if got := n.matchIndex[peer]; got != match {
		t.Errorf("matchIndex[%s] = %d, want %d", peer, got, match)
	}
	if got := n.nextIndex[peer]; got != next {
		t.Errorf("nextIndex[%s] = %d, want %d", peer, got, next)
	}
}

// Slide 8: a success reply proves the follower holds everything up to MatchIndex,
// so the leader records that and sends from the entry after it next time.
// The leader here (term 6, last index 4) starts every peer at nextIndex 3, matchIndex 0.
func TestLeaderReply_SuccessAdvances(t *testing.T) {
	tests := []struct {
		name  string
		match Index
		prev  Index
	}{
		{"all four entries", 4, 2},
		{"heartbeat confirms up to 2", 2, 2},
		{"part of the log", 3, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := leaderWithFourEntries(t)
			aeSuccess(n, "n2", 6, tt.match, tt.prev)
			wantProgress(t, n, "n2", tt.match, tt.match+1)
			wantProgress(t, n, "n3", 0, 3) // another peer's reply touches only that peer
		})
	}
}

func TestLeaderReply_SuccessInOrder(t *testing.T) {
	n := leaderWithFourEntries(t)
	aeSuccess(n, "n2", 6, 2, 2)
	aeSuccess(n, "n2", 6, 3, 2)
	aeSuccess(n, "n2", 6, 4, 3)
	wantProgress(t, n, "n2", 4, 5)
}

// Replies are one-way and may arrive late, twice, or out of order. A late reply describes
// an older state of the follower's log, so it must never move matchIndex backwards.
func TestLeaderReply_SuccessMatchIndexNeverDecreases(t *testing.T) {
	tests := []struct {
		name    string
		replies []Index // MatchIndex of each success reply from n2, in arrival order
	}{
		{"reordered: newer reply first", []Index{4, 2}},
		{"duplicated", []Index{4, 4, 4}},
		{"old reply between new ones", []Index{3, 4, 2, 3}},
		{"late heartbeat reply at 0", []Index{4, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := leaderWithFourEntries(t)
			for _, m := range tt.replies {
				aeSuccess(n, "n2", 6, m, 2)
			}
			wantProgress(t, n, "n2", 4, 5)
		})
	}
}

// Only replies from real peers count: a stray or forged reply must not add progress for anyone.
func TestLeaderReply_SuccessFromNonPeerIgnored(t *testing.T) {
	n := leaderWithFourEntries(t)
	selfMatch := n.matchIndex["n1"]

	aeSuccess(n, "zz", 6, 4, 2)
	aeSuccess(n, "n1", 6, 1, 0) // a reply claiming to come from the leader itself

	if _, ok := n.matchIndex["zz"]; ok {
		t.Error("matchIndex gained an entry for zz, which is not a peer")
	}
	if _, ok := n.nextIndex["zz"]; ok {
		t.Error("nextIndex gained an entry for zz, which is not a peer")
	}
	if got := n.matchIndex["n1"]; got != selfMatch {
		t.Errorf("matchIndex[n1] = %d after a reply from n1, want it unchanged at %d", got, selfMatch)
	}
	wantProgress(t, n, "n2", 0, 3)
	wantProgress(t, n, "n3", 0, 3)
}

// A reply from an earlier term answers a request this leader did not send in its current term.
func TestLeaderReply_SuccessStaleTermIgnored(t *testing.T) {
	n := leaderWithFourEntries(t)
	aeSuccess(n, "n2", 5, 4, 2)
	wantProgress(t, n, "n2", 0, 3)
	wantRole(t, n, Leader, 6)
}

// Only a leader tracks progress; a success reply anywhere else changes nothing and must not panic.
func TestLeaderReply_SuccessAtNonLeaderIgnored(t *testing.T) {
	t.Run("follower", func(t *testing.T) {
		n, _, _ := emptyNodeAt(t, 6, "", Follower)
		if out := aeSuccess(n, "n2", 6, 4, 2); len(out) != 0 {
			t.Errorf("follower sent %+v, want nothing", out)
		}
		if len(n.matchIndex) != 0 || len(n.nextIndex) != 0 {
			t.Errorf("follower has progress maps %v / %v, want none", n.matchIndex, n.nextIndex)
		}
		wantRole(t, n, Follower, 6)
	})
	t.Run("candidate", func(t *testing.T) {
		n, _ := candidateNode(t, "n2", "n3")
		if out := aeSuccess(n, "n2", 6, 4, 2); len(out) != 0 {
			t.Errorf("candidate sent %+v, want nothing", out)
		}
		wantRole(t, n, Candidate, 6)
	})
	t.Run("deposed leader", func(t *testing.T) {
		n := leaderWithFourEntries(t)
		n.Step(Message{From: "n3", To: "n1", Body: AppendEntriesReply{Term: 9}}) // steps down to term 9
		wantRole(t, n, Follower, 9)

		aeSuccess(n, "n2", 6, 4, 2) // its term-6 replies are still in flight
		aeSuccess(n, "n2", 9, 4, 2)
		wantRole(t, n, Follower, 9)
		if got := n.matchIndex["n2"]; got != 0 {
			t.Errorf("matchIndex[n2] = %d after stepping down, want it untouched at 0", got)
		}
	})
}

func TestLeaderReply_SuccessHigherTermStepsDown(t *testing.T) {
	n := leaderWithFourEntries(t)
	n.Step(Message{From: "n2", To: "n1", Body: AppendEntriesReply{Term: 8, Success: false, PrevLogIndex: 2}})
	got := n.Snapshot()
	if got.Role != Follower || got.Term != 8 || got.VotedFor != "" {
		t.Errorf("state = role %v, term %d, votedFor %q; want Follower, 8, \"\"", got.Role, got.Term, got.VotedFor)
	}
}
