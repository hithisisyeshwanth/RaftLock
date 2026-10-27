package raft

import "testing"

// leaderWithFourEntries is n1, leader of term 6, with log
// 1:(t1,"a") 2:(t3,"b") 3:(t6,"x") 4:(t6,"y") and commitIndex 2.
func leaderWithFourEntries(t *testing.T) *Node {
	t.Helper()
	n, _, _ := electedLeader(t, "n2", "n3")
	n.Propose([]byte("x"))
	n.Propose([]byte("y"))
	n.commitIndex = 2 // no commit logic yet (T4.3); set so LeaderCommit can be checked
	return n
}

// Slide 5, Figure 2 leader rules: send the entries from nextIndex on, anchored at the entry just before it.
func TestBuildAppendEntries(t *testing.T) {
	log := []Entry{
		{Term: 1, Index: 1, Cmd: []byte("a")},
		{Term: 3, Index: 2, Cmd: []byte("b")},
		{Term: 6, Index: 3, Cmd: []byte("x")},
		{Term: 6, Index: 4, Cmd: []byte("y")},
	}
	tests := []struct {
		name     string
		next     Index
		prev     Index
		prevTerm Term
		entries  []Entry
	}{
		{"nextIndex past the end: empty heartbeat", 5, 4, 6, nil},
		{"nextIndex 1: everything, anchored at the sentinel", 1, 0, 0, log},
		{"nextIndex 2", 2, 1, 1, log[1:]},
		{"nextIndex 3: prev entry is from an older term", 3, 2, 3, log[2:]},
		{"nextIndex 4: one entry", 4, 3, 6, log[3:]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := leaderWithFourEntries(t)
			n.nextIndex["n2"] = tt.next

			got := n.buildAppendEntries("n2")

			if got.Term != 6 || got.LeaderID != "n1" || got.LeaderCommit != 2 {
				t.Errorf("Term/LeaderID/LeaderCommit = %d/%q/%d, want 6/\"n1\"/2", got.Term, got.LeaderID, got.LeaderCommit)
			}
			if got.PrevLogIndex != tt.prev || got.PrevLogTerm != tt.prevTerm {
				t.Errorf("prev = (%d, %d), want (%d, %d)", got.PrevLogIndex, got.PrevLogTerm, tt.prev, tt.prevTerm)
			}
			if !entriesEqual(got.Entries, tt.entries) {
				t.Errorf("Entries = %+v, want %+v", got.Entries, tt.entries)
			}
		})
	}
}

// Each peer gets args built from its own nextIndex.
func TestBuildAppendEntriesPerPeer(t *testing.T) {
	n := leaderWithFourEntries(t)
	n.nextIndex["n2"] = 1
	n.nextIndex["n3"] = 5

	if got := n.buildAppendEntries("n2"); got.PrevLogIndex != 0 || len(got.Entries) != 4 {
		t.Errorf("n2 (nextIndex 1): prev %d with %d entries, want prev 0 with 4", got.PrevLogIndex, len(got.Entries))
	}
	if got := n.buildAppendEntries("n3"); got.PrevLogIndex != 4 || len(got.Entries) != 0 {
		t.Errorf("n3 (nextIndex 5): prev %d with %d entries, want prev 4 with 0", got.PrevLogIndex, len(got.Entries))
	}
}

// The args travel through the network and are read by another node, so they must not share the leader's log.
func TestBuildAppendEntriesCopiesEntries(t *testing.T) {
	n := leaderWithFourEntries(t)
	n.nextIndex["n2"] = 1

	got := n.buildAppendEntries("n2")
	got.Entries[0].Term = 99
	got.Entries[1].Cmd[0] = 'Z'

	log := n.Snapshot().Log
	if log[0].Term != 1 || string(log[1].Cmd) != "b" {
		t.Errorf("leader log changed after editing the built args: %+v", log)
	}
}

func TestBuildAppendEntriesLeaderCommitTracksCommitIndex(t *testing.T) {
	n := leaderWithFourEntries(t)
	for _, ci := range []Index{0, 3, 4} {
		n.commitIndex = ci
		if got := n.buildAppendEntries("n2").LeaderCommit; got != ci {
			t.Errorf("commitIndex %d: LeaderCommit = %d", ci, got)
		}
	}
}
