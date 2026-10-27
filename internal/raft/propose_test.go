package raft

import "testing"

// Slide 4: a leader appends the command to its own log, stamped with its current term, and saves it.
func TestPropose_LeaderAppends(t *testing.T) {
	n, _, st := electedLeader(t, "n2", "n3") // term 6, log terms (1, 3)
	saves := st.SaveCount()

	res, _ := n.Propose([]byte("x"))

	want := ProposeResult{Index: 3, Term: 6, IsLeader: true}
	if res != want {
		t.Fatalf("Propose = %+v, want %+v", res, want)
	}
	log := n.Snapshot().Log
	if len(log) != 3 || !log[2].Equal(Entry{Term: 6, Index: 3, Cmd: []byte("x")}) {
		t.Fatalf("log = %+v, want entry 3 = {term 6, cmd x} appended", log)
	}
	if st.SaveCount() != saves+1 {
		t.Errorf("SaveCount() = %d, want %d (the new entry must be saved)", st.SaveCount(), saves+1)
	}
	if s := lastSaved(t, st); !entriesEqual(s.Log, log) || s.CurrentTerm != 6 {
		t.Errorf("saved term %d, log %+v; want term 6 and the log with entry 3", s.CurrentTerm, s.Log)
	}
	if got := n.matchIndex["n1"]; got != 3 {
		t.Errorf("matchIndex[n1] = %d, want 3 (the leader holds its own entry)", got)
	}
}

func TestPropose_LeaderAppendsConsecutively(t *testing.T) {
	n, _, _ := electedLeader(t, "n2", "n3")

	for i, cmd := range []string{"x", "y", "z"} {
		res, _ := n.Propose([]byte(cmd))
		if want := Index(3 + i); res.Index != want || res.Term != 6 || !res.IsLeader {
			t.Errorf("Propose(%q) = %+v, want index %d, term 6, leader", cmd, res, want)
		}
	}
	if got := n.matchIndex["n1"]; got != 5 {
		t.Errorf("matchIndex[n1] = %d after three proposals, want 5", got)
	}
	if got := len(n.Snapshot().Log); got != 5 {
		t.Errorf("log length = %d, want 5", got)
	}
}

// The caller may reuse its buffer, so the log must keep its own copy of the command.
func TestPropose_LeaderCopiesCommand(t *testing.T) {
	n, _, st := electedLeader(t, "n2", "n3")
	cmd := []byte("x")

	n.Propose(cmd)
	cmd[0] = 'Q'

	if got := string(n.Snapshot().Log[2].Cmd); got != "x" {
		t.Errorf("log entry 3 holds %q after the caller changed its buffer, want \"x\"", got)
	}
	if got := string(lastSaved(t, st).Log[2].Cmd); got != "x" {
		t.Errorf("saved entry 3 holds %q, want \"x\"", got)
	}
}

// CLAUDE.md rule 2: if persisting fails, nothing is sent, and memory must not run ahead of disk.
func TestPropose_LeaderSaveFails(t *testing.T) {
	n, _, _ := electedLeader(t, "n2", "n3")
	mem := NewMemStorage()
	mem.Save(PersistentState{CurrentTerm: 6, VotedFor: "n1", Log: n.Snapshot().Log})
	n.storage = &spyStorage{MemStorage: mem, failOn: map[int]bool{1: true}}

	_, out := n.Propose([]byte("x"))

	if len(out) != 0 {
		t.Errorf("Propose sent %+v after a failed save, want nothing", out)
	}
	if got := len(n.Snapshot().Log); got != 2 {
		t.Errorf("log length = %d after a failed save, want 2 (entry not appended)", got)
	}
	if got := n.matchIndex["n1"]; got != 0 {
		t.Errorf("matchIndex[n1] = %d after a failed save, want 0", got)
	}
}

// A follower cannot append; it points the client at the leader it knows of.
func TestPropose_FollowerReturnsHint(t *testing.T) {
	tests := []struct {
		name  string
		setup func(n *Node)
		hint  NodeID
	}{
		{"knows the leader of its term", func(n *Node) { heartbeat(n, "n2", 5) }, "n2"},
		{"has not heard from any leader", func(n *Node) {}, ""},
		{"moved to a newer term with no leader yet", func(n *Node) {
			heartbeat(n, "n2", 5)
			n.Step(Message{From: "n3", To: "n1", Body: RequestVoteArgs{Term: 7, CandidateID: "n3"}})
		}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, st, _ := emptyNodeAt(t, 5, "", Follower)
			tt.setup(n)
			saves := st.SaveCount()

			res, out := n.Propose([]byte("x"))

			if res.IsLeader || res.LeaderHint != tt.hint {
				t.Errorf("Propose = %+v, want IsLeader false, LeaderHint %q", res, tt.hint)
			}
			if len(out) != 0 {
				t.Errorf("a follower's Propose sent %+v, want nothing", out)
			}
			if got := len(n.Snapshot().Log); got != 0 {
				t.Errorf("follower log length = %d, want 0 (nothing appended)", got)
			}
			if st.SaveCount() != saves {
				t.Errorf("SaveCount() = %d, want %d (a follower's Propose writes nothing)", st.SaveCount(), saves)
			}
		})
	}
}

// A candidate does not know who will lead its term, so it gives no hint, not even a stale one.
func TestPropose_CandidateNoHint(t *testing.T) {
	n, _, st := timerNode(t, NewFixedTimeouts(150), PersistentState{CurrentTerm: 5}, Follower)
	heartbeat(n, "n2", 5) // learns of n2, the leader of term 5
	tick(n, 150)          // hears nothing more and starts an election for term 6
	wantRole(t, n, Candidate, 6)
	saves := st.(*MemStorage).SaveCount()

	res, out := n.Propose([]byte("x"))

	if res.IsLeader || res.LeaderHint != "" {
		t.Errorf("Propose = %+v, want IsLeader false and no hint", res)
	}
	if len(out) != 0 || len(n.Snapshot().Log) != 0 || st.(*MemStorage).SaveCount() != saves {
		t.Errorf("a candidate's Propose sent %d messages, appended or saved; want nothing", len(out))
	}
}
