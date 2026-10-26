package raft

import (
	"errors"
	"testing"
)

var errDiskFull = errors.New("spy: disk full")

// spyStorage wraps MemStorage and fails chosen Save calls. Calls are numbered 1, 2, 3, ...
// from the moment the spy is built, so setup saves made before then never count.
type spyStorage struct {
	*MemStorage
	calls  int
	failOn map[int]bool
}

func (s *spyStorage) Save(st PersistentState) error {
	s.calls++
	if s.failOn[s.calls] {
		return errDiskFull
	}
	return s.MemStorage.Save(st)
}

// spyNode builds n1 from stored state with the given role; the listed Save calls fail.
func spyNode(t *testing.T, stored PersistentState, role Role, failOn ...int) (*Node, *spyStorage) {
	t.Helper()
	mem := NewMemStorage()
	if err := mem.Save(stored); err != nil {
		t.Fatal(err)
	}
	spy := &spyStorage{MemStorage: mem, failOn: map[int]bool{}}
	for _, c := range failOn {
		spy.failOn[c] = true
	}
	n := newTestNode(t, spy)
	n.role = role
	return n, spy
}

func requestVote(n *Node, candidate NodeID, term Term) []Message {
	return n.Step(Message{From: candidate, To: n.id, Body: RequestVoteArgs{Term: term, CandidateID: candidate}})
}

// When Step returns a granted vote, storage already holds that vote: the reply never leaves first.
func TestPersistBeforeReply_VoteGranted(t *testing.T) {
	n, spy := spyNode(t, PersistentState{CurrentTerm: 5}, Follower)

	out := requestVote(n, "n3", 5)

	if len(out) != 1 || !out[0].Body.(RequestVoteReply).VoteGranted {
		t.Fatalf("Step() = %+v, want one granted vote", out)
	}
	if s := lastSaved(t, spy.MemStorage); s.CurrentTerm != 5 || s.VotedFor != "n3" {
		t.Errorf("storage holds term/votedFor = %d/%q when the reply is returned, want 5/\"n3\"", s.CurrentTerm, s.VotedFor)
	}
}

func TestPersistBeforeReply_TermAdopted(t *testing.T) {
	// The candidate's log is stale, so this request only changes the term.
	n, spy := spyNode(t, PersistentState{CurrentTerm: 5, Log: []Entry{{Term: 5, Index: 1}}}, Leader)

	out := requestVote(n, "n3", 8)

	if len(out) != 1 {
		t.Fatalf("Step() returned %d messages, want 1", len(out))
	}
	reply := out[0].Body.(RequestVoteReply)
	if reply.Term != 8 || reply.VoteGranted {
		t.Fatalf("reply = %+v, want term 8, not granted", reply)
	}
	if s := lastSaved(t, spy.MemStorage); s.CurrentTerm != 8 || s.VotedFor != "" {
		t.Errorf("storage holds term/votedFor = %d/%q when the reply is returned, want 8/\"\"", s.CurrentTerm, s.VotedFor)
	}
}

// If the vote cannot be saved, no reply leaves and the vote is not granted, not even in memory.
func TestPersistBeforeReply_VoteSaveFails(t *testing.T) {
	n, _ := spyNode(t, PersistentState{CurrentTerm: 5}, Follower, 1)

	out := requestVote(n, "n3", 5)

	if len(out) != 0 {
		t.Errorf("Step() returned %+v after a failed Save, want no messages", out)
	}
	if got := n.Snapshot(); got.VotedFor != "" || got.Term != 5 {
		t.Errorf("state = term %d, votedFor %q; want 5, \"\" (memory must match disk)", got.Term, got.VotedFor)
	}
}

// If a higher term cannot be saved, no reply leaves and the node keeps its old term and role.
func TestPersistBeforeReply_TermSaveFails(t *testing.T) {
	tests := []struct {
		name string
		role Role
		log  []Entry
	}{
		{"leader, stale candidate log (term change only)", Leader, []Entry{{Term: 5, Index: 1}}},
		{"follower, up-to-date candidate (term change then vote)", Follower, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, _ := spyNode(t, PersistentState{CurrentTerm: 5, VotedFor: "n2", Log: tt.log}, tt.role, 1)

			out := requestVote(n, "n3", 8)

			if len(out) != 0 {
				t.Errorf("Step() returned %+v after a failed Save, want no messages", out)
			}
			got := n.Snapshot()
			if got.Term != 5 || got.VotedFor != "n2" || got.Role != tt.role {
				t.Errorf("state = term %d, votedFor %q, role %v; want 5, \"n2\", %v (unchanged)", got.Term, got.VotedFor, got.Role, tt.role)
			}
		})
	}
}

// The term is saved, then the vote's save fails: no reply, the new term stands, no vote is cast.
func TestPersistBeforeReply_TermSavedVoteSaveFails(t *testing.T) {
	n, spy := spyNode(t, PersistentState{CurrentTerm: 5, VotedFor: "n2"}, Follower, 2)

	out := requestVote(n, "n3", 8)

	if len(out) != 0 {
		t.Errorf("Step() returned %+v after the vote's Save failed, want no messages", out)
	}
	got := n.Snapshot()
	if got.Term != 8 || got.VotedFor != "" {
		t.Errorf("state = term %d, votedFor %q; want 8, \"\" (the saved term, no vote)", got.Term, got.VotedFor)
	}
	if s := lastSaved(t, spy.MemStorage); s.CurrentTerm != 8 || s.VotedFor != "" {
		t.Errorf("storage holds term/votedFor = %d/%q, want 8/\"\"", s.CurrentTerm, s.VotedFor)
	}
}

// A request that needs no write still gets its reply even when storage is failing.
func TestPersistBeforeReply_NoSaveNeededStillReplies(t *testing.T) {
	t.Run("stale term is denied", func(t *testing.T) {
		n, spy := spyNode(t, PersistentState{CurrentTerm: 5}, Follower, 1, 2, 3)
		out := requestVote(n, "n3", 4)
		if len(out) != 1 || out[0].Body.(RequestVoteReply).VoteGranted {
			t.Errorf("Step() = %+v, want one denial", out)
		}
		if spy.calls != 0 {
			t.Errorf("Save was called %d times for a stale request, want 0", spy.calls)
		}
	})
	t.Run("repeat request from the candidate already voted for is re-granted", func(t *testing.T) {
		n, spy := spyNode(t, PersistentState{CurrentTerm: 5, VotedFor: "n3"}, Follower, 1, 2, 3)
		out := requestVote(n, "n3", 5)
		if len(out) != 1 || !out[0].Body.(RequestVoteReply).VoteGranted {
			t.Errorf("Step() = %+v, want one grant", out)
		}
		if spy.calls != 0 {
			t.Errorf("Save was called %d times for a repeat grant, want 0", spy.calls)
		}
	})
}

// A failure leaves nothing half-done, so the same request succeeds once storage recovers.
func TestPersistBeforeReply_RetryAfterFailure(t *testing.T) {
	n, _ := spyNode(t, PersistentState{CurrentTerm: 5}, Follower, 1)

	if out := requestVote(n, "n3", 5); len(out) != 0 {
		t.Fatalf("first Step() = %+v, want no messages (Save failed)", out)
	}
	out := requestVote(n, "n3", 5)
	if len(out) != 1 || !out[0].Body.(RequestVoteReply).VoteGranted {
		t.Errorf("retry Step() = %+v, want one grant", out)
	}
	if got := n.Snapshot().VotedFor; got != "n3" {
		t.Errorf("VotedFor = %q after the retry, want n3", got)
	}
}
