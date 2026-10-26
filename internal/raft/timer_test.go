package raft

import "testing"

const expiryKind = "election-timeout"

// seqTimeouts returns the given timeouts in order, then repeats the last one,
// so a test can see whether a reset drew a new timeout.
type seqTimeouts struct{ vals []int }

func (s *seqTimeouts) Next() int {
	v := s.vals[0]
	if len(s.vals) > 1 {
		s.vals = s.vals[1:]
	}
	return v
}

// timerNode builds n1 from stored state with the given role and a recording tracer.
func timerNode(t *testing.T, timeouts TimeoutSource, stored PersistentState, role Role) (*Node, *Recorder, Storage) {
	t.Helper()
	st := NewMemStorage()
	if err := st.Save(stored); err != nil {
		t.Fatal(err)
	}
	rec := NewRecorder()
	n, err := NewNode(DefaultConfig(), "n1", []NodeID{"n2", "n3"}, st, rec, timeouts, func(Entry) {})
	if err != nil {
		t.Fatal(err)
	}
	n.role = role
	return n, rec, st
}

func tick(n *Node, k int) {
	for i := 0; i < k; i++ {
		n.Tick()
	}
}

func expiries(rec *Recorder) int {
	c := 0
	for _, e := range rec.Events() {
		if e.Kind == expiryKind && e.Node == "n1" {
			c++
		}
	}
	return c
}

// A node draws its first timeout when it is built, and reports expiry on the tick that reaches it.
func TestElectionTimer_ExpiresAfterTimeout(t *testing.T) {
	for _, role := range []Role{Follower, Candidate} {
		t.Run(role.String(), func(t *testing.T) {
			n, rec, _ := timerNode(t, NewFixedTimeouts(150), PersistentState{CurrentTerm: 5}, role)

			tick(n, 149)
			if got := expiries(rec); got != 0 {
				t.Fatalf("expiry reported %d times after 149 of 150 ticks, want 0", got)
			}
			tick(n, 1)
			if got := expiries(rec); got != 1 {
				t.Errorf("expiry reported %d times on tick 150, want 1", got)
			}
		})
	}
}

func TestElectionTimer_LeaderHasNoTimer(t *testing.T) {
	n, rec, _ := timerNode(t, NewFixedTimeouts(150), PersistentState{CurrentTerm: 5, VotedFor: "n1"}, Leader)

	tick(n, 1000)

	if got := expiries(rec); got != 0 {
		t.Errorf("a leader reported election expiry %d times in 1000 ticks, want 0", got)
	}
}

// Figure 2: a follower converts to candidate only if the timeout elapses without granting a vote.
func TestElectionTimer_ResetsOnGrantedVote(t *testing.T) {
	tests := []struct {
		name   string
		stored PersistentState
		args   RequestVoteArgs
	}{
		{"first vote in the term", PersistentState{CurrentTerm: 5}, RequestVoteArgs{Term: 5, CandidateID: "n3"}},
		{"vote in a higher term", PersistentState{CurrentTerm: 5, VotedFor: "n2"}, RequestVoteArgs{Term: 6, CandidateID: "n3"}},
		{"duplicate request re-granted", PersistentState{CurrentTerm: 5, VotedFor: "n3"}, RequestVoteArgs{Term: 5, CandidateID: "n3"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, rec, _ := timerNode(t, NewFixedTimeouts(150), tt.stored, Follower)

			tick(n, 100)
			out := n.Step(Message{From: tt.args.CandidateID, To: "n1", Body: tt.args})
			if len(out) != 1 || !out[0].Body.(RequestVoteReply).VoteGranted {
				t.Fatalf("setup: vote was not granted: %+v", out)
			}

			tick(n, 149) // tick 249: well past the original deadline at 150
			if got := expiries(rec); got != 0 {
				t.Fatalf("expiry reported %d times 149 ticks after granting a vote, want 0 (the grant resets the timer)", got)
			}
			tick(n, 1)
			if got := expiries(rec); got != 1 {
				t.Errorf("expiry reported %d times 150 ticks after the grant, want 1", got)
			}
		})
	}
}

func TestElectionTimer_NotResetOnDeniedVote(t *testing.T) {
	tests := []struct {
		name   string
		stored PersistentState
		args   RequestVoteArgs
	}{
		{"stale term", PersistentState{CurrentTerm: 5}, RequestVoteArgs{Term: 4, CandidateID: "n3"}},
		{"already voted for another candidate", PersistentState{CurrentTerm: 5, VotedFor: "n2"}, RequestVoteArgs{Term: 5, CandidateID: "n3"}},
		{"candidate log not up to date", PersistentState{CurrentTerm: 5, Log: []Entry{{Term: 5, Index: 1}}}, RequestVoteArgs{Term: 5, CandidateID: "n3"}},
		{"higher term adopted but log not up to date", PersistentState{CurrentTerm: 5, Log: []Entry{{Term: 5, Index: 1}}}, RequestVoteArgs{Term: 9, CandidateID: "n3"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, rec, _ := timerNode(t, NewFixedTimeouts(150), tt.stored, Follower)

			tick(n, 100)
			out := n.Step(Message{From: tt.args.CandidateID, To: "n1", Body: tt.args})
			if len(out) != 1 || out[0].Body.(RequestVoteReply).VoteGranted {
				t.Fatalf("setup: vote was not denied: %+v", out)
			}

			tick(n, 50) // tick 150: the original deadline
			if got := expiries(rec); got != 1 {
				t.Errorf("expiry reported %d times at the original deadline after a denial, want 1 (a denial must not reset the timer)", got)
			}
		})
	}
}

// A vote whose Save fails is not granted, so it must not reset the timer either.
func TestElectionTimer_NotResetWhenVoteSaveFails(t *testing.T) {
	mem := NewMemStorage()
	mem.Save(PersistentState{CurrentTerm: 5})
	spy := &spyStorage{MemStorage: mem, failOn: map[int]bool{1: true}}
	rec := NewRecorder()
	n, err := NewNode(DefaultConfig(), "n1", []NodeID{"n2", "n3"}, spy, rec, NewFixedTimeouts(150), func(Entry) {})
	if err != nil {
		t.Fatal(err)
	}

	tick(n, 100)
	if out := n.Step(Message{From: "n3", To: "n1", Body: RequestVoteArgs{Term: 5, CandidateID: "n3"}}); len(out) != 0 {
		t.Fatalf("setup: Step returned %+v, want nothing (Save failed)", out)
	}
	tick(n, 50)
	if got := expiries(rec); got != 1 {
		t.Errorf("expiry reported %d times at the original deadline after a failed vote, want 1", got)
	}
}

// A reset draws a fresh timeout from the TimeoutSource instead of reusing the first one.
func TestElectionTimer_ResetDrawsNewTimeout(t *testing.T) {
	n, rec, _ := timerNode(t, &seqTimeouts{vals: []int{150, 40}}, PersistentState{CurrentTerm: 5}, Follower)

	tick(n, 10)
	n.Step(Message{From: "n3", To: "n1", Body: RequestVoteArgs{Term: 5, CandidateID: "n3"}})

	tick(n, 39)
	if got := expiries(rec); got != 0 {
		t.Fatalf("expiry reported %d times 39 ticks after a reset to 40, want 0", got)
	}
	tick(n, 1)
	if got := expiries(rec); got != 1 {
		t.Errorf("expiry reported %d times 40 ticks after a reset to 40, want 1", got)
	}
}
