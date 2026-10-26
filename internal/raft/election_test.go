package raft

import "testing"

// voteRequests checks that msgs are exactly one RequestVote from n1 to each of n2 and n3, and returns them by peer.
func voteRequests(t *testing.T, msgs []Message) map[NodeID]RequestVoteArgs {
	t.Helper()
	out := map[NodeID]RequestVoteArgs{}
	for _, m := range msgs {
		args, ok := m.Body.(RequestVoteArgs)
		if !ok {
			t.Fatalf("message to %s has body %T, want RequestVoteArgs", m.To, m.Body)
		}
		if m.From != "n1" {
			t.Errorf("message From = %q, want n1", m.From)
		}
		if _, dup := out[m.To]; dup {
			t.Errorf("two RequestVotes to %s", m.To)
		}
		out[m.To] = args
	}
	if len(msgs) != 2 || out["n2"] == (RequestVoteArgs{}) || out["n3"] == (RequestVoteArgs{}) {
		t.Fatalf("got %d messages %+v, want one RequestVote to each of n2 and n3", len(msgs), msgs)
	}
	return out
}

// tickUntilOutput ticks up to max times and returns the tick number and output of the first tick that sends anything.
func tickUntilOutput(n *Node, max int) (int, []Message) {
	for i := 1; i <= max; i++ {
		if out := n.Tick(); len(out) > 0 {
			return i, out
		}
	}
	return 0, nil
}

// Section 5.2: on timeout a follower increments its term, becomes a candidate, votes for
// itself, and sends RequestVote to every other server. All of it is saved before Tick returns.
func TestStartElection_OnTimeout(t *testing.T) {
	tests := []struct {
		name         string
		log          []Entry
		wantLastIdx  Index
		wantLastTerm Term
	}{
		{"empty log", nil, 0, 0},
		{"log ending in the current term", []Entry{{Term: 1, Index: 1}, {Term: 3, Index: 2}, {Term: 5, Index: 3}}, 3, 5},
		{"log ending in an older term", []Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}, 2, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, _, st := timerNode(t, NewFixedTimeouts(150), PersistentState{CurrentTerm: 5, VotedFor: "n2", Log: tt.log}, Follower)
			mem := st.(*MemStorage)

			at, out := tickUntilOutput(n, 1000)
			if at != 150 {
				t.Fatalf("first output came on tick %d, want 150 (the timeout)", at)
			}

			for peer, args := range voteRequests(t, out) {
				want := RequestVoteArgs{Term: 6, CandidateID: "n1", LastLogIndex: tt.wantLastIdx, LastLogTerm: tt.wantLastTerm}
				if args != want {
					t.Errorf("RequestVote to %s = %+v, want %+v", peer, args, want)
				}
			}

			got := n.Snapshot()
			if got.Role != Candidate || got.Term != 6 || got.VotedFor != "n1" {
				t.Errorf("state = role %v, term %d, votedFor %q; want Candidate, 6, \"n1\"", got.Role, got.Term, got.VotedFor)
			}
			if s := lastSaved(t, mem); s.CurrentTerm != 6 || s.VotedFor != "n1" || !entriesEqual(s.Log, got.Log) {
				t.Errorf("storage holds term %d, votedFor %q, %d entries when Tick returns; want 6, \"n1\", %d entries",
					s.CurrentTerm, s.VotedFor, len(s.Log), len(got.Log))
			}
		})
	}
}

// A candidate that times out again starts a new election in the next term, with a fresh timeout.
func TestStartElection_CandidateRetriesInNewTerm(t *testing.T) {
	n, _, st := timerNode(t, &seqTimeouts{vals: []int{150, 60, 80}}, PersistentState{CurrentTerm: 5}, Follower)
	mem := st.(*MemStorage)

	if at, _ := tickUntilOutput(n, 1000); at != 150 {
		t.Fatalf("first election started on tick %d, want 150", at)
	}

	at, out := tickUntilOutput(n, 1000)
	if at != 60 {
		t.Fatalf("second election started %d ticks after the first, want 60 (the reset draws a new timeout)", at)
	}
	for peer, args := range voteRequests(t, out) {
		if args.Term != 7 || args.CandidateID != "n1" {
			t.Errorf("retry RequestVote to %s = %+v, want term 7 from n1", peer, args)
		}
	}
	got := n.Snapshot()
	if got.Role != Candidate || got.Term != 7 || got.VotedFor != "n1" {
		t.Errorf("state = role %v, term %d, votedFor %q; want Candidate, 7, \"n1\"", got.Role, got.Term, got.VotedFor)
	}
	if s := lastSaved(t, mem); s.CurrentTerm != 7 || s.VotedFor != "n1" {
		t.Errorf("storage holds term/votedFor = %d/%q, want 7/\"n1\"", s.CurrentTerm, s.VotedFor)
	}

	if at, _ := tickUntilOutput(n, 1000); at != 80 {
		t.Errorf("third election started %d ticks after the second, want 80", at)
	}
	if got := n.Snapshot().Term; got != 8 {
		t.Errorf("term after the third timeout = %d, want 8", got)
	}
}

func TestStartElection_NothingBeforeTimeout(t *testing.T) {
	n, _, st := timerNode(t, NewFixedTimeouts(150), PersistentState{CurrentTerm: 5}, Follower)
	saves := st.(*MemStorage).SaveCount()

	for i := 1; i < 150; i++ {
		if out := n.Tick(); len(out) != 0 {
			t.Fatalf("tick %d sent %+v before the timeout", i, out)
		}
	}
	if got := n.Snapshot(); got.Role != Follower || got.Term != 5 {
		t.Errorf("state = role %v, term %d before the timeout; want Follower, 5", got.Role, got.Term)
	}
	if got := st.(*MemStorage).SaveCount(); got != saves {
		t.Errorf("SaveCount() = %d before the timeout, want %d", got, saves)
	}
}

// CLAUDE.md rule 2: if persisting fails, return no messages. The node stays as it was on disk,
// its deadline is still past, and the next tick tries again.
func TestStartElection_SaveFailsSendsNothing(t *testing.T) {
	mem := NewMemStorage()
	mem.Save(PersistentState{CurrentTerm: 5})
	spy := &spyStorage{MemStorage: mem, failOn: map[int]bool{1: true}}
	n, err := NewNode(DefaultConfig(), "n1", []NodeID{"n2", "n3"}, spy, nil, NewFixedTimeouts(150), func(Entry) {})
	if err != nil {
		t.Fatal(err)
	}

	tick(n, 149)
	if out := n.Tick(); len(out) != 0 {
		t.Fatalf("tick 150 sent %+v although the election could not be saved", out)
	}
	if got := n.Snapshot(); got.Role != Follower || got.Term != 5 || got.VotedFor != "" {
		t.Fatalf("state = role %v, term %d, votedFor %q after a failed save; want Follower, 5, \"\"", got.Role, got.Term, got.VotedFor)
	}

	out := n.Tick()
	for peer, args := range voteRequests(t, out) {
		if args.Term != 6 {
			t.Errorf("RequestVote to %s on the retry tick has term %d, want 6", peer, args.Term)
		}
	}
}
