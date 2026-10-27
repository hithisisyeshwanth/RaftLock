package raft

import "testing"

// electedLeader builds n1 at term 5 over log (terms 1, 3) with the given peers, lets it time out
// into a term-6 election, and hands it the votes it needs. It returns the node, the messages
// sent by the reply that won the election, and the storage.
func electedLeader(t *testing.T, peers ...NodeID) (*Node, []Message, *MemStorage) {
	t.Helper()
	st := NewMemStorage()
	err := st.Save(PersistentState{CurrentTerm: 5, Log: []Entry{
		{Term: 1, Index: 1, Cmd: []byte("a")},
		{Term: 3, Index: 2, Cmd: []byte("b")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DisableNoop = true // these tests predate the T4.6 no-op and count log indexes without it
	n, err := NewNode(cfg, "n1", peers, st, nil, NewFixedTimeouts(150), func(Entry) {})
	if err != nil {
		t.Fatal(err)
	}
	tick(n, 150)
	var won []Message
	for _, p := range peers[:len(peers)/2] { // self plus half the peers is a majority
		won = voteReply(n, p, 6, true)
	}
	wantRole(t, n, Leader, 6)
	return n, won, st
}

// appendEntriesTo checks msgs are exactly one AppendEntries from n1 to each peer and returns them by peer.
func appendEntriesTo(t *testing.T, msgs []Message, peers ...NodeID) map[NodeID]AppendEntriesArgs {
	t.Helper()
	out := map[NodeID]AppendEntriesArgs{}
	for _, m := range msgs {
		args, ok := m.Body.(AppendEntriesArgs)
		if !ok {
			t.Fatalf("message to %s has body %T, want AppendEntriesArgs", m.To, m.Body)
		}
		if m.From != "n1" {
			t.Errorf("message From = %q, want n1", m.From)
		}
		if _, dup := out[m.To]; dup {
			t.Errorf("two AppendEntries to %s", m.To)
		}
		out[m.To] = args
	}
	if len(out) != len(peers) || len(msgs) != len(peers) {
		t.Fatalf("got %d messages %+v, want one AppendEntries to each of %v", len(msgs), msgs, peers)
	}
	for _, p := range peers {
		if _, ok := out[p]; !ok {
			t.Fatalf("no AppendEntries to %s in %+v", p, msgs)
		}
	}
	return out
}

// Figure 2, leader state: nextIndex starts just past the leader's last entry and matchIndex at 0.
func TestBecomeLeader_InitializesNextAndMatchIndex(t *testing.T) {
	peers := []NodeID{"n2", "n3", "n4", "n5"}
	n, _, _ := electedLeader(t, peers...)

	for _, p := range peers {
		if got := n.nextIndex[p]; got != 3 {
			t.Errorf("nextIndex[%s] = %d, want 3 (last index 2, plus 1)", p, got)
		}
		if got, ok := n.matchIndex[p]; !ok || got != 0 {
			t.Errorf("matchIndex[%s] = %d (present %v), want 0", p, got, ok)
		}
	}
	if len(n.nextIndex) != len(peers) || len(n.matchIndex) != len(peers) {
		t.Errorf("nextIndex has %d keys and matchIndex %d, want one per peer (%d)", len(n.nextIndex), len(n.matchIndex), len(peers))
	}
}

// Section 5.2: a new leader sends AppendEntries to every peer at once, to establish its
// authority before anyone else times out.
func TestBecomeLeader_ImmediateHeartbeat(t *testing.T) {
	for _, peers := range [][]NodeID{{"n2", "n3"}, {"n2", "n3", "n4", "n5"}} {
		_, won, _ := electedLeader(t, peers...)
		for p, args := range appendEntriesTo(t, won, peers...) {
			if args.Term != 6 || args.LeaderID != "n1" || args.PrevLogIndex != 2 || args.PrevLogTerm != 3 || args.LeaderCommit != 0 {
				t.Errorf("AppendEntries to %s = %+v, want term 6, leader n1, prev (2, 3), leaderCommit 0", p, args)
			}
		}
	}
}

func TestBecomeLeader_PeriodicHeartbeats(t *testing.T) {
	n, _, _ := electedLeader(t, "n2", "n3")
	hb := DefaultConfig().HeartbeatInterval

	for round := 1; round <= 5; round++ {
		for i := 1; i < hb; i++ {
			if out := n.Tick(); len(out) != 0 {
				t.Fatalf("round %d, tick %d of %d: sent %+v between heartbeats", round, i, hb, out)
			}
		}
		for p, args := range appendEntriesTo(t, n.Tick(), "n2", "n3") {
			if args.Term != 6 || args.LeaderID != "n1" || args.PrevLogIndex != 2 || args.PrevLogTerm != 3 {
				t.Errorf("round %d heartbeat to %s = %+v, want term 6 from n1 with prev (2, 3)", round, p, args)
			}
		}
	}
}

// A leader has no election timer: however long it runs, it only ever sends heartbeats.
func TestBecomeLeader_NeverStartsElection(t *testing.T) {
	n, _, _ := electedLeader(t, "n2", "n3")

	for i := 1; i <= 2000; i++ {
		for _, m := range n.Tick() {
			if _, ok := m.Body.(AppendEntriesArgs); !ok {
				t.Fatalf("tick %d: leader sent a %T", i, m.Body)
			}
		}
	}
	wantRole(t, n, Leader, 6)
}

// Figure 2, all servers: a reply with a higher term means a newer leader exists.
func TestBecomeLeader_HigherTermReplyStepsDown(t *testing.T) {
	n, _, st := electedLeader(t, "n2", "n3")
	tick(n, 20)

	if out := n.Step(Message{From: "n2", To: "n1", Body: AppendEntriesReply{Term: 9}}); len(out) != 0 {
		t.Errorf("step-down sent %+v, want nothing", out)
	}
	got := n.Snapshot()
	if got.Role != Follower || got.Term != 9 || got.VotedFor != "" {
		t.Fatalf("state = role %v, term %d, votedFor %q; want Follower, 9, \"\"", got.Role, got.Term, got.VotedFor)
	}
	if s := lastSaved(t, st); s.CurrentTerm != 9 {
		t.Errorf("saved term = %d, want 9", s.CurrentTerm)
	}

	for i := 1; i <= 300; i++ {
		for _, m := range n.Tick() {
			if _, ok := m.Body.(AppendEntriesArgs); ok {
				t.Fatalf("tick %d after stepping down: still sent AppendEntries to %s", i, m.To)
			}
		}
	}
}

// Replies from the leader's own term or earlier do not unseat it.
func TestBecomeLeader_NotHigherReplyKeepsLeading(t *testing.T) {
	n, _, _ := electedLeader(t, "n2", "n3")

	n.Step(Message{From: "n2", To: "n1", Body: AppendEntriesReply{Term: 4}})
	n.Step(Message{From: "n3", To: "n1", Body: AppendEntriesReply{Term: 6, Success: true}})

	wantRole(t, n, Leader, 6)
	tick(n, DefaultConfig().HeartbeatInterval-1)
	appendEntriesTo(t, n.Tick(), "n2", "n3")
}
