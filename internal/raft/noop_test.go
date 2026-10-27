package raft

import "testing"

// noopLeader builds n1 at term 5 over log 1:(t1,a) 2:(t3,b), with the given config, and lets it
// win the term-6 election. It returns the node, the messages sent on winning, the storage, and
// the recorder of applied entries.
func noopLeader(t *testing.T, cfg Config) (*Node, []Message, *MemStorage, *applier) {
	t.Helper()
	st := NewMemStorage()
	st.Save(PersistentState{CurrentTerm: 5, Log: []Entry{en(1, 1, "a"), en(2, 3, "b")}})
	a := &applier{t: t}
	n, err := NewNode(cfg, "n1", []NodeID{"n2", "n3"}, st, nil, NewFixedTimeouts(150), a.onApply)
	if err != nil {
		t.Fatal(err)
	}
	a.n = n
	tick(n, 150)
	won := voteReply(n, "n2", 6, true)
	wantRole(t, n, Leader, 6)
	return n, won, st, a
}

// Section 8: a new leader appends a no-op entry in its own term as soon as it is elected.
func TestNoop_AppendedOnElection(t *testing.T) {
	n, won, st, _ := noopLeader(t, DefaultConfig())

	log := n.Snapshot().Log
	if len(log) != 3 || log[2].Index != 3 || log[2].Term != 6 {
		t.Fatalf("leader log = %+v, want a third entry at index 3 in term 6", log)
	}
	if s := lastSaved(t, st); len(s.Log) != 3 || s.Log[2].Term != 6 {
		t.Errorf("saved log has %d entries, want 3 (the no-op is saved before it is sent)", len(s.Log))
	}
	for p, args := range appendEntriesTo(t, won, "n2", "n3") {
		if args.PrevLogIndex != 2 || len(args.Entries) != 1 || args.Entries[0].Index != 3 || args.Entries[0].Term != 6 {
			t.Errorf("first AppendEntries to %s = prev %d with %+v, want prev 2 carrying the term-6 entry at 3", p, args.PrevLogIndex, args.Entries)
		}
	}
}

// A client proposal after the election goes after the no-op.
func TestNoop_ProposalsFollowIt(t *testing.T) {
	n, _, _, _ := noopLeader(t, DefaultConfig())
	if res, _ := n.Propose([]byte("x")); res.Index != 4 || res.Term != 6 {
		t.Errorf("Propose after election = %+v, want index 4 in term 6", res)
	}
}

// The older-term entries 1 and 2 cannot commit by counting replicas (Figure 8), but once the
// term-6 no-op is on a majority, they commit with it, without waiting for a client command.
func TestNoop_CommitsOlderEntries(t *testing.T) {
	n, _, _, _ := noopLeader(t, DefaultConfig())

	aeSuccess(n, "n2", 6, 3, 2)

	if got := n.Snapshot().CommitIndex; got != 3 {
		t.Errorf("commitIndex = %d after n2 confirmed the no-op, want 3", got)
	}
}

// The no-op exists only for Raft; the state machine sees the older entries but never the no-op.
func TestNoop_SkippedAtApply(t *testing.T) {
	n, _, _, a := noopLeader(t, DefaultConfig())
	aeSuccess(n, "n2", 6, 3, 2)

	wantApplied(t, a, []Entry{en(1, 1, "a"), en(2, 3, "b")})
	if got := n.Snapshot().LastApplied; got != 3 {
		t.Errorf("lastApplied = %d, want 3 (the no-op is passed over, not left pending)", got)
	}

	n.Propose([]byte("x"))
	aeSuccess(n, "n2", 6, 4, 3)
	wantApplied(t, a, []Entry{en(1, 1, "a"), en(2, 3, "b"), en(4, 6, "x")})
}

// DisableNoop exists only for tests like Figure 8 (T4.7), which need a leader that does not append one.
func TestNoop_DisableFlag(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DisableNoop = true
	n, won, _, _ := noopLeader(t, cfg)

	if got := len(n.Snapshot().Log); got != 2 {
		t.Fatalf("log has %d entries after election with DisableNoop, want 2 (no no-op)", got)
	}
	for p, args := range appendEntriesTo(t, won, "n2", "n3") {
		if len(args.Entries) != 0 {
			t.Errorf("first AppendEntries to %s carries %+v, want none", p, args.Entries)
		}
	}
	aeSuccess(n, "n2", 6, 2, 2)
	if got := n.Snapshot().CommitIndex; got != 0 {
		t.Errorf("commitIndex = %d, want 0 (without a no-op the term-3 entry cannot commit)", got)
	}
	if res, _ := n.Propose([]byte("x")); res.Index != 3 {
		t.Errorf("Propose with DisableNoop = index %d, want 3", res.Index)
	}
}
