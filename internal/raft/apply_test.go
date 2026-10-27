package raft

import "testing"

// applier records every onApply call and checks, inside each call, that the node's own
// state already reflects the entry (the callback runs after state updates). It only reads
// fields; the callback must never call back into the node.
type applier struct {
	t       *testing.T
	n       *Node
	applied []Entry
}

func (a *applier) onApply(e Entry) {
	if a.n.lastApplied != e.Index {
		a.t.Errorf("onApply(%d) ran with lastApplied %d, want it already set to %d", e.Index, a.n.lastApplied, e.Index)
	}
	if a.n.commitIndex < e.Index {
		a.t.Errorf("onApply(%d) ran with commitIndex %d: applied an uncommitted entry", e.Index, a.n.commitIndex)
	}
	a.applied = append(a.applied, e)
}

// applyingFollower is n1, a follower in term 5 over followerLog, with an applier attached.
func applyingFollower(t *testing.T, st *MemStorage) (*Node, *applier) {
	t.Helper()
	a := &applier{t: t}
	n, err := NewNode(DefaultConfig(), "n1", []NodeID{"n2", "n3"}, st, nil, NewFixedTimeouts(150), a.onApply)
	if err != nil {
		t.Fatal(err)
	}
	a.n = n
	return n, a
}

func storedFollowerLog(t *testing.T) *MemStorage {
	t.Helper()
	st := NewMemStorage()
	if err := st.Save(PersistentState{CurrentTerm: 5, Log: followerLog()}); err != nil {
		t.Fatal(err)
	}
	return st
}

func commitTo(n *Node, leaderCommit Index) {
	appendEntries(n, AppendEntriesArgs{Term: 5, PrevLogIndex: 4, PrevLogTerm: 2, LeaderCommit: leaderCommit})
}

func wantApplied(t *testing.T, a *applier, want []Entry) {
	t.Helper()
	if !entriesEqual(a.applied, want) {
		t.Errorf("applied %+v\nwant    %+v", a.applied, want)
	}
}

// Committed entries reach the state machine in log order, each once, as soon as they commit.
func TestApplyInOrderAsCommitAdvances(t *testing.T) {
	n, a := applyingFollower(t, storedFollowerLog(t))
	log := followerLog()

	commitTo(n, 2)
	wantApplied(t, a, log[:2])
	commitTo(n, 3)
	wantApplied(t, a, log[:3])
	commitTo(n, 4)
	wantApplied(t, a, log)

	if s := n.Snapshot(); s.LastApplied != 4 || s.CommitIndex != 4 {
		t.Errorf("lastApplied/commitIndex = %d/%d, want 4/4", s.LastApplied, s.CommitIndex)
	}
}

func TestApplyExactlyOnce(t *testing.T) {
	t.Run("repeated heartbeats with the same leaderCommit", func(t *testing.T) {
		n, a := applyingFollower(t, storedFollowerLog(t))
		for i := 0; i < 3; i++ {
			commitTo(n, 3)
		}
		wantApplied(t, a, followerLog()[:3])
	})
	t.Run("the apply routine run repeatedly", func(t *testing.T) {
		n, a := applyingFollower(t, storedFollowerLog(t))
		commitTo(n, 4)
		for i := 0; i < 3; i++ {
			n.applyCommitted()
		}
		wantApplied(t, a, followerLog())
	})
	t.Run("a lower leaderCommit later", func(t *testing.T) {
		n, a := applyingFollower(t, storedFollowerLog(t))
		commitTo(n, 4)
		commitTo(n, 2)
		wantApplied(t, a, followerLog())
	})
}

// Nothing beyond commitIndex is ever applied, even if the log holds more.
func TestApplyBoundedByCommitIndex(t *testing.T) {
	n, a := applyingFollower(t, storedFollowerLog(t))

	if len(a.applied) != 0 || n.Snapshot().LastApplied != 0 {
		t.Fatalf("applied %d entries at start with commitIndex 0, want none", len(a.applied))
	}
	n.applyCommitted()
	if len(a.applied) != 0 {
		t.Errorf("applyCommitted with commitIndex 0 applied %+v, want nothing", a.applied)
	}

	commitTo(n, 2)
	if s := n.Snapshot(); s.LastApplied > s.CommitIndex || s.LastApplied != 2 {
		t.Errorf("lastApplied/commitIndex = %d/%d, want 2/2", s.LastApplied, s.CommitIndex)
	}
	if len(a.applied) != 2 {
		t.Errorf("applied %d entries, want 2 (entries 3 and 4 are in the log but not committed)", len(a.applied))
	}
}

// A leader applies entries as soon as a reply commits them.
func TestApplyOnLeader(t *testing.T) {
	st := NewMemStorage()
	st.Save(PersistentState{CurrentTerm: 5, Log: []Entry{en(1, 1, "a"), en(2, 3, "b")}})
	n, a := applyingFollower(t, st)
	tick(n, 150)
	voteReply(n, "n2", 6, true)
	wantRole(t, n, Leader, 6)
	n.Propose([]byte("x"))

	aeSuccess(n, "n2", 6, 3, 2)

	wantApplied(t, a, []Entry{en(1, 1, "a"), en(2, 3, "b"), en(3, 6, "x")})
}

// lastApplied is volatile: after a restart it is 0, and the state machine is rebuilt by
// applying the log again from the start as the commit index is relearned.
func TestApplyReplayAfterRestart(t *testing.T) {
	st := storedFollowerLog(t)
	n, a := applyingFollower(t, st)
	commitTo(n, 4)
	wantApplied(t, a, followerLog())

	restarted, b := applyingFollower(t, st)
	if s := restarted.Snapshot(); s.LastApplied != 0 || s.CommitIndex != 0 {
		t.Fatalf("after restart lastApplied/commitIndex = %d/%d, want 0/0", s.LastApplied, s.CommitIndex)
	}
	if len(b.applied) != 0 {
		t.Fatalf("restart applied %+v before learning the commit index, want nothing", b.applied)
	}

	commitTo(restarted, 4)
	wantApplied(t, b, followerLog())
}
