package raft

import (
	"errors"
	"testing"
)

// failingStorage lets a test make Load fail.
type failingStorage struct{ err error }

func (f failingStorage) Save(PersistentState) error     { return f.err }
func (f failingStorage) Load() (PersistentState, error) { return PersistentState{}, f.err }

func newTestNode(t *testing.T, st Storage) *Node {
	t.Helper()
	n, err := NewNode(DefaultConfig(), "n1", []NodeID{"n2", "n3"}, st, nil, NewFixedTimeouts(150), func(Entry) {})
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}
	return n
}

func entriesEqual(a, b []Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

func TestNewNodeFresh(t *testing.T) {
	st := NewMemStorage()
	got := newTestNode(t, st).Snapshot()

	if got.ID != "n1" {
		t.Errorf("ID = %q, want n1", got.ID)
	}
	if got.Role != Follower {
		t.Errorf("Role = %v, want Follower", got.Role)
	}
	if got.Term != 0 {
		t.Errorf("Term = %d, want 0", got.Term)
	}
	if got.VotedFor != "" {
		t.Errorf("VotedFor = %q, want empty", got.VotedFor)
	}
	if len(got.Log) != 0 {
		t.Errorf("Log = %+v, want empty", got.Log)
	}
	if got.CommitIndex != 0 {
		t.Errorf("CommitIndex = %d, want 0", got.CommitIndex)
	}
	if got.LastApplied != 0 {
		t.Errorf("LastApplied = %d, want 0", got.LastApplied)
	}
	if n := st.SaveCount(); n != 0 {
		t.Errorf("SaveCount() = %d after NewNode on empty storage, want 0 (construction must not write)", n)
	}
}

func TestNewNodeRestartsFromStoredState(t *testing.T) {
	tests := []struct {
		name   string
		stored PersistentState
	}{
		{"term only", PersistentState{CurrentTerm: 4}},
		{"term and vote", PersistentState{CurrentTerm: 4, VotedFor: "n2"}},
		{"term, vote and log", PersistentState{
			CurrentTerm: 7,
			VotedFor:    "n3",
			Log: []Entry{
				{Term: 1, Index: 1, Cmd: []byte("a")},
				{Term: 3, Index: 2, Cmd: []byte("b")},
				{Term: 7, Index: 3, Cmd: nil},
			},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := NewMemStorage()
			if err := st.Save(tt.stored); err != nil {
				t.Fatal(err)
			}
			saves := st.SaveCount()

			got := newTestNode(t, st).Snapshot()

			if got.Term != tt.stored.CurrentTerm {
				t.Errorf("Term = %d, want %d", got.Term, tt.stored.CurrentTerm)
			}
			if got.VotedFor != tt.stored.VotedFor {
				t.Errorf("VotedFor = %q, want %q", got.VotedFor, tt.stored.VotedFor)
			}
			if !entriesEqual(got.Log, tt.stored.Log) {
				t.Errorf("Log = %+v, want %+v", got.Log, tt.stored.Log)
			}
			if st.SaveCount() != saves {
				t.Errorf("NewNode wrote to storage: SaveCount() went from %d to %d", saves, st.SaveCount())
			}
		})
	}
}

// commitIndex and lastApplied are volatile (Figure 2): a restart always starts them at 0
// and a node always restarts as a follower, whatever it was before the crash.
func TestNewNodeVolatileStateResets(t *testing.T) {
	st := NewMemStorage()
	st.Save(PersistentState{
		CurrentTerm: 9,
		VotedFor:    "n1",
		Log: []Entry{
			{Term: 8, Index: 1, Cmd: []byte("a")},
			{Term: 9, Index: 2, Cmd: []byte("b")},
		},
	})

	got := newTestNode(t, st).Snapshot()

	if got.CommitIndex != 0 {
		t.Errorf("CommitIndex = %d after restart, want 0", got.CommitIndex)
	}
	if got.LastApplied != 0 {
		t.Errorf("LastApplied = %d after restart, want 0", got.LastApplied)
	}
	if got.Role != Follower {
		t.Errorf("Role = %v after restart, want Follower", got.Role)
	}
}

func TestNewNodeLoadErrorIsReturned(t *testing.T) {
	boom := errors.New("disk on fire")
	n, err := NewNode(DefaultConfig(), "n1", []NodeID{"n2", "n3"}, failingStorage{boom}, nil, NewFixedTimeouts(150), func(Entry) {})
	if err == nil {
		t.Fatal("NewNode() with failing storage returned no error")
	}
	if !errors.Is(err, boom) {
		t.Errorf("NewNode() error = %v, want it to wrap the storage error", err)
	}
	if n != nil {
		t.Errorf("NewNode() returned a node alongside the error: %+v", n)
	}
}

func TestNodeSnapshotReturnsCopies(t *testing.T) {
	st := NewMemStorage()
	st.Save(PersistentState{
		CurrentTerm: 3,
		Log: []Entry{
			{Term: 1, Index: 1, Cmd: []byte("a")},
			{Term: 3, Index: 2, Cmd: []byte("b")},
		},
	})
	n := newTestNode(t, st)

	first := n.Snapshot()
	first.Term = 99
	first.Log[0].Term = 99
	first.Log[1].Cmd[0] = 'X'
	first.Log = append(first.Log, Entry{Term: 99, Index: 3})

	second := n.Snapshot()
	want := []Entry{
		{Term: 1, Index: 1, Cmd: []byte("a")},
		{Term: 3, Index: 2, Cmd: []byte("b")},
	}
	if second.Term != 3 {
		t.Errorf("Term = %d after mutating an earlier Snapshot, want 3", second.Term)
	}
	if !entriesEqual(second.Log, want) {
		t.Errorf("Log = %+v after mutating an earlier Snapshot, want %+v", second.Log, want)
	}
}
