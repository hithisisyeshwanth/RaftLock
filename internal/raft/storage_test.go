package raft

import "testing"

var _ Storage = (*MemStorage)(nil)

func sampleState() PersistentState {
	return PersistentState{
		CurrentTerm: 3,
		VotedFor:    "n2",
		Log: []Entry{
			{Term: 1, Index: 1, Cmd: []byte("a")},
			{Term: 3, Index: 2, Cmd: []byte("b")},
		},
	}
}

func statesEqual(a, b PersistentState) bool {
	if a.CurrentTerm != b.CurrentTerm || a.VotedFor != b.VotedFor || len(a.Log) != len(b.Log) {
		return false
	}
	for i := range a.Log {
		if !a.Log[i].Equal(b.Log[i]) {
			return false
		}
	}
	return true
}

func TestMemStorageLoadEmpty(t *testing.T) {
	s := NewMemStorage()
	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	if got.CurrentTerm != 0 || got.VotedFor != "" || len(got.Log) != 0 {
		t.Errorf("Load() on empty storage = %+v, want the zero state", got)
	}
	if n := s.SaveCount(); n != 0 {
		t.Errorf("SaveCount() = %d, want 0", n)
	}
}

func TestMemStorageRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		state PersistentState
	}{
		{"term only", PersistentState{CurrentTerm: 7}},
		{"term and vote", PersistentState{CurrentTerm: 2, VotedFor: "n1"}},
		{"full state", sampleState()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemStorage()
			if err := s.Save(tt.state); err != nil {
				t.Fatalf("Save() error = %v", err)
			}
			got, err := s.Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if !statesEqual(got, tt.state) {
				t.Errorf("Load() = %+v, want %+v", got, tt.state)
			}
		})
	}
}

func TestMemStorageSaveOverwrites(t *testing.T) {
	s := NewMemStorage()
	s.Save(sampleState())
	latest := PersistentState{CurrentTerm: 4, VotedFor: "n3"}
	s.Save(latest)
	got, _ := s.Load()
	if !statesEqual(got, latest) {
		t.Errorf("Load() = %+v, want only the latest state %+v", got, latest)
	}
}

func TestMemStorageSaveDeepCopies(t *testing.T) {
	s := NewMemStorage()
	state := sampleState()
	s.Save(state)

	state.CurrentTerm = 99
	state.Log[0].Term = 99
	state.Log[1].Cmd[0] = 'X'
	state.Log = append(state.Log, Entry{Term: 99, Index: 3})

	got, _ := s.Load()
	if !statesEqual(got, sampleState()) {
		t.Errorf("mutating the caller's state after Save changed stored state: %+v", got)
	}
}

func TestMemStorageLoadReturnsCopy(t *testing.T) {
	s := NewMemStorage()
	s.Save(sampleState())

	first, _ := s.Load()
	first.Log[0].Term = 99
	first.Log[1].Cmd[0] = 'X'

	second, _ := s.Load()
	if !statesEqual(second, sampleState()) {
		t.Errorf("mutating a loaded state changed stored state: %+v", second)
	}
}

func TestMemStorageRecordsSaves(t *testing.T) {
	s := NewMemStorage()
	states := []PersistentState{
		{CurrentTerm: 1},
		{CurrentTerm: 2, VotedFor: "n1"},
		{CurrentTerm: 2, VotedFor: "n1", Log: []Entry{{Term: 2, Index: 1, Cmd: []byte("x")}}},
	}
	for i, st := range states {
		s.Save(st)
		if n := s.SaveCount(); n != i+1 {
			t.Errorf("SaveCount() after %d saves = %d", i+1, n)
		}
	}

	history := s.History()
	if len(history) != len(states) {
		t.Fatalf("History() has %d states, want %d", len(history), len(states))
	}
	for i := range states {
		if !statesEqual(history[i], states[i]) {
			t.Errorf("History()[%d] = %+v, want %+v (saves must be in call order)", i, history[i], states[i])
		}
	}
}

func TestMemStorageHistoryIsSnapshot(t *testing.T) {
	s := NewMemStorage()
	s.Save(sampleState())

	history := s.History()
	history[0].CurrentTerm = 99
	history[0].Log[0].Term = 99

	if again := s.History(); !statesEqual(again[0], sampleState()) {
		t.Errorf("mutating History() output changed the recorded history: %+v", again[0])
	}
}
