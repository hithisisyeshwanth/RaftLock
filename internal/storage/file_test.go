package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

var _ raft.Storage = (*FileStorage)(nil)

func sampleState() raft.PersistentState {
	return raft.PersistentState{
		CurrentTerm: 3,
		VotedFor:    "n2",
		Log: []raft.Entry{
			{Term: 1, Index: 1, Cmd: []byte("first command")},
			{Term: 3, Index: 2, Cmd: []byte("second command")},
		},
	}
}

func statesEqual(a, b raft.PersistentState) bool {
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

// newSaved returns the state file path after saving sampleState to it.
func newSaved(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state")
	if err := NewFileStorage(path).Save(sampleState()); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	return path
}

func TestFileStorageRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		state raft.PersistentState
	}{
		{"zero state", raft.PersistentState{}},
		{"term only", raft.PersistentState{CurrentTerm: 7}},
		{"term and vote", raft.PersistentState{CurrentTerm: 2, VotedFor: "n1"}},
		{"full state", sampleState()},
		{"empty command", raft.PersistentState{CurrentTerm: 1, Log: []raft.Entry{{Term: 1, Index: 1}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewFileStorage(filepath.Join(t.TempDir(), "state"))
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

// A new FileStorage on the same path is a process restart.
func TestFileStorageSurvivesRestart(t *testing.T) {
	path := newSaved(t)
	got, err := NewFileStorage(path).Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !statesEqual(got, sampleState()) {
		t.Errorf("Load() after restart = %+v, want %+v", got, sampleState())
	}
}

func TestFileStorageOverwriteKeepsLatest(t *testing.T) {
	s := NewFileStorage(filepath.Join(t.TempDir(), "state"))
	s.Save(sampleState())
	latest := raft.PersistentState{CurrentTerm: 4, VotedFor: "n3"} // shorter than the first save
	if err := s.Save(latest); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !statesEqual(got, latest) {
		t.Errorf("Load() = %+v, want only the latest state %+v", got, latest)
	}
}

func TestFileStorageLoadMissingFile(t *testing.T) {
	s := NewFileStorage(filepath.Join(t.TempDir(), "state"))
	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load() on a missing file: error = %v, want nil", err)
	}
	if got.CurrentTerm != 0 || got.VotedFor != "" || len(got.Log) != 0 {
		t.Errorf("Load() on a missing file = %+v, want the zero state", got)
	}
}

func TestFileStorageTruncatedFile(t *testing.T) {
	good, _ := os.ReadFile(newSaved(t))
	tests := []struct {
		name string
		keep int
	}{
		{"empty file", 0},
		{"shorter than a checksum", 2},
		{"half the file", len(good) / 2},
		{"missing the last byte", len(good) - 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := newSaved(t)
			if err := os.Truncate(path, int64(tt.keep)); err != nil {
				t.Fatal(err)
			}
			if _, err := NewFileStorage(path).Load(); err == nil {
				t.Errorf("Load() of a file truncated to %d bytes returned no error", tt.keep)
			}
		})
	}
}

func TestFileStorageCorruptFile(t *testing.T) {
	good, _ := os.ReadFile(newSaved(t))
	tests := []struct {
		name string
		pos  int
	}{
		{"first byte", 0},
		{"middle byte", len(good) / 2},
		{"last byte", len(good) - 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := newSaved(t)
			bad := append([]byte(nil), good...)
			bad[tt.pos] ^= 0xFF
			if err := os.WriteFile(path, bad, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := NewFileStorage(path).Load(); err == nil {
				t.Errorf("Load() with byte %d flipped returned no error", tt.pos)
			}
		})
	}
}

func TestFileStorageGarbageFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	if err := os.WriteFile(path, []byte("this is not a raftlock state file at all"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileStorage(path).Load(); err == nil {
		t.Error("Load() of a garbage file returned no error")
	}
}

// A crash between creating the temp file and the rename leaves "<path>.tmp" behind.
func TestFileStorageIgnoresLeftoverTempFile(t *testing.T) {
	t.Run("with a good state file", func(t *testing.T) {
		path := newSaved(t)
		os.WriteFile(path+".tmp", []byte("half-written garbage"), 0o644)
		got, err := NewFileStorage(path).Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if !statesEqual(got, sampleState()) {
			t.Errorf("Load() = %+v, want the last completed save %+v", got, sampleState())
		}
	})
	t.Run("with no state file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state")
		os.WriteFile(path+".tmp", []byte("half-written garbage"), 0o644)
		got, err := NewFileStorage(path).Load()
		if err != nil {
			t.Fatalf("Load() error = %v, want nil (a temp file is not saved state)", err)
		}
		if got.CurrentTerm != 0 || got.VotedFor != "" || len(got.Log) != 0 {
			t.Errorf("Load() = %+v, want the zero state", got)
		}
	})
	t.Run("next save succeeds and wins", func(t *testing.T) {
		path := newSaved(t)
		os.WriteFile(path+".tmp", []byte("half-written garbage"), 0o644)
		s := NewFileStorage(path)
		latest := raft.PersistentState{CurrentTerm: 9, VotedFor: "n1"}
		if err := s.Save(latest); err != nil {
			t.Fatalf("Save() with a leftover temp file: error = %v", err)
		}
		got, err := s.Load()
		if err != nil || !statesEqual(got, latest) {
			t.Errorf("Load() = %+v, %v; want %+v, nil", got, err, latest)
		}
	})
}

func TestFileStorageLeavesNoTempFileBehind(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStorage(filepath.Join(dir, "state"))
	s.Save(sampleState())
	s.Save(raft.PersistentState{CurrentTerm: 5})

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "state" {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("directory contains %v after Save, want only [state]", names)
	}
}
