package check

import (
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// ent describes one log entry; mkLog assigns indexes 1, 2, 3, ... in order.
type ent struct {
	term raft.Term
	cmd  string
}

func mkLog(ents ...ent) []raft.Entry {
	log := make([]raft.Entry, len(ents))
	for i, e := range ents {
		log[i] = raft.Entry{Term: e.term, Index: raft.Index(i + 1), Cmd: []byte(e.cmd)}
	}
	return log
}

func node(id raft.NodeID, ents ...ent) Snapshot {
	return Snapshot{ID: id, Log: mkLog(ents...)}
}

func TestLogMatching(t *testing.T) {
	tests := []struct {
		name      string
		snaps     []Snapshot
		wantError bool
	}{
		// Logs that satisfy Log Matching.
		{
			name:  "no snapshots",
			snaps: nil,
		},
		{
			name:  "single node",
			snaps: []Snapshot{node("n1", ent{1, "a"}, ent{1, "b"})},
		},
		{
			name:  "all logs empty",
			snaps: []Snapshot{node("n1"), node("n2"), node("n3")},
		},
		{
			name: "identical logs",
			snaps: []Snapshot{
				node("n1", ent{1, "a"}, ent{1, "b"}, ent{2, "c"}),
				node("n2", ent{1, "a"}, ent{1, "b"}, ent{2, "c"}),
				node("n3", ent{1, "a"}, ent{1, "b"}, ent{2, "c"}),
			},
		},
		{
			name: "different lengths with a matching prefix",
			snaps: []Snapshot{
				node("n1", ent{1, "a"}, ent{1, "b"}, ent{2, "c"}, ent{2, "d"}),
				node("n2", ent{1, "a"}, ent{1, "b"}),
				node("n3"),
			},
		},
		{
			name: "uncommitted tails with different terms at the same index",
			snaps: []Snapshot{
				node("n1", ent{1, "a"}, ent{1, "b"}, ent{2, "c"}),
				node("n2", ent{1, "a"}, ent{1, "b"}, ent{3, "x"}),
			},
		},
		{
			name: "logs diverging from index 1 with different terms",
			snaps: []Snapshot{
				node("n1", ent{1, "a"}),
				node("n2", ent{2, "b"}),
			},
		},

		// Logs that violate Log Matching.
		{
			name: "same index and term but different command",
			snaps: []Snapshot{
				node("n1", ent{1, "a"}, ent{2, "b"}),
				node("n2", ent{1, "a"}, ent{2, "X"}),
			},
			wantError: true,
		},
		{
			name: "same index and term but different command at index 1",
			snaps: []Snapshot{
				node("n1", ent{1, "a"}),
				node("n2", ent{1, "X"}),
			},
			wantError: true,
		},
		{
			name: "same term at index 3 but different term earlier in the prefix",
			snaps: []Snapshot{
				node("n1", ent{1, "a"}, ent{1, "b"}, ent{2, "c"}),
				node("n2", ent{1, "a"}, ent{2, "Z"}, ent{2, "c"}),
			},
			wantError: true,
		},
		{
			name: "same term at index 3 but different command earlier in the prefix",
			snaps: []Snapshot{
				node("n1", ent{1, "a"}, ent{1, "b"}, ent{1, "c"}),
				node("n2", ent{1, "a"}, ent{1, "X"}, ent{1, "c"}),
			},
			wantError: true,
		},
		{
			name: "conflict inside the shared part of logs of different length",
			snaps: []Snapshot{
				node("n1", ent{1, "a"}, ent{1, "b"}),
				node("n2", ent{1, "a"}, ent{1, "X"}, ent{1, "c"}),
			},
			wantError: true,
		},
		{
			name: "only two of three nodes conflict",
			snaps: []Snapshot{
				node("n1", ent{1, "a"}, ent{1, "b"}),
				node("n2", ent{1, "a"}, ent{2, "c"}),
				node("n3", ent{1, "a"}, ent{2, "Y"}),
			},
			wantError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The result must not depend on the order the snapshots arrive in.
			orders := map[string][]Snapshot{"given order": tt.snaps}
			rev := make([]Snapshot, len(tt.snaps))
			for i, s := range tt.snaps {
				rev[len(tt.snaps)-1-i] = s
			}
			orders["reversed"] = rev

			for name, snaps := range orders {
				err := LogMatching{}.Observe(snaps)
				if tt.wantError && err == nil {
					t.Errorf("%s: Observe returned nil, want a violation", name)
				}
				if !tt.wantError && err != nil {
					t.Errorf("%s: Observe returned %v, want nil", name, err)
				}
			}
		})
	}
}

// Log Matching is checked on each call alone; an earlier violation must not poison later calls.
func TestLogMatchingHasNoMemory(t *testing.T) {
	bad := []Snapshot{node("n1", ent{1, "a"}), node("n2", ent{1, "X"})}
	good := []Snapshot{node("n1", ent{1, "a"}), node("n2", ent{1, "a"})}
	var c LogMatching
	if err := c.Observe(bad); err == nil {
		t.Fatal("Observe(bad) returned nil")
	}
	if err := c.Observe(good); err != nil {
		t.Errorf("Observe(good) after a violation = %v, want nil", err)
	}
}

func TestLogMatchingDoesNotModifySnapshots(t *testing.T) {
	snaps := []Snapshot{
		node("n1", ent{1, "a"}, ent{2, "b"}),
		node("n2", ent{1, "a"}, ent{2, "X"}),
	}
	LogMatching{}.Observe(snaps)
	if got := string(snaps[1].Log[1].Cmd); got != "X" || len(snaps[0].Log) != 2 {
		t.Errorf("Observe modified its input: %+v", snaps)
	}
}
