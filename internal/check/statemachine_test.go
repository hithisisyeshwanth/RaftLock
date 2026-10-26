package check

import (
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// appliedEntry builds one applied entry. Index is explicit so tests can build gaps, repeats, and reordering.
func appliedEntry(index raft.Index, term raft.Term, cmd string) raft.Entry {
	return raft.Entry{Term: term, Index: index, Cmd: []byte(cmd)}
}

// canonicalEntries returns the first n entries of one fixed history: index i is term 1 with command "a", "b", "c", ...
func canonicalEntries(n int) []raft.Entry {
	out := make([]raft.Entry, n)
	for i := range out {
		out[i] = appliedEntry(raft.Index(i+1), 1, string(rune('a'+i)))
	}
	return out
}

// applied is a node's snapshot whose Applied field is every entry it has applied since it booted, in apply order.
func applied(id raft.NodeID, entries []raft.Entry) Snapshot {
	return Snapshot{ID: id, Applied: entries}
}

// TestStateMachineSafety feeds each case one Observe call per step, in order,
// and expects the first error at step wantErrAt (-1 for none).
func TestStateMachineSafety(t *testing.T) {
	tests := []struct {
		name      string
		steps     [][]Snapshot
		wantErrAt int
	}{
		// Histories that satisfy State Machine Safety.
		{
			name:      "no snapshots",
			steps:     [][]Snapshot{{}},
			wantErrAt: -1,
		},
		{
			name:      "nothing applied anywhere",
			steps:     [][]Snapshot{{applied("n1", nil), applied("n2", nil), applied("n3", nil)}},
			wantErrAt: -1,
		},
		{
			name: "all nodes applied the same entries",
			steps: [][]Snapshot{
				{applied("n1", canonicalEntries(3)), applied("n2", canonicalEntries(3)), applied("n3", canonicalEntries(3))},
			},
			wantErrAt: -1,
		},
		{
			name: "nodes at different progress agree on the shared prefix",
			steps: [][]Snapshot{
				{applied("n1", canonicalEntries(4)), applied("n2", canonicalEntries(2)), applied("n3", nil)},
			},
			wantErrAt: -1,
		},
		{
			name: "nodes apply more entries over time",
			steps: [][]Snapshot{
				{applied("n1", canonicalEntries(1)), applied("n2", nil)},
				{applied("n1", canonicalEntries(2)), applied("n2", canonicalEntries(1))},
				{applied("n1", canonicalEntries(4)), applied("n2", canonicalEntries(4))},
			},
			wantErrAt: -1,
		},
		{
			name: "a restarted node replays the same entries from index 1",
			steps: [][]Snapshot{
				{applied("n1", canonicalEntries(3)), applied("n2", canonicalEntries(3))},
				{applied("n1", nil), applied("n2", canonicalEntries(3))},                 // n1 restarted: lastApplied is 0 again
				{applied("n1", canonicalEntries(2)), applied("n2", canonicalEntries(3))}, // replaying
				{applied("n1", canonicalEntries(4)), applied("n2", canonicalEntries(4))},
			},
			wantErrAt: -1,
		},

		// Divergence: different entries applied at the same index.
		{
			name: "different command at the same index in one snapshot set",
			steps: [][]Snapshot{
				{
					applied("n1", []raft.Entry{appliedEntry(1, 1, "a"), appliedEntry(2, 1, "b")}),
					applied("n2", []raft.Entry{appliedEntry(1, 1, "a"), appliedEntry(2, 1, "X")}),
				},
			},
			wantErrAt: 0,
		},
		{
			name: "different term at the same index in one snapshot set",
			steps: [][]Snapshot{
				{
					applied("n1", []raft.Entry{appliedEntry(1, 1, "a"), appliedEntry(2, 1, "b")}),
					applied("n2", []raft.Entry{appliedEntry(1, 1, "a"), appliedEntry(2, 2, "b")}),
				},
			},
			wantErrAt: 0,
		},
		{
			name: "divergence across time after the first node left the snapshot set",
			steps: [][]Snapshot{
				{applied("n1", canonicalEntries(2)), applied("n2", nil)},
				{applied("n2", []raft.Entry{appliedEntry(1, 1, "a"), appliedEntry(2, 1, "X")})}, // n1 is gone, but index 2 was already applied as "b"
			},
			wantErrAt: 1,
		},
		{
			name: "a restarted node replays a different entry than it applied before",
			steps: [][]Snapshot{
				{applied("n1", canonicalEntries(3))},
				{applied("n1", nil)},
				{applied("n1", []raft.Entry{appliedEntry(1, 1, "a"), appliedEntry(2, 1, "b"), appliedEntry(3, 1, "Y")})},
			},
			wantErrAt: 2,
		},
		{
			name: "only one of three nodes disagrees",
			steps: [][]Snapshot{
				{
					applied("n1", canonicalEntries(3)),
					applied("n2", canonicalEntries(3)),
					applied("n3", []raft.Entry{appliedEntry(1, 1, "a"), appliedEntry(2, 1, "b"), appliedEntry(3, 2, "c")}),
				},
			},
			wantErrAt: 0,
		},

		// A node must apply index 1, 2, 3, ... exactly once each, in order.
		{
			name: "gap: index 2 skipped",
			steps: [][]Snapshot{
				{applied("n1", []raft.Entry{appliedEntry(1, 1, "a"), appliedEntry(3, 1, "c")})},
			},
			wantErrAt: 0,
		},
		{
			name: "gap: first applied entry is index 2",
			steps: [][]Snapshot{
				{applied("n1", []raft.Entry{appliedEntry(2, 1, "b"), appliedEntry(3, 1, "c")})},
			},
			wantErrAt: 0,
		},
		{
			name: "out of order",
			steps: [][]Snapshot{
				{applied("n1", []raft.Entry{appliedEntry(1, 1, "a"), appliedEntry(3, 1, "c"), appliedEntry(2, 1, "b")})},
			},
			wantErrAt: 0,
		},
		{
			name: "duplicate: index 2 applied twice in a row",
			steps: [][]Snapshot{
				{applied("n1", []raft.Entry{appliedEntry(1, 1, "a"), appliedEntry(2, 1, "b"), appliedEntry(2, 1, "b")})},
			},
			wantErrAt: 0,
		},
		{
			name: "duplicate: index 1 applied twice without a restart",
			steps: [][]Snapshot{
				{applied("n1", canonicalEntries(2))},
				{applied("n1", []raft.Entry{appliedEntry(1, 1, "a"), appliedEntry(2, 1, "b"), appliedEntry(1, 1, "a")})},
			},
			wantErrAt: 1,
		},
		{
			name: "hand-built bad history: clean for two steps, then a gap",
			steps: [][]Snapshot{
				{applied("n1", canonicalEntries(1)), applied("n2", canonicalEntries(1))},
				{applied("n1", canonicalEntries(2)), applied("n2", canonicalEntries(2))},
				{applied("n1", canonicalEntries(3)), applied("n2", []raft.Entry{appliedEntry(1, 1, "a"), appliedEntry(2, 1, "b"), appliedEntry(4, 1, "d")})},
			},
			wantErrAt: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewStateMachineSafety()
			for i, step := range tt.steps {
				err := c.Observe(step)
				switch {
				case i == tt.wantErrAt && err == nil:
					t.Fatalf("step %d: Observe returned nil, want a violation", i)
				case i != tt.wantErrAt && err != nil:
					t.Fatalf("step %d: Observe returned %v, want nil", i, err)
				}
				if i == tt.wantErrAt {
					return
				}
			}
		})
	}
}

func TestStateMachineSafetyFreshCheckerHasNoHistory(t *testing.T) {
	first := NewStateMachineSafety()
	if err := first.Observe([]Snapshot{applied("n1", canonicalEntries(2))}); err != nil {
		t.Fatalf("Observe() = %v", err)
	}
	second := NewStateMachineSafety()
	other := []raft.Entry{appliedEntry(1, 1, "a"), appliedEntry(2, 1, "X")}
	if err := second.Observe([]Snapshot{applied("n2", other)}); err != nil {
		t.Errorf("a new checker flagged an entry seen only by another checker: %v", err)
	}
}

func TestStateMachineSafetyDoesNotKeepCallersSlices(t *testing.T) {
	c := NewStateMachineSafety()
	entries := canonicalEntries(2)
	if err := c.Observe([]Snapshot{applied("n1", entries)}); err != nil {
		t.Fatalf("Observe() = %v", err)
	}
	// The caller reuses its buffer; the checker must still remember what was applied.
	entries[1].Cmd[0] = 'X'
	if err := c.Observe([]Snapshot{applied("n2", canonicalEntries(2))}); err != nil {
		t.Errorf("mutating a slice passed to an earlier Observe changed the checker's history: %v", err)
	}
}
