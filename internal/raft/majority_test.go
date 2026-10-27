package raft

import "testing"

// majorityMatch takes one matchIndex per node, the leader's own included, and returns the
// largest index that a majority of nodes hold: sorted descending, the value at position quorum-1.
func TestMajorityMatch(t *testing.T) {
	tests := []struct {
		name    string
		matches []Index
		want    Index
	}{
		// Three nodes: quorum 2.
		{"3 nodes, two at the top", []Index{5, 5, 0}, 5},
		{"3 nodes, all different", []Index{5, 3, 0}, 3},
		{"3 nodes, nothing replicated", []Index{0, 0, 0}, 0},
		{"3 nodes, all equal", []Index{4, 4, 4}, 4},
		{"3 nodes, only the leader has it", []Index{6, 0, 0}, 0},
		{"3 nodes, unsorted input", []Index{0, 5, 3}, 3},

		// Five nodes: quorum 3.
		{"5 nodes, spec example", []Index{9, 7, 7, 2, 0}, 7},
		{"5 nodes, unsorted spec example", []Index{2, 7, 0, 9, 7}, 7},
		{"5 nodes, two ahead is not a majority", []Index{9, 9, 1, 1, 1}, 1},
		{"5 nodes, three ahead is", []Index{9, 9, 9, 1, 1}, 9},
		{"5 nodes, all different", []Index{5, 4, 3, 2, 1}, 3},
		{"5 nodes, nothing replicated", []Index{0, 0, 0, 0, 0}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := majorityMatch(tt.matches); got != tt.want {
				t.Errorf("majorityMatch(%v) = %d, want %d", tt.matches, got, tt.want)
			}
		})
	}
}

// The caller passes its own matchIndex values; sorting must happen on a copy.
func TestMajorityMatchLeavesInputAlone(t *testing.T) {
	in := []Index{2, 7, 0, 9, 7}
	majorityMatch(in)
	want := []Index{2, 7, 0, 9, 7}
	for i := range in {
		if in[i] != want[i] {
			t.Fatalf("input changed to %v, want %v", in, want)
		}
	}
}
