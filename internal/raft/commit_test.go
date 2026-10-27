package raft

import "testing"

// Figure 2, leader rule 4 (slide 9, section 5.4.2): set commitIndex = N if N > commitIndex,
// a majority has matchIndex >= N, and log[N].term == currentTerm. Counting replicas alone is not
// enough for entries from earlier terms (Figure 8); they commit only through a later current-term entry.
func TestNextCommitIndex(t *testing.T) {
	tests := []struct {
		name        string
		logTerms    []Term  // terms of the leader's entries 1, 2, 3, ...
		matches     []Index // one per node, the leader's own included
		currentTerm Term
		commit      Index // commitIndex before the call
		want        Index
	}{
		// The spec's three-node case, currentTerm 4.
		{"Figure 8: old-term entry on a majority is not committed", []Term{1, 2}, []Index{2, 2, 0}, 4, 0, 0},
		{"Figure 8: and does not move an existing commitIndex", []Term{1, 2}, []Index{2, 2, 0}, 4, 1, 1},
		{"current-term entry on a majority commits it and the older one before it", []Term{1, 2, 4}, []Index{3, 3, 0}, 4, 0, 3},
		{"majority only reaches the old-term entry", []Term{1, 2, 4}, []Index{3, 2, 0}, 4, 0, 0},

		// The five-node Figure 8, stage (c): S1 leads term 4; entry 2 (term 2) is on S1, S2, S3,
		// entry 3 (term 4) only on S1. Index 2 is on a majority but must stay uncommitted.
		{"Figure 8 stage (c), five nodes", []Term{1, 2, 4}, []Index{3, 2, 2, 1, 1}, 4, 1, 1},
		{"five nodes, current-term entry on three", []Term{1, 2, 4, 4}, []Index{4, 4, 3, 1, 0}, 4, 1, 3},

		// Plain current-term commits.
		{"current-term log, majority at 1", []Term{4, 4, 4}, []Index{2, 1, 0}, 4, 0, 1},
		{"only the leader has it", []Term{4, 4, 4}, []Index{3, 0, 0}, 4, 0, 0},
		{"empty log", nil, []Index{0, 0, 0}, 4, 0, 0},

		// Monotonic: commitIndex never goes down.
		{"already committed at the majority index", []Term{1, 2, 4}, []Index{3, 3, 0}, 4, 3, 3},
		{"commitIndex above what replies now show", []Term{4, 4, 4, 4, 4}, []Index{5, 3, 3}, 4, 5, 5},
		{"stale low matches after a restart of the counts", []Term{4, 4, 4}, []Index{3, 0, 0}, 4, 2, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := NewLog()
			for _, term := range tt.logTerms {
				log.Append(term, nil)
			}
			if got := nextCommitIndex(log, tt.matches, tt.currentTerm, tt.commit); got != tt.want {
				t.Errorf("nextCommitIndex(terms %v, matches %v, currentTerm %d, commit %d) = %d, want %d",
					tt.logTerms, tt.matches, tt.currentTerm, tt.commit, got, tt.want)
			}
		})
	}
}
