package raft

import "testing"

// Slide 6, Figure 2 AppendEntries rule 2: the follower accepts only if its log has an entry
// at prevIndex whose term is prevTerm. Index 0 is the sentinel, which every log has.
func TestLogOK(t *testing.T) {
	tests := []struct {
		name      string
		logTerms  []Term // follower's entries 1, 2, 3, ...
		prevIndex Index
		prevTerm  Term
		want      bool
	}{
		// prevIndex 0 is always OK.
		{"prev 0, empty log", nil, 0, 0, true},
		{"prev 0, non-empty log", []Term{1, 1, 2, 3}, 0, 0, true},
		{"prev 0 with a nonzero term still matches the sentinel", []Term{1, 1, 2, 3}, 0, 5, true},

		// A match passes.
		{"match at the last entry", []Term{1, 1, 2, 3}, 4, 3, true},
		{"match at the first entry", []Term{1, 1, 2, 3}, 1, 1, true},
		{"match in the middle, before a term change", []Term{1, 1, 2, 3}, 2, 1, true},
		{"match in the middle, after a term change", []Term{1, 1, 2, 3}, 3, 2, true},

		// Length first: an entry the follower does not have can never match.
		{"prev one past the end", []Term{1, 1, 2, 3}, 5, 3, false},
		{"prev far past the end", []Term{1, 1, 2, 3}, 10, 3, false},
		{"prev past the end with term 0 (would match a missing entry's zero term)", []Term{1, 1, 2, 3}, 5, 0, false},
		{"prev 1 on an empty log", nil, 1, 1, false},
		{"prev 1 with term 0 on an empty log", nil, 1, 0, false},

		// Then the term at prevIndex.
		{"term mismatch at the last entry", []Term{1, 1, 2, 3}, 4, 2, false},
		{"term mismatch in the middle", []Term{1, 1, 2, 3}, 2, 2, false},
		{"leader's prev term is older than the follower's", []Term{1, 1, 2, 3}, 3, 1, false},
		{"term 0 never matches a real entry", []Term{1, 1, 2, 3}, 1, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := NewLog()
			for _, term := range tt.logTerms {
				log.Append(term, nil)
			}
			if got := logOK(log, tt.prevIndex, tt.prevTerm); got != tt.want {
				t.Errorf("logOK(log of terms %v, prev (%d, %d)) = %v, want %v", tt.logTerms, tt.prevIndex, tt.prevTerm, got, tt.want)
			}
		})
	}
}
