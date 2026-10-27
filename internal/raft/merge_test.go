package raft

import "testing"

func en(index Index, term Term, cmd string) Entry {
	return Entry{Term: term, Index: index, Cmd: []byte(cmd)}
}

// base is a follower log: 1:(t1,a) 2:(t1,b) 3:(t2,c) 4:(t2,d).
func base() []Entry {
	return []Entry{en(1, 1, "a"), en(2, 1, "b"), en(3, 2, "c"), en(4, 2, "d")}
}

// Figure 2, AppendEntries rules 3 and 4, slide 7. Every case has logOK(prev) true, as the
// handler checks that first. Logs are entries from index 1, with no sentinel.
func TestMergeEntries(t *testing.T) {
	tests := []struct {
		name        string
		log         []Entry
		prevIndex   Index
		entries     []Entry
		want        []Entry
		wantChanged bool
	}{
		// Already have them: nothing changes, so a duplicated message is harmless.
		{"exact duplicate of the whole log", base(), 0, base(), base(), false},
		{"duplicate of a middle run", base(), 1, []Entry{en(2, 1, "b"), en(3, 2, "c")}, base(), false},
		{"duplicate of the last entry", base(), 3, []Entry{en(4, 2, "d")}, base(), false},

		// A delayed old message carries a shorter prefix. Its entries all match, so the
		// later entries must stay: truncating here could delete committed entries.
		{"stale shorter prefix from the start", base(), 0, []Entry{en(1, 1, "a"), en(2, 1, "b")}, base(), false},
		{"stale shorter run in the middle", base(), 2, []Entry{en(3, 2, "c")}, base(), false},

		// A conflict (same index, different term) truncates from there and takes the rest.
		{"conflict at the first new entry", base(), 2,
			[]Entry{en(3, 3, "X"), en(4, 3, "Y"), en(5, 3, "Z")},
			[]Entry{en(1, 1, "a"), en(2, 1, "b"), en(3, 3, "X"), en(4, 3, "Y"), en(5, 3, "Z")}, true},
		{"conflict after a matching entry, shorter than the old log", base(), 1,
			[]Entry{en(2, 1, "b"), en(3, 3, "X")},
			[]Entry{en(1, 1, "a"), en(2, 1, "b"), en(3, 3, "X")}, true},
		{"conflict at index 1", base(), 0,
			[]Entry{en(1, 5, "Q")},
			[]Entry{en(1, 5, "Q")}, true},

		// Nothing to merge.
		{"empty entries at the end", base(), 4, nil, base(), false},
		{"empty entries in the middle (a heartbeat)", base(), 2, nil, base(), false},
		{"empty entries on an empty log", nil, 0, nil, nil, false},

		// New entries past the end are appended.
		{"append right after the end", base(), 4,
			[]Entry{en(5, 2, "e"), en(6, 3, "f")},
			append(base(), en(5, 2, "e"), en(6, 3, "f")), true},
		{"overlap then extend", base(), 2,
			[]Entry{en(3, 2, "c"), en(4, 2, "d"), en(5, 3, "e")},
			append(base(), en(5, 3, "e")), true},
		{"empty log takes everything", nil, 0,
			[]Entry{en(1, 1, "a"), en(2, 1, "b")},
			[]Entry{en(1, 1, "a"), en(2, 1, "b")}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, changed := mergeEntries(tt.log, tt.prevIndex, tt.entries)
			if !entriesEqual(got, tt.want) {
				t.Errorf("merged log = %+v\nwant          %+v", got, tt.want)
			}
			if changed != tt.wantChanged {
				t.Errorf("changed = %v, want %v", changed, tt.wantChanged)
			}
		})
	}
}

// mergeEntries is pure: the handler must be able to save the merged log before it touches its
// own (persist before reply), so the inputs must come back untouched.
func TestMergeEntriesLeavesInputsAlone(t *testing.T) {
	log := base()
	entries := []Entry{en(3, 3, "X"), en(4, 3, "Y"), en(5, 3, "Z")}

	merged, _ := mergeEntries(log, 2, entries)
	merged[0].Term = 99
	merged[2].Term = 99

	if !entriesEqual(log, base()) {
		t.Errorf("input log changed: %+v", log)
	}
	if !entriesEqual(entries, []Entry{en(3, 3, "X"), en(4, 3, "Y"), en(5, 3, "Z")}) {
		t.Errorf("input entries changed: %+v", entries)
	}
}

// Applying the same message twice gives the same log as applying it once.
func TestMergeEntriesIdempotent(t *testing.T) {
	entries := []Entry{en(3, 3, "X"), en(4, 3, "Y")}
	once, _ := mergeEntries(base(), 2, entries)
	twice, changed := mergeEntries(once, 2, entries)
	if !entriesEqual(once, twice) || changed {
		t.Errorf("second merge = %+v (changed %v), want the same log %+v unchanged", twice, changed, once)
	}
}
