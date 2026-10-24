package raft

import "testing"

// logWithTerms builds a log whose entry i (1-based) has term terms[i-1] and cmd "c<i>".
func logWithTerms(terms ...Term) *Log {
	l := NewLog()
	for i, term := range terms {
		l.Append(term, []byte{'c', byte('1' + i)})
	}
	return l
}

func TestLogEmpty(t *testing.T) {
	l := NewLog()
	if got := l.LastIndex(); got != 0 {
		t.Errorf("LastIndex() = %d, want 0", got)
	}
	if got := l.LastTerm(); got != 0 {
		t.Errorf("LastTerm() = %d, want 0", got)
	}
}

func TestLogAppend(t *testing.T) {
	tests := []struct {
		name          string
		terms         []Term
		wantLastIndex Index
		wantLastTerm  Term
	}{
		{"one entry", []Term{1}, 1, 1},
		{"same term", []Term{1, 1, 1}, 3, 1},
		{"rising terms", []Term{1, 1, 2, 4}, 4, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewLog()
			for i, term := range tt.terms {
				want := Index(i + 1)
				if got := l.Append(term, []byte{byte(i)}); got != want {
					t.Fatalf("Append #%d returned index %d, want %d", i+1, got, want)
				}
			}
			if got := l.LastIndex(); got != tt.wantLastIndex {
				t.Errorf("LastIndex() = %d, want %d", got, tt.wantLastIndex)
			}
			if got := l.LastTerm(); got != tt.wantLastTerm {
				t.Errorf("LastTerm() = %d, want %d", got, tt.wantLastTerm)
			}
			for i, term := range tt.terms {
				e, ok := l.Get(Index(i + 1))
				want := Entry{Term: term, Index: Index(i + 1), Cmd: []byte{byte(i)}}
				if !ok || !e.Equal(want) {
					t.Errorf("Get(%d) = %+v, %v; want %+v, true", i+1, e, ok, want)
				}
			}
		})
	}
}

func TestLogTerm(t *testing.T) {
	tests := []struct {
		name     string
		log      *Log
		index    Index
		wantTerm Term
		wantOK   bool
	}{
		{"sentinel on empty log", NewLog(), 0, 0, true},
		{"sentinel on non-empty log", logWithTerms(3, 3), 0, 0, true},
		{"first entry", logWithTerms(3, 3, 5), 1, 3, true},
		{"last entry", logWithTerms(3, 3, 5), 3, 5, true},
		{"one past the end", logWithTerms(3, 3, 5), 4, 0, false},
		{"far past the end", logWithTerms(3, 3, 5), 100, 0, false},
		{"empty log index 1", NewLog(), 1, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.log.Term(tt.index)
			if got != tt.wantTerm || ok != tt.wantOK {
				t.Errorf("Term(%d) = %d, %v; want %d, %v", tt.index, got, ok, tt.wantTerm, tt.wantOK)
			}
		})
	}
}

func TestLogGet(t *testing.T) {
	l := logWithTerms(1, 2)
	if _, ok := l.Get(3); ok {
		t.Error("Get(3) on a 2-entry log returned ok=true")
	}
	e, ok := l.Get(0)
	if !ok || e.Term != 0 || e.Index != 0 {
		t.Errorf("Get(0) = %+v, %v; want the sentinel (term 0, index 0), true", e, ok)
	}
}

func TestLogTruncateFrom(t *testing.T) {
	tests := []struct {
		name          string
		from          Index
		wantLastIndex Index
		wantLastTerm  Term
	}{
		{"middle removes it and the tail", 3, 2, 2},
		{"last entry only", 5, 4, 3},
		{"first entry empties the log", 1, 0, 0},
		{"one past the end is a no-op", 6, 5, 3},
		{"far past the end is a no-op", 50, 5, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := logWithTerms(1, 2, 2, 3, 3)
			l.TruncateFrom(tt.from)
			if got := l.LastIndex(); got != tt.wantLastIndex {
				t.Errorf("LastIndex() = %d, want %d", got, tt.wantLastIndex)
			}
			if got := l.LastTerm(); got != tt.wantLastTerm {
				t.Errorf("LastTerm() = %d, want %d", got, tt.wantLastTerm)
			}
			if _, ok := l.Get(tt.wantLastIndex + 1); ok {
				t.Errorf("Get(%d) still present after TruncateFrom(%d)", tt.wantLastIndex+1, tt.from)
			}
		})
	}
}

func TestLogAppendAfterTruncate(t *testing.T) {
	l := logWithTerms(1, 1, 1)
	l.TruncateFrom(2)
	if got := l.Append(2, []byte("new")); got != 2 {
		t.Errorf("Append after TruncateFrom(2) returned index %d, want 2", got)
	}
	if got, _ := l.Term(2); got != 2 {
		t.Errorf("Term(2) = %d, want 2 (the replacement entry)", got)
	}
}

// Slice(from, to) is half-open: entries from..to-1.
func TestLogSlice(t *testing.T) {
	tests := []struct {
		name     string
		from, to Index
		want     []Index
	}{
		{"middle", 2, 4, []Index{2, 3}},
		{"single entry", 3, 4, []Index{3}},
		{"whole log", 1, 5, []Index{1, 2, 3, 4}},
		{"empty range", 3, 3, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := logWithTerms(1, 1, 2, 2) // last index 4
			got := l.Slice(tt.from, tt.to)
			if len(got) != len(tt.want) {
				t.Fatalf("Slice(%d, %d) has %d entries, want %d", tt.from, tt.to, len(got), len(tt.want))
			}
			for i, idx := range tt.want {
				if got[i].Index != idx {
					t.Errorf("Slice(%d, %d)[%d].Index = %d, want %d", tt.from, tt.to, i, got[i].Index, idx)
				}
			}
		})
	}
}

func TestLogSliceReturnsCopy(t *testing.T) {
	l := logWithTerms(1, 1, 2)
	s := l.Slice(1, 4)
	s[0].Term = 99
	s[1] = Entry{Term: 99, Index: 99}
	for i, wantTerm := range []Term{1, 1, 2} {
		if got, _ := l.Term(Index(i + 1)); got != wantTerm {
			t.Errorf("after mutating the slice, Term(%d) = %d, want %d", i+1, got, wantTerm)
		}
	}
}
