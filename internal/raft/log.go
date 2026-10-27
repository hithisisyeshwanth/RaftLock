package raft

// Log is 1-based: entries[0] is a sentinel with term 0, so length-1 is the last index.
type Log struct {
	entries []Entry
}

func NewLog() *Log {
	return &Log{entries: []Entry{{}}}
}

func (l *Log) LastIndex() Index {
	return Index(len(l.entries) - 1)
}

func (l *Log) LastTerm() Term {
	return l.entries[len(l.entries)-1].Term
}

func (l *Log) Term(i Index) (Term, bool) {
	e, ok := l.Get(i)
	return e.Term, ok
}

func (l *Log) Get(i Index) (Entry, bool) {
	if i > l.LastIndex() {
		return Entry{}, false
	}
	return l.entries[i], true
}

// Append stamps the entry with the next index and returns it.
func (l *Log) Append(term Term, cmd []byte) Index {
	i := l.LastIndex() + 1
	l.entries = append(l.entries, Entry{Term: term, Index: i, Cmd: cmd})
	return i
}

// TruncateFrom removes entry i and everything after it. The sentinel is never removed.
func (l *Log) TruncateFrom(i Index) {
	if i >= 1 && i <= l.LastIndex() {
		l.entries = l.entries[:i]
	}
}

// Slice returns a copy of entries from..to-1.
func (l *Log) Slice(from, to Index) []Entry {
	return append([]Entry(nil), l.entries[from:to]...)
}

// logOK is slide 6 and Figure 2's AppendEntries rule 2: the follower's log must hold an entry
// at prevIndex with term prevTerm. Length is checked first, because Term reports 0 for a
// missing entry, which would otherwise match a prevTerm of 0. Index 0 is the sentinel, always present.
func logOK(log *Log, prevIndex Index, prevTerm Term) bool {
	if prevIndex == 0 {
		return true
	}
	term, ok := log.Term(prevIndex)
	return ok && term == prevTerm
}

// mergeEntries is Figure 2's AppendEntries rules 3 and 4 (slide 7), given that logOK(prevIndex)
// holds. log and the result are entries from index 1 with no sentinel. Entries the log already
// has are skipped; the log is truncated only at the first index whose term conflicts, never just
// because a message is shorter (a delayed old message must not delete later entries). The inputs
// are not modified, so a caller can save the result before replacing its own log.
func mergeEntries(log []Entry, prevIndex Index, entries []Entry) ([]Entry, bool) {
	merged := append([]Entry(nil), log...)
	for i, e := range entries {
		pos := int(prevIndex) + i // position of e in merged
		if pos < len(merged) {
			if merged[pos].Term == e.Term {
				continue
			}
			merged = merged[:pos]
		}
		return append(merged, entries[i:]...), true
	}
	return merged, false
}
