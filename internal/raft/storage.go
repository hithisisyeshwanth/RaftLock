package raft

type PersistentState struct {
	CurrentTerm Term
	VotedFor    NodeID
	Log         []Entry // real entries from index 1, no sentinel
}

type Storage interface {
	Save(PersistentState) error
	Load() (PersistentState, error)
}

// MemStorage deep-copies on Save and Load, and records every Save for spy tests.
type MemStorage struct {
	state   PersistentState
	history []PersistentState
}

func NewMemStorage() *MemStorage {
	return &MemStorage{}
}

func (m *MemStorage) Save(s PersistentState) error {
	m.state = cloneState(s)
	m.history = append(m.history, cloneState(s))
	return nil
}

func (m *MemStorage) Load() (PersistentState, error) {
	return cloneState(m.state), nil
}

func (m *MemStorage) SaveCount() int {
	return len(m.history)
}

func (m *MemStorage) History() []PersistentState {
	out := make([]PersistentState, len(m.history))
	for i, s := range m.history {
		out[i] = cloneState(s)
	}
	return out
}

func cloneState(s PersistentState) PersistentState {
	if s.Log != nil {
		log := make([]Entry, len(s.Log))
		for i, e := range s.Log {
			e.Cmd = append([]byte(nil), e.Cmd...) // copy the whole entry, so fields like Noop survive
			log[i] = e
		}
		s.Log = log
	}
	return s
}
