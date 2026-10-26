package raft

import "fmt"

// Node is the deterministic Raft core: no goroutines, clocks, or network. It is not safe for concurrent use.
type Node struct {
	cfg      Config
	id       NodeID
	peers    []NodeID
	storage  Storage
	tracer   Tracer
	timeouts TimeoutSource
	onApply  func(Entry)

	// Persistent state (Figure 2): saved before any reply leaves.
	currentTerm Term
	votedFor    NodeID
	log         *Log

	// Volatile state: reset to zero and Follower on every start.
	role        Role
	commitIndex Index
	lastApplied Index
}

// NewNode builds a follower from whatever storage holds; commitIndex and lastApplied start at 0.
func NewNode(cfg Config, id NodeID, peers []NodeID, storage Storage, tracer Tracer, timeouts TimeoutSource, onApply func(Entry)) (*Node, error) {
	state, err := storage.Load()
	if err != nil {
		return nil, fmt.Errorf("load persisted state: %w", err)
	}
	log := NewLog()
	for _, e := range state.Log {
		log.Append(e.Term, e.Cmd)
	}
	return &Node{
		cfg:         cfg,
		id:          id,
		peers:       append([]NodeID(nil), peers...),
		storage:     storage,
		tracer:      tracer,
		timeouts:    timeouts,
		onApply:     onApply,
		currentTerm: state.CurrentTerm,
		votedFor:    state.VotedFor,
		log:         log,
		role:        Follower,
	}, nil
}

// Snapshot is a read-only copy of a node's state for tests and the safety checkers.
type Snapshot struct {
	ID          NodeID
	Term        Term
	Role        Role
	VotedFor    NodeID
	Log         []Entry // real entries from index 1, no sentinel
	CommitIndex Index
	LastApplied Index
}

func (n *Node) Snapshot() Snapshot {
	entries := n.log.Slice(1, n.log.LastIndex()+1)
	for i := range entries {
		entries[i].Cmd = append([]byte(nil), entries[i].Cmd...)
	}
	return Snapshot{
		ID:          n.id,
		Term:        n.currentTerm,
		Role:        n.role,
		VotedFor:    n.votedFor,
		Log:         entries,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
	}
}
