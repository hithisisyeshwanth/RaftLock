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

// adoptTermIfHigher is Figure 2's "all servers" rule: on seeing a higher term in any RPC or
// reply, adopt it, clear the vote, and become a follower. It saves before changing memory,
// so an unsaved term is never acted on.
// ponytail: a failed Save returns false, which callers cannot tell from "term not higher".
// T2.6 must surface the error before any reply path relies on this.
func (n *Node) adoptTermIfHigher(t Term) bool {
	if t <= n.currentTerm {
		return false
	}
	err := n.storage.Save(PersistentState{CurrentTerm: t, Log: n.log.Slice(1, n.log.LastIndex()+1)})
	if err != nil {
		return false
	}
	n.currentTerm, n.votedFor, n.role = t, "", Follower
	return true
}

// Step handles one incoming message and returns the messages to send in response.
func (n *Node) Step(m Message) []Message {
	switch body := m.Body.(type) {
	case RequestVoteArgs:
		return n.handleRequestVote(m.From, body)
	}
	return nil
}

// handleRequestVote implements only the term rule (Figure 2, RequestVote rule 1).
// Granting votes comes with T2.4 and T2.5, so the reply is always a denial for now.
func (n *Node) handleRequestVote(from NodeID, args RequestVoteArgs) []Message {
	n.adoptTermIfHigher(args.Term) // before the reply, so it carries the new term
	reply := RequestVoteReply{Term: n.currentTerm}
	return []Message{{From: n.id, To: from, Body: reply}}
}
