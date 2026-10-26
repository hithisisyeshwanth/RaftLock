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

// handleRequestVote implements Figure 2's RequestVote rules 1 and 2: deny a stale term, and
// grant at most one vote per term (re-granting the same candidate) to an up-to-date candidate.
func (n *Node) handleRequestVote(from NodeID, args RequestVoteArgs) []Message {
	n.adoptTermIfHigher(args.Term) // before the reply, so it carries the new term
	reply := RequestVoteReply{Term: n.currentTerm}
	if args.Term == n.currentTerm && (n.votedFor == "" || n.votedFor == args.CandidateID) &&
		n.candidateUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = n.recordVote(args.CandidateID)
	}
	return []Message{{From: n.id, To: from, Body: reply}}
}

// candidateUpToDate is section 5.4.1: compare last terms first, and only on a tie compare lengths.
func (n *Node) candidateUpToDate(lastIdx Index, lastTerm Term) bool {
	if lastTerm != n.log.LastTerm() {
		return lastTerm > n.log.LastTerm()
	}
	return lastIdx >= n.log.LastIndex()
}

// recordVote saves the vote before setting it in memory, so a granted vote is never lost
// in a crash. A repeat vote for the same candidate changes nothing and is not saved again.
func (n *Node) recordVote(candidate NodeID) bool {
	if n.votedFor == candidate {
		return true
	}
	err := n.storage.Save(PersistentState{CurrentTerm: n.currentTerm, VotedFor: candidate, Log: n.log.Slice(1, n.log.LastIndex()+1)})
	if err != nil {
		return false
	}
	n.votedFor = candidate
	return true
}
