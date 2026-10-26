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

	// Logical time in ticks since start, and the tick at which a follower or candidate times out.
	now              int
	electionDeadline int

	// votes is the set of nodes that granted this candidate a vote in currentTerm, itself included (slide 3).
	votes map[NodeID]bool
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
	n := &Node{
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
	}
	n.resetElectionTimer()
	return n, nil
}

// resetElectionTimer draws a fresh timeout, so nodes that reset together do not expire together.
func (n *Node) resetElectionTimer() {
	n.electionDeadline = n.now + n.timeouts.Next()
}

// Tick advances logical time by one tick. A follower or candidate whose deadline has
// passed reports an election timeout; a leader has no election timer.
func (n *Node) Tick() []Message {
	n.now++
	if n.role != Leader && n.now >= n.electionDeadline {
		trace(n.tracer, Event{Node: n.id, Term: n.currentTerm, Role: n.role, Kind: "election-timeout"})
		return n.startElection()
	}
	return nil
}

// startElection is section 5.2: move to the next term, vote for itself, and ask every peer
// for a vote. The new term and self-vote are saved first; if that fails nothing changes and
// nothing is sent, and the still-passed deadline makes the next tick try again.
func (n *Node) startElection() []Message {
	term := n.currentTerm + 1
	err := n.storage.Save(PersistentState{CurrentTerm: term, VotedFor: n.id, Log: n.log.Slice(1, n.log.LastIndex()+1)})
	if err != nil {
		return nil
	}
	n.currentTerm, n.votedFor, n.role = term, n.id, Candidate
	n.votes = map[NodeID]bool{n.id: true}
	n.resetElectionTimer()

	args := RequestVoteArgs{Term: term, CandidateID: n.id, LastLogIndex: n.log.LastIndex(), LastLogTerm: n.log.LastTerm()}
	out := make([]Message, 0, len(n.peers))
	for _, p := range n.peers {
		out = append(out, Message{From: n.id, To: p, Body: args})
	}
	return out
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
// so an unsaved term is never acted on. A failed Save also returns false; a caller that must
// not reply in that case checks whether t is still above currentTerm afterwards.
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
	case RequestVoteReply:
		return n.handleVoteReply(m.From, body)
	}
	return nil
}

// handleVoteReply counts a grant toward this candidate's election. Replies are one-way and may be
// stale, duplicated, or reordered, so only a candidate counts them, only for its current term, and
// only once per peer; a reply with a higher term steps the node down instead.
func (n *Node) handleVoteReply(from NodeID, r RequestVoteReply) []Message {
	if n.adoptTermIfHigher(r.Term) || r.Term > n.currentTerm {
		return nil // stepped down, or the higher term could not be saved
	}
	if n.role != Candidate || r.Term != n.currentTerm || !r.VoteGranted || !n.isPeer(from) {
		return nil
	}
	n.votes[from] = true
	if len(n.votes) > (len(n.peers)+1)/2 {
		n.role = Leader
	}
	return nil
}

func (n *Node) isPeer(id NodeID) bool {
	for _, p := range n.peers {
		if p == id {
			return true
		}
	}
	return false
}

// handleRequestVote implements Figure 2's RequestVote rules 1 and 2: deny a stale term, and
// grant at most one vote per term (re-granting the same candidate) to an up-to-date candidate.
// Every write is saved before the reply is built; if a save fails, nothing is sent.
func (n *Node) handleRequestVote(from NodeID, args RequestVoteArgs) []Message {
	n.adoptTermIfHigher(args.Term) // before the reply, so it carries the new term
	if args.Term > n.currentTerm {
		return nil // the higher term could not be saved
	}
	reply := RequestVoteReply{Term: n.currentTerm}
	if args.Term == n.currentTerm && (n.votedFor == "" || n.votedFor == args.CandidateID) &&
		n.candidateUpToDate(args.LastLogIndex, args.LastLogTerm) {
		if err := n.recordVote(args.CandidateID); err != nil {
			return nil
		}
		reply.VoteGranted = true
		n.resetElectionTimer() // Figure 2: only granting a vote resets it, never denying

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
func (n *Node) recordVote(candidate NodeID) error {
	if n.votedFor == candidate {
		return nil
	}
	err := n.storage.Save(PersistentState{CurrentTerm: n.currentTerm, VotedFor: candidate, Log: n.log.Slice(1, n.log.LastIndex()+1)})
	if err != nil {
		return err
	}
	n.votedFor = candidate
	return nil
}
