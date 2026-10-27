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

	// leaderID is the leader of currentTerm, once one has been heard from.
	leaderID NodeID

	// Leader state (Figure 2), reinitialized on every election win.
	nextIndex         map[NodeID]Index // next log index to send each peer
	matchIndex        map[NodeID]Index // highest index known replicated on each peer
	heartbeatDeadline int              // tick at which the leader next sends heartbeats
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

// Tick advances logical time by one tick. A leader sends heartbeats every HeartbeatInterval
// and has no election timer; a follower or candidate whose deadline has passed starts an election.
func (n *Node) Tick() []Message {
	n.now++
	if n.role == Leader {
		if n.now >= n.heartbeatDeadline {
			n.heartbeatDeadline = n.now + n.cfg.HeartbeatInterval
			return n.sendHeartbeats()
		}
		return nil
	}
	if n.now >= n.electionDeadline {
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

// ProposeResult says where a proposed command was placed, or who to ask instead.
type ProposeResult struct {
	Index      Index
	Term       Term
	IsLeader   bool
	LeaderHint NodeID // the leader a follower knows of; empty if unknown
}

// Propose is slide 4: a leader appends cmd to its own log in its current term and saves it.
// Anyone else returns IsLeader false; only a follower can name a leader, since a candidate's
// last known leader belongs to an older term. Replication happens on the next heartbeat.
func (n *Node) Propose(cmd []byte) (ProposeResult, []Message) {
	if n.role != Leader {
		if n.role == Follower {
			return ProposeResult{LeaderHint: n.leaderID}, nil
		}
		return ProposeResult{}, nil
	}
	e := Entry{Term: n.currentTerm, Index: n.log.LastIndex() + 1, Cmd: append([]byte(nil), cmd...)}
	log := append(n.log.Slice(1, n.log.LastIndex()+1), e)
	if err := n.storage.Save(PersistentState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor, Log: log}); err != nil {
		return ProposeResult{}, nil // not accepted; nothing appended, nothing sent
	}
	n.log.Append(e.Term, e.Cmd)
	n.matchIndex[n.id] = e.Index
	return ProposeResult{Index: e.Index, Term: e.Term, IsLeader: true}, nil
}

// Snapshot is a read-only copy of a node's state for tests and the safety checkers.
type Snapshot struct {
	ID          NodeID
	Term        Term
	Role        Role
	VotedFor    NodeID
	LeaderID    NodeID  // leader of Term, if known
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
		LeaderID:    n.leaderID,
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
	n.currentTerm, n.votedFor, n.role, n.leaderID = t, "", Follower, ""
	return true
}

// Step handles one incoming message and returns the messages to send in response.
func (n *Node) Step(m Message) []Message {
	switch body := m.Body.(type) {
	case RequestVoteArgs:
		return n.handleRequestVote(m.From, body)
	case RequestVoteReply:
		return n.handleVoteReply(m.From, body)
	case AppendEntriesArgs:
		return n.handleAppendEntries(m.From, body)
	case AppendEntriesReply:
		return n.handleAppendEntriesReply(body)
	}
	return nil
}

// handleAppendEntries implements the term and role part of AppendEntries (slide 6, Figure 2
// rule 1): reject a stale term; otherwise the sender is this term's leader, so record it,
// step down if a candidate, and reset the election timer.
// ponytail: no log consistency check or merge yet (T3.3 to T3.5), so every current-term
// request succeeds. Only empty-log heartbeats with prevLogIndex 0 are correct until then.
func (n *Node) handleAppendEntries(from NodeID, args AppendEntriesArgs) []Message {
	n.adoptTermIfHigher(args.Term)
	if args.Term > n.currentTerm {
		return nil // the higher term could not be saved
	}
	reply := AppendEntriesReply{Term: n.currentTerm, PrevLogIndex: args.PrevLogIndex}
	if args.Term == n.currentTerm {
		n.role = Follower
		n.leaderID = args.LeaderID
		n.resetElectionTimer()
		reply.Success = true
	}
	return []Message{{From: n.id, To: from, Body: reply}}
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
		return n.becomeLeader()
	}
	return nil
}

// becomeLeader sets up Figure 2's leader state and heartbeats at once, so followers
// hear from the new leader before their own election timers run out (section 5.2).
func (n *Node) becomeLeader() []Message {
	n.role = Leader
	n.leaderID = n.id
	trace(n.tracer, Event{Node: n.id, Term: n.currentTerm, Role: n.role, Kind: "became-leader"})
	n.nextIndex = make(map[NodeID]Index, len(n.peers))
	n.matchIndex = make(map[NodeID]Index, len(n.peers))
	for _, p := range n.peers {
		n.nextIndex[p] = n.log.LastIndex() + 1
		n.matchIndex[p] = 0
	}
	n.heartbeatDeadline = n.now + n.cfg.HeartbeatInterval
	return n.sendHeartbeats()
}

// sendHeartbeats sends every peer an empty AppendEntries anchored at its nextIndex.
// Carrying entries is T3.2.
func (n *Node) sendHeartbeats() []Message {
	out := make([]Message, 0, len(n.peers))
	for _, p := range n.peers {
		prev := n.nextIndex[p] - 1
		prevTerm, _ := n.log.Term(prev)
		args := AppendEntriesArgs{Term: n.currentTerm, LeaderID: n.id, PrevLogIndex: prev, PrevLogTerm: prevTerm, LeaderCommit: n.commitIndex}
		out = append(out, Message{From: n.id, To: p, Body: args})
	}
	return out
}

// buildAppendEntries is slide 5 and Figure 2's leader rule: send peer every entry from its
// nextIndex on, anchored at the entry just before it. Entries are deep copies, because the
// args travel to another node and must not share the leader's log.
func (n *Node) buildAppendEntries(peer NodeID) AppendEntriesArgs {
	next := n.nextIndex[peer]
	prevTerm, _ := n.log.Term(next - 1)
	entries := n.log.Slice(next, n.log.LastIndex()+1)
	for i := range entries {
		entries[i].Cmd = append([]byte(nil), entries[i].Cmd...)
	}
	return AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: next - 1,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
}

// handleAppendEntriesReply only checks the term for now: a higher term means a newer
// leader exists, so step down. Acting on success and failure is T3.6 and T3.7.
func (n *Node) handleAppendEntriesReply(r AppendEntriesReply) []Message {
	n.adoptTermIfHigher(r.Term)
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
