package lock

import (
	"errors"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

var (
	ErrNotLeader     = errors.New("not the leader")
	ErrLeaderChanged = errors.New("leadership changed before the command committed")
	ErrTimeout       = errors.New("timed out waiting for the command to apply")
)

// DefaultTimeout is how many ticks Submit waits for its command to apply.
const DefaultTimeout = 2000

// Call is a Submit in flight. The core runs without goroutines, so Submit cannot block; the call
// completes as the server is ticked and stepped.
type Call struct {
	done     bool
	result   Result
	err      error
	hint     raft.NodeID
	deadline int
}

func (c *Call) Done() bool                 { return c.done }
func (c *Call) Result() Result             { return c.result }
func (c *Call) Err() error                 { return c.err }
func (c *Call) Hint() raft.NodeID          { return c.hint }
func (c *Call) finish(r Result, err error) { c.done, c.result, c.err = true, r, err }

// NewCall returns a finished Call, for a Transport that gets its answer from elsewhere.
func NewCall(r Result, err error, hint raft.NodeID) *Call {
	c := &Call{hint: hint}
	c.finish(r, err)
	return c
}

// waitKey names the log entry a Submit is waiting for. The term matters: if a different entry
// ends up at that index, the command was lost with its leader and will never apply.
type waitKey struct {
	index raft.Index
	term  raft.Term
}

// Server is the lock service on one node: a Raft node, the lock state machine it applies to,
// the leases that expire locks, and the Submits waiting for their commands to apply.
type Server struct {
	node    *raft.Node
	sm      *LockSM
	leases  *Leases
	onApply func(raft.Entry)
	now     int
	waiters map[waitKey]*Call
	reads   []*pendingRead // ReadIndex reads not yet answered
	round   uint64         // last ReadIndex confirmation round
	id      raft.NodeID
	peers   []raft.NodeID
	cfg     raft.Config

	Timeout int // ticks a Submit waits before ErrTimeout
}

// NewServer takes raft.NewNode's arguments and builds the node itself; onApply, if set, also sees
// every applied entry.
func NewServer(cfg raft.Config, id raft.NodeID, peers []raft.NodeID, st raft.Storage, tr raft.Tracer, to raft.TimeoutSource, onApply func(raft.Entry)) (*Server, error) {
	s := &Server{id: id, peers: append([]raft.NodeID(nil), peers...), cfg: cfg, sm: NewLockSM(), leases: NewLeases(), onApply: onApply, waiters: map[waitKey]*Call{}, Timeout: DefaultTimeout}
	n, err := raft.NewNode(cfg, id, peers, st, tr, to, s.apply)
	if err != nil {
		return nil, err
	}
	s.node = n
	return s, nil
}

// Submit proposes cmd for clientID's request seq. On a follower it answers at once with
// ErrNotLeader and the leader it knows of. On the leader it waits until the entry is applied.
func (s *Server) Submit(clientID string, seq uint64, cmd Command) (*Call, []raft.Message) {
	cmd.ClientID, cmd.Seq = clientID, seq
	call := &Call{deadline: s.now + s.Timeout}
	b, err := Encode(cmd)
	if err != nil {
		call.finish(Result{}, err)
		return call, nil
	}
	res, msgs := s.node.Propose(b)
	if !res.IsLeader {
		call.hint = res.LeaderHint
		call.finish(Result{}, ErrNotLeader)
		return call, msgs
	}
	s.waiters[waitKey{res.Index, res.Term}] = call
	return call, msgs
}

// Tick advances the node, runs the leases on the leader, and times out old waiters.
func (s *Server) Tick() []raft.Message {
	s.now++
	out := s.node.Tick()
	// ponytail: Snapshot copies the whole log every tick just to read the role; add a cheap
	// raft.Node.Role() if this shows up in long simulations.
	leader := s.node.Snapshot().Role == raft.Leader
	for _, c := range s.leases.Tick(leader) {
		b, err := Encode(c)
		if err != nil {
			continue
		}
		_, msgs := s.node.Propose(b)
		out = append(out, msgs...)
	}
	s.checkWaiters()
	return append(out, s.progressReads()...)
}

// Step handles the server's own ReadIndex messages and passes everything else to the node.
func (s *Server) Step(m raft.Message) []raft.Message {
	switch b := m.Body.(type) {
	case ReadIndexPing:
		return s.handlePing(m.From, b)
	case ReadIndexPong:
		s.handlePong(m.From, b)
		return s.progressReads()
	}
	out := s.node.Step(m)
	s.checkWaiters()
	return append(out, s.progressReads()...)
}

func (s *Server) Snapshot() raft.Snapshot { return s.node.Snapshot() }

// Propose passes raw bytes to the node, bypassing waiters; it lets the harness treat a Server as a node.
func (s *Server) Propose(cmd []byte) (raft.ProposeResult, []raft.Message) { return s.node.Propose(cmd) }

// Waiting is how many Submits are still waiting for their command to apply.
func (s *Server) Waiting() int { return len(s.waiters) }

// Dump is the lock state machine's state, for comparing servers.
func (s *Server) Dump() string { return s.sm.Dump() }

// apply is the node's onApply: run the command, track its lease, and answer the Submit waiting for
// exactly this entry. No-ops are never passed here.
func (s *Server) apply(e raft.Entry) {
	r := s.sm.Apply(e.Index, e.Cmd)
	if c, err := Decode(e.Cmd); err == nil {
		s.leases.Applied(c, r)
	}
	if call, ok := s.waiters[waitKey{e.Index, e.Term}]; ok {
		call.finish(r, nil)
		delete(s.waiters, waitKey{e.Index, e.Term})
	}
	if s.onApply != nil {
		s.onApply(e)
	}
}

// checkWaiters ends Submits that can no longer succeed: their index was applied with another
// entry (possibly a no-op, which apply never sees), or their time ran out.
func (s *Server) checkWaiters() {
	if len(s.waiters) == 0 {
		return
	}
	snap := s.node.Snapshot()
	for k, call := range s.waiters {
		switch {
		case snap.LastApplied >= k.index && (int(k.index) > len(snap.Log) || snap.Log[k.index-1].Term != k.term):
			call.finish(Result{}, ErrLeaderChanged)
			delete(s.waiters, k)
		case s.now >= call.deadline:
			call.finish(Result{}, ErrTimeout)
			delete(s.waiters, k)
		}
	}
}
