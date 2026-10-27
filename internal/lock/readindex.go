package lock

import "github.com/hithisisyeshwanth/RaftLock/internal/raft"

// ReadIndexPing and ReadIndexPong are the lock server's own leadership check for ReadIndex reads.
// Raft's heartbeat replies cannot serve: they carry no round, so a reply could answer a heartbeat
// sent before the read arrived. These carry one, and the Raft core never sees them.
type ReadIndexPing struct {
	Term  raft.Term
	Round uint64
}

type ReadIndexPong struct {
	Term  raft.Term
	Round uint64
}

// pendingRead is a ReadIndex read the leader has not answered yet.
type pendingRead struct {
	call      *Call
	lock      string
	term      raft.Term
	ready     bool       // readIndex recorded and a confirmation round started
	readIndex raft.Index // commitIndex when the read became ready
	round     uint64
	acks      map[raft.NodeID]bool // peers that confirmed round
	sentAt    int
}

// Read answers a lock-status query without a log entry (ReadIndex, section 8). The leader answers
// only after it has committed an entry from its current term, recorded readIndex = commitIndex,
// had a majority confirm it is still the leader in a round sent after that, and applied up to
// readIndex. A follower answers at once with ErrNotLeader.
func (s *Server) Read(lockName string) (*Call, []raft.Message) {
	snap := s.node.Snapshot()
	call := &Call{deadline: s.now + s.Timeout}
	if snap.Role != raft.Leader {
		if snap.Role == raft.Follower {
			call.hint = snap.LeaderID
		}
		call.finish(Result{}, ErrNotLeader)
		return call, nil
	}
	s.reads = append(s.reads, &pendingRead{call: call, lock: lockName, term: snap.Term, acks: map[raft.NodeID]bool{}})
	return call, s.progressReads()
}

// progressReads moves every pending read as far as it can go, and returns any pings to send.
func (s *Server) progressReads() []raft.Message {
	if len(s.reads) == 0 {
		return nil
	}
	snap := s.node.Snapshot()
	var out []raft.Message
	kept := s.reads[:0]
	for _, r := range s.reads {
		switch {
		case snap.Role != raft.Leader || snap.Term != r.term:
			r.call.finish(Result{}, ErrLeaderChanged)
			continue
		case s.now >= r.call.deadline:
			r.call.finish(Result{}, ErrTimeout)
			continue
		}
		// Until an entry of its own term commits, a new leader's commitIndex can be behind what
		// the previous leader committed, so it is not yet a safe readIndex.
		if !r.ready && snap.CommitIndex > 0 && snap.Log[snap.CommitIndex-1].Term == snap.Term {
			r.ready, r.readIndex = true, snap.CommitIndex
			out = append(out, s.ping(r)...)
		}
		if r.ready && 1+len(r.acks) > (len(s.peers)+1)/2 && snap.LastApplied >= r.readIndex {
			h := s.sm.locks[r.lock]
			r.call.finish(Result{OK: true, Holder: h.Client, Token: h.Token}, nil)
			continue
		}
		if r.ready && s.now-r.sentAt >= s.cfg.HeartbeatInterval {
			out = append(out, s.ping(r)...) // a ping or pong may have been lost: start a new round
		}
		kept = append(kept, r)
	}
	s.reads = kept
	return out
}

// ping starts a new confirmation round for r; acknowledgements of older rounds no longer count.
func (s *Server) ping(r *pendingRead) []raft.Message {
	s.round++
	r.round, r.sentAt = s.round, s.now
	clear(r.acks)
	out := make([]raft.Message, 0, len(s.peers))
	for _, p := range s.peers {
		out = append(out, raft.Message{From: s.id, To: p, Body: ReadIndexPing{Term: r.term, Round: r.round}})
	}
	return out
}

// handlePing confirms the sender only if this node is in the same term and follows that sender.
func (s *Server) handlePing(from raft.NodeID, p ReadIndexPing) []raft.Message {
	snap := s.node.Snapshot()
	if snap.Term != p.Term || snap.LeaderID != from {
		return nil
	}
	return []raft.Message{{From: s.id, To: from, Body: ReadIndexPong(p)}}
}

func (s *Server) handlePong(from raft.NodeID, p ReadIndexPong) {
	for _, r := range s.reads {
		if r.ready && r.round == p.Round && r.term == p.Term {
			r.acks[from] = true
		}
	}
}
