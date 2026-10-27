package lock

import (
	"errors"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// Transport carries a clerk's request to one server. An error means the server could not be
// reached at all; a reachable server answers through the returned Call.
type Transport interface {
	Submit(server raft.NodeID, clientID string, seq uint64, cmd Command) (*Call, error)
}

// DefaultAttemptTimeout is how many ticks a clerk waits on one server before trying another.
// It is longer than a server's own DefaultTimeout, so a live server normally answers first.
const DefaultAttemptTimeout = 2500

const (
	minBackoff = 10
	maxBackoff = 320
)

// Clerk is a lock-service client. It runs one request at a time, retrying until it succeeds:
// it goes to the leader it last heard from, follows a follower's hint, otherwise walks its server
// list, and backs off when nothing answers. Every try of a request carries the same
// (clientID, seq), so the state machine applies the request once however often it arrives.
// Like the core, it has no goroutines: Tick drives it.
type Clerk struct {
	id      string
	servers []raft.NodeID
	tr      Transport
	leader  raft.NodeID // whoever answered the last request; empty when unknown
	next    int         // the server to try next when there is no leader or hint
	seq     uint64      // seq of the last request that succeeded

	op       *Call // the request in progress
	cmd      Command
	attempt  *Call // the current try, nil while backing off
	target   raft.NodeID
	sentAt   int
	now      int
	waitTill int
	backoff  int

	AttemptTimeout int // ticks
}

func NewClerk(clientID string, servers []raft.NodeID, tr Transport) *Clerk {
	return &Clerk{id: clientID, servers: append([]raft.NodeID(nil), servers...), tr: tr, AttemptTimeout: DefaultAttemptTimeout}
}

func (c *Clerk) Leader() raft.NodeID { return c.leader }

// Do starts a request and returns its Call, which completes once a leader has applied it.
func (c *Clerk) Do(cmd Command) *Call {
	c.op, c.cmd, c.backoff = &Call{}, cmd, 0
	if c.leader != "" {
		c.send(c.leader)
	} else {
		c.send(c.servers[c.next])
	}
	return c.op
}

// Tick advances the clerk one tick: it checks the current try, or sends the next one once its backoff ends.
func (c *Clerk) Tick() {
	c.now++
	if c.op == nil || c.op.done {
		return
	}
	if c.attempt == nil {
		if c.now >= c.waitTill {
			c.send(c.servers[c.next])
		}
		return
	}
	switch a := c.attempt; {
	case a.Done() && a.Err() == nil:
		c.op.finish(a.Result(), nil)
		c.leader, c.seq, c.attempt, c.backoff = c.target, c.seq+1, nil, 0
	case a.Done() && errors.Is(a.Err(), ErrNotLeader) && a.Hint() != "" && a.Hint() != c.target:
		c.attempt = nil
		c.send(a.Hint()) // a hint is worth following at once
	case a.Done() || c.now-c.sentAt >= c.AttemptTimeout:
		c.fail()
	}
}

func (c *Clerk) send(server raft.NodeID) {
	c.target, c.sentAt = server, c.now
	call, err := c.tr.Submit(server, c.id, c.seq+1, c.cmd)
	if err != nil {
		c.fail()
		return
	}
	c.attempt = call
}

// fail gives up on the current server: forget it as leader, move to the next one in the list,
// and wait longer than last time before trying again.
func (c *Clerk) fail() {
	c.attempt, c.leader = nil, ""
	for i, id := range c.servers {
		if id == c.target {
			c.next = (i + 1) % len(c.servers)
		}
	}
	c.backoff = min(max(minBackoff, c.backoff*2), maxBackoff)
	c.waitTill = c.now + c.backoff
}
