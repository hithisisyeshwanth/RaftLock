package lock

import (
	"maps"
	"slices"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

type lease struct {
	token    raft.Index
	ttl      int
	deadline int  // tick at which the leader proposes Expire
	proposed bool // Expire already proposed; wait for it to commit
}

// Leases turns lock TTLs into Expire proposals. Every node feeds it the commands it applies, so
// a new leader already knows every lock, but only a leader's timers run. Time is counted in ticks
// on this node, never read from a clock, and the state machine never sees it: expiry reaches the
// state machine only as a replicated Expire entry.
type Leases struct {
	now    int
	leader bool
	leases map[string]*lease // by lock name
}

func NewLeases() *Leases {
	return &Leases{leases: map[string]*lease{}}
}

// Applied records the effect of a command this node just applied: a granted Acquire with a TTL
// starts a lease, and a successful Release or Expire ends it.
func (l *Leases) Applied(c Command, r Result) {
	if !r.OK {
		return
	}
	switch c.Kind {
	case Acquire:
		if old, ok := l.leases[c.Lock]; ok && old.token == r.Token {
			return // the holder asked again: its lease keeps running
		}
		if c.TTL > 0 {
			l.leases[c.Lock] = &lease{token: r.Token, ttl: c.TTL, deadline: l.now + c.TTL}
		}
	case Release, Expire:
		delete(l.leases, c.Lock)
	}
}

// Tick advances one tick and returns the Expire commands the leader should propose now.
// On the first tick of a new leadership every lease restarts with its full TTL: the new leader
// cannot know how long its predecessor had counted, and restarting only ever gives holders more time.
func (l *Leases) Tick(leader bool) []Command {
	l.now++
	if !leader {
		l.leader = false
		return nil
	}
	if !l.leader {
		l.leader = true
		for _, ls := range l.leases {
			ls.deadline = l.now + ls.ttl - 1 // this tick is the first of the TTL
			ls.proposed = false
		}
	}
	var out []Command
	for _, name := range slices.Sorted(maps.Keys(l.leases)) { // sorted, so replays propose in one order
		ls := l.leases[name]
		if !ls.proposed && l.now >= ls.deadline {
			ls.proposed = true
			out = append(out, Command{Kind: Expire, Lock: name, Token: ls.token})
		}
	}
	return out
}
