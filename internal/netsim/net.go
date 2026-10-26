// Package netsim is a deterministic fake network: every random choice comes from one seeded
// generator and time is a tick counter the caller advances.
package netsim

import (
	"math/rand"
	"sort"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

type pending struct {
	at int // tick at which the message becomes deliverable
	m  raft.Message
}

type Network struct {
	rng      *rand.Rand
	known    map[raft.NodeID]bool
	now      int
	inflight []pending // kept in send order so equal-tick deliveries keep it

	delayMin, delayMax int
	drop, duplicate    float64
	group              map[raft.NodeID]int // nil means no partition; unlisted nodes share group 0
}

func NewNetwork(seed int64, nodes []raft.NodeID) *Network {
	known := make(map[raft.NodeID]bool, len(nodes))
	for _, id := range nodes {
		known[id] = true
	}
	return &Network{rng: rand.New(rand.NewSource(seed)), known: known}
}

// SetDelay sets the per-message delay, drawn uniformly from [min, max] ticks.
func (n *Network) SetDelay(min, max int) { n.delayMin, n.delayMax = min, max }

// SetDrop sets the probability, 0 to 1, that a message is lost.
func (n *Network) SetDrop(rate float64) { n.drop = rate }

// SetDuplicate sets the probability, 0 to 1, that a delivered message arrives twice.
func (n *Network) SetDuplicate(rate float64) { n.duplicate = rate }

// Partition splits the nodes into groups that cannot reach each other, in either direction.
func (n *Network) Partition(groups ...[]raft.NodeID) {
	n.group = map[raft.NodeID]int{}
	for i, g := range groups {
		for _, id := range g {
			n.group[id] = i + 1
		}
	}
}

func (n *Network) Heal() { n.group = nil }

// Send queues m, stamped with the tick of the latest Advance. It is silently lost if
// the destination is unknown, the nodes are partitioned, or the drop roll hits.
func (n *Network) Send(m raft.Message) {
	if !n.known[m.To] || n.group[m.From] != n.group[m.To] || n.rng.Float64() < n.drop {
		return
	}
	n.enqueue(m)
	if n.rng.Float64() < n.duplicate {
		n.enqueue(m)
	}
}

func (n *Network) enqueue(m raft.Message) {
	delay := n.delayMin + n.rng.Intn(n.delayMax-n.delayMin+1)
	n.inflight = append(n.inflight, pending{at: n.now + delay, m: m})
}

// Advance moves time to now and returns every message due by then, earliest first.
func (n *Network) Advance(now int) []raft.Message {
	n.now = now
	var due []pending
	rest := n.inflight[:0]
	for _, p := range n.inflight {
		if p.at <= now {
			due = append(due, p)
		} else {
			rest = append(rest, p)
		}
	}
	n.inflight = rest

	sort.SliceStable(due, func(i, j int) bool { return due[i].at < due[j].at })
	out := make([]raft.Message, len(due))
	for i, p := range due {
		out[i] = p.m
	}
	return out
}
