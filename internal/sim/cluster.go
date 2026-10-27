// Package sim runs a deterministic in-memory Raft cluster: real nodes, the fake network,
// and the Figure 3 safety checkers after every step. Every random choice comes from one seed.
package sim

import (
	"fmt"

	"github.com/hithisisyeshwanth/RaftLock/internal/check"
	"github.com/hithisisyeshwanth/RaftLock/internal/netsim"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// Cluster wires n nodes named n1..nN to a fake network. It is not safe for concurrent use.
type Cluster struct {
	cfg      raft.Config
	ids      []raft.NodeID
	nodes    map[raft.NodeID]*raft.Node // nil while crashed
	storage  map[raft.NodeID]*raft.MemStorage
	timeouts map[raft.NodeID]raft.TimeoutSource
	applied  map[raft.NodeID][]raft.Entry // every entry applied since the node last booted
	net      *netsim.Network
	checks   *check.All
	now      int

	// Tamper, if set, edits the snapshots just before the checkers see them. Tests use it to inject violations.
	Tamper func([]check.Snapshot)

	// Tracer, if set, receives every node's trace events. It can be set any time after NewCluster.
	Tracer raft.Tracer
}

// forwardTracer lets nodes built before Cluster.Tracer is set still reach it.
type forwardTracer struct{ c *Cluster }

func (f forwardTracer) Trace(e raft.Event) {
	if f.c.Tracer != nil {
		f.c.Tracer.Trace(e)
	}
}

func NewCluster(n int, seed int64) *Cluster {
	c := &Cluster{
		cfg:      raft.DefaultConfig(),
		nodes:    map[raft.NodeID]*raft.Node{},
		storage:  map[raft.NodeID]*raft.MemStorage{},
		timeouts: map[raft.NodeID]raft.TimeoutSource{},
		applied:  map[raft.NodeID][]raft.Entry{},
		checks:   check.NewAll(),
	}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, raft.NodeID(fmt.Sprintf("n%d", i)))
	}
	c.net = netsim.NewNetwork(seed, c.ids)
	c.net.SetDelay(1, 5)
	for i, id := range c.ids {
		c.storage[id] = raft.NewMemStorage()
		// Each node gets its own seeded source so their timeouts differ but replay identically.
		c.timeouts[id] = raft.NewSeededTimeouts(seed*int64(n+1)+int64(i+1), c.cfg.ElectionTimeoutMin, c.cfg.ElectionTimeoutMax)
		if err := c.Restart(id); err != nil {
			panic(err) // a fresh MemStorage cannot fail to load
		}
	}
	return c
}

// Step advances the cluster one tick: tick every live node in id order, deliver every message
// now due, send what the nodes return, then run the safety checkers.
func (c *Cluster) Step() error {
	c.now++
	for _, id := range c.ids {
		if n := c.nodes[id]; n != nil {
			c.send(n.Tick())
		}
	}
	for _, m := range c.net.Advance(c.now) {
		if n := c.nodes[m.To]; n != nil { // a crashed node receives nothing
			c.send(n.Step(m))
		}
	}
	snaps := c.checkSnapshots()
	if c.Tamper != nil {
		c.Tamper(snaps)
	}
	if err := c.checks.Observe(snaps); err != nil {
		return fmt.Errorf("tick %d: %w", c.now, err)
	}
	return nil
}

func (c *Cluster) send(msgs []raft.Message) {
	for _, m := range msgs {
		c.net.Send(m)
	}
}

func (c *Cluster) checkSnapshots() []check.Snapshot {
	var out []check.Snapshot
	for _, id := range c.ids {
		s, live := c.Snapshot(id)
		if !live {
			continue
		}
		out = append(out, check.Snapshot{
			ID: s.ID, Term: s.Term, Role: s.Role, Log: s.Log,
			CommitIndex: s.CommitIndex, Applied: c.applied[id],
		})
	}
	return out
}

// RunUntil steps until cond holds, a checker fails, or maxTicks steps have run.
func (c *Cluster) RunUntil(cond func() bool, maxTicks int) error {
	for i := 0; i < maxTicks; i++ {
		if cond() {
			return nil
		}
		if err := c.Step(); err != nil {
			return err
		}
	}
	if cond() {
		return nil
	}
	return fmt.Errorf("condition not met after %d ticks (now tick %d)", maxTicks, c.now)
}

// Leader returns the live leader with the highest term. A deposed leader that has not yet
// heard of the newer term can still think it leads, so the highest term wins.
func (c *Cluster) Leader() (raft.NodeID, bool) {
	var best raft.Snapshot
	found := false
	for _, id := range c.ids {
		s, live := c.Snapshot(id)
		if live && s.Role == raft.Leader && (!found || s.Term > best.Term) {
			best, found = s, true
		}
	}
	return best.ID, found
}

// Crash stops a node. Its storage survives; everything in memory is lost.
func (c *Cluster) Crash(id raft.NodeID) {
	c.nodes[id] = nil
}

// Restart builds a fresh node from id's storage, as a process restart would.
func (c *Cluster) Restart(id raft.NodeID) error {
	var peers []raft.NodeID
	for _, p := range c.ids {
		if p != id {
			peers = append(peers, p)
		}
	}
	c.applied[id] = nil
	onApply := func(e raft.Entry) { c.applied[id] = append(c.applied[id], e) }
	n, err := raft.NewNode(c.cfg, id, peers, c.storage[id], forwardTracer{c}, c.timeouts[id], onApply)
	if err != nil {
		return fmt.Errorf("restart %s: %w", id, err)
	}
	c.nodes[id] = n
	return nil
}

// Propose hands cmd to node id and sends whatever it returns into the network.
func (c *Cluster) Propose(id raft.NodeID, cmd []byte) (raft.ProposeResult, error) {
	n := c.nodes[id]
	if n == nil {
		return raft.ProposeResult{}, fmt.Errorf("propose: %s is crashed", id)
	}
	res, out := n.Propose(cmd)
	c.send(out)
	return res, nil
}

// SetTimeouts replaces the election timeout source id will use from its next Restart on.
func (c *Cluster) SetTimeouts(id raft.NodeID, ts raft.TimeoutSource) { c.timeouts[id] = ts }

func (c *Cluster) Partition(groups ...[]raft.NodeID) { c.net.Partition(groups...) }

func (c *Cluster) Heal() { c.net.Heal() }

// Snapshot returns id's current state, or false while id is crashed.
func (c *Cluster) Snapshot(id raft.NodeID) (raft.Snapshot, bool) {
	n := c.nodes[id]
	if n == nil {
		return raft.Snapshot{}, false
	}
	return n.Snapshot(), true
}

func (c *Cluster) Storage(id raft.NodeID) *raft.MemStorage { return c.storage[id] }

func (c *Cluster) Now() int { return c.now }
