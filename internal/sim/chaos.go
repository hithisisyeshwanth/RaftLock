package sim

import (
	"fmt"
	"math/rand"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// Chaos is a chaos monkey for the cluster: on some ticks it crashes or restarts a node, partitions
// one node away, heals the network, or changes the drop rate. Its choices come from its own
// generator, seeded from the run's seed, so the same seed always replays the same faults.
type Chaos struct {
	c   *Cluster
	rng *rand.Rand
}

func NewChaos(c *Cluster, seed int64) *Chaos {
	// A separate stream from the network's, which is also seeded from seed.
	return &Chaos{c: c, rng: rand.New(rand.NewSource(seed*7919 + 13))}
}

// Step does at most one fault, about once every 100 ticks, and returns what it did ("" if nothing).
// The description starts with the kind: crash, restart, partition, heal or drop.
func (ch *Chaos) Step() string {
	if ch.rng.Intn(100) != 0 {
		return ""
	}
	var live, down []raft.NodeID
	for _, id := range ch.c.ids { // ids, not the map, so the choice is the same on every run
		if ch.c.nodes[id] != nil {
			live = append(live, id)
		} else {
			down = append(down, id)
		}
	}
	switch ch.rng.Intn(5) {
	case 0:
		if len(live) > 0 {
			id := live[ch.rng.Intn(len(live))]
			ch.c.Crash(id)
			return "crash " + string(id)
		}
	case 1:
		if len(down) > 0 {
			id := down[ch.rng.Intn(len(down))]
			if err := ch.c.Restart(id); err != nil {
				return fmt.Sprintf("restart %s failed: %v", id, err)
			}
			return "restart " + string(id)
		}
	case 2:
		alone := ch.c.ids[ch.rng.Intn(len(ch.c.ids))]
		var rest []raft.NodeID
		for _, id := range ch.c.ids {
			if id != alone {
				rest = append(rest, id)
			}
		}
		ch.c.Partition([]raft.NodeID{alone}, rest)
		return fmt.Sprintf("partition %s | %v", alone, rest)
	case 3:
		ch.c.Heal()
		return "heal"
	case 4:
		rate := []float64{0, 0.05, 0.1, 0.2}[ch.rng.Intn(4)]
		ch.c.SetDrop(rate)
		return fmt.Sprintf("drop %.2f", rate)
	}
	return ""
}
