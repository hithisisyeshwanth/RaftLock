package netsim

import (
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

var nodes = []raft.NodeID{"a", "b", "c"}

// msg uses an int Body so messages compare with ==.
func msg(from, to raft.NodeID, body int) raft.Message {
	return raft.Message{From: from, To: to, Body: body}
}

// run advances the network through ticks from..to inclusive and returns what each tick delivered.
func run(n *Network, from, to int) map[int][]raft.Message {
	out := map[int][]raft.Message{}
	for tick := from; tick <= to; tick++ {
		if got := n.Advance(tick); len(got) > 0 {
			out[tick] = got
		}
	}
	return out
}

func total(delivered map[int][]raft.Message) int {
	n := 0
	for _, ms := range delivered {
		n += len(ms)
	}
	return n
}

func TestNetDelay(t *testing.T) {
	tests := []struct {
		name  string
		delay int
	}{
		{"one tick", 1},
		{"three ticks", 3},
		{"ten ticks", 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NewNetwork(1, nodes)
			n.SetDelay(tt.delay, tt.delay)
			m := msg("a", "b", 7)
			n.Send(m)

			delivered := run(n, 1, 20)
			if total(delivered) != 1 {
				t.Fatalf("delivered %d messages, want 1: %v", total(delivered), delivered)
			}
			got := delivered[tt.delay]
			if len(got) != 1 || got[0] != m {
				t.Errorf("delivered at ticks %v, want exactly %+v at tick %d", delivered, m, tt.delay)
			}
		})
	}
}

func TestNetDeliversOnce(t *testing.T) {
	n := NewNetwork(1, nodes)
	n.SetDelay(2, 2)
	n.Send(msg("a", "b", 1))
	run(n, 1, 2)
	if got := n.Advance(2); len(got) != 0 {
		t.Errorf("Advance(2) a second time returned %v, want nothing", got)
	}
	if got := n.Advance(50); len(got) != 0 {
		t.Errorf("Advance(50) returned %v, want nothing (already delivered)", got)
	}
}

func TestNetEqualDelayKeepsSendOrder(t *testing.T) {
	n := NewNetwork(1, nodes)
	n.SetDelay(2, 2)
	for i := 0; i < 5; i++ {
		n.Send(msg("a", "b", i))
	}
	got := n.Advance(2)
	if len(got) != 5 {
		t.Fatalf("delivered %d messages at tick 2, want 5", len(got))
	}
	for i, m := range got {
		if m.Body != i {
			t.Errorf("delivery %d has body %v, want %d (same-tick order is send order)", i, m.Body, i)
		}
	}
}

func TestNetPartition(t *testing.T) {
	n := NewNetwork(1, nodes)
	n.SetDelay(1, 1)
	n.Partition([]raft.NodeID{"a"}, []raft.NodeID{"b", "c"})

	tests := []struct {
		name     string
		from, to raft.NodeID
		blocked  bool
	}{
		{"a to b crosses", "a", "b", true},
		{"b to a crosses (both directions)", "b", "a", true},
		{"a to c crosses", "a", "c", true},
		{"b to c same group", "b", "c", false},
		{"c to b same group", "c", "b", false},
	}
	tick := 0
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n.Send(msg(tt.from, tt.to, 1))
			tick++
			got := n.Advance(tick)
			if tt.blocked && len(got) != 0 {
				t.Errorf("message across the partition was delivered: %v", got)
			}
			if !tt.blocked && len(got) != 1 {
				t.Errorf("message inside a group delivered %d times, want 1", len(got))
			}
		})
	}
}

func TestNetHealRestoresDelivery(t *testing.T) {
	n := NewNetwork(1, nodes)
	n.SetDelay(1, 1)
	n.Partition([]raft.NodeID{"a"}, []raft.NodeID{"b", "c"})
	n.Send(msg("a", "b", 1))
	if got := n.Advance(1); len(got) != 0 {
		t.Fatalf("delivered across the partition: %v", got)
	}

	n.Heal()
	for _, pair := range [][2]raft.NodeID{{"a", "b"}, {"b", "a"}, {"a", "c"}} {
		n.Send(msg(pair[0], pair[1], 2))
	}
	if got := n.Advance(2); len(got) != 3 {
		t.Errorf("after Heal, delivered %d of 3 messages: %v", len(got), got)
	}
}

func TestNetDropRate(t *testing.T) {
	const sent = 1000
	tests := []struct {
		name           string
		rate           float64
		minGot, maxGot int
	}{
		{"zero drops nothing", 0, sent, sent},
		{"one drops everything", 1, 0, 0},
		{"half drops about half", 0.5, 350, 650},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NewNetwork(5, nodes)
			n.SetDelay(1, 1)
			n.SetDrop(tt.rate)
			for i := 0; i < sent; i++ {
				n.Send(msg("a", "b", i))
			}
			got := len(n.Advance(1))
			if got < tt.minGot || got > tt.maxGot {
				t.Errorf("drop rate %v delivered %d of %d, want %d to %d", tt.rate, got, sent, tt.minGot, tt.maxGot)
			}
		})
	}
}

func TestNetDuplicate(t *testing.T) {
	t.Run("rate one delivers each message twice", func(t *testing.T) {
		n := NewNetwork(1, nodes)
		n.SetDelay(1, 1)
		n.SetDuplicate(1)
		m := msg("a", "b", 9)
		n.Send(m)
		got := n.Advance(1)
		if len(got) != 2 || got[0] != m || got[1] != m {
			t.Errorf("delivered %v, want the same message twice", got)
		}
	})
	t.Run("rate zero never duplicates", func(t *testing.T) {
		n := NewNetwork(1, nodes)
		n.SetDelay(1, 1)
		n.SetDuplicate(0)
		for i := 0; i < 100; i++ {
			n.Send(msg("a", "b", i))
		}
		if got := len(n.Advance(1)); got != 100 {
			t.Errorf("delivered %d of 100, want exactly 100", got)
		}
	})
	t.Run("duplicates survive random delays", func(t *testing.T) {
		n := NewNetwork(1, nodes)
		n.SetDelay(1, 10)
		n.SetDuplicate(1)
		for i := 0; i < 20; i++ {
			n.Send(msg("a", "b", i))
		}
		if got := total(run(n, 1, 30)); got != 40 {
			t.Errorf("delivered %d, want 40 (every message twice)", got)
		}
	})
	t.Run("a dropped message is not duplicated", func(t *testing.T) {
		n := NewNetwork(1, nodes)
		n.SetDelay(1, 1)
		n.SetDrop(1)
		n.SetDuplicate(1)
		n.Send(msg("a", "b", 1))
		if got := n.Advance(1); len(got) != 0 {
			t.Errorf("delivered %v, want nothing at drop rate 1", got)
		}
	})
}

func TestNetRandomDelaysReorder(t *testing.T) {
	n := NewNetwork(3, nodes)
	n.SetDelay(1, 20)
	const sent = 50
	for i := 0; i < sent; i++ {
		n.Send(msg("a", "b", i))
	}

	var order []int
	for tick := 1; tick <= 25; tick++ {
		for _, m := range n.Advance(tick) {
			order = append(order, m.Body.(int))
		}
	}
	if len(order) != sent {
		t.Fatalf("delivered %d of %d messages", len(order), sent)
	}
	inOrder := true
	for i := 1; i < len(order); i++ {
		if order[i] < order[i-1] {
			inOrder = false
		}
	}
	if inOrder {
		t.Errorf("50 messages with delays 1 to 20 all arrived in send order: %v", order)
	}
}

func TestNetDelayStaysInRange(t *testing.T) {
	n := NewNetwork(9, nodes)
	n.SetDelay(4, 8)
	for i := 0; i < 200; i++ {
		n.Send(msg("a", "b", i))
	}
	delivered := run(n, 1, 20)
	if total(delivered) != 200 {
		t.Fatalf("delivered %d of 200", total(delivered))
	}
	for tick := range delivered {
		if tick < 4 || tick > 8 {
			t.Errorf("a message was delivered at tick %d, outside the delay range 4 to 8", tick)
		}
	}
}

func TestNetUnknownNodeDropped(t *testing.T) {
	n := NewNetwork(1, nodes)
	n.SetDelay(1, 1)
	n.Send(msg("a", "zz", 1))
	n.Send(msg("a", "b", 2))
	delivered := run(n, 1, 10)
	if total(delivered) != 1 || delivered[1][0].To != "b" {
		t.Errorf("delivered %v, want only the message to the known node b", delivered)
	}
}

// chaos configures a network with every random control on, then sends the same traffic.
func chaos(seed int64) map[int][]raft.Message {
	n := NewNetwork(seed, nodes)
	n.SetDelay(1, 10)
	n.SetDrop(0.3)
	n.SetDuplicate(0.2)
	for i := 0; i < 200; i++ {
		n.Send(msg("a", "b", i))
	}
	return run(n, 1, 30)
}

func sameSchedule(a, b map[int][]raft.Message) bool {
	if len(a) != len(b) {
		return false
	}
	for tick, ms := range a {
		other := b[tick]
		if len(ms) != len(other) {
			return false
		}
		for i := range ms {
			if ms[i] != other[i] {
				return false
			}
		}
	}
	return true
}

func TestNetSameSeedSameSchedule(t *testing.T) {
	if !sameSchedule(chaos(42), chaos(42)) {
		t.Error("two networks with seed 42 and identical traffic delivered different schedules")
	}
}

func TestNetDifferentSeedDifferentSchedule(t *testing.T) {
	if sameSchedule(chaos(1), chaos(2)) {
		t.Error("seeds 1 and 2 produced identical schedules over 200 messages")
	}
}
