package sim

import "testing"

// Across a real cluster, every node commits the leader's no-op, but no node's state machine
// ever receives it: before any proposal nothing is applied anywhere, and afterwards exactly
// the proposed commands are.
func TestNoopNeverAppliedOnAnyNode(t *testing.T) {
	c, leader := electedCluster(t, 1)
	committed := func() bool {
		for _, id := range three {
			if s, _ := c.Snapshot(id); s.CommitIndex == 0 {
				return false
			}
		}
		return true
	}
	if err := c.RunUntil(committed, 200); err != nil {
		t.Fatalf("the no-op did not commit on every node: %v", err)
	}
	for _, id := range three {
		if len(c.applied[id]) != 0 {
			t.Errorf("%s applied %+v with only the no-op committed, want nothing", id, c.applied[id])
		}
	}

	propose(t, c, leader, "x", "y")
	applied := func() bool {
		for _, id := range three {
			if len(c.applied[id]) != 2 {
				return false
			}
		}
		return true
	}
	if err := c.RunUntil(applied, 200); err != nil {
		t.Fatalf("the proposals were not applied everywhere: %v", err)
	}
	for _, id := range three {
		if got := c.applied[id]; string(got[0].Cmd) != "x" || string(got[1].Cmd) != "y" {
			t.Errorf("%s applied %+v, want exactly x then y", id, got)
		}
	}
}
