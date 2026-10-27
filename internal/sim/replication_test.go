package sim

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// electedCluster runs three nodes until there is a leader and the followers have heard from it.
func electedCluster(t *testing.T, seed int64) (*Cluster, raft.NodeID) {
	t.Helper()
	c := NewCluster(3, seed)
	if err := c.RunUntil(hasLeader(c), 1000); err != nil {
		t.Fatal(err)
	}
	mustStep(t, c, 60)
	id, _ := c.Leader()
	return c, id
}

func propose(t *testing.T, c *Cluster, id raft.NodeID, cmds ...string) {
	t.Helper()
	for _, cmd := range cmds {
		if res, err := c.Propose(id, []byte(cmd)); err != nil || !res.IsLeader {
			t.Fatalf("Propose(%s, %q) = %+v, %v; want accepted by the leader", id, cmd, res, err)
		}
	}
}

func cmds(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("c%d", i+1)
	}
	return out
}

// converged reports whether all three nodes are live, hold identical logs, and those logs contain every cmd.
func converged(c *Cluster, want ...string) func() bool {
	return func() bool {
		var logs [][]raft.Entry
		for _, id := range three {
			s, live := c.Snapshot(id)
			if !live {
				return false
			}
			logs = append(logs, s.Log)
		}
		for _, l := range logs[1:] {
			if !sameLog(l, logs[0]) {
				return false
			}
		}
		for _, w := range want {
			found := false
			for _, e := range logs[0] {
				if string(e.Cmd) == w {
					found = true
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
}

func sameLog(a, b []raft.Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Term != b[i].Term || a[i].Index != b[i].Index || !bytes.Equal(a[i].Cmd, b[i].Cmd) {
			return false
		}
	}
	return true
}

func hasCmd(c *Cluster, id raft.NodeID, cmd string) bool {
	s, _ := c.Snapshot(id)
	for _, e := range s.Log {
		if string(e.Cmd) == cmd {
			return true
		}
	}
	return false
}

// Every heartbeat is an AppendEntries from each peer's nextIndex, so it carries whatever the
// follower is missing: new entries reach followers within one heartbeat interval (50) plus delay (5).
func TestReplicationHeartbeatCarriesNewEntries(t *testing.T) {
	c, leader := electedCluster(t, 1)
	propose(t, c, leader, "x", "y")

	if err := c.RunUntil(converged(c, "x", "y"), 60); err != nil {
		t.Fatalf("followers did not receive the new entries within one heartbeat: %v", err)
	}
}

// Every message delivered twice: the second copy of each AppendEntries finds the entries
// already there and changes nothing, so the logs end identical, with no extra entries.
func TestReplicationDuplicatedMessages(t *testing.T) {
	c, leader := electedCluster(t, 1)
	c.SetDuplicate(1)
	propose(t, c, leader, cmds(5)...)

	if err := c.RunUntil(converged(c, cmds(5)...), 300); err != nil {
		t.Fatalf("logs did not converge with every message duplicated: %v", err)
	}
	before, _ := c.Snapshot(leader)
	mustStep(t, c, 300)
	for _, id := range three {
		if s, _ := c.Snapshot(id); !sameLog(s.Log, before.Log) {
			t.Errorf("%s's log changed while only duplicates and heartbeats arrived: %d entries, want %d", id, len(s.Log), len(before.Log))
		}
	}
}

// A heartbeat that is lost costs nothing permanent: the next one carries the same entries.
func TestReplicationDroppedMessageRepaired(t *testing.T) {
	c, leader := electedCluster(t, 1)
	propose(t, c, leader, "x")

	c.SetDrop(1)
	mustStep(t, c, 55) // at least one heartbeat is sent and lost; far below the election timeout
	for _, id := range three {
		if id != leader && hasCmd(c, id, "x") {
			t.Fatalf("%s already has x although every message was dropped", id)
		}
	}

	c.SetDrop(0)
	if err := c.RunUntil(converged(c, "x"), 60); err != nil {
		t.Errorf("the next heartbeat did not repair the dropped one: %v", err)
	}
}

// Random delays of 1 to 40 ticks deliver AppendEntries and replies out of order. Stale replies
// are recognised by term and echoed PrevLogIndex, and merges never truncate on a shorter message.
func TestReplicationReorderedMessages(t *testing.T) {
	c, leader := electedCluster(t, 1)
	c.SetDelay(1, 40)
	all := cmds(10)
	for _, cmd := range all {
		propose(t, c, leader, cmd)
		mustStep(t, c, 7)
	}
	if err := c.RunUntil(converged(c, all...), 1000); err != nil {
		t.Errorf("logs did not converge with reordered messages: %v", err)
	}
}

// The spec's scenario: with 30 percent of all messages lost, every proposal on the leader still
// reaches all three logs, and the checkers (Log Matching among them) never fire along the way.
func TestReplicationUnder30PercentDrops(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c, leader := electedCluster(t, seed)
		c.SetDrop(0.3)
		all := cmds(20)
		propose(t, c, leader, all...)

		if err := c.RunUntil(converged(c, all...), 3000); err != nil {
			id, _ := c.Leader()
			t.Fatalf("logs did not converge under 30%% drops (leader now %s): %v", id, err)
		}
	})
}
