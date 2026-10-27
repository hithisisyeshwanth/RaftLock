package sim

import "testing"

// Cluster.Propose passes a command to one node and sends whatever that node returns.
func TestClusterPropose(t *testing.T) {
	c := NewCluster(3, 1)
	if err := c.RunUntil(hasLeader(c), 1000); err != nil {
		t.Fatal(err)
	}
	mustStep(t, c, 60) // let the first heartbeats reach the followers, so they know the leader
	leader, _ := c.Leader()

	res, err := c.Propose(leader, []byte("x"))
	if err != nil || !res.IsLeader || res.Index == 0 {
		t.Fatalf("Propose on the leader = %+v, %v; want IsLeader with an index", res, err)
	}
	s, _ := c.Snapshot(leader)
	if got := s.Log[res.Index-1]; string(got.Cmd) != "x" || got.Term != res.Term {
		t.Errorf("leader's entry %d = %+v, want cmd x in term %d", res.Index, got, res.Term)
	}

	for _, id := range three {
		if id == leader {
			continue
		}
		res, err := c.Propose(id, []byte("y"))
		if err != nil || res.IsLeader || res.LeaderHint != leader {
			t.Errorf("Propose on follower %s = %+v, %v; want not leader, hint %s", id, res, err, leader)
		}
	}

	c.Crash(leader)
	if _, err := c.Propose(leader, []byte("z")); err == nil {
		t.Error("Propose on a crashed node returned no error")
	}
}
