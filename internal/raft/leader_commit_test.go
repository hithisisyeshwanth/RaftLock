package raft

import "testing"

// leaderWithProposal is n1, leader of term 6 over log 1:(t1) 2:(t3), after proposing "x" at index 3.
func leaderWithProposal(t *testing.T, peers ...NodeID) (*Node, *MemStorage) {
	t.Helper()
	n, _, st := electedLeader(t, peers...)
	if res, _ := n.Propose([]byte("x")); res.Index != 3 || res.Term != 6 {
		t.Fatalf("setup: Propose = %+v, want index 3 in term 6", res)
	}
	return n, st
}

func commitIndex(n *Node) Index { return n.Snapshot().CommitIndex }

// A success reply that puts the current-term entry on a majority commits it, and with it every
// earlier entry (slide 9).
func TestLeaderCommit_AdvancesOnMajority(t *testing.T) {
	n, _ := leaderWithProposal(t, "n2", "n3")
	if got := commitIndex(n); got != 0 {
		t.Fatalf("commitIndex = %d before any reply, want 0", got)
	}

	aeSuccess(n, "n2", 6, 3, 2)

	if got := commitIndex(n); got != 3 {
		t.Errorf("commitIndex = %d after n2 confirmed index 3, want 3 (leader + n2 is a majority of 3)", got)
	}
}

// Figure 8 inside a real leader: the old-term entries reach a majority but stay uncommitted
// until the term-6 entry does.
func TestLeaderCommit_NotForOldTermEntries(t *testing.T) {
	n, _ := leaderWithProposal(t, "n2", "n3")

	aeSuccess(n, "n2", 6, 2, 2)
	if got := commitIndex(n); got != 0 {
		t.Fatalf("commitIndex = %d with only the term-3 entry on a majority, want 0", got)
	}
	aeSuccess(n, "n2", 6, 3, 2)
	if got := commitIndex(n); got != 3 {
		t.Errorf("commitIndex = %d once the term-6 entry is on a majority, want 3", got)
	}
}

func TestLeaderCommit_FiveNodesNeedThree(t *testing.T) {
	n, _ := leaderWithProposal(t, "n2", "n3", "n4", "n5")

	aeSuccess(n, "n2", 6, 3, 2)
	if got := commitIndex(n); got != 0 {
		t.Fatalf("commitIndex = %d with 2 of 5 holding index 3, want 0", got)
	}
	aeSuccess(n, "n4", 6, 3, 2)
	if got := commitIndex(n); got != 3 {
		t.Errorf("commitIndex = %d with 3 of 5 holding index 3, want 3", got)
	}
}

// Late or reordered replies cannot pull commitIndex back.
func TestLeaderCommit_NeverDecreases(t *testing.T) {
	n, _ := leaderWithProposal(t, "n2", "n3")
	aeSuccess(n, "n2", 6, 3, 2)
	aeSuccess(n, "n2", 6, 2, 2)
	aeSuccess(n, "n3", 6, 0, 0)
	if got := commitIndex(n); got != 3 {
		t.Errorf("commitIndex = %d after late lower replies, want it to stay 3", got)
	}
}

// Followers learn the commit from LeaderCommit on the leader's next AppendEntries.
func TestLeaderCommit_PropagatedInNextAppendEntries(t *testing.T) {
	t.Run("next heartbeat", func(t *testing.T) {
		n, _ := leaderWithProposal(t, "n2", "n3")
		aeSuccess(n, "n2", 6, 3, 2)

		var out []Message
		for i := 0; i < 50 && len(out) == 0; i++ {
			out = n.Tick()
		}
		for p, args := range appendEntriesTo(t, out, "n2", "n3") {
			if args.LeaderCommit != 3 {
				t.Errorf("heartbeat to %s has LeaderCommit %d, want 3", p, args.LeaderCommit)
			}
		}
	})
	t.Run("backoff resend", func(t *testing.T) {
		n, _ := leaderWithProposal(t, "n2", "n3")
		aeSuccess(n, "n2", 6, 3, 2)

		args := resendTo(t, aeFailure(n, "n3", 6, 2), "n3")
		if args.LeaderCommit != 3 {
			t.Errorf("resend to n3 has LeaderCommit %d, want 3", args.LeaderCommit)
		}
	})
}

// commitIndex is volatile state (Figure 2): advancing it writes nothing to storage.
func TestLeaderCommit_NotPersisted(t *testing.T) {
	n, st := leaderWithProposal(t, "n2", "n3")
	saves := st.SaveCount()

	aeSuccess(n, "n2", 6, 3, 2)

	if commitIndex(n) != 3 {
		t.Fatalf("setup: commitIndex = %d, want 3", commitIndex(n))
	}
	if st.SaveCount() != saves {
		t.Errorf("SaveCount() = %d after the commit advanced, want %d (commitIndex is not persisted)", st.SaveCount(), saves)
	}
}
