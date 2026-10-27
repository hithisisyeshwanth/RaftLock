package raft

import "testing"

func aeFailure(n *Node, from NodeID, term Term, prev Index) []Message {
	return n.Step(Message{From: from, To: n.id, Body: AppendEntriesReply{Term: term, Success: false, PrevLogIndex: prev}})
}

// resendTo checks out is exactly one AppendEntries to peer and returns its args.
func resendTo(t *testing.T, out []Message, peer NodeID) AppendEntriesArgs {
	t.Helper()
	if len(out) != 1 || out[0].To != peer || out[0].From != "n1" {
		t.Fatalf("Step() = %+v, want exactly one AppendEntries from n1 to %s", out, peer)
	}
	args, ok := out[0].Body.(AppendEntriesArgs)
	if !ok {
		t.Fatalf("message body is %T, want AppendEntriesArgs", out[0].Body)
	}
	return args
}

// Slide 8: a failure means the follower's log does not match at nextIndex-1, so the leader
// steps back one entry and tries again at once. The leader here (term 6, log
// 1:(t1,a) 2:(t3,b) 3:(t6,x) 4:(t6,y), commitIndex 2) starts every peer at nextIndex 3.
func TestLeaderReply_BackoffStepsBackAndResends(t *testing.T) {
	n := leaderWithFourEntries(t)

	args := resendTo(t, aeFailure(n, "n2", 6, 2), "n2")

	wantProgress(t, n, "n2", 0, 2)
	wantProgress(t, n, "n3", 0, 3)
	want := []Entry{en(2, 3, "b"), en(3, 6, "x"), en(4, 6, "y")}
	if args.Term != 6 || args.LeaderID != "n1" || args.PrevLogIndex != 1 || args.PrevLogTerm != 1 || args.LeaderCommit != 2 {
		t.Errorf("resend = %+v, want term 6 from n1, prev (1, 1), leaderCommit 2", args)
	}
	if !entriesEqual(args.Entries, want) {
		t.Errorf("resend entries = %+v, want %+v (everything from the new nextIndex)", args.Entries, want)
	}
}

func TestLeaderReply_BackoffRepeatedly(t *testing.T) {
	n := leaderWithFourEntries(t)

	args := resendTo(t, aeFailure(n, "n2", 6, 2), "n2")
	if args.PrevLogIndex != 1 {
		t.Fatalf("first resend prev = %d, want 1", args.PrevLogIndex)
	}
	args = resendTo(t, aeFailure(n, "n2", 6, 1), "n2")
	if args.PrevLogIndex != 0 || len(args.Entries) != 4 {
		t.Errorf("second resend = prev %d with %d entries, want prev 0 with all 4", args.PrevLogIndex, len(args.Entries))
	}
	wantProgress(t, n, "n2", 0, 1)
}

// nextIndex 1 is already the start of the log; there is nothing earlier to try.
func TestLeaderReply_BackoffFloorsAtOne(t *testing.T) {
	n := leaderWithFourEntries(t)
	n.nextIndex["n2"] = 1

	aeFailure(n, "n2", 6, 0) // a correct follower never fails at prev 0, but nextIndex must still hold

	if got := n.nextIndex["n2"]; got != 1 {
		t.Errorf("nextIndex[n2] = %d after a failure at nextIndex 1, want it to stay 1", got)
	}
}

// A failure answers the request sent with a particular nextIndex, echoed as PrevLogIndex. If
// nextIndex has moved since, the failure is about an old request and must not move it again.
func TestLeaderReply_BackoffIgnoresStaleFailure(t *testing.T) {
	t.Run("duplicate of a failure already acted on", func(t *testing.T) {
		n := leaderWithFourEntries(t)
		aeFailure(n, "n2", 6, 2) // nextIndex 3 -> 2

		if out := aeFailure(n, "n2", 6, 2); len(out) != 0 {
			t.Errorf("duplicate failure sent %+v, want nothing", out)
		}
		wantProgress(t, n, "n2", 0, 2)
	})
	t.Run("failure for a request from before a success", func(t *testing.T) {
		n := leaderWithFourEntries(t)
		aeSuccess(n, "n2", 6, 4, 2) // nextIndex 3 -> 5

		if out := aeFailure(n, "n2", 6, 2); len(out) != 0 {
			t.Errorf("stale failure sent %+v, want nothing", out)
		}
		wantProgress(t, n, "n2", 4, 5)
	})
	t.Run("failure echoing a prev the leader never sent", func(t *testing.T) {
		n := leaderWithFourEntries(t)
		aeFailure(n, "n2", 6, 1)
		aeFailure(n, "n2", 6, 4)
		wantProgress(t, n, "n2", 0, 3)
	})
}

func TestLeaderReply_BackoffIgnoresWrongTermOrSender(t *testing.T) {
	n := leaderWithFourEntries(t)

	if out := aeFailure(n, "n2", 5, 2); len(out) != 0 {
		t.Errorf("stale-term failure sent %+v, want nothing", out)
	}
	if out := aeFailure(n, "zz", 6, 2); len(out) != 0 {
		t.Errorf("failure from a non-peer sent %+v, want nothing", out)
	}
	wantProgress(t, n, "n2", 0, 3)
	if _, ok := n.nextIndex["zz"]; ok {
		t.Error("nextIndex gained an entry for zz, which is not a peer")
	}
}

// Once the leader has stepped back far enough, the follower accepts and progress jumps forward.
func TestLeaderReply_BackoffThenSuccessAdvances(t *testing.T) {
	n := leaderWithFourEntries(t)

	resend := resendTo(t, aeFailure(n, "n2", 6, 2), "n2") // nextIndex 3 -> 2, resend prev 1 with entries 2..4
	match := resend.PrevLogIndex + Index(len(resend.Entries))
	aeSuccess(n, "n2", 6, match, resend.PrevLogIndex)

	wantProgress(t, n, "n2", 4, 5)
}
