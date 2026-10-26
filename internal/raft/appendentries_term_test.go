package raft

import "testing"

// heartbeat delivers an AppendEntries with no entries and prevLogIndex 0 from leader for term.
func heartbeat(n *Node, leader NodeID, term Term) []Message {
	return n.Step(Message{From: leader, To: n.id, Body: AppendEntriesArgs{Term: term, LeaderID: leader}})
}

// aeReply checks out is a single AppendEntriesReply from n1 to the given node and returns it.
func aeReply(t *testing.T, out []Message, to NodeID) AppendEntriesReply {
	t.Helper()
	if len(out) != 1 {
		t.Fatalf("Step(AppendEntries) returned %d messages, want 1 reply: %+v", len(out), out)
	}
	if out[0].From != "n1" || out[0].To != to {
		t.Errorf("reply is from %q to %q, want from n1 to %q", out[0].From, out[0].To, to)
	}
	reply, ok := out[0].Body.(AppendEntriesReply)
	if !ok {
		t.Fatalf("reply body is %T, want AppendEntriesReply", out[0].Body)
	}
	return reply
}

// Figure 2, AppendEntries rule 1: reply false if term < currentTerm. A stale leader is not
// recorded, and since it is not the current leader it must not hold off an election either.
func TestAppendEntries_TermStaleRejected(t *testing.T) {
	for _, role := range []Role{Follower, Candidate, Leader} {
		t.Run(role.String(), func(t *testing.T) {
			n, rec, st := timerNode(t, NewFixedTimeouts(150), PersistentState{CurrentTerm: 5, VotedFor: "n1"}, role)
			saves := st.(*MemStorage).SaveCount()

			tick(n, 100)
			reply := aeReply(t, heartbeat(n, "n2", 4), "n2")

			if reply.Success {
				t.Error("Success = true for a stale term, want false")
			}
			if reply.Term != 5 {
				t.Errorf("reply Term = %d, want the current term 5", reply.Term)
			}
			got := n.Snapshot()
			if got.Term != 5 || got.Role != role || got.VotedFor != "n1" || got.LeaderID != "" {
				t.Errorf("state = term %d, role %v, votedFor %q, leader %q; want 5, %v, \"n1\", \"\"",
					got.Term, got.Role, got.VotedFor, got.LeaderID, role)
			}
			if st.(*MemStorage).SaveCount() != saves {
				t.Errorf("SaveCount() = %d, want %d (a stale request writes nothing)", st.(*MemStorage).SaveCount(), saves)
			}
			if role != Leader {
				tick(n, 50) // tick 150: the deadline drawn at start
				if got := expiries(rec); got != 1 {
					t.Errorf("expiry reported %d times at the original deadline, want 1 (a stale leader must not reset the timer)", got)
				}
			}
		})
	}
}

// An AppendEntries in the current term comes from that term's leader.
func TestAppendEntries_TermEqualFollowerAccepts(t *testing.T) {
	n, rec, st := timerNode(t, NewFixedTimeouts(150), PersistentState{CurrentTerm: 5, VotedFor: "n2"}, Follower)
	saves := st.(*MemStorage).SaveCount()

	tick(n, 100)
	reply := aeReply(t, heartbeat(n, "n2", 5), "n2")

	if !reply.Success || reply.Term != 5 || reply.MatchIndex != 0 || reply.PrevLogIndex != 0 {
		t.Errorf("reply = %+v, want {Term:5 Success:true MatchIndex:0 PrevLogIndex:0}", reply)
	}
	got := n.Snapshot()
	if got.Term != 5 || got.Role != Follower || got.VotedFor != "n2" || got.LeaderID != "n2" {
		t.Errorf("state = term %d, role %v, votedFor %q, leader %q; want 5, Follower, \"n2\", \"n2\"",
			got.Term, got.Role, got.VotedFor, got.LeaderID)
	}
	if st.(*MemStorage).SaveCount() != saves {
		t.Errorf("SaveCount() = %d, want %d (nothing durable changed)", st.(*MemStorage).SaveCount(), saves)
	}

	tick(n, 149)
	if got := expiries(rec); got != 0 {
		t.Fatalf("expiry reported %d times 149 ticks after a heartbeat, want 0 (the heartbeat resets the timer)", got)
	}
	tick(n, 1)
	if got := expiries(rec); got != 1 {
		t.Errorf("expiry reported %d times 150 ticks after the heartbeat, want 1", got)
	}
}

// A candidate that hears from a leader of its own term lost the election (section 5.2).
// The term is unchanged, so its self-vote for this term stays.
func TestAppendEntries_TermEqualCandidateStepsDown(t *testing.T) {
	n, rec, _ := timerNode(t, NewFixedTimeouts(150), PersistentState{CurrentTerm: 5}, Follower)
	tick(n, 150) // election: candidate in term 6, next deadline at tick 300
	wantRole(t, n, Candidate, 6)
	before := expiries(rec)

	tick(n, 50) // tick 200
	reply := aeReply(t, heartbeat(n, "n2", 6), "n2")

	if !reply.Success || reply.Term != 6 {
		t.Errorf("reply = %+v, want term 6, success", reply)
	}
	got := n.Snapshot()
	if got.Role != Follower || got.Term != 6 || got.VotedFor != "n1" || got.LeaderID != "n2" {
		t.Errorf("state = role %v, term %d, votedFor %q, leader %q; want Follower, 6, \"n1\", \"n2\"",
			got.Role, got.Term, got.VotedFor, got.LeaderID)
	}

	tick(n, 149) // tick 349: past the candidate's own deadline at 300
	if got := expiries(rec) - before; got != 0 {
		t.Fatalf("expiry reported %d times 149 ticks after stepping down, want 0", got)
	}
	tick(n, 1)
	if got := expiries(rec) - before; got != 1 {
		t.Errorf("expiry reported %d times 150 ticks after stepping down, want 1", got)
	}
}

func TestAppendEntries_TermHigherAdopted(t *testing.T) {
	for _, role := range []Role{Follower, Candidate, Leader} {
		t.Run(role.String(), func(t *testing.T) {
			n, rec, st := timerNode(t, NewFixedTimeouts(150), PersistentState{CurrentTerm: 5, VotedFor: "n1"}, role)

			tick(n, 100)
			reply := aeReply(t, heartbeat(n, "n3", 8), "n3")

			if !reply.Success || reply.Term != 8 {
				t.Errorf("reply = %+v, want term 8, success", reply)
			}
			got := n.Snapshot()
			if got.Term != 8 || got.Role != Follower || got.VotedFor != "" || got.LeaderID != "n3" {
				t.Errorf("state = term %d, role %v, votedFor %q, leader %q; want 8, Follower, \"\", \"n3\"",
					got.Term, got.Role, got.VotedFor, got.LeaderID)
			}
			if s := lastSaved(t, st.(*MemStorage)); s.CurrentTerm != 8 || s.VotedFor != "" {
				t.Errorf("saved term/votedFor = %d/%q, want 8/\"\"", s.CurrentTerm, s.VotedFor)
			}

			tick(n, 149)
			if got := expiries(rec); got != 0 {
				t.Fatalf("expiry reported %d times 149 ticks after the heartbeat, want 0", got)
			}
			tick(n, 1)
			if got := expiries(rec); got != 1 {
				t.Errorf("expiry reported %d times 150 ticks after the heartbeat, want 1", got)
			}
		})
	}
}

// A higher term that cannot be saved: nothing is sent and nothing changes (T2.6, CLAUDE.md rule 2).
func TestAppendEntries_TermHigherSaveFails(t *testing.T) {
	n, _ := spyNode(t, PersistentState{CurrentTerm: 5, VotedFor: "n1"}, Candidate, 1)

	if out := heartbeat(n, "n3", 8); len(out) != 0 {
		t.Errorf("Step() = %+v after a failed save, want no messages", out)
	}
	got := n.Snapshot()
	if got.Term != 5 || got.Role != Candidate || got.LeaderID != "" {
		t.Errorf("state = term %d, role %v, leader %q; want 5, Candidate, \"\" (unchanged)", got.Term, got.Role, got.LeaderID)
	}
}

// A newer leader replaces the one recorded for an earlier term.
func TestAppendEntries_TermNewLeaderReplacesOld(t *testing.T) {
	n, _, _ := emptyNodeAt(t, 5, "", Follower)
	heartbeat(n, "n2", 5)
	heartbeat(n, "n3", 6)
	if got := n.Snapshot().LeaderID; got != "n3" {
		t.Errorf("LeaderID = %q, want n3 (the leader of the newer term)", got)
	}
	heartbeat(n, "n2", 5) // a late heartbeat from the old leader
	if got := n.Snapshot().LeaderID; got != "n3" {
		t.Errorf("LeaderID = %q after a stale heartbeat, want n3", got)
	}
}
