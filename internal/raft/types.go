package raft

import "bytes"

type Term uint64

// Index is 1-based; index 0 is the log sentinel.
type Index uint64

type NodeID string

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

type Entry struct {
	Term  Term
	Index Index
	Cmd   []byte
}

// Equal exists because Entry holds a slice and so cannot use ==.
func (e Entry) Equal(o Entry) bool {
	return e.Term == o.Term && e.Index == o.Index && bytes.Equal(e.Cmd, o.Cmd)
}

// Message is a one-way async message; Body is one of the four structs below.
type Message struct {
	From, To NodeID
	Body     any
}

type RequestVoteArgs struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex Index
	LastLogTerm  Term
}

type RequestVoteReply struct {
	Term        Term
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []Entry
	LeaderCommit Index
}

// PrevLogIndex echoes the request. MatchIndex = prev + len(entries) on success.
type AppendEntriesReply struct {
	Term         Term
	Success      bool
	MatchIndex   Index
	PrevLogIndex Index
}
