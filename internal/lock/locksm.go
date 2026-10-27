package lock

import (
	"encoding/json"
	"fmt"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

type holder struct {
	Client string
	Token  raft.Index // the log index of the Acquire that granted the lock
}

// session is a client's latest command, for applying each (clientID, seq) exactly once.
type session struct {
	Seq    uint64
	Result Result
}

// LockSM is the lock table. There is no wait queue: a held lock is refused and the client retries.
// Its state, the deduplication table included, depends only on the commands applied and their indexes.
type LockSM struct {
	locks    map[string]holder
	sessions map[string]session
}

func NewLockSM() *LockSM {
	return &LockSM{locks: map[string]holder{}, sessions: map[string]session{}}
}

// Apply deduplicates at apply time (section 8): a client may resend a command after a timeout and
// both copies can be committed. A repeat of the client's latest seq gets the cached result, and an
// older seq is ignored, since the client has moved on and is no longer waiting for it.
func (s *LockSM) Apply(index raft.Index, b []byte) Result {
	c, err := Decode(b)
	if err != nil {
		return Result{Err: err.Error()}
	}
	if c.ClientID == "" {
		return s.apply(index, c)
	}
	if last, ok := s.sessions[c.ClientID]; ok {
		if c.Seq == last.Seq {
			return last.Result
		}
		if c.Seq < last.Seq {
			return Result{Err: fmt.Sprintf("stale request: %s seq %d is older than %d", c.ClientID, c.Seq, last.Seq)}
		}
	}
	r := s.apply(index, c)
	s.sessions[c.ClientID] = session{Seq: c.Seq, Result: r}
	return r
}

func (s *LockSM) apply(index raft.Index, c Command) Result {
	switch c.Kind {
	case Acquire:
		h, held := s.locks[c.Lock]
		switch {
		case !held:
			s.locks[c.Lock] = holder{Client: c.ClientID, Token: index}
			return Result{OK: true, Token: index}
		case h.Client == c.ClientID:
			return Result{OK: true, Token: h.Token}
		default:
			return Result{Holder: h.Client, Err: fmt.Sprintf("lock %q is held by %s", c.Lock, h.Client)}
		}
	case Release:
		h, held := s.locks[c.Lock]
		if !held || h.Client != c.ClientID || h.Token != c.Token {
			return Result{Err: fmt.Sprintf("lock %q is not held by %s with token %d", c.Lock, c.ClientID, c.Token)}
		}
		delete(s.locks, c.Lock)
		return Result{OK: true}
	}
	return Result{Err: fmt.Sprintf("%s is not supported yet", c.Kind)} // Expire is T5.4
}

// Dump is the whole replicated state, lock table and deduplication table, as a string for
// comparing replicas. json.Marshal sorts map keys, so the same state always gives the same string.
func (s *LockSM) Dump() string {
	b, _ := json.Marshal(struct {
		Locks    map[string]holder
		Sessions map[string]session
	}{s.locks, s.sessions})
	return string(b)
}
