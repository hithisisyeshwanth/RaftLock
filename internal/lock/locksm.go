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

// LockSM is the lock table. There is no wait queue: a held lock is refused and the client retries.
// Its state depends only on the commands applied and their indexes.
type LockSM struct {
	locks map[string]holder
}

func NewLockSM() *LockSM {
	return &LockSM{locks: map[string]holder{}}
}

func (s *LockSM) Apply(index raft.Index, b []byte) Result {
	c, err := Decode(b)
	if err != nil {
		return Result{Err: err.Error()}
	}
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

// Dump is the lock table as a string, for comparing replicas. json.Marshal sorts map keys,
// so the same table always gives the same string.
func (s *LockSM) Dump() string {
	b, _ := json.Marshal(s.locks)
	return string(b)
}
