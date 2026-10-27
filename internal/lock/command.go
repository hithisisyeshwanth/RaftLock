// Package lock is the lock service built on the Raft log: commands, the lock state machine,
// and the server and client on top of it.
package lock

import (
	"encoding/json"
	"fmt"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// StateMachine applies committed log entries. It has no clock: given the same entries at the same
// indexes, every replica reaches the same state.
type StateMachine interface {
	Apply(index raft.Index, cmd []byte) Result
}

// Result is what applying one command produced. T5.2 adds the lock outcome fields.
type Result struct {
	Err string // set when the command could not be applied, e.g. it did not decode
}

type Kind string

const (
	Acquire Kind = "acquire"
	Release Kind = "release"
	Expire  Kind = "expire" // proposed by the leader, never by a client
)

// Command is one lock operation as stored in the Raft log. Not every field applies to every kind.
type Command struct {
	Kind     Kind
	Lock     string
	ClientID string
	Seq      uint64     // per-client sequence number, for deduplication
	TTL      int        // Acquire: ticks the lock may be held before the leader expires it
	Token    raft.Index // Release, Expire: the fencing token being released or expired
}

func Encode(c Command) ([]byte, error) {
	if !c.Kind.valid() {
		return nil, fmt.Errorf("encode lock command: unknown kind %q", c.Kind)
	}
	return json.Marshal(c)
}

// Decode never panics on bad input, because the bytes come from the log.
func Decode(b []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(b, &c); err != nil {
		return Command{}, fmt.Errorf("decode lock command: %w", err)
	}
	if !c.Kind.valid() {
		return Command{}, fmt.Errorf("decode lock command: unknown kind %q", c.Kind)
	}
	return c, nil
}

func (k Kind) valid() bool {
	return k == Acquire || k == Release || k == Expire
}
