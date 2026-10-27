package raft

import (
	"fmt"
	"math/rand"
)

// Config times are in ticks (1 tick = 1 ms of logical time).
type Config struct {
	ElectionTimeoutMin int
	ElectionTimeoutMax int
	HeartbeatInterval  int

	// DisableNoop stops a new leader appending its no-op. Tests only: Figure 8 (T4.7) needs a leader without one.
	DisableNoop bool
}

func DefaultConfig() Config {
	return Config{ElectionTimeoutMin: 150, ElectionTimeoutMax: 300, HeartbeatInterval: 50}
}

func (c Config) Validate() error {
	if c.ElectionTimeoutMin >= c.ElectionTimeoutMax {
		return fmt.Errorf("election timeout min (%d) must be below max (%d)", c.ElectionTimeoutMin, c.ElectionTimeoutMax)
	}
	if c.HeartbeatInterval >= c.ElectionTimeoutMin {
		return fmt.Errorf("heartbeat interval (%d) must be below election timeout min (%d)", c.HeartbeatInterval, c.ElectionTimeoutMin)
	}
	return nil
}

// TimeoutSource supplies election timeouts, so tests can make them reproducible or force split votes.
type TimeoutSource interface {
	Next() int
}

// SeededTimeouts draws uniformly from [min, max], inclusive, from a private seeded generator.
type SeededTimeouts struct {
	rng      *rand.Rand
	min, max int
}

func NewSeededTimeouts(seed int64, min, max int) *SeededTimeouts {
	return &SeededTimeouts{rng: rand.New(rand.NewSource(seed)), min: min, max: max}
}

func (s *SeededTimeouts) Next() int {
	return s.min + s.rng.Intn(s.max-s.min+1)
}

// FixedTimeouts always returns the same value.
type FixedTimeouts struct {
	v int
}

func NewFixedTimeouts(v int) *FixedTimeouts {
	return &FixedTimeouts{v: v}
}

func (f *FixedTimeouts) Next() int {
	return f.v
}
