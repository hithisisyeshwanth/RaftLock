package sim

import (
	"math"

	"github.com/anishathalye/porcupine"

	"github.com/hithisisyeshwanth/RaftLock/internal/lock"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// LockInput and LockOutput are one client operation and its answer, as recorded for checking.
type LockInput struct {
	Kind   lock.Kind
	Lock   string
	Client string
	Token  raft.Index // Release: the token being released
}

type LockOutput struct {
	OK     bool
	Token  raft.Index
	Holder string
}

// lockState is one lock as a sequential object: who holds it, with which token, and the highest
// token ever granted, since fencing tokens must keep increasing.
type lockState struct {
	holder string
	token  raft.Index
	last   raft.Index
}

// LockModel describes the lock service as a single sequential object per lock, for Porcupine.
// A history is linearizable if every answer clients got matches the operations taking effect one at
// a time, each at some instant between its invoke and its return. An operation that never returned
// has no output: it may or may not have taken effect, and the model accepts it either way.
var LockModel = porcupine.Model{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		by := map[string][]porcupine.Operation{}
		var order []string // first-seen order, so the partition is the same on every run
		for _, o := range h {
			l := o.Input.(LockInput).Lock
			if _, ok := by[l]; !ok {
				order = append(order, l)
			}
			by[l] = append(by[l], o)
		}
		out := make([][]porcupine.Operation, 0, len(order))
		for _, l := range order {
			out = append(out, by[l])
		}
		return out
	},
	Init: func() interface{} { return lockState{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		s, in := state.(lockState), input.(LockInput)
		out, known := output.(LockOutput)
		switch in.Kind {
		case lock.Acquire:
			switch {
			case s.holder == "":
				if !known {
					return true, s
				}
				if out.OK && out.Token > s.last {
					return true, lockState{holder: in.Client, token: out.Token, last: out.Token}
				}
				return false, s
			case s.holder == in.Client: // re-acquire keeps the token
				return !known || (out.OK && out.Token == s.token), s
			default: // denied, naming the holder
				return !known || (!out.OK && out.Holder == s.holder), s
			}
		case lock.Release:
			if s.holder == in.Client && s.token == in.Token {
				if !known || out.OK {
					return true, lockState{last: s.last}
				}
				return false, s
			}
			return !known || !out.OK, s
		case lock.Status:
			return !known || (out.OK && out.Holder == s.holder && out.Token == s.token), s
		}
		return false, s
	},
	Equal: func(a, b interface{}) bool { return a == b },
}

// History records client operations with their invoke and return ticks, for LockModel.
type History struct {
	ops []porcupine.Operation
}

func NewHistory() *History { return &History{} }

// Invoke records that client started in at tick, and returns an id to pass to Return.
func (h *History) Invoke(client int, in LockInput, tick int64) int {
	h.ops = append(h.ops, porcupine.Operation{ClientId: client, Input: in, Call: tick, Return: math.MaxInt64})
	return len(h.ops) - 1
}

// Return records the answer to operation id at tick.
func (h *History) Return(id int, out LockOutput, tick int64) {
	h.ops[id].Output, h.ops[id].Return = out, tick
}

// Operations is the history so far. An operation that has not returned keeps Return at +infinity
// and no output, which LockModel treats as "may or may not have happened".
func (h *History) Operations() []porcupine.Operation {
	return append([]porcupine.Operation(nil), h.ops...)
}
