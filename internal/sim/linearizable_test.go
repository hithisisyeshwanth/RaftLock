package sim

import (
	"testing"

	"github.com/anishathalye/porcupine"

	"github.com/hithisisyeshwanth/RaftLock/internal/lock"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// op builds one completed operation on lock "jobs": client invoked it at call and saw out at ret.
func op(client int, kind lock.Kind, token raft.Index, call, ret int64, out LockOutput) porcupine.Operation {
	return porcupine.Operation{
		ClientId: client,
		Input:    LockInput{Kind: kind, Lock: "jobs", Client: clientName(client), Token: token},
		Call:     call,
		Output:   out,
		Return:   ret,
	}
}

func clientName(i int) string { return []string{"a", "b", "c"}[i] }

func granted(tok raft.Index) LockOutput         { return LockOutput{OK: true, Token: tok} }
func held(by string, tok raft.Index) LockOutput { return LockOutput{OK: true, Holder: by, Token: tok} }
func denied(by string) LockOutput               { return LockOutput{Holder: by} }
func ok() LockOutput                            { return LockOutput{OK: true} }

const a, b, cc = 0, 1, 2

// A plain, sequential history: every answer matches a single lock taking the operations one at a time.
func TestLinearizableValidHistory(t *testing.T) {
	h := []porcupine.Operation{
		op(a, lock.Acquire, 0, 1, 5, granted(3)),
		op(b, lock.Status, 0, 6, 8, held("a", 3)),
		op(cc, lock.Acquire, 0, 9, 11, denied("a")),
		op(a, lock.Release, 3, 12, 15, ok()),
		op(b, lock.Status, 0, 16, 18, held("", 0)),
		op(b, lock.Acquire, 0, 19, 22, granted(7)),
	}
	if !porcupine.CheckOperations(LockModel, h) {
		t.Error("a sequential, correct history was judged not linearizable")
	}
}

// The deliberate failure: a's release has returned, so any read that starts afterwards must see
// the lock free. A read that still reports a as the holder is stale, as a deposed leader serving
// reads from memory would produce.
func TestLinearizableStaleReadFails(t *testing.T) {
	h := []porcupine.Operation{
		op(a, lock.Acquire, 0, 1, 5, granted(3)),
		op(a, lock.Release, 3, 6, 9, ok()),
		op(b, lock.Status, 0, 10, 12, held("a", 3)), // starts after the release returned
	}
	if porcupine.CheckOperations(LockModel, h) {
		t.Error("a stale read after a completed release was judged linearizable")
	}
}

// Operations that overlap in time may take effect in either order, so a read overlapping the
// release may see either state, but not both orders at once in one history.
func TestLinearizableOverlapAllowsEitherAnswer(t *testing.T) {
	for _, read := range []LockOutput{held("a", 3), held("", 0)} {
		h := []porcupine.Operation{
			op(a, lock.Acquire, 0, 1, 5, granted(3)),
			op(a, lock.Release, 3, 6, 12, ok()),
			op(b, lock.Status, 0, 8, 10, read), // inside the release
		}
		if !porcupine.CheckOperations(LockModel, h) {
			t.Errorf("a read overlapping the release that saw %+v was judged not linearizable", read)
		}
	}
}

func TestLinearizableModelRejectsWrongAnswers(t *testing.T) {
	tests := []struct {
		name string
		h    []porcupine.Operation
	}{
		{"two clients granted the lock", []porcupine.Operation{
			op(a, lock.Acquire, 0, 1, 3, granted(3)),
			op(b, lock.Acquire, 0, 4, 6, granted(5)),
		}},
		{"a later grant with a lower token", []porcupine.Operation{
			op(a, lock.Acquire, 0, 1, 3, granted(5)),
			op(a, lock.Release, 5, 4, 6, ok()),
			op(b, lock.Acquire, 0, 7, 9, granted(2)),
		}},
		{"a denial naming the wrong holder", []porcupine.Operation{
			op(a, lock.Acquire, 0, 1, 3, granted(3)),
			op(b, lock.Acquire, 0, 4, 6, denied("c")),
		}},
		{"a release by a client that never held it", []porcupine.Operation{
			op(a, lock.Acquire, 0, 1, 3, granted(3)),
			op(b, lock.Release, 3, 4, 6, ok()),
		}},
		{"the holder re-acquiring gets a new token", []porcupine.Operation{
			op(a, lock.Acquire, 0, 1, 3, granted(3)),
			op(a, lock.Acquire, 0, 4, 6, granted(8)),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if porcupine.CheckOperations(LockModel, tt.h) {
				t.Error("judged linearizable, want rejected")
			}
		})
	}
}

// The recorder turns what clients did into Porcupine operations, with invoke and return ticks.
func TestLinearizableHistoryRecorder(t *testing.T) {
	h := NewHistory()
	id := h.Invoke(1, LockInput{Kind: lock.Acquire, Lock: "jobs", Client: "b"}, 10)
	h.Invoke(2, LockInput{Kind: lock.Status, Lock: "jobs", Client: "c"}, 12) // never returns
	h.Return(id, granted(4), 17)

	ops := h.Operations()
	var done *porcupine.Operation
	for i := range ops {
		if ops[i].ClientId == 1 {
			done = &ops[i]
		}
	}
	if done == nil || done.Call != 10 || done.Return != 17 || done.Output != granted(4) ||
		done.Input != (LockInput{Kind: lock.Acquire, Lock: "jobs", Client: "b"}) {
		t.Errorf("recorded %+v, want client 1's acquire called at 10, returned at 17 with %+v", ops, granted(4))
	}
}

// End to end: three clients take, read and release the lock through real servers while messages
// are dropped. Every answer they got must be explainable by one lock taking the operations one at
// a time, in an order consistent with real time.
func TestLinearizableRecordedRun(t *testing.T) {
	c := NewClusterWith(3, 1, lockServer)
	if err := c.RunUntil(hasLeader(c), 1000); err != nil {
		t.Fatal(err)
	}
	c.SetDrop(0.1)
	h := NewHistory()

	type client struct {
		ck    *lock.Clerk
		call  *lock.Call
		id    int
		in    LockInput
		token raft.Index
		step  int
	}
	var cs []*client
	for i := 0; i < 3; i++ {
		ck := lock.NewClerk(clientName(i), three, clerkTransport{c})
		ck.AttemptTimeout = 400
		cs = append(cs, &client{ck: ck})
	}
	for tick := 0; tick < 3000; tick++ {
		for i, cl := range cs {
			cl.ck.Tick()
			if cl.call != nil && !cl.call.Done() {
				continue
			}
			if cl.call != nil {
				r := cl.call.Result()
				h.Return(cl.id, LockOutput{OK: r.OK, Token: r.Token, Holder: r.Holder}, int64(c.Now()))
				if cl.in.Kind == lock.Acquire && r.OK {
					cl.token = r.Token
				}
				if cl.in.Kind == lock.Release && r.OK {
					cl.token = 0
				}
			}
			// cycle: acquire, status, release (if holding), status
			cl.step++
			cmd := lock.Command{Kind: lock.Status, Lock: "jobs"}
			switch cl.step % 4 {
			case 1:
				cmd = lock.Command{Kind: lock.Acquire, Lock: "jobs"}
			case 3:
				if cl.token != 0 {
					cmd = lock.Command{Kind: lock.Release, Lock: "jobs", Token: cl.token}
				}
			}
			cl.in = LockInput{Kind: cmd.Kind, Lock: cmd.Lock, Client: clientName(i), Token: cmd.Token}
			cl.id = h.Invoke(i, cl.in, int64(c.Now()))
			cl.call = cl.ck.Do(cmd)
		}
		if err := c.Step(); err != nil {
			t.Fatal(err)
		}
	}

	ops := h.Operations()
	if len(ops) < 30 {
		t.Fatalf("only %d operations recorded; the run shows too little", len(ops))
	}
	if !porcupine.CheckOperations(LockModel, ops) {
		t.Error("the recorded history is not linearizable")
	}
}
