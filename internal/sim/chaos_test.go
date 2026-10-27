package sim

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/lock"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

const chaosTicks = 20000

// traceTail keeps the last lines of a run, so a failure can show how it got there.
type traceTail struct {
	lines []string
	max   int
	now   func() int
}

func (tt *traceTail) add(s string) {
	tt.lines = append(tt.lines, fmt.Sprintf("t=%d %s", tt.now(), s))
	if len(tt.lines) > tt.max {
		tt.lines = tt.lines[len(tt.lines)-tt.max:]
	}
}

func (tt *traceTail) Trace(e raft.Event) {
	tt.add(fmt.Sprintf("%s term=%d %v %s %s", e.Node, e.Term, e.Role, e.Kind, e.Detail))
}

// worker is a client that keeps taking the lock, holding it a little, releasing it, and reading it.
type worker struct {
	id      string
	ck      *lock.Clerk
	call    *lock.Call
	holding bool
	token   raft.Index
	held    int
}

func (w *worker) tick(tr *traceTail) {
	w.ck.Tick()
	switch {
	case w.call == nil:
		w.call = w.ck.Do(lock.Command{Kind: lock.Acquire, Lock: "jobs", TTL: 300})
	case !w.call.Done():
	case !w.holding && w.call.Result().OK:
		w.holding, w.token, w.held = true, w.call.Result().Token, 0
		tr.add(fmt.Sprintf("%s holds token %d", w.id, w.token))
		w.call = &lock.Call{} // holding: nothing in flight
	case !w.holding:
		w.call = nil // denied, try again
	default:
		w.holding, w.call = false, nil // released
	}
	if w.holding && !w.call.Done() {
		if w.held++; w.held == 25 {
			w.call = w.ck.Do(lock.Command{Kind: lock.Release, Lock: "jobs", Token: w.token})
		}
	}
}

// runChaos drives one seeded run: three lock servers, three clients, and a chaos monkey that
// crashes, restarts, partitions, heals and changes the drop rate. Every random choice, the
// monkey's included, comes from the seed. It returns the trace so callers can compare runs.
func runChaos(t *testing.T, seed int64, ticks int) *traceTail {
	t.Helper()
	c := NewClusterWith(3, seed, lockServer)
	tr := &traceTail{max: 200, now: c.Now}
	c.Tracer = tr
	monkey := NewChaos(c, seed)
	var ws []*worker
	for i := 1; i <= 3; i++ {
		ck := lock.NewClerk(fmt.Sprintf("c%d", i), three, clerkTransport{c})
		ck.AttemptTimeout = 400
		ws = append(ws, &worker{id: fmt.Sprintf("c%d", i), ck: ck})
	}
	for i := 0; i < ticks; i++ {
		if action := monkey.Step(); action != "" {
			tr.add("chaos: " + action)
		}
		for _, w := range ws {
			w.tick(tr)
		}
		if err := c.Step(); err != nil {
			t.Fatalf("seed %d failed at tick %d: %v\nreplay: CHAOS_SEED=%d go test ./internal/sim -run TestChaos -v\nlast %d trace lines:\n%s",
				seed, c.Now(), err, seed, len(tr.lines), strings.Join(tr.lines, "\n"))
		}
	}
	return tr
}

// chaosSeeds is CHAOS_SEED (one seed, to replay a failure) or 1..CHAOS_SEEDS (default 20).
func chaosSeeds(t *testing.T) []int64 {
	if s := os.Getenv("CHAOS_SEED"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatalf("CHAOS_SEED=%q: %v", s, err)
		}
		return []int64{n}
	}
	count := 20
	if s := os.Getenv("CHAOS_SEEDS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			t.Fatalf("CHAOS_SEEDS=%q: want a positive number", s)
		}
		count = n
	}
	var out []int64
	for i := 1; i <= count; i++ {
		out = append(out, int64(i))
	}
	return out
}

// TestChaos: 20000 ticks of random faults per seed, with all five Figure 3 checkers run by the
// harness after every tick. On failure it prints the seed, how to replay it, and the trace tail.
//
//	CHAOS_SEEDS=100 go test ./internal/sim -run TestChaos -race -count=1
//	CHAOS_SEED=17   go test ./internal/sim -run TestChaos -v
func TestChaos(t *testing.T) {
	for _, seed := range chaosSeeds(t) {
		t.Run(fmt.Sprintf("seed %d", seed), func(t *testing.T) {
			runChaos(t, seed, chaosTicks)
		})
	}
}

// The same seed replays the same run, which is what makes a printed seed useful.
func TestChaosSameSeedSameRun(t *testing.T) {
	a := runChaos(t, 7, 3000).lines
	b := runChaos(t, 7, 3000).lines
	if strings.Join(a, "\n") != strings.Join(b, "\n") {
		t.Fatal("two runs of seed 7 produced different traces")
	}
}

// The monkey really does all five kinds of fault.
func TestChaosMonkeyUsesEveryAction(t *testing.T) {
	c := NewClusterWith(3, 1, lockServer)
	monkey := NewChaos(c, 1)
	seen := map[string]bool{}
	for i := 0; i < chaosTicks; i++ {
		if action := monkey.Step(); action != "" {
			seen[strings.Fields(action)[0]] = true
		}
		if err := c.Step(); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []string{"crash", "restart", "partition", "heal", "drop"} {
		if !seen[kind] {
			t.Errorf("in %d ticks the monkey never did %q (did %v)", chaosTicks, kind, seen)
		}
	}
}
