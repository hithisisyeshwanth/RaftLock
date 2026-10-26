// Command raftsim runs a simulated Raft cluster and prints its trace, so you can watch elections happen.
//
//	go run ./cmd/raftsim -nodes 3 -seed 1 -ticks 600 -kill-leader-at 300
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
	"github.com/hithisisyeshwanth/RaftLock/internal/sim"
)

type Config struct {
	Nodes        int
	Seed         int64
	Ticks        int
	KillLeaderAt int // tick at which to crash the current leader; 0 means never
}

// Run simulates cfg.Ticks ticks and writes one line per trace event to w. The same cfg always prints the same output.
func Run(cfg Config, w io.Writer) error {
	c := sim.NewCluster(cfg.Nodes, cfg.Seed)
	c.Tracer = raft.NewWriterTracer(w)
	for c.Now() < cfg.Ticks {
		if cfg.KillLeaderAt > 0 && c.Now() == cfg.KillLeaderAt {
			if id, ok := c.Leader(); ok {
				c.Crash(id)
				fmt.Fprintf(w, "tick %d: killed leader %s\n", c.Now(), id)
			}
		}
		if err := c.Step(); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	var cfg Config
	flag.IntVar(&cfg.Nodes, "nodes", 3, "number of nodes")
	flag.Int64Var(&cfg.Seed, "seed", 1, "random seed; the same seed replays the same run")
	flag.IntVar(&cfg.Ticks, "ticks", 600, "ticks to simulate (1 tick = 1 ms of logical time)")
	flag.IntVar(&cfg.KillLeaderAt, "kill-leader-at", 0, "tick at which to crash the leader (0 = never)")
	flag.Parse()

	if err := Run(cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "raftsim:", err)
		os.Exit(1)
	}
}
