package main

import (
	"bytes"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Event lines come from the writer tracer: "<node> term=<n> <role> <kind> <detail>".
var termRE = regexp.MustCompile(`term=(\d+)`)

func run(t *testing.T, cfg Config) []string {
	t.Helper()
	var buf bytes.Buffer
	if err := Run(cfg, &buf); err != nil {
		t.Fatalf("Run(%+v) = %v", cfg, err)
	}
	if buf.Len() == 0 {
		t.Fatalf("Run(%+v) printed nothing", cfg)
	}
	return strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
}

type leaderLine struct {
	at   int // line number
	node string
	term int
}

func leaderLines(t *testing.T, lines []string) []leaderLine {
	t.Helper()
	var out []leaderLine
	for i, l := range lines {
		if !strings.Contains(l, "became-leader") {
			continue
		}
		m := termRE.FindStringSubmatch(l)
		if m == nil {
			t.Fatalf("leader line %q has no term=<n>", l)
		}
		term, _ := strconv.Atoi(m[1])
		out = append(out, leaderLine{at: i, node: strings.Fields(l)[0], term: term})
	}
	return out
}

func TestRunSameSeedSameOutput(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		cfg := Config{Nodes: 3, Seed: seed, Ticks: 600, KillLeaderAt: 300}
		a, b := run(t, cfg), run(t, cfg)
		if strings.Join(a, "\n") != strings.Join(b, "\n") {
			t.Errorf("seed %d: two runs printed different output", seed)
		}
	}
}

func TestRunPrintsElection(t *testing.T) {
	lines := run(t, Config{Nodes: 3, Seed: 1, Ticks: 600})
	leaders := leaderLines(t, lines)
	if len(leaders) == 0 {
		t.Fatalf("no line containing \"became-leader\" in:\n%s", strings.Join(lines, "\n"))
	}
	if !strings.HasPrefix(leaders[0].node, "n") {
		t.Errorf("leader line %q does not start with the node id", lines[leaders[0].at])
	}
	for _, l := range lines {
		if strings.Contains(l, "kill") {
			t.Errorf("printed %q although KillLeaderAt is 0 (no kill)", l)
		}
	}
}

// The manual check from the spec: an election, the kill, and a new election in a higher term.
func TestRunPrintsKillAndReelection(t *testing.T) {
	lines := run(t, Config{Nodes: 3, Seed: 1, Ticks: 600, KillLeaderAt: 300})
	leaders := leaderLines(t, lines)

	kill := -1
	for i, l := range lines {
		if strings.Contains(l, "kill") {
			kill = i
			break
		}
	}
	if kill < 0 {
		t.Fatalf("no kill line in:\n%s", strings.Join(lines, "\n"))
	}

	var before, after *leaderLine
	for i := range leaders {
		if leaders[i].at < kill {
			before = &leaders[i]
		} else if after == nil {
			after = &leaders[i]
		}
	}
	if before == nil {
		t.Fatal("the kill line comes before any leader was elected")
	}
	if !strings.Contains(lines[kill], before.node) {
		t.Errorf("kill line %q does not name the leader %s", lines[kill], before.node)
	}
	if after == nil {
		t.Fatal("no new leader was elected after the kill")
	}
	if after.node == before.node || after.term <= before.term {
		t.Errorf("leader after the kill is %s in term %d, want a different node in a term above %d", after.node, after.term, before.term)
	}
}
