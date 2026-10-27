package main

import (
	"bytes"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
	"github.com/hithisisyeshwanth/RaftLock/internal/rpc"
)

// These tests run the CLI against three real servers in this process, each on its own localhost
// port, talking HTTP/JSON with real 1 ms ticks.

var ids = []raft.NodeID{"n1", "n2", "n3"}

type cluster struct {
	rts   map[raft.NodeID]*rpc.Runtime
	addrs map[raft.NodeID]string
}

func startCluster(t *testing.T) *cluster {
	t.Helper()
	c := &cluster{rts: map[raft.NodeID]*rpc.Runtime{}, addrs: map[raft.NodeID]string{}}
	lns := map[raft.NodeID]net.Listener{}
	for _, id := range ids {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[id], c.addrs[id] = ln, ln.Addr().String()
	}
	for i, id := range ids {
		peers := map[raft.NodeID]string{}
		for _, p := range ids {
			if p != id {
				peers[p] = c.addrs[p]
			}
		}
		rt, err := rpc.Start(rpc.Config{
			ID:       id,
			Peers:    peers,
			Listener: lns[id],
			Storage:  raft.NewMemStorage(),
			Raft:     raft.DefaultConfig(),
			Seed:     int64(i + 1),
		})
		if err != nil {
			t.Fatalf("Start(%s): %v", id, err)
		}
		c.rts[id] = rt
		t.Cleanup(func() { rt.Close() })
	}
	return c
}

// leader waits until exactly one live server reports itself leader.
func (c *cluster) leader(t *testing.T) raft.NodeID {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []raft.NodeID
		for id, rt := range c.rts {
			if s, err := rt.Snapshot(); err == nil && s.Role == raft.Leader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no single leader within 5s")
	return ""
}

// servers is the --servers value: every server, in the given order.
func (c *cluster) servers(order ...raft.NodeID) string {
	if len(order) == 0 {
		order = ids
	}
	var out []string
	for _, id := range order {
		out = append(out, c.addrs[id])
	}
	return strings.Join(out, ",")
}

type result struct {
	code int
	out  string // stdout and stderr together
}

// ctl runs the CLI once, as one process would.
func ctl(t *testing.T, args ...string) result {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(args, &stdout, &stderr)
	return result{code, stdout.String() + stderr.String()}
}

var tokenRE = regexp.MustCompile(`token=(\d+)`)

func token(t *testing.T, r result) raft.Index {
	t.Helper()
	m := tokenRE.FindStringSubmatch(r.out)
	if m == nil {
		t.Fatalf("no token=N in output %q", r.out)
	}
	n, _ := strconv.ParseUint(m[1], 10, 64)
	return raft.Index(n)
}

func mustOK(t *testing.T, r result, what string) {
	t.Helper()
	if r.code != 0 {
		t.Fatalf("%s: exit %d, output %q; want exit 0", what, r.code, r.out)
	}
}

func TestAcquirePrintsToken(t *testing.T) {
	c := startCluster(t)
	c.leader(t)

	r := ctl(t, "--servers", c.servers(), "--client", "c1", "acquire", "jobs", "--ttl", "5000")

	mustOK(t, r, "acquire")
	if tok := token(t, r); tok == 0 {
		t.Errorf("acquire printed token 0, want the log index of the grant")
	}
}

// Drill step 2: a second client is denied, and told who holds the lock.
func TestAcquireDeniedNamesHolder(t *testing.T) {
	c := startCluster(t)
	c.leader(t)
	mustOK(t, ctl(t, "--servers", c.servers(), "--client", "c1", "acquire", "jobs", "--ttl", "5000"), "c1 acquire")

	r := ctl(t, "--servers", c.servers(), "--client", "c2", "acquire", "jobs", "--ttl", "5000")

	if r.code != 1 {
		t.Errorf("denied acquire: exit %d, want 1", r.code)
	}
	if !strings.Contains(r.out, "c1") {
		t.Errorf("denied acquire output %q, want it to name the holder c1", r.out)
	}
}

func TestStatus(t *testing.T) {
	c := startCluster(t)
	c.leader(t)
	free := ctl(t, "--servers", c.servers(), "--client", "c2", "status", "jobs")
	mustOK(t, free, "status of a free lock")
	if !strings.Contains(free.out, "free") {
		t.Errorf("status of a free lock printed %q, want it to say free", free.out)
	}

	tok := token(t, ctl(t, "--servers", c.servers(), "--client", "c1", "acquire", "jobs", "--ttl", "5000"))
	held := ctl(t, "--servers", c.servers(), "--client", "c2", "status", "jobs")

	mustOK(t, held, "status of a held lock")
	if !strings.Contains(held.out, "c1") || token(t, held) != tok {
		t.Errorf("status printed %q, want holder c1 and token=%d", held.out, tok)
	}
}

// Drill step 5. Each run is a new process for the same client, and each must take effect: a
// release must not be answered with the cached result of that client's earlier acquire.
func TestReleaseThenReacquireGivesHigherToken(t *testing.T) {
	c := startCluster(t)
	c.leader(t)
	first := token(t, ctl(t, "--servers", c.servers(), "--client", "c1", "acquire", "jobs", "--ttl", "5000"))

	rel := ctl(t, "--servers", c.servers(), "--client", "c1", "release", "jobs", "--token", fmt.Sprint(first))
	mustOK(t, rel, "release")
	if st := ctl(t, "--servers", c.servers(), "--client", "c3", "status", "jobs"); !strings.Contains(st.out, "free") {
		t.Fatalf("after release, status printed %q, want free", st.out)
	}
	again := ctl(t, "--servers", c.servers(), "--client", "c2", "acquire", "jobs", "--ttl", "5000")

	mustOK(t, again, "acquire after release")
	if second := token(t, again); second <= first {
		t.Errorf("token after release = %d, want more than the first grant's %d", second, first)
	}
}

// Two runs for the same client are two requests, not a retry of one.
func TestSameClientRunsAreSeparateRequests(t *testing.T) {
	c := startCluster(t)
	c.leader(t)
	a := token(t, ctl(t, "--servers", c.servers(), "--client", "c1", "acquire", "a", "--ttl", "5000"))
	b := token(t, ctl(t, "--servers", c.servers(), "--client", "c1", "acquire", "b", "--ttl", "5000"))

	if a == b {
		t.Errorf("acquire a and acquire b both got token %d; the second run was answered as a duplicate of the first", a)
	}
}

func TestReleaseWrongTokenRefused(t *testing.T) {
	c := startCluster(t)
	c.leader(t)
	tok := token(t, ctl(t, "--servers", c.servers(), "--client", "c1", "acquire", "jobs", "--ttl", "5000"))

	r := ctl(t, "--servers", c.servers(), "--client", "c1", "release", "jobs", "--token", fmt.Sprint(tok+100))

	if r.code != 1 {
		t.Errorf("release with a wrong token: exit %d, output %q; want exit 1", r.code, r.out)
	}
	if st := ctl(t, "--servers", c.servers(), "--client", "c2", "status", "jobs"); token(t, st) != tok {
		t.Errorf("after a refused release, status printed %q, want still held with token=%d", st.out, tok)
	}
}

// --ttl reaches the server: the lease runs out and the lock is freed.
func TestAcquireTTLExpires(t *testing.T) {
	c := startCluster(t)
	c.leader(t)
	mustOK(t, ctl(t, "--servers", c.servers(), "--client", "c1", "acquire", "jobs", "--ttl", "200"), "acquire")

	deadline := time.Now().Add(3 * time.Second)
	for {
		st := ctl(t, "--servers", c.servers(), "--client", "c2", "status", "jobs")
		if strings.Contains(st.out, "free") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lock with ttl 200 ms still held after 3 s: %q", st.out)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A follower listed first redirects the CLI to the leader.
func TestFollowerFirstReachesLeader(t *testing.T) {
	c := startCluster(t)
	leader := c.leader(t)
	var order []raft.NodeID
	for _, id := range ids {
		if id != leader {
			order = append(order, id)
		}
	}
	order = append(order, leader)

	r := ctl(t, "--servers", c.servers(order...), "--client", "c1", "acquire", "jobs", "--ttl", "5000")

	mustOK(t, r, "acquire via a follower")
	token(t, r)
}

// Drill steps 3 and 4: with the leader gone, status is served by the new leader.
func TestStatusAfterLeaderKilled(t *testing.T) {
	c := startCluster(t)
	leader := c.leader(t)
	tok := token(t, ctl(t, "--servers", c.servers(), "--client", "c1", "acquire", "jobs", "--ttl", "60000"))

	c.rts[leader].Close()
	delete(c.rts, leader)
	r := ctl(t, "--servers", c.servers(), "--client", "c2", "status", "jobs")

	mustOK(t, r, "status after the leader was killed")
	if !strings.Contains(r.out, "c1") || token(t, r) != tok {
		t.Errorf("status printed %q, want still held by c1 with token=%d", r.out, tok)
	}
}

// Bad command lines exit 2 with a message naming the problem, without contacting any server.
func TestUsageErrors(t *testing.T) {
	const srv = "127.0.0.1:1" // nothing listens here; a usage error must not get that far
	tests := []struct {
		name string
		args []string
		want string // the output must contain this
	}{
		{"no servers", []string{"--client", "c1", "status", "jobs"}, "servers"},
		{"no client", []string{"--servers", srv, "status", "jobs"}, "client"},
		{"no command", []string{"--servers", srv, "--client", "c1"}, "command"},
		{"unknown command", []string{"--servers", srv, "--client", "c1", "grab", "jobs"}, "grab"},
		{"acquire without lock", []string{"--servers", srv, "--client", "c1", "acquire"}, "lock"},
		{"release without token", []string{"--servers", srv, "--client", "c1", "release", "jobs"}, "token"},
		{"token not a number", []string{"--servers", srv, "--client", "c1", "release", "jobs", "--token", "abc"}, "token"},
		{"ttl not a number", []string{"--servers", srv, "--client", "c1", "acquire", "jobs", "--ttl", "soon"}, "ttl"},
		{"status extra argument", []string{"--servers", srv, "--client", "c1", "status", "jobs", "more"}, "more"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := ctl(t, tt.args...)
			if r.code != 2 {
				t.Errorf("run(%q): exit %d, want 2", tt.args, r.code)
			}
			if !strings.Contains(r.out, tt.want) {
				t.Errorf("run(%q) output %q, want it to mention %q", tt.args, r.out, tt.want)
			}
		})
	}
}
