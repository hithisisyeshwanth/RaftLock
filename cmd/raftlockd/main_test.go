package main

import (
	"maps"
	"strings"
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

func TestParseFlags(t *testing.T) {
	args := []string{"--id", "n1", "--listen", ":7001", "--peers", "n2=:7002,n3=localhost:7003", "--data-dir", "/tmp/rl/n1"}

	got, err := parseFlags(args)
	if err != nil {
		t.Fatalf("parseFlags(%q) = %v", args, err)
	}
	if got.ID != "n1" || got.Listen != ":7001" || got.DataDir != "/tmp/rl/n1" {
		t.Errorf("parseFlags = %+v, want id n1, listen :7001, data-dir /tmp/rl/n1", got)
	}
	want := map[raft.NodeID]string{"n2": ":7002", "n3": "localhost:7003"}
	if !maps.Equal(got.Peers, want) {
		t.Errorf("Peers = %v, want %v", got.Peers, want)
	}
}

// Bad input fails with an error that names the problem, so the operator can fix the command line.
func TestParseFlagsRejectsBadInput(t *testing.T) {
	valid := map[string]string{"id": "n1", "listen": ":7001", "peers": "n2=:7002,n3=:7003", "data-dir": "/tmp/rl/n1"}
	with := func(flag, value string) []string {
		var args []string
		for _, f := range []string{"id", "listen", "peers", "data-dir"} {
			v := valid[f]
			if f == flag {
				if value == "" {
					continue // leave the flag out
				}
				v = value
			}
			args = append(args, "--"+f, v)
		}
		return args
	}

	tests := []struct {
		name string
		args []string
		want string // the error must contain this
	}{
		{"missing id", with("id", ""), "id"},
		{"missing listen", with("listen", ""), "listen"},
		{"missing peers", with("peers", ""), "peers"},
		{"missing data-dir", with("data-dir", ""), "data-dir"},
		{"peer without =", with("peers", "n2:7002,n3=:7003"), "n2:7002"},
		{"peer without id", with("peers", "=:7002,n3=:7003"), "=:7002"},
		{"peer without address", with("peers", "n2=,n3=:7003"), "n2="},
		{"duplicate peer", with("peers", "n2=:7002,n2=:7003"), "n2"},
		{"peer is self", with("peers", "n1=:7002,n3=:7003"), "n1"},
		{"unknown flag", append(with("id", "n1"), "--bogus", "x"), "bogus"},
		{"stray argument", append(with("id", "n1"), "extra"), "extra"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFlags(tt.args)
			if err == nil {
				t.Fatalf("parseFlags(%q) = %+v, want an error", tt.args, got)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("parseFlags(%q) error = %q, want it to mention %q", tt.args, err, tt.want)
			}
		})
	}
}
