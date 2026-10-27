package lock

import (
	"bytes"
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// The state machine has no clock: Apply sees only the log index and the command bytes, so every
// replica applying the same log reaches the same state. This fake only compiles if the interface
// is exactly Apply(raft.Index, []byte) Result; a time parameter would break it.
type noClock struct{}

func (noClock) Apply(raft.Index, []byte) Result { return Result{} }

var _ StateMachine = noClock{}

func TestCommandRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		cmd  Command
	}{
		{"acquire", Command{Kind: Acquire, Lock: "jobs", ClientID: "c1", Seq: 1, TTL: 5000}},
		{"acquire with no TTL", Command{Kind: Acquire, Lock: "jobs", ClientID: "c1", Seq: 2}},
		{"release", Command{Kind: Release, Lock: "jobs", ClientID: "c1", Seq: 3, Token: 17}},
		{"expire", Command{Kind: Expire, Lock: "jobs", Token: 17}},
		{"lock name with spaces and unicode", Command{Kind: Acquire, Lock: "build queue ✓", ClientID: "c 2", Seq: 9}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := Encode(tt.cmd)
			if err != nil {
				t.Fatalf("Encode(%+v) error = %v", tt.cmd, err)
			}
			got, err := Decode(b)
			if err != nil {
				t.Fatalf("Decode(%q) error = %v", b, err)
			}
			if got != tt.cmd {
				t.Errorf("round trip = %+v, want %+v", got, tt.cmd)
			}
		})
	}
}

// The encoded bytes go into the Raft log and every replica decodes them, so the same command must
// always encode to the same bytes.
func TestCommandEncodingIsDeterministic(t *testing.T) {
	cmd := Command{Kind: Release, Lock: "jobs", ClientID: "c1", Seq: 3, Token: 17}
	a, _ := Encode(cmd)
	b, _ := Encode(cmd)
	if !bytes.Equal(a, b) {
		t.Errorf("Encode gave %q then %q for the same command", a, b)
	}
}

// Bad bytes in the log must never crash a replica: decoding reports an error instead.
func TestCommandDecodeRejectsBadInput(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
	}{
		{"unknown kind", []byte(`{"Kind":"teleport","Lock":"jobs"}`)},
		{"missing kind", []byte(`{"Lock":"jobs"}`)},
		{"not JSON", []byte("acquire jobs")},
		{"empty", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("Decode(%q) panicked: %v", tt.in, p)
				}
			}()
			if _, err := Decode(tt.in); err == nil {
				t.Errorf("Decode(%q) returned no error", tt.in)
			}
		})
	}
}

func TestCommandEncodeRejectsUnknownKind(t *testing.T) {
	if _, err := Encode(Command{Kind: "teleport", Lock: "jobs"}); err == nil {
		t.Error("Encode of an unknown kind returned no error")
	}
}
