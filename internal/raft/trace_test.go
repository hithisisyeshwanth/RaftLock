package raft

import "testing"

var _ Tracer = (*Recorder)(nil)

func TestTracerRecordsEventsInOrder(t *testing.T) {
	r := NewRecorder()
	want := []Event{
		{Node: "n1", Term: 1, Role: Candidate, Kind: "election-start", Detail: "term 1"},
		{Node: "n2", Term: 1, Role: Follower, Kind: "vote-granted", Detail: "to n1"},
		{Node: "n1", Term: 1, Role: Leader, Kind: "became-leader"},
	}
	for _, e := range want {
		r.Trace(e)
	}

	got := r.Events()
	if len(got) != len(want) {
		t.Fatalf("Events() has %d events, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Events()[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestTracerEmptyRecorder(t *testing.T) {
	if got := NewRecorder().Events(); len(got) != 0 {
		t.Errorf("Events() on a new recorder = %+v, want none", got)
	}
}

func TestTracerEventsReturnsCopy(t *testing.T) {
	r := NewRecorder()
	r.Trace(Event{Node: "n1", Term: 2, Kind: "k"})

	first := r.Events()
	first[0].Term = 99
	_ = append(first, Event{Node: "extra"})

	got := r.Events()
	if len(got) != 1 || got[0].Term != 2 {
		t.Errorf("mutating Events() output changed the recording: %+v", got)
	}
}

// trace is how the core emits events; a node built without a tracer passes nil.
func TestTracerNilTracerIsSafe(t *testing.T) {
	t.Run("nil interface", func(t *testing.T) {
		defer func() {
			if p := recover(); p != nil {
				t.Errorf("trace(nil, ...) panicked: %v", p)
			}
		}()
		trace(nil, Event{Node: "n1", Kind: "k"})
	})
	t.Run("nil recorder", func(t *testing.T) {
		defer func() {
			if p := recover(); p != nil {
				t.Errorf("nil *Recorder panicked: %v", p)
			}
		}()
		var r *Recorder
		r.Trace(Event{Node: "n1", Kind: "k"})
		if got := r.Events(); len(got) != 0 {
			t.Errorf("Events() on a nil recorder = %+v, want none", got)
		}
	})
}

func TestTracerHelperForwardsToTracer(t *testing.T) {
	r := NewRecorder()
	e := Event{Node: "n3", Term: 4, Role: Follower, Kind: "k", Detail: "d"}
	trace(r, e)
	if got := r.Events(); len(got) != 1 || got[0] != e {
		t.Errorf("Events() = %+v, want [%+v]", got, e)
	}
}
