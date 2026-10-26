package raft

import (
	"fmt"
	"io"
)

type Event struct {
	Node   NodeID
	Term   Term
	Role   Role
	Kind   string
	Detail string
}

type Tracer interface {
	Trace(Event)
}

// trace lets the core run without a tracer: callers pass nil instead of checking.
func trace(t Tracer, e Event) {
	if t != nil {
		t.Trace(e)
	}
}

// Recorder keeps events in memory for tests. A nil *Recorder is a no-op.
type Recorder struct {
	events []Event
}

func NewRecorder() *Recorder {
	return &Recorder{}
}

func (r *Recorder) Trace(e Event) {
	if r != nil {
		r.events = append(r.events, e)
	}
}

func (r *Recorder) Events() []Event {
	if r == nil {
		return nil
	}
	return append([]Event(nil), r.events...)
}

// WriterTracer prints one line per event, for cmd/raftsim.
type WriterTracer struct {
	w io.Writer
}

func NewWriterTracer(w io.Writer) *WriterTracer {
	return &WriterTracer{w: w}
}

func (t *WriterTracer) Trace(e Event) {
	fmt.Fprintf(t.w, "%s term=%d %s %s %s\n", e.Node, e.Term, e.Role, e.Kind, e.Detail)
}
