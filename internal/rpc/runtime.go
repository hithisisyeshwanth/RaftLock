// Package rpc runs a lock server for real: HTTP/JSON between nodes, a 1 ms ticker, and one
// goroutine that owns the node. The Raft core stays goroutine-free; this is the only place with
// clocks, sockets and concurrency.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/hithisisyeshwanth/RaftLock/internal/lock"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

var ErrClosed = errors.New("runtime closed")

type Config struct {
	ID       raft.NodeID
	Peers    map[raft.NodeID]string // every other node's address, host:port
	Listener net.Listener           // where this node accepts messages; Close shuts it
	Storage  raft.Storage
	Raft     raft.Config
	Seed     int64 // seeds election timeouts; give each node a different one
}

// Runtime is one running node. Every method is safe to call from any goroutine: each becomes an
// event for the single loop goroutine, which is the only one that ever touches the node.
type Runtime struct {
	srv     *lock.Server
	events  chan func()
	done    chan struct{} // closed by Close
	stopped chan struct{} // closed when the loop has exited
	http    *http.Server
	out     map[raft.NodeID]chan raft.Message
	pending []pending // Submits waiting for their Call to finish; loop-owned
	once    sync.Once
	senders sync.WaitGroup
}

type pending struct {
	call   *lock.Call
	notify chan struct{} // closed by the loop once call is done
}

// sendTimeout bounds one message delivery; a lost message is fine, Raft retries.
const sendTimeout = 200 * time.Millisecond

// outboxSize is how many messages may wait for a slow peer before newer ones are dropped.
const outboxSize = 256

func Start(cfg Config) (*Runtime, error) {
	peers := make([]raft.NodeID, 0, len(cfg.Peers))
	for p := range cfg.Peers {
		peers = append(peers, p)
	}
	slices.Sort(peers)
	to := raft.NewSeededTimeouts(cfg.Seed, cfg.Raft.ElectionTimeoutMin, cfg.Raft.ElectionTimeoutMax)
	srv, err := lock.NewServer(cfg.Raft, cfg.ID, peers, cfg.Storage, nil, to, nil)
	if err != nil {
		return nil, err
	}
	r := &Runtime{
		srv:     srv,
		events:  make(chan func()),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
		out:     map[raft.NodeID]chan raft.Message{},
	}
	client := &http.Client{Timeout: sendTimeout}
	for _, p := range peers {
		ch := make(chan raft.Message, outboxSize)
		r.out[p] = ch
		r.senders.Add(1)
		go r.sendTo(client, cfg.Peers[p], ch)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft", r.handleRaft)
	r.http = &http.Server{Handler: mux}
	go r.http.Serve(cfg.Listener)
	go r.loop()
	return r, nil
}

// loop is the only goroutine that touches the node: it ticks every millisecond and runs events.
func (r *Runtime) loop() {
	defer close(r.stopped)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-tick.C:
			r.send(r.srv.Tick())
		case ev := <-r.events:
			ev()
		}
		kept := r.pending[:0]
		for _, p := range r.pending {
			if p.call.Done() {
				close(p.notify) // the loop never touches p.call again, so the caller may read it
			} else {
				kept = append(kept, p)
			}
		}
		r.pending = kept
	}
}

// do runs f on the loop goroutine, or reports false if the runtime is closed.
func (r *Runtime) do(f func()) bool {
	select {
	case r.events <- f:
		return true
	case <-r.done:
		return false
	}
}

// send queues messages for delivery after Step or Tick has returned, which in the core means
// after their state was saved. A full outbox drops, so a slow peer never stalls the loop.
func (r *Runtime) send(msgs []raft.Message) {
	for _, m := range msgs {
		select {
		case r.out[m.To] <- m:
		default:
		}
	}
}

func (r *Runtime) sendTo(client *http.Client, addr string, ch chan raft.Message) {
	defer r.senders.Done()
	for m := range ch {
		b, err := encode(m)
		if err != nil {
			continue
		}
		resp, err := client.Post("http://"+addr+"/raft", "application/json", bytes.NewReader(b))
		if err == nil {
			resp.Body.Close()
		}
	}
}

func (r *Runtime) handleRaft(w http.ResponseWriter, req *http.Request) {
	var env envelope
	if err := json.NewDecoder(req.Body).Decode(&env); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	m, err := decode(env)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !r.do(func() { r.send(r.srv.Step(m)) }) {
		http.Error(w, ErrClosed.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent) // messages are one-way; any reply is a message of its own
}

// Submit proposes cmd and waits until it is answered: applied, redirected, or failed.
// The error is only for a cancelled ctx or a closed runtime; the outcome is in the Call.
func (r *Runtime) Submit(ctx context.Context, clientID string, seq uint64, cmd lock.Command) (*lock.Call, error) {
	notify := make(chan struct{})
	var call *lock.Call
	if !r.do(func() {
		var msgs []raft.Message
		call, msgs = r.srv.Submit(clientID, seq, cmd)
		r.send(msgs)
		r.pending = append(r.pending, pending{call, notify})
	}) {
		return nil, ErrClosed
	}
	select {
	case <-notify:
		return call, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.done:
		return nil, ErrClosed
	}
}

func (r *Runtime) Snapshot() (raft.Snapshot, error) {
	ch := make(chan raft.Snapshot, 1)
	if !r.do(func() { ch <- r.srv.Snapshot() }) {
		return raft.Snapshot{}, ErrClosed
	}
	return <-ch, nil
}

func (r *Runtime) Dump() (string, error) {
	ch := make(chan string, 1)
	if !r.do(func() { ch <- r.srv.Dump() }) {
		return "", ErrClosed
	}
	return <-ch, nil
}

// Close stops the loop, the HTTP server and the senders. Calling it again does nothing.
func (r *Runtime) Close() error {
	var err error
	r.once.Do(func() {
		close(r.done)
		<-r.stopped
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err = r.http.Shutdown(ctx)
		for _, ch := range r.out {
			close(ch)
		}
		r.senders.Wait()
	})
	return err
}

// envelope carries one message between nodes; Type names the Go type of Body.
type envelope struct {
	From, To raft.NodeID
	Type     string
	Body     json.RawMessage
}

var decoders = map[string]func(json.RawMessage) (any, error){
	"RequestVoteArgs":    decodeAs[raft.RequestVoteArgs],
	"RequestVoteReply":   decodeAs[raft.RequestVoteReply],
	"AppendEntriesArgs":  decodeAs[raft.AppendEntriesArgs],
	"AppendEntriesReply": decodeAs[raft.AppendEntriesReply],
	"ReadIndexPing":      decodeAs[lock.ReadIndexPing],
	"ReadIndexPong":      decodeAs[lock.ReadIndexPong],
}

func decodeAs[T any](b json.RawMessage) (any, error) {
	var v T
	err := json.Unmarshal(b, &v)
	return v, err
}

func encode(m raft.Message) ([]byte, error) {
	typ := reflect.TypeOf(m.Body).Name()
	if decoders[typ] == nil {
		return nil, fmt.Errorf("encode: unknown message type %T", m.Body)
	}
	body, err := json.Marshal(m.Body)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope{From: m.From, To: m.To, Type: typ, Body: body})
}

func decode(env envelope) (raft.Message, error) {
	dec := decoders[env.Type]
	if dec == nil {
		return raft.Message{}, fmt.Errorf("decode: unknown message type %q", env.Type)
	}
	body, err := dec(env.Body)
	return raft.Message{From: env.From, To: env.To, Body: body}, err
}
