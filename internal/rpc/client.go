package rpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/hithisisyeshwanth/RaftLock/internal/lock"
	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// clientRequest and clientReply are one Submit over POST /client.
type clientRequest struct {
	ClientID string
	Seq      uint64
	Cmd      lock.Command
}

type clientReply struct {
	Result    lock.Result
	Err       string
	NotLeader bool
	Hint      string // the leader's address, so a client that knows only addresses can follow it
}

func (r *Runtime) handleClient(w http.ResponseWriter, req *http.Request) {
	var in clientRequest
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	call, err := r.Submit(req.Context(), in.ClientID, in.Seq, in.Cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	out := clientReply{Result: call.Result(), NotLeader: errors.Is(call.Err(), lock.ErrNotLeader), Hint: r.peers[call.Hint()]}
	if call.Err() != nil {
		out.Err = call.Err().Error()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// ClientTransport is a lock.Transport over POST /client. Servers are named by address.
// Submit blocks until the server answers, which a single-request client like a CLI can afford.
type ClientTransport struct {
	http *http.Client
}

func NewClientTransport() *ClientTransport {
	// Longer than a server's own Submit timeout, so a live server answers before we give up.
	return &ClientTransport{http: &http.Client{Timeout: 3 * time.Second}}
}

func (t *ClientTransport) Submit(server raft.NodeID, clientID string, seq uint64, cmd lock.Command) (*lock.Call, error) {
	b, err := json.Marshal(clientRequest{ClientID: clientID, Seq: seq, Cmd: cmd})
	if err != nil {
		return nil, err
	}
	resp, err := t.http.Post("http://"+string(server)+"/client", "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", server, resp.Status)
	}
	var out clientReply
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	var callErr error
	switch {
	case out.NotLeader:
		callErr = lock.ErrNotLeader // the Clerk follows the hint only for this error
	case out.Err != "":
		callErr = errors.New(out.Err)
	}
	return lock.NewCall(out.Result, callErr, raft.NodeID(out.Hint)), nil
}
