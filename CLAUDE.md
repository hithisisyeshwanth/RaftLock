# RaftLock

A distributed lock service built on the Raft consensus algorithm. Clients acquire and release named locks; the Raft log makes lock state consistent and fault-tolerant across a cluster.

Reference: Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm" (extended version). When behavior is ambiguous, the paper's Figure 2 is the spec.

Module: `github.com/hithisisyeshwanth/RaftLock`. Go, standard library first.

## Workflow (follow this every session)
- Tasks live in `docs/spec.md`. Work on one task at a time and stay inside it.
- Step 1: write tests only. Run them, show they fail for the right reason, then stop and wait for me.
- Step 2 (only when I say so): implement the minimum needed to pass. Never edit tests unless I ask.
- Do not commit. I commit one task at a time as `Tx.y: title`.
- No new dependencies without asking (Porcupine only, in T7.2).
- If something looks missing from the spec, flag it. Do not add it.

## Architecture
1. Deterministic core. `internal/raft` has no goroutines, no `time.Now`, no global rand, no network. Time enters through `Tick()` (1 tick = 1 ms of logical time). Messages enter through `Step(msg)`. Outputs are returned as messages. Every test and chaos run is reproducible from a seed.
2. Persist before send. `Step` and `Tick` persist any changed durable state first, then return messages. The harness only sends after the call returns, so a reply never leaves before its state is on disk (Figure 2, persistent state). If persisting fails, return no messages.
3. One-way async messages. Replies are separate messages handled explicitly. Replies carry the term, so stale, duplicated, and reordered replies are gated by role and term.
4. 1-based indexes, `log[0]` is a sentinel with term 0. Log length equals last index.
5. Fixed three-node cluster for the system. The test harness accepts N nodes (Figure 8 needs five).
6. The lock state machine has no clock. It receives `(index, command)`. The fencing token is the log index of the acquire entry.
7. Runtime wrapper comes last (Phase 7). One goroutine in `internal/rpc` owns the node; the core itself stays goroutine-free.
8. Simple persistence. Each `Save` rewrites the whole durable state atomically. Optimize later if ever.

## Node API and messages
```go
type RequestVoteArgs struct { Term Term; CandidateID NodeID; LastLogIndex Index; LastLogTerm Term }
type RequestVoteReply struct { Term Term; VoteGranted bool }
type AppendEntriesArgs struct { Term Term; LeaderID NodeID; PrevLogIndex Index; PrevLogTerm Term; Entries []Entry; LeaderCommit Index }
// PrevLogIndex echoes the request. MatchIndex = prev + len(entries) on success.
type AppendEntriesReply struct { Term Term; Success bool; MatchIndex Index; PrevLogIndex Index }

func (n *Node) Step(m Message) []Message
func (n *Node) Tick() []Message
func (n *Node) Propose(cmd []byte) (ProposeResult, []Message) // ProposeResult{Index, Term, IsLeader, LeaderHint}
```

## Repo layout
```
raftlock/
  CLAUDE.md
  Makefile
  docs/spec.md                 task list, tests, run steps, prompts
  internal/raft/               core node, log, storage interface, messages
  internal/storage/            FileStorage
  internal/netsim/             fake network
  internal/check/              Figure 3 safety checkers
  internal/sim/                cluster harness, chaos tests
  internal/lock/               lock state machine, server, clerk
  internal/rpc/                real HTTP/JSON transport and runtime loop
  cmd/raftsim/                 prints a trace of a simulated cluster
  cmd/raftlockd/               real node binary
  cmd/raftlockctl/             CLI client
```

## Commands
```
make test         # go test ./... -race -count=1
make vet          # go vet ./...
gofmt -l .        # must print nothing
go build ./...
```
Local cluster and failure drill: see T7.4 and T7.5 in `docs/spec.md`.

## Raft design rules
- Follow Figure 2 exactly: persist `currentTerm`, `votedFor`, and `log[]` before responding to any RPC.
- Adopt any higher term seen in any RPC or reply, then step down to follower.
- Only apply committed entries to the lock state machine, in log order, exactly once.
- A leader commits an entry only if it is on a majority AND its term equals the leader's current term (section 5.4.2, Figure 8). New leaders append a no-op.
- Follower `commitIndex = min(leaderCommit, index of last new entry)`. Never the log length.
- Truncate a follower's log only on a term conflict, never on a plain overlap.

## Lock service rules
- Deduplicate by `(clientID, seq)` at apply time, inside the state machine. The dedup table is part of the state machine state.
- Lock expiry is a replicated `Expire{lock, token}` entry proposed by the leader. The state machine never reads a clock, and never uses a node's local wall clock.
- Reads go through the log first (T5.7). ReadIndex (T5.8) is an optimization in the lock server layer, not in the core. Leases are out of scope.
- Membership changes are out of scope. Snapshots (T6.4, T6.5) are a stretch goal.

## Testing
- Unit-test the core with the fake network and a controllable clock: partitions, drop, reorder, duplicate, crash and restart.
- The Figure 3 checkers in `internal/check` run after every harness step: Election Safety, Leader Append-Only, Log Matching, Leader Completeness, State Machine Safety.
- Test lock semantics (mutual exclusion, expiry, fencing) separately from consensus.
- Always run tests with `-race`.

## Conventions
- Name things after the paper (`currentTerm`, `commitIndex`, `lastApplied`, `nextIndex`, `matchIndex`). Unexported fields stay lowerCamel.
- Idiomatic Go: small interfaces defined where they are used, errors wrapped with `%w`, table-driven tests.
- Comment only the why. Cite the Figure 2 rule or paper section for non-obvious safety rules.
- Keep changes small and each one verifiable by a test.

## Slide to paper vocabulary
- sentLength[f] = nextIndex[f] - 1
- ackedLength[f] = matchIndex[f]
- commitLength = commitIndex
- delivered length = lastApplied
- replicate-log message and its ack = AppendEntries args and reply
- votesReceived = the votes set; currentRole = role; currentLeader = leaderID
