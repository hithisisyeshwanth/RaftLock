# RaftLock Spec: Tasks, Tests, Run Steps, and Claude Code Prompts

Built from the Raft paper (Figure 2, sections 5.1 to 5.6 and 8, section 7 for the stretch goal), Kleppmann's Raft lecture slides, and our hand traces (test-case doc, Section J). Every task is one small, reviewable change. Tests are always written before the logic.

## 1. How to use this spec

The loop for every task:

1. Paste the task's test prompt into Claude Code. It writes tests only, runs them, shows them failing for the right reason, and stops.
2. You read the tests. They are the spec. Fix them now, not later.
3. Paste the implement prompt (standard prompt P2 plus the task's Implement note).
4. Run the task's Run command. It must be green with -race.
5. Review the diff (P3). Commit as `Tx.y: title`. Move to the next task.

A task is done only when: its tests pass with -race, go vet is clean, the diff touches nothing outside the task, and you can explain every line. The prompts are reference. Adapt or ignore them freely.

## 2. Architecture decisions (these become CLAUDE.md rules)

1. **Deterministic core.** `internal/raft` has no goroutines, no time.Now, no global rand, no network. Time enters through `Tick()` (1 tick = 1 ms of logical time). Messages enter through `Step(msg)`. Outputs are returned as messages. Every test and chaos run is reproducible from a seed.
2. **Persist before send.** Step and Tick persist any changed durable state first, then return messages. The harness only sends after the call returns, so a reply can never leave before its state is on disk (Figure 2, persistent state). If persisting fails, return no messages.
3. **One-way async messages.** Replies are separate messages handled explicitly, as on slides 5 to 8. Replies carry the term, so stale, duplicated, and reordered replies are gated.
4. **1-based indexes, log[0] is a sentinel with term 0.** Log length equals last index, so slide 'length' fields equal paper 'index' fields.
5. **Fixed three-node cluster for the system.** The test harness accepts N nodes, which Figure 8 needs (five servers).
6. **The state machine has no clock.** It receives (index, command). The fencing token is the log index of the acquire entry.
7. **Runtime wrapper last.** One goroutine owns the node. Real network and real timers arrive only in Phase 7.
8. **Simple persistence.** Each Save rewrites the whole durable state atomically. Optimize later if ever.

### Slide to paper vocabulary (code uses the paper names)

- sentLength[f] = nextIndex[f] - 1
- ackedLength[f] = matchIndex[f]
- commitLength = commitIndex
- delivered length = lastApplied
- replicate-log message and its ack = AppendEntries args and reply
- votesReceived = the votes set; currentRole = role; currentLeader = leaderID

### Message shapes and node API

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

### Repo layout

```
raftlock/
  CLAUDE.md
  Makefile
  docs/spec.md                 (this document)
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

### Standard prompts

**P0 (once per Claude Code session):**

```
Read CLAUDE.md and docs/spec.md. We work one task at a time. For each task: (1) write tests only, (2) run them and show they fail for the right reason, (3) stop and wait for me. Never implement before I say so. Never edit tests during implementation unless I ask.
```

**P2 (implement, paste after reviewing the tests):**

```
I reviewed the tests for <Tx.y>. Implement the minimum needed to pass them, following the Implement note: <paste note>. Do not modify the tests. Run `make test` and `go vet ./...`, show the output, then stop.
```

**P3 (review):**

```
Show git diff --stat and git diff. Flag anything outside task <Tx.y>, any goroutine, time.Now, or global rand inside internal/raft, and any exported API not listed in docs/spec.md.
```

**P4 (a test fails and you do not know why):**

```
Test <name> fails. Do not change the tests. First explain the root cause and which Figure 2 rule or slide step it violates. Then propose the smallest fix and wait for me.
```

## Phase 0: Setup and building blocks

### T0.1 Repo skeleton, Makefile, CLAUDE.md

**Test first:** `internal/raft/smoke_test.go` with TestSmoke. `make test` and `make vet` must exist and run.

**Implement:** go mod init, the directories above, Makefile (`test` = `go test ./... -race -count=1`, `vet` = `go vet ./...`), and CLAUDE.md containing the section 2 decisions as rules. Copy this spec into docs/spec.md first.

**Run:** `make test`

**Prompt (tests):**

```
Read docs/spec.md sections 1 and 2. Create the repo skeleton and Makefile from the spec, plus a trivial smoke test. Write CLAUDE.md turning the architecture decisions into rules. Show me the files, then stop.
```

### T0.2 Core types

**Test first** (`types_test.go`): Role.String returns Follower, Candidate, Leader. Entry equality compares term, index, and command bytes. Zero values: Term 0, Index 0, empty NodeID.

**Implement:** `Term uint64`, `Index uint64`, `NodeID string`, `Role`, `Entry{Term, Index, Cmd}`, `Message{From, To, Body}`, and the four message structs from section 2.

**Run:** `go test ./internal/raft -run TestTypes -race -count=1`

**Prompt (tests):**

```
Task T0.2 in docs/spec.md. Write tests only for the core types: Role.String, Entry equality, zero values. Run them, show failures, stop.
```

### T0.3 In-memory log

**Test first** (`log_test.go`): empty log has LastIndex 0 and LastTerm 0. Append assigns consecutive indexes. Term(0) is 0. Term beyond the end returns ok=false. TruncateFrom(i) removes i and everything after. Slice(from, to) returns a copy, so mutating it does not change the log.

**Implement:** `Log` with sentinel at position 0 and methods LastIndex, LastTerm, Term(i), Get(i), Append, TruncateFrom, Slice.

**Run:** `go test ./internal/raft -run TestLog -race -count=1`

**Prompt (tests):**

```
Task T0.3. Write table-driven tests only for the 1-based Log with a sentinel entry at index 0, covering the cases listed in the spec. Run, show failures, stop.
```

### T0.4 Storage interface and MemStorage

**Test first** (`storage_test.go`): load from empty storage returns the zero state. Save then Load round-trips term, votedFor, and log. Save deep-copies, so later mutation of the caller's slice does not change what was saved. MemStorage counts Save calls and records call order (used by spy tests later).

**Implement:** `PersistentState{CurrentTerm, VotedFor, Log}`, `Storage{Save, Load}`, and MemStorage.

**Run:** `go test ./internal/raft -run TestMemStorage -race -count=1`

**Prompt (tests):**

```
Task T0.4. Write tests only for the Storage interface and an in-memory implementation that deep-copies and records Save calls. Run, show failures, stop.
```

### T0.5 FileStorage with fsync

**Test first** (`internal/storage/file_test.go`, use t.TempDir): round trip. Overwrite keeps only the latest state. A truncated file returns an error (CRC32 checksum). A leftover temp file from a crashed Save is ignored on Load. Load from a missing file returns the zero state.

**Implement:** write to a temp file, fsync the file, rename over the real file, fsync the directory. Checksum the payload.

**Run:** `go test ./internal/storage -race -count=1`

**Prompt (tests):**

```
Task T0.5. Write tests only for a crash-safe FileStorage (temp file, fsync, atomic rename, directory fsync, CRC32). Include corrupt file, truncated file, and leftover temp file cases. Run, show failures, stop.
```

### T0.6 Tracer

**Test first** (`trace_test.go`): the recorder keeps events in order with node id, term, role, kind, and detail. A nil tracer is safe to call.

**Implement:** `Tracer` interface, a recording implementation, and a writer-backed one used by cmd/raftsim.

**Run:** `go test ./internal/raft -run TestTracer -race -count=1`

**Prompt (tests):**

```
Task T0.6. Write tests only for a Tracer interface with an in-memory recorder, including safe nil handling. Run, show failures, stop.
```

### T0.7 Config and timeout source

**Test first** (`config_test.go`): defaults are election timeout 150 to 300 ticks and heartbeat 50 ticks. Validate rejects min >= max and heartbeat >= min. A seeded timeout source is reproducible, always within range, and varies across calls. A fixed timeout source always returns the same value (used to force split votes).

**Implement:** `Config`, `Validate`, `TimeoutSource` interface, seeded and fixed implementations.

**Run:** `go test ./internal/raft -run TestConfig -race -count=1`

**Prompt (tests):**

```
Task T0.7. Write tests only for Config validation and two TimeoutSource implementations (seeded random, fixed). Run, show failures, stop.
```

### T0.8 Fake network

**Test first** (`internal/netsim/net_test.go`): a message arrives after its delay in ticks. A partition blocks both directions, and healing restores delivery. Drop rate 100 percent drops all. Duplicate delivers the same message twice. Random delays can reorder messages. The same seed produces the same schedule. Messages to unknown nodes are dropped.

**Implement:** `Network` with Send, Advance(now) returning due messages, SetDrop, SetDelay, SetDuplicate, Partition(groups), Heal.

**Run:** `go test ./internal/netsim -race -count=1`

**Prompt (tests):**

```
Task T0.8. Write tests only for a deterministic fake network with delay, drop, duplicate, reorder, and partition controls, all driven by a seed and a tick counter. Run, show failures, stop.
```

## Phase 1: Safety checkers (built early, used by every multi-node test)

The checkers take node snapshots (id, term, role, log, commitIndex, applied entries) and report violations. They are tested with hand-built bad histories, so you know they can fail.

### T1.1 Election Safety checker

**Test first** (`internal/check/election_test.go`): the same leader seen again in a term is fine. Two different leaders in the same term is a violation. Different leaders in different terms is fine. Only nodes with role Leader count.

**Implement:** stateful `ElectionSafety.Observe(snapshots) error`.

**Run:** `go test ./internal/check -run TestElectionSafety -race -count=1`

**Prompt (tests):**

```
Task T1.1. Write tests only for an Election Safety checker (at most one leader per term), including a hand-built violating history. Run, show failures, stop.
```

### T1.2 Log Matching checker

**Test first:** matching logs pass. Same index and same term but different command is a violation. Same term at index i but different entries before i is a violation. Logs of different length with a matching prefix pass.

**Implement:** pairwise comparison of node logs.

**Run:** `go test ./internal/check -run TestLogMatching -race -count=1`

**Prompt (tests):**

```
Task T1.2. Write tests only for a Log Matching checker (paper Figure 3). Include violating pairs. Run, show failures, stop.
```

### T1.3 State Machine Safety checker

**Test first:** all nodes applying the same entry at an index passes. Different entries at the same index is a violation. A node applying indexes out of order, skipping one, or applying one twice is a violation.

**Implement:** track applied entries per node and per index.

**Run:** `go test ./internal/check -run TestStateMachineSafety -race -count=1`

**Prompt (tests):**

```
Task T1.3. Write tests only for a State Machine Safety checker, including gap, duplicate, and divergence cases. Run, show failures, stop.
```

### T1.4 Leader Append-Only and Leader Completeness checkers

**Test first:** a leader's log in the same term must only grow, with the old log as a prefix (shrinking or overwriting is a violation). An entry committed on any node must appear in every leader log of a higher term (a missing committed entry is a violation).

**Implement:** two stateful checkers, plus `check.All` that runs all five.

**Run:** `go test ./internal/check -race -count=1`

**Prompt (tests):**

```
Task T1.4. Write tests only for Leader Append-Only and Leader Completeness checkers, plus a check.All aggregator. Include violating histories. Run, show failures, stop.
```

## Phase 2: Election

### T2.1 Node struct and restart behavior

**Test first** (`node_test.go`): a fresh node is a follower with term 0, empty log, commitIndex 0, lastApplied 0. A node built over storage that holds term, votedFor, and log restores them, and commitIndex and lastApplied are still 0 (volatile). `Snapshot()` returns copies.

**Implement:** `NewNode(cfg, id, peers, storage, tracer, timeouts, onApply)`, loading persisted state, and `Snapshot()`.

**Run:** `go test ./internal/raft -run TestNewNode -race -count=1`

**Prompt (tests):**

```
Task T2.1. Write tests only for NewNode: fresh state, restart from stored state, volatile state reset, Snapshot returning copies. Run, show failures, stop.
```

### T2.2 Term adoption helper

**Test first:** a higher term sets currentTerm, clears votedFor, makes any role Follower, persists, and returns true. An equal or lower term changes nothing, does not persist, and returns false.

**Implement:** `adoptTermIfHigher(t) bool` (Figure 2 rules for servers, all RPCs and replies).

**Run:** `go test ./internal/raft -run TestAdoptTerm -race -count=1`

**Prompt (tests):**

```
Task T2.2. Write tests only for the shared term-adoption helper: higher, equal, lower term; leader steps down; votedFor cleared; persisted only on change. Run, show failures, stop.
```

### T2.3 RequestVote: term check

**Test first** (`requestvote_test.go`, empty logs): a lower term is rejected, and the reply carries the current term. A higher term is adopted first, and the reply carries the new term.

**Implement:** the RequestVote branch of Step, term logic only.

**Run:** `go test ./internal/raft -run TestRequestVote_Term -race -count=1`

**Prompt (tests):**

```
Task T2.3. Write tests only for the term check of the RequestVote handler (Figure 2, RequestVote rule 1). Run, show failures, stop.
```

### T2.4 RequestVote: votedFor

**Test first:** grant when votedFor is empty. Deny when already voted for another candidate this term. Re-grant to the same candidate (duplicate request). A new term resets the vote. The vote is persisted.

**Implement:** votedFor logic in the handler.

**Run:** `go test ./internal/raft -run TestRequestVote_VotedFor -race -count=1`

**Prompt (tests):**

```
Task T2.4. Write tests only for the one-vote-per-term rule, including duplicate requests and new-term reset. Run, show failures, stop.
```

### T2.5 RequestVote: log up-to-date check

**Test first** (table): candidate last term higher grants. Same last term, candidate log longer or equal grants. Same last term, candidate shorter denies. Candidate last term lower denies even if its log is longer. A denied vote does not set votedFor.

**Implement:** `candidateUpToDate(lastIdx, lastTerm)` (paper section 5.4.1).

**Run:** `go test ./internal/raft -run TestRequestVote_LogCheck -race -count=1`

**Prompt (tests):**

```
Task T2.5. Write a table-driven test only for the up-to-date comparison (last term first, then length) in RequestVote. Run, show failures, stop.
```

### T2.6 Persist before reply

**Test first** (spy storage): when a vote is granted, Save was called with the new votedFor before Step returned. If Save returns an error, Step returns no messages and the vote is not granted. Same check for a term adoption.

**Implement:** order operations so the handler persists, then builds the reply. Return nothing on persist error.

**Run:** `go test ./internal/raft -run TestPersistBeforeReply -race -count=1`

**Prompt (tests):**

```
Task T2.6. Write tests only using a spy Storage that proves durable state is saved before any reply is returned, and that a Save error yields no reply. Run, show failures, stop.
```

### T2.7 Election timer mechanics

**Test first:** the timer resets when a vote is granted. It does not reset when a vote is denied. A follower that sees no reset for its timeout reports expiry. A leader has no election timer.

**Implement:** electionDeadline, `resetElectionTimer()` using the TimeoutSource, and expiry check inside Tick.

**Run:** `go test ./internal/raft -run TestElectionTimer -race -count=1`

**Prompt (tests):**

```
Task T2.7. Write tests only for election timer reset rules (reset on granting a vote, not on denying) and expiry detection in Tick. Run, show failures, stop.
```

### T2.8 Start election

**Test first:** on expiry a follower becomes Candidate, increments the term, votes for itself, persists, resets the timer, and emits one RequestVote per peer with correct lastLogIndex and lastLogTerm. A second timeout as Candidate increments the term again.

**Implement:** `startElection()` (paper section 5.2).

**Run:** `go test ./internal/raft -run TestStartElection -race -count=1`

**Prompt (tests):**

```
Task T2.8. Write tests only for starting an election: term++, vote self, persist, reset timer, RequestVote to every peer, retry with a new term. Run, show failures, stop.
```

### T2.9 Vote collection and becoming leader

**Test first:** a stale reply from an earlier term is ignored. A duplicate grant from one node counts once. A denial does not count. A reply with a higher term makes the node step down. Self plus one grant out of three wins. A reply arriving at a follower or leader is ignored.

**Implement:** RequestVoteReply handler with role and term gates and a set-based majority (slide 3).

**Run:** `go test ./internal/raft -run TestVoteCollection -race -count=1`

**Prompt (tests):**

```
Task T2.9. Write tests only for handling RequestVote replies: role gate, term gate, set-based majority, duplicates, step-down on higher term. Run, show failures, stop.
```

### T2.10 AppendEntries term handling (heartbeat only)

**Test first:** a lower-term AppendEntries is rejected with the current term and does not reset the election timer. An equal or higher term records the leader, resets the timer, and makes a Candidate step down. A higher term is adopted. No log logic yet (empty entries only).

**Implement:** the term and role part of the AppendEntries handler (slide 6, Figure 2 AppendEntries rule 1).

**Run:** `go test ./internal/raft -run TestAppendEntries_Term -race -count=1`

**Prompt (tests):**

```
Task T2.10. Write tests only for the term and role handling of AppendEntries (heartbeats): reject stale, adopt higher, candidate steps down, timer reset only for a valid leader. Run, show failures, stop.
```

### T2.11 Become leader and heartbeats

**Test first:** becoming leader sets nextIndex to lastIndex+1 and matchIndex to 0 for every peer, and sends an immediate AppendEntries to all peers. Heartbeats repeat every heartbeat interval. A leader never starts an election. A reply with a higher term makes the leader step down and stop heartbeating.

**Implement:** `becomeLeader()`, heartbeat timer in Tick, minimal AppendEntriesReply term check.

**Run:** `go test ./internal/raft -run TestBecomeLeader -race -count=1`

**Prompt (tests):**

```
Task T2.11. Write tests only for becoming leader: nextIndex/matchIndex init, immediate heartbeat, periodic heartbeats, step-down on higher-term reply. Run, show failures, stop.
```

### T2.12 Cluster harness

**Test first** (`internal/sim/cluster_test.go`): 100 ticks run without panic. A crashed node receives nothing. Restart rebuilds the node from its storage. A deliberately injected bad snapshot makes the harness fail the test through the checkers.

**Implement:** `Cluster` with Step (tick all nodes in id order, deliver due messages, send outputs into the network, run `check.All`), RunUntil(cond, maxTicks), Leader(), Crash, Restart, Partition, Heal, Propose. Accepts N nodes.

**Run:** `go test ./internal/sim -run TestCluster -race -count=1`

**Prompt (tests):**

```
Task T2.12. Write tests only for a deterministic cluster harness that wires nodes, fake network, and safety checkers together. Include crash, restart, and an injected violation. Run, show failures, stop.
```

### T2.13 Election integration tests

**Test first** (`election_integration_test.go`): for seeds 1 to 50, three nodes elect exactly one leader within 1000 ticks. The leader stays stable (term unchanged) for 2000 ticks. Killing the leader elects a new one in a higher term. A restarted old leader becomes a follower. Two nodes with a fixed identical timeout split the vote, then random timeouts resolve it. An isolated node's term keeps rising, and after healing it forces a re-election (expected without PreVote, so the test documents it).

**Implement:** only fixes to the core that the tests expose.

**Run:** `go test ./internal/sim -run TestElection -race -count=1`

**Prompt (tests):**

```
Task T2.13. Write tests only for election integration scenarios using the cluster harness, over seeds 1 to 50, as listed in the spec. Run, show failures, stop.
```

### T2.14 Simulation trace CLI (first thing you can watch running)

**Test first** (`cmd/raftsim/main_test.go`): `Run(cfg, w)` with the same seed twice produces identical output. Output contains a line when a leader is elected and a line when the leader is killed.

**Implement:** `cmd/raftsim` with flags -nodes, -seed, -ticks, -kill-leader-at, printing tracer events.

**Run:** `go test ./cmd/raftsim -race -count=1`, then by hand: `go run ./cmd/raftsim -nodes 3 -seed 1 -ticks 600 -kill-leader-at 300`. You should see an election, steady heartbeats, the kill, and a new election in a higher term.

**Prompt (tests):**

```
Task T2.14. Write tests only for a raftsim command whose Run function is deterministic per seed and prints election events. Run, show failures, stop.
```

## Phase 3: Replication

### T3.1 Propose

**Test first:** on a leader, Propose appends an entry stamped with the current term at lastIndex+1, persists it, sets matchIndex for itself, and returns IsLeader true. On a follower it returns IsLeader false with the known leader as hint. On a candidate it returns false with no hint.

**Implement:** `Propose` (slide 4).

**Run:** `go test ./internal/raft -run TestPropose -race -count=1`

**Prompt (tests):**

```
Task T3.1. Write tests only for Propose on leader, follower with hint, and candidate. Run, show failures, stop.
```

### T3.2 Build AppendEntries for a peer

**Test first** (pure function): nextIndex = last+1 gives an empty-entries heartbeat. nextIndex = 1 gives prev index 0, prev term 0, and all entries. A middle nextIndex gives the correct prevLogTerm and tail. Entries are copied. LeaderCommit is the leader's commitIndex.

**Implement:** `buildAppendEntries(peer)` (slide 5, Figure 2 leader rules).

**Run:** `go test ./internal/raft -run TestBuildAppendEntries -race -count=1`

**Prompt (tests):**

```
Task T3.2. Write tests only for building AppendEntries args for a peer from nextIndex. Run, show failures, stop.
```

### T3.3 Follower log consistency check

**Test first** (table, pure function): prevIndex 0 is always OK. prevIndex beyond the log end fails (length first). A term mismatch at prevIndex fails. A match passes.

**Implement:** `logOK(log, prevIndex, prevTerm)` (slide 6, Figure 2 AppendEntries rule 2).

**Run:** `go test ./internal/raft -run TestLogOK -race -count=1`

**Prompt (tests):**

```
Task T3.3. Write a table-driven test only for the follower consistency check: length first, then term at prevIndex. Run, show failures, stop.
```

### T3.4 Merge entries (truncate only on conflict)

**Test first** (pure function): exact duplicate entries change nothing (idempotent). A delayed old AppendEntries carrying a shorter prefix must NOT truncate later entries (the key test). A conflicting term at some index truncates from there and appends the rest. Empty entries are a no-op. New entries beyond the end are appended.

**Implement:** `mergeEntries(log, prevIndex, entries)` returning the new log and whether it changed (Figure 2 AppendEntries rules 3 and 4, slide 7).

**Run:** `go test ./internal/raft -run TestMergeEntries -race -count=1`

**Prompt (tests):**

```
Task T3.4. Write tests only for mergeEntries: idempotent duplicates, stale shorter message must not truncate, conflict truncates, new tail appended. Run, show failures, stop.
```

### T3.5 Full AppendEntries handler

**Test first:** a stale term is rejected. A failed consistency check replies Success false, even for a heartbeat. A success appends via merge, persists before replying, and replies with MatchIndex = prev + len(entries) and the echoed PrevLogIndex.

**Implement:** wire T2.10, T3.3, and T3.4 into the handler.

**Run:** `go test ./internal/raft -run TestAppendEntries -race -count=1`

**Prompt (tests):**

```
Task T3.5. Write tests only for the complete AppendEntries handler: stale term, failed check, successful merge, persist before reply, reply fields. Run, show failures, stop.
```

### T3.6 Leader handles success replies

**Test first:** a success reply sets matchIndex to the reply's MatchIndex and nextIndex to matchIndex+1. matchIndex never decreases (old, duplicated, or reordered replies). A reply from a node that is not a peer, or arriving at a non-leader, is ignored. A reply with a higher term steps the leader down.

**Implement:** the success branch of the AppendEntriesReply handler (slide 8).

**Run:** `go test ./internal/raft -run TestLeaderReply_Success -race -count=1`

**Prompt (tests):**

```
Task T3.6. Write tests only for the leader's handling of successful AppendEntries replies, including monotonic matchIndex and stale or reordered replies. Run, show failures, stop.
```

### T3.7 Leader backoff on failure

**Test first:** a failure reply decrements nextIndex by one and triggers an immediate resend. nextIndex never goes below 1. A failure whose echoed PrevLogIndex is not nextIndex-1 is stale and ignored. After backing off, a success advances normally.

**Implement:** the failure branch (slide 8: decrement sentLength, retry).

**Run:** `go test ./internal/raft -run TestLeaderReply_Backoff -race -count=1`

**Prompt (tests):**

```
Task T3.7. Write tests only for leader backoff on failed AppendEntries: step back one, floor at 1, ignore stale failure via the echoed PrevLogIndex, recover on success. Run, show failures, stop.
```

### T3.8 Heartbeat doubles as retry

**Test first** (harness): on each heartbeat the leader sends every peer an AppendEntries from its current nextIndex. A duplicated AppendEntries leaves the follower's log unchanged. A dropped message is repaired by the next heartbeat. With 30 percent drops, proposals on the leader reach all three logs, and the Log Matching checker never fires.

**Implement:** only fixes the tests expose.

**Run:** `go test ./internal/sim -run TestReplication -race -count=1`

**Prompt (tests):**

```
Task T3.8. Write tests only, using the cluster harness, for replication under dropped, duplicated, and reordered messages. Check that logs converge and checkers stay quiet. Run, show failures, stop.
```

### T3.9 Section J scenario as an integration test

**Test first:** build the initial persisted logs of nodes A (leader), B (lagging), and C (conflicting) from Section J of the test-case doc. Run the cluster. Assert B catches up, C's conflicting entries are truncated, and all three logs end identical. Assert the exact backoff sequence from the hand trace.

**Implement:** only fixes the test exposes.

**Run:** `go test ./internal/sim -run TestSectionJ -race -count=1`

**Prompt (tests):**

```
Task T3.9. Turn the hand trace below into one deterministic integration test using the cluster harness and crafted initial storage. Assert the final logs and the backoff sequence. Tests only, run, show failures, stop.

[paste Section J initial state and expected steps here]
```

### T3.10 (optional) Conflict hint for faster backoff

**Test first:** the failure reply carries ConflictIndex and ConflictTerm. The leader jumps nextIndex using them. Fewer round trips than one-by-one in a long-divergence case.

**Implement:** paper section 5.3 optimization. Skip unless the long-divergence test bothers you.

**Run:** `go test ./internal/raft ./internal/sim -run TestConflictHint -race -count=1`

**Prompt (tests):**

```
Task T3.10. Write tests only for the conflict-hint optimization: reply fields, leader jump, and fewer round trips in a long-divergence scenario. Run, show failures, stop.
```

## Phase 4: Commit and apply

### T4.1 Majority match calculation

**Test first** (pure function): for three nodes [5,5,0] gives 5, [5,3,0] gives 3, [0,0,0] gives 0. For five nodes [9,7,7,2,0] gives 7. The leader's own value is included by the caller.

**Implement:** `majorityMatch(matchIndexes)` = sort descending, take position quorum-1.

**Run:** `go test ./internal/raft -run TestMajorityMatch -race -count=1`

**Prompt (tests):**

```
Task T4.1. Write a table-driven test only for the largest index replicated on a majority, for 3 and 5 nodes. Run, show failures, stop.
```

### T4.2 Current-term-only commit rule

**Test first** (pure function, three nodes, currentTerm 4): log terms [1,2], matchIndex [2,2,0] gives no commit because entry 2 is from term 2 (Figure 8). After appending index 3 with term 4 and a majority at 3, commit moves to 3, which also commits index 2 indirectly. The result never decreases.

**Implement:** `nextCommitIndex(...)` (slide 9, Figure 2 leader rule 4, paper section 5.4.2).

**Run:** `go test ./internal/raft -run TestNextCommitIndex -race -count=1`

**Prompt (tests):**

```
Task T4.2. Write tests only for the commit rule: majority AND log[N].term == currentTerm, monotonic, with the Figure 8 situation as a case. Run, show failures, stop.
```

### T4.3 Leader advances commitIndex

**Test first:** after a success reply that creates a majority, the leader's commitIndex advances and the next AppendEntries carries the new LeaderCommit. commitIndex is not persisted.

**Implement:** call `nextCommitIndex` from the success branch.

**Run:** `go test ./internal/raft -run TestLeaderCommit -race -count=1`

**Prompt (tests):**

```
Task T4.3. Write tests only for leader commit advancement through replies and propagation via LeaderCommit. Run, show failures, stop.
```

### T4.4 Follower commit index

**Test first:** a successful AppendEntries with higher leaderCommit sets commitIndex to min(leaderCommit, index of last new entry). Key case: follower log length 5 with stale entries at 4 and 5, heartbeat with prev 3, no entries, leaderCommit 10, gives commitIndex 3 and not 5. Never decreases. A failed AppendEntries never moves commitIndex.

**Implement:** the commit step in the handler (slide 7, Figure 2 AppendEntries rule 5).

**Run:** `go test ./internal/raft -run TestFollowerCommit -race -count=1`

**Prompt (tests):**

```
Task T4.4. Write tests only for follower commitIndex: min with the last NEW entry (not log length), monotonic, unchanged on failure. Include the stale-tail heartbeat case. Run, show failures, stop.
```

### T4.5 Apply loop

**Test first:** committed entries are passed to onApply in order, exactly once, even if the apply routine runs repeatedly. lastApplied never exceeds commitIndex. After a restart, lastApplied is 0 and entries are applied again from the start. The callback runs after state updates and must not call back into the node.

**Implement:** `applyCommitted()` called whenever commitIndex changes.

**Run:** `go test ./internal/raft -run TestApply -race -count=1`

**Prompt (tests):**

```
Task T4.5. Write tests only for the apply loop: ordering, exactly-once, bounded by commitIndex, replay after restart. Run, show failures, stop.
```

### T4.6 Leader no-op on election

**Test first:** a new leader appends a no-op entry in its own term. Committing the no-op commits earlier entries from older terms. The no-op is never delivered to the state machine as a command. A test-only config flag `DisableNoop` turns it off (needed for T4.7).

**Implement:** no-op in `becomeLeader` (paper section 8), flagged entries skipped by the apply loop.

**Run:** `go test ./internal/raft -run TestNoop -race -count=1`

**Prompt (tests):**

```
Task T4.6. Write tests only for the leader no-op: appended on election, commits older entries, skipped at apply, disable flag for tests. Run, show failures, stop.
```

### T4.7 Figure 8 scenario (five nodes)

**Test first** (harness with five nodes, no-op disabled): reproduce stages (a) to (e). In stage (c) the term-2 entry is on a majority but commitIndex must stay below it. Then crash S1, let S5 win with the entry that overwrites it, and assert the committed prefix is never lost. Repeat with no-op enabled and assert the old entry commits only through the new-term no-op.

**Implement:** only fixes the test exposes.

**Run:** `go test ./internal/sim -run TestFigure8 -race -count=1`

**Prompt (tests):**

```
Task T4.7. Write one deterministic five-node integration test that reproduces paper Figure 8 stages (a) to (e) using crafted storage, partitions, and crashes. Assert the old-term entry is not committed by counting. Tests only, run, show failures, stop.
```

### T4.8 End-to-end commit under faults

**Test first:** propose a stream of commands on a three-node cluster with drops and one leader crash. All nodes eventually apply the same sequence. Every command acknowledged as committed survives the leader change. Checkers stay quiet.

**Implement:** only fixes the test exposes.

**Run:** `go test ./internal/sim -run TestCommitEndToEnd -race -count=1`

**Prompt (tests):**

```
Task T4.8. Write tests only for end-to-end commit and apply on three nodes under message drops and a leader crash, with all checkers on. Run, show failures, stop.
```

## Phase 5: Lock service on the log

### T5.1 State machine interface and command encoding

**Test first** (`internal/lock/command_test.go`): Acquire, Release, and Expire commands round-trip through encoding. An unknown command type yields an error result and no panic. The interface is `Apply(index, cmd) Result`, with no time parameter.

**Implement:** `StateMachine` interface, command types (JSON is fine), result types.

**Run:** `go test ./internal/lock -run TestCommand -race -count=1`

**Prompt (tests):**

```
Task T5.1. Write tests only for lock command encoding and a StateMachine interface Apply(index, cmd) Result that has no clock. Run, show failures, stop.
```

### T5.2 Acquire and release semantics

**Test first:** acquiring a free lock grants it with token = log index. Acquiring a lock held by another client is denied and names the holder (no wait queue). Acquiring a lock you already hold returns the same token. Release by the holder frees it. Release by a non-holder, or with a wrong token, is rejected. Double release is rejected. Tokens strictly increase across grants. Applying the same command sequence to two fresh state machines gives identical dumps.

**Implement:** `LockSM` with a lock table and a `Dump()` for comparison.

**Run:** `go test ./internal/lock -run TestLockSM -race -count=1`

**Prompt (tests):**

```
Task T5.2. Write tests only for deterministic acquire/release with fencing token = log index, including denial, idempotent re-acquire, wrong-owner release, and identical dumps for identical sequences. Run, show failures, stop.
```

### T5.3 Client deduplication

**Test first:** a retried command (same clientID and seq) is applied once and returns the cached result. Dangerous case: acquire (seq 1), release (seq 2), then a delayed retry of acquire (seq 1) must return the old cached result and must NOT re-acquire. A sequence number lower than the last one is ignored. The dedup table is part of Dump().

**Implement:** per-client last seq and last result, checked at apply time (paper section 8).

**Run:** `go test ./internal/lock -run TestDedup -race -count=1`

**Prompt (tests):**

```
Task T5.3. Write tests only for exactly-once application using clientID and seq inside the state machine, including the retry-after-release case. Run, show failures, stop.
```

### T5.4 Lock expiry as a log entry

**Test first:** in the state machine, Expire{lock, token} frees the lock only if the current holder's token matches. A stale Expire for an old token is ignored (the lock was re-granted since). On the leader side, a lock held longer than its TTL in ticks makes the leader propose an Expire. A new leader restarts every TTL timer from the election, which only ever extends holders' time (safe direction). No node decides expiry from its own clock inside the state machine.

**Implement:** TTL carried in Acquire, leader-local lease timers, Expire proposals.

**Run:** `go test ./internal/lock -run TestExpiry -race -count=1`

**Prompt (tests):**

```
Task T5.4. Write tests only for lock expiry as a replicated Expire{lock, token} entry proposed by the leader's tick-based timers, ignoring stale tokens, with timers restarted on a new leader. Run, show failures, stop.
```

### T5.5 Server Submit

**Test first** (harness): Submit on the leader returns after the entry is applied, with the result. On a follower it returns ErrNotLeader with a hint. If the leader loses leadership before commit, and the index now holds an entry of a different term, Submit returns ErrLeaderChanged. Timeouts return an error and waiters are cleaned up. A retry on the new leader applies exactly once.

**Implement:** `Server.Submit(clientID, seq, cmd)` with waiters keyed by (index, term).

**Run:** `go test ./internal/lock -run TestSubmit -race -count=1`

**Prompt (tests):**

```
Task T5.5. Write tests only for a Server.Submit that waits for apply, redirects from followers, detects lost leadership by comparing the term at the index, and times out cleanly. Run, show failures, stop.
```

### T5.6 Clerk (client retry and redirect)

**Test first:** the clerk caches the last known leader, follows hints, falls back to trying other servers, and backs off. It reuses the same (clientID, seq) across retries and increments seq only after success. A leader change mid-request still yields exactly one effect. A duplicate request after a partition heals is deduplicated.

**Implement:** `Clerk` over a small `Transport` interface (sim and real implementations).

**Run:** `go test ./internal/lock -run TestClerk -race -count=1`

**Prompt (tests):**

```
Task T5.6. Write tests only for a Clerk with leader caching, redirect hints, round-robin fallback, backoff, and stable (clientID, seq) across retries. Run, show failures, stop.
```

### T5.7 Reads through the log

**Test first:** a Status command goes through the log like a write, so a read after a completed write always sees it. A deposed leader cannot answer (its Submit never commits).

**Implement:** a read-only command type applied without changing state. Simple and always correct.

**Run:** `go test ./internal/lock -run TestReadThroughLog -race -count=1`

**Prompt (tests):**

```
Task T5.7. Write tests only for linearizable lock-status reads implemented as log entries, including a partitioned old leader. Run, show failures, stop.
```

### T5.8 ReadIndex reads (optimization)

**Test first:** the leader serves a read only when it has committed an entry of its current term (the no-op), records readIndex = commitIndex, confirms leadership by a heartbeat round acknowledged by a majority, and waits until lastApplied >= readIndex. A partitioned old leader cannot serve reads. A read immediately after election waits for the no-op to commit. A read after a write sees the write.

**Implement:** heartbeat round ids, readIndex wait (paper section 8). Leases stay out until you want them, because of the clock-drift risk.

**Run:** `go test ./internal/lock ./internal/sim -run TestReadIndex -race -count=1`

**Prompt (tests):**

```
Task T5.8. Write tests only for ReadIndex reads: no-op committed first, leadership confirmed by a majority heartbeat round, wait for apply, and a partitioned old leader unable to serve. Run, show failures, stop.
```

### T5.9 Mutual exclusion and fencing integration

**Test first:** several clerks contend for one lock under message drops and leader crashes. Assert at most one holder per token, tokens increase with each grant, and a simulated protected resource that rejects any token lower than the highest it has seen rejects a stale holder after expiry.

**Implement:** only fixes the test exposes. Background reading: Kleppmann on distributed locking.

**Run:** `go test ./internal/sim -run TestMutualExclusion -race -count=1`

**Prompt (tests):**

```
Task T5.9. Write tests only for mutual exclusion with fencing tokens under faults, including a fake resource that enforces monotonic tokens. Run, show failures, stop.
```

## Phase 6: Robustness

### T6.1 Crash and restart matrix

**Test first:** crash a follower mid-replication and restart it. Crash the leader after commit but before followers learn the commit index. Restart all three nodes at once. In every case: the log and term come back from disk, commitIndex and lastApplied restart at 0 and are relearned, the state machine and dedup table are rebuilt by replay, and checkers stay quiet.

**Implement:** only fixes the tests expose.

**Run:** `go test ./internal/sim -run TestCrashRestart -race -count=1`

**Prompt (tests):**

```
Task T6.1. Write tests only for crash and restart scenarios (follower, leader after commit, whole cluster) with replay of the state machine. Run, show failures, stop.
```

### T6.2 Timing validation

**Test first:** Config.Validate rejects election min below 5 times the heartbeat interval. A benchmark reports FileStorage Save latency (fsync cost). A documented rule: election timeout minimum should be at least 10 times the sum of p99 fsync latency and round-trip time.

**Implement:** stricter Validate, `BenchmarkFileStorageSave`. Paper section 5.6: safety does not depend on timing, availability does.

**Run:** `go test ./internal/raft -run TestConfigTiming -race -count=1` and `go test ./internal/storage -bench Save`

**Prompt (tests):**

```
Task T6.2. Write tests only for stricter timing validation plus a benchmark of FileStorage.Save. Run, show failures, stop.
```

### T6.3 (optional) CheckQuorum

**Test first:** a leader that has not heard from a majority within one election timeout steps down. A partitioned leader steps down while the majority side elects a new one.

**Implement:** per-peer last-heard tick. Source: Ongaro's thesis. Not read yet, so check the section number in your copy.

**Run:** `go test ./internal/raft ./internal/sim -run TestCheckQuorum -race -count=1`

**Prompt (tests):**

```
Task T6.3. Write tests only for CheckQuorum: leader steps down without majority contact within an election timeout. Run, show failures, stop.
```

### T6.4 (stretch, read paper section 7 first) Log compaction

**Test first:** the log tracks lastIncludedIndex and lastIncludedTerm. Term(i) and prevLogTerm work at the snapshot boundary. Compacting up to the applied index keeps later entries and indexes unchanged. The state machine snapshot includes the dedup table.

**Implement:** offset-aware Log, state machine Snapshot and Restore.

**Run:** `go test ./internal/raft ./internal/lock -run TestCompaction -race -count=1`

**Prompt (tests):**

```
Task T6.4. Write tests only for an offset-aware log with lastIncludedIndex/Term and a state machine snapshot that includes the dedup table. Run, show failures, stop.
```

### T6.5 (stretch) InstallSnapshot

**Test first:** a leader whose nextIndex for a peer falls at or below its lastIncludedIndex sends InstallSnapshot. The follower replaces its state, keeps a log suffix if it matches, and resets lastApplied. A lagging follower catches up through a snapshot in an integration test.

**Implement:** the third RPC (paper section 7), single-chunk is enough.

**Run:** `go test ./internal/raft ./internal/sim -run TestInstallSnapshot -race -count=1`

**Prompt (tests):**

```
Task T6.5. Write tests only for InstallSnapshot: leader decision, follower install with matching-suffix retention, and a lagging-follower integration test. Run, show failures, stop.
```

## Phase 7: System tests and the real runnable cluster

### T7.1 Chaos test

**Test first** (`internal/sim/chaos_test.go`): for each seed, run 20000 ticks of random crash, restart, partition, heal, and drop-rate changes while clerks issue commands. Safety checkers run after every step. On failure the test prints the seed and the trace tail. The environment variable CHAOS_SEEDS sets how many seeds (default 20).

**Implement:** the random action generator. Same seed gives the same run.

**Run:** `CHAOS_SEEDS=100 go test ./internal/sim -run TestChaos -race -count=1`. To replay a failure: `CHAOS_SEED=<n> go test ./internal/sim -run TestChaos -v`

**Prompt (tests):**

```
Task T7.1. Write tests only for a seeded chaos test over the cluster harness with all safety checkers active, printing seed and trace on failure. Run, show failures, stop.
```

### T7.2 Linearizability check

**Test first:** record client histories (invoke and return ticks for Acquire, Release, Status). Check them with Porcupine using a lock model. A valid history passes. A deliberately stale-read history fails.

**Implement:** history recorder and the Porcupine model.

**Run:** `go test ./internal/sim -run TestLinearizable -race -count=1`

**Prompt (tests):**

```
Task T7.2. Write tests only for a history recorder and a Porcupine model of the lock service, with one valid and one stale-read history. Run, show failures, stop.
```

### T7.3 Real runtime and transport

**Test first** (`internal/rpc`): three in-process servers on localhost ports elect a leader and serve a Submit. The runtime loop is the only goroutine touching the node: a ticker every 1 ms calls Tick, incoming HTTP requests become events on a channel, and outgoing messages are sent after Step returns.

**Implement:** HTTP/JSON transport, event loop, graceful shutdown.

**Run:** `go test ./internal/rpc -race -count=1`

**Prompt (tests):**

```
Task T7.3. Write tests only for a real runtime: three in-process servers on localhost elect a leader and complete a Submit. One goroutine owns the node. Run, show failures, stop.
```

### T7.4 raftlockd binary

**Test first:** flag parsing unit test for `--id`, `--listen`, `--peers id=addr,id=addr`, and `--data-dir`. Bad input gives a clear error.

**Implement:** `cmd/raftlockd` wiring FileStorage, runtime, and logging.

**Run:** `go test ./cmd/raftlockd -race -count=1`. Then by hand, in three terminals:

```
go run ./cmd/raftlockd --id n1 --listen :7001 --peers n2=:7002,n3=:7003 --data-dir /tmp/rl/n1
go run ./cmd/raftlockd --id n2 --listen :7002 --peers n1=:7001,n3=:7003 --data-dir /tmp/rl/n2
go run ./cmd/raftlockd --id n3 --listen :7003 --peers n1=:7001,n2=:7002 --data-dir /tmp/rl/n3
```

Watch one node log 'became leader' within about a second.

**Prompt (tests):**

```
Task T7.4. Write tests only for raftlockd flag parsing and validation. Run, show failures, stop.
```

### T7.5 raftlockctl and the failure drill

**Test first:** the CLI parses `acquire <lock> --ttl`, `release <lock> --token`, and `status <lock>`, uses the Clerk over HTTP against in-process servers, and prints the fencing token.

**Implement:** `cmd/raftlockctl` with `--servers`.

**Run:** `go test ./cmd/raftlockctl -race -count=1`. Failure drill by hand with the three nodes from T7.4:

1. `go run ./cmd/raftlockctl --servers :7001,:7002,:7003 acquire jobs --ttl 5000` (prints a token)
2. From a second terminal acquire the same lock (denied, names the holder).
3. Find the leader in the node logs and kill that process.
4. Run `status jobs` (still held, served by the new leader).
5. Release with the token, then acquire again from the second terminal (a higher token).
6. Restart the killed node and confirm it catches up.

**Prompt (tests):**

```
Task T7.5. Write tests only for the raftlockctl commands (acquire, release, status) against in-process servers, printing fencing tokens. Run, show failures, stop.
```

### T7.6 README, diagram, decision log

**Test first:** a doc check that every command in the README exists as a make target or cmd directory.

**Implement:** README (what it is, how to run, how it is tested, what is deliberately out of scope), architecture diagram, and a decision log entry per section 2 decision.

**Run:** `make test`

**Prompt (tests):**

```
Task T7.6. Write a small test that checks every command mentioned in README.md exists. Then draft the README, an architecture diagram in Mermaid, and a decision log from the spec's architecture decisions. Tests first, then stop.
```

## What may be missing (your call)

1. **Section 7 (snapshots) and the thesis (CheckQuorum) are unread.** T6.3 to T6.5 are stretch and blocked until you read them.
2. **Lock semantics are my defaults.** No wait queue (clients retry), TTL carried in Acquire, re-acquire by the holder returns the same token. Change them before Phase 5 if you disagree.
3. **Transport choice.** HTTP/JSON for simplicity. Swap for gRPC only if you want it for the portfolio.
4. **PreVote and membership changes are out of scope.** T2.13 documents the disruption an isolated node causes without PreVote.
5. **Formal verification is deferred.** The Phase 1 checkers are the Figure 3 properties in executable form. They translate directly to TLA+ invariants later.
6. **Checkers are only trustworthy if they can fail.** Phase 1 tests them with bad histories. Consider mutation checks later: break one Raft rule on purpose and confirm the chaos test catches it.

## Sources

- [Raft paper (Ongaro and Ousterhout)](https://raft.github.io/raft.pdf)
- [Ongaro PhD thesis](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
- [Kleppmann, How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
- [Porcupine linearizability checker](https://github.com/anishathalye/porcupine)
- Kleppmann's Raft lecture slides (Cambridge Distributed Systems course), used as the source for the slide references
