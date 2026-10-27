# Decision log

One entry per architecture decision in `CLAUDE.md`. Each says what was decided, why, and what it costs.

## D1. Deterministic core

**Decision.** `internal/raft` has no goroutines, no `time.Now`, no global rand and no network. Time enters through `Tick()` (1 tick = 1 ms of logical time), messages through `Step(msg)`, and outputs are returned as messages.

**Why.** Consensus bugs live in rare interleavings. With every source of nondeterminism outside the core, a test can drive any interleaving on purpose, and every chaos run replays exactly from its seed.

**Cost.** A runtime wrapper is needed to run it for real (D7), and timeouts are counted in ticks, so a busy runtime that drops ticks stretches them.

## D2. Persist before send

**Decision.** `Step` and `Tick` save any changed durable state (`currentTerm`, `votedFor`, `log[]`) before returning messages. The harness sends only after the call returns. If the save fails, no messages are returned.

**Why.** Figure 2 requires durable state to be on disk before responding to any RPC. Otherwise a node could vote twice in one term, or acknowledge an entry it then forgets, after a crash.

**Cost.** Every vote and append waits for an fsync (about 8 ms on the development machine), which bounds throughput. The heartbeat interval was raised to 30 ms so the timing rules leave room for it.

## D3. One-way async messages

**Decision.** Requests and replies are separate one-way messages. Replies carry the term, and a node handles a reply only if its role and term still match.

**Why.** Real networks drop, duplicate, delay and reorder. Treating every reply as possibly stale makes those cases ordinary code paths instead of exceptions, and the fake network can produce all of them.

**Cost.** Some replies echo request fields (`PrevLogIndex`, `MatchIndex`) so the leader can tell which request an answer belongs to.

## D4. 1-based indexes with a sentinel

**Decision.** Log indexes start at 1. `log[0]` is a sentinel with term 0, so the log length equals the last index.

**Why.** It matches the paper exactly, and the consistency check for the first entry (`prevLogIndex = 0`, `prevLogTerm = 0`) needs no special case.

**Cost.** Code that slices the log must skip the sentinel; snapshots and the stored state hold real entries only.

## D5. Fixed three-node cluster, N-node harness

**Decision.** The system runs exactly three nodes. The test harness accepts any N.

**Why.** Membership changes are a separate, subtle protocol and out of scope. Three nodes tolerate one failure. The harness needs five nodes to reproduce Figure 8.

**Cost.** Replacing a machine means stopping the cluster.

## D6. Clock-free lock state machine

**Decision.** The lock state machine receives `(index, command)` and never reads a clock. The fencing token is the log index of the acquire entry. Expiry is a replicated `Expire{lock, token}` entry proposed by the leader's tick-based lease timers. Duplicates are filtered by `(clientID, seq)` inside the state machine.

**Why.** Every replica must reach the same state from the same log; a clock read during apply would break that. Log indexes only increase, so tokens are unique and monotonic for free. Naming the token in `Expire` makes a late expiry of an already-released grant harmless.

**Cost.** A lease runs on the leader's logical clock, so expiry is approximate, and a new leader restarts every lease timer in full.

## D7. Runtime wrapper last

**Decision.** `internal/rpc` runs each node in one goroutine that owns it, with a 1 ms ticker, an event channel for incoming messages and client calls, and HTTP/JSON between nodes. It was built only after the core was complete and tested.

**Why.** One owner goroutine keeps the core free of locks and makes the race detector a check on the wrapper itself. Building it last meant all the consensus logic was tested deterministically first.

**Cost.** HTTP/JSON is simple but not the fastest transport, and there is no authentication or TLS.

## D8. Simple persistence

**Decision.** Each `Save` rewrites the whole durable state: a CRC32 followed by JSON, written to a temp file, fsynced, renamed, and the directory fsynced.

**Why.** The rename makes each save atomic, and the CRC catches a torn or corrupt file, with very little code.

**Cost.** Save time grows with the log. An append-only log file, or snapshots (out of scope), would fix that if it ever matters.
