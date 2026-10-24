# RaftLock

A distributed lock service built on the Raft consensus algorithm. Clients acquire and release named locks; the Raft log makes lock state consistent and fault-tolerant across a cluster.

Reference: Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm" (extended version). When behavior is ambiguous, the paper's Figure 2 is the spec.

## Status

Greenfield. Written in Go, standard library first. Module: `github.com/hithisisyeshwanth/RaftLock`. Final package layout is **TBD**.

## Commands

```
go build ./...
go test -race ./...
go vet ./...
gofmt -l .        # must print nothing
```

Running a local cluster: TBD.

## Planned layout

- Raft core: leader election, log replication, safety, persistence. No networking or I/O inside it.
- Transport: RPC layer (`RequestVote`, `AppendEntries`), pluggable so tests can use an in-memory network.
- State machine: the lock table (acquire, release, owner, TTL/lease), applied only from committed log entries.
- Client API: lock/unlock requests, leader redirect, retry-safe via client ID + sequence number.

## Design rules

- Implement Raft as a deterministic state machine driven by events (message received, timer fired, client command). Inject the clock, RNG, storage, and transport so tests are reproducible.
- Follow Figure 2 exactly: persist `currentTerm`, `votedFor`, and `log[]` before responding to any RPC.
- Only apply committed entries to the lock state machine, in log order.
- Lock expiry must be driven through the log (or a leader-assigned timestamp in the log entry), never by each node's local wall clock, or replicas diverge.
- Client requests must be idempotent: deduplicate by (client ID, sequence number) so retries cannot double-acquire or double-release.
- Membership changes, snapshots/log compaction, and linearizable reads (ReadIndex or leases) are separate milestones. Do not mix them into the core.

## Testing

- Unit-test the core with a simulated network and fake clock: partitions, message drop/reorder/duplicate, crash and restart.
- Assert Raft's invariants after every step in tests: Election Safety, Leader Append-Only, Log Matching, Leader Completeness, State Machine Safety.
- Test lock semantics (mutual exclusion, expiry, fencing) separately from consensus.
- Always run tests with `-race`.

## Conventions

- Name things after the paper (`currentTerm`, `commitIndex`, `lastApplied`, `nextIndex`, `matchIndex`) so code maps to the spec; in Go, unexported fields stay lowerCamel.
- Idiomatic Go: small interfaces defined where they are used, `context.Context` first on blocking calls, errors wrapped with `%w`, table-driven tests.
- Keep the Raft core to a single goroutine owning its state (events in via a channel) rather than scattering mutexes.
- Comment only the why, and cite the paper section for non-obvious safety rules.
- Keep changes small and each one verifiable by a test.
