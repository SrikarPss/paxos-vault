# PaxosVault: A Resilient Distributed Banking and Settlement System Using Multi-Paxos

**Project report**

## Problem and scope

A banking operation can lose its response after the server commits it. Retrying naively can move money twice. A leader can also fail after enough replicas accept a command but before any replica knows it was chosen. A useful replicated banking project must address both protocol recovery and application-level retry semantics.

PaxosVault implements a single replicated settlement state machine using Multi-Paxos. It runs as independent Go processes and serves account creation, single transfers, atomic batches, balance reads, audit queries, and monitoring. The implementation uses standard-library HTTP/JSON and a custom durable WAL rather than an external database or consensus library.

## System capabilities

PaxosVault combines leader election, fault tolerance, replicated banking, and monitoring with a fixed-supply financial model, net settlement across transfer cycles, permanent request receipts, conflicting-request detection, independent audit verification, and a reproducible experiment harness. Strong balance reads explicitly require a replicated barrier. Monitoring observations explicitly permit stale local state.

The project also makes the failure contract concrete: availability depends on a majority; a lost response is an unknown outcome; an accepted-but-unlearned value must survive election; and failed business commands retain their original receipts.

## Implementation

The protocol layer maintains durable global promises and per-slot accepted values. Election prepares the log once, adopts prior values, fills holes, and reaccepts the recovered prefix. A stable leader then uses phase two for new slots. Periodic pulses refresh contact timers and transfer missing decisions to returning replicas.

The storage layer frames JSON records with lengths and CRC32 checksums. It syncs data before acknowledgment and rejects concurrent access to one data directory. Replay repairs incomplete final writes while failing closed on complete corrupted records.

The accounting layer uses signed 64-bit integer cents. All funds originate in a fixed treasury, so the sum of balances is constant. A batch calculates every account's final net change before updating any balances. A command ID identifies a canonical payload and stable result, covering both successful and rejected business operations.

An audit hash chain covers all chosen slots, including barriers, retries, and rejections. The Python verifier checks event hashes, contiguous ordering, a saved checkpoint, final balance hash, nonnegative balances, and total supply. It does not establish the authenticity of an untrusted checkpoint or independently prove every historical state transition.

## Evaluation deliverables

| Deliverable | Evidence produced |
| --- | --- |
| Unit and protocol tests | Race-enabled Go test results and coverage when run |
| Process failure demonstration | Fresh cluster configuration, member logs, named checks, recovery time, final state |
| Audit export | Full consistent prefix and saved checkpoint, independently hash-verified |
| Workload runner | Actual client throughput and p50/p95/p99 latency, concurrency and scope |
| Monitoring | Scrape endpoint, alert configuration, dashboard import |
| CI | Reproducible check/demo commands and uploaded experiment artifacts |

Both executables were built with Go 1.27.3 on macOS arm64. All 16 tests pass with the race detector enabled, and `go vet` reports no findings. A three-replica integration test exercises the production JSON transport, peer authentication, HTTP handlers, partition failover, strong reads, and catch-up using an in-process HTTP round trip. The storage and consensus tests use real WAL files.

The sandbox denies local TCP port binding. The process failure demonstration and benchmark were attempted but blocked during startup; their failure reports are retained. Accordingly, there are no process failover measurements or workload performance results. Docker and optional monitoring imports remain unverified. [VALIDATION.md](VALIDATION.md) records logs, coverage, and the distinction between in-process protocol evidence and process-level evaluation.

## Design properties to evaluate

- **Agreement:** each chosen slot should retain one value through leader changes.
- **Durability:** acknowledged acceptances should survive process crashes when the filesystem honors sync.
- **Consistent execution:** replicas should apply the same contiguous prefix and obtain matching state/audit hashes.
- **Money conservation:** all balances should remain nonnegative and sum to the fixed supply.
- **Retry safety:** the same ID and payload should produce one financial effect and its original receipt.
- **Partition behavior:** a minority should fail new writes and strong reads.
- **Recovery:** restored replicas should converge; a restored majority should eventually elect a stable leader.

These are conditional implementation properties, not unconditional or formally verified guarantees.

## Limitations and next work

The main scalability constraints are serialized proposals, synchronous storage, full-history election recovery, unbounded receipts/history, and bounded prepare-response decoding. Useful next steps are snapshot transfer with proven safe truncation, proposal batching/pipelining, deterministic network simulation or model checking, and richer failure injection during disk writes. Dynamic membership requires a separate protocol design.

The financial API is a trusted lab service. Customer authentication, signed audit checkpoints, TLS, multi-currency accounting, regulatory requirements, and geographically separated failure domains are outside this implementation.

## Reference

Leslie Lamport, [*Paxos Made Simple*](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf), 2001. Its majority-intersection and highest-accepted-value rules underpin the protocol; the state-machine construction motivates the replicated command sequence.
