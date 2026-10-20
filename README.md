# PaxosVault: A Resilient Distributed Banking and Settlement System Using Multi-Paxos

**A durable, replicated banking ledger built in Go around Multi-Paxos.**

PaxosVault is a fault-tolerant banking and settlement service. Clients create accounts, fund them from a fixed treasury, and submit atomic transfer batches. Multi-Paxos establishes a shared command order across replicas, keeping transaction execution consistent through leader changes and replica restarts. The system provides an HTTP API, a client CLI, persistent consensus state, automatic elections, replica catch-up, and monitoring.

## Features

| Capability | Implementation |
| --- | --- |
| Distributed banking | Fixed-supply ledger with integer cents, account creation, and atomic net settlement of up to 64 transfer legs |
| Multi-Paxos | A global prepare per leader ballot; stable leaders reuse that ballot for successive log slots |
| Failure recovery | Fsynced, checksummed WAL; persistent promises and acceptances; recovery of accepted values; hole filling; periodic catch-up |
| Transaction consistency | Deterministic execution, nonnegative balances, conserved supply, stable receipts for retries, conflicting-ID detection |
| Leader election | Randomized election timeouts, totally ordered ballots, preemption, majority-based decisions |
| Monitoring | Prometheus metrics, node status, explicit local diagnostics, Grafana dashboard configuration |
| Tooling and documentation | Audit-chain verifier, isolated failure demonstration, workload runner, Docker setup, CI, design report, presentation, and demo guide |

## Quick start

Requires **Go 1.23+**, Python 3.10+, and Make on macOS or Linux. Use a currently supported Go release from [go.dev](https://go.dev/dl/). The Go service and Python scripts have no third-party library dependencies.

```sh
git clone https://github.com/SrikarPss/paxos-vault.git
cd paxos-vault
make check
make demo
```

The demo creates a fresh three-process cluster on dynamically allocated loopback ports. It checks retries, overdrafts, leader SIGKILL, failover, follower recovery, majority loss, safe retry of an uncertain transaction, full-cluster restart, and the audit chain. It shuts its processes down and saves logs, `report.json`, `audit.json`, and `metrics.prom` in `artifacts/demo-<timestamp>-<suffix>/`.

**Validation status:** Both executables build with Go 1.27.3, `go vet` is clean, and all 16 tests pass with the race detector enabled. Tests include three replicas using the production JSON transport and HTTP handlers through an in-process transport. The sandbox forbids binding local ports, so the process demonstration and benchmark were attempted but blocked at startup. Docker remains unverified. See [the verification record](deliverables/VALIDATION.md) for logs, coverage, and the remaining checks. No benchmark numbers or process failover measurements are claimed.

## Interactive use

Start the persistent local cluster in one terminal:

```sh
make run
```

In another terminal:

```sh
./bin/bankctl status
./bin/bankctl -file examples/open-alice.json submit
./bin/bankctl -file examples/open-bob.json submit
./bin/bankctl -file examples/fund-alice.json submit
./bin/bankctl -file examples/payroll.json submit
./bin/bankctl state
./bin/bankctl audit
```

Repeat the payroll submission with the same file to receive its original receipt. Balances move only once. Flags precede the command. Ctrl+C stops the processes; `data/` is preserved for the next run. The default member ports are 8101–8103.

```sh
make benchmark
# Or, after make build:
python3 scripts/cluster.py benchmark --transactions 500 --concurrency 8
python3 scripts/audit.py artifacts/<demo-directory>/audit.json
```

Benchmark reports contain measured client throughput and p50/p95/p99 latency, with scope stated in the output. Each benchmark uses a fresh cluster and leaves the interactive cluster untouched.

## Docker option

```sh
docker compose up --build
# Stop and retain the three durable named volumes:
docker compose down
```

The images expose the same host ports as the native cluster, so run one of these configurations at a time. For a container-only client, use `curl` against the host ports and discover the leader through `/status`; the native CLI is built by `make build`. The image runs as a non-root user. Docker verification is pending in this workspace.

## API at a glance

| Endpoint | Behavior |
| --- | --- |
| `POST /v1/transactions` | Submit an account creation, single transfer, or batch to the leader |
| `GET /v1/state` | Read balances after a replicated barrier; requires a majority |
| `GET /v1/audit?after=0&limit=100` | Read a page of audit events after a replicated barrier |
| `GET /v1/local-state` | Read local replica state; may be stale |
| `GET /status` | Inspect the member's current leadership belief and durable ballot |
| `GET /healthz` | Process/storage health; does not certify quorum availability |
| `GET /metrics` | Prometheus exposition |

Followers return a leader hint; `bankctl` discovers the leader and retries. A `503`, timeout, or broken connection can leave a transaction's outcome unknown. Reuse its original ID and payload. Business rejections are also durable receipts and require a new ID for a new attempt after circumstances change.

## Guarantees and boundaries

The implementation targets crash/restart failures with fixed, trusted membership and reliable durable storage. A command becomes chosen after a strict majority durably accepts it. Learners execute a contiguous prefix; reads commit barriers rather than depending on clock leases. A three-member cluster can continue with one unavailable member after leadership stabilizes. Losing a majority stops progress.

These are implementation design properties supported by the included test cases, **not a formal proof or a production banking certification**. Eventual progress requires a connected majority, bounded communication and storage delays after some point, and a leader that remains stable long enough to recover the log. Peer messages are trusted; Byzantine failures are outside the model.

The log, receipts, and audit history are retained indefinitely. There is no compaction, dynamic membership, multi-currency accounting, customer authentication, TLS termination, or cross-shard transaction protocol. Public APIs are intended for loopback/private lab use. The audit hash links detect changes relative to a trusted checkpoint; they are not signatures.

## Project map

```text
cmd/ledgerd/             node executable and graceful shutdown
cmd/bankctl/             leader discovery and retrying client
internal/consensus/      ballots, acceptors, leader recovery, replication
internal/ledger/         deterministic settlement and receipts
internal/storage/        framed WAL, CRC checks, fsync, process lock
internal/api/            HTTP transport, API, Prometheus metrics
config/                 native and Docker membership, monitoring assets
examples/               ready-to-submit transaction commands
scripts/                process harness, workload, independent audit verifier
docs/                   protocol, API, operation and failure contracts
deliverables/           report, presentation, acceptance record, demo guide
```

Read [the design](docs/DESIGN.md), [API reference](docs/API.md), [operations guide](docs/OPERATIONS.md), and [project report](deliverables/PROJECT_REPORT.md).

Protocol reference: Leslie Lamport, [*Paxos Made Simple*](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf), especially the quorum-adoption rule and state-machine construction. PaxosVault is an independent educational implementation.
