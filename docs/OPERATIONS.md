# PaxosVault — Operations Guide

## Native workflow

1. Install a supported [Go distribution](https://go.dev/dl/) and Python 3.10+.
2. Run `make check`, then `make demo` for an isolated experiment.
3. Run `make run` for persistent interactive use on ports 8101–8103.
4. In another terminal, use the README's `bankctl` sequence. Stop with Ctrl+C.

The Python supervisor starts compiled node binaries. It stops its own children on normal exit and saves their logs. `make run` preserves `data/`; demo/benchmark modes use fresh `artifacts/` directories and dynamically allocated ports. Avoid mixing clusters on the same data directories or ports.

Run members individually when testing manual failures:

```sh
./bin/ledgerd -id n1 -config config/local.json -data data/n1
./bin/ledgerd -id n2 -config config/local.json -data data/n2
./bin/ledgerd -id n3 -config config/local.json -data data/n3
```

Each command belongs in its own terminal. Restart a member using the same ID, cluster configuration, and directory. A full-cluster restart is supported when the durable state remains intact; a full-cluster data wipe starts a new ledger and loses all receipts.

## Failure behavior

| Event | Expected behavior |
| --- | --- |
| One follower stops | Writes can continue through the remaining majority |
| Active leader stops | A surviving candidate prepares, recovers history, and resumes writes after election |
| Two of three members stop | New writes and strong reads fail; local diagnostics remain available |
| A partition isolates the old leader | Its local leader belief can briefly be stale, but it cannot acknowledge a new quorum decision |
| Failed member returns | Leader pulses transfer missing decisions in bounded batches |
| All members restart | Each replays its WAL, then election recovers any chosen/unlearned values |
| WAL contains an incomplete final frame | Replay truncates that partial frame |
| WAL contains a checksum mismatch | Startup fails; retain evidence and investigate |

A failed submission can become chosen during later recovery, including an operation previously accepted only by a minority. Reusing its original ID makes the recovery/retry combination safe. Clients must not infer abort solely from a timeout.

## Timing

The local configuration uses 150 ms heartbeats, 300 ms per-RPC timeouts, and election delays randomized between 1500 and 3000 ms. Validation requires the minimum election delay to be at least three times `(heartbeat + RPC timeout)`. This is a starting configuration for a lab, not an SLA. Disk stalls, recovery size, scheduling, and network delays affect failover duration.

The proposer serializes all writes and read barriers. Larger client concurrency does not imply pipelined consensus. Each positive durable transition syncs its WAL; benchmark results should state filesystem, storage, hardware, membership count, and whether the processes shared a host.

## Metrics

Start a separately installed Prometheus with:

```sh
prometheus --config.file=config/prometheus.yml
```

That configuration scrapes the native cluster and evaluates `config/alerts.yml`. Import `config/grafana-dashboard.json` into a Grafana instance connected to Prometheus. The monitoring services are optional and are not automatically installed or hosted.

Useful PromQL:

```promql
# Local leadership beliefs: temporary disagreement is possible during election.
sum(ledger_leader)

# Client submission attempts per second, across members.
sum(rate(ledger_submissions_total[1m]))

# Replica progress spread among reachable members.
max(ledger_applied_slot) - min(ledger_applied_slot)

# Mean submission duration, including invalid/rejected attempts.
sum(rate(ledger_submission_duration_seconds_total[1m]))
  / sum(rate(ledger_submissions_total[1m]))
```

There are no server-side latency histogram buckets; p50/p95/p99 come from the workload runner's recorded client timings. Prometheus process counters reset on restart.

## Data and deployment boundaries

Public banking endpoints have no user authentication. Bind them to loopback or place them behind an authenticated private gateway for a lab deployment. The internal token is a lab shared secret sent over HTTP; use a trusted private network. Customer identity, transport encryption, secret rotation orchestration, and production controls are separate work.

Do not change the fixed node/address map while retaining these WALs. A matching membership count alone does not make a replacement acceptor safe. Restoring an outdated individual WAL or losing durable quorum state can break protocol assumptions. Snapshot installation, membership changes, and disk replacement need a designed recovery protocol.

WAL and audit histories are unbounded. Archive experiment directories after collecting results. The source contains no automatic data deletion command.
