# PaxosVault — Demonstration Guide

## Prerequisites and preparation

Use Go 1.23+, Python 3.10+, and macOS/Linux. From the project directory:

```sh
make check
make demo
```

The demonstration is isolated from `data/`. All failure injection targets child processes started for that experiment. Do not describe the checks as passing until the generated report records `"passed": true`.

## Suggested walkthrough

1. **Problem:** show how a lost response creates ambiguous transaction outcomes, then explain permanent receipt IDs.
2. **Architecture:** show `docs/DESIGN.md` and the prepare/adopt/accept/learn diagram.
3. **Settlement:** the harness opens three accounts, funds Alice, and applies a payroll batch. The identical retry returns the original receipt. An overdraft is rejected.
4. **Leader crash:** the harness kills the leader with SIGKILL, waits for a successor, and makes another transfer. The report measures recovery through a successful write, rather than claiming a fixed SLA.
5. **Replica return:** the killed member reopens its WAL and catches up to the live majority.
6. **Majority loss:** the harness stops two members. The survivor refuses a new transfer and a strong balance read.
7. **Recovery and ambiguity:** the majority returns. The previously uncertain transfer is retried with its original ID and takes effect once.
8. **Full restart:** every member is killed and restarted with its original WAL. Balances and the saved receipt survive.
9. **Evidence:** open `report.json`, per-member logs, `audit.json`, and `metrics.prom`. Run the independent verifier against `audit.json`.

## Expected accounting checkpoint

These are expected deterministic balances for the included scenario, **not measured results from the authoring workspace**:

| Account | Cents |
| --- | ---: |
| alice | 64,900 |
| bob | 20,100 |
| carol | 15,000 |
| treasury | 999,999,900,000 |
| **Total** | **1,000,000,000,000** |

Slot counts and audit heads vary with read barriers and recovery/no-op slots. The saved original receipt should remain unchanged.

## Optional workload

```sh
make build
python3 scripts/cluster.py benchmark --transactions 500 --concurrency 8
```

Record actual throughput and latency from the report, along with machine, filesystem, Go version, and client settings. The benchmark includes leader discovery in client timings and uses real WAL syncs. Single-host processes do not model independent rack or geographic failures.
