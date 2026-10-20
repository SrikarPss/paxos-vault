# PaxosVault — Verification Record and Acceptance Checklist

## Environment and result

Verified at approximately **2026-10-20 05:29 UTC** on macOS arm64 using `go version go1.27.3 darwin/arm64`, installed at `/usr/local/go/bin/go`.

Both binaries build, static checks pass, and **all 16 tests pass with the race detector enabled**. No races were reported. Go formatting is clean. The Makefile discovers Go on PATH and falls back to `/usr/local/go/bin/go` when necessary.

The sandbox allows file operations but denies local socket binding with `Operation not permitted`. Consequently, the real process demonstration and benchmark cannot start here. Docker is also unavailable. These remaining limitations are distinct from passing compilation and in-process tests.

## Validation status

| Check | Status | Command/evidence |
| --- | --- | --- |
| JSON configuration/example/dashboard syntax | Passed | `python3 scripts/check_assets.py` |
| Python helper syntax and election timing | Passed | Offline asset checks |
| Audit verifier checkpoint and tamper rejection | Passed | Synthetic deterministic checkpoint; no server required |
| Directory sync support | Passed | Local filesystem supports syncing an open directory descriptor |
| Go formatting | Passed | `make fmt`; `gofmt -l cmd internal` returns no paths |
| Static Go checks | Passed | `go vet ./...` through `make check` |
| All 16 tests with race detection | Passed | `go test -race -cover ./...` through `make check` |
| Three-replica JSON/HTTP integration | Passed | `TestJSONPeerTransportFailover`, using the production codec and handlers with an in-process round trip |
| Both executables | Built | `make build` produces `bin/ledgerd` and `bin/bankctl` |
| Executable argument help | Passed | `./bin/ledgerd -h`; `./bin/bankctl -h` |
| Process crash/failover demonstration | Attempted; blocked by sandbox | Local port allocation fails with `PermissionError: [Errno 1] Operation not permitted` |
| Real workload measurements | Attempted; blocked by sandbox | Same local port-binding restriction |
| Docker deployment and monitoring import | Pending: tools unavailable | `docker compose up --build`; optional Prometheus/Grafana |

## Coverage and logs

| Package | Statement coverage |
| --- | ---: |
| `internal/api` | 80.6% |
| `internal/consensus` | 78.6% |
| `internal/ledger` | 77.8% |
| `internal/storage` | 74.6% |
| `cmd/bankctl`, `cmd/ledgerd` | 0.0% automated statement coverage; compiled and help smoke-tested |

The test result is not a coverage guarantee or a formal correctness proof. Cached passing results were reused for the unchanged consensus, ledger, and storage packages; the modified HTTP tests ran afresh.

Saved evidence:

- [Verification metadata](verification/report.json)
- [Static checks and race-test output](verification/check.log)
- [Build output](verification/build.log)
- [Blocked process-demo report](verification/demo-startup.json)
- [Blocked benchmark report](verification/benchmark-startup.json)

This saved verification evidence is included in the repository. Runtime data, binaries, and experiment directories under `artifacts/` are excluded by `.gitignore`.

The verification commands used writable temporary Go caches to respect the sandbox:

```sh
GOCACHE=/private/tmp/quorum-ledger-gocache \
GOMODCACHE=/private/tmp/quorum-ledger-gomodcache make check

GOCACHE=/private/tmp/quorum-ledger-gocache \
GOMODCACHE=/private/tmp/quorum-ledger-gomodcache make build
```

These cache overrides are environment-specific; a normal local shell can use `make check` and `make build` directly.

## Included behavioral tests

| Test family | Behavior covered |
| --- | --- |
| Banking | Atomic net settlement, stable success/rejection receipts, conflicting IDs, negative/invalid amounts, supply conservation across seeded random transfers |
| Storage | Durable record replay, partial final header/payload repair, checksum-corruption rejection, process lock |
| Consensus | Minority write/read refusal, majority failover, accepted-but-unlearned recovery, hole filling, highest accepted adoption, full restart, durable promises, same-ballot conflict rejection, concurrent retries, wrong-member WAL rejection, automatic elections |
| HTTP contracts | Follower errors, peer-token requirement, reserved no-op rejection, strict JSON, conflicting ID response, state read, audit bounds, metrics format |
| JSON peer integration | Three replicas, production serialization and peer authentication, banking requests, partitioned leader rejection, successor writes/reads, and restored replica convergence |

HTTP contract tests invoke the production handler using `httptest.NewRequest` and `httptest.NewRecorder`. The peer integration test injects an HTTP `RoundTripper` that routes requests to the production handlers. This exercises JSON encoding/decoding and application HTTP behavior without opening a TCP listener. It does not validate TCP behavior, separate-process scheduling, or SIGKILL recovery.

The process harness now records a startup-phase failure report even when cluster construction fails, rather than exiting with an uncaught traceback. No throughput or process failover measurements are claimed.

## Remaining acceptance procedure outside the sandbox

From a terminal that permits local port binding:

```sh
make demo
make benchmark
```

Preserve the generated artifact directories. The demo must report overall success and every scenario must pass. Verify its exported audit using `python3 scripts/audit.py <path>/audit.json`. Record actual results, Go version, and machine/storage details here after running. Docker and optional Prometheus/Grafana checks also require their respective installed tools.

## Suggested additional evaluation

The consensus tests use real WALs and a controllable partition transport; the HTTP integration test uses the production JSON codec and handlers. Neither is exhaustive. A deterministic scheduler with delayed/duplicated messages, crash points between each WAL operation, filesystem error injection, and model checking would strengthen the evidence before broad deployment.
