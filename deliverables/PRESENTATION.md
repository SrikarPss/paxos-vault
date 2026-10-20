# PaxosVault: A Resilient Distributed Banking and Settlement System Using Multi-Paxos

**Presentation outline**

## 1. Consistent money movement during failure

- Three independent replicas maintain one banking command order.
- A leader crash must not lose an already chosen payment.
- Lost client responses must not produce duplicate financial effects.

Speaker cue: introduce an accepted payment whose response disappears.

## 2. A concrete settlement service

- Fixed treasury and integer-cent balances.
- Atomic batches settle final account deltas, including cycles.
- Permanent command receipts identify retries and conflicting payloads.
- Hash-linked audit events cover the entire chosen sequence.

Speaker cue: show `examples/payroll.json` and explain net settlement.

## 3. Multi-Paxos and recovery

- Prepare a majority using a fresh durable ballot.
- Adopt highest accepted values; fill holes before new requests.
- Reuse the ballot for successive slots while leadership remains stable.
- Apply learned slots in order and catch up returning replicas.

Speaker cue: use the sequence diagram in the design document.

## 4. Evidence from injected failures

- Leader SIGKILL followed by a successor write.
- Majority loss blocks writes and strong reads.
- Restored replicas and full restart preserve balances and receipts.
- Independent audit verification and replica convergence checks.

Speaker cue: use a generated passing process-demo report once available. The 16 race-enabled tests pass, including in-process partition failover, restart recovery, and JSON peer transport. The process demonstration is blocked by sandbox port-binding restrictions; no passing process report is claimed.

## 5. Monitoring and measurement

- Applied slots, ballot rounds, leadership beliefs, and supply gauges.
- Client workload records measured throughput and latency percentiles.
- Reports retain cluster configurations and per-member logs.

Speaker cue: insert actual results after running; do not substitute target values.

## 6. Boundaries and next steps

- Fixed trusted membership and durable crash/restart model.
- Progress requires a connected majority and stable leadership.
- Serialized proposals and retained history limit scale.
- Next work: snapshot/truncation protocol, pipelining, model checking, and production identity/security controls.
