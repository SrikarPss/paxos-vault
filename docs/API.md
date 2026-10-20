# PaxosVault — HTTP API

Member addresses come from the fixed cluster configuration. Client traffic is intended for a trusted local/private network. Internal routes require the shared peer token. There is no customer identity or authorization model.

## Commands

`POST /v1/transactions` accepts one JSON object, capped at 64 KiB. Unknown fields and trailing JSON are rejected. IDs and account names use 1–64 ASCII letters, digits, dots, underscores, or dashes, beginning with a letter or digit.

Create a zero-balance account:

```json
{"id":"open-alice","kind":"open","account":"alice"}
```

Fund it from the existing treasury account. Amounts are **integer cents**:

```json
{"id":"fund-alice","kind":"transfer","transfers":[{"from":"treasury","to":"alice","amount_cents":100000}]}
```

Settle a batch atomically:

```json
{
  "id":"settlement-001",
  "kind":"batch",
  "transfers":[
    {"from":"alice","to":"bob","amount_cents":25000},
    {"from":"bob","to":"alice","amount_cents":1000}
  ]
}
```

`transfer` requires exactly one leg; `batch` permits 1–64. Every referenced account must already exist. Treasury participates in the same rules as other accounts. Self-transfers, zero/negative amounts, and amounts over total supply are rejected structurally. A no-op is reserved for the protocol and cannot be submitted through this endpoint.

The receipt fields are `id`, `ok`, `code`, `slot`, and `audit_hash`. A successful request returns HTTP 200. Business rejections return HTTP 422 with the same receipt shape. Codes are `applied`, `account_exists`, `account_missing`, `insufficient_funds`, and `id_conflict`.

Identical retries preserve the original receipt's slot and hash even though the retry itself occupies another audit slot. Conflicting payloads receive `id_conflict`; they never replace the saved receipt. For this conflict, the public lookup refers to the original receipt's slot/hash. The audit page records the conflicting attempt's own event.

## Reads

`GET /v1/state` returns `applied_slot`, `accounts`, `total_cents`, `audit_head`, and `receipt_count`. It commits a quorum-backed barrier first and carries `X-Ledger-Consistency: linearizable`.

`GET /v1/local-state` returns the same shape immediately from that replica and carries `X-Ledger-Consistency: local`. It can lag and remains available in a minority; use it for diagnostics and convergence checks.

`GET /v1/audit?after=<slot>&limit=<count>` returns a `snapshot` and an `events` array. `after` is an exclusive slot cursor, defaults to 0, and must be nonnegative. `limit` defaults to 100 and ranges from 1 to 1000. This endpoint also commits a barrier. Each event has `slot`, `command`, `code`, `previous`, `state_hash`, and `hash`.

For a complete audit export, fix the first page's snapshot index as a target and read only through that index. Subsequent audit reads add barriers beyond the target. The demo harness implements this pagination and exports a consistent prefix.

## Discovery and errors

`GET /status` returns member ID, cluster ID, local leader belief, `leader_id`, promised ballot, applied index, retained accepted/decided slot counts, quorum size, and any fatal storage/state failure. A leader hint can identify a candidate or obsolete leader; rediscover after transient failures.

| HTTP status | Meaning | Client action |
| --- | --- | --- |
| 200 | Applied command, saved duplicate receipt, or successful read | Inspect payload |
| 400 | Malformed/invalid input | Correct the request |
| 409 | Recipient is not the current active leader | Rediscover, retry same ID/payload |
| 422 | Deterministic banking rejection, including conflicting ID | Inspect code; use a fresh ID for a genuinely new attempt |
| 503 | Could not obtain a majority; write outcome may be unknown | Back off, rediscover, retry same ID/payload |
| 500 | Storage, internal protocol, or state-machine failure | Inspect member logs/status |

A connection failure, a client timeout, or a 500 after acceptance can also leave the outcome unknown even if an error body lacks `outcome_unknown`. Treat every failure without a definitive receipt as potentially accepted. Do not compensate or invent a new ID solely because a response was lost.

Leader/unavailability errors include `error`, `leader_id`, `leader_url`, `retryable`, and `outcome_unknown`. Followers do not proxy or redirect a banking request. `bankctl` discovers members from its own configured URL list, which also works against Docker host port mappings.

## Monitoring and peer RPCs

`GET /healthz` returns 200 when the member has no recorded fatal storage/state error; an unhealthy member returns 503. It is not a quorum readiness check.

`GET /metrics` exports gauges for leadership belief, applied index, promised round, retained acceptances, account count, conserved cents, and uptime, plus counters for HTTP requests, submissions, rejections, and accumulated submission handling time.

`POST /internal/{prepare,accept,heartbeat,learn}` carries trusted consensus messages. It requires `Authorization: Bearer <internal_token>` and rejects unknown senders, zero ballots, malformed commands, and oversized learn batches. These routes are not a client-facing API. A learn message relies on the trusted sender's prior collection of a quorum; no quorum certificate is attached.
