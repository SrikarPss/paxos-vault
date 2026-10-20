#!/usr/bin/env python3
"""Verify an exported audit chain against its checkpoint using only stdlib."""
import argparse
import hashlib
import json
from pathlib import Path

SUPPLY = 1_000_000_000_000


def digest(value, *, sort_keys=False):
    raw = json.dumps(value, separators=(",", ":"), ensure_ascii=False, sort_keys=sort_keys)
    return hashlib.sha256(raw.encode()).hexdigest()


def canonical_command(command):
    if set(command) - {"id", "kind", "account", "transfers"}:
        raise ValueError("unknown command fields")
    result = {}
    if command.get("id"):
        result["id"] = command["id"]
    result["kind"] = command["kind"]
    if command.get("account"):
        result["account"] = command["account"]
    if command.get("transfers"):
        result["transfers"] = [
            {"from": t["from"], "to": t["to"], "amount_cents": t["amount_cents"]}
            for t in command["transfers"]
        ]
    return result


def verify(export):
    events, snapshot = export["events"], export["snapshot"]
    previous = "genesis"
    for slot, event in enumerate(events, 1):
        if event["slot"] != slot or event["previous"] != previous:
            raise ValueError(f"broken chain at slot {slot}")
        unhashed = {
            "slot": slot, "command": canonical_command(event["command"]),
            "code": event["code"], "previous": previous,
            "state_hash": event["state_hash"], "hash": "",
        }
        if digest(unhashed) != event["hash"]:
            raise ValueError(f"hash mismatch at slot {slot}")
        previous = event["hash"]
    if snapshot["applied_slot"] != len(events):
        raise ValueError("export is not a complete contiguous prefix")
    if snapshot["audit_head"] != (previous if events else ""):
        raise ValueError("checkpoint head mismatch")
    accounts = snapshot["accounts"]
    if any(type(v) is not int or v < 0 for v in accounts.values()):
        raise ValueError("invalid balance")
    if sum(accounts.values()) != SUPPLY or snapshot["total_cents"] != SUPPLY:
        raise ValueError("money conservation failed")
    if events and events[-1]["state_hash"] != digest(accounts, sort_keys=True):
        raise ValueError("checkpoint balances do not match the final audit event")
    return {"verified_slots": len(events), "audit_head": snapshot["audit_head"], "total_cents": SUPPLY}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("export", type=Path)
    args = parser.parse_args()
    try:
        print(json.dumps(verify(json.loads(args.export.read_text())), indent=2))
    except (ValueError, KeyError, TypeError) as exc:
        parser.exit(1, f"verification failed: {exc}\n")


if __name__ == "__main__":
    main()
