#!/usr/bin/env python3
"""Offline checks for configurations, examples, and the audit verifier.

These checks do not compile Go or establish consensus correctness.
"""
import copy
import json
from pathlib import Path
import py_compile
import tempfile

from audit import SUPPLY, digest, verify

ROOT = Path(__file__).resolve().parents[1]


def main():
    files = [
        path for path in ROOT.rglob("*.json")
        if not any(part in ("artifacts", "data") for part in path.relative_to(ROOT).parts)
    ]
    for path in files:
        json.loads(path.read_text())
    with tempfile.TemporaryDirectory() as directory:
        for script in (ROOT / "scripts").glob("*.py"):
            py_compile.compile(str(script), cfile=str(Path(directory) / (script.stem + ".pyc")), doraise=True)
    for path in (ROOT / "config/local.json", ROOT / "config/docker.json"):
        config = json.loads(path.read_text())
        assert len(config["nodes"]) == 3 and len(set(config["nodes"].values())) == 3
        assert config["election_min_ms"] >= 3 * (config["heartbeat_ms"] + config["rpc_timeout_ms"])
    balances = {"treasury": SUPPLY}
    event = {"slot": 1, "command": {"kind": "noop"}, "code": "noop", "previous": "genesis", "state_hash": digest(balances, sort_keys=True), "hash": ""}
    event["hash"] = digest(event)
    export = {"events": [event], "snapshot": {"applied_slot": 1, "accounts": balances, "total_cents": SUPPLY, "audit_head": event["hash"]}}
    assert verify(export)["verified_slots"] == 1
    tampered = copy.deepcopy(export)
    tampered["events"][0]["code"] = "applied"
    try:
        verify(tampered)
    except ValueError:
        pass
    else:
        raise AssertionError("tampering was not detected")
    print(f"PASS: JSON assets, Python syntax, election timing, audit checkpoint and tamper detection ({len(files)} JSON files scanned).")


if __name__ == "__main__":
    main()
