#!/usr/bin/env python3
"""Run a local cluster, a destructive isolated failover demo, or a workload.

Demo and benchmark always allocate fresh private artifact directories and ports.
The run command preserves the configured data directories across invocations.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from audit import verify

ROOT = Path(__file__).resolve().parents[1]


def http(url, path, body=None, timeout=3):
    payload = None if body is None else json.dumps(body).encode()
    req = Request(url + path, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=timeout) as response:
            raw = response.read().decode()
            return response.status, json.loads(raw) if raw.startswith("{") else raw
    except HTTPError as exc:
        return exc.code, json.loads(exc.read())


def wait_for(action, timeout=30, description="condition"):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = action()
            if result:
                return result
        except (OSError, URLError, TimeoutError, ValueError) as exc:
            last = exc
        time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for {description}: {last}")


class Cluster:
    def __init__(self, folder, config=None):
        self.folder = Path(folder)
        self.folder.mkdir(parents=True, exist_ok=True)
        self.processes = {}
        self.logs = {}
        if config:
            self.config_path = Path(config).resolve()
            self.config = json.loads(self.config_path.read_text())
        else:
            # Hold all ephemeral sockets until the complete port set is known.
            sockets = [socket.socket() for _ in range(3)]
            try:
                for sock in sockets:
                    sock.bind(("127.0.0.1", 0))
                nodes = {f"n{i+1}": f"http://127.0.0.1:{sock.getsockname()[1]}" for i, sock in enumerate(sockets)}
            finally:
                for sock in sockets:
                    sock.close()
            self.config = {
                "cluster_id": "experiment-" + secrets.token_hex(8),
                "internal_token": secrets.token_hex(24), "nodes": nodes,
                "heartbeat_ms": 100, "rpc_timeout_ms": 250, "election_min_ms": 1200,
            }
            self.config_path = self.folder / "cluster.json"
            self.config_path.write_text(json.dumps(self.config, indent=2) + "\n")
        self.urls = self.config["nodes"]

    def start(self, member):
        if self.processes.get(member) and self.processes[member].poll() is None:
            return
        if member in self.logs:
            self.logs[member].close()
        self.logs[member] = (self.folder / f"{member}.log").open("ab")
        self.processes[member] = subprocess.Popen(
            [str(ROOT / "bin/ledgerd"), "-id", member, "-config", str(self.config_path),
             "-data", str(self.folder / member)], cwd=ROOT,
            stdout=self.logs[member], stderr=subprocess.STDOUT,
        )
        def healthy():
            proc = self.processes[member]
            if proc.poll() is not None:
                raise RuntimeError(f"{member} exited; see {self.folder / (member + '.log')}")
            code, body = http(self.urls[member], "/healthz")
            return code == 200 and body["healthy"]
        wait_for(healthy, description=f"{member} health")

    def start_all(self):
        for member in self.urls:
            self.start(member)

    def stop(self, member, *, kill=False):
        proc = self.processes.get(member)
        if proc and proc.poll() is None:
            proc.kill() if kill else proc.terminate()
            try:
                proc.wait(timeout=12)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)

    def close(self):
        for member in self.urls:
            self.stop(member)
        for handle in self.logs.values():
            handle.close()

    def find_leader(self):
        for member, url in self.urls.items():
            if not self.processes.get(member) or self.processes[member].poll() is not None:
                continue
            try:
                code, status = http(url, "/status", timeout=1)
                if code == 200 and status["leader"]:
                    return member
            except (OSError, URLError, TimeoutError):
                pass
        return None

    def leader(self):
        return wait_for(self.find_leader, description="a leader")

    def request(self, path, body=None, *, allow_rejection=False):
        def attempt():
            member = self.find_leader()
            if not member:
                return None
            code, result = http(self.urls[member], path, body)
            if code in (409, 503):
                return None
            if code == 200 or (allow_rejection and code == 422):
                return result
            raise RuntimeError(f"HTTP {code}: {result}")
        return wait_for(attempt, timeout=40, description=path)

    def submit(self, command, *, allow_rejection=False):
        return self.request("/v1/transactions", command, allow_rejection=allow_rejection)

    def converge(self):
        def check():
            states = [http(url, "/v1/local-state")[1] for url in self.urls.values()]
            return states[0] if all(state == states[0] for state in states) else None
        return wait_for(check, description="identical replica states and audit heads")

    def export_audit(self):
        first = self.request("/v1/audit?after=0&limit=1000")
        target, events = first["snapshot"]["applied_slot"], first["events"]
        while len(events) < target:
            page = self.request(f"/v1/audit?after={len(events)}&limit=1000")
            events.extend(event for event in page["events"] if event["slot"] <= target)
        return {"snapshot": first["snapshot"], "events": events}


def payment(txid, source, target, cents):
    return {"id": txid, "kind": "transfer", "transfers": [{"from": source, "to": target, "amount_cents": cents}]}


def bootstrap(cluster):
    for name in ("alice", "bob", "carol"):
        cluster.submit({"id": "open-" + name, "kind": "open", "account": name})
    cluster.submit(payment("fund-alice", "treasury", "alice", 100_000))


def demo(cluster, report):
    checks = report["checks"]
    def passed(name, detail=None):
        checks.append({"name": name, "passed": True, "detail": detail})
        print(f"PASS {name}", flush=True)
    cluster.start_all()
    original = cluster.leader()
    passed("automatic leader election", original)
    bootstrap(cluster)
    batch = {"id": "payroll-001", "kind": "batch", "transfers": [
        {"from": "alice", "to": "bob", "amount_cents": 25_000},
        {"from": "alice", "to": "carol", "amount_cents": 10_000},
    ]}
    receipt = cluster.submit(batch)
    assert receipt["ok"]
    assert cluster.submit(batch) == receipt
    passed("atomic batch and stable retry receipt", receipt)
    denied = cluster.submit(payment("overdraft", "alice", "bob", 999_999), allow_rejection=True)
    assert denied["code"] == "insufficient_funds" and not denied["ok"]
    passed("overdraft rejected")
    cluster.converge()
    started = time.monotonic()
    cluster.stop(original, kill=True)
    successor = cluster.leader()
    assert successor != original
    result = cluster.submit(payment("after-failover", "bob", "carol", 5000))
    assert result["ok"]
    passed("leader SIGKILL and successful successor write", {"leader": successor, "recovery_seconds": time.monotonic() - started})
    cluster.start(original)
    cluster.converge()
    passed("restarted replica catches up")
    survivor = cluster.leader()
    others = [member for member in cluster.urls if member != survivor]
    for member in others:
        cluster.stop(member, kill=True)
    ambiguous = payment("during-outage", "alice", "bob", 100)
    code, _ = http(cluster.urls[survivor], "/v1/transactions", ambiguous)
    assert code in (409, 503), f"minority acknowledged HTTP {code}"
    code, _ = http(cluster.urls[survivor], "/v1/state")
    assert code in (409, 503), f"minority served a strong read: HTTP {code}"
    passed("minority refuses writes and strong reads")
    for member in others:
        cluster.start(member)
    assert cluster.submit(ambiguous)["ok"]
    assert cluster.submit(batch) == receipt
    state = cluster.converge()
    assert {a: state["accounts"][a] for a in ("alice", "bob", "carol")} == {"alice": 64_900, "bob": 20_100, "carol": 15_000}
    passed("majority restoration and safe retry of unknown outcome")
    for member in cluster.urls:
        cluster.stop(member, kill=True)
    cluster.start_all()
    assert cluster.submit(batch) == receipt
    state = cluster.converge()
    assert state["accounts"]["alice"] == 64_900 and state["total_cents"] == 1_000_000_000_000
    passed("full cluster restart preserves balances and receipts")
    export = cluster.export_audit()
    (cluster.folder / "audit.json").write_text(json.dumps(export, indent=2) + "\n")
    verified = verify(export)
    passed("independent audit hash and checkpoint verification", verified)
    state = cluster.converge()
    leader = cluster.leader()
    _, metrics = http(cluster.urls[leader], "/metrics")
    (cluster.folder / "metrics.prom").write_text(metrics)
    report["final_state"] = state


def benchmark(cluster, report, count, concurrency):
    cluster.start_all()
    cluster.leader()
    cluster.submit({"id": "open-bench", "kind": "open", "account": "bench"})
    assert cluster.submit(payment("fund-bench", "treasury", "bench", count + 1000))["ok"]
    def transfer(index):
        started = time.monotonic()
        result = cluster.submit(payment(f"bench-{index}", "bench", "treasury", 1))
        if not result["ok"]:
            raise RuntimeError(result)
        return (time.monotonic() - started) * 1000
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        timings = sorted(pool.map(transfer, range(count)))
    elapsed = time.monotonic() - started
    state = cluster.converge()
    assert state["accounts"]["bench"] == 1000
    def percentile(q):
        return timings[max(0, math.ceil(q * len(timings)) - 1)]
    report["measurements"] = {
        "transactions": count, "concurrency": concurrency, "elapsed_seconds": elapsed,
        "transactions_per_second": count / elapsed,
        "latency_ms": {"p50": percentile(.50), "p95": percentile(.95), "p99": percentile(.99)},
        "scope": "client observed, includes leader discovery and retries; single host; fsync enabled",
    }
    report["checks"] = [{"name": "all requested transfers applied and replicas converge", "passed": True}]
    print(json.dumps(report["measurements"], indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("run", "demo", "benchmark"))
    parser.add_argument("--config", type=Path, default=ROOT / "config/local.json")
    parser.add_argument("--transactions", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()
    if args.transactions < 1 or args.transactions > 1_000_000 or args.concurrency < 1 or args.concurrency > 64:
        parser.error("transactions must be 1–1,000,000; concurrency must be 1–64")
    if not (ROOT / "bin/ledgerd").exists():
        parser.exit(1, "Build first: make build (Go 1.23+ is required).\n")
    if args.mode == "run":
        cluster = Cluster(ROOT / "data", args.config)
        def stop(signum, frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, stop)
        try:
            cluster.start_all()
            print("Cluster ready. Leader:", cluster.leader(), "— Ctrl+C stops all members.", flush=True)
            while True:
                time.sleep(1)
                for member, proc in cluster.processes.items():
                    if proc.poll() is not None:
                        raise RuntimeError(f"{member} exited; see data/{member}.log")
        except KeyboardInterrupt:
            pass
        finally:
            cluster.close()
        return 0
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    folder = ROOT / "artifacts" / f"{args.mode}-{timestamp}-{secrets.token_hex(3)}"
    cluster = None
    report = {"mode": args.mode, "started_at": timestamp, "passed": False, "phase": "startup", "checks": []}
    try:
        cluster = Cluster(folder)
        report["phase"] = "scenarios" if args.mode == "demo" else "workload"
        if args.mode == "demo":
            demo(cluster, report)
        else:
            benchmark(cluster, report, args.transactions, args.concurrency)
        report["passed"] = True
        report["phase"] = "complete"
    except (Exception, KeyboardInterrupt) as exc:
        report["error"] = repr(exc)
        print(f"FAILED: {exc}", file=sys.stderr)
    finally:
        if cluster is not None:
            cluster.close()
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print("Artifacts:", folder)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
