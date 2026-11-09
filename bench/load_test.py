"""Concurrent-connection load test for the SSE gateway.

Starts the gateway in a separate process (uvicorn, 1 worker, stdlib asyncio + h11),
then opens N raw-socket SSE clients from this process. Each client holds its stream
open for the full generation, so "concurrent" means sockets streaming at the same time.

Usage:
    python3 bench/load_test.py --levels 100 1000 2000 5000 --tokens 100 --delay-ms 100

Writes results/load_test.json. Every number in it is measured on the machine that ran it.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import resource
import socket
import statistics
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    k = min(len(xs) - 1, max(0, round(p / 100 * (len(xs) - 1))))
    return round(xs[k], 2)


def get_json(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())


def rss_mb(pid: int) -> float:
    out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)]).decode().strip()
    return round(int(out) / 1024, 1)


async def client(port, path, start_evt, stats, live, peak):
    t0 = time.perf_counter()
    try:
        r, w = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), 30)
    except Exception as e:
        stats["connect_errors"] += 1
        stats["errors"].setdefault(type(e).__name__, 0)
        stats["errors"][type(e).__name__] += 1
        return
    w.write(f"GET {path} HTTP/1.1\r\nHost: bench\r\nAccept: text/event-stream\r\n\r\n".encode())
    await w.drain()
    tokens = 0
    first = None
    status = None
    counted = False
    try:
        line = await asyncio.wait_for(r.readline(), 60)
        status = int(line.split()[1]) if line else None
        if status != 200:
            stats["non_200"] += 1
            return
        live[0] += 1
        counted = True
        peak[0] = max(peak[0], live[0])
        while True:
            line = await asyncio.wait_for(r.readline(), 60)
            if not line:
                stats["early_eof"] += 1
                return
            if line.startswith(b"event: token"):
                tokens += 1
                if first is None:
                    first = time.perf_counter()
                    stats["ttft_ms"].append((first - t0) * 1000)
            elif line.startswith(b"event: done"):
                stats["completed"] += 1
                stats["duration_s"].append(time.perf_counter() - t0)
                return
    except Exception as e:
        stats["stream_errors"] += 1
        stats["errors"].setdefault(type(e).__name__, 0)
        stats["errors"][type(e).__name__] += 1
    finally:
        stats["tokens"] += tokens
        if counted:
            live[0] -= 1
        w.close()


async def run_level(port, n, tokens, delay_ms, ramp_per_s, server_pid):
    stats = {
        "connect_errors": 0,
        "non_200": 0,
        "early_eof": 0,
        "stream_errors": 0,
        "completed": 0,
        "tokens": 0,
        "ttft_ms": [],
        "duration_s": [],
        "errors": {},
    }
    live, peak = [0], [0]
    path = f"/v1/stream?prompt=load+test&max_tokens={tokens}&delay_ms={delay_ms}"
    base = get_json(f"http://127.0.0.1:{port}/metrics")
    t0 = time.perf_counter()
    tasks = []
    batch = max(1, ramp_per_s // 20)
    peak_rss = rss_mb(server_pid)
    for i in range(0, n, batch):
        for _ in range(min(batch, n - i)):
            tasks.append(asyncio.create_task(client(port, path, None, stats, live, peak)))
        await asyncio.sleep(batch / ramp_per_s)
    while any(not t.done() for t in tasks):
        await asyncio.sleep(0.25)
        try:
            peak_rss = max(peak_rss, rss_mb(server_pid))
        except Exception:
            pass
    wall = time.perf_counter() - t0
    after = get_json(f"http://127.0.0.1:{port}/metrics")
    return {
        "target_connections": n,
        "completed_streams": stats["completed"],
        "success_rate": round(stats["completed"] / n, 4),
        "peak_concurrent_client_side": peak[0],
        "server_peak_active_cumulative": after["peak_active"],
        "connect_errors": stats["connect_errors"],
        "non_200": stats["non_200"],
        "early_eof": stats["early_eof"],
        "stream_errors": stats["stream_errors"],
        "error_types": stats["errors"],
        "tokens_received": stats["tokens"],
        "tokens_per_s": round(stats["tokens"] / wall, 1),
        "ttft_ms_p50": pct(stats["ttft_ms"], 50),
        "ttft_ms_p95": pct(stats["ttft_ms"], 95),
        "ttft_ms_p99": pct(stats["ttft_ms"], 99),
        "stream_duration_s_p50": pct(stats["duration_s"], 50),
        "stream_duration_s_p99": pct(stats["duration_s"], 99),
        "ideal_stream_duration_s": round(tokens * delay_ms / 1000, 2),
        "wall_s": round(wall, 2),
        "server_peak_rss_mb": peak_rss,
        "server_slow_client_aborts": after["slow_client_aborts"] - base["slow_client_aborts"],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--levels", type=int, nargs="+", default=[100, 1000, 2000, 5000])
    ap.add_argument("--tokens", type=int, default=100)
    ap.add_argument("--delay-ms", type=float, default=100)
    ap.add_argument("--ramp-per-s", type=int, default=1000)
    ap.add_argument("--out", default=str(ROOT / "results" / "load_test.json"))
    args = ap.parse_args()

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    want = min(hard, 65536)
    resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))

    port = free_port()
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    srv = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import resource,uvicorn;"
            f"resource.setrlimit(resource.RLIMIT_NOFILE,({want},{hard}));"
            "from sse_gateway.app import create_app, GatewayConfig;"
            "uvicorn.run(create_app(GatewayConfig(max_streams=20000, send_timeout_s=10)),"
            f"host='127.0.0.1', port={port}, log_level='warning', backlog=4096)",
        ],
        env=env,
    )
    try:
        for _ in range(100):
            try:
                get_json(f"http://127.0.0.1:{port}/healthz")
                break
            except Exception:
                time.sleep(0.1)
        results = []
        for n in args.levels:
            print(f"level {n} ...", flush=True)
            res = asyncio.run(run_level(port, n, args.tokens, args.delay_ms, args.ramp_per_s, srv.pid))
            print(json.dumps(res), flush=True)
            results.append(res)
            time.sleep(2)
        out = {
            "kind": "MEASURED",
            "date": time.strftime("%Y-%m-%d"),
            "machine": {
                "model": subprocess.check_output(["sysctl", "-n", "hw.model"]).decode().strip()
                if sys.platform == "darwin"
                else platform.machine(),
                "cpus": os.cpu_count(),
                "os": platform.platform(),
                "python": platform.python_version(),
            },
            "server": "uvicorn 1 worker, stdlib asyncio loop, h11 parser, same host as client",
            "client": "single-process asyncio raw-socket SSE client (bench/load_test.py)",
            "workload": {
                "tokens_per_stream": args.tokens,
                "inter_token_delay_ms": args.delay_ms,
                "ramp_connections_per_s": args.ramp_per_s,
            },
            "levels": results,
        }
        Path(args.out).write_text(json.dumps(out, indent=2) + "\n")
        print("wrote", args.out)
    finally:
        srv.terminate()
        srv.wait(10)


if __name__ == "__main__":
    main()
