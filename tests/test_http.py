"""Integration tests against a real uvicorn server on a random local port."""
import asyncio
import json
import socket
import time

import httpx

from conftest import wait_for


def collect(url, **params):
    events = []
    with httpx.stream("GET", url + "/v1/stream", params=params, timeout=10) as r:
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        for line in r.iter_lines():
            if line.startswith("event: "):
                events.append([line[7:], None])
            elif line.startswith("data: "):
                events[-1][1] = json.loads(line[6:])
    return events


def test_get_stream_end_to_end(live_server):
    with live_server() as srv:
        ev = collect(srv.url, prompt="alpha beta", max_tokens=12, delay_ms=1)
        assert [e for e, _ in ev].count("token") == 12
        assert ev[0][1]["t"] == "alpha "
        assert ev[-1] == ["done", {"tokens": 12, "reason": "stop"}]
        assert wait_for(lambda: srv.metrics.completed == 1 and srv.metrics.active == 0)


def test_post_stream_end_to_end(live_server):
    with live_server() as srv:
        with httpx.stream(
            "POST", srv.url + "/v1/chat/stream", json={"prompt": "hi", "max_tokens": 5, "delay_ms": 0}
        ) as r:
            body = r.read().decode()
        assert body.count("event: token") == 5 and "event: done" in body


def test_validation_rejects_bad_input(live_server):
    with live_server() as srv:
        r = httpx.post(srv.url + "/v1/chat/stream", json={"prompt": "", "max_tokens": 5})
        assert r.status_code == 422
        r = httpx.get(srv.url + "/v1/stream", params={"max_tokens": 0})
        assert r.status_code == 422


def test_admission_control_returns_503_over_capacity(live_server):
    with live_server(max_streams=2) as srv:
        holders = [
            httpx.stream("GET", srv.url + "/v1/stream", params={"max_tokens": 50, "delay_ms": 100})
            for _ in range(2)
        ]
        opened = [h.__enter__() for h in holders]
        try:
            assert all(o.status_code == 200 for o in opened)
            assert wait_for(lambda: srv.metrics.active == 2)
            r = httpx.get(srv.url + "/v1/stream", params={"max_tokens": 1})
            assert r.status_code == 503 and r.headers["retry-after"] == "1"
            assert srv.metrics.rejected == 1
        finally:
            for h in holders:
                h.__exit__(None, None, None)
        assert wait_for(lambda: srv.metrics.active == 0)


def test_client_disconnect_cancels_upstream(live_server):
    with live_server() as srv:
        with httpx.stream("GET", srv.url + "/v1/stream", params={"max_tokens": 10_000, "delay_ms": 5}) as r:
            it = r.iter_lines()
            for _ in range(6):
                next(it)
        # leaving the context closes the socket mid-stream
        assert wait_for(lambda: srv.metrics.client_disconnects == 1 and srv.metrics.active == 0)
        assert srv.metrics.completed == 0
        assert srv.metrics.tokens_sent < 10_000


def test_slow_client_is_cut_off_and_memory_stays_bounded(live_server):
    """Raw socket that sends a request and never reads. Big padded tokens fill the
    kernel buffers, then the server's send blocks, the write times out, and the
    socket is closed by the server."""
    with live_server(queue_size=8, send_timeout_s=0.3) as srv:
        s = socket.create_connection(("127.0.0.1", srv.port))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        s.sendall(
            b"GET /v1/stream?max_tokens=100000&delay_ms=0&pad=16000 HTTP/1.1\r\nHost: x\r\n\r\n"
        )
        try:
            assert wait_for(lambda: srv.metrics.slow_client_aborts == 1, timeout=15)
            assert wait_for(lambda: srv.metrics.active == 0)
            assert srv.metrics.max_queue_depth_seen <= 8
            assert srv.metrics.tokens_sent < 100_000
        finally:
            s.close()


def test_concurrent_streams_are_isolated(live_server):
    async def one(client, i):
        toks = 0
        async with client.stream(
            "GET", "/v1/stream", params={"prompt": f"p{i}", "max_tokens": 20, "delay_ms": 2}
        ) as r:
            first = None
            async for line in r.aiter_lines():
                if line.startswith("data: ") and '"t"' in line:
                    toks += 1
                    first = first or json.loads(line[6:])["t"]
        return toks, first

    async def go(url):
        async with httpx.AsyncClient(base_url=url, timeout=20) as c:
            return await asyncio.gather(*(one(c, i) for i in range(50)))

    with live_server() as srv:
        res = asyncio.run(go(srv.url))
        assert all(t == 20 for t, _ in res)
        assert [f for _, f in res] == [f"p{i} " for i in range(50)]
        assert wait_for(lambda: srv.metrics.completed == 50)
        assert srv.metrics.peak_active >= 2


def test_metrics_endpoint(live_server):
    with live_server() as srv:
        collect(srv.url, max_tokens=3, delay_ms=0)
        assert wait_for(lambda: srv.metrics.completed == 1)
        m = httpx.get(srv.url + "/metrics").json()
        assert m["completed"] == 1 and m["tokens_sent"] == 3 and m["active"] == 0
