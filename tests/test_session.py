"""Unit tests for StreamSession (no HTTP)."""
import asyncio
import json

from sse_gateway.mock_llm import UpstreamError, mock_llm_stream
from sse_gateway.session import HEARTBEAT, Metrics, StreamSession, sse_event


def run(coro):
    return asyncio.run(coro)


def parse(chunks):
    out = []
    for c in chunks:
        if c == HEARTBEAT:
            out.append(("ping", None))
            continue
        lines = c.decode().strip().split("\n")
        ev = [l[7:] for l in lines if l.startswith("event: ")][0]
        data = json.loads([l[6:] for l in lines if l.startswith("data: ")][0])
        out.append((ev, data))
    return out


def test_sse_event_format():
    b = sse_event("token", {"t": "hi"}, 3)
    assert b == b'id: 3\nevent: token\ndata: {"t":"hi"}\n\n'


def test_mock_llm_is_deterministic_and_counts():
    async def go():
        return [t async for t in mock_llm_stream("a b c", max_tokens=5, delay_s=0)]

    toks = run(go())
    assert len(toks) == 5
    assert toks[:3] == ["a ", "b ", "c "]
    assert toks == run(go())


def test_mock_llm_fail_after():
    async def go():
        got = []
        try:
            async for t in mock_llm_stream("x", max_tokens=10, delay_s=0, fail_after=3):
                got.append(t)
        except UpstreamError:
            return got, True
        return got, False

    got, failed = run(go())
    assert failed and len(got) == 3


def test_full_stream_emits_all_tokens_then_done():
    async def go():
        s = StreamSession(mock_llm_stream("one two", max_tokens=8, delay_s=0))
        sent = []

        async def send(b):
            sent.append(b)

        reason = await s.pump(send)
        return reason, parse(sent), s

    reason, events, s = run(go())
    assert reason == "stop"
    assert [e for e, _ in events].count("token") == 8
    assert events[-1] == ("done", {"tokens": 8, "reason": "stop"})
    assert s.tokens_sent == 8 and s.upstream_closed.is_set()


def test_queue_is_bounded_and_upstream_pauses_when_consumer_stalls():
    """With nobody draining, the producer must stop at queue_size, not run ahead."""

    async def go():
        s = StreamSession(mock_llm_stream("x", max_tokens=1000, delay_s=0), queue_size=8)
        s.start()
        await asyncio.sleep(0.2)
        produced = s.tokens_produced
        depth = s.queue.qsize()
        await s.cancel("test")
        return produced, depth

    produced, depth = run(go())
    assert depth == 8
    # producer is blocked on the 9th put: it has pulled at most queue_size+1 tokens
    assert produced <= 9


def test_slow_client_is_aborted_and_upstream_closed():
    async def go():
        m = Metrics()
        s = StreamSession(
            mock_llm_stream("x", max_tokens=1000, delay_s=0),
            queue_size=4,
            send_timeout_s=0.1,
            metrics=m,
        )
        calls = 0

        async def send(b):
            nonlocal calls
            calls += 1
            if calls > 3:
                await asyncio.sleep(10)  # client stops reading

        reason = await s.pump(send)
        return reason, m, s

    reason, m, s = run(go())
    assert reason == "slow_client"
    assert m.slow_client_aborts == 1
    assert s.upstream_closed.is_set()
    assert s.tokens_produced < 20


def test_cancel_during_ttft_stops_immediately():
    async def go():
        s = StreamSession(mock_llm_stream("x", max_tokens=5, ttft_s=30))
        sent = []

        async def send(b):
            sent.append(b)

        task = asyncio.create_task(s.pump(send))
        await asyncio.sleep(0.05)
        t0 = asyncio.get_running_loop().time()
        await s.cancel("client_disconnect")
        reason = await asyncio.wait_for(task, 1)
        return reason, asyncio.get_running_loop().time() - t0, sent, s

    reason, dt, sent, s = run(go())
    assert reason == "client_disconnect"
    assert dt < 0.5 and sent == [] and s.upstream_closed.is_set()


def test_upstream_error_becomes_error_event():
    async def go():
        m = Metrics()
        s = StreamSession(mock_llm_stream("x", max_tokens=10, delay_s=0, fail_after=2), metrics=m)
        sent = []

        async def send(b):
            sent.append(b)

        reason = await s.pump(send)
        return reason, parse(sent), m

    reason, events, m = run(go())
    assert reason == "upstream_error"
    assert events[-1][0] == "error" and events[-1][1]["tokens"] == 2
    assert m.upstream_errors == 1


def test_heartbeat_emitted_while_upstream_is_silent():
    async def go():
        s = StreamSession(mock_llm_stream("x", max_tokens=1, ttft_s=0.35), heartbeat_s=0.1)
        sent = []

        async def send(b):
            sent.append(b)

        await s.pump(send)
        return sent

    sent = run(go())
    assert sent.count(HEARTBEAT) >= 2
    assert parse(sent)[-1][0] == "done"


def test_send_oserror_is_treated_as_disconnect():
    async def go():
        s = StreamSession(mock_llm_stream("x", max_tokens=100, delay_s=0))
        n = 0

        async def send(b):
            nonlocal n
            n += 1
            if n == 2:
                raise ConnectionResetError("peer gone")

        return await s.pump(send), s

    reason, s = run(go())
    assert reason == "client_disconnect" and s.upstream_closed.is_set()
