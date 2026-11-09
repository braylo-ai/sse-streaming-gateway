"""Per-connection stream session: bounded queue between upstream and client.

Backpressure model
------------------
upstream --(producer task)--> asyncio.Queue(maxsize=N) --(consumer)--> ASGI send

* The producer awaits `queue.put`. When the client reads slowly the queue fills and the
  producer blocks, so the upstream iterator is no longer pulled: backpressure reaches
  the model call instead of growing memory.
* The consumer wraps each `send` in a timeout. If one write blocks longer than
  `send_timeout_s` the client is classed as slow, the session is aborted and the
  upstream is cancelled.
* Client disconnect or server shutdown calls `cancel()`, which cancels the producer
  (and so closes the upstream generator) immediately.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import AsyncIterator, Awaitable, Callable

_END = object()


@dataclass
class Metrics:
    active: int = 0
    peak_active: int = 0
    started: int = 0
    completed: int = 0
    client_disconnects: int = 0
    slow_client_aborts: int = 0
    upstream_errors: int = 0
    rejected: int = 0
    tokens_sent: int = 0
    max_queue_depth_seen: int = 0

    def as_dict(self) -> dict:
        return dict(self.__dict__)


def sse_event(event: str, data: dict, event_id: int | None = None) -> bytes:
    lines = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    lines.append("data: " + json.dumps(data, separators=(",", ":")))
    return ("\n".join(lines) + "\n\n").encode()


HEARTBEAT = b": ping\n\n"


@dataclass
class StreamSession:
    upstream: AsyncIterator[str]
    queue_size: int = 32
    send_timeout_s: float = 2.0
    heartbeat_s: float = 15.0
    metrics: Metrics = field(default_factory=Metrics)

    def __post_init__(self) -> None:
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=self.queue_size)
        self.reason: str | None = None
        self.tokens_produced = 0
        self.tokens_sent = 0
        self.max_depth = 0
        self.cancelled = False
        self._producer: asyncio.Task | None = None
        self.upstream_closed = asyncio.Event()

    # ---------- producer ----------
    def start(self) -> None:
        if self._producer is None:
            self._producer = asyncio.create_task(self._produce())

    async def _produce(self) -> None:
        try:
            async for tok in self.upstream:
                await self.queue.put(("token", tok))  # blocks when full -> backpressure
                self.tokens_produced += 1
                depth = self.queue.qsize()
                if depth > self.max_depth:
                    self.max_depth = depth
            await self.queue.put(("done", None))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # upstream failure mid-stream
            self.metrics.upstream_errors += 1
            self.reason = "upstream_error"
            await self.queue.put(("error", str(exc)))
        finally:
            aclose = getattr(self.upstream, "aclose", None)
            if aclose is not None:
                try:
                    await aclose()
                except Exception:
                    pass
            self.upstream_closed.set()

    async def cancel(self, reason: str) -> None:
        if self.cancelled:
            return
        self.cancelled = True
        if self.reason is None:
            self.reason = reason
        if self._producer is not None and not self._producer.done():
            self._producer.cancel()
            try:
                await self._producer
            except (asyncio.CancelledError, Exception):
                pass
        else:
            self.upstream_closed.set()
        try:
            self.queue.put_nowait(("end", _END))
        except asyncio.QueueFull:
            pass

    # ---------- consumer ----------
    async def events(self) -> AsyncIterator[bytes]:
        """Yield SSE-encoded chunks until done, error or cancel."""
        self.start()
        n = 0
        while True:
            try:
                kind, payload = await asyncio.wait_for(
                    self.queue.get(), self.heartbeat_s
                )
            except asyncio.TimeoutError:
                if self.cancelled:
                    return
                yield HEARTBEAT
                continue
            if self.cancelled or kind == "end":
                return
            if kind == "token":
                n += 1
                yield sse_event("token", {"t": payload}, n)
            elif kind == "done":
                self.reason = self.reason or "stop"
                yield sse_event("done", {"tokens": n, "reason": "stop"})
                return
            elif kind == "error":
                yield sse_event("error", {"message": payload, "tokens": n})
                return

    async def pump(self, send: Callable[[bytes], Awaitable[None]]) -> str:
        """Drive the session into `send`. Returns the final reason.

        `send` is the raw byte writer. A write that takes longer than send_timeout_s
        aborts the stream as a slow client.
        """
        try:
            async for chunk in self.events():
                try:
                    await asyncio.wait_for(send(chunk), self.send_timeout_s)
                except asyncio.TimeoutError:
                    self.metrics.slow_client_aborts += 1
                    await self.cancel("slow_client")
                    return "slow_client"
                except OSError:
                    await self.cancel("client_disconnect")
                    return "client_disconnect"
                if chunk.startswith(b"id:"):
                    self.tokens_sent += 1
                    self.metrics.tokens_sent += 1
                if self.cancelled:
                    return self.reason or "cancelled"
            return self.reason or "stop"
        finally:
            await self.cancel(self.reason or "finished")
