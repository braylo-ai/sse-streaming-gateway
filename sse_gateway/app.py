"""FastAPI SSE gateway with admission control, bounded queues and slow-client aborts."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.responses import Response
from starlette.types import Receive, Scope, Send

from .mock_llm import mock_llm_stream
from .session import Metrics, StreamSession


@dataclass
class GatewayConfig:
    max_streams: int = 10_000
    queue_size: int = 32
    send_timeout_s: float = 2.0
    heartbeat_s: float = 15.0
    max_tokens_cap: int = 4096


class SSEResponse(Response):
    """Raw ASGI response so each body write can be timed out and the stream aborted."""

    def __init__(self, session: StreamSession, metrics: Metrics):
        super().__init__(content=None, status_code=200, media_type="text/event-stream")
        self.session = session
        self.metrics = metrics

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        m = self.metrics
        disconnected = asyncio.Event()

        async def watch() -> None:
            while True:
                msg = await receive()
                if msg["type"] == "http.disconnect":
                    disconnected.set()
                    await self.session.cancel("client_disconnect")
                    return

        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"text/event-stream"),
                    (b"cache-control", b"no-cache"),
                    (b"x-accel-buffering", b"no"),
                ],
            }
        )

        async def write(chunk: bytes) -> None:
            if disconnected.is_set():
                raise OSError("client disconnected")
            await send({"type": "http.response.body", "body": chunk, "more_body": True})

        watcher = asyncio.create_task(watch())
        reason = "stop"
        try:
            reason = await self.session.pump(write)
            if self.session.reason == "client_disconnect" or disconnected.is_set():
                m.client_disconnects += 1
                return
            if reason == "slow_client":
                # Return without completing the body: the server closes the socket.
                return
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            m.completed += 1
        finally:
            watcher.cancel()
            m.active -= 1
            m.max_queue_depth_seen = max(m.max_queue_depth_seen, self.session.max_depth)


class ChatRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=20_000)
    max_tokens: int = Field(default=64, ge=1)
    delay_ms: float = Field(default=10.0, ge=0, le=5000)
    ttft_ms: float = Field(default=0.0, ge=0, le=60_000)
    fail_after: int | None = Field(default=None, ge=0)
    pad: int = Field(default=0, ge=0, le=65_536)


def create_app(config: GatewayConfig | None = None) -> FastAPI:
    cfg = config or GatewayConfig()
    app = FastAPI(title="sse-streaming-gateway")
    metrics = Metrics()
    app.state.metrics = metrics
    app.state.config = cfg

    def admit(req: ChatRequest):
        if metrics.active >= cfg.max_streams:
            metrics.rejected += 1
            return JSONResponse(
                {"error": "over_capacity", "active": metrics.active},
                status_code=503,
                headers={"retry-after": "1"},
            )
        metrics.active += 1
        metrics.started += 1
        metrics.peak_active = max(metrics.peak_active, metrics.active)
        upstream = mock_llm_stream(
            req.prompt,
            max_tokens=min(req.max_tokens, cfg.max_tokens_cap),
            delay_s=req.delay_ms / 1000,
            ttft_s=req.ttft_ms / 1000,
            fail_after=req.fail_after,
            pad=req.pad,
        )
        session = StreamSession(
            upstream,
            queue_size=cfg.queue_size,
            send_timeout_s=cfg.send_timeout_s,
            heartbeat_s=cfg.heartbeat_s,
            metrics=metrics,
        )
        return SSEResponse(session, metrics)

    @app.post("/v1/chat/stream")
    async def chat_stream(req: ChatRequest):
        return admit(req)

    @app.get("/v1/stream")
    async def stream_get(
        prompt: str = Query("hello world", min_length=1, max_length=20_000),
        max_tokens: int = Query(64, ge=1),
        delay_ms: float = Query(10.0, ge=0, le=5000),
        ttft_ms: float = Query(0.0, ge=0, le=60_000),
        fail_after: int | None = Query(None, ge=0),
        pad: int = Query(0, ge=0, le=65_536),
    ):
        return admit(
            ChatRequest(
                prompt=prompt,
                max_tokens=max_tokens,
                delay_ms=delay_ms,
                ttft_ms=ttft_ms,
                fail_after=fail_after,
                pad=pad,
            )
        )

    @app.get("/metrics")
    async def get_metrics():
        return metrics.as_dict()

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    return app


app = create_app()
