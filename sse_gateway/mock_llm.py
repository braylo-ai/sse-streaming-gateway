"""Mock LLM upstream: an async token generator with configurable latency and failures.

No network, no model. Tokens are deterministic words derived from the prompt so tests
can assert on content.
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator

_FILLER = (
    "the gateway streams each token as a server sent event while bounded queues "
    "keep memory flat and slow clients are cut off before they stall upstream"
).split()


class UpstreamError(RuntimeError):
    """Raised by the mock upstream to simulate a provider failure mid-stream."""


async def mock_llm_stream(
    prompt: str,
    max_tokens: int = 32,
    delay_s: float = 0.01,
    ttft_s: float = 0.0,
    fail_after: int | None = None,
    pad: int = 0,
) -> AsyncIterator[str]:
    """Yield `max_tokens` tokens, sleeping `delay_s` between them.

    ttft_s: extra delay before the first token (simulates prefill).
    fail_after: raise UpstreamError after this many tokens.
    pad: append this many '.' chars to each token (inflates bytes for backpressure tests).
    """
    words = prompt.split() or ["hello"]
    if ttft_s:
        await asyncio.sleep(ttft_s)
    for i in range(max_tokens):
        if fail_after is not None and i >= fail_after:
            raise UpstreamError(f"mock upstream failed after {i} tokens")
        if i and delay_s:
            await asyncio.sleep(delay_s)
        src = words if i < len(words) else _FILLER
        tok = src[i % len(src)]
        yield (tok + ("." * pad)) + " "
