from __future__ import annotations

import asyncio
import random
from typing import Iterator

def next_backoff(prev: float, cap: float) -> float:
    """Exponential backoff progression with cap (no jitter)."""
    return min(prev * 2.0, cap)

def jitter(v: float, *, ratio: float = 0.2) -> float:
    """
    Add ±ratio jitter. ratio=0.2 -> multiply by [0.8, 1.2].
    """
    lo = 1.0 - ratio
    hi = 1.0 + ratio
    return v * (lo + (hi - lo) * random.random())

async def sleep_with_jitter(base: float, *, ratio: float = 0.2) -> None:
    """async sleep(base with jitter)."""
    await asyncio.sleep(jitter(base, ratio=ratio))

def backoff_iter(initial: float = 0.25, cap: float = 30.0) -> Iterator[float]:
    """
    Deterministic (no jitter) iterator of backoff values:
    0.25, 0.5, 1, 2, 4, ... (capped).
    """
    v = initial
    while True:
        yield v
        v = next_backoff(v, cap)
