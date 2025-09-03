from __future__ import annotations
import asyncio
from dataclasses import dataclass

@dataclass(slots=True)
class QueueStats:
    enq_ok: int = 0
    enq_drop: int = 0
    deq_ok: int = 0

class NotifyQueue:
    """
    Bounded, non-blocking queue wrapper for notifications.
    - try_put(evt) drops on full and increments a counter
    - get() awaits like a normal queue
    """
    def __init__(self, maxsize: int = 2000):
        self._q: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self.stats = QueueStats()

    def try_put(self, evt) -> bool:
        try:
            self._q.put_nowait(evt)
            self.stats.enq_ok += 1
            return True
        except asyncio.QueueFull:
            self.stats.enq_drop += 1
            return False

    async def get(self):
        item = await self._q.get()
        self.stats.deq_ok += 1
        return item

    def qsize(self) -> int:
        return self._q.qsize()
