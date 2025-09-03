from __future__ import annotations

import asyncio
from typing import Optional

try:
    import redis.asyncio as redis
except Exception:
    redis = None  # make optional

class RedisMirror:
    """
    Optional RedisTimeSeries mirror. Non-blocking best-effort writes.
    """
    def __init__(self, url: str, retention_ms: int = 2 * 60 * 60 * 1000, enabled: bool = False):
        self.enabled = enabled and (redis is not None)
        self.url = url
        self.retention_ms = retention_ms
        self._r: Optional[redis.Redis] = None
        self._q: asyncio.Queue = asyncio.Queue(maxsize=5000)
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        if not self.enabled:
            return
        self._r = redis.from_url(self.url, decode_responses=True)
        self._task = asyncio.create_task(self._writer_loop(), name="redis-mirror")

    async def stop(self):
        if not self.enabled:
            return
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass
        if self._r:
            await self._r.close()

    def write_bar(self, symbol: str, epoch: int, close: float, volume: float) -> None:
        """
        Enqueue a best-effort write (TS.ADD) for close and volume.
        """
        if not self.enabled:
            return
        try:
            self._q.put_nowait((symbol, epoch, close, volume))
        except asyncio.QueueFull:
            # drop under pressure
            pass

    async def _writer_loop(self):
        assert self._r is not None
        r = self._r
        try:
            while True:
                symbol, epoch, close, volume = await self._q.get()
                key_px = f"ts:{symbol}:close"
                key_v  = f"ts:{symbol}:vol"
                p = r.pipeline()
                # create-if-missing with retention
                p.execute_command("TS.CREATE", key_px, "RETENTION", self.retention_ms, "ON_DUPLICATE", "LAST", "LABELS", "symbol", symbol, "metric", "close")
                p.execute_command("TS.CREATE", key_v,  "RETENTION", self.retention_ms, "ON_DUPLICATE", "LAST", "LABELS", "symbol", symbol, "metric", "volume")
                # add points (epoch in ms)
                ts_ms = epoch * 1000
                p.execute_command("TS.ADD", key_px, ts_ms, close)
                p.execute_command("TS.ADD", key_v,  ts_ms, volume)
                try:
                    await p.execute()
                except Exception:
                    # ignore intermittent errors; it's a mirror
                    pass
        except asyncio.CancelledError:
            return
