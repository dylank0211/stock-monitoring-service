from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Dict, Optional

from monitor.utils.time import floor_to_second, utc_now_s
from monitor.data.ring_buffer import RingBufferOHLCV, Bar as RingBar

@dataclass(slots=True)
class BarConfig:
    buffer_seconds: int = 7200
    emit_gap_fill: bool = True       # fill missing seconds with flat bars
    idle_finalize_ms: int = 250      # finalize open bar if idle for this long

@dataclass(slots=True)
class _Acc:
    epoch: Optional[int] = None
    o: float = 0.0
    h: float = 0.0
    l: float = 0.0
    c: float = 0.0
    v: float = 0.0
    last_update_ts: float = 0.0

class BarAggregator:
    """
    Consumes ticks and builds finalized 1s bars per symbol.
    - Writes finalized bars into per-symbol RingBufferOHLCV
    - Optionally emits (symbol, epoch) to q_bars for downstream indicator engine
    """
    def __init__(
        self,
        symbols: list[str],
        buffer_seconds: int,
        q_ticks: asyncio.Queue,
        q_bars: Optional[asyncio.Queue] = None,
        cfg: Optional[BarConfig] = None,
    ):
        self.cfg = cfg or BarConfig(buffer_seconds=buffer_seconds)
        self.q_ticks = q_ticks
        self.q_bars = q_bars
        self.rings: Dict[str, RingBufferOHLCV] = {s: RingBufferOHLCV(self.cfg.buffer_seconds) for s in symbols}
        self.acc: Dict[str, _Acc] = {s: _Acc() for s in symbols}
        self._stop = asyncio.Event()

    def get_ring(self, symbol: str) -> RingBufferOHLCV:
        if symbol not in self.rings:
            self.rings[symbol] = RingBufferOHLCV(self.cfg.buffer_seconds)
            self.acc[symbol] = _Acc()
        return self.rings[symbol]

    async def start(self) -> None:
        idle_check = asyncio.create_task(self._idle_finalizer_loop())
        try:
            while not self._stop.is_set():
                tick = await self.q_ticks.get()
                self._on_tick(tick.symbol, float(tick.px), float(tick.size), float(tick.ts))
        finally:
            self._stop.set()
            idle_check.cancel()
            # flush any open bars
            for s in list(self.acc.keys()):
                self._finalize_if_open(s, force=True)

    async def stop(self) -> None:
        self._stop.set()

    # ---- core ----

    def _on_tick(self, symbol: str, px: float, size: float, ts: float) -> None:
        epoch = int(ts)  # floor to second, ticks already normalized upstream
        a = self.acc.setdefault(symbol, _Acc())
        if a.epoch is None:
            # first tick for this symbol
            self._start_new(symbol, epoch, px, size, ts)
            return

        if epoch < a.epoch:
            # late tick; ignore (policy: drop)
            return

        if epoch == a.epoch:
            # same second: update accumulator
            a.c = px
            a.h = px if px > a.h else a.h
            a.l = px if px < a.l else a.l
            a.v += size
            a.last_update_ts = ts
            return

        # epoch advanced: finalize previous second and fill any gaps
        self._finalize(symbol, a.epoch, a.o, a.h, a.l, a.c, a.v)

        # gap fill between a.epoch+1 .. epoch-1
        if self.cfg.emit_gap_fill and a.c > 0.0:
            for sec in range(a.epoch + 1, epoch):
                self._finalize(symbol, sec, a.c, a.c, a.c, a.c, 0.0)

        # start accumulator for this tick's second
        self._start_new(symbol, epoch, px, size, ts)

    def _start_new(self, symbol: str, epoch: int, px: float, size: float, ts: float) -> None:
        a = self.acc[symbol]
        a.epoch = epoch
        a.o = a.h = a.l = a.c = px
        a.v = size
        a.last_update_ts = ts

    def _finalize(self, symbol: str, epoch: int, o: float, h: float, l: float, c: float, v: float) -> None:
        rb = self.get_ring(symbol)
        rb.append(RingBar(epoch=epoch, o=o, h=h, l=l, c=c, v=v))
        if self.q_bars:
            try:
                self.q_bars.put_nowait((symbol, epoch))
            except asyncio.QueueFull:
                # drop: downstream is slow; keep hot path non-blocking
                pass

    def _finalize_if_open(self, symbol: str, force: bool = False) -> None:
        a = self.acc.get(symbol)
        if not a or a.epoch is None:
            return
        # if idle for configured time or force at shutdown
        if force or (utc_now_s() - a.last_update_ts) * 1000.0 >= self.cfg.idle_finalize_ms:
            self._finalize(symbol, a.epoch, a.o, a.h, a.l, a.c, a.v)
            # reset epoch so next tick starts a new bar
            self.acc[symbol] = _Acc()

    async def _idle_finalizer_loop(self) -> None:
        try:
            while not self._stop.is_set():
                await asyncio.sleep(self.cfg.idle_finalize_ms / 1000.0)
                for s in list(self.acc.keys()):
                    self._finalize_if_open(s, force=False)
        except asyncio.CancelledError:
            return
