# src/monitor/data/bars.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Dict, Optional

from monitor.utils.time import utc_now_s
from monitor.data.ring_buffer import RingBufferOHLCV, Bar as RingBar


@dataclass(slots=True)
class BarConfig:
    buffer_seconds: int = 7200          # how much history (in wall-clock seconds) to retain
    bar_seconds:     int = 1            # bar width: 1 = 1s bars, 10 = 10s bars, 60 = 1m bars, ...
    emit_gap_fill:   bool = True        # fill missing buckets with flat bars (O=H=L=C=prev_close, V=0)
    idle_finalize_ms:int = 250          # finalize open bucket if idle for this long


@dataclass(slots=True)
class _Acc:
    epoch: Optional[int] = None   # bucket start (aligned to bar_seconds)
    o: float = 0.0
    h: float = 0.0
    l: float = 0.0
    c: float = 0.0
    v: float = 0.0
    last_update_ts: float = 0.0   # last tick timestamp seen (float seconds)


class BarAggregator:
    """
    Consumes ticks and builds finalized OHLCV bars per symbol with configurable period.
    - Buckets ticks by `bar_seconds` (e.g., 1s, 10s, 60s)
    - Writes finalized bars into per-symbol RingBufferOHLCV
    - Optionally emits (symbol, epoch) to q_bars for downstream engines
    """
    def __init__(
        self,
        symbols: list[str],
        buffer_seconds: int,
        q_ticks: asyncio.Queue,
        q_bars: Optional[asyncio.Queue] = None,
        cfg: Optional[BarConfig] = None,
    ):
        # allow passing buffer_seconds for convenience; cfg can override bar_seconds, etc.
        self.cfg = cfg or BarConfig(buffer_seconds=buffer_seconds)

        if self.cfg.bar_seconds <= 0:
            raise ValueError("bar_seconds must be >= 1")

        self.q_ticks = q_ticks
        self.q_bars = q_bars

        # Ring capacity is measured in *bars*, not seconds.
        self._cap_bars = max(1, int(self.cfg.buffer_seconds // self.cfg.bar_seconds))

        self.rings: Dict[str, RingBufferOHLCV] = {
            s: RingBufferOHLCV(self._cap_bars) for s in symbols
        }
        self.acc: Dict[str, _Acc] = {s: _Acc() for s in symbols}

        self._stop = asyncio.Event()

    def get_ring(self, symbol: str) -> RingBufferOHLCV:
        if symbol not in self.rings:
            self.rings[symbol] = RingBufferOHLCV(self._cap_bars)
            self.acc[symbol] = _Acc()
        return self.rings[symbol]

    async def start(self) -> None:
        idle_check = asyncio.create_task(self._idle_finalizer_loop())
        try:
            while not self._stop.is_set():
                tick = await self.q_ticks.get()
                # Expect tick has attributes: symbol, px, size, ts (float seconds)
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
        # Align the tick to the start of the current bar bucket.
        bs = self.cfg.bar_seconds
        epoch = (int(ts) // bs) * bs

        a = self.acc.setdefault(symbol, _Acc())

        if a.epoch is None:
            # first tick for this symbol
            self._start_new(symbol, epoch, px, size, ts)
            return

        if epoch < a.epoch:
            # late/old tick relative to current bucket; policy: drop
            return

        if epoch == a.epoch:
            # same bucket: update accumulator
            a.c = px
            if px > a.h:
                a.h = px
            if px < a.l:
                a.l = px
            a.v += size
            a.last_update_ts = ts
            return

        # bucket advanced: finalize the previous bucket
        self._finalize(symbol, a.epoch, a.o, a.h, a.l, a.c, a.v)

        # gap fill for any missing buckets between a.epoch and new epoch
        if self.cfg.emit_gap_fill and a.c > 0.0:
            missing = a.epoch + bs
            while missing < epoch:
                # flat bar at previous close, zero volume
                self._finalize(symbol, missing, a.c, a.c, a.c, a.c, 0.0)
                missing += bs

        # start accumulator for this new bucket
        self._start_new(symbol, epoch, px, size, ts)

    def _start_new(self, symbol: str, epoch: int, px: float, size: float, ts: float) -> None:
        a = self.acc[symbol]
        a.epoch = epoch
        a.o = px
        a.h = px
        a.l = px
        a.c = px
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

        bs = self.cfg.bar_seconds
        now = utc_now_s()
        now_epoch = (int(now) // bs) * bs  # align "now" to current bucket start

        if not force:
            # Don't close while we're still inside the current bucket.
            if now_epoch <= a.epoch:
                return
            # Be sure we've actually been idle long enough.
            if (now - a.last_update_ts) * 1000.0 < self.cfg.idle_finalize_ms:
                return

        # 1) finalize the current (just-finished) bucket
        self._finalize(symbol, a.epoch, a.o, a.h, a.l, a.c, a.v)

        # Choose a prior close to seed empty bars/new bucket.
        prev_close = a.c if a.c > 0.0 else (a.o if a.o > 0.0 else 0.0)

        # 2) emit *all* missing empty buckets up to now_epoch (exclusive), stepping by bar size
        if self.cfg.emit_gap_fill and prev_close > 0.0:
            t = a.epoch + bs
            while t < now_epoch:
                self._finalize(symbol, t, prev_close, prev_close, prev_close, prev_close, 0.0)
                t += bs

        # 3) open a new (empty) accumulator for the *current* bucket
        a.epoch = now_epoch
        if prev_close > 0.0:
            a.o = a.h = a.l = a.c = prev_close
        else:
            # no prior price yet; leave zeroed until first tick arrives
            a.o = a.h = a.l = a.c = 0.0
        a.v = 0.0
        a.last_update_ts = now


    async def _idle_finalizer_loop(self) -> None:
        """
        Periodically check for idle buckets and roll them forward.
        Cadence: a few times per bar (but not too chatty).
        """
        # e.g., bar_seconds=30 -> interval ~0.5s; bar_seconds=1 -> 0.2s
        interval = max(0.2, min(0.5, self.cfg.bar_seconds / 60.0))
        try:
            while not self._stop.is_set():
                await asyncio.sleep(interval)
                for s in list(self.acc.keys()):
                    self._finalize_if_open(s, force=False)
        except asyncio.CancelledError:
            return
