from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import Dict, Optional

from monitor.utils.time import utc_now_s
from monitor.data.ring_buffer import RingBufferOHLCV, Bar as RingBar


@dataclass(slots=True)
class BarConfig:
    """
    Configuration for bar aggregation.

    buffer_seconds: how much history (in wall-clock seconds) to retain
    bar_seconds:    bar width: 1 = 1s bars, 10 = 10s bars, 60 = 1m bars, ...
    emit_gap_fill:  fill missing buckets with flat bars (O=H=L=C=prev_close, V=0)
    idle_finalize_ms: finalize open bucket if idle for this long (no ticks)
    """
    buffer_seconds: int = 7200
    bar_seconds: int = 1
    emit_gap_fill: bool = True
    idle_finalize_ms: int = 250


@dataclass(slots=True)
class _Acc:
    epoch: Optional[int] = None   # bucket start (aligned to bar_seconds, UNIX seconds)
    o: float = 0.0
    h: float = 0.0
    l: float = 0.0
    c: float = 0.0
    v: float = 0.0
    last_update_ts: float = 0.0   # last tick timestamp seen (float seconds)


class BarAggregator:
    """
    Consumes ticks and builds finalized OHLCV bars per symbol with configurable period.

    Key behavior:
    - Ticks are bucketed by `bar_seconds` (e.g., 1s, 10s, 60s).
    - Finalized bars are written into a per-symbol RingBufferOHLCV.
    - Optionally emits (symbol, epoch) to q_bars for downstream engines.
    - **Clock-driven roll** guarantees all symbols finalize exactly at boundaries
      (e.g., :00/:30 for 30-second bars), avoiding per-symbol skew from tick timing.
    - Idle finalizer remains to close buckets that have gone quiet mid-interval.

    Expected tick fields: tick.symbol, tick.px, tick.size, tick.ts (float UNIX seconds)
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

    # -------------------------------------------------------------------------
    # lifecycle
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """
        Main loop: consumes ticks and updates accumulators.
        Also runs:
          - _idle_finalizer_loop: closes idle buckets mid-interval if needed
          - _clock_roll_loop:     rolls ALL symbols at exact boundaries
        """
        idle_check = asyncio.create_task(self._idle_finalizer_loop(), name="bars-idle-finalizer")
        clock_roll = asyncio.create_task(self._clock_roll_loop(), name="bars-clock-roll")
        try:
            while not self._stop.is_set():
                tick = await self.q_ticks.get()
                # Expect tick has attributes: symbol, px, size, ts (float seconds)
                self._on_tick(tick.symbol, float(tick.px), float(tick.size), float(tick.ts))
        finally:
            self._stop.set()
            # Cancel helpers and flush
            for task in (idle_check, clock_roll):
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            # Final flush of any open bars
            for s in list(self.acc.keys()):
                self._finalize_if_open(s, force=True)

    async def stop(self) -> None:
        self._stop.set()

    # -------------------------------------------------------------------------
    # core tick handling
    # -------------------------------------------------------------------------

    def _on_tick(self, symbol: str, px: float, size: float, ts: float) -> None:
        """
        Update per-symbol accumulator with a new tick.
        Buckets by aligned epoch = floor(ts / bar_seconds) * bar_seconds
        """
        bs = self.cfg.bar_seconds
        # Align to bucket start (UNIX seconds)
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

    # -------------------------------------------------------------------------
    # finalization helpers
    # -------------------------------------------------------------------------

    def _finalize_if_open(self, symbol: str, force: bool = False) -> None:
        """
        Finalize a symbol's current bucket if it's past the boundary and either:
          - we've been idle long enough, or
          - force=True (shutdown flush)
        Also fills gaps and opens the new current bucket seeded with prev close.
        """
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

    # -------------------------------------------------------------------------
    # background loops
    # -------------------------------------------------------------------------

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

    async def _clock_roll_loop(self) -> None:
        """
        At each bar boundary, roll ALL symbols forward so bar epochs are synchronized.
        This ensures finalized bars line up across symbols (e.g., exactly :00/:30 for 30s bars).
        """
        bs = self.cfg.bar_seconds
        try:
            while not self._stop.is_set():
                now = utc_now_s()
                # Next boundary time (UNIX seconds aligned to bar size)
                next_boundary = (math.floor(now / bs) + 1) * bs
                # Sleep until just after the boundary to avoid race with arriving ticks
                await asyncio.sleep(max(0.0, next_boundary - now + 0.01))
                self._roll_all_to_epoch(int(next_boundary))
        except asyncio.CancelledError:
            return

    def _roll_all_to_epoch(self, now_epoch: int) -> None:
        """
        Finalize and gap-fill all symbols whose current bucket is *before* now_epoch.
        Then open a fresh bucket at now_epoch seeded with previous close.
        """
        bs = self.cfg.bar_seconds
        for symbol, a in self.acc.items():
            if a.epoch is None:
                # No data yet; initialize at now_epoch so we align future ticks
                a.epoch = now_epoch
                a.o = a.h = a.l = a.c = 0.0
                a.v = 0.0
                a.last_update_ts = float(now_epoch)
                continue

            if a.epoch >= now_epoch:
                # Already rolled for this boundary (or beyond, which shouldn't happen)
                continue

            # 1) finalize the just-finished bucket
            self._finalize(symbol, a.epoch, a.o, a.h, a.l, a.c, a.v)

            prev_close = a.c if a.c > 0.0 else (a.o if a.o > 0.0 else 0.0)

            # 2) fill any missing buckets up to (now_epoch - bs)
            if self.cfg.emit_gap_fill and prev_close > 0.0:
                t = a.epoch + bs
                while t < now_epoch:
                    self._finalize(symbol, t, prev_close, prev_close, prev_close, prev_close, 0.0)
                    t += bs

            # 3) open the new current bucket at now_epoch seeded with prev_close
            a.epoch = now_epoch
            if prev_close > 0.0:
                a.o = a.h = a.l = a.c = prev_close
            else:
                a.o = a.h = a.l = a.c = 0.0
            a.v = 0.0
            a.last_update_ts = float(now_epoch)