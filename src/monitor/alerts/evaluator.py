from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Optional

from monitor.alerts.rules import PriceDropRule
from monitor.alerts.state import SymbolAlertState
from monitor.alerts.dedup import TTLDeduper
from monitor.data.ring_buffer import RingBufferOHLCV
from monitor.utils.types import AlertEvent

@dataclass(slots=True)
class EvaluatorConfig:
        rule: PriceDropRule = field(default_factory=PriceDropRule)


class PriceDropEvaluator:
    """
    Evaluates: price drop over last window_seconds >= drop_threshold.
    Inputs:
      - q_bars:     asyncio.Queue[(symbol:str, epoch:int)]
      - get_ring:   Callable[[symbol], RingBufferOHLCV]  (from BarAggregator)
      - q_alerts:   asyncio.Queue[AlertEvent]            (output events)
    """
    def __init__(
        self,
        q_bars: asyncio.Queue,
        get_ring: Callable[[str], RingBufferOHLCV],
        q_alerts: asyncio.Queue,
        cfg: Optional[EvaluatorConfig] = None,
    ):
        self.q_bars = q_bars
        self.get_ring = get_ring
        self.q_alerts = q_alerts
        self.cfg = cfg or EvaluatorConfig()
        self.rule = self.cfg.rule
        self._states: dict[str, SymbolAlertState] = {}
        # dedupe by (symbol, rule, bucketed_epoch)
        self._dedupe = TTLDeduper(ttl_s=self.rule.dedupe_bucket_seconds, max_size=50_000)
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        self._task = asyncio.create_task(self._loop(), name="alerts-evaluator")

    async def stop(self):
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass

    async def _loop(self):
        try:
            while not self._stop.is_set():
                symbol, epoch = await self.q_bars.get()
                self._eval_symbol(symbol, epoch)
        except asyncio.CancelledError:
            return

    # --- core evaluation ---

    def _state_for(self, symbol: str) -> SymbolAlertState:
        st = self._states.get(symbol)
        if st is None:
            st = SymbolAlertState()
            self._states[symbol] = st
        return st

    def _eval_symbol(self, symbol: str, epoch: int) -> None:
        """
        Compute drop over window and emit AlertEvents on threshold crossing,
        honoring cooldown and dedupe.
        """
        ring = self.get_ring(symbol)
        if ring.size == 0:
            return

        # find last close
        v = ring.view_last(1)
        last_close = None
        for seg in v.slices:
            last_close = float(seg[4][-1])  # c slice
        if last_close is None or last_close <= 0.0:
            return

        # find reference close roughly window_seconds ago:
        window_secs = self.rule.window_seconds
        # We want “oldest in window”: last_epoch - window_secs.
        # We'll walk slices to find the first bar with epoch >= (last_epoch - window_secs).
        vwin = ring.view_last(window_secs + 1)  # +1 safe guard
        ref_close = None
        ref_epoch_cut = None

        # gather epochs and closes in time order
        epochs: list[int] = []
        closes: list[float] = []
        for seg in vwin.slices:
            epochs.extend(seg[0].tolist())
            closes.extend(seg[4].tolist())

        if not epochs:
            return

        last_epoch = epochs[-1]
        cut_epoch = last_epoch - window_secs
        ref_close = closes[0]      # fallback: oldest if we haven't filled 2h yet
        ref_epoch_cut = epochs[0]
        # find first epoch >= cut_epoch
        for e, c in zip(epochs, closes):
            if e >= cut_epoch:
                ref_close = float(c)
                ref_epoch_cut = int(e)
                break

        if ref_close <= 0.0:
            return

        drop = (ref_close - last_close) / ref_close
        st = self._state_for(symbol).ensure_rule(self.rule.name)
        st.last_value = float(drop)

        # cooldown check
        if st.cooldown_until is not None and epoch < st.cooldown_until:
            return

        # threshold check
        if drop >= self.rule.drop_threshold:
            # dedupe bucket key
            bucket = (epoch // self.rule.dedupe_bucket_seconds) * self.rule.dedupe_bucket_seconds
            dkey = f"{symbol}:{self.rule.name}:{bucket}"
            if self._dedupe.seen_recently(dkey):
                return

            self._dedupe.mark(dkey)
            st.active = True
            st.last_trigger_epoch = epoch
            st.cooldown_until = epoch + self.rule.cooldown_seconds

            evt: AlertEvent = {
                "symbol": symbol,
                "rule": self.rule.name,
                "type": "start",
                "value": round(drop, 6),
                "ts": float(epoch),
                "message": f"{symbol} dropped {drop*100:.2f}% over ~{self.rule.window_seconds//60}h "
                           f"(from ~{ref_epoch_cut} to {epoch})",
                "dedupe_key": dkey,
            }
            self._emit(evt)
        else:
            # condition cleared
            if st.active and self.rule.auto_resolve:
                st.active = False
                evt: AlertEvent = {
                    "symbol": symbol,
                    "rule": self.rule.name,
                    "type": "resolve",
                    "value": round(drop, 6),
                    "ts": float(epoch),
                    "message": f"{symbol} recovered (drop now {drop*100:.2f}%).",
                }
                self._emit(evt)

    def _emit(self, evt: AlertEvent) -> None:
        try:
            self.q_alerts.put_nowait(evt)
        except asyncio.QueueFull:
            # drop alerts if downstream is blocked; consider logging
            pass
