# src/monitor/alerts/evaluator_redis.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Optional, Mapping, Any

from redis.asyncio import Redis

from monitor.alerts.rules import PercentMoveRule
from monitor.alerts.state import SymbolAlertState
from monitor.alerts.dedup import TTLDeduper
from storage.redis_metrics import key  # ts:{SYM}:{TF}:{FIELD}
from monitor.utils.types import AlertEvent


@dataclass(slots=True)
class RedisEvaluatorConfig:
    rule: PercentMoveRule = field(default_factory=PercentMoveRule)
    timeframe: str = "30s"   # must match what you write in Redis (we use "30s")
    field: str = "C"         # we compare closes


class PercentMoveEvaluatorRedis:
    """
    Consumes (symbol, epoch) bar events for timing, but reads prices from RedisTimeSeries.
    """
    def __init__(
        self,
        *,
        q_bars: asyncio.Queue,       # (symbol, epoch) from BarAggregator
        q_alerts: asyncio.Queue,     # AlertEvent out
        redis: Redis,
        cfg: Optional[RedisEvaluatorConfig] = None,
    ):
        self.q_bars = q_bars
        self.q_alerts = q_alerts
        self.redis = redis
        self.cfg = cfg or RedisEvaluatorConfig()
        self.rule = self.cfg.rule

        self._states: dict[str, SymbolAlertState] = {}
        self._dedupe = TTLDeduper(ttl_s=self.rule.dedupe_bucket_seconds, max_size=50_000)
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="alerts-pctmove-redis")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass

    # --------------- internals ---------------

    def _state_for(self, symbol: str) -> SymbolAlertState:
        st = self._states.get(symbol)
        if st is None:
            st = SymbolAlertState()
            self._states[symbol] = st
        return st

    async def _loop(self) -> None:
        try:
            while not self._stop.is_set():
                symbol, epoch_s = await self.q_bars.get()
                await self._eval_symbol(symbol, epoch_s)
        except asyncio.CancelledError:
            return

    async def _eval_symbol(self, symbol: str, epoch_s: int) -> None:
        """
        Compute percent move over last window using Redis TS:
          - last_close: TS.GET ts:{sym}:{tf}:C
          - ref_close: first sample with ts >= (now - window) via TS.RANGE COUNT 1
        """
        ts = self.redis.ts()
        k = key(symbol, self.cfg.field, self.cfg.timeframe)

        # RTS is ms; our epoch is seconds.
        last_ms = epoch_s * 1000
        from_ms = (epoch_s - self.rule.window_seconds) * 1000

        # 1) Last close (current bar’s close) — TS.GET is fine (we just wrote it)
        last = await ts.get(k)
        if not last:
            return
        # last = (timestamp, value)
        last_close = float(last[1])
        if last_close <= 0.0:
            return

        # 2) Reference close: first sample with ts >= from_ms
        # TS.RANGE key from_ms + COUNT 1
        # (+ means +inf)
        ref_range = await ts.range(k, from_ms, "+", count=1)
        if not ref_range:
            # Not enough history yet; skip
            return
        ref_ms, ref_close = ref_range[0]
        ref_close = float(ref_close)
        if ref_close <= 0.0:
            return

        pct = (last_close - ref_close) / ref_close

        # Threshold logic
        hit = (
            (self.rule.direction == "abs"  and abs(pct) >= self.rule.threshold) or
            (self.rule.direction == "up"   and pct      >= self.rule.threshold) or
            (self.rule.direction == "down" and pct      <= -self.rule.threshold)
        )

        st = self._state_for(symbol).ensure_rule(self.rule.name)
        st.last_value = float(pct)

        # Cooldown
        if st.cooldown_until is not None and epoch_s < st.cooldown_until:
            return

        if hit:
            bucket = (epoch_s // self.rule.dedupe_bucket_seconds) * self.rule.dedupe_bucket_seconds
            sign = "up" if pct >= 0 else "down"
            dkey = f"{symbol}:{self.rule.name}:{sign}:{bucket}"
            if self._dedupe.seen_recently(dkey):
                return
            self._dedupe.mark(dkey)

            st.active = True
            st.last_trigger_epoch = epoch_s
            st.cooldown_until = epoch_s + self.rule.cooldown_seconds

            arrow = "↑" if pct >= 0 else "↓"
            minutes = (last_ms - ref_ms) // 60000
            msg = (f"{symbol} {arrow} {pct*100:.2f}% over ~{minutes}m "
                   f"(C {ref_close:.2f} → {last_close:.2f})")

            evt: AlertEvent = {
                "symbol": symbol,
                "rule": self.rule.name,
                "type": "start",
                "value": round(pct, 6),
                "ts": float(epoch_s),
                "message": msg,
                "dedupe_key": dkey,
            }
            await self._emit(evt)
        else:
            if st.active and self.rule.auto_resolve:
                st.active = False
                msg = f"{symbol} normalized (move now {pct*100:.2f}%)."
                evt: AlertEvent = {
                    "symbol": symbol,
                    "rule": self.rule.name,
                    "type": "resolve",
                    "value": round(pct, 6),
                    "ts": float(epoch_s),
                    "message": msg,
                }
                await self._emit(evt)

    async def _emit(self, evt: Mapping[str, Any]) -> None:
        try:
            self.q_alerts.put_nowait(evt)
        except asyncio.QueueFull:
            pass
