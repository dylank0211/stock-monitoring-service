# src/monitor/alerts/evaluator_redis.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Optional, Literal, Tuple, List, Dict

from redis.asyncio import Redis
from monitor.alerts.rules import PercentMoveRule
from monitor.alerts.dedup import TTLDeduper

Direction = Literal["abs", "up", "down"]


@dataclass(slots=True)
class RedisEvaluatorConfig:
    rule: PercentMoveRule = field(default_factory=PercentMoveRule)
    timeframe: str = "30s"
    field: str = "C"
    min_points: int = 2
    pad_seconds: int = 5

    # NEW: require an extra step beyond the last alert before re-alerting
    step_realert_pct: float = 0.0020  # 0.20% additional extension required

    # NEW: optional per-direction cooldown overrides (seconds)
    cooldown_up_seconds: Optional[int] = None
    cooldown_down_seconds: Optional[int] = None

class PercentMoveEvaluatorRedis:
    """
    Extremum-based percent move evaluator using RedisTimeSeries data.

    For each finalized bar event (symbol, epoch):
      1) Reads close prices from Redis TS in [epoch - window_seconds - pad, epoch].
      2) Computes the move vs rolling extremum within that window:
          - direction="up"   -> compare to window LOW   ( (last - low)/low )
          - direction="down" -> compare to window HIGH  ( (last - high)/high )  -> negative
          - direction="abs"  -> pick the larger |move| between the above two.
      3) Applies cooldown and TTL-based dedupe.
      4) Emits an AlertEvent dict to q_alerts with extra fields for pretty output:
         {
            symbol, rule, type, ts,
            direction, pct, window_seconds,
            curr_price, curr_epoch,
            ref_price, ref_epoch, ref_kind
         }

    Expected Redis TS key: ts:{SYMBOL}:{timeframe}:{FIELD}
    (Matches storage/redis_metrics.py -> write_bar_bundle())
    """

    def __init__(
        self,
        *,
        q_bars: asyncio.Queue,          # (symbol:str, epoch:int)
        q_alerts: asyncio.Queue,        # AlertEvent dict
        redis: Redis,
        cfg: Optional[RedisEvaluatorConfig] = None,
    ):
        self.q_bars = q_bars
        self.q_alerts = q_alerts
        self.redis = redis
        self.cfg = cfg or RedisEvaluatorConfig()
        self.rule = self.cfg.rule

        # per-symbol rule state (cooldown / last value)
        self._cooldown_until: dict[Tuple[str, str], int] = {}  # (sym, rule) -> epoch
        self._last_value: dict[Tuple[str, str], float] = {}    # last computed pct (for observability)
        self._last_fired_pct: Dict[Tuple[str, str, str], float] = {}

        # TTL dedupe (symbol:rule:bucket)
        self._dedupe = TTLDeduper(ttl_s=self.rule.dedupe_bucket_seconds, max_size=50_000)

        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name=f"evaluator-redis-{self.rule.name}")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass

    async def _loop(self) -> None:
        try:
            while not self._stop.is_set():
                symbol, epoch = await self.q_bars.get()
                await self._eval_symbol(symbol, epoch)
        except asyncio.CancelledError:
            return

    # ---------- core helpers ----------

    def _key(self, symbol: str) -> str:
        # Close price time series (matches write_bar_bundle() naming)
        # ts:{SYM}:{TF}:{FIELD}
        return f"ts:{symbol}:{self.cfg.timeframe}:{self.cfg.field.upper()}"

    async def _fetch_window_closes(self, symbol: str, epoch: int) -> List[Tuple[int, float]]:
        """
        Fetch closes in [epoch - window_seconds - pad, epoch] (milliseconds) from RedisTimeSeries.
        Returns list of (ts_ms, value_float) ascending by timestamp.
        """
        start_ms = max(0, (epoch - self.rule.window_seconds - self.cfg.pad_seconds) * 1000)
        end_ms = epoch * 1000
        key = self._key(symbol)

        # TS.RANGE returns: [[ts, value], [ts, value], ...] or None/empty if no data
        data = await self.redis.execute_command("TS.RANGE", key, start_ms, end_ms)
        if not data:
            return []
        out: List[Tuple[int, float]] = []
        for ts, val in data:
            try:
                v = float(val if not isinstance(val, (bytes, bytearray)) else val.decode("utf-8"))
                t = int(ts)
                out.append((t, v))
            except Exception:
                # skip malformed points
                continue
        return out

    # ---------- evaluation ----------

    async def _eval_symbol(self, symbol: str, epoch: int) -> None:
        # Pull closes in the window
        points = await self._fetch_window_closes(symbol, epoch)
        if len(points) < self.cfg.min_points:
            return

        # Latest close at end of window
        last_ts_ms, last_close = points[-1]
        if last_close <= 0.0:
            return

        # Rolling extremums (based on closes)
        closes = [v for _, v in points]
        try:
            min_idx = min(range(len(closes)), key=lambda i: closes[i])
            max_idx = max(range(len(closes)), key=lambda i: closes[i])
        except ValueError:
            return  # safety

        w_min = closes[min_idx]
        w_max = closes[max_idx]
        if w_min <= 0.0 or w_max <= 0.0:
            return

        w_min_ts_ms = points[min_idx][0]
        w_max_ts_ms = points[max_idx][0]

        # Compute pct vs window LOW/HIGH
        up_pct = (last_close - w_min) / w_min
        down_pct = (last_close - w_max) / w_max  # <= 0 typically

        direction: Direction = self.rule.direction
        eff_direction: Direction = direction
        if direction == "up":
            pct = up_pct
            ref_price = w_min
            ref_epoch = w_min_ts_ms // 1000
            ref_kind = f"{self.rule.window_seconds // 60}m LOW"
        elif direction == "down":
            pct = down_pct
            ref_price = w_max
            ref_epoch = w_max_ts_ms // 1000
            ref_kind = f"{self.rule.window_seconds // 60}m HIGH"
        else:  # "abs" -> pick the stronger magnitude
            if abs(down_pct) >= abs(up_pct):
                pct = down_pct
                eff_direction = "down"
                ref_price = w_max
                ref_epoch = w_max_ts_ms // 1000
                ref_kind = f"{self.rule.window_seconds // 60}m HIGH"
            else:
                pct = up_pct
                eff_direction = "up"
                ref_price = w_min
                ref_epoch = w_min_ts_ms // 1000
                ref_kind = f"{self.rule.window_seconds // 60}m LOW"

        # Record latest computed pct (observability)
        self._last_value[(symbol, self.rule.name)] = float(pct)

        # Direction-aware cooldown (fallback to rule.cooldown_seconds)
        dir_cd = self.rule.cooldown_seconds
        if eff_direction == "up" and self.cfg.cooldown_up_seconds is not None:
            dir_cd = self.cfg.cooldown_up_seconds
        if eff_direction == "down" and self.cfg.cooldown_down_seconds is not None:
            dir_cd = self.cfg.cooldown_down_seconds

        cd_key = (symbol, self.rule.name)  # keep rule-level cooldown
        cd_until = self._cooldown_until.get(cd_key)
        if cd_until is not None and epoch < cd_until:
            return

        # Threshold gate
        thr = self.rule.threshold
        if eff_direction == "up":
            fire = pct >= thr
        elif eff_direction == "down":
            fire = pct <= -thr
        else:
            fire = abs(pct) >= thr
        if not fire:
            return

        # Step re-alert gate: require additional extension beyond last fired pct in this direction
        step = max(0.0, float(getattr(self.cfg, "step_realert_pct", 0.0)))
        fired_key = (symbol, self.rule.name, eff_direction)  # track per-direction
        last_fired = self._last_fired_pct.get(fired_key)
        if last_fired is not None:
            if abs(pct) < abs(last_fired) + step:
                return  # hasn’t extended enough to bother you again

        # TTL dedupe per time bucket
        bucket = (epoch // self.rule.dedupe_bucket_seconds) * self.rule.dedupe_bucket_seconds
        dkey = f"{symbol}:{self.rule.name}:{bucket}"
        if self._dedupe.seen_recently(dkey):
            return
        self._dedupe.mark(dkey)

        # Arm cooldown (direction-aware) and remember last fired pct
        if dir_cd > 0:
            self._cooldown_until[cd_key] = epoch + dir_cd
        self._last_fired_pct[fired_key] = float(pct)

        elapsed_min = max(0.0, (epoch - ref_epoch) / 60.0)

        evt = {
            "symbol": symbol,
            "rule": self.rule.name,
            "type": "start",
            "ts": float(epoch),
            "direction": eff_direction,         # "up" | "down"
            "pct": float(pct),                  # fraction
            "window_seconds": int(self.rule.window_seconds),

            # Current point
            "curr_price": float(last_close),
            "curr_epoch": int(last_ts_ms // 1000),

            # Reference extremum
            "ref_price": float(ref_price),
            "ref_epoch": int(ref_epoch),
            "ref_kind": ref_kind,               # "30m HIGH" or "30m LOW"

            # New field used by the formatter:
            "elapsed_min": float(elapsed_min),

            # keep a simple message string for logs/back-compat (optional)
            "message": (
                f"{symbol} {'↑' if eff_direction=='up' else '↓'} {pct*100:.2f}% "
                f"in past {elapsed_min:.0f}m (ref: {self.rule.window_seconds//60}m "
                f"{'LOW' if eff_direction=='up' else 'HIGH'}) | "
                f"{ref_price:.2f} → {last_close:.2f}"
            ),
            "dedupe_key": dkey,
        }

        # Emit (non-blocking)
        try:
            self.q_alerts.put_nowait(evt)
        except asyncio.QueueFull:
            pass
