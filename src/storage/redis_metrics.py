# src/monitor/storage/redis_metrics.py
from __future__ import annotations
import math
from typing import Dict, Optional
from redis.asyncio import Redis

TIMEFRAME = "30s"

def key(symbol: str, field: str, timeframe: str = TIMEFRAME) -> str:
    # ts:{SYM}:{TF}:{FIELD}
    return f"ts:{symbol}:{timeframe}:{field}"

async def ensure_series(
    r: Redis,
    symbol: str,
    fields: list[str],
    timeframe: str = TIMEFRAME,
    retention_ms: int = 86_400_000,
):
    """
    Idempotently create TS series with labels & retention for given fields.
    """
    ts = r.ts()
    for f in fields:
        k = key(symbol, f, timeframe)
        try:
            await ts.create(
                k,
                retention_msecs=retention_ms,
                labels={"symbol": symbol, "tf": timeframe, "field": f},
                duplicate_policy="last",
                encoding="compressed",
            )
        except Exception:
            # likely already exists
            pass

async def bootstrap_symbols(r: Redis, symbols: list[str], timeframe: str = TIMEFRAME):
    base_fields = ["O", "H", "L", "C", "V", "VWAP"]
    for sym in symbols:
        await ensure_series(r, sym, base_fields, timeframe=timeframe)

def _finite(x) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(x)

async def write_bar_bundle(
    r: Redis,
    symbol: str,
    epoch_ms: int,
    *,
    o: float,
    h: float,
    l: float,
    c: float,
    v: float,
    vwap: Optional[float] = None,
    indicators: Optional[Dict[str, float]] = None,
    timeframe: str = TIMEFRAME,
):
    """
    Write OHLCV + VWAP + indicators for a single bar timestamp.
    Skips any values that are NaN/Inf/None to satisfy RedisTimeSeries.
    Uses pure-async calls (no mixed pipeline).
    """
    ts = r.ts()

    # OHLCV
    if _finite(o): await ts.add(key(symbol, "O", timeframe), epoch_ms, float(o), duplicate_policy="last")
    if _finite(h): await ts.add(key(symbol, "H", timeframe), epoch_ms, float(h), duplicate_policy="last")
    if _finite(l): await ts.add(key(symbol, "L", timeframe), epoch_ms, float(l), duplicate_policy="last")
    if _finite(c): await ts.add(key(symbol, "C", timeframe), epoch_ms, float(c), duplicate_policy="last")
    if _finite(v): await ts.add(key(symbol, "V", timeframe), epoch_ms, float(v), duplicate_policy="last")

    # VWAP
    if _finite(vwap):
        await ts.add(key(symbol, "VWAP", timeframe), epoch_ms, float(vwap), duplicate_policy="last")

    # Indicators
    if indicators:
        for name, val in indicators.items():
            if not _finite(val):
                continue
            k = key(symbol, name, timeframe)
            # Best-effort create (idempotent)
            try:
                await ts.create(
                    k,
                    retention_msecs=86_400_000,
                    labels={"symbol": symbol, "tf": timeframe, "field": name},
                    duplicate_policy="last",
                    encoding="compressed",
                )
            except Exception:
                pass
            await ts.add(k, epoch_ms, float(val), duplicate_policy="last")
