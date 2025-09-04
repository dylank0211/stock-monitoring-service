from __future__ import annotations
from typing import Dict, Iterable, List
from redis.asyncio import Redis

RETENTION_MS = 86_400_000  # 24h
TIMEFRAME = "30s"

CORE_FIELDS = ["O", "H", "L", "C", "V", "VWAP"]
INDICATOR_FIELDS = [
    "EMA9", "EMA20", "EMA50",
    "RSI14", "RSI60", "RSI120",
    "ATR14",
    "MACD12_26", "MACDsig9", "MACDhist",
    "OBV",
]
ALL_FIELDS = CORE_FIELDS + INDICATOR_FIELDS

def key(sym: str, field: str, tf: str = TIMEFRAME) -> str:
    return f"ts:{sym}:{tf}:{field}"

async def ensure_series(r: Redis, sym: str, fields: Iterable[str] = ALL_FIELDS, tf: str = TIMEFRAME) -> None:
    ts = r.ts()
    for f in fields:
        k = key(sym, f, tf)
        try:
            await ts.create(
                k,
                retention_msecs=RETENTION_MS,
                labels={"sym": sym, "tf": tf, "field": f},
                duplicate_policy="last",
            )
        except Exception as e:
            if "already exists" not in str(e):
                raise

async def bootstrap_symbols(r: Redis, symbols: List[str]) -> None:
    for sym in symbols:
        await ensure_series(r, sym, ALL_FIELDS, TIMEFRAME)

async def write_bar_bundle(
    r: Redis,
    sym: str,
    epoch_ms: int,
    *,
    o: float, h: float, l: float, c: float, v: float,
    vwap: float,
    indicators: Dict[str, float],
    tf: str = TIMEFRAME,
) -> None:
    p = r.pipeline()
    p.ts().add(key(sym, "O", tf), epoch_ms, float(o))
    p.ts().add(key(sym, "H", tf), epoch_ms, float(h))
    p.ts().add(key(sym, "L", tf), epoch_ms, float(l))
    p.ts().add(key(sym, "C", tf), epoch_ms, float(c))
    p.ts().add(key(sym, "V", tf), epoch_ms, float(v))
    p.ts().add(key(sym, "VWAP", tf), epoch_ms, float(vwap))
    for fname, val in indicators.items():
        p.ts().add(key(sym, fname, tf), epoch_ms, float(val))
    await p.execute()
