from __future__ import annotations
from typing import Optional
from monitor.utils.types import Tick
from monitor.utils.time import utc_now_s

def parse_trade_msg(m: dict) -> Optional[Tick]:
    """
    Return Tick if `m` is a trade message; else None.

    Alpaca v2 trade message examples vary by feed.
    Common fields you may see:
      - "T": "t"              (message type = trade)
      - "S": "AAPL"           (symbol)
      - "p": 189.22           (price)
      - "s": 100              (size)
      - "t": "2024-02-01T14:32:01.123456Z"  (ISO time)
    """
    T = m.get("T") or m.get("type")
    if T not in ("t", "trade", "trade_message"):
        return None

    sym = m.get("S") or m.get("symbol")
    px = m.get("p") or m.get("price")
    size = m.get("s") or m.get("size") or 0
    ts = m.get("t") or m.get("timestamp")

    if sym is None or px is None:
        return None

    # normalize timestamp to epoch seconds
    if isinstance(ts, str):
        # best-effort fast parse; you can use ciso8601 if installed
        try:
            # very rough; swap in a proper parser later
            import datetime
            ts = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
        except Exception:
            ts = utc_now_s()
    elif ts is None:
        ts = utc_now_s()
    elif isinstance(ts, (int, float)):
        # assume already epoch seconds or ns
        if ts > 1e12:  # ns → s
            ts = ts / 1e9
        elif ts > 1e10:  # ms → s
            ts = ts / 1e3

    return Tick(symbol=str(sym), px=float(px), size=float(size or 0.0), ts=float(ts))