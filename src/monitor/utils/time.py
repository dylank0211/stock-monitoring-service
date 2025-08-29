from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta

# --- fast, allocation-free time helpers ---

def utc_now_s() -> float:
    """Unix epoch seconds (float)."""
    return time.time()

def utc_now_ns() -> int:
    """Unix epoch nanoseconds (int)."""
    return time.time_ns()

def floor_to_second(ts: float) -> int:
    """Floor timestamp (s) to integer epoch second."""
    return int(ts)

def ceil_to_second(ts: float) -> int:
    """Ceil timestamp (s) to integer epoch second."""
    i = int(ts)
    return i if ts == i else i + 1

def epoch_s(dt: datetime) -> float:
    """Convert aware datetime -> epoch seconds."""
    if dt.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return dt.timestamp()

def utc_dt(ts: float | int) -> datetime:
    """Epoch seconds -> timezone-aware UTC datetime."""
    return datetime.fromtimestamp(float(ts), tz=timezone.utc)

# --- trading-day helpers (UTC boundaries convenience) ---

def seconds_until(ts_target: float) -> float:
    """Non-negative time until target (clamped at 0)."""
    now = utc_now_s()
    return max(0.0, ts_target - now)

def seconds_since(ts_past: float) -> float:
    """Non-negative time since past (clamped at 0)."""
    now = utc_now_s()
    return max(0.0, now - ts_past)
