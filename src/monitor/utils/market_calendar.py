from __future__ import annotations

import functools
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Optional

try:
    import pandas_market_calendars as mcal  # optional
except Exception:
    mcal = None  # fallback path

try:
    import zoneinfo  # py3.9+
except Exception:
    from backports import zoneinfo  # type: ignore[no-redef]

NY = zoneinfo.ZoneInfo("America/New_York")
UTC = timezone.utc

# Regular trading hours (RTH)
RTH_OPEN = dtime(9, 30)
RTH_CLOSE = dtime(16, 0)

# Minimal fallback holiday set (UTC dates for 2025; extend as needed)
# We'll check in local NY midnight boundaries and map to dates.
FALLBACK_HOLIDAYS_2025 = {
    # NYSE 2025 observed (subset): New Year’s Day, MLK, Presidents, Good Friday, Memorial, Juneteenth, Independence, Labor, Thanksgiving, Christmas
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18", "2025-05-26",
    "2025-06-19", "2025-07-04", "2025-09-01", "2025-11-27", "2025-12-25",
}

@functools.lru_cache(maxsize=1)
def _nyse_calendar():
    if mcal is None:
        return None
    return mcal.get_calendar("XNYS")

def _local_now_ny() -> datetime:
    return datetime.now(tz=NY)

def _is_rth_now_simple(now_ny: Optional[datetime] = None) -> bool:
    """Simple check: Mon–Fri, not in fallback holiday set, between 09:30–16:00 NY time."""
    now_ny = now_ny or _local_now_ny()
    if now_ny.weekday() >= 5:  # Sat=5, Sun=6
        return False
    dstr = now_ny.date().isoformat()
    if dstr in FALLBACK_HOLIDAYS_2025:
        return False
    t = now_ny.timetz()
    return (t >= RTH_OPEN.replace(tzinfo=NY)) and (t < RTH_CLOSE.replace(tzinfo=NY))

def is_market_open_now() -> bool:
    """
    Returns True when the US stock market (NYSE) is in a regular session *now*.

    Uses pandas-market-calendars when available; otherwise falls back
    to a simple Mon–Fri + clock window + small holiday set.
    """
    cal = _nyse_calendar()
    if cal is None:
        return _is_rth_now_simple()

    now_utc = datetime.now(tz=UTC)
    # window across +/- 2 days to avoid edge cache misses
    sched = cal.schedule(start_date=(now_utc - timedelta(days=2)).date(),
                         end_date=(now_utc + timedelta(days=2)).date())
    if sched.empty:
        return False

    # find the row containing now (if any)
    # schedule has market_open / market_close in UTC tz-aware
    # select today’s session quickly
    today_str = _local_now_ny().date().isoformat()
    if today_str not in sched.index.astype(str):
        # If not a trading day, closed.
        return False

    row = sched.loc[sched.index.astype(str) == today_str].iloc[0]
    open_ts: datetime = row["market_open"].to_pydatetime().replace(tzinfo=UTC)
    close_ts: datetime = row["market_close"].to_pydatetime().replace(tzinfo=UTC)
    return open_ts <= now_utc < close_ts

# Optional: expose next session info if you need it later
def next_session_open_close() -> tuple[Optional[datetime], Optional[datetime]]:
    cal = _nyse_calendar()
    if cal is None:
        # simple: find next weekday and return RTH window
        now_ny = _local_now_ny()
        d = now_ny.date()
        for i in range(0, 10):
            cand = (now_ny + timedelta(days=i)).date()
            if (cand.weekday() < 5) and (cand.isoformat() not in FALLBACK_HOLIDAYS_2025):
                start = datetime.combine(cand, RTH_OPEN, tzinfo=NY).astimezone(UTC)
                end = datetime.combine(cand, RTH_CLOSE, tzinfo=NY).astimezone(UTC)
                if end > datetime.now(tz=UTC):
                    return start, end
        return None, None

    now_utc = datetime.now(tz=UTC)
    sched = cal.schedule(start_date=(now_utc - timedelta(days=1)).date(),
                         end_date=(now_utc + timedelta(days=10)).date())
    future = sched[sched["market_close"] > now_utc]
    if future.empty:
        return None, None
    row = future.iloc[0]
    return row["market_open"].to_pydatetime().replace(tzinfo=UTC), \
           row["market_close"].to_pydatetime().replace(tzinfo=UTC)
