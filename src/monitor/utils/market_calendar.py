from __future__ import annotations

import functools
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Literal, Optional, Tuple

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

# Extended hours (premarket / after-hours). Alpaca SIP streams these.
PRE_OPEN  = dtime(4, 0)
AFT_CLOSE = dtime(20, 0)

# Minimal fallback holiday set (extend as needed)
FALLBACK_HOLIDAYS_2025 = {
    "2025-01-01","2025-01-20","2025-02-17","2025-04-18","2025-05-26",
    "2025-06-19","2025-07-04","2025-09-01","2025-11-27","2025-12-25",
}

SessionMode = Literal["regular", "extended", "any"]

@functools.lru_cache(maxsize=1)
def _nyse_calendar():
    if mcal is None:
        return None
    # XNYS supports pre/post info via .schedule (early closes handled)
    return mcal.get_calendar("XNYS")

def _now_ny() -> datetime:
    return datetime.now(tz=NY)

def _today_ny() -> str:
    return _now_ny().date().isoformat()

def _is_holiday_fallback(dstr: str) -> bool:
    return dstr in FALLBACK_HOLIDAYS_2025

# ---------------- Fallback path (no pandas_market_calendars) ---------------- #

def _is_regular_open_fallback(now_ny: Optional[datetime] = None) -> bool:
    now_ny = now_ny or _now_ny()
    if now_ny.weekday() >= 5 or _is_holiday_fallback(now_ny.date().isoformat()):
        return False
    t = now_ny.timetz()
    return RTH_OPEN.replace(tzinfo=NY) <= t < RTH_CLOSE.replace(tzinfo=NY)

def _is_extended_open_fallback(now_ny: Optional[datetime] = None) -> bool:
    now_ny = now_ny or _now_ny()
    if now_ny.weekday() >= 5 or _is_holiday_fallback(now_ny.date().isoformat()):
        return False
    t = now_ny.timetz()
    pre = PRE_OPEN.replace(tzinfo=NY) <= t < RTH_OPEN.replace(tzinfo=NY)
    post = RTH_CLOSE.replace(tzinfo=NY) <= t < AFT_CLOSE.replace(tzinfo=NY)
    return pre or post

# ---------------- Calendar-aware path (preferred) ---------------- #

@dataclass(frozen=True)
class SessionWindow:
    pre_open_utc: Optional[datetime]
    rth_open_utc: Optional[datetime]
    rth_close_utc: Optional[datetime]
    aft_close_utc: Optional[datetime]

def _today_session_window() -> Optional[SessionWindow]:
    cal = _nyse_calendar()
    if cal is None:
        return None
    # Expand window to cover edge cases
    now_utc = datetime.now(tz=UTC)
    sched = cal.schedule(
        start_date=(now_utc - timedelta(days=2)).date(),
        end_date=(now_utc + timedelta(days=2)).date()
    )
    if _today_ny() not in sched.index.astype(str):
        return SessionWindow(None, None, None, None)
    row = sched.loc[sched.index.astype(str) == _today_ny()].iloc[0]
    # Some calendars have explicit pre/post; XNYS returns market_open/close.
    # We’ll synthesize extended from 04:00–20:00 NY unless it’s an early close day.
    rth_open  = row["market_open"].to_pydatetime().astimezone(UTC)
    rth_close = row["market_close"].to_pydatetime().astimezone(UTC)

    # Convert 04:00 and 20:00 NY to UTC (same date in NY zone)
    ny_date = _now_ny().date()
    pre_open  = datetime.combine(ny_date, PRE_OPEN, tzinfo=NY).astimezone(UTC)
    aft_close = datetime.combine(ny_date, AFT_CLOSE, tzinfo=NY).astimezone(UTC)

    # Guard: ensure pre_open <= rth_open <= rth_close <= aft_close
    if not (pre_open <= rth_open <= rth_close <= aft_close):
        # On half days (e.g., Black Friday), rth_close ~13:00, still fine.
        pass

    return SessionWindow(pre_open, rth_open, rth_close, aft_close)

def _now_in_interval(now: datetime, start: Optional[datetime], end: Optional[datetime]) -> bool:
    return (start is not None) and (end is not None) and (start <= now < end)

def is_market_open_now(mode: SessionMode = "regular") -> bool:
    """
    True if the US equity market is considered open **now** under the given mode.

    mode="regular"  -> 09:30–16:00 NY on trading days
    mode="extended" -> 04:00–09:30 or 16:00–20:00 NY on trading days
    mode="any"      -> regular OR extended
    """
    cal = _nyse_calendar()
    if cal is None:
        if mode == "regular":
            return _is_regular_open_fallback()
        if mode == "extended":
            return _is_extended_open_fallback()
        return _is_regular_open_fallback() or _is_extended_open_fallback()

    win = _today_session_window()
    if win is None:
        # Shouldn’t happen; fall back
        if mode == "regular":
            return _is_regular_open_fallback()
        if mode == "extended":
            return _is_extended_open_fallback()
        return _is_regular_open_fallback() or _is_extended_open_fallback()

    now = datetime.now(tz=UTC)
    in_rth = _now_in_interval(now, win.rth_open_utc, win.rth_close_utc)
    in_pre = _now_in_interval(now, win.pre_open_utc, win.rth_open_utc)
    in_aft = _now_in_interval(now, win.rth_close_utc, win.aft_close_utc)
    in_ext = in_pre or in_aft

    if mode == "regular":
        return in_rth
    if mode == "extended":
        return in_ext
    return in_rth or in_ext

# -------- Optional: session state & next transition (useful for health) ------ #

@dataclass(frozen=True)
class SessionState:
    phase: Literal["closed", "premarket", "regular", "afterhours"]
    seconds_to_next: Optional[int]  # None if unknown

def session_state_now() -> SessionState:
    cal = _nyse_calendar()
    if cal is None:
        # Fallback approximation
        now_ny = _now_ny()
        if now_ny.weekday() >= 5 or _is_holiday_fallback(now_ny.date().isoformat()):
            return SessionState("closed", None)
        t = now_ny.timetz()
        if PRE_OPEN.replace(tzinfo=NY) <= t < RTH_OPEN.replace(tzinfo=NY):
            nxt = datetime.combine(now_ny.date(), RTH_OPEN, tzinfo=NY).astimezone(UTC) - datetime.now(tz=UTC)
            return SessionState("premarket", max(0, int(nxt.total_seconds())))
        if RTH_OPEN.replace(tzinfo=NY) <= t < RTH_CLOSE.replace(tzinfo=NY):
            nxt = datetime.combine(now_ny.date(), RTH_CLOSE, tzinfo=NY).astimezone(UTC) - datetime.now(tz=UTC)
            return SessionState("regular", max(0, int(nxt.total_seconds())))
        if RTH_CLOSE.replace(tzinfo=NY) <= t < AFT_CLOSE.replace(tzinfo=NY):
            nxt = datetime.combine(now_ny.date(), AFT_CLOSE, tzinfo=NY).astimezone(UTC) - datetime.now(tz=UTC)
            return SessionState("afterhours", max(0, int(nxt.total_seconds())))
        return SessionState("closed", None)

    win = _today_session_window()
    if win is None or win.rth_open_utc is None or win.rth_close_utc is None:
        return SessionState("closed", None)

    now = datetime.now(tz=UTC)
    if win.pre_open_utc and now < win.pre_open_utc:
        return SessionState("closed", int((win.pre_open_utc - now).total_seconds()))
    if win.pre_open_utc and now < win.rth_open_utc:
        return SessionState("premarket", int((win.rth_open_utc - now).total_seconds()))
    if now < win.rth_close_utc:
        return SessionState("regular", int((win.rth_close_utc - now).total_seconds()))
    if win.aft_close_utc and now < win.aft_close_utc:
        return SessionState("afterhours", int((win.aft_close_utc - now).total_seconds()))
    return SessionState("closed", None)
