from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo

def _fmt_ts(ts_s: int, tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    return datetime.fromtimestamp(ts_s, tz).strftime("%-I:%M:%S %Z")  # e.g., 11:28:30 CDT

def format_alert_pretty(evt: dict, tz_name: str = "America/Chicago") -> str:
    sym   = evt.get("symbol", "?")
    dirn  = evt.get("direction", "down")  # "up" | "down"
    pct   = float(evt.get("pct", 0.0)) * 100.0
    curr_epoch = int(evt.get("curr_epoch", int(evt.get("ts", 0))))
    ref_epoch  = int(evt.get("ref_epoch", curr_epoch))
    ref_price  = float(evt.get("ref_price", 0.0))
    curr_price = float(evt.get("curr_price", 0.0))
    ref_kind   = evt.get("ref_kind", "window EXTREMUM")
    elapsed_min = evt.get("elapsed_min")
    if elapsed_min is None:
        # fallback if old events without elapsed_min
        elapsed_min = max(0.0, (curr_epoch - ref_epoch) / 60.0)

    arrow = "↑" if dirn == "up" else "↓"
    sign  = "+" if dirn == "up" else "-"
    now_s = _fmt_ts(curr_epoch, tz_name)
    ref_s = _fmt_ts(ref_epoch, tz_name)

    return (
        f"[{sym} {'UP' if dirn=='up' else 'DOWN'}] {now_s} {arrow} {sign}{abs(pct):.2f}% "
        f"in past {elapsed_min:.0f}m  |  "
        f"{ref_price:.2f} @ {ref_s} → {curr_price:.2f} now "
        f"(ref: {ref_kind})"
    )

