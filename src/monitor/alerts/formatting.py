from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo

def _fmt_ts(epoch_s: int | None, tz_name: str = "America/Chicago") -> str:
    if not epoch_s:
        return "?"
    try:
        tz = ZoneInfo(tz_name)
        return datetime.fromtimestamp(int(epoch_s), tz).strftime("%H:%M:%S %Z")
    except Exception:
        return "?"

def format_alert_pretty(evt: dict, tz_name: str = "America/Chicago") -> str:
    sym   = evt.get("symbol", "?")
    dir_  = evt.get("direction") or ("DOWN" if (evt.get("pct", 0.0) < 0) else "UP")
    arrow = "↑" if dir_.lower() == "up" else "↓"
    pct   = float(evt.get("pct", 0.0)) * 100.0

    curr_ep  = evt.get("curr_epoch") or int(evt.get("ts", 0))  # fallback
    curr_px  = evt.get("curr_price")
    ref_ep   = evt.get("ref_epoch")
    ref_px   = evt.get("ref_price")
    ref_kind = evt.get("ref_kind", "reference")

    now_str = _fmt_ts(curr_ep, tz_name)
    ref_str = _fmt_ts(ref_ep, tz_name)
    win_m   = int(evt.get("window_seconds", 0)) // 60

    return (f"[{sym} {dir_.upper()}] {now_str} {arrow} {pct:+.2f}% in {win_m}m  |  "
            f"{ref_px:.2f} @ {ref_str} → {curr_px:.2f} now (ref: {ref_kind})")
