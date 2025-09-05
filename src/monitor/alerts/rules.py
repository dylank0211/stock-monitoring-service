# src/monitor/alerts/rules.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal

Direction = Literal["abs", "up", "down"]

@dataclass(slots=True)
class PercentMoveRule:
    """
    Fire when the close price moves by >= threshold (fraction) over the last window_seconds.
    - direction = "abs"  → |pct| >= threshold
                 "up"   →  pct  >= threshold
                 "down" →  pct <= -threshold
    """
    name: str = "pct_move_30m_0p25"
    window_seconds: int = 1800          # 30 minutes
    threshold: float = 0.0025           # 0.25%
    direction: Direction = "abs"
    cooldown_seconds: int = 600         # 10 minutes
    dedupe_bucket_seconds: int = 300    # group alerts per 5-minute bucket
    auto_resolve: bool = False          # set True if you want “resolve” events
