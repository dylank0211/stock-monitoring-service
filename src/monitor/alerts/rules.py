from __future__ import annotations
from dataclasses import dataclass

@dataclass(slots=True)
class PriceDropRule:
    name: str = "price_drop_2h"
    window_seconds: int = 7200          # 2 hours
    drop_threshold: float = 0.01         # 1% drop: (ref - last)/ref >= 0.01
    cooldown_seconds: int = 600          # 10 minutes cooldown between alerts
    dedupe_bucket_seconds: int = 300     # group alerts by 5-minute buckets
    auto_resolve: bool = False           # flip to True if you want "resolve" events when recovered
