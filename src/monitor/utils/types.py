from __future__ import annotations

from dataclasses import dataclass
from typing import TypedDict, Literal, Optional

# ---- ingest-level primitives ----

@dataclass(slots=True)
class Tick:
    symbol: str
    px: float
    size: float
    ts: float  # epoch seconds

@dataclass(slots=True)
class SecBar:
    """
    1-second OHLCV bar snapshot (optional, if you want a struct).
    """
    epoch: int        # integer epoch second
    open: float
    high: float
    low: float
    close: float
    volume: float

# ---- alerting domain (if useful for later modules) ----

AlertType = Literal["start", "update", "resolve"]

class AlertEvent(TypedDict, total=False):
    symbol: str
    rule: str
    type: AlertType
    value: float
    ts: float
    message: str
    dedupe_key: str

# Optional config-typed dicts (if you read YAML into structured types)

class RuleConfig(TypedDict, total=False):
    name: str
    indicator: str
    direction: Literal["above", "below"]
    threshold: float
    clear_level: float
    persist_secs: int
    cooldown_secs: int
