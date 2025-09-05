# src/monitor/alerts/notifiers.py
from __future__ import annotations
import asyncio
import structlog
from typing import Callable, Optional

log = structlog.get_logger("notifier")

class ConsoleNotifier:
    def __init__(self, format_fn: Optional[Callable[[dict], str]] = None):
        self._format_fn = format_fn

    async def send(self, evt: dict):
        if self._format_fn:
            try:
                text = self._format_fn(evt)
                print(text, flush=True)
                return
            except Exception as e:
                log.warning("console_format_failed", err=str(e))
        # fallback (raw)
        print(f"[ALERT] {evt.get('symbol')} {evt.get('rule')} {evt.get('type')} "
              f"pct={evt.get('pct')} msg={evt.get('message')}", flush=True)