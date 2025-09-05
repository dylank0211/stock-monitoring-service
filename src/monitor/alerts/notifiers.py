# src/monitor/alerts/notifiers.py
from __future__ import annotations
from typing import Mapping, Any

class ConsoleNotifier:
    """
    Simple console notifier that just prints alerts.
    """
    async def send(self, evt: Mapping[str, Any]) -> None:
        print(
            f"[ALERT] {evt.get('symbol')} {evt.get('rule')} {evt.get('type')} "
            f"value={evt.get('value')} msg='{evt.get('message')}'"
        )
