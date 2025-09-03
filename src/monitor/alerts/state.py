from __future__ import annotations
from dataclasses import dataclass, field

@dataclass(slots=True)
class RuleState:
    active: bool = False
    last_trigger_epoch: int | None = None
    cooldown_until: int | None = None
    last_value: float = 0.0              # last computed drop pct for observability

# per-symbol container so you can add more rules later
@dataclass(slots=True)
class SymbolAlertState:
    rules: dict[str, RuleState] = field(default_factory=dict)

    def ensure_rule(self, rule_name: str) -> RuleState:
        st = self.rules.get(rule_name)
        if st is None:
            st = RuleState()
            self.rules[rule_name] = st
        return st
