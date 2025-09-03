from __future__ import annotations
import time

class TTLDeduper:
    """
    TTL-based dedupe cache with max size. Keys expire after ttl_s.
    """
    def __init__(self, ttl_s: int, max_size: int = 10_000):
        self.ttl_s = ttl_s
        self.max_size = max_size
        self._store: dict[str, float] = {}  # key -> expire_ts

    def _now(self) -> float:
        return time.time()

    def seen_recently(self, key: str) -> bool:
        now = self._now()
        exp = self._store.get(key)
        if exp is None:
            return False
        if exp < now:
            # expired; cleanup
            self._store.pop(key, None)
            return False
        return True

    def mark(self, key: str) -> None:
        # opportunistic cleanup when large
        if len(self._store) > self.max_size:
            now = self._now()
            # drop ~25% oldest/expired keys
            for k, exp in list(self._store.items())[: self.max_size // 4]:
                if exp < now:
                    self._store.pop(k, None)
        self._store[key] = self._now() + self.ttl_s
