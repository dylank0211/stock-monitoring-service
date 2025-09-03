from __future__ import annotations

import asyncio
import os
import random
from dataclasses import dataclass
from typing import Optional

import aiohttp
import structlog

log = structlog.get_logger("telegram")

# --------- small rate limiter (token bucket) ----------

class RateLimiter:
    def __init__(self, rate_per_sec: float, burst: int = 1):
        self.rate = float(rate_per_sec)
        self.capacity = int(burst)
        self.tokens = float(burst)
        self.updated = asyncio.get_event_loop().time()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = asyncio.get_event_loop().time()
            # refill
            self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
            self.updated = now
            # wait if no token
            if self.tokens < 1.0:
                needed = 1.0 - self.tokens
                await asyncio.sleep(needed / self.rate)
                self.updated = asyncio.get_event_loop().time()
                self.tokens = max(0.0, self.tokens)  # just for safety
            self.tokens -= 1.0

# --------- config & client ----------

@dataclass(slots=True)
class TelegramConfig:
    bot_token: str
    chat_id: str                # personal chat id or group id
    parse_mode: Optional[str] = None  # "HTML" or "MarkdownV2" or None
    timeout_s: float = 8.0
    per_chat_rate_per_sec: float = 1.0  # Telegram is generous, but keep it tame
    per_chat_burst: int = 3
    max_retries: int = 5
    initial_backoff_s: float = 0.5
    max_backoff_s: float = 8.0

class TelegramNotifier:
    """
    Background worker that drains an alerts queue and sends to Telegram with
    rate limiting and retry w/ backoff.
    """
    def __init__(self, cfg: TelegramConfig, alerts_queue, format_fn=None):
        self.cfg = cfg
        self.q = alerts_queue  # something with .get() (q_alerts or NotifyQueue)
        self._session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._rl = RateLimiter(rate_per_sec=cfg.per_chat_rate_per_sec, burst=cfg.per_chat_burst)
        self._format_fn = format_fn or self._default_format

    async def start(self):
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self.cfg.timeout_s)
            self._session = aiohttp.ClientSession(timeout=timeout)
        self._stop.clear()
        self._task = asyncio.create_task(self._loop(), name="telegram-notifier")

    async def stop(self):
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass
        if self._session:
            await self._session.close()
            self._session = None

    async def _loop(self):
        assert self._session is not None
        try:
            while not self._stop.is_set():
                evt = await self.q.get()
                text = self._format_fn(evt)
                await self._rl.acquire()
                await self._send(text)
        except asyncio.CancelledError:
            return

    async def _send(self, text: str):
        assert self._session is not None
        url = f"https://api.telegram.org/bot{self.cfg.bot_token}/sendMessage"
        payload = {"chat_id": self.cfg.chat_id, "text": text}
        if self.cfg.parse_mode:
            payload["parse_mode"] = self.cfg.parse_mode

        backoff = self.cfg.initial_backoff_s
        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                async with self._session.post(url, data=payload) as resp:
                    if resp.status == 200:
                        return
                    # 429 or 5xx → retry with backoff
                    detail = await _maybe_text(resp)
                    log.warning("telegram_send_failed", status=resp.status, body=detail, attempt=attempt)
                    if resp.status == 429:
                        # Telegram may include retry_after (seconds)
                        try:
                            data = await resp.json()
                            ra = data.get("parameters", {}).get("retry_after")
                            if ra:
                                await asyncio.sleep(float(ra))
                                continue
                        except Exception:
                            pass
                    if 500 <= resp.status < 600 or resp.status == 429:
                        await asyncio.sleep(self._jitter(backoff))
                        backoff = min(backoff * 2.0, self.cfg.max_backoff_s)
                        continue
                    # other 4xx: don't retry
                    return
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                log.warning("telegram_network_error", err=str(e), attempt=attempt)
                await asyncio.sleep(self._jitter(backoff))
                backoff = min(backoff * 2.0, self.cfg.max_backoff_s)
        log.error("telegram_give_up_after_retries")

    @staticmethod
    def _jitter(base: float) -> float:
        return base * (0.8 + 0.4 * random.random())

    @staticmethod
    def _default_format(evt: dict) -> str:
        """
        Default alert text. evt is the AlertEvent dict your evaluator emits.
        """
        sym = evt.get("symbol", "?")
        rule = evt.get("rule", "")
        value = evt.get("value", 0.0)
        msg = evt.get("message", "")
        return f"[{sym}] {rule}: {value*100:.2f}%\n{msg}"

async def _maybe_text(resp: aiohttp.ClientResponse) -> str:
    try:
        return await resp.text()
    except Exception:
        return "<no body>"
