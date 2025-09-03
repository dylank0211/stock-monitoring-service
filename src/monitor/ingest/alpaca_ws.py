from __future__ import annotations

import asyncio
import json
import os
import random
import time
from dataclasses import dataclass
from typing import Iterable, Optional

import structlog
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed

from monitor.utils.time import utc_now_s
from monitor.utils.types import Tick  # dataclass: symbol:str, px:float, size:float, ts:float
from monitor.ingest import parser  # must expose parse_trade_msg(dict)->Tick|None


@dataclass(slots=True)
class AlpacaWSConfig:
    stream_url: str
    key_id: str
    secret_key: str
    symbols: list[str]
    # reconnect behavior
    max_backoff_s: float = 30.0
    initial_backoff_s: float = 0.25
    # heartbeat / staleness
    expect_heartbeat_s: float = 5.0  # if no messages for this long during market hours, mark degraded
    # queue limits
    ticks_queue_maxsize: int = 10_000
    # timeouts
    open_timeout_s: float = 5.0
    ping_interval_s: float = 20.0
    # subscription kinds (trades/quotes/bars); we’ll default to trades
    subscribe_trades: bool = True
    subscribe_quotes: bool = True
    subscribe_bars: bool = False


class AlpacaWS:
    """
    Robust Alpaca WebSocket client (v2 stream).

    Lifecycle:
      - Connect → Auth → Subscribe → Stream
      - On any error, close and reconnect with jittered backoff (cap)
      - Parses inbound JSON messages and enqueues normalized Tick objects.

    Notes:
      - Message formats vary by channel (trades/quotes/bars). We push only
        normalized trade-like events to the ticks queue via parser.parse_trade_msg().
      - Use a separate BarAggregator to turn Tick -> 1s OHLCV bars.

    Usage:
        cfg = AlpacaWSConfig(stream_url=..., key_id=..., secret_key=..., symbols=["NVDA","AAPL"])
        client = AlpacaWS(cfg, q_ticks, market_is_open_callable)
        await client.start()   # runs until cancelled/stop() called
    """

    # --- in your class __init__ signature & body ---
    def __init__(
        self,
        cfg: AlpacaWSConfig,
        ticks_queue: asyncio.Queue,
        market_is_open: Optional[callable] = None,
        heartbeat_secs_getter: Optional[callable] = None,  # NEW: dynamic heartbeat by phase
    ):
        self.cfg = cfg
        self.q_ticks = ticks_queue
        self.market_is_open = market_is_open or (lambda: True)

        # If provided, should return an int/float seconds (or None when market closed)
        # e.g., lambda: 8 during RTH, 45 during extended, None when closed.
        self._heartbeat_secs_getter = heartbeat_secs_getter

        self._log = structlog.get_logger("alpaca_ws")
        self._stop = asyncio.Event()
        self._connected = asyncio.Event()
        self._last_msg_ts: float = 0.0
        self._ws = None

        self.connected: bool = False
        self.authenticated: bool = False
        self.subscribed: bool = False

        self._prebuffer: list[dict] = []


   # ---------------------------- public API ---------------------------- #
   
    async def start(self) -> None:
        backoff = self.cfg.initial_backoff_s
        while not self._stop.is_set():
            try:
                await self._connect_and_stream()
                break
            except asyncio.CancelledError:
                # allow cooperative shutdown without error
                break
            except Exception as e:
                # if we're stopping, don't backoff-sleep; just exit
                if self._stop.is_set():
                    break
                self._log.warning("ws_error_reconnect", err=str(e), backoff_s=round(backoff, 3))
                await asyncio.sleep(self._jitter(backoff))
                backoff = min(backoff * 2.0, self.cfg.max_backoff_s)
        self._log.info("ws_loop_exit")

    async def stop(self) -> None:
        self._stop.set()
        if self._ws and hasattr(self._ws, "close"):
            try:
                await self._ws.close()
            except Exception:
                pass


    # --------------------------- core internals ------------------------- #

    async def _connect_and_stream(self) -> None:
        """
        Establishes connection, authenticates, subscribes, then streams messages.
        Returns only on stop() or connection closure/error.
        """
        self._reset_state()
        url = self.cfg.stream_url

        self._log.info("ws_connecting", url=url)
        async with ws_connect(
            url,
            open_timeout=self.cfg.open_timeout_s,
            ping_interval=self.cfg.ping_interval_s,
            ping_timeout=None,
            max_queue=None,  # don't buffer internally; we handle backpressure
        ) as ws:
            self._ws = ws
            self.connected = True
            self._connected.set()
            self._log.info("ws_connected")

            # Authenticate
            await self._auth(ws)

            # Subscribe to channels
            await self._subscribe(ws)

            # Stream messages
            await self._stream_loop(ws)


    async def _auth(self, ws) -> None:
        auth_msg = {
            "action": "auth",
            "key": self.cfg.key_id,
            "secret": self.cfg.secret_key,
        }
        await ws.send(json.dumps(auth_msg))

        # Expect an "auth" status message
        # Alpaca typically replies with: {"T": "success", "msg": "authenticated"} or similar
        # We’ll wait until we see an auth success in the stream prelude.
        ok = await self._wait_for_auth_response(ws, timeout=2.0)
        if not ok:
            raise RuntimeError("Alpaca auth failed or timed out")
        self.authenticated = True
        self._log.info("ws_authenticated")


    async def _subscribe(self, ws) -> None:
            subs: dict[str, list[str]] = {"action": "subscribe"}
            if self.cfg.subscribe_trades:
                subs["trades"] = self.cfg.symbols
            if self.cfg.subscribe_quotes:
                subs["quotes"] = self.cfg.symbols
            if self.cfg.subscribe_bars:
                subs["bars"] = self.cfg.symbols

            await ws.send(json.dumps(subs))
            # You can optionally wait for a confirmation message. We assume success;
            # if stream yields errors, we handle them in _stream_loop.
            self.subscribed = True
            self._log.info("ws_subscribed", symbols=self.cfg.symbols)


    async def _stream_loop(self, ws) -> None:
        """
        Reads messages and enqueues normalized ticks. Monitors staleness and stop signal.
        """
        if self._prebuffer:
            batch = self._prebuffer
            self._prebuffer = []
            for m in batch:
                tick = None
                try:
                    tick = parser.parse_trade_msg(m)
                except Exception as e:
                    self._log.warning("parse_trade_error", err=str(e), snippet=str(m)[:200])
                if tick:
                    self._enqueue_tick(tick)
                else:
                    self._handle_non_trade(m)
                self._last_msg_ts = utc_now_s()
        while not self._stop.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=self._recv_timeout())
            except asyncio.TimeoutError:
                # No messages for a while; check staleness if market is open
                if self.market_is_open() and (utc_now_s() - self._last_msg_ts) > self.cfg.expect_heartbeat_s:
                    self._log.warning(
                        "ws_stale_no_messages",
                        age_s=round(utc_now_s() - self._last_msg_ts, 3),
                    )
                continue
            except (ConnectionClosed) as e:
                self._log.warning("ws_closed", code=getattr(e, "code", None), reason=str(e))
                raise
            except asyncio.CancelledError:
                # graceful path: socket closed or stop requested; exit the stream loop cleanly
                self._log.info("ws_recv_cancelled")
                return


            self._last_msg_ts = utc_now_s()

            # Alpaca v2 can batch messages as a JSON array. Normalize to a list.
            try:
                msg = json.loads(raw)
                batch = msg if isinstance(msg, list) else [msg]
            except Exception as e:
                self._log.warning("ws_json_error", err=str(e))
                continue

            for m in batch:
                # fast-path: trade messages
                tick = None
                try:
                    tick = parser.parse_trade_msg(m)
                except Exception as e:
                    self._log.warning("parse_trade_error", err=str(e), snippet=str(m)[:200])

                if tick:
                    self._enqueue_tick(tick)
                else:
                    # You may want to handle other message types here (errors, status, subscription acks)
                    self._handle_non_trade(m)

        # graceful stop path: exit stream
        self._log.info("ws_stream_loop_exit")


# --------------------------- helpers -------------------------------- #

    def healthy(self) -> bool:
        """Quick health signal for /healthz."""
        if not self.connected or not self.authenticated or not self.subscribed:
            return False
        if self.market_is_open() and (utc_now_s() - self._last_msg_ts) > self.cfg.expect_heartbeat_s:
            return False
        return True

    def last_message_age_s(self) -> float:
        return max(0.0, utc_now_s() - self._last_msg_ts) if self._last_msg_ts else float("inf")

    def _enqueue_tick(self, t: Tick) -> None:
        try:
            self.q_ticks.put_nowait(t)
        except asyncio.QueueFull:
            # Drop oldest semantics aren’t built-in for asyncio.Queue.
            # Here we choose to drop this tick to keep system responsive.
            # Since we aggregate to 1s bars later, losing some sub-second trades is acceptable.
            self._log.info("ticks_queue_full_drop", symbol=t.symbol)

    
    def _handle_non_trade(self, msg: dict) -> None:
        """
        Handle subscription acks, errors, heartbeats. Extend as needed.
        """
        # Examples you might observe:
        # {"T":"success","msg":"authenticated"}
        # {"T":"subscription","trades":["AAPL","NVDA"],"quotes":[],"bars":[]}
        # {"T":"error","code":400,"msg":"invalid symbol"}
        T = msg.get("T") or msg.get("type")
        if T in ("success", "subscription"):
            # good news—optional to log at debug
            return
        if T in ("error", "error_message"):
            self._log.warning("alpaca_stream_error", msg=msg)
            return
        # heartbeat-like? (Alpaca sometimes sends info messages)
        if T in ("ping", "info"):
            return
        # else: ignore
        return

    async def _wait_for_auth_response(self, ws, timeout: float) -> bool:
        """
        Consume messages up to `timeout` seconds looking for an auth success ack.
        If we receive a JSON array and the auth ack is inside it, buffer the
        remaining messages so the stream loop can process them later.
        """
        end = time.time() + timeout
        while time.time() < end:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=end - time.time())
            except asyncio.TimeoutError:
                break

            try:
                msg = json.loads(raw)
            except Exception:
                continue

            if isinstance(msg, list):
                # scan the batch; if we find auth success, buffer the rest
                for i, m in enumerate(msg):
                    T = m.get("T") or m.get("type")
                    if (T == "success" and m.get("msg") == "authenticated") or (
                        T == "authorization" and m.get("action") == "authenticated"
                    ):
                        # buffer messages AFTER this one; also buffer any BEFORE that we didn't examine
                        self._prebuffer.extend(msg[i+1:])
                        return True
                # no auth found; buffer whole batch and continue looking
                self._prebuffer.extend(msg)
            else:
                T = msg.get("T") or msg.get("type")
                if (T == "success" and msg.get("msg") == "authenticated") or (
                    T == "authorization" and msg.get("action") == "authenticated"
                ):
                    return True
                # Not auth—buffer it for the stream loop
                self._prebuffer.append(msg)

            # keep freshness
            self._last_msg_ts = utc_now_s()

        return False

    def _recv_timeout(self) -> float:
        # how long we’re okay waiting for a message before we check staleness
        return max(1.0, min(self.cfg.expect_heartbeat_s, 5.0))

    def _reset_state(self) -> None:
        self.connected = False
        self.authenticated = False
        self.subscribed = False
        self._connected.clear()
        self._last_msg_ts = 0.0
        self._ws = None

    @staticmethod
    def _jitter(base: float) -> float:
        # ±20% jitter
        return base * (0.8 + 0.4 * random.random())
