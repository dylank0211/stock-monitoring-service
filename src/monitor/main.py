# src/monitor/main.py
import os
import asyncio
import structlog
from dotenv import load_dotenv

from monitor.ingest.alpaca_ws import AlpacaWS, AlpacaWSConfig
from monitor.data.bars import BarAggregator, BarConfig

from monitor.indicators.vwap import VwapEngine, VwapConfig
from monitor.indicators.basic_indicators import IndicatorsEngine, IndicatorsConfig

from monitor.utils.market_calendar import is_market_open_now, session_state_now

# Alerts (Redis-backed, extremum-based)
from monitor.alerts.rules import PercentMoveRule
from monitor.alerts.evaluator_redis import PercentMoveEvaluatorRedis, RedisEvaluatorConfig
from monitor.alerts.notifiers import ConsoleNotifier
from monitor.alerts.formatting import format_alert_pretty


# Redis storage helpers
from redis.asyncio import Redis
from storage.redis_metrics import write_bar_bundle, bootstrap_symbols

# Telegram notifier (optional)
from monitor.notify.queue import NotifyQueue
from monitor.notify.telegram import TelegramNotifier, config_from_env

load_dotenv()
log = structlog.get_logger()


# ---------------------------
# Utilities
# ---------------------------

async def bars_fanout(src: asyncio.Queue, *dests: asyncio.Queue):
    """Broadcast every (symbol, epoch) bar event from `src` to all `dests`."""
    while True:
        item = await src.get()
        for q in dests:
            try:
                q.put_nowait(item)
            except asyncio.QueueFull:
                # non-fatal: downstream slow; skip to keep hot path
                pass


def today_rth_open_epoch(tz_name: str = "America/New_York") -> int:
    from datetime import datetime, time
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)
    rth_open_local = datetime.combine(now_local.date(), time(9, 30), tzinfo=tz)
    return int(rth_open_local.timestamp())


# ---------------------------
# Join & Store (no printing)
# ---------------------------

async def feeder_bar(q_bars_in: asyncio.Queue, q_join: asyncio.Queue, get_ring):
    """
    Convert (symbol, epoch) → ('bar', symbol, epoch, {o,h,l,c,v}) for the joiner.
    """
    import numpy as np
    while True:
        symbol, epoch = await q_bars_in.get()
        ring = get_ring(symbol)
        view = ring.view_last(ring.size or 1)
        found = False
        for (ep, o, h, l, c, v) in view.slices:
            idxs = np.where(ep == epoch)[0]
            if idxs.size:
                i = int(idxs[-1])
                payload = {
                    "o": float(o[i]),
                    "h": float(h[i]),
                    "l": float(l[i]),
                    "c": float(c[i]),
                    "v": float(v[i]),
                }
                await q_join.put(("bar", symbol, int(epoch), payload))
                found = True
                break
        if not found:
            # Very rare: fallback to latest
            v2 = ring.view_last(1)
            if v2.slices:
                ep, o, h, l, c, vol = v2.slices[-1]
                i = -1
                payload = {
                    "o": float(o[i]),
                    "h": float(h[i]),
                    "l": float(l[i]),
                    "c": float(c[i]),
                    "v": float(vol[i]),
                }
                await q_join.put(("bar", symbol, int(ep[i]), payload))


async def feeder_vwap(q_vwap_out: asyncio.Queue, q_join: asyncio.Queue):
    """Convert (symbol, epoch, vwap) → ('vwap', symbol, epoch, {'vwap': ...})."""
    while True:
        symbol, epoch, vwap = await q_vwap_out.get()
        await q_join.put(("vwap", symbol, int(epoch), {"vwap": float(vwap)}))


async def feeder_ind(q_ind_out: asyncio.Queue, q_join: asyncio.Queue):
    """Convert (symbol, epoch, dict) → ('ind', symbol, epoch, dict)."""
    while True:
        symbol, epoch, updates = await q_ind_out.get()
        ups = {k: float(v) for k, v in updates.items()}  # ensure floats
        await q_join.put(("ind", symbol, int(epoch), ups))


async def join_and_store(q_join: asyncio.Queue, q_store: asyncio.Queue | None = None):
    """
    Join BAR + VWAP + IND messages by (symbol, epoch) and emit a consolidated payload
    to q_store for Redis persistence. No printing here.
    """
    cache: dict[tuple[str, int], dict] = {}

    while True:
        kind, sym, epoch, payload = await q_join.get()
        entry = cache.get((sym, epoch))
        if entry is None:
            entry = {}
            cache[(sym, epoch)] = entry
        entry[kind] = payload

        if "bar" in entry and "vwap" in entry and "ind" in entry:
            b = entry["bar"]
            v = entry["vwap"]
            ind = entry["ind"]

            if q_store is not None:
                full = {"o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"], "v": b["v"], "vwap": v["vwap"]}
                full.update(ind)
                try:
                    q_store.put_nowait(("full", sym, epoch, full))
                except asyncio.QueueFull:
                    # drop persist if backpressure; hot path must not block
                    pass

            del cache[(sym, epoch)]


async def persist_full(q_store: asyncio.Queue, symbols: list[str]):
    """
    Consumes ('full', sym, epoch, dict) and writes to RedisTimeSeries.
    """
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = Redis.from_url(REDIS_URL)

    # create TS keys (idempotent) with 24h retention & labels
    await bootstrap_symbols(r, symbols)

    while True:
        kind, sym, epoch, full = await q_store.get()
        if kind != "full":
            continue
        epoch_ms = int(epoch * 1000)  # RTS uses milliseconds
        indicators = {k: v for k, v in full.items() if k not in ("o", "h", "l", "c", "v", "vwap")}
        await write_bar_bundle(
            r, sym, epoch_ms,
            o=full["o"], h=full["h"], l=full["l"], c=full["c"], v=full["v"],
            vwap=full["vwap"],  # your redis_metrics skips/guards NaN internally
            indicators=indicators,
        )


# ---------------------------
# Optional: live bar printer
# ---------------------------

async def bar_printer(q_bars_print: asyncio.Queue, get_ring, vwap_engine):
    """Print finalized bars (and last VWAP) as they arrive. Toggle with PRINT_BARS=1."""
    from datetime import datetime, UTC
    import numpy as np

    show_vwap = os.getenv("PRINT_VWAP_WITH_BARS", "1").lower() in ("1", "true", "yes")

    while True:
        symbol, epoch = await q_bars_print.get()
        ring = get_ring(symbol)

        # find the bar we were notified about
        view = ring.view_last(ring.size or 1)
        printed = False
        for (ep, o, h, l, c, v) in view.slices:
            idxs = np.where(ep == epoch)[0]
            if idxs.size:
                i = int(idxs[-1])
                ts = datetime.fromtimestamp(int(ep[i]), UTC).strftime("%Y-%m-%d %H:%M:%S")
                parts = [
                    f"BAR {symbol} {ts}",
                    f"O={float(o[i]):.3f}",
                    f"H={float(h[i]):.3f}",
                    f"L={float(l[i]):.3f}",
                    f"C={float(c[i]):.3f}",
                    f"V={float(v[i]):.0f}",
                ]
                if show_vwap:
                    last_vwap = vwap_engine.get_last(symbol)
                    if isinstance(last_vwap, (int, float)) and last_vwap == last_vwap:  # not NaN
                        parts.append(f"VWAP={last_vwap:.4f}")
                print(" ".join(parts))
                printed = True
                break

        if not printed:
            # fallback: print most recent bar if exact epoch not found
            v2 = ring.view_last(1)
            if v2.slices:
                ep, o, h, l, c, v = v2.slices[-1]
                i = -1
                ts = datetime.fromtimestamp(int(ep[i]), UTC).strftime("%Y-%m-%d %H:%M:%S")
                parts = [
                    f"BAR {symbol} {ts}",
                    f"O={float(o[i]):.3f}",
                    f"H={float(h[i]):.3f}",
                    f"L={float(l[i]):.3f}",
                    f"C={float(c[i]):.3f}",
                    f"V={float(v[i]):.0f}",
                ]
                if show_vwap:
                    last_vwap = vwap_engine.get_last(symbol)
                    if isinstance(last_vwap, (int, float)) and last_vwap == last_vwap:
                        parts.append(f"VWAP={last_vwap:.4f}")
                print(" ".join(parts))


# ---------------------------
# Telegram startup ping helper
# ---------------------------

async def telegram_startup_ping(tg_notifier: TelegramNotifier):
    # give the notifier a moment to create its aiohttp session in start()
    await asyncio.sleep(0)
    try:
        await tg_notifier._send("✅ Stock monitor started and Telegram is live.")
    except Exception as e:
        log.warning("telegram_startup_ping_failed", err=str(e))


# ---------------------------
# Main
# ---------------------------

async def main():
    stream_url = os.getenv("ALPACA_STREAM_URL", "wss://stream.data.alpaca.markets/v2/sip")
    key_id = os.getenv("ALPACA_KEY_ID")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    # Symbols
    symbols_env = os.getenv("SYMBOLS", "AAPL,NVDA,AMD,QQQ,SOFI")
    symbols = [s.strip().upper() for s in symbols_env.split(",") if s.strip()]

    # Queues
    q_ticks = asyncio.Queue(maxsize=10_000)

    # Aggregator upstream queue (single source)
    q_bars_up = asyncio.Queue(maxsize=2_000)

    # Fan-out queues (multiple consumers)
    q_bars_eval = asyncio.Queue(maxsize=2_000)     # alerts (timing)
    q_bars_vwap = asyncio.Queue(maxsize=2_000)     # vwap engine
    q_bars_ind  = asyncio.Queue(maxsize=2_000)     # indicators engine
    q_bars_join = asyncio.Queue(maxsize=2_000)     # feeder_bar → joiner
    q_bars_print = asyncio.Queue(maxsize=2_000)    # optional: live bar printing

    # Engine outputs
    q_vwap_out = asyncio.Queue(maxsize=2_000)
    q_ind_out  = asyncio.Queue(maxsize=2_000)

    # Joiner + storage
    q_join  = asyncio.Queue(maxsize=5_000)
    q_store = asyncio.Queue(maxsize=5_000)

    # Alerts output (from evaluators)
    q_alerts_move = asyncio.Queue(maxsize=2_000)

    # Heartbeat policy for Alpaca WS
    def heartbeat_secs_getter():
        phase = session_state_now().phase  # "premarket" | "regular" | "afterhours" | "closed"
        if phase == "regular":
            return 8.0
        if phase in ("premarket", "afterhours"):
            return 45.0
        return None

    # Ingestor
    ingestor = AlpacaWS(
        AlpacaWSConfig(
            stream_url=stream_url,
            key_id=key_id,
            secret_key=secret_key,
            symbols=symbols,
            expect_heartbeat_s=8.0,
            subscribe_trades=True,
            subscribe_quotes=False,
            subscribe_bars=False,
        ),
        q_ticks,
        market_is_open=lambda: is_market_open_now("any"),
        heartbeat_secs_getter=heartbeat_secs_getter,
    )

    # Bars (30s) over last 2h
    bar_seconds = 30
    aggregator = BarAggregator(
        symbols=symbols,
        buffer_seconds=7200,
        q_ticks=q_ticks,
        q_bars=q_bars_up,
        cfg=BarConfig(
            buffer_seconds=7200,
            bar_seconds=bar_seconds,
            emit_gap_fill=True,
            idle_finalize_ms=300,
        ),
    )

    # Engines: VWAP
    vwap_engine = VwapEngine(
        aggregator=aggregator,
        q_in=q_bars_vwap,
        q_out=q_vwap_out,
        cfg=VwapConfig(basis="hlc3"),
    )

    # Engines: Indicators (EMA/RSI/ATR/MACD/OBV tuned for 30s bars)
    ind_engine = IndicatorsEngine(
        aggregator=aggregator,
        q_in=q_bars_ind,
        q_out=q_ind_out,
        cfg=IndicatorsConfig(
            emas=[9, 20, 50],
            rsis=[14, 60, 120],
            atr_periods=14,
            macd_tuple=(12, 26, 9),
            include_obv=True,
            emit_on_update=True,
        ),
    )

    # Seed indicators from existing rings (helps if you start mid-session)
    ind_engine.seed_all(symbols)

    # Redis client (shared) for Redis-backed alerts
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis_client = Redis.from_url(REDIS_URL)

    # ----- Alerts: separate UP and DOWN extremum rules -----
    up_rule = PercentMoveRule(
        name="pct_up_30m_0p25",
        window_seconds=1800,
        threshold=0.0025,
        direction="up",
        cooldown_seconds=180,          # 3 min
        dedupe_bucket_seconds=180,     # dedupe per 3-min bucket
        auto_resolve=False,
    )
    down_rule = PercentMoveRule(
        name="pct_down_30m_0p25",
        window_seconds=1800,
        threshold=0.0025,
        direction="down",
        cooldown_seconds=0,            # allow immediate down alerts after an up
        dedupe_bucket_seconds=60,      # but dedupe within 1-min buckets
        auto_resolve=False,
    )

    pct_eval_up = PercentMoveEvaluatorRedis(
        q_bars=q_bars_eval,
        q_alerts=q_alerts_move,
        redis=redis_client,
        cfg=RedisEvaluatorConfig(rule=up_rule, timeframe="30s", field="C"),
    )
    pct_eval_down = PercentMoveEvaluatorRedis(
        q_bars=q_bars_eval,
        q_alerts=q_alerts_move,
        redis=redis_client,
        cfg=RedisEvaluatorConfig(rule=down_rule, timeframe="30s", field="C"),
    )

    # ----- Notifications -----
    console_notifier = ConsoleNotifier(format_fn=lambda e: format_alert_pretty(e, "America/Chicago"))

    # Optional Telegram (built from env). If not configured, we skip it.
    telegram_enabled = False
    tg_notifier = None
    notify_q = None
    try:
        tg_cfg = config_from_env()  # raises if env missing
        notify_q = NotifyQueue(maxsize=2000)
        tg_notifier = TelegramNotifier(cfg=tg_cfg, alerts_queue=notify_q,
                                    format_fn=lambda e: format_alert_pretty(e, "America/Chicago"))        
        telegram_enabled = True
        log.info("telegram_enabled")
    except Exception:
        log.info("telegram_disabled_missing_env")

    async def notifier_router_loop():
        """
        Consume alerts from evaluators and fan out:
          - always print to console
          - if Telegram configured, enqueue to Telegram notifier queue
        """
        while True:
            evt = await q_alerts_move.get()
            await console_notifier.send(evt)
            if telegram_enabled and notify_q is not None:
                notify_q.try_put(evt)  # drop if full to protect hot path

    # Toggle live bar printing with env
    PRINT_BARS = os.getenv("PRINT_BARS", "0").lower() in ("1", "true", "yes")

    # ----- Run everything -----
    tasks = [
        # Feeds
        ingestor.start(),
        aggregator.start(),

        # Fan-out finalized bars to downstream consumers
        bars_fanout(q_bars_up, q_bars_eval, q_bars_vwap, q_bars_ind, q_bars_join, q_bars_print),

        # Engines
        vwap_engine.start(),
        ind_engine.start(),

        # Joiner feeders → consolidated payload
        feeder_bar(q_bars_join, q_join, aggregator.get_ring),
        feeder_vwap(q_vwap_out, q_join),
        feeder_ind(q_ind_out, q_join),

        # Consolidate and store to Redis (no printing)
        join_and_store(q_join, q_store),
        persist_full(q_store, symbols),

        # Alerts (Redis-backed, extremum-based) + notifier router
        pct_eval_up.start(),
        pct_eval_down.start(),
        notifier_router_loop(),
    ]
    if PRINT_BARS:
        tasks.append(bar_printer(q_bars_print, aggregator.get_ring, vwap_engine))
    if telegram_enabled and tg_notifier is not None:
        tasks.append(tg_notifier.start())
        tasks.append(telegram_startup_ping(tg_notifier))

    try:
        await asyncio.gather(*tasks)
    finally:
        # graceful shutdown to avoid unclosed sessions
        for obj in (
            pct_eval_up, pct_eval_down, ind_engine, vwap_engine, aggregator, ingestor
        ):
            try:
                await obj.stop()
            except Exception:
                pass
        if telegram_enabled and tg_notifier is not None:
            try:
                await tg_notifier.stop()
            except Exception:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
