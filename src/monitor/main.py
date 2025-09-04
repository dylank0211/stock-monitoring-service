# src/monitor/main.py
import os
import asyncio
import structlog
from dotenv import load_dotenv

from monitor.ingest.alpaca_ws import AlpacaWS, AlpacaWSConfig
from monitor.data.bars import BarAggregator, BarConfig
from monitor.alerts.evaluator import PriceDropEvaluator, EvaluatorConfig
from monitor.alerts.rules import PriceDropRule

from monitor.indicators.vwap import VwapEngine, VwapConfig
from monitor.indicators.basic_indicators import IndicatorsEngine, IndicatorsConfig

from monitor.utils.market_calendar import is_market_open_now, session_state_now

# Redis storage helpers (make sure you created src/monitor/storage/redis_metrics.py)
from redis.asyncio import Redis
from storage.redis_metrics import write_bar_bundle, bootstrap_symbols

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
                log.warning("bars_fanout_drop", dest=repr(q))


def today_rth_open_epoch(tz_name: str = "America/New_York") -> int:
    from datetime import datetime, time
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)
    rth_open_local = datetime.combine(now_local.date(), time(9, 30), tzinfo=tz)
    return int(rth_open_local.timestamp())


# ---------------------------
# Join, Log, and Store
# ---------------------------

async def feeder_bar(q_bars_in: asyncio.Queue, q_join: asyncio.Queue, get_ring):
    """Convert (symbol, epoch) → ('bar', symbol, epoch, {o,h,l,c,v}) and forward to joiner."""
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
            # Fallback: last bar (rare)
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


async def join_and_log(q_join: asyncio.Queue, bar_seconds: int, q_store: asyncio.Queue | None = None):
    """
    Join BAR + VWAP + IND messages by (symbol, epoch).
    - Prints one unified line.
    - Optionally emits a consolidated payload to q_store for Redis persistence.
    """
    from datetime import datetime, UTC

    # cache[(sym, epoch)] = {"bar": {...}, "vwap": {...}, "ind": {...}}
    cache = {}

    def fmt_ts(e: int) -> str:
        return datetime.fromtimestamp(e, UTC).strftime("%Y-%m-%d %H:%M:%S")

    while True:
        kind, sym, epoch, payload = await q_join.get()
        entry = cache.get((sym, epoch))
        if entry is None:
            entry = {}
            cache[(sym, epoch)] = entry
        entry[kind] = payload

        # Print once we have all three pieces
        if "bar" in entry and "vwap" in entry and "ind" in entry:
            b = entry["bar"]
            v = entry["vwap"]
            ind = entry["ind"]

            # Build a compact, stable field order
            parts = [
                f"BAR {sym} {fmt_ts(epoch)}",
                f"O={b['o']:.3f}",
                f"H={b['h']:.3f}",
                f"L={b['l']:.3f}",
                f"C={b['c']:.3f}",
                f"V={b['v']:.0f}",
                f"VWAP={v['vwap']:.4f}",
            ]

            preferred = ["EMA9", "EMA20", "EMA50", "RSI14", "RSI60", "RSI120",
                         "ATR14", "MACD12_26", "MACDsig9", "MACDhist", "OBV"]
            for k in preferred:
                if k in ind:
                    val = ind[k]
                    if k == "OBV":
                        parts.append(f"{k}={val:.0f}")
                    elif k.startswith(("ATR", "EMA", "MACD")):
                        parts.append(f"{k}={val:.4f}")
                    else:  # RSI
                        parts.append(f"{k}={val:.2f}")

            # Any remaining indicators
            for k in sorted(ind.keys()):
                if k in preferred:
                    continue
                val = ind[k]
                try:
                    parts.append(f"{k}={float(val):.4f}")
                except Exception:
                    parts.append(f"{k}={val}")

            print(" ".join(parts))

            # Optional: emit to storage
            if q_store is not None:
                full = {"o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"], "v": b["v"], "vwap": v["vwap"]}
                full.update(ind)
                await q_store.put(("full", sym, epoch, full))

            # Clean up
            del cache[(sym, epoch)]


async def persist_full(q_store: asyncio.Queue, symbols: list[str]):
    """
    Consumes consolidated ('full', sym, epoch, dict) payloads and writes to RedisTimeSeries.
    """
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = Redis.from_url(REDIS_URL)

    # Create the labeled time series per symbol (idempotent; 24h retention inside helper)
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
            vwap=full["vwap"],
            indicators=indicators,
        )


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
    q_bars_eval = asyncio.Queue(maxsize=2_000)     # alerts
    q_bars_vwap = asyncio.Queue(maxsize=2_000)     # vwap engine
    q_bars_ind  = asyncio.Queue(maxsize=2_000)     # indicators engine
    q_bars_log  = asyncio.Queue(maxsize=2_000)     # feeder_bar (to joiner)

    # Engine outputs
    q_alerts   = asyncio.Queue(maxsize=2_000)
    q_vwap_out = asyncio.Queue(maxsize=2_000)
    q_ind_out  = asyncio.Queue(maxsize=2_000)

    # Joiner + storage queues
    q_join  = asyncio.Queue(maxsize=5_000)
    q_store = asyncio.Queue(maxsize=5_000)

    # Heartbeat policy
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

    # Alerts (example rule)
    alerts = PriceDropEvaluator(
        q_bars=q_bars_eval,
        get_ring=aggregator.get_ring,
        q_alerts=q_alerts,
        cfg=EvaluatorConfig(rule=PriceDropRule(drop_threshold=0.01)),  # 1%
    )

    # VWAP
    vwap_engine = VwapEngine(
        aggregator=aggregator,
        q_in=q_bars_vwap,
        q_out=q_vwap_out,
        cfg=VwapConfig(basis="hlc3"),
    )

    # Indicators (EMA/RSI/ATR/MACD/OBV tuned for 30s bars)
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

    # Seed engines from existing rings so mid-session starts have sensible initial values
    session_open_epoch = today_rth_open_epoch()
    # If you added VwapEngine.seed_from_rings_since(session_open_epoch, symbols), call it here.
    ind_engine.seed_all(symbols)

    await asyncio.gather(
        # Feeds
        ingestor.start(),
        aggregator.start(),

        # Fan-out finalized bars to all downstream consumers
        bars_fanout(q_bars_up, q_bars_eval, q_bars_vwap, q_bars_ind, q_bars_log),

        # Engines
        alerts.start(),
        vwap_engine.start(),
        ind_engine.start(),

        # Joiner feeders
        feeder_bar(q_bars_log, q_join, aggregator.get_ring),
        feeder_vwap(q_vwap_out, q_join),
        feeder_ind(q_ind_out, q_join),

        # Unified printer + storage emitter
        join_and_log(q_join, bar_seconds, q_store),

        # Redis persistence
        persist_full(q_store, symbols),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
