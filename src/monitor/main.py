import os, asyncio, structlog
from dotenv import load_dotenv; load_dotenv()

from monitor.ingest.alpaca_ws import AlpacaWS, AlpacaWSConfig
from monitor.data.bars import BarAggregator
from monitor.alerts.evaluator import PriceDropEvaluator, EvaluatorConfig
from monitor.alerts.rules import PriceDropRule

# calendar helpers you added earlier
from monitor.utils.market_calendar import is_market_open_now, session_state_now

log = structlog.get_logger()

async def main():
    stream_url = os.getenv("ALPACA_STREAM_URL", "wss://stream.data.alpaca.markets/v2/sip")
    key_id     = os.getenv("ALPACA_KEY_ID")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    # Normalize symbols (avoid leading/trailing spaces)
    symbols_env = os.getenv("SYMBOLS", "AAPL,NVDA")
    symbols     = [s.strip().upper() for s in symbols_env.split(",") if s.strip()]

    q_ticks  = asyncio.Queue(maxsize=10_000)
    q_bars   = asyncio.Queue(maxsize=2_000)
    q_alerts = asyncio.Queue(maxsize=2_000)

    # Dynamic heartbeat: tight in RTH, looser in extended, off when closed
    def heartbeat_secs_getter():
        phase = session_state_now().phase  # "premarket" | "regular" | "afterhours" | "closed"
        if phase == "regular":
            return 8.0
        if phase in ("premarket", "afterhours"):
            return 45.0
        return None  # closed → don't enforce heartbeat

    ingestor = AlpacaWS(
        AlpacaWSConfig(
            stream_url=stream_url,
            key_id=key_id,
            secret_key=secret_key,
            symbols=symbols,
            expect_heartbeat_s=8.0,     # fallback if getter isn't used
            subscribe_trades=True,
            subscribe_quotes=True,      # prove trades first; turn on later
            subscribe_bars=True,
        ),
        q_ticks,
        market_is_open=lambda: is_market_open_now("any"),  # count extended hours as open
        heartbeat_secs_getter=heartbeat_secs_getter,       # NEW: dynamic heartbeat by phase
    )

    aggregator = BarAggregator(
        symbols=symbols,
        buffer_seconds=7200,
        q_ticks=q_ticks,
        q_bars=q_bars
    )

    alerts = PriceDropEvaluator(
        q_bars=q_bars,
        get_ring=aggregator.get_ring,
        q_alerts=q_alerts,
        cfg=EvaluatorConfig(rule=PriceDropRule(drop_threshold=0.01)),  # 1%
    )

    async def notifier_loop():
        while True:
            evt = await q_alerts.get()
            log.info("ALERT", **evt)

    async def debug_bar_logger(q_bars, get_ring):
        from datetime import datetime
        while True:
            symbol, epoch = await q_bars.get()
            ring = get_ring(symbol)
            v = ring.view_last(1)
            if not v.slices:
                continue
            ep, o, h, l, c, vol = v.slices[-1]
            i = -1
            ts = datetime.utcfromtimestamp(int(ep[i])).strftime("%Y-%m-%d %H:%M:%S")
            print(f"BAR {symbol} {ts} O={o[i]:.3f} H={h[i]:.3f} L={l[i]:.3f} C={c[i]:.3f} V={vol[i]:.0f}")

    await asyncio.gather(
        ingestor.start(),
        aggregator.start(),
        alerts.start(),
        debug_bar_logger(q_bars, aggregator.get_ring),
        notifier_loop(),
    )

if __name__ == "__main__":
    asyncio.run(main())
