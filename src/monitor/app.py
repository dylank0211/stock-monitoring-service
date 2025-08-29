from monitor.ingest.alpaca_ws import AlpacaWS, AlpacaWSConfig

self.q_ticks = asyncio.Queue(maxsize=10_000)

cfg = AlpacaWSConfig(
    stream_url=self.cfg.alpaca.stream_url,
    key_id=self.cfg.alpaca.key_id,
    secret_key=self.cfg.alpaca.secret_key,
    symbols=self.cfg.symbols,
)

self.alpaca = AlpacaWS(cfg, self.q_ticks, market_is_open=self.market_calendar.is_open_now)

tasks.append(asyncio.create_task(self.alpaca.start(), name="alpaca_ws"))