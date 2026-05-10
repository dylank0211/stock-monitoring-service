# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies (requires Python 3.11+)
pip install -r requirements.txt

# Run the service
python -m monitor.main

# Run all tests
pytest

# Run a single test file
pytest tests/unit/test_bars.py

# Run with verbose output
pytest -v

# Docker dev (live-mounts src/)
docker-compose -f docker/docker-compose.yml --profile dev up

# Docker prod (code baked into image)
docker-compose -f docker/docker-compose.yml --profile prod up
```

**Required env vars** (in `.env` or shell):
```
ALPACA_KEY_ID, ALPACA_SECRET_KEY, SYMBOLS, REDIS_URL
TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID  # optional
```

## Architecture

The service is an async pipeline of independent `asyncio.Task`s connected by bounded queues. Each stage drops messages under backpressure to keep the WebSocket hot path responsive.

```
AlpacaWS → q_ticks → BarAggregator → q_bars_up → [fanout]
                                                    ├─ q_bars_eval  → PercentMoveEvaluatorRedis → q_alerts_move
                                                    ├─ q_bars_vwap  → VwapEngine
                                                    ├─ q_bars_ind   → IndicatorsEngine
                                                    ├─ q_bars_join  ─┐
                                                    └─ q_bars_print  │
                                                    join_and_store ──┘─ q_store → persist_full → RedisTimeSeries

q_alerts_move → notifier_router → ConsoleNotifier + TelegramNotifier (if configured)
```

**Key components:**

- **`src/monitor/ingest/alpaca_ws.py`** — WebSocket client with jittered reconnect backoff (0.25s→30s) and dynamic heartbeat monitoring based on market phase.
- **`src/monitor/data/bars.py`** — Clock-driven bar aggregation: all symbols roll at exact second boundaries so downstream consumers see synchronized bar epochs.
- **`src/monitor/data/ring_buffer.py`** — NumPy circular buffer (int64 epochs, float64 OHLCV); supports zero-copy contiguous/wrapped views used by indicator engines.
- **`src/monitor/indicators/`** — `VwapEngine` (session-aware, resets at market open) and `IndicatorsEngine` (EMA 9/20/50, RSI 14/60/120, ATR 14, MACD 12/26/9, OBV) — both seeded from existing ring history on startup.
- **`src/monitor/alerts/evaluator_redis.py`** — Compares each bar's close to the 30-min rolling LOW (up moves) or HIGH (down moves) in RedisTimeSeries. Applies per-direction cooldown, TTL dedup by `(symbol, rule, time_bucket)`, and a step re-alert gate requiring further extension beyond the last fired %.
- **`src/monitor/notify/telegram.py`** — Token-bucket rate limiter (1 msg/s, burst 3) with exponential backoff (0.5s→8s, 5 retries).
- **`src/storage/redis_metrics.py`** — Writes `ts:{symbol}:{timeframe}:{field}` keys to RedisTimeSeries with 24h retention; NaN/Inf values are filtered before write.

## Key Design Decisions

**Clock-driven bar finalization** (`bars.py`): Bars close at wall-clock boundaries, not on tick arrival. This is critical — downstream indicators and alert evaluators rely on synchronized epochs across all symbols.

**Extremum-based alerts** (`rules.py`, `evaluator_redis.py`): Rules fire when `close` moves ≥ threshold from the rolling window LOW or HIGH, not on simple threshold crossing. This avoids choppy false positives and is more meaningful for momentum signals.

**Redis as alert state store**: The evaluator queries RedisTimeSeries directly rather than maintaining in-process state, enabling persistence across restarts and potential horizontal scaling.

**Tuned for 30-second bars**: Default indicator periods (EMA 9/20/50, RSI 14/60/120) are calibrated for 30s bars. If `bar_seconds` changes, indicator config should be revisited.

## Testing

Tests live in `tests/unit/`. Async tests use `asyncio_mode=auto` (set in `pytest.ini` — no `@pytest.mark.asyncio` decorator needed).

`tests/helpers/fake_ws.py` provides a mock WebSocket for `AlpacaWS` tests without a live connection.

`tests/integration/test_end_to_end_replay.py` feeds a pre-recorded `replay_ticks.jsonl` through the full pipeline and asserts alert generation.
