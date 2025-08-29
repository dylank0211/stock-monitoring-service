stock-monitor/
├── README.md
├── pyproject.toml
├── .gitignore
├── .env.example
├── Makefile
├── configs/
│   ├── default.yaml
│   └── symbols.dev.yaml
├── src/
│   └── monitor/
│       ├── __init__.py
│       ├── main.py                 # process entrypoint (uvloop + asyncio.run(App().start()))
│       ├── app.py                  # wires everything together (queues, tasks, lifecycle)
│       ├── config.py               # pydantic settings + YAML loading + env overrides
│       ├── logging.py              # structlog/json logging setup
│       ├── version.py
│       ├── utils/
│       │   ├── time.py             # monotonic helpers, utc now, second boundaries
│       │   ├── backoff.py          # jittered exponential backoff helpers
│       │   ├── market_calendar.py  # market open/close (pandas_market_calendars) + caching
│       │   └── types.py            # common TypedDict/dataclasses for ticks/bars
│       ├── ingest/
│       │   ├── __init__.py
│       │   ├── alpaca_ws.py        # async client: connect→auth→subscribe→stream Tick
│       │   └── parser.py           # message normalization → Tick(dataclass)
│       ├── data/
│       │   ├── __init__.py
│       │   ├── ring_buffer.py      # NumPy 1s OHLCV ring buffer (7200 deep)
│       │   ├── bars.py             # bar aggregator (ticks→finalize 1s bar events)
│       │   └── mirror_redis.py     # optional RedisTimeSeries mirror (feature-flagged)
│       ├── indicators/
│       │   ├── __init__.py
│       │   ├── base.py             # Indicator interface + State protocol
│       │   ├── registry.py         # string→indicator factory
│       │   ├── ema.py
│       │   ├── pct_change.py
│       │   └── rsi.py
│       ├── alerts/
│       │   ├── __init__.py
│       │   ├── rules.py            # Rule dataclass (threshold, hysteresis, persist, cooldown)
│       │   ├── evaluator.py        # runs rules over indicator state; emits AlertEvents
│       │   ├── dedupe.py           # LRU/ttl dedup for (symbol,rule,timebucket)
│       │   └── state.py            # per-symbol rule states (active, cooldown_until, hits)
│       ├── notify/
│       │   ├── __init__.py
│       │   ├── telegram.py         # async Telegram client, backoff, rate limiting
│       │   └── queue.py            # asyncio.Queue façade with metrics
│       ├── telemetry/
│       │   ├── __init__.py
│       │   ├── metrics.py          # Prometheus client & metric definitions
│       │   └── health.py           # health/readiness evaluators
│       └── server/
│           ├── __init__.py
│           └── http.py             # FastAPI app → /healthz, /readyz, /metrics
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── unit/
│   │   ├── test_ring_buffer.py
│   │   ├── test_bars.py
│   │   ├── test_indicators.py
│   │   ├── test_rules.py
│   │   └── test_dedupe.py
│   └── integration/
│       ├── test_end_to_end_replay.py   # feeds replay trace, asserts alerts
│       └── fixtures/
│           └── replay_ticks.jsonl
├── docker/
│   ├── Dockerfile
│   └── compose.yaml
├── infra/
│   ├── gce_startup.sh              # installs Docker, writes env, docker compose up
│   ├── mig_create.sh               # gcloud commands to build MIG + health check
│   └── prom-alert-rules.yaml       # Cloud Monitoring/Prometheus alert rules
└── scripts/
    ├── run_local.sh                # poetry run … with local env
    └── format_check.sh             # ruff/black/mypy hooks
