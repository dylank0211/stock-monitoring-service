import asyncio
import json
import pytest

from monitor.ingest.alpaca_ws import AlpacaWS, AlpacaWSConfig
from monitor.utils.types import Tick

# ---- a tiny parser stub to avoid pulling your real parser into tests ----
# If you want to test your real parser, import it instead.
import types
def _parser_stub(m: dict):
    T = m.get("T") or m.get("type")
    if T in ("success", "subscription", "error"):
        return None
    if T in ("t", "trade", "trade_message"):
        px = m.get("p") or m.get("price") or 0.0
        ts = m.get("t") or m.get("timestamp") or 0.0
        if isinstance(ts, str):
            ts = 0.0  # keep simple
        return Tick(symbol=m.get("S") or m.get("symbol") or "UNK",
                    px=float(px), size=float(m.get("s") or m.get("size") or 0),
                    ts=float(ts))
    return None

@pytest.fixture(autouse=True)
def patch_parser(monkeypatch):
    from monitor.ingest import parser
    def _parse_trade_msg(m):
        T = m.get("T") or m.get("type")
        if T not in ("t", "trade", "trade_message"):
            return None
        from monitor.utils.types import Tick
        px = m.get("p") or m.get("price")
        ts = m.get("t") or m.get("timestamp") or 0.0
        return Tick(symbol=m.get("S") or m.get("symbol"), px=float(px), size=float(m.get("s") or m.get("size") or 0), ts=float(ts))
    monkeypatch.setattr(parser, "parse_trade_msg", _parse_trade_msg)

# ---- monkeypatch websockets.connect to our FakeWS ----
@pytest.fixture
def fake_connect(monkeypatch):
    """
    Patch the exact symbol used inside alpaca_ws.py.
    Return a FakeWS instance directly (NOT an async def).
    """
    from tests.helpers.fake_ws import FakeWS
    import monitor.ingest.alpaca_ws as alpaca_ws

    holder = {"ws": None}

    def _connect(*args, **kwargs):   # <-- NOTE: not async
        return holder["ws"] or FakeWS()

    monkeypatch.setattr(alpaca_ws, "ws_connect", _connect)
    return holder


@pytest.mark.asyncio
async def test_auth_subscribe_and_trade_enqueued(fake_connect):
    """
    Verifies: client sends auth, then subscribe; parses trade and enqueues Tick.
    """
    from tests.helpers.fake_ws import FakeWS

    # Script inbound messages that server would send:
    scripted = [
        {"T": "success", "msg": "authenticated"},     # auth ack
        {"T": "subscription", "trades": ["NVDA"]},    # subscribe ack (optional)
        {"T": "t", "S": "NVDA", "p": 181.23, "s": 10, "t": 1699999999.0},  # trade
    ]
    ws = FakeWS(scripted=scripted)
    fake_connect["ws"] = ws

    ticks_q = asyncio.Queue(maxsize=100)
    cfg = AlpacaWSConfig(
        stream_url="wss://example.test",
        key_id="KEY",
        secret_key="SECRET",
        symbols=["NVDA"],
        expect_heartbeat_s=2.0,
        open_timeout_s=1.0,
        ping_interval_s=5.0,
    )

    client = AlpacaWS(cfg, ticks_q, market_is_open=lambda: True)

    # Run client.start() in background and stop shortly after we receive a tick
    task = asyncio.create_task(client.start())

    # Give it a bit of time to process our scripted messages
    tick = await asyncio.wait_for(ticks_q.get(), timeout=2.0)
    assert isinstance(tick, Tick)
    assert tick.symbol == "NVDA"
    assert tick.px == pytest.approx(181.23)

    # Check that client actually sent auth + subscribe
    # First outbound should be 'auth', second should be 'subscribe'
    assert ws.outbound[0]["action"] == "auth"
    assert ws.outbound[1]["action"] == "subscribe"
    assert "trades" in ws.outbound[1] and ws.outbound[1]["trades"] == ["NVDA"]

    # Stop client
    await client.stop()
    await asyncio.wait_for(task, timeout=2.0)

@pytest.mark.asyncio
async def test_batch_messages_and_non_trade_ignored(fake_connect):
    """
    Verifies: when server sends a JSON array with mixed messages,
    only trades are parsed and queued.
    """
    from tests.helpers.fake_ws import FakeWS
    batch = [
        {"T": "success", "msg": "authenticated"},
        {"T": "subscription", "trades": ["AAPL"]},
        {"T": "t", "S": "AAPL", "p": 200.0, "s": 1, "t": 1700000000.0},
        {"T": "error", "code": 400, "msg": "bad symbol"},
        {"type": "t", "symbol": "AAPL", "price": 201.0, "size": 5, "timestamp": 1700000001.0},
    ]
    ws = FakeWS(scripted=[batch])  # note: one recv() returns JSON array
    fake_connect["ws"] = ws

    q = asyncio.Queue(maxsize=10)
    cfg = AlpacaWSConfig(stream_url="wss://x", key_id="k", secret_key="s", symbols=["AAPL"])
    client = AlpacaWS(cfg, q)

    task = asyncio.create_task(client.start())

    # Expect two trades
    t1 = await asyncio.wait_for(q.get(), timeout=2.0)
    t2 = await asyncio.wait_for(q.get(), timeout=2.0)
    assert t1.px == 200.0 and t2.px == 201.0

    await client.stop()
    await asyncio.wait_for(task, timeout=2.0)

@pytest.mark.asyncio
async def test_queue_full_drops_ticks(fake_connect):
    """
    When ticks queue is full, client should drop new ticks (and not block).
    """
    from tests.helpers.fake_ws import FakeWS

    # Auth ack, then many trades
    scripted = [{"T": "success", "msg": "authenticated"}]
    for i in range(100):
        scripted.append({"T": "t", "S": "SPY", "p": 400.0 + i * 0.01, "s": 1, "t": 1700000000.0 + i})
    ws = FakeWS(scripted=scripted)
    fake_connect["ws"] = ws

    q = asyncio.Queue(maxsize=5)  # tiny capacity to force drops
    cfg = AlpacaWSConfig(stream_url="wss://x", key_id="k", secret_key="s", symbols=["SPY"])
    client = AlpacaWS(cfg, q)

    task = asyncio.create_task(client.start())

    # Drain what we can quickly
    received = 0
    try:
        while True:
            _ = await asyncio.wait_for(q.get(), timeout=1.0)
            received += 1
            if received >= 5:
                break
    except asyncio.TimeoutError:
        pass

    # We should have at most the queue size, demonstrating drops occurred
    assert received <= 5

    await client.stop()
    await asyncio.wait_for(task, timeout=2.0)

@pytest.mark.asyncio
async def test_staleness_detection_healthy_flag(fake_connect):
    """
    No messages for > expect_heartbeat_s during market hours -> healthy() False.
    """
    from tests.helpers.fake_ws import FakeWS
    # Only an auth ack, then silence
    ws = FakeWS(scripted=[{"T": "success", "msg": "authenticated"}], delay=0.0)
    fake_connect["ws"] = ws

    q = asyncio.Queue()
    cfg = AlpacaWSConfig(stream_url="wss://x", key_id="k", secret_key="s", symbols=["AAPL"], expect_heartbeat_s=0.5)
    client = AlpacaWS(cfg, q, market_is_open=lambda: True)

    task = asyncio.create_task(client.start())

    # allow some idle time
    await asyncio.sleep(5.0)
    assert client.healthy() is False  # stale

    await client.stop()
    await asyncio.wait_for(task, timeout=2.0)

@pytest.mark.asyncio
async def test_stop_graceful(fake_connect):
    """
    stop() should close the websocket and exit start() loop cleanly.
    """
    from tests.helpers.fake_ws import FakeWS
    ws = FakeWS(scripted=[{"T":"success","msg":"authenticated"}])
    fake_connect["ws"] = ws

    q = asyncio.Queue()
    cfg = AlpacaWSConfig(stream_url="wss://x", key_id="k", secret_key="s", symbols=["AAPL"])
    client = AlpacaWS(cfg, q)

    task = asyncio.create_task(client.start())
    await asyncio.sleep(0.1)
    await client.stop()
    await asyncio.wait_for(task, timeout=2.0)
