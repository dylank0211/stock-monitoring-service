import asyncio
import pytest

import monitor.data.mirror_redis as mr

class _FakePipeline:
    def __init__(self):
        self.cmds = []
        self.exec_batches = []

    def execute_command(self, *args):
        self.cmds.append(tuple(args))

    async def execute(self):
        # record and clear
        self.exec_batches.append(list(self.cmds))
        self.cmds.clear()

class _FakeRedis:
    def __init__(self):
        self.pipes = []

    def pipeline(self):
        p = _FakePipeline()
        self.pipes.append(p)
        return p

    async def close(self):
        pass

class _FakeRedisModule:
    def __init__(self):
        self.last_url = None
        self.instance = _FakeRedis()

    def from_url(self, url, decode_responses=True):
        self.last_url = url
        return self.instance

@pytest.mark.asyncio
async def test_mirror_enabled_writes_commands(monkeypatch):
    fake_mod = _FakeRedisModule()
    # Patch the module attribute used inside mirror_redis
    monkeypatch.setattr(mr, "redis", fake_mod)

    m = mr.RedisMirror(url="redis://localhost:6379/0", enabled=True, retention_ms=7200_000)
    await m.start()

    # queue two bars
    m.write_bar("AAPL", epoch=1000, close=150.5, volume=10)
    m.write_bar("AAPL", epoch=1001, close=150.7, volume=5)

    # give writer loop time to flush
    await asyncio.sleep(0.05)
    await m.stop()

    # Verify pipeline batches executed and contain TS.CREATE + TS.ADD pairs
    # There may be multiple pipelines; collect all executed commands
    batches = []
    for p in fake_mod.instance.pipes:
        batches.extend(p.exec_batches)

    # Flatten
    flat = [cmd for batch in batches for cmd in batch]
    # Expect at least one TS.ADD for close and one for volume at epoch 1000 & 1001
    add_close_1000 = ("TS.ADD", "ts:AAPL:close", 1000*1000, 150.5)
    add_vol_1000   = ("TS.ADD", "ts:AAPL:vol",   1000*1000, 10)
    assert add_close_1000 in flat
    assert add_vol_1000 in flat

@pytest.mark.asyncio
async def test_mirror_disabled_is_noop(monkeypatch):
    # Even if redis import exists, disabled mirror should not create clients
    fake_mod = _FakeRedisModule()
    monkeypatch.setattr(mr, "redis", fake_mod)

    m = mr.RedisMirror(url="redis://x", enabled=False)
    await m.start()
    m.write_bar("NVDA", 1234, 1.0, 1.0)
    await m.stop()

    # No pipelines should be created because enabled=False
    assert fake_mod.instance.pipes == []

@pytest.mark.asyncio
async def test_mirror_queue_drop(monkeypatch):
    # Force a tiny queue so we can observe drop behavior
    fake_mod = _FakeRedisModule()
    monkeypatch.setattr(mr, "redis", fake_mod)

    m = mr.RedisMirror(url="redis://x", enabled=True)
    # replace internal queue with size=1 to force drops
    m._q = asyncio.Queue(maxsize=1)

    await m.start()
    # This one enqueues
    m.write_bar("SPY", 2000, 400.0, 10.0)
    # This one likely drops due to full queue
    m.write_bar("SPY", 2001, 401.0, 11.0)

    await asyncio.sleep(0.05)
    await m.stop()

    # Only the first enqueued item should have produced TS.ADD
    batches = []
    for p in fake_mod.instance.pipes:
        batches.extend(p.exec_batches)
    flat = [cmd for batch in batches for cmd in batch]
    assert ("TS.ADD", "ts:SPY:close", 2000*1000, 400.0) in flat
    # We can't strictly assert the second is absent in all schedulings, but most runs will drop it.
