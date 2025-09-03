import asyncio
import pytest

from monitor.data.bars import BarAggregator, BarConfig
from monitor.utils.types import Tick

@pytest.mark.asyncio
async def test_basic_aggregation_and_emit():
    q_ticks = asyncio.Queue()
    q_bars  = asyncio.Queue()
    agg = BarAggregator(symbols=["AAPL"], buffer_seconds=10, q_ticks=q_ticks, q_bars=q_bars,
                        cfg=BarConfig(buffer_seconds=10, emit_gap_fill=True, idle_finalize_ms=50))

    task = asyncio.create_task(agg.start())

    # two ticks in same second -> one finalized bar when second advances
    await q_ticks.put(Tick(symbol="AAPL", px=100.0, size=1, ts=1000.10))
    await q_ticks.put(Tick(symbol="AAPL", px=101.0, size=2, ts=1000.90))
    await q_ticks.put(Tick(symbol="AAPL", px=102.0, size=3, ts=1001.10))  # advances second -> finalize 1000

    # give aggregator a moment
    await asyncio.sleep(0.05)

    ring = agg.get_ring("AAPL")
    # first finalized bar is second 1000 with O=100,H=101,L=100,C=101,V=3
    v = ring.view_last(1)
    ep,o,h,l,c,vv = v.slices[0]
    assert ep[-1] == 1000
    assert o[-1] == pytest.approx(100.0)
    assert h[-1] == pytest.approx(101.0)
    assert l[-1] == pytest.approx(100.0)
    assert c[-1] == pytest.approx(101.0)
    assert vv[-1] == pytest.approx(3.0)

    # drain bar notifications
    seen = []
    while not q_bars.empty():
        seen.append(await q_bars.get())
    assert ("AAPL", 1000) in seen

    # stop (cancels task; start() flushes in finally)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

@pytest.mark.asyncio
async def test_gap_fill_between_seconds():
    q_ticks = asyncio.Queue()
    agg = BarAggregator(symbols=["NVDA"], buffer_seconds=10, q_ticks=q_ticks, q_bars=None,
                        cfg=BarConfig(buffer_seconds=10, emit_gap_fill=True, idle_finalize_ms=50))
    task = asyncio.create_task(agg.start())

    # Tick at 1000, then next tick jumps to 1003 -> expect bars at 1000, 1001 (flat), 1002 (flat)
    await q_ticks.put(Tick(symbol="NVDA", px=200.0, size=1, ts=1000.20))
    await q_ticks.put(Tick(symbol="NVDA", px=201.0, size=1, ts=1003.10))

    await asyncio.sleep(0.05)
    ring = agg.get_ring("NVDA")
    v = ring.view_last(3)
    epochs = []
    closes = []
    for sl in v.slices:
        epochs.extend(sl[0].tolist())
        closes.extend(sl[4].tolist())

    assert epochs == [1000, 1001, 1002]
    assert closes == [200.0, 200.0, 200.0]  # flat fills use last close

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

@pytest.mark.asyncio
async def test_late_ticks_are_ignored():
    q_ticks = asyncio.Queue()
    agg = BarAggregator(symbols=["SPY"], buffer_seconds=10, q_ticks=q_ticks, q_bars=None,
                        cfg=BarConfig(buffer_seconds=10, emit_gap_fill=False, idle_finalize_ms=50))
    task = asyncio.create_task(agg.start())

    # First tick at 1001 establishes accumulator
    await q_ticks.put(Tick(symbol="SPY", px=400.0, size=1, ts=1001.00))
    # Late tick earlier second 1000 should be ignored
    await q_ticks.put(Tick(symbol="SPY", px=399.0, size=1, ts=1000.50))
    # Advance to next second to finalize 1001
    await q_ticks.put(Tick(symbol="SPY", px=401.0, size=1, ts=1002.00))

    await asyncio.sleep(0.05)
    ring = agg.get_ring("SPY")
    v = ring.view_last(1)
    ep,o,h,l,c,vv = v.slices[0]
    assert ep[-1] == 1001
    assert o[-1] == pytest.approx(400.0)
    assert l[-1] == pytest.approx(400.0)  # not 399 (late tick ignored)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

@pytest.mark.asyncio
async def test_idle_finalize_without_new_ticks():
    q_ticks = asyncio.Queue()
    agg = BarAggregator(symbols=["QQQ"], buffer_seconds=10, q_ticks=q_ticks, q_bars=None,
                        cfg=BarConfig(buffer_seconds=10, emit_gap_fill=False, idle_finalize_ms=20))
    task = asyncio.create_task(agg.start())

    # One tick, then no more activity -> idle finalizer should flush open bar
    await q_ticks.put(Tick(symbol="QQQ", px=350.0, size=1, ts=2000.10))
    await asyncio.sleep(0.05)  # > idle_finalize_ms

    ring = agg.get_ring("QQQ")
    v = ring.view_last(1)
    ep, *_ = v.slices[0]
    assert ep[-1] == 2000

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
