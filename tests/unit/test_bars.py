import asyncio
import time
import pytest
from monitor.data.bars import BarAggregator, BarConfig
from monitor.utils.types import Tick
from monitor.utils.time import utc_now_s


def bucket(ts: float, bs: int) -> int:
    """Align timestamp to the start of its bar bucket."""
    return (int(ts) // bs) * bs


def idle_interval_seconds(bs: int) -> float:
    """Mirror BarAggregator's idle loop interval calculation."""
    return max(0.2, min(0.5, bs / 60.0))


async def wait_for_last_epoch(agg: BarAggregator, symbol: str, expected_epoch: int, timeout: float = 2.0):
    """Poll the ring until the last epoch equals expected_epoch or timeout."""
    start = time.time()
    ring = agg.get_ring(symbol)
    while time.time() - start < timeout:
        le = ring.last_epoch()
        if le == expected_epoch:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"Timed out waiting for last_epoch == {expected_epoch}, got {ring.last_epoch()}")


async def wait_for_ring_size_at_least(agg: BarAggregator, symbol: str, n: int, timeout: float = 2.0):
    """Poll the ring until it has at least n finalized bars or timeout."""
    start = time.time()
    ring = agg.get_ring(symbol)
    while time.time() - start < timeout:
        if ring.size >= n:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"Timed out waiting for ring.size >= {n}, got {ring.size}")


@pytest.mark.asyncio
async def test_basic_aggregation_and_emit_30s():
    """
    Two ticks in the same 30s bucket, then one tick that crosses into the next 30s bucket
    -> finalize the first bucket with correct OHLCV.
    """
    bs = 30
    q_ticks = asyncio.Queue()
    q_bars  = asyncio.Queue()
    agg = BarAggregator(
        symbols=["AAPL"],
        buffer_seconds=300,
        q_ticks=q_ticks,
        q_bars=q_bars,
        cfg=BarConfig(buffer_seconds=300, bar_seconds=bs, emit_gap_fill=True, idle_finalize_ms=50),
    )

    task = asyncio.create_task(agg.start())

    # Same-bucket ticks (epoch 990), then cross to epoch 1020
    t0 = 1000.10
    t1 = 1000.90
    t2 = 1020.10
    await q_ticks.put(Tick(symbol="AAPL", px=100.0, size=1, ts=t0))
    await q_ticks.put(Tick(symbol="AAPL", px=101.0, size=2, ts=t1))
    await q_ticks.put(Tick(symbol="AAPL", px=102.0, size=3, ts=t2))

    expected_epoch = bucket(t0, bs)  # 990

    # Wait until that bar is finalized
    await wait_for_last_epoch(agg, "AAPL", expected_epoch)

    ring = agg.get_ring("AAPL")
    v = ring.view_last(1)
    ep, o, h, l, c, vv = v.slices[0]

    assert ep[-1] == expected_epoch
    assert o[-1] == pytest.approx(100.0)
    assert h[-1] == pytest.approx(101.0)
    assert l[-1] == pytest.approx(100.0)
    assert c[-1] == pytest.approx(101.0)
    assert vv[-1] == pytest.approx(3.0)

    # drain bar notifications
    seen = []
    while not q_bars.empty():
        seen.append(await q_bars.get())
    assert ("AAPL", expected_epoch) in seen

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_gap_fill_between_30s_buckets():
    """
    Tick in one 30s bucket, then next tick jumps over *two* whole 30s buckets.
    With emit_gap_fill=True, we should see flat bars for the missing buckets at the prior close.
    """
    bs = 30
    q_ticks = asyncio.Queue()
    agg = BarAggregator(
        symbols=["NVDA"],
        buffer_seconds=600,
        q_ticks=q_ticks,
        q_bars=None,
        cfg=BarConfig(buffer_seconds=600, bar_seconds=bs, emit_gap_fill=True, idle_finalize_ms=50),
    )
    task = asyncio.create_task(agg.start())

    # First tick at ~1000.20s => epoch 990
    t0 = 1000.20  # epoch 990
    # Jump to ~1095.10s => epoch 1080 (skips epochs 1020 and 1050)
    t1 = 1095.10  # epoch 1080

    await q_ticks.put(Tick(symbol="NVDA", px=200.0, size=1, ts=t0))
    await q_ticks.put(Tick(symbol="NVDA", px=201.0, size=1, ts=t1))

    # We expect finalized bars for: 990 (real), 1020 (flat), 1050 (flat)
    # Wait until we have at least 3 bars
    await wait_for_ring_size_at_least(agg, "NVDA", 3)

    ring = agg.get_ring("NVDA")
    v = ring.view_last(3)

    epochs = []
    closes = []
    for sl in v.slices:
        epochs.extend(sl[0].tolist())
        closes.extend(sl[4].tolist())

    expected_epochs = [bucket(t0, bs), bucket(t0, bs) + bs, bucket(t0, bs) + 2 * bs]
    assert epochs == expected_epochs
    assert closes == [200.0, 200.0, 200.0]  # flat fills use previous close

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_late_ticks_are_ignored_30s():
    """
    Establish a bucket with a first tick, then advance into the *next* 30s bucket.
    A tick that arrives later but belongs to an *earlier* bucket must be ignored.
    """
    bs = 30
    q_ticks = asyncio.Queue()
    agg = BarAggregator(
        symbols=["SPY"],
        buffer_seconds=300,
        q_ticks=q_ticks,
        q_bars=None,
        cfg=BarConfig(buffer_seconds=300, bar_seconds=bs, emit_gap_fill=False, idle_finalize_ms=50),
    )
    task = asyncio.create_task(agg.start())

    t0 = 1001.00  # epoch 990
    await q_ticks.put(Tick(symbol="SPY", px=400.0, size=1, ts=t0))

    t1 = 1020.00  # crosses to epoch 1020, finalizes 990
    await q_ticks.put(Tick(symbol="SPY", px=401.0, size=1, ts=t1))

    # Late tick that maps to epoch 990 should be ignored
    t_late = 1000.50
    await q_ticks.put(Tick(symbol="SPY", px=399.0, size=1, ts=t_late))

    expected_epoch = bucket(t0, bs)  # 990
    await wait_for_last_epoch(agg, "SPY", expected_epoch)

    ring = agg.get_ring("SPY")
    v = ring.view_last(1)
    ep, o, h, l, c, vv = v.slices[0]

    assert ep[-1] == expected_epoch
    assert o[-1] == pytest.approx(400.0)
    # Late tick shouldn't lower L
    assert l[-1] == pytest.approx(400.0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_idle_finalize_without_new_ticks_30s():
    """
    One tick in the *previous* 30s bucket relative to 'now', then no activity.
    Idle finalizer should flush that open bar once we cross into the new bucket and idle threshold passes.
    """
    bs = 30
    idle_ms = 20
    q_ticks = asyncio.Queue()
    agg = BarAggregator(
        symbols=["QQQ"],
        buffer_seconds=300,
        q_ticks=q_ticks,
        q_bars=None,
        cfg=BarConfig(buffer_seconds=300, bar_seconds=bs, emit_gap_fill=False, idle_finalize_ms=idle_ms),
    )
    task = asyncio.create_task(agg.start())

    # Put a tick into the previous bucket relative to real 'now'
    now = utc_now_s()
    current_bucket = bucket(now, bs)
    prev_bucket = current_bucket - bs
    t_prev_inside = prev_bucket + 0.10
    await q_ticks.put(Tick(symbol="QQQ", px=350.0, size=1, ts=t_prev_inside))

    # Sleep long enough for:
    # 1) The idle loop to tick (0.5s for 30s bars)
    # 2) The idle threshold to pass
    # 3) Some margin for scheduling
    sleep_needed = idle_interval_seconds(bs) + (idle_ms / 1000.0) + 0.25
    await asyncio.sleep(sleep_needed)

    # Wait explicitly until that previous bucket is finalized
    await wait_for_last_epoch(agg, "QQQ", prev_bucket)

    ring = agg.get_ring("QQQ")
    v = ring.view_last(1)
    ep, *_ = v.slices[0]
    assert ep[-1] == prev_bucket

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
