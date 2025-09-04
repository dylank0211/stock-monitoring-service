from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Literal, Optional, Dict, Tuple, List

import numpy as np

from monitor.data.bars import BarAggregator
from monitor.data.ring_buffer import RingBufferOHLCV


PriceBasis = Literal["close", "hlc3", "ohlc4"]


def _price_basis(o: np.ndarray, h: np.ndarray, l: np.ndarray, c: np.ndarray, basis: PriceBasis) -> np.ndarray:
    if basis == "close":
        return c
    if basis == "hlc3":
        return (h + l + c) / 3.0
    if basis == "ohlc4":
        return (o + h + l + c) / 4.0
    raise ValueError(f"Unsupported price basis: {basis}")


def compute_vwap_series(
    ring: RingBufferOHLCV,
    *,
    session_start_epoch: Optional[int] = None,
    basis: PriceBasis = "hlc3",
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Compute VWAP for all (or session-filtered) bars present in the ring.

    Args:
        ring: RingBufferOHLCV containing epoch, o,h,l,c,v arrays.
        session_start_epoch: If provided, only bars with epoch >= this will be included.
        basis: 'close' | 'hlc3' | 'ohlc4'

    Returns:
        epochs: np.ndarray[int64] of epochs (aligned to bar size)
        vwap:   np.ndarray[float64] VWAP value per bar (NaN until cum volume > 0)
    """
    view = ring.view_last(ring.size)
    if view.length == 0:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float64)

    # Stitch slices into contiguous arrays in time order
    epochs: List[np.ndarray] = []
    o_list: List[np.ndarray] = []
    h_list: List[np.ndarray] = []
    l_list: List[np.ndarray] = []
    c_list: List[np.ndarray] = []
    v_list: List[np.ndarray] = []

    for (ep, o, h, l, c, v) in view.slices:
        epochs.append(ep)
        o_list.append(o)
        h_list.append(h)
        l_list.append(l)
        c_list.append(c)
        v_list.append(v)

    ep = np.concatenate(epochs)
    o = np.concatenate(o_list)
    h = np.concatenate(h_list)
    l = np.concatenate(l_list)
    c = np.concatenate(c_list)
    v = np.concatenate(v_list)

    if session_start_epoch is not None:
        sel = ep >= session_start_epoch
        ep, o, h, l, c, v = ep[sel], o[sel], h[sel], l[sel], c[sel], v[sel]

    if ep.size == 0:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float64)

    px = _price_basis(o, h, l, c, basis)
    pv = px * v

    cum_v = np.cumsum(v)
    cum_pv = np.cumsum(pv)

    # Avoid divide-by-zero: set VWAP to NaN until cum_v > 0
    vwap = np.empty_like(px, dtype=np.float64)
    mask = cum_v > 0
    vwap[~mask] = np.nan
    vwap[mask] = cum_pv[mask] / cum_v[mask]

    return ep.astype(np.int64, copy=False), vwap


def compute_anchored_vwap_series(
    ring: RingBufferOHLCV,
    *,
    anchor_epoch: int,
    basis: PriceBasis = "hlc3",
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Anchored VWAP starting from a specific bar epoch (inclusive).
    """
    return compute_vwap_series(ring, session_start_epoch=anchor_epoch, basis=basis)


@dataclass(slots=True)
class VwapConfig:
    """
    Config for the streaming VWAP engine.
    - basis: how to compute per-bar price proxy
    - anchor_epoch: if set, compute anchored VWAP from this epoch; else session-wide (from first seen bar)
    - emit_zero_volume_passthrough: if True, we still emit VWAP on zero-volume gap-fill bars
    """
    basis: PriceBasis = "hlc3"
    anchor_epoch: Optional[int] = None
    emit_zero_volume_passthrough: bool = True


class VwapEngine:
    """
    Streaming VWAP engine that consumes finalized bar notifications from q_in,
    looks up the finalized bar in the aggregator's ring, updates per-symbol cumulative PV & V,
    and (optionally) forwards a message to q_out: (symbol, epoch, vwap).

    Notes:
    - q_in is expected to receive (symbol, epoch) emitted by BarAggregator._finalize.
    - If you already have an aggregator producing into a shared q_bars, and multiple consumers
      need to observe the events, you should fan out (tee) the queue prior to passing q_in here.
      asyncio.Queue is single-consumer; this engine will consume events it receives.
    """

    def __init__(
        self,
        *,
        aggregator: BarAggregator,
        q_in: asyncio.Queue,
        q_out: Optional[asyncio.Queue] = None,
        cfg: Optional[VwapConfig] = None,
    ):
        self.agg = aggregator
        self.q_in = q_in
        self.q_out = q_out
        self.cfg = cfg or VwapConfig()

        # per-symbol cumulative state
        self._cum_pv: Dict[str, float] = {}
        self._cum_v: Dict[str, float] = {}
        self._last_vwap: Dict[str, float] = {}
        self._started_epoch: Dict[str, int] = {}  # effective start epoch (anchor/session)

        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    def get_last(self, symbol: str) -> Optional[float]:
        return self._last_vwap.get(symbol)

    def get_started_epoch(self, symbol: str) -> Optional[int]:
        return self._started_epoch.get(symbol)

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="vwap-engine")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass

    async def _run(self) -> None:
        try:
            while not self._stop.is_set():
                symbol, epoch = await self.q_in.get()
                self._on_bar(symbol, epoch)
        except asyncio.CancelledError:
            return

    def _on_bar(self, symbol: str, epoch: int) -> None:
        ring = self.agg.get_ring(symbol)
        # Grab the very last bar (should correspond to 'epoch')
        view = ring.view_last(1)
        if view.length == 0:
            return
        ep, o, h, l, c, v = view.slices[0]
        last_epoch = int(ep[-1])
        if last_epoch != epoch:
            # In case of out-of-order consumption, try to locate the exact epoch in the ring.
            # (Ring is small; do a quick scan backwards.)
            found = False
            # Check up to ring.size items
            view_all = ring.view_last(ring.size)
            for (epp, oo, hh, ll, cc, vv) in view_all.slices[::-1]:
                # reverse slices order to look latest first
                idx = np.where(epp == epoch)[0]
                if idx.size:
                    i = int(idx[-1])  # last occurrence within the slice
                    px = _price_basis(oo[i:i+1], hh[i:i+1], ll[i:i+1], cc[i:i+1], self.cfg.basis)[0]
                    vol = float(vv[i])
                    self._accumulate(symbol, epoch, px, vol)
                    found = True
                    break
            if not found:
                return
            return

        # Normal path: last bar is the one we were notified about
        px = _price_basis(o[-1:], h[-1:], l[-1:], c[-1:], self.cfg.basis)[0]
        vol = float(v[-1])
        self._accumulate(symbol, epoch, px, vol)

    def _accumulate(self, symbol: str, epoch: int, px: float, vol: float) -> None:
        # Initialize start anchor per symbol
        start_epoch = self._started_epoch.get(symbol)
        if start_epoch is None:
            start_epoch = self.cfg.anchor_epoch if self.cfg.anchor_epoch is not None else epoch
            self._started_epoch[symbol] = start_epoch
            self._cum_pv[symbol] = 0.0
            self._cum_v[symbol] = 0.0

        # If we receive an epoch earlier than our start (shouldn't happen in-order), ignore
        if epoch < start_epoch:
            return

        # Update cumulative sums; zero-volume flat bars do not change VWAP
        self._cum_pv[symbol] += px * vol
        self._cum_v[symbol] += vol

        if self._cum_v[symbol] <= 0.0:
            vwap = float("nan")
        else:
            vwap = self._cum_pv[symbol] / self._cum_v[symbol]

        self._last_vwap[symbol] = vwap

        # Optionally forward downstream
        if self.q_out is not None:
            try:
                # (symbol, epoch, vwap) – minimal payload; add sums if you like.
                self.q_out.put_nowait((symbol, epoch, vwap))
            except asyncio.QueueFull:
                pass
