# src/monitor/indicators/basic_indicators.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np

from monitor.data.bars import BarAggregator
from monitor.data.ring_buffer import RingBufferOHLCV

# ----------------------------
# Parameter presets for 30s bars
# ----------------------------
DEFAULT_EMAS   = [9, 20, 50]       # fast, baseline, slow trend lines
DEFAULT_RSIS   = [14, 60, 120]     # ~7m, ~30m, ~60m
DEFAULT_ATR    = 14                # Wilder ATR(14)
DEFAULT_MACD   = (12, 26, 9)       # classic MACD in bar units
ENABLE_OBV     = True              # cumulative volume-based pressure


# ============================================================
# Batch compute helpers (from a RingBuffer of 30s bars)
# ============================================================

def _stitch(ring: RingBufferOHLCV) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    view = ring.view_last(ring.size)
    if view.length == 0:
        return tuple(np.empty(0) for _ in range(6))  # ep,o,h,l,c,v

    eps: List[np.ndarray] = []
    os_: List[np.ndarray] = []
    hs: List[np.ndarray] = []
    ls: List[np.ndarray] = []
    cs: List[np.ndarray] = []
    vs: List[np.ndarray] = []

    for (ep, o, h, l, c, v) in view.slices:
        eps.append(ep); os_.append(o); hs.append(h); ls.append(l); cs.append(c); vs.append(v)

    ep = np.concatenate(eps).astype(np.int64)
    o  = np.concatenate(os_).astype(np.float64)
    h  = np.concatenate(hs).astype(np.float64)
    l  = np.concatenate(ls).astype(np.float64)
    c  = np.concatenate(cs).astype(np.float64)
    v  = np.concatenate(vs).astype(np.float64)
    return ep, o, h, l, c, v


def compute_ema_series(c: np.ndarray, periods: int) -> np.ndarray:
    if c.size == 0:
        return c.copy()
    alpha = 2.0 / (periods + 1.0)
    ema = np.empty_like(c, dtype=np.float64)
    ema[0] = c[0]
    for i in range(1, c.size):
        ema[i] = alpha * c[i] + (1.0 - alpha) * ema[i-1]
    return ema


def compute_rsi_series(c: np.ndarray, periods: int) -> np.ndarray:
    if c.size == 0:
        return c.copy()
    delta = np.diff(c, prepend=c[0])
    gain = np.maximum(delta, 0.0)
    loss = np.maximum(-delta, 0.0)

    # Wilder smoothing (EMA-like with alpha=1/periods)
    rs = np.empty_like(c, dtype=np.float64)
    rsi = np.empty_like(c, dtype=np.float64)
    avg_gain = 0.0
    avg_loss = 0.0
    for i in range(c.size):
        if i == 0:
            avg_gain = gain[0]
            avg_loss = loss[0]
        else:
            avg_gain = (avg_gain * (periods - 1) + gain[i]) / periods
            avg_loss = (avg_loss * (periods - 1) + loss[i]) / periods

        if avg_loss == 0.0:
            rsi[i] = 100.0 if avg_gain > 0.0 else 50.0
        else:
            rs[i] = avg_gain / avg_loss
            rsi[i] = 100.0 - 100.0 / (1.0 + rs[i])
    return rsi


def compute_true_range(h: np.ndarray, l: np.ndarray, c_prev: np.ndarray) -> np.ndarray:
    # TR_t = max( H-L, |H - prevClose|, |L - prevClose| )
    tr1 = h - l
    tr2 = np.abs(h - c_prev)
    tr3 = np.abs(l - c_prev)
    return np.maximum(tr1, np.maximum(tr2, tr3))


def compute_atr_series(h: np.ndarray, l: np.ndarray, c: np.ndarray, periods: int) -> np.ndarray:
    if c.size == 0:
        return c.copy()
    c_prev = np.roll(c, 1)
    c_prev[0] = c[0]
    tr = compute_true_range(h, l, c_prev)
    atr = np.empty_like(tr, dtype=np.float64)
    # Wilder smoothing for ATR
    atr[0] = tr[0]
    for i in range(1, tr.size):
        atr[i] = (atr[i-1] * (periods - 1) + tr[i]) / periods
    return atr


def compute_macd_series(c: np.ndarray, fast: int, slow: int, signal: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    ema_fast = compute_ema_series(c, fast)
    ema_slow = compute_ema_series(c, slow)
    macd = ema_fast - ema_slow
    signal_line = compute_ema_series(macd, signal)
    hist = macd - signal_line
    return macd, signal_line, hist


def compute_obv_series(c: np.ndarray, v: np.ndarray) -> np.ndarray:
    if c.size == 0:
        return c.copy()
    obv = np.empty_like(c, dtype=np.float64)
    obv[0] = v[0]
    for i in range(1, c.size):
        if c[i] > c[i-1]:
            obv[i] = obv[i-1] + v[i]
        elif c[i] < c[i-1]:
            obv[i] = obv[i-1] - v[i]
        else:
            obv[i] = obv[i-1]
    return obv


def compute_all_from_ring(
    ring: RingBufferOHLCV,
    *,
    emas: List[int] = DEFAULT_EMAS,
    rsis: List[int] = DEFAULT_RSIS,
    atr_periods: int = DEFAULT_ATR,
    macd_tuple: Tuple[int, int, int] = DEFAULT_MACD,
    include_obv: bool = ENABLE_OBV,
):
    ep, o, h, l, c, v = _stitch(ring)
    out = {"epochs": ep, "close": c}
    if ep.size == 0:
        return out

    # MAs
    for n in emas:
        out[f"EMA{n}"] = compute_ema_series(c, n)

    # RSI
    for n in rsis:
        out[f"RSI{n}"] = compute_rsi_series(c, n)

    # ATR
    out[f"ATR{atr_periods}"] = compute_atr_series(h, l, c, atr_periods)

    # MACD
    f, s, sig = macd_tuple
    macd, sigl, hist = compute_macd_series(c, f, s, sig)
    out[f"MACD{f}_{s}"] = macd
    out[f"MACDsig{sig}"] = sigl
    out["MACDhist"] = hist

    # OBV
    if include_obv:
        out["OBV"] = compute_obv_series(c, v)

    return out


# ============================================================
# Streaming Engine (incremental, bar-by-bar)
# ============================================================

@dataclass(slots=True)
class IndicatorsConfig:
    emas: List[int] = field(default_factory=lambda: DEFAULT_EMAS.copy())
    rsis: List[int] = field(default_factory=lambda: DEFAULT_RSIS.copy())
    atr_periods: int = DEFAULT_ATR
    macd_tuple: Tuple[int, int, int] = DEFAULT_MACD
    include_obv: bool = ENABLE_OBV
    emit_on_update: bool = True  # if True, put (symbol, epoch, dict_of_updates) to q_out


class IndicatorsEngine:
    """
    Consumes finalized (symbol, epoch) from q_in (BarAggregator),
    looks up the finalized bar in the ring, updates incremental indicator states,
    and optionally emits a compact dict of updated values via q_out.
    """

    def __init__(
        self,
        *,
        aggregator: BarAggregator,
        q_in: asyncio.Queue,
        q_out: Optional[asyncio.Queue] = None,
        cfg: Optional[IndicatorsConfig] = None,
    ):
        self.agg = aggregator
        self.q_in = q_in
        self.q_out = q_out
        self.cfg = cfg or IndicatorsConfig()

        # Per-symbol state
        self._ema: Dict[str, Dict[int, float]] = {}            # sym -> {period -> ema}
        self._rsi_gain: Dict[str, Dict[int, float]] = {}       # Wilder avg gain
        self._rsi_loss: Dict[str, Dict[int, float]] = {}       # Wilder avg loss
        self._atr: Dict[str, float] = {}                       # last ATR value (periods fixed)
        self._prev_close: Dict[str, float] = {}
        self._macd_fast: Dict[str, float] = {}
        self._macd_slow: Dict[str, float] = {}
        self._macd_signal: Dict[str, float] = {}
        self._obv: Dict[str, float] = {}

        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="indicators-engine")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # ---------- streaming core ----------

    async def _run(self) -> None:
        try:
            while not self._stop.is_set():
                symbol, epoch = await self.q_in.get()
                self._on_bar(symbol, epoch)
        except asyncio.CancelledError:
            return

    def _on_bar(self, symbol: str, epoch: int) -> None:
        ring = self.agg.get_ring(symbol)
        view = ring.view_last(1)
        if view.length == 0 or not view.slices:
            return
        ep, o, h, l, c, v = view.slices[0]
        # use the final element of the last slice
        px_o, px_h, px_l, px_c, vol = float(o[-1]), float(h[-1]), float(l[-1]), float(c[-1]), float(v[-1])
        prev_c = self._prev_close.get(symbol, px_c)

        updates: Dict[str, float] = {}

        # --- EMA ---
        for n in self.cfg.emas:
            cur = self._ema.setdefault(symbol, {}).get(n)
            alpha = 2.0 / (n + 1.0)
            if cur is None:
                ema_val = px_c
            else:
                ema_val = alpha * px_c + (1.0 - alpha) * cur
            self._ema[symbol][n] = ema_val
            updates[f"EMA{n}"] = ema_val

        # --- RSI (Wilder) ---
        delta = px_c - prev_c
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        for n in self.cfg.rsis:
            gmap = self._rsi_gain.setdefault(symbol, {})
            lmap = self._rsi_loss.setdefault(symbol, {})
            ag = gmap.get(n, gain)
            al = lmap.get(n, loss)
            ag = (ag * (n - 1) + gain) / n
            al = (al * (n - 1) + loss) / n
            gmap[n] = ag
            lmap[n] = al
            if al == 0.0:
                rsi_val = 100.0 if ag > 0.0 else 50.0
            else:
                rs = ag / al
                rsi_val = 100.0 - 100.0 / (1.0 + rs)
            updates[f"RSI{n}"] = rsi_val

        # --- ATR (Wilder) ---
        if self.cfg.atr_periods:
            tr = max(px_h - px_l, abs(px_h - prev_c), abs(px_l - prev_c))
            prev_atr = self._atr.get(symbol, tr)
            n = self.cfg.atr_periods
            atr_val = (prev_atr * (n - 1) + tr) / n
            self._atr[symbol] = atr_val
            updates[f"ATR{n}"] = atr_val

        # --- MACD ---
        f, s, sig = self.cfg.macd_tuple
        # two EMAs on close
        ef = self._macd_fast.get(symbol, px_c)
        es = self._macd_slow.get(symbol, px_c)
        af = 2.0 / (f + 1.0)
        aS = 2.0 / (s + 1.0)
        ef = af * px_c + (1.0 - af) * ef
        es = aS * px_c + (1.0 - aS) * es
        self._macd_fast[symbol] = ef
        self._macd_slow[symbol] = es
        macd_val = ef - es

        # signal EMA on MACD
        sig_alpha = 2.0 / (sig + 1.0)
        sig_prev = self._macd_signal.get(symbol, macd_val)
        sig_val = sig_alpha * macd_val + (1.0 - sig_alpha) * sig_prev
        self._macd_signal[symbol] = sig_val
        updates[f"MACD{f}_{s}"] = macd_val
        updates[f"MACDsig{sig}"] = sig_val
        updates["MACDhist"] = macd_val - sig_val

        # --- OBV ---
        if self.cfg.include_obv:
            obv_prev = self._obv.get(symbol, vol)
            if px_c > prev_c:
                obv_val = obv_prev + vol
            elif px_c < prev_c:
                obv_val = obv_prev - vol
            else:
                obv_val = obv_prev
            self._obv[symbol] = obv_val
            updates["OBV"] = obv_val

        # Commit previous close for next bar
        self._prev_close[symbol] = px_c

        # Emit (optional)
        if self.cfg.emit_on_update and self.q_out is not None:
            try:
                # keep it small: (symbol, epoch, dict-of-updates)
                self.q_out.put_nowait((symbol, int(ep[-1]), updates))
            except asyncio.QueueFull:
                pass

    # -------------------------------------------------
    # Optional: seed states from existing ring history
    # -------------------------------------------------
    def seed_from_ring(self, symbol: str) -> None:
        """
        Initialize indicator states from ring history for a symbol.
        Useful when process starts mid-session: we replay history to warm up.
        """
        ring = self.agg.get_ring(symbol)
        ep, o, h, l, c, v = _stitch(ring)
        if ep.size == 0:
            return

        # EMA warmup
        for n in self.cfg.emas:
            self._ema.setdefault(symbol, {})[n] = compute_ema_series(c, n)[-1]

        # RSI warmup (Wilder)
        for n in self.cfg.rsis:
            self._rsi_gain.setdefault(symbol, {})[n] = 0.0
            self._rsi_loss.setdefault(symbol, {})[n] = 0.0
            # Derive avg gain/loss from historical series: approximate via back-pass
            delta = np.diff(c, prepend=c[0])
            gain = np.maximum(delta, 0.0)
            loss = np.maximum(-delta, 0.0)
            ag = 0.0; al = 0.0
            for i in range(gain.size):
                ag = (ag * (n - 1) + gain[i]) / n
                al = (al * (n - 1) + loss[i]) / n
            self._rsi_gain[symbol][n] = ag
            self._rsi_loss[symbol][n] = al

        # ATR warmup
        n = self.cfg.atr_periods
        if n:
            atr_series = compute_atr_series(h, l, c, n)
            self._atr[symbol] = float(atr_series[-1])

        # MACD warmup
        f, s, sig = self.cfg.macd_tuple
        macd, sigl, _ = compute_macd_series(c, f, s, sig)
        # reconstruct component EMAs approximately:
        ema_f = compute_ema_series(c, f)[-1]
        ema_s = compute_ema_series(c, s)[-1]
        self._macd_fast[symbol] = float(ema_f)
        self._macd_slow[symbol] = float(ema_s)
        self._macd_signal[symbol] = float(sigl[-1])

        # OBV warmup
        if self.cfg.include_obv:
            self._obv[symbol] = float(compute_obv_series(c, v)[-1])

        # prev close
        self._prev_close[symbol] = float(c[-1])

    def seed_all(self, symbols: Optional[List[str]] = None) -> None:
        symbols = symbols or list(self.agg.rings.keys())
        for s in symbols:
            self.seed_from_ring(s)
