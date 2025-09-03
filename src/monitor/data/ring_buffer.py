from __future__ import annotations

import numpy as np
from dataclasses import dataclass

class RingView:
    """
    Zero-copy view of last N bars.
    - If the buffer hasn't wrapped, slices is [np.ndarray slice].
    - If it has wrapped, slices is [slice1, slice2] in time order.
    """
    __slots__ = ("slices", "length")
    def __init__(self, slices: list[tuple[np.ndarray, ...]] | None, length: int):
        self.slices = slices or []
        self.length = length

@dataclass(slots=True)
class Bar:
    epoch: int
    o: float
    h: float
    l: float
    c: float
    v: float

class RingBufferOHLCV:
    """
    Fixed-size circular buffer for 1s OHLCV bars, per symbol.
    Arrays:
      epoch[int64], o,h,l,c,v[float64]
    """
    __slots__ = ("capacity","size","head","epoch","o","h","l","c","v")
    def __init__(self, capacity: int):
        self.capacity = int(capacity)
        self.size = 0
        self.head = 0  # next write index
        self.epoch = np.empty(self.capacity, dtype=np.int64)
        self.o = np.empty(self.capacity, dtype=np.float64)
        self.h = np.empty(self.capacity, dtype=np.float64)
        self.l = np.empty(self.capacity, dtype=np.float64)
        self.c = np.empty(self.capacity, dtype=np.float64)
        self.v = np.empty(self.capacity, dtype=np.float64)

    def append(self, bar: Bar) -> None:
        i = self.head
        self.epoch[i] = bar.epoch
        self.o[i] = bar.o
        self.h[i] = bar.h
        self.l[i] = bar.l
        self.c[i] = bar.c
        self.v[i] = bar.v
        self.head = (i + 1) % self.capacity
        if self.size < self.capacity:
            self.size += 1

    def last_epoch(self) -> int | None:
        if self.size == 0:
            return None
        idx = (self.head - 1) % self.capacity
        return int(self.epoch[idx])

    def view_last(self, n: int) -> RingView:
        """
        Return up to last n bars as zero-copy slices in time order.
        Use RingView.slices which is a list of tuples:
          [(epoch_slice, o_slice, h_slice, l_slice, c_slice, v_slice), ...]
        Each slice-tuple refers to the same time range segment.
        """
        if self.size == 0:
            return RingView([], 0)
        n = int(n)
        if n <= 0:
            return RingView([], 0)
        n = min(n, self.size)

        end = self.head  # exclusive
        start = (end - n) % self.capacity

        if start < end:
            # contiguous
            sl = slice(start, end)
            return RingView(
                [(self.epoch[sl], self.o[sl], self.h[sl], self.l[sl], self.c[sl], self.v[sl])],
                n,
            )
        else:
            # wrapped: [start..cap) + [0..end)
            sl1 = slice(start, self.capacity)
            sl2 = slice(0, end)
            return RingView(
                [
                    (self.epoch[sl1], self.o[sl1], self.h[sl1], self.l[sl1], self.c[sl1], self.v[sl1]),
                    (self.epoch[sl2], self.o[sl2], self.h[sl2], self.l[sl2], self.c[sl2], self.v[sl2]),
                ],
                n,
            )
