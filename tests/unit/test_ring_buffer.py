import numpy as np
from monitor.data.ring_buffer import RingBufferOHLCV, Bar

def test_append_and_last_epoch():
    rb = RingBufferOHLCV(capacity=5)
    assert rb.last_epoch() is None

    rb.append(Bar(1,1,1,1,1,10))
    rb.append(Bar(2,2,2,2,2,20))
    assert rb.last_epoch() == 2
    assert rb.size == 2

def test_view_last_contiguous():
    rb = RingBufferOHLCV(capacity=5)
    for i in range(1,4):
        rb.append(Bar(i, i+0.1, i+0.2, i-0.1, i+0.0, i*10))

    v = rb.view_last(2)
    assert v.length == 2
    # single contiguous slice
    assert len(v.slices) == 1
    ep, o, h, l, c, vol = v.slices[0]
    assert list(ep) == [2,3]
    assert np.allclose(c, [2.0, 3.0])

def test_view_last_wraparound_two_segments():
    rb = RingBufferOHLCV(capacity=4)
    # Fill 5 bars to force wrap (head cycles)
    for i in range(1,6):
        rb.append(Bar(i, i, i, i, i, i))

    # buffer has [epochs]: 2,3,4,5 (in ring order)
    # ask last 3 -> should be [3,4,5], potentially across boundary
    v = rb.view_last(3)
    assert v.length == 3
    assert len(v.slices) in (1,2)  # wrapped view may come as two segments

    # Reassemble epochs from slices to check order
    epochs = []
    for sl in v.slices:
        epochs.extend(sl[0].tolist())
    assert epochs == [3,4,5]
