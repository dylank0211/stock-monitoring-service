from monitor.utils.backoff import next_backoff, backoff_iter

def test_next_backoff_caps():
    assert next_backoff(1, 4) == 2
    assert next_backoff(2, 4) == 4
    assert next_backoff(4, 4) == 4

def test_backoff_iter_progression():
    it = backoff_iter(0.25, 2.0)
    vals = [next(it) for _ in range(5)]
    assert vals == [0.25, 0.5, 1.0, 2.0, 2.0]
