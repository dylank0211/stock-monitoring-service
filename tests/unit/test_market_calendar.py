from monitor.utils.market_calendar import is_market_open_now

def test_market_open_now_callable():
    # Can't assert true/false deterministically here without a fixed clock,
    # but we can at least ensure it returns a bool and doesn't throw.
    assert isinstance(is_market_open_now(), bool)
