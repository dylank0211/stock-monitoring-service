from monitor.utils.time import floor_to_second, ceil_to_second

def test_floor_ceil_second():
    assert floor_to_second(10.1) == 10
    assert ceil_to_second(10.0) == 10
    assert ceil_to_second(10.1) == 11
