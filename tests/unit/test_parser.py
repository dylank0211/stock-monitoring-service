import pytest
from monitor.ingest import parser
from monitor.utils.types import Tick

def test_parse_trade_variants():
    m1 = {"T":"t","S":"NVDA","p":181.5,"s":10,"t":1700000000.0}
    m2 = {"type":"t","symbol":"NVDA","price":181.5,"size":10,"timestamp":1700000000.0}
    t1 = parser.parse_trade_msg(m1)
    t2 = parser.parse_trade_msg(m2)
    assert isinstance(t1, Tick) and isinstance(t2, Tick)
    assert t1.symbol == "NVDA" and t2.symbol == "NVDA"
    assert t1.px == pytest.approx(181.5) and t2.px == pytest.approx(181.5)

def test_parse_non_trade_returns_none():
    for m in [{"T":"success","msg":"authenticated"}, {"T":"subscription"}]:
        assert parser.parse_trade_msg(m) is None
