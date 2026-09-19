"""B19/B20 — fail closed on stale, incomplete or missing market data.

The previous behaviour estimated slippage as 0.0 whenever the book was absent,
which disabled the slippage firewall exactly during a WebSocket outage: the REST
fallback writes a depth-less snapshot.
"""
import time
import pytest

from execution.sor import SmartOrderRouter
from tests.conftest import FakeExecutor, FakeCoreEngine, BTC_LIMITS, book, run

BTC = 100_000.0


def sor():
    s = SmartOrderRouter(executor=FakeExecutor(), core_engine=FakeCoreEngine())
    s.instrument_cache["BTCUSDT"] = dict(BTC_LIMITS)
    return s


# --- B19: slippage estimator must not report zero for an absent book -------

@pytest.mark.parametrize("snapshot", [
    None, {}, {"best_bid": 100.0, "best_ask": 101.0},          # REST fallback shape
    {"bids": [], "asks": []}, {"bids": [[100.0, 1.0]]},         # half a book
])
def test_missing_depth_is_unknown_not_zero(snapshot):
    s = sor()
    est = s.estimate_orderbook_slippage_bps(snapshot, "BUY", 0.01, BTC)
    assert est == s.SLIPPAGE_UNKNOWN, (
        f"B19: absent depth reported {est} bps -- a missing book must never read "
        f"as zero execution cost"
    )


def test_real_book_still_produces_a_finite_estimate():
    s = sor()
    est = s.estimate_orderbook_slippage_bps(book(BTC), "BUY", 0.01, BTC)
    assert 0.0 <= est < 1000.0


def test_unknown_slippage_blocks_execution():
    ex = FakeExecutor()
    s = SmartOrderRouter(executor=ex, core_engine=FakeCoreEngine())
    s.instrument_cache["BTCUSDT"] = dict(BTC_LIMITS)
    ok, _, filled = run(s.execute_alpha_signal(
        symbol="BTCUSDT", direction="BUY", prob_success=0.7, exec_weight=1.0,
        current_mid_price=BTC, sl_price=98_000.0, tp_price=104_000.0, inst_var=1e-5,
        depth_snapshot={"best_bid": BTC - 1, "best_ask": BTC + 1},  # no ladders
        target_notional=500.0, regime="TRENDING",
    ))
    assert ok is False and filled == 0.0
    assert ex.created_orders == [], "traded against an unavailable book"


# --- B20: freshness contract ----------------------------------------------

from market_data import is_tradeable

MAX_AGE = 5.0


class Engine:
    """Thin adapter so the tests read like the call site."""
    @staticmethod
    def _market_data_is_tradeable(payload, now):
        return is_tradeable(payload, now, MAX_AGE)


def fresh_book(now, **over):
    b = book(BTC)
    b.update({"as_of": now, "source": "WS_L2", "timestamp": int(now * 1000)})
    b.update(over)
    return b


def test_fresh_complete_book_is_tradeable():
    now = time.time()
    ok, reason = Engine()._market_data_is_tradeable(fresh_book(now), now)
    assert ok is True, reason


def test_stale_book_is_rejected():
    now = time.time()
    ok, reason = Engine()._market_data_is_tradeable(fresh_book(now - 30.0), now)
    assert ok is False and "age" in reason


def test_missing_as_of_is_rejected():
    now = time.time()
    b = fresh_book(now); b.pop("as_of")
    ok, reason = Engine()._market_data_is_tradeable(b, now)
    assert ok is False and "as_of" in reason


def test_future_timestamp_is_rejected():
    now = time.time()
    ok, reason = Engine()._market_data_is_tradeable(fresh_book(now + 60.0), now)
    assert ok is False and "future" in reason


def test_rest_fallback_snapshot_is_rejected():
    """The exact shape written during a WebSocket outage."""
    now = time.time()
    payload = {"best_bid": BTC - 1, "best_ask": BTC + 1, "micro_price": BTC,
               "as_of": now, "source": "REST_BBO_FALLBACK", "depth_available": False}
    ok, reason = Engine()._market_data_is_tradeable(payload, now)
    assert ok is False and "REST fallback" in reason


def test_empty_payload_rejected():
    ok, _ = Engine()._market_data_is_tradeable({}, time.time())
    assert ok is False


def test_one_sided_book_rejected():
    now = time.time()
    b = fresh_book(now); b["asks"] = []
    ok, reason = Engine()._market_data_is_tradeable(b, now)
    assert ok is False and "ladder" in reason
