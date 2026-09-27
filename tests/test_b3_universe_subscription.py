"""B3 — a universe change must produce a subscription change.

The audited failure: `run_universe_refresher` swapped `asset_basket` every 900s
but never touched the WebSocket. Ticks for old symbols were rejected because
they were no longer in the basket, and new symbols were never subscribed, so the
engine went blind to its own universe.

Chain under test:  universe change -> subscription change -> incoming data.
"""
import asyncio

from ingestion.multi_feed import MarketStateMatrix
from tests.conftest import run


class FakeWS:
    def __init__(self, closed=False):
        self.closed = closed
        self.sent = []

    async def send_json(self, payload):
        if self.closed:
            raise ConnectionError("socket closed")
        self.sent.append(payload)

    def topics(self, op):
        out = []
        for m in self.sent:
            if m.get("op") == op:
                out.extend(m.get("args", []))
        return out


def feed(basket, ws=None):
    noop = lambda *a, **k: None
    f = MarketStateMatrix(
        basket=list(basket), intervals=["15", "60"],
        orderbook_callback=noop, screener_callback=noop,
        kline_callback=noop, trade_callback=noop, engine_reference=None,
    )
    f.active_ws = ws if ws is not None else FakeWS()
    f.is_running = True
    return f


# --- subscribe / unsubscribe primitives -----------------------------------

def test_subscribe_symbol_sends_all_topics():
    f = feed(["BTCUSDT"])
    assert run(f.subscribe_symbol("ETHUSDT")) is True
    subs = f.active_ws.topics("subscribe")
    assert "tickers.ETHUSDT" in subs
    assert "orderbook.50.ETHUSDT" in subs
    assert "publicTrade.ETHUSDT" in subs
    assert "kline.15.ETHUSDT" in subs and "kline.60.ETHUSDT" in subs
    assert "ETHUSDT" in f.basket


def test_unsubscribe_symbol_sends_and_purges():
    f = feed(["BTCUSDT", "ETHUSDT"])
    f.l2_bids["ETHUSDT"] = {1.0: 1.0}
    f.micro_prices["ETHUSDT"] = 3000.0
    assert run(f.unsubscribe_symbol("ETHUSDT")) is True
    assert "orderbook.50.ETHUSDT" in f.active_ws.topics("unsubscribe")
    assert "ETHUSDT" not in f.basket
    assert "ETHUSDT" not in f.l2_bids
    assert "ETHUSDT" not in f.micro_prices


def test_subscribe_reports_failure_when_socket_down():
    f = feed(["BTCUSDT"], ws=FakeWS(closed=True))
    assert run(f.subscribe_symbol("ETHUSDT")) is False


def test_subscribe_reports_failure_when_no_socket():
    f = feed(["BTCUSDT"])
    f.active_ws = None
    assert run(f.subscribe_symbol("ETHUSDT")) is False


# --- engine-level resubscription ------------------------------------------

class EngineStub:
    """Exercises the real _resubscribe_universe / _verify_new_symbol_data."""
    from main import DistributedQuantEngine as _E
    _resubscribe_universe = _E._resubscribe_universe
    _verify_new_symbol_data = _E._verify_new_symbol_data

    def __init__(self, feed_obj):
        self.stream_feed_instance = feed_obj
        self.stream_restart_event = asyncio.Event()
        self.asset_basket = []
        self.shadow_basket = []
        self.subscription_state = set()
        self.orderbook_snapshots = {}
        self.active_positions_map = {}
        self.in_flight_symbols = {}

    def track_task(self, coro):
        return asyncio.ensure_future(coro)


def test_added_symbols_get_subscribed():
    f = feed(["BTCUSDT"])
    eng = EngineStub(f)
    run(eng._resubscribe_universe(to_add=["ETHUSDT", "SOLUSDT"], to_drop=[]))
    subs = f.active_ws.topics("subscribe")
    assert "orderbook.50.ETHUSDT" in subs
    assert "orderbook.50.SOLUSDT" in subs


def test_dropped_symbols_get_unsubscribed():
    f = feed(["BTCUSDT", "ETHUSDT"])
    eng = EngineStub(f)
    run(eng._resubscribe_universe(to_add=[], to_drop=["ETHUSDT"]))
    assert "orderbook.50.ETHUSDT" in f.active_ws.topics("unsubscribe")


def test_equal_add_and_drop_uses_hot_swap():
    f = feed(["BTCUSDT", "ETHUSDT"])
    eng = EngineStub(f)
    run(eng._resubscribe_universe(to_add=["SOLUSDT"], to_drop=["ETHUSDT"]))
    assert "orderbook.50.SOLUSDT" in f.active_ws.topics("subscribe")
    assert "orderbook.50.ETHUSDT" in f.active_ws.topics("unsubscribe")


def test_no_change_sends_nothing():
    f = feed(["BTCUSDT"])
    eng = EngineStub(f)
    run(eng._resubscribe_universe(to_add=[], to_drop=[]))
    assert f.active_ws.sent == []


def test_socket_down_requests_restart_rather_than_silently_ignoring():
    """The blindness case: a change we cannot apply must escalate, not vanish."""
    f = feed(["BTCUSDT"], ws=FakeWS(closed=True))
    f.active_ws = None
    eng = EngineStub(f)
    run(eng._resubscribe_universe(to_add=["ETHUSDT"], to_drop=[]))
    assert eng.stream_restart_event.is_set()


def test_resubscribe_failure_forces_restart():
    f = feed(["BTCUSDT"], ws=FakeWS(closed=True))
    eng = EngineStub(f)
    run(eng._resubscribe_universe(to_add=["ETHUSDT"], to_drop=[]))
    assert eng.stream_restart_event.is_set()


# --- the fourth link: data must actually arrive ----------------------------

def test_verify_detects_silent_subscription():
    f = feed(["BTCUSDT"])
    eng = EngineStub(f)
    run(eng._verify_new_symbol_data(["ETHUSDT"], timeout=2.0))
    assert eng.stream_restart_event.is_set(), (
        "a subscription that delivers no data must be escalated"
    )


def test_verify_passes_when_data_arrives():
    import time as _t
    f = feed(["BTCUSDT"])
    eng = EngineStub(f)
    eng.orderbook_snapshots["ETHUSDT"] = {"as_of": _t.time(), "bids": [[1, 1]], "asks": [[2, 1]]}
    run(eng._verify_new_symbol_data(["ETHUSDT"], timeout=3.0))
    assert not eng.stream_restart_event.is_set()
