"""NEW-1..NEW-5 — paper isolation, margin, rejections, execution records.

The original PaperBroker delegated any undefined attribute to the live
executor, so interception covered only `safe_call`. The engine's DIRECT method
calls escaped: get_wallet_balance_usdt returned the REAL balance, and
adjust_leverage issued a real authenticated POST /v5/position/set-leverage.

These tests exist because the original 19 paper tests all exercised `safe_call`
and none asked what happens to a method call.
"""
import pytest

from execution.paper_broker import PaperBroker, PaperIsolationError
from tests.conftest import run


class LiveSpy:
    """Any attribute reached here represents an escape to production."""
    def __init__(self):
        self.reached = []

    async def safe_call(self, method, endpoint, **kw):
        self.reached.append(f"safe_call:{endpoint}")
        return {"retCode": 0, "result": {"list": []}}

    async def get_wallet_balance_usdt(self):
        self.reached.append("get_wallet_balance_usdt")
        return 999_999.0                      # unmistakably the "real" account

    async def adjust_leverage(self, symbol, lev):
        self.reached.append(f"adjust_leverage:{symbol}")
        return True

    async def connect_ws(self):
        self.reached.append("connect_ws")

    async def get_fee_rates(self, symbol="BTCUSDT"):
        self.reached.append("get_fee_rates")
        return {"taker": 0.1, "maker": 0.1}

    async def get_top_volatile_assets(self, **kw):
        self.reached.append("get_top_volatile_assets")
        return ["BTCUSDT"]


def broker(balance=10_000.0):
    return PaperBroker(LiveSpy(), starting_balance=balance)


def create(b, **kw):
    kw.setdefault("category", "linear")
    return run(b.safe_call("POST", "/v5/order/create", **kw))


# --- NEW-1: the balance the engine sees ------------------------------------

def test_wallet_balance_is_the_paper_balance():
    b = broker(balance=1234.0)
    assert run(b.get_wallet_balance_usdt()) == pytest.approx(1234.0)
    assert "get_wallet_balance_usdt" not in b._real.reached, (
        "NEW-1: balance query escaped to the live account"
    )


def test_paper_balance_moves_with_paper_trades():
    """The real account cannot move; the paper balance must."""
    b = broker(balance=10_000.0)
    b.update_mark("ETHUSDT", 3000.0)
    before = run(b.get_wallet_balance_usdt())
    create(b, symbol="ETHUSDT", side="Buy", orderType="Market", qty="1")
    b.update_mark("ETHUSDT", 3300.0)
    create(b, symbol="ETHUSDT", side="Sell", orderType="Market", qty="1", reduceOnly=True)
    assert run(b.get_wallet_balance_usdt()) != pytest.approx(before)


# --- NEW-2 / NEW-3: state-changing methods ---------------------------------

def test_adjust_leverage_never_reaches_the_exchange():
    b = broker()
    assert run(b.adjust_leverage("BTCUSDT", 2)) is True
    assert not any("adjust_leverage" in c for c in b._real.reached), (
        "NEW-2: leverage change escaped to the live account"
    )
    assert b.leverage["BTCUSDT"] == 2.0


def test_connect_ws_opens_no_live_session():
    b = broker()
    run(b.connect_ws())
    assert "connect_ws" not in b._real.reached, (
        "NEW-3: PAPER opened an authenticated live WebSocket"
    )


def test_fee_rates_are_simulated():
    b = broker()
    fees = run(b.get_fee_rates("BTCUSDT"))
    assert fees["taker"] == b.taker_fee
    assert "get_fee_rates" not in b._real.reached


# --- the isolation boundary itself -----------------------------------------

@pytest.mark.parametrize("method", [
    "place_order", "cancel_all_orders", "set_margin_mode",
    "withdraw", "transfer", "_safe_api_call",
])
def test_unknown_methods_raise_rather_than_delegate(method):
    b = broker()
    with pytest.raises((PaperIsolationError, AttributeError)):
        getattr(b, method)


def test_public_readonly_passthrough_still_works():
    b = broker()
    assert run(b.get_top_volatile_assets()) == ["BTCUSDT"]
    assert "get_top_volatile_assets" in b._real.reached


def test_full_entry_exit_cycle_touches_nothing_live():
    """End-to-end: a complete paper trade must leave no trace on the live spy."""
    b = broker(balance=10_000.0)
    b.update_mark("ETHUSDT", 3000.0)
    run(b.adjust_leverage("ETHUSDT", 2))
    create(b, symbol="ETHUSDT", side="Buy", orderType="Market", qty="1",
           stopLoss="2900", takeProfit="3200")
    b.update_mark("ETHUSDT", 3250.0)          # take-profit triggers
    run(b.get_wallet_balance_usdt())
    run(b.safe_call("GET", "/v5/position/list", category="linear", symbol="ETHUSDT"))
    run(b.safe_call("GET", "/v5/position/closed-pnl", category="linear", symbol="ETHUSDT"))
    assert b._real.reached == [], f"escaped to live: {b._real.reached}"


# --- NEW-4: margin ---------------------------------------------------------

def test_oversized_order_rejected_with_110007():
    b = broker(balance=100.0)
    b.update_mark("BTCUSDT", 100_000.0)
    res = create(b, symbol="BTCUSDT", side="Buy", orderType="Market", qty="10")
    assert res["retCode"] == 110007
    assert "BTCUSDT" not in b.positions


def test_balance_cannot_go_negative_from_an_opening_trade():
    b = broker(balance=100.0)
    b.update_mark("BTCUSDT", 100_000.0)
    create(b, symbol="BTCUSDT", side="Buy", orderType="Market", qty="10")
    assert b.balance > 0.0


def test_leverage_raises_affordable_size():
    """At 2x, a $10k account can carry ~$20k notional."""
    b = broker(balance=10_000.0)
    b.update_mark("ETHUSDT", 3000.0)
    run(b.adjust_leverage("ETHUSDT", 2))
    assert create(b, symbol="ETHUSDT", side="Buy", orderType="Market", qty="6")["retCode"] == 0
    assert b.positions["ETHUSDT"].size == pytest.approx(6.0)


def test_margin_accounts_for_existing_positions():
    b = broker(balance=10_000.0)
    b.update_mark("ETHUSDT", 3000.0); b.update_mark("BTCUSDT", 100_000.0)
    run(b.adjust_leverage("ETHUSDT", 2)); run(b.adjust_leverage("BTCUSDT", 2))
    create(b, symbol="ETHUSDT", side="Buy", orderType="Market", qty="6")   # ~$9k margin
    res = create(b, symbol="BTCUSDT", side="Buy", orderType="Market", qty="1")
    assert res["retCode"] == 110007, "second position ignored margin already committed"


def test_reduce_only_is_exempt_from_margin():
    b = broker(balance=10_000.0)
    b.update_mark("ETHUSDT", 3000.0)
    run(b.adjust_leverage("ETHUSDT", 2))
    create(b, symbol="ETHUSDT", side="Buy", orderType="Market", qty="6")
    b.update_mark("ETHUSDT", 2500.0)          # large unrealised loss
    res = create(b, symbol="ETHUSDT", side="Sell", orderType="Market",
                 qty="6", reduceOnly=True)
    assert res["retCode"] == 0, "closing a position must never be margin-blocked"
    assert "ETHUSDT" not in b.positions


# --- NEW-5: rejection taxonomy ---------------------------------------------

@pytest.mark.parametrize("code,msg", [
    (10006, "rate limit"), (110013, "risk limit"), (110126, "agreement not signed"),
    (10002, "timestamp drift"),
])
def test_injected_rejections_surface_with_real_codes(code, msg):
    b = broker()
    b.update_mark("BTCUSDT", 100_000.0)
    b.reject_next = (code, msg)
    res = create(b, symbol="BTCUSDT", side="Buy", orderType="Market", qty="0.01")
    assert res["retCode"] == code
    assert b.rejections_by_code[code] == 1


def test_injection_is_single_shot():
    b = broker()
    b.update_mark("BTCUSDT", 100_000.0)
    b.reject_next = (10006, "rate limit")
    assert create(b, symbol="BTCUSDT", side="Buy", orderType="Market", qty="0.01")["retCode"] == 10006
    assert create(b, symbol="BTCUSDT", side="Buy", orderType="Market", qty="0.01")["retCode"] == 0


def test_rejections_are_counted_by_code():
    b = broker()
    b.update_mark("BTCUSDT", 100_000.0)
    create(b, symbol="BTCUSDT", side="Buy", orderType="Market", qty="0")
    assert b.rejections_by_code.get(10001, 0) >= 1


# --- B12 in paper: execution records ---------------------------------------

def test_execution_list_endpoint_returns_fills():
    b = broker(balance=10_000.0)
    b.update_mark("ETHUSDT", 3000.0)
    create(b, symbol="ETHUSDT", side="Buy", orderType="Market", qty="1")
    res = run(b.safe_call("GET", "/v5/execution/list", category="linear",
                          symbol="ETHUSDT", limit=50))
    rows = res["result"]["list"]
    assert rows, "B12: paper must serve execution records or the fills fallback is dead"
    assert float(rows[0]["execQty"]) == pytest.approx(1.0)
    assert float(rows[0]["execFee"]) > 0.0


def test_execution_records_cover_both_legs():
    b = broker(balance=10_000.0)
    b.update_mark("ETHUSDT", 3000.0)
    create(b, symbol="ETHUSDT", side="Buy", orderType="Market", qty="1")
    b.update_mark("ETHUSDT", 3200.0)
    create(b, symbol="ETHUSDT", side="Sell", orderType="Market", qty="1", reduceOnly=True)
    rows = run(b.safe_call("GET", "/v5/execution/list", category="linear",
                           symbol="ETHUSDT"))["result"]["list"]
    assert {r["side"] for r in rows} == {"Buy", "Sell"}
