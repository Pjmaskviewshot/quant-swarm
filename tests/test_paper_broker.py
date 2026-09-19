"""P0-B7 — paper broker correctness.

Paper mode is the instrument that lets the system gather forward evidence
without risking capital. If its accounting is wrong, every downstream
measurement is wrong, so it is tested like production code.
"""
import pytest

from execution.paper_broker import PaperBroker
from tests.conftest import run


class RealStub:
    """Stands in for the live executor; only public market calls should reach it."""
    def __init__(self):
        self.calls = []

    async def safe_call(self, method, endpoint, **kwargs):
        self.calls.append(endpoint)
        return {"retCode": 0, "result": {"list": []}}


def broker(balance=1000.0):
    return PaperBroker(RealStub(), starting_balance=balance)


def create(b, **kw):
    kw.setdefault("category", "linear")
    return run(b.safe_call("POST", "/v5/order/create", **kw))


# --- no real orders --------------------------------------------------------

def test_order_create_never_reaches_the_real_exchange():
    b = broker(); b.update_mark("BTCUSDT", 100_000.0)
    create(b, symbol="BTCUSDT", side="Buy", orderType="Market", qty="0.01")
    assert "/v5/order/create" not in b._real.calls


def test_public_market_data_passes_through():
    b = broker()
    run(b.safe_call("GET", "/v5/market/tickers", category="linear"))
    assert "/v5/market/tickers" in b._real.calls


# --- fill model ------------------------------------------------------------

def test_market_buy_opens_position_with_adverse_slippage():
    b = broker(); b.update_mark("BTCUSDT", 100_000.0)
    create(b, symbol="BTCUSDT", side="Buy", orderType="Market", qty="0.01")
    pos = b.positions["BTCUSDT"]
    assert pos.size == pytest.approx(0.01)
    assert pos.avg_price > 100_000.0, "market buy should slip adversely"


def test_non_marketable_ioc_fills_nothing():
    """Directly reproduces the zero-fill IOC that B2 mishandled."""
    b = broker(); b.update_mark("BTCUSDT", 100_000.0)
    res = create(b, symbol="BTCUSDT", side="Buy", orderType="Limit",
                 price="99000", qty="0.01", timeInForce="IOC")
    oid = res["result"]["orderId"]
    rec = b.orders[oid]
    assert rec["orderStatus"] == "Cancelled"
    assert float(rec["cumExecQty"]) == 0.0
    assert "BTCUSDT" not in b.positions


def test_marketable_ioc_fills_at_limit():
    b = broker(); b.update_mark("BTCUSDT", 100_000.0)
    create(b, symbol="BTCUSDT", side="Buy", orderType="Limit",
           price="100500", qty="0.01", timeInForce="IOC")
    assert b.positions["BTCUSDT"].avg_price == pytest.approx(100_500.0)


def test_postonly_rests_then_fills_on_touch():
    b = broker(); b.update_mark("BTCUSDT", 100_000.0)
    res = create(b, symbol="BTCUSDT", side="Buy", orderType="Limit",
                 price="99500", qty="0.01", timeInForce="PostOnly")
    oid = res["result"]["orderId"]
    assert b.orders[oid]["orderStatus"] == "New"
    assert "BTCUSDT" not in b.positions
    b.update_mark("BTCUSDT", 99_400.0)
    assert b.orders[oid]["orderStatus"] == "Filled"
    assert b.positions["BTCUSDT"].size == pytest.approx(0.01)


# --- reduceOnly ------------------------------------------------------------

def test_reduce_only_cannot_open_a_position():
    b = broker(); b.update_mark("BTCUSDT", 100_000.0)
    create(b, symbol="BTCUSDT", side="Sell", orderType="Market",
           qty="0.01", reduceOnly=True)
    assert "BTCUSDT" not in b.positions


def test_reduce_only_cannot_flip_a_position():
    b = broker(); b.update_mark("BTCUSDT", 100_000.0)
    create(b, symbol="BTCUSDT", side="Buy", orderType="Market", qty="0.01")
    create(b, symbol="BTCUSDT", side="Sell", orderType="Market",
           qty="0.05", reduceOnly=True)
    assert "BTCUSDT" not in b.positions, "should close flat, never flip"


# --- pnl accounting --------------------------------------------------------

def test_round_trip_profit_is_credited_and_recorded():
    b = broker(balance=10_000.0); b.update_mark("ETHUSDT", 3000.0)
    create(b, symbol="ETHUSDT", side="Buy", orderType="Market", qty="1")
    entry = b.positions["ETHUSDT"].avg_price
    b.update_mark("ETHUSDT", 3300.0)
    create(b, symbol="ETHUSDT", side="Sell", orderType="Market", qty="1", reduceOnly=True)
    assert "ETHUSDT" not in b.positions
    assert b.balance > 10_000.0
    assert len(b.closed_pnl) == 1
    assert float(b.closed_pnl[0]["closedPnl"]) > 0.0
    assert float(b.closed_pnl[0]["avgEntryPrice"]) == pytest.approx(entry)


def test_round_trip_loss_is_debited():
    b = broker(balance=10_000.0); b.update_mark("ETHUSDT", 3000.0)
    create(b, symbol="ETHUSDT", side="Buy", orderType="Market", qty="1")
    b.update_mark("ETHUSDT", 2700.0)
    create(b, symbol="ETHUSDT", side="Sell", orderType="Market", qty="1", reduceOnly=True)
    assert b.balance < 10_000.0
    assert float(b.closed_pnl[0]["closedPnl"]) < 0.0


def test_fees_are_charged_and_tracked():
    b = broker(balance=10_000.0); b.update_mark("ETHUSDT", 3000.0)
    create(b, symbol="ETHUSDT", side="Buy", orderType="Market", qty="1")
    assert b.total_fees > 0.0
    assert b.balance < 10_000.0


def test_wallet_balance_includes_unrealised():
    b = broker(balance=10_000.0); b.update_mark("ETHUSDT", 3000.0)
    create(b, symbol="ETHUSDT", side="Buy", orderType="Market", qty="1")
    b.update_mark("ETHUSDT", 3100.0)
    res = run(b.safe_call("GET", "/v5/account/wallet-balance", accountType="UNIFIED"))
    equity = float(res["result"]["list"][0]["totalEquity"])
    wallet = float(res["result"]["list"][0]["totalWalletBalance"])
    assert equity > wallet, "equity must include unrealised PnL"


# --- exchange-native stops -------------------------------------------------

def test_stop_loss_triggers_on_mark_update():
    b = broker(balance=10_000.0); b.update_mark("ETHUSDT", 3000.0)
    create(b, symbol="ETHUSDT", side="Buy", orderType="Market", qty="1",
           stopLoss="2900", takeProfit="3200")
    assert b.positions["ETHUSDT"].stop_loss == pytest.approx(2900.0)
    b.update_mark("ETHUSDT", 2850.0)
    assert "ETHUSDT" not in b.positions
    assert float(b.closed_pnl[0]["closedPnl"]) < 0.0


def test_take_profit_triggers_on_mark_update():
    b = broker(balance=10_000.0); b.update_mark("ETHUSDT", 3000.0)
    create(b, symbol="ETHUSDT", side="Buy", orderType="Market", qty="1",
           stopLoss="2900", takeProfit="3200")
    b.update_mark("ETHUSDT", 3250.0)
    assert "ETHUSDT" not in b.positions
    assert float(b.closed_pnl[0]["closedPnl"]) > 0.0


def test_short_stop_loss_triggers_upward():
    b = broker(balance=10_000.0); b.update_mark("ETHUSDT", 3000.0)
    create(b, symbol="ETHUSDT", side="Sell", orderType="Market", qty="1", stopLoss="3100")
    b.update_mark("ETHUSDT", 3150.0)
    assert "ETHUSDT" not in b.positions


def test_trading_stop_endpoint_updates_brackets():
    b = broker(balance=10_000.0); b.update_mark("ETHUSDT", 3000.0)
    create(b, symbol="ETHUSDT", side="Buy", orderType="Market", qty="1")
    run(b.safe_call("POST", "/v5/position/trading-stop", category="linear",
                    symbol="ETHUSDT", stopLoss="2950"))
    assert b.positions["ETHUSDT"].stop_loss == pytest.approx(2950.0)


# --- guards ----------------------------------------------------------------

def test_order_without_reference_price_is_rejected():
    b = broker()  # no mark set
    res = create(b, symbol="NEWUSDT", side="Buy", orderType="Market", qty="1")
    assert res["retCode"] != 0
    assert b.rejected_count == 1


def test_zero_quantity_rejected():
    b = broker(); b.update_mark("BTCUSDT", 100_000.0)
    assert create(b, symbol="BTCUSDT", side="Buy", orderType="Market", qty="0")["retCode"] != 0


def test_position_list_reports_positionidx():
    b = broker(); b.update_mark("BTCUSDT", 100_000.0)
    create(b, symbol="BTCUSDT", side="Buy", orderType="Market", qty="0.01", positionIdx=0)
    rows = run(b.safe_call("GET", "/v5/position/list", category="linear",
                           symbol="BTCUSDT"))["result"]["list"]
    assert rows and rows[0]["positionIdx"] == 0
    assert float(rows[0]["size"]) == pytest.approx(0.01)
