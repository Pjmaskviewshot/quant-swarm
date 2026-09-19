"""Regression guard: the B1/B2 patches must not alter normal trading semantics.

An emergency patch that blocks legitimate orders is its own outage. These tests
pin the behaviour of the ordinary entry and exit paths.
"""
import pytest

from execution.sor import SmartOrderRouter
from core.intelligent_exit import ExecutionGovernorFSM, ExitDecision, PositionExitState
from tests.conftest import FakeExecutor, FakeCoreEngine, BTC_LIMITS, book, run

BTC_PRICE = 100_000.0


def test_normal_flash_strike_still_executes_at_correct_size():
    ex = FakeExecutor(fill_plan={
        "orderStatus": "Filled", "cumExecQty": "0.005", "avgPrice": str(BTC_PRICE),
    })
    core = FakeCoreEngine(orderbook_snapshots={"BTCUSDT": book(BTC_PRICE)})
    sor = SmartOrderRouter(executor=ex, core_engine=core)
    sor.instrument_cache["BTCUSDT"] = dict(BTC_LIMITS)

    ok, avg_price, filled = run(sor.execute_alpha_signal(
        symbol="BTCUSDT", direction="BUY", prob_success=0.62, exec_weight=1.0,
        current_mid_price=BTC_PRICE, sl_price=98_000.0, tp_price=104_000.0,
        inst_var=1e-5, depth_snapshot=book(BTC_PRICE),
        target_notional=500.0, regime="TRENDING",
    ))

    assert ok is True, "a well-formed signal with a healthy book was blocked"
    assert filled == pytest.approx(0.005, rel=1e-6)
    submitted = [float(o["qty"]) for o in ex.created_orders if "qty" in o]
    assert submitted, "no order reached the exchange"
    assert submitted[0] * BTC_PRICE == pytest.approx(500.0, rel=0.25)


def test_normal_maker_peg_with_book_still_sizes_correctly():
    ex = FakeExecutor(fill_plan={
        "orderStatus": "Filled", "cumExecQty": "0.005", "avgPrice": str(BTC_PRICE),
    })
    sor = SmartOrderRouter(executor=ex, core_engine=FakeCoreEngine())
    sor.instrument_cache["BTCUSDT"] = dict(BTC_LIMITS)

    ok, price, filled = run(sor._execute_dynamic_maker_peg(
        "BTCUSDT", "BUY", 0.005, depth_snapshot=book(BTC_PRICE), timeout=2,
    ))

    assert ok is True and filled == pytest.approx(0.005, rel=1e-6)
    submitted = [float(o["qty"]) for o in ex.created_orders if "qty" in o]
    assert submitted[0] * BTC_PRICE == pytest.approx(500.0, rel=0.25)


def test_clean_full_exit_still_closes():
    """The ordinary case -- IOC fills fully, exchange flat -- must still close."""
    ex = FakeExecutor(fill_plan={
        "orderStatus": "Filled", "cumExecQty": "10.0", "avgPrice": "100.0",
    })
    ex.position_size = 0.0
    state = PositionExitState(
        position_id="s", entry_time=0.0, entry_price=100.0, exit_side="Sell",
        entry_balance=1000.0, actual_qty=10.0, base_qty=10.0, execution_state="OBSERVE",
    )
    ok = run(ExecutionGovernorFSM.manage_execution(
        ExitDecision("EXIT", 0.0, "FLASH_IOC", 100.0, 0.0, 0.0, "TP", ""),
        state, {"symbol": "TESTUSDT", "qty_step": "0.1", "position_idx": 0}, ex,
    ))
    assert ok is True and state.execution_state == "CLOSED"


def test_hold_decision_is_still_a_noop():
    ex = FakeExecutor()
    state = PositionExitState(
        position_id="s", entry_time=0.0, entry_price=100.0, exit_side="Sell",
        entry_balance=1000.0, actual_qty=10.0, base_qty=10.0,
    )
    ok = run(ExecutionGovernorFSM.manage_execution(
        ExitDecision("HOLD", 1.0, "NONE", 100.0, 0.0, 0.0, "hold", ""),
        state, {"symbol": "TESTUSDT", "qty_step": "0.1", "position_idx": 0}, ex,
    ))
    assert ok is False and ex.created_orders == []


def test_exit_engine_evaluate_unchanged_on_normal_tick():
    """The CAMB decision logic itself must be untouched by these patches."""
    from core.intelligent_exit import IntelligentExitEngine
    state = PositionExitState(
        position_id="s", entry_time=__import__("time").time(), entry_price=100.0,
        exit_side="Sell", entry_balance=1000.0, actual_qty=10.0, base_qty=10.0,
    )
    ctx = {
        "symbol": "TESTUSDT", "is_buy": True, "atr": 1.0,
        "last_ob": {"best_bid": 100.5, "best_ask": 100.6},
        "latest_tick_price": 100.5, "mark_price": 100.5,
        "initial_risk_dist": 2.5, "drawdown_pct": 0.0, "max_drawdown_pct": 0.15,
        "taker_fee_rate": 0.00055, "slippage_buffer_pct": 0.0004,
    }
    decision = IntelligentExitEngine.evaluate(ctx, state)
    assert decision.action == "HOLD"
