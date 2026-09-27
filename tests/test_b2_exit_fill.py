"""B2 — IOC exit fill accounting.

Audit finding B2 [C]: `ExecutionGovernorFSM.manage_execution` treated
`retCode == 0` as proof the exit filled. A Limit-IOC with a 15 bps collar that
fills nothing also returns retCode 0, so the position was marked CLOSED, the
lifecycle daemon broke its monitoring loop, and a live position was left
unmonitored on the exchange.

Invariant under test: the engine may only declare an exit complete when
exchange state confirms the remaining position is zero.
"""
import pytest

from core.intelligent_exit import (
    ExecutionGovernorFSM, ExitDecision, PositionExitState, ExitFillStatus,
)
from tests.conftest import FakeExecutor, run


ENTRY = 100.0
QTY = 10.0


def make_state(qty=QTY):
    return PositionExitState(
        position_id="sig-1", entry_time=0.0, entry_price=ENTRY,
        exit_side="Sell", entry_balance=1000.0,
        actual_qty=qty, base_qty=qty, execution_state="OBSERVE",
    )


def make_ctx():
    return {"symbol": "TESTUSDT", "qty_step": "0.1", "position_idx": 0,
            "latest_tick_price": ENTRY}


def full_exit(price=ENTRY):
    return ExitDecision("EXIT", 0.0, "FLASH_IOC", price, 0.0, 0.0, "TEST", "")


def scale_out(price=ENTRY):
    return ExitDecision("SCALE_OUT", 0.5, "FLASH_IOC", price, 0.0, 0.0, "TEST", "")


# ---------------------------------------------------------------------------
# The core defect
# ---------------------------------------------------------------------------

def test_zero_fill_ioc_must_not_mark_position_closed():
    """The headline B2 case: retCode 0, zero fill, position still live."""
    ex = FakeExecutor(fill_plan={"orderStatus": "Cancelled", "cumExecQty": "0", "avgPrice": ""})
    ex.position_size = QTY          # exchange still shows the full position
    ex.position_avg = ENTRY
    state = make_state()

    run(ExecutionGovernorFSM.manage_execution(full_exit(), state, make_ctx(), ex))

    assert state.execution_state != "CLOSED", (
        "B2: a zero-fill IOC was recorded as a completed exit"
    )
    assert state.actual_qty == pytest.approx(QTY), (
        f"B2: local qty zeroed while exchange still holds {ex.position_size}"
    )
    assert state.q_retained > 0.0


def test_full_fill_confirmed_flat_marks_closed():
    ex = FakeExecutor(fill_plan={"orderStatus": "Filled", "cumExecQty": str(QTY), "avgPrice": str(ENTRY)})
    ex.position_size = 0.0          # exchange confirms flat
    state = make_state()

    run(ExecutionGovernorFSM.manage_execution(full_exit(), state, make_ctx(), ex))

    assert state.execution_state == "CLOSED"
    assert state.actual_qty == pytest.approx(0.0)
    assert state.q_retained == pytest.approx(0.0)


def test_partial_fill_subtracts_filled_not_requested():
    """Remaining must be original - actual filled, never original - requested."""
    ex = FakeExecutor(fill_plan={"orderStatus": "PartiallyFilled", "cumExecQty": "3.0", "avgPrice": str(ENTRY)})
    ex.position_size = 7.0          # exchange agrees: 7 left
    state = make_state()

    run(ExecutionGovernorFSM.manage_execution(full_exit(), state, make_ctx(), ex))

    assert state.execution_state != "CLOSED"
    assert state.actual_qty == pytest.approx(7.0), (
        f"expected 7.0 remaining after a 3.0 fill, got {state.actual_qty}"
    )


def test_scale_out_partial_fill_uses_filled_quantity():
    """Scale-out asked for 5.0 but only 2.0 filled => 8.0 remains, not 5.0."""
    ex = FakeExecutor(fill_plan={"orderStatus": "Cancelled", "cumExecQty": "2.0", "avgPrice": str(ENTRY)})
    ex.position_size = 8.0
    state = make_state()

    run(ExecutionGovernorFSM.manage_execution(scale_out(), state, make_ctx(), ex))

    assert state.actual_qty == pytest.approx(8.0), (
        f"scale-out subtracted the requested qty, not the filled qty "
        f"(got {state.actual_qty}, expected 8.0)"
    )
    assert state.execution_state != "CLOSED"


# ---------------------------------------------------------------------------
# Ambiguity and failure modes
# ---------------------------------------------------------------------------

def test_rejected_order_leaves_position_under_management():
    class RejectingExecutor(FakeExecutor):
        async def safe_call(self, method, endpoint, **kwargs):
            if endpoint == "/v5/order/create":
                return {"retCode": 110007, "retMsg": "insufficient balance"}
            return await super().safe_call(method, endpoint, **kwargs)

    ex = RejectingExecutor()
    ex.position_size = QTY
    state = make_state()

    ok = run(ExecutionGovernorFSM.manage_execution(full_exit(), state, make_ctx(), ex))

    assert ok is False
    assert state.execution_state != "CLOSED"
    assert state.actual_qty == pytest.approx(QTY)


def test_missing_execution_response_is_unknown_not_closed():
    """No order record anywhere => UNKNOWN => reconcile, never assume closed."""
    class SilentExecutor(FakeExecutor):
        async def safe_call(self, method, endpoint, **kwargs):
            if endpoint in ("/v5/order/realtime", "/v5/order/history"):
                return {"retCode": 0, "result": {"list": []}}
            return await super().safe_call(method, endpoint, **kwargs)

    ex = SilentExecutor()
    ex.position_size = QTY
    state = make_state()

    run(ExecutionGovernorFSM.manage_execution(full_exit(), state, make_ctx(), ex))

    assert state.execution_state != "CLOSED"
    assert state.actual_qty == pytest.approx(QTY)


def test_timeout_on_verification_preserves_monitoring():
    class TimingOutExecutor(FakeExecutor):
        async def safe_call(self, method, endpoint, **kwargs):
            if endpoint in ("/v5/order/realtime", "/v5/order/history", "/v5/position/list"):
                raise TimeoutError("exchange unreachable")
            return await super().safe_call(method, endpoint, **kwargs)

    ex = TimingOutExecutor()
    state = make_state()

    run(ExecutionGovernorFSM.manage_execution(full_exit(), state, make_ctx(), ex))

    assert state.execution_state != "CLOSED", (
        "verification timeout must not be read as a successful exit"
    )
    assert state.actual_qty == pytest.approx(QTY)


def test_exchange_position_disagreement_wins():
    """Order report claims a full fill, exchange says size remains: trust the exchange."""
    ex = FakeExecutor(fill_plan={"orderStatus": "Filled", "cumExecQty": str(QTY), "avgPrice": str(ENTRY)})
    ex.position_size = 4.0          # disagreement
    state = make_state()

    run(ExecutionGovernorFSM.manage_execution(full_exit(), state, make_ctx(), ex))

    assert state.execution_state != "CLOSED"
    assert state.actual_qty == pytest.approx(4.0), (
        "local state must be reconciled to the exchange's reported size"
    )


# ---------------------------------------------------------------------------
# Status classification
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("report,requested,expected", [
    ({"orderStatus": "Filled", "cumExecQty": "10"}, 10.0, ExitFillStatus.FILLED),
    ({"orderStatus": "PartiallyFilled", "cumExecQty": "4"}, 10.0, ExitFillStatus.PARTIALLY_FILLED),
    ({"orderStatus": "Cancelled", "cumExecQty": "0"}, 10.0, ExitFillStatus.UNFILLED),
    ({"orderStatus": "Cancelled", "cumExecQty": "4"}, 10.0, ExitFillStatus.PARTIALLY_FILLED),
    ({"orderStatus": "Rejected", "cumExecQty": "0"}, 10.0, ExitFillStatus.CANCELLED),
    ({}, 10.0, ExitFillStatus.UNKNOWN),
])
def test_status_classification(report, requested, expected):
    status, _, _ = ExecutionGovernorFSM._classify_fill_report(report, requested)
    assert status == expected


def test_malformed_quantities_do_not_crash():
    status, filled, avg = ExecutionGovernorFSM._classify_fill_report(
        {"orderStatus": "Filled", "cumExecQty": "", "avgPrice": "abc"}, 10.0
    )
    assert filled == 0.0 and avg == 0.0
    assert status in (ExitFillStatus.UNFILLED, ExitFillStatus.UNKNOWN)
