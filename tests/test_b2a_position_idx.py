"""B2a — `_fetch_position_size` must respect positionIdx.

A defect introduced by the B2 emergency patch itself: reading `rows[0]` without
filtering on positionIdx. Bybit returns one row per side, and in hedge mode
(1 = Buy, 2 = Sell) rows[0] can be the OPPOSITE side -- wrong in the dangerous
direction, because a zero-size opposite side would be read as "we are flat".
"""
import pytest

from core.intelligent_exit import ExecutionGovernorFSM, ExitDecision, PositionExitState
from tests.conftest import run


class IdxExecutor:
    """Returns a configurable position/list payload."""

    def __init__(self, rows, ret_code=0):
        self.rows = rows
        self.ret_code = ret_code
        self.created = []

    async def safe_call(self, method, endpoint, **kwargs):
        if endpoint == "/v5/order/create":
            self.created.append(kwargs)
            return {"retCode": 0, "result": {"orderId": "OID-1"}}
        if endpoint == "/v5/position/list":
            return {"retCode": self.ret_code, "result": {"list": self.rows}}
        if endpoint in ("/v5/order/realtime", "/v5/order/history"):
            return {"retCode": 0, "result": {"list": [
                {"orderStatus": "Cancelled", "cumExecQty": "0", "avgPrice": ""}
            ]}}
        return {"retCode": 0, "result": {"list": []}}


def row(idx, size, side):
    return {"positionIdx": idx, "size": str(size), "side": side, "avgPrice": "100"}


# ---------------------------------------------------------------------------

def test_hedge_mode_reads_our_side_not_row_zero():
    """Buy side (idx 1) holds 7; Sell side (idx 2) is flat and listed first."""
    ex = IdxExecutor(rows=[row(2, 0, "Sell"), row(1, 7.0, "Buy")])
    size = run(ExecutionGovernorFSM._fetch_position_size(ex, "TESTUSDT", position_idx=1))
    assert size == pytest.approx(7.0), (
        f"B2a: read the wrong side -- got {size}, expected 7.0 (idx=1)"
    )


def test_hedge_mode_row_zero_would_have_been_wrong():
    """Guard-rail: confirm the naive rows[0] read really would have said 'flat'."""
    rows = [row(2, 0, "Sell"), row(1, 7.0, "Buy")]
    assert float(rows[0]["size"]) == 0.0


def test_one_way_mode_with_idx_zero():
    ex = IdxExecutor(rows=[row(0, 4.0, "Buy")])
    size = run(ExecutionGovernorFSM._fetch_position_size(ex, "TESTUSDT", position_idx=0))
    assert size == pytest.approx(4.0)


def test_one_way_payload_without_position_idx_field():
    """Some one-way payloads omit positionIdx entirely; single row is unambiguous."""
    ex = IdxExecutor(rows=[{"size": "3.0", "side": "Buy", "avgPrice": "100"}])
    size = run(ExecutionGovernorFSM._fetch_position_size(ex, "TESTUSDT", position_idx=0))
    assert size == pytest.approx(3.0)


def test_empty_list_is_flat():
    ex = IdxExecutor(rows=[])
    assert run(ExecutionGovernorFSM._fetch_position_size(ex, "TESTUSDT", position_idx=0)) == 0.0


def test_no_matching_idx_is_unknown_not_zero():
    """The dangerous case: never resolve a non-match to 'flat'."""
    ex = IdxExecutor(rows=[row(1, 5.0, "Buy"), row(2, 2.0, "Sell")])
    size = run(ExecutionGovernorFSM._fetch_position_size(ex, "TESTUSDT", position_idx=7))
    assert size is None, f"non-matching idx must be UNKNOWN, got {size}"


def test_unreachable_exchange_is_unknown():
    ex = IdxExecutor(rows=[], ret_code=10006)
    assert run(ExecutionGovernorFSM._fetch_position_size(ex, "TESTUSDT", position_idx=0)) is None


def test_hedge_mode_exit_does_not_close_on_opposite_side_flatness():
    """End-to-end: a zero-fill exit in hedge mode must not mark CLOSED."""
    ex = IdxExecutor(rows=[row(2, 0, "Sell"), row(1, 10.0, "Buy")])
    state = PositionExitState(
        position_id="s", entry_time=0.0, entry_price=100.0, exit_side="Sell",
        entry_balance=1000.0, actual_qty=10.0, base_qty=10.0, execution_state="OBSERVE",
    )
    ctx = {"symbol": "TESTUSDT", "qty_step": "0.1", "position_idx": 1,
           "latest_tick_price": 100.0}
    run(ExecutionGovernorFSM.manage_execution(
        ExitDecision("EXIT", 0.0, "FLASH_IOC", 100.0, 0.0, 0.0, "TEST", ""), state, ctx, ex,
    ))
    assert state.execution_state != "CLOSED"
    assert state.actual_qty == pytest.approx(10.0)
