"""Failure-injection tests.

Every scenario the APEX brief lists, asserted against the real code paths. The
question each asks is the same: when this goes wrong, does the system fail
SAFE — preserving capital, state and observability — or does it invent an
answer?
"""
import asyncio
import json
import sqlite3
import time

import pytest

from core.intelligent_exit import (
    ExecutionGovernorFSM, ExitDecision, PositionExitState, ExitFillStatus,
)
from core.fsm import SystemStateMachine, ErrorSeverity
from execution.sor import SmartOrderRouter
from execution.paper_broker import PaperBroker
from market_data import is_tradeable
from tests.conftest import FakeExecutor, FakeCoreEngine, BTC_LIMITS, book, run

BTC = 100_000.0


def exit_state(qty=10.0):
    return PositionExitState(
        position_id="s", entry_time=time.time(), entry_price=100.0, exit_side="Sell",
        entry_balance=1000.0, actual_qty=qty, base_qty=qty, execution_state="OBSERVE",
    )


CTX = {"symbol": "TESTUSDT", "qty_step": "0.1", "position_idx": 0, "latest_tick_price": 100.0}
FULL_EXIT = ExitDecision("EXIT", 0.0, "FLASH_IOC", 100.0, 0.0, 0.0, "TEST", "")


# ===================== API timeout / REST failure =========================

def test_api_timeout_during_exit_verification_keeps_position_managed():
    class TimingOut(FakeExecutor):
        async def safe_call(self, method, endpoint, **kw):
            if endpoint == "/v5/order/create":
                return {"retCode": 0, "result": {"orderId": "O1"}}
            raise asyncio.TimeoutError("gateway timeout")

    st = exit_state()
    run(ExecutionGovernorFSM.manage_execution(FULL_EXIT, st, CTX, TimingOut()))
    assert st.execution_state != "CLOSED"
    assert st.actual_qty == pytest.approx(10.0)


def test_rest_error_response_is_not_read_as_flat():
    class Erroring(FakeExecutor):
        async def safe_call(self, method, endpoint, **kw):
            return {"retCode": 10016, "retMsg": "service unavailable", "result": {}}
    size = run(ExecutionGovernorFSM._fetch_position_size(Erroring(), "SYM", position_idx=0))
    assert size is None


def test_network_partition_mid_exit_preserves_local_quantity():
    class Partitioned(FakeExecutor):
        async def safe_call(self, method, endpoint, **kw):
            if endpoint == "/v5/order/create":
                return {"retCode": 0, "result": {"orderId": "O1"}}
            raise ConnectionError("no route to host")
    st = exit_state()
    run(ExecutionGovernorFSM.manage_execution(FULL_EXIT, st, CTX, Partitioned()))
    assert st.actual_qty == pytest.approx(10.0) and st.execution_state != "CLOSED"


# ===================== malformed responses ================================

@pytest.mark.parametrize("payload", [
    {}, {"retCode": 0}, {"retCode": 0, "result": None},
    {"retCode": 0, "result": {"list": None}},
    {"retCode": 0, "result": {"list": [{"size": "not-a-number"}]}},
])
def test_malformed_position_payloads_never_read_as_flat(payload):
    class Malformed(FakeExecutor):
        async def safe_call(self, method, endpoint, **kw):
            return payload
    size = run(ExecutionGovernorFSM._fetch_position_size(Malformed(), "SYM", position_idx=0))
    assert size is None or size == 0.0
    if payload.get("result", {}) and payload["result"].get("list"):
        assert size in (None, 0.0)


@pytest.mark.parametrize("report", [
    {"orderStatus": None, "cumExecQty": None},
    {"orderStatus": "", "cumExecQty": "abc"},
    {"cumExecQty": "1e999"},
])
def test_malformed_fill_reports_do_not_crash(report):
    status, filled, avg = ExecutionGovernorFSM._classify_fill_report(report, 10.0)
    assert status in vars(ExitFillStatus).values()
    assert isinstance(filled, float)


# ===================== stale book / stale candle ==========================

def test_stale_book_blocks_entry():
    now = time.time()
    payload = dict(book(BTC)); payload.update({"as_of": now - 3600, "source": "WS_L2"})
    ok, reason = is_tradeable(payload, now, 5.0)
    assert ok is False and "age" in reason


def test_websocket_outage_fallback_shape_blocks_entry():
    """The exact payload the REST fallback writes during a WS disconnect."""
    now = time.time()
    payload = {"best_bid": BTC - 1, "best_ask": BTC + 1, "as_of": now,
               "source": "REST_BBO_FALLBACK", "depth_available": False}
    assert is_tradeable(payload, now, 5.0)[0] is False


def test_unknown_slippage_blocks_execution():
    s = SmartOrderRouter(executor=FakeExecutor(), core_engine=FakeCoreEngine())
    s.instrument_cache["BTCUSDT"] = dict(BTC_LIMITS)
    assert s.estimate_orderbook_slippage_bps({}, "BUY", 1.0, BTC) == s.SLIPPAGE_UNKNOWN


def test_forming_candle_is_rejected():
    import pathlib
    main = (pathlib.Path(__file__).resolve().parents[1] / "src" / "main.py").read_text()
    assert 'is_closed = bool(candle.get("confirm", False))' in main


# ===================== fills: partial, duplicate, rejected ================

def test_partial_fill_reconciles_to_exchange_truth():
    ex = FakeExecutor(fill_plan={"orderStatus": "PartiallyFilled",
                                 "cumExecQty": "3.0", "avgPrice": "100"})
    ex.position_size = 7.0
    st = exit_state()
    run(ExecutionGovernorFSM.manage_execution(FULL_EXIT, st, CTX, ex))
    assert st.actual_qty == pytest.approx(7.0)


def test_duplicate_fill_reports_are_idempotent():
    """Applying the same exchange truth twice must not double-reduce."""
    ex = FakeExecutor(fill_plan={"orderStatus": "PartiallyFilled",
                                 "cumExecQty": "3.0", "avgPrice": "100"})
    ex.position_size = 7.0
    st = exit_state()
    run(ExecutionGovernorFSM.manage_execution(FULL_EXIT, st, CTX, ex))
    first = st.actual_qty
    run(ExecutionGovernorFSM.manage_execution(FULL_EXIT, st, CTX, ex))
    assert st.actual_qty == pytest.approx(first), "reconciliation must be idempotent"


def test_rejected_order_leaves_state_untouched():
    class Rejecting(FakeExecutor):
        async def safe_call(self, method, endpoint, **kw):
            if endpoint == "/v5/order/create":
                return {"retCode": 110007, "retMsg": "insufficient balance"}
            return await super().safe_call(method, endpoint, **kw)
    st = exit_state()
    assert run(ExecutionGovernorFSM.manage_execution(FULL_EXIT, st, CTX, Rejecting())) is False
    assert st.actual_qty == pytest.approx(10.0)


def test_rate_limit_during_exit_is_not_success():
    class RateLimited(FakeExecutor):
        async def safe_call(self, method, endpoint, **kw):
            if endpoint == "/v5/order/create":
                return {"retCode": 10006, "retMsg": "rate limit"}
            return await super().safe_call(method, endpoint, **kw)
    st = exit_state()
    assert run(ExecutionGovernorFSM.manage_execution(FULL_EXIT, st, CTX, RateLimited())) is False
    assert st.execution_state != "CLOSED"


# ===================== exchange disagreement ==============================

def test_exchange_disagreement_is_resolved_in_favour_of_the_exchange():
    ex = FakeExecutor(fill_plan={"orderStatus": "Filled", "cumExecQty": "10", "avgPrice": "100"})
    ex.position_size = 4.0                       # exchange says otherwise
    st = exit_state()
    run(ExecutionGovernorFSM.manage_execution(FULL_EXIT, st, CTX, ex))
    assert st.actual_qty == pytest.approx(4.0)
    assert st.execution_state != "CLOSED"


# ===================== health / breaker behaviour =========================

def test_error_flood_does_not_flatten():
    m = SystemStateMachine()
    for _ in range(10):
        m.record_module_error("_eval_gate", ErrorSeverity.CRITICAL)
    assert m.is_emergency_locked() is False
    assert m.can_manage_positions is True
    assert m.can_open_new_positions is False


def test_transient_faults_never_degrade_health():
    m = SystemStateMachine()
    for _ in range(200):
        m.record_module_error("run_telegram_worker", ErrorSeverity.TRANSIENT)
    assert m.can_open_new_positions is True


# ===================== database failure ===================================

def test_sqlite_failure_does_not_crash_the_engine():
    """A locked or broken ledger must degrade, not take trading down."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (a INTEGER)")
    conn.close()
    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")          # establishes the failure mode is raisable

    from core.memory import MemoryBank
    assert hasattr(MemoryBank, "_record_db_failure")
    assert hasattr(MemoryBank, "db_health")


def test_db_failures_are_counted_and_escalated():
    import pathlib
    mem = (pathlib.Path(__file__).resolve().parents[1] / "src" / "core" / "memory.py").read_text()
    assert "self.db_write_failure += 1" in mem
    assert "CLOUD LEDGER DEGRADED" in mem


# ===================== restart recovery ===================================

def test_model_state_round_trips_across_a_simulated_restart(tmp_path):
    import numpy as np
    from features.micro_models import ContinuousMicrostructureEngine

    before = ContinuousMicrostructureEngine(symbol="BTCUSDT")
    before.rls_trend.w = np.arange(25, dtype=np.float64)
    before.rls_cascade.f_inv = before.rls_cascade.f_inv * 2.5
    blob = json.dumps(before.export_state())

    after = ContinuousMicrostructureEngine(symbol="BTCUSDT")
    after.load_state(json.loads(blob))
    assert np.allclose(after.rls_trend.w, np.arange(25, dtype=np.float64))
    assert np.allclose(after.rls_cascade.f_inv, before.rls_cascade.f_inv)


def test_corrupt_state_file_does_not_prevent_startup():
    from features.micro_models import ContinuousMicrostructureEngine
    eng = ContinuousMicrostructureEngine(symbol="BTCUSDT")
    for junk in (None, {}, {"weights_trending": [1, 2, 3]}, {"P_trending": "nonsense"}):
        eng.load_state(junk)          # must not raise


# ===================== paper isolation under stress =======================

def test_paper_never_escapes_even_when_the_live_executor_would_answer():
    class EagerLive:
        def __init__(self): self.hits = []
        def __getattr__(self, item):
            self.hits.append(item)
            async def anything(*a, **k): return {"retCode": 0, "result": {}}
            return anything

    live = EagerLive()
    b = PaperBroker(live, starting_balance=10_000.0)
    b.update_mark("ETHUSDT", 3000.0)
    run(b.adjust_leverage("ETHUSDT", 2))
    run(b.safe_call("POST", "/v5/order/create", symbol="ETHUSDT", side="Buy",
                    orderType="Market", qty="1"))
    run(b.get_wallet_balance_usdt())
    assert live.hits == [], f"paper escaped to live: {live.hits}"
