"""Property-based invariant tests.

Example-based tests check the cases we thought of. These assert properties that
must hold for ALL inputs — which is how the original audit's defects would have
been caught, since each was a case nobody thought to write.

Invariants under test (from the APEX brief):
  * a risk-approved quantity cannot become larger downstream
  * reduce-only cannot increase exposure
  * a closed position cannot reopen through settlement
  * paper cannot mutate the live account
  * stale data cannot become current data
  * an unknown fill cannot become a zero fill
  * an unknown position cannot become flat
"""
import math
import time
from decimal import Decimal

import pytest
from hypothesis import given, settings, strategies as st, HealthCheck

from execution.sor import SmartOrderRouter
from execution.paper_broker import PaperBroker, PaperIsolationError
from core.intelligent_exit import ExecutionGovernorFSM, ExitFillStatus, _safe_float
from market_data import is_tradeable
from probability import make_estimate
from equity import EquitySnapshot, estimate_live_equity, compute_drawdown
from tests.conftest import FakeExecutor, FakeCoreEngine, run

SETTINGS = settings(max_examples=150, deadline=None,
                    suppress_health_check=[HealthCheck.function_scoped_fixture])

prices = st.floats(min_value=0.0001, max_value=500_000.0,
                   allow_nan=False, allow_infinity=False)
qtys = st.floats(min_value=1e-8, max_value=1e6,
                 allow_nan=False, allow_infinity=False)


def sor_for(step="0.001", min_qty="0.001", min_notional="5.0"):
    s = SmartOrderRouter(executor=FakeExecutor(), core_engine=FakeCoreEngine())
    s.instrument_cache["SYM"] = {
        "min_qty": Decimal(min_qty), "qty_step": Decimal(step),
        "tick_size": Decimal("0.01"), "min_notional": Decimal(min_notional),
    }
    return s


# ---- INVARIANT: approved quantity cannot grow downstream ------------------

@SETTINGS
@given(raw_qty=qtys, price=prices)
def test_quantisation_never_silently_inflates_beyond_the_floor(raw_qty, price):
    """
    B1/B21: the only legitimate upward move is to the exchange minimum, and with
    skip_below_min_notional on (the default) even that is refused.
    """
    s = sor_for()
    out = s._apply_dynamic_exchange_limits(raw_qty, price, "SYM")
    assert out >= 0.0
    if out > 0.0:
        assert out <= raw_qty + float(Decimal("0.001")), (
            f"quantisation inflated {raw_qty} -> {out}"
        )


@SETTINGS
@given(price=st.floats(min_value=-1e6, max_value=0.0, allow_nan=False, allow_infinity=False))
def test_non_positive_price_always_fails_closed(price):
    assert sor_for()._apply_dynamic_exchange_limits(1.0, price, "SYM") == 0.0


@SETTINGS
@given(intended=st.floats(min_value=1.0, max_value=1e6, allow_nan=False, allow_infinity=False),
       factor=st.floats(min_value=1.3, max_value=1000.0, allow_nan=False, allow_infinity=False))
def test_gross_inflation_is_always_rejected(intended, factor):
    s = sor_for()
    inflated = intended * factor
    ok, _ = s._check_notional_sanity("SYM", intended, inflated)
    min_floor = 6.5
    if inflated > max(intended * 1.25, min_floor):
        assert ok is False


# ---- INVARIANT: reduce-only cannot increase exposure ---------------------

@SETTINGS
@given(open_qty=st.floats(min_value=0.01, max_value=100.0, allow_nan=False, allow_infinity=False),
       close_qty=st.floats(min_value=0.01, max_value=1000.0, allow_nan=False, allow_infinity=False),
       price=st.floats(min_value=1.0, max_value=5000.0, allow_nan=False, allow_infinity=False))
def test_reduce_only_never_increases_exposure(open_qty, close_qty, price):
    b = PaperBroker(FakeExecutor(), starting_balance=1e9)
    b.update_mark("SYM", price)
    run(b.adjust_leverage("SYM", 2))
    run(b.safe_call("POST", "/v5/order/create", symbol="SYM", side="Buy",
                    orderType="Market", qty=str(open_qty)))
    before = b.positions["SYM"].size if "SYM" in b.positions else 0.0
    run(b.safe_call("POST", "/v5/order/create", symbol="SYM", side="Sell",
                    orderType="Market", qty=str(close_qty), reduceOnly=True))
    after = b.positions["SYM"].size if "SYM" in b.positions else 0.0
    assert after <= before + 1e-9, f"reduceOnly grew exposure {before} -> {after}"


@SETTINGS
@given(qty=qtys, price=st.floats(min_value=1.0, max_value=5000.0,
                                 allow_nan=False, allow_infinity=False))
def test_reduce_only_on_flat_book_never_opens(qty, price):
    b = PaperBroker(FakeExecutor(), starting_balance=1e9)
    b.update_mark("SYM", price)
    run(b.safe_call("POST", "/v5/order/create", symbol="SYM", side="Sell",
                    orderType="Market", qty=str(qty), reduceOnly=True))
    assert "SYM" not in b.positions


# ---- INVARIANT: paper cannot mutate the live account ---------------------

@SETTINGS
@given(name=st.text(alphabet=st.characters(whitelist_categories=("Ll",)), min_size=3, max_size=25))
def test_undeclared_attributes_never_reach_the_live_executor(name):
    """
    `__getattr__` fires only when normal lookup FAILS, so the guard here has to
    exclude the broker's own instance attributes as well as its class
    attributes. An earlier version checked only the class, and passed for a long
    time purely because hypothesis had not yet generated the name of an instance
    attribute -- it eventually produced 'leverage' (set as `self.leverage` in
    __init__) and the test failed on its own bug rather than on a defect.
    """
    allow = PaperBroker._PUBLIC_READONLY_PASSTHROUGH
    b = PaperBroker(FakeExecutor(), starting_balance=100.0)
    if name in allow or name in vars(b) or hasattr(type(b), name):
        return
    with pytest.raises((PaperIsolationError, AttributeError)):
        getattr(b, name)


# ---- INVARIANT: stale data cannot become current -------------------------

@SETTINGS
@given(age=st.floats(min_value=5.001, max_value=1e6, allow_nan=False, allow_infinity=False))
def test_data_older_than_the_limit_is_never_tradeable(age):
    now = time.time()
    payload = {"bids": [[100.0, 1.0]], "asks": [[101.0, 1.0]], "as_of": now - age}
    ok, _ = is_tradeable(payload, now, max_age_sec=5.0)
    assert ok is False


@SETTINGS
@given(age=st.floats(min_value=0.0, max_value=4.9, allow_nan=False, allow_infinity=False))
def test_fresh_complete_data_is_always_tradeable(age):
    now = time.time()
    payload = {"bids": [[100.0, 1.0]], "asks": [[101.0, 1.0]], "as_of": now - age}
    ok, reason = is_tradeable(payload, now, max_age_sec=5.0)
    assert ok is True, reason


# ---- INVARIANT: unknown fill cannot become zero fill ---------------------

@SETTINGS
@given(requested=st.floats(min_value=0.001, max_value=1000.0,
                           allow_nan=False, allow_infinity=False))
def test_missing_report_is_unknown_never_unfilled(requested):
    status, filled, _ = ExecutionGovernorFSM._classify_fill_report({}, requested)
    assert status == ExitFillStatus.UNKNOWN
    assert filled == 0.0


@SETTINGS
@given(requested=st.floats(min_value=0.01, max_value=1000.0, allow_nan=False, allow_infinity=False),
       filled=st.floats(min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False))
def test_classification_never_reports_more_filled_than_reported(requested, filled):
    status, got, _ = ExecutionGovernorFSM._classify_fill_report(
        {"orderStatus": "PartiallyFilled", "cumExecQty": str(filled)}, requested)
    assert got == pytest.approx(filled)
    if filled >= requested * 0.999:
        assert status == ExitFillStatus.FILLED
    elif filled > 0:
        assert status == ExitFillStatus.PARTIALLY_FILLED


@SETTINGS
@given(v=st.one_of(st.none(), st.text(max_size=8), st.floats(allow_nan=True, allow_infinity=True)))
def test_safe_float_never_raises_and_never_returns_nan(v):
    out = _safe_float(v)
    assert isinstance(out, float) and math.isfinite(out)


# ---- INVARIANT: unknown position cannot become flat ----------------------

@SETTINGS
@given(code=st.integers(min_value=1, max_value=200000))
def test_unreachable_exchange_is_never_read_as_flat(code):
    class Broken:
        async def safe_call(self, *a, **k):
            return {"retCode": code, "result": {}}
    size = run(ExecutionGovernorFSM._fetch_position_size(Broken(), "SYM", position_idx=0))
    assert size is None, "a failed position query must be UNKNOWN, never 0.0"


# ---- INVARIANT: probability direction --------------------------------------

@SETTINGS
@given(p=st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False))
def test_continuation_probabilities_are_complementary(p):
    e = make_estimate(p, horizon_seconds=60)
    assert e.continuation_prob(True) + e.continuation_prob(False) == pytest.approx(1.0)


@SETTINGS
@given(p=st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False))
def test_confidence_is_direction_free(p):
    assert make_estimate(p, horizon_seconds=60).confidence == pytest.approx(
        make_estimate(1.0 - p, horizon_seconds=60).confidence)


# ---- INVARIANT: equity accounting ------------------------------------------

@SETTINGS
@given(wallet=st.floats(min_value=0.01, max_value=1e7, allow_nan=False, allow_infinity=False),
       unreal=st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False))
def test_live_equity_never_double_counts(wallet, unreal):
    snap = EquitySnapshot(wallet_balance=wallet, equity=wallet + unreal, as_of=0.0)
    live = estimate_live_equity(snap, {"SYM": unreal})
    assert live == pytest.approx(wallet + unreal, rel=1e-9, abs=1e-6)


@SETTINGS
@given(peak=st.floats(min_value=0.0, max_value=1e7, allow_nan=False, allow_infinity=False),
       cur=st.floats(min_value=0.0, max_value=1e7, allow_nan=False, allow_infinity=False))
def test_drawdown_is_bounded_zero_to_one(peak, cur):
    dd = compute_drawdown(peak, cur)
    assert 0.0 <= dd <= 1.0
