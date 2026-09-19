"""B1 — TWAP phantom-price notional oversizing.

Audit finding B1 [C]: `_execute_twap_iceberg` calls `_execute_dynamic_maker_peg`
without `depth_snapshot`. Inside the peg, `best_bid` falls back to the literal
100.0, and that phantom price is what the exchange min-notional floor is
computed against. On any instrument priced well above $100 this inflates the
order by orders of magnitude, bypassing the risk vault entirely.

test_b1_reproduction_* demonstrate the defect. The remaining tests specify the
required post-fix behaviour.
"""
import pytest

from execution.sor import SmartOrderRouter
from tests.conftest import (
    FakeExecutor, FakeCoreEngine, BTC_LIMITS, ALT_LIMITS, book, run,
)

BTC_PRICE = 100_000.0
ETH_PRICE = 3_000.0
ALT_PRICE = 0.42


def make_sor(executor=None, core=None, limits=BTC_LIMITS, symbol="BTCUSDT"):
    ex = executor or FakeExecutor()
    sor = SmartOrderRouter(executor=ex, core_engine=core or FakeCoreEngine())
    sor.instrument_cache[symbol] = dict(limits)
    return sor, ex


# ---------------------------------------------------------------------------
# Reproduction: the phantom $100 reference price
# ---------------------------------------------------------------------------

def test_b1_reproduction_maker_peg_without_book_oversizes_btc():
    """A $500 BTC slice must not become a $6,500 order."""
    sor, ex = make_sor()
    intended_qty = 0.005                      # $500 at $100k
    intended_notional = intended_qty * BTC_PRICE

    run(sor._execute_dynamic_maker_peg(
        "BTCUSDT", "BUY", intended_qty, depth_snapshot=None, timeout=1,
    ))

    # Post-fix the correct behaviour is EITHER abort (no book anywhere) OR an
    # order within tolerance. What must never happen is a submitted order whose
    # notional bears no relation to the one risk approved.
    for submitted_qty in ex.submitted_quantities():
        actual_notional = submitted_qty * BTC_PRICE
        deviation = abs(actual_notional - intended_notional) / intended_notional
        assert deviation <= 0.25, (
            f"B1: intended ${intended_notional:,.2f} but submitted "
            f"${actual_notional:,.2f} ({deviation:.1%} deviation, "
            f"{actual_notional / intended_notional:.0f}x)"
        )


def test_b1_reproduction_twap_passes_depth_snapshot_through():
    """TWAP must hand its book down to each slice."""
    sor, ex = make_sor()
    seen = []

    async def spy(*args, **kwargs):
        seen.append(kwargs.get("depth_snapshot", "MISSING"))
        return False, 0.0, 0.0

    sor._execute_dynamic_maker_peg = spy
    snapshot = book(BTC_PRICE)

    run(sor._execute_twap_iceberg(
        "BTCUSDT", "BUY", 0.005, BTC_PRICE, sl=99_000.0, tp=102_000.0,
        depth_snapshot=snapshot, slices=2, slice_interval_sec=0.0,
    ))

    assert seen, "TWAP submitted no slices"
    for got in seen:
        assert got not in (None, "MISSING"), (
            "B1: TWAP slice received no depth_snapshot, forcing the peg onto "
            "its phantom $100 fallback price"
        )


# ---------------------------------------------------------------------------
# Fail-closed on invalid reference prices
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_price", [0.0, -1.0, float("nan"), float("inf")])
def test_invalid_reference_price_fails_closed(bad_price):
    sor, _ = make_sor()
    qty = sor._apply_dynamic_exchange_limits(0.005, bad_price, "BTCUSDT")
    assert qty == 0.0, (
        f"reference price {bad_price!r} must fail closed, got qty={qty}"
    )


def test_missing_book_aborts_rather_than_guessing():
    """No book anywhere => no order, not an order at a made-up price."""
    sor, ex = make_sor(core=FakeCoreEngine(orderbook_snapshots={}))
    ok, price, filled = run(sor._execute_dynamic_maker_peg(
        "BTCUSDT", "BUY", 0.005, depth_snapshot=None, timeout=1,
    ))
    assert ok is False and filled == 0.0
    assert ex.created_orders == [], (
        "submitted an order with no market data available"
    )


def test_empty_book_dict_aborts():
    sor, ex = make_sor()
    ok, _, filled = run(sor._execute_dynamic_maker_peg(
        "BTCUSDT", "BUY", 0.005, depth_snapshot={"bids": [], "asks": []}, timeout=1,
    ))
    assert ok is False and filled == 0.0
    assert ex.created_orders == []


def test_falls_back_to_engine_snapshot_when_slice_has_no_book():
    """A stale-but-real book beats a phantom price."""
    core = FakeCoreEngine(orderbook_snapshots={"BTCUSDT": book(BTC_PRICE)})
    sor, ex = make_sor(core=core)
    run(sor._execute_dynamic_maker_peg(
        "BTCUSDT", "BUY", 0.005, depth_snapshot=None, timeout=1,
    ))
    if ex.created_orders:
        notional = float(ex.created_orders[0]["qty"]) * BTC_PRICE
        assert 375.0 <= notional <= 625.0, f"sized off a wrong price: ${notional:,.2f}"


# ---------------------------------------------------------------------------
# Notional deviation guard, across price scales
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("symbol,price,limits,intended_notional", [
    ("BTCUSDT", BTC_PRICE, BTC_LIMITS, 500.0),
    ("ETHUSDT", ETH_PRICE, {**BTC_LIMITS, "qty_step": __import__("decimal").Decimal("0.01"),
                            "min_qty": __import__("decimal").Decimal("0.01")}, 200.0),
    ("DOGEUSDT", ALT_PRICE, ALT_LIMITS, 50.0),
])
def test_quantisation_stays_within_tolerance(symbol, price, limits, intended_notional):
    sor, _ = make_sor(limits=limits, symbol=symbol)
    raw_qty = intended_notional / price
    cleaned = sor._apply_dynamic_exchange_limits(raw_qty, price, symbol)
    actual_notional = cleaned * price
    floor = float(max(__import__("decimal").Decimal("6.50"),
                      limits["min_notional"] * __import__("decimal").Decimal("1.05")))
    # Only legitimate inflation is up to the exchange minimum notional.
    assert actual_notional <= max(intended_notional * 1.25, floor) + 1e-6, (
        f"{symbol}: intended ${intended_notional:.2f} -> submitted ${actual_notional:.2f}"
    )


def test_sub_minimum_order_is_skipped_not_inflated():
    """
    B21: a risk-sized notional below the exchange floor must be SKIPPED.

    This supersedes the earlier expectation that it be rounded up — rounding up
    silently overrode the risk engine's sizing decision.
    """
    sor, _ = make_sor(limits=ALT_LIMITS, symbol="DOGEUSDT")
    assert sor.skip_below_min_notional is True
    cleaned = sor._apply_dynamic_exchange_limits(1.0, ALT_PRICE, "DOGEUSDT")
    assert cleaned == 0.0, (
        f"B21: sub-minimum order inflated to {cleaned * ALT_PRICE:.2f} USD "
        f"instead of being skipped"
    )


def test_legacy_inflate_behaviour_still_available_behind_flag():
    """Escape hatch, off by default, so the change is reversible without a deploy."""
    sor, _ = make_sor(limits=ALT_LIMITS, symbol="DOGEUSDT")
    sor.skip_below_min_notional = False
    cleaned = sor._apply_dynamic_exchange_limits(1.0, ALT_PRICE, "DOGEUSDT")
    assert cleaned * ALT_PRICE >= 6.50 - 1e-6


def test_normal_sized_order_unaffected_by_b21():
    sor, _ = make_sor()
    cleaned = sor._apply_dynamic_exchange_limits(0.005, BTC_PRICE, "BTCUSDT")
    assert cleaned == pytest.approx(0.005)


def test_notional_guard_rejects_gross_inflation_directly():
    sor, _ = make_sor()
    ok, reason = sor._check_notional_sanity(
        "BTCUSDT", intended_notional=500.0, actual_notional=6500.0,
    )
    assert ok is False and "deviation" in reason.lower()


def test_notional_guard_accepts_normal_rounding():
    sor, _ = make_sor()
    ok, _ = sor._check_notional_sanity(
        "BTCUSDT", intended_notional=500.0, actual_notional=505.0,
    )
    assert ok is True


def test_notional_tolerance_is_configurable():
    sor, _ = make_sor()
    assert hasattr(sor, "notional_deviation_tolerance")
    assert 0.0 < sor.notional_deviation_tolerance < 1.0
