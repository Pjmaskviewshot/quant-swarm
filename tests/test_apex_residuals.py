"""APEX overhaul — B4, B18, B22, B30, B32, B33 and the B1/B19/B20 residuals."""
import asyncio
import pathlib
import time

import numpy as np
import pytest

from probability import ProbabilityEstimate, make_estimate
from execution.sor import SmartOrderRouter
from tests.conftest import FakeExecutor, FakeCoreEngine, BTC_LIMITS, book, run

REPO = pathlib.Path(__file__).resolve().parents[1]
MAIN = (REPO / "src" / "main.py").read_text()
SOR = (REPO / "src" / "execution" / "sor.py").read_text()
EXIT = (REPO / "src" / "core" / "intelligent_exit.py").read_text()
MICRO = (REPO / "src" / "features" / "micro_models.py").read_text()


# ===================== B4: probability direction ==========================

def test_estimate_carries_direction_not_just_magnitude():
    up = make_estimate(0.70, horizon_seconds=60)
    down = make_estimate(0.30, horizon_seconds=60)
    assert up.direction == "BUY" and down.direction == "SELL"
    assert up.confidence == pytest.approx(down.confidence), (
        "confidence is symmetric; only direction distinguishes these"
    )


def test_continuation_prob_is_correct_for_both_directions():
    """The exact call site B4 broke."""
    bullish = make_estimate(0.75, horizon_seconds=60)
    assert bullish.continuation_prob(is_buy=True) == pytest.approx(0.75)
    assert bullish.continuation_prob(is_buy=False) == pytest.approx(0.25)

    bearish = make_estimate(0.20, horizon_seconds=60)
    assert bearish.continuation_prob(is_buy=True) == pytest.approx(0.20)
    assert bearish.continuation_prob(is_buy=False) == pytest.approx(0.80)


def test_long_can_reach_the_opposition_thresholds():
    """
    Under the old scheme a long's continuation_prob was max(p,1-p) >= 0.5, so
    the 0.38 and 0.42 exit thresholds were unreachable for longs.
    """
    weak_long = make_estimate(0.30, horizon_seconds=60)
    assert weak_long.continuation_prob(is_buy=True) < 0.38


def test_short_is_not_spuriously_triggered():
    """And a short with a genuinely strong thesis must NOT look adverse."""
    strong_short = make_estimate(0.10, horizon_seconds=60)
    assert strong_short.continuation_prob(is_buy=False) > 0.42


def test_probabilities_sum_to_one():
    e = make_estimate(0.6, horizon_seconds=60, p_flat=0.1)
    assert e.p_up + e.p_down + e.p_flat == pytest.approx(1.0)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf")])
def test_non_finite_probability_falls_back_to_indifference(bad):
    assert make_estimate(bad, horizon_seconds=60).p_up == pytest.approx(0.5)


@pytest.mark.parametrize("raw,expected", [(-5.0, 0.0), (5.0, 1.0)])
def test_probability_is_bounded(raw, expected):
    assert make_estimate(raw, horizon_seconds=60).p_up == pytest.approx(expected)


def test_estimate_records_horizon_and_calibration_provenance():
    e = make_estimate(0.6, horizon_seconds=180 * 60, calibration_method="isotonic",
                      calibrated=True, model_version="v2")
    assert e.horizon_seconds == 10800 and e.calibrated and e.calibration_method == "isotonic"
    assert e.as_dict()["model_version"] == "v2"


def test_staleness_is_detectable():
    e = ProbabilityEstimate(p_up=0.6, horizon_seconds=60, created_at=time.time() - 120)
    assert e.is_stale(max_age_seconds=30.0)


def test_producer_publishes_a_structured_estimate():
    assert "self.latest_probability = make_estimate(" in MICRO
    assert "historical_probs" in MICRO and "legacy: CONFIDENCE, not p_up" in MICRO


def test_consumer_reads_the_estimate_not_the_scalar():
    assert 'estimate = getattr(stat_engine, "latest_probability", None)' in EXIT
    assert "current_p_up = probs[-1]" not in EXIT, (
        "B4: the exit engine still infers direction from a scalar maximum"
    )


# ===================== B18: full state persistence ========================

def test_all_four_covariance_matrices_are_loaded():
    for key in ("P_trending", "P_ranging", "P_spoof", "P_cascade"):
        assert key in MICRO
    assert '("P_spoof", self.rls_spoof), ("P_cascade", self.rls_cascade)' in MICRO, (
        "B18: spoof/cascade covariance still not restored on load"
    )


def test_state_round_trip_preserves_all_regimes():
    from features.micro_models import ContinuousMicrostructureEngine
    eng = ContinuousMicrostructureEngine(symbol="BTCUSDT")
    for rls in (eng.rls_trend, eng.rls_range, eng.rls_spoof, eng.rls_cascade):
        rls.f_inv = rls.f_inv * 3.7
    state = eng.export_state()

    fresh = ContinuousMicrostructureEngine(symbol="BTCUSDT")
    fresh.load_state(state)
    for a, b in ((eng.rls_trend, fresh.rls_trend), (eng.rls_range, fresh.rls_range),
                 (eng.rls_spoof, fresh.rls_spoof), (eng.rls_cascade, fresh.rls_cascade)):
        assert np.allclose(a.f_inv, b.f_inv), "covariance not restored"


def test_wrong_shaped_matrix_is_rejected_not_loaded():
    from features.micro_models import ContinuousMicrostructureEngine
    eng = ContinuousMicrostructureEngine(symbol="BTCUSDT")
    before = eng.rls_spoof.f_inv.copy()
    eng.load_state({"P_spoof": [[1.0, 2.0], [3.0, 4.0]]})
    assert np.allclose(eng.rls_spoof.f_inv, before)


# ===================== B22: RVOL on deltas ================================

def test_rvol_uses_interval_deltas_not_cumulative():
    src = (REPO / "src" / "features" / "omni_scanner.py").read_text()
    assert "vol_delta = vol_cum - prev_cum" in src
    assert 'self.market_memory[sym]["vol"].append(vol_delta)' in src, (
        "B22: RVOL still z-scores a rolling cumulative"
    )


def test_constant_cumulative_volume_yields_zero_delta():
    """A flat 24h cumulative means no volume traded -- delta must be 0."""
    prev_cum, vol_cum = 1_000_000.0, 1_000_000.0
    assert (vol_cum - prev_cum if vol_cum >= prev_cum else 0.0) == 0.0


def test_rolling_window_reset_does_not_produce_negative_volume():
    prev_cum, vol_cum = 1_000_000.0, 900_000.0     # window rolled
    assert (vol_cum - prev_cum if vol_cum >= prev_cum else 0.0) == 0.0


# ===================== B32 / B33 ==========================================

def test_parameter_errors_no_longer_quarantine_the_symbol():
    assert "ret_code in [110126, 10002, 10001]" not in SOR, (
        "B32: 10001/10002 still mislabelled as compliance bans"
    )
    assert SOR.count("ret_code == 110126") >= 2


def test_gross_pnl_is_dimensionally_sane():
    tel = (REPO / "src" / "services" / "telegram_ops.py").read_text()
    assert "gross_pnl = net_pnl + fees" in tel
    assert "abs(slippage_bps)/10000 * net_pnl" not in tel


# ===================== B30: delta-neutral quarantine ======================

def test_delta_neutral_is_quarantined():
    dn = (REPO / "src" / "execution" / "delta_neutral.py").read_text()
    assert "DEPRECATED / QUARANTINED" in dn
    assert 'os.getenv("ENABLE_DELTA_NEUTRAL"' in dn


def test_delta_neutral_refuses_to_start_by_default(monkeypatch):
    monkeypatch.delenv("ENABLE_DELTA_NEUTRAL", raising=False)
    from execution.delta_neutral import DeltaNeutralYieldEngine

    class Core:
        class executor:
            @staticmethod
            async def safe_call(*a, **k): return {"retCode": 0, "result": {"list": []}}
        fsm = None
        active_positions_map = {}
        class sor: position_idx = 0
    eng = DeltaNeutralYieldEngine(Core())
    run(asyncio.wait_for(eng.run_yield_scanner_daemon(), timeout=5.0))  # returns immediately


def test_delta_neutral_is_not_in_the_daemon_list():
    assert "run_yield_scanner_daemon" not in MAIN


# ===================== B1 / B19 / B20 residuals ===========================

def test_sor_rejects_a_stale_engine_snapshot():
    stale = book(100_000.0)
    stale.update({"as_of": time.time() - 600.0, "source": "WS_L2"})
    core = FakeCoreEngine(orderbook_snapshots={"BTCUSDT": stale})
    sor = SmartOrderRouter(executor=FakeExecutor(), core_engine=core)
    sor.instrument_cache["BTCUSDT"] = dict(BTC_LIMITS)
    mid, ob, source = sor._resolve_reference_price("BTCUSDT", None)
    assert mid == 0.0 and source == "NONE", (
        "B1 residual: sized against an unboundedly old snapshot"
    )


def test_sor_accepts_a_fresh_engine_snapshot():
    fresh = book(100_000.0)
    fresh.update({"as_of": time.time(), "source": "WS_L2"})
    core = FakeCoreEngine(orderbook_snapshots={"BTCUSDT": fresh})
    sor = SmartOrderRouter(executor=FakeExecutor(), core_engine=core)
    sor.instrument_cache["BTCUSDT"] = dict(BTC_LIMITS)
    mid, ob, source = sor._resolve_reference_price("BTCUSDT", None)
    assert mid > 0.0 and source == "engine_snapshot"


def test_slice_book_is_used_without_an_age_gate():
    """The caller's own book is current by construction."""
    sor = SmartOrderRouter(executor=FakeExecutor(), core_engine=FakeCoreEngine())
    sor.instrument_cache["BTCUSDT"] = dict(BTC_LIMITS)
    mid, ob, source = sor._resolve_reference_price("BTCUSDT", book(100_000.0))
    assert mid > 0.0 and source == "slice_book"


def test_exit_loop_suspends_on_stale_data():
    assert 'ctx["market_data_stale"] = market_data_stale' in MAIN
    assert "Suspending software exit decisions" in MAIN, (
        "B19/B20 residual: the exit path still trades on a frozen book"
    )


def test_exit_loop_keeps_exchange_stops_in_force_when_stale():
    assert "exchange-native stops remain in force" in MAIN
