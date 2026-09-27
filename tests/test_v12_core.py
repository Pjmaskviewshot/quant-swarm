"""
V12 decision core: forecaster, regime, costs, EV, stops, sizing, learning.

Each test states the property the owner asked for, in his words where possible.
"""
import math

import numpy as np
import pytest

from v12.edge import (CostModel, Z_GRID, liquidity_grade, policy_edge_r, quality_score,
                      simulate_runner_ev, two_barrier_up_probability)
from v12.horizons import Calibrator, MultiHorizonForecaster, OnlineLogit
from v12.learning import ConditionalEdgeModel, conditional_performance, edge_band
from v12.regime import RegimeClassifier
from v12.risk import RiskConfig, size_position, stop_distance


def walk(n, seed, sigma=0.0007, drift=0.0):
    rng = np.random.default_rng(seed)
    return 100 * np.exp(np.cumsum(drift + sigma * rng.standard_normal(n)))


# ---- (2) entry on expected value AFTER costs ------------------------------

def test_driftless_forecast_never_shows_positive_edge():
    for sig, stop in ((0.0005, 0.005), (0.0007, 0.008), (0.0011, 0.012)):
        assert policy_edge_r(0.0, sig, stop, 60).ev_r == pytest.approx(-0.01)


def test_edge_is_monotone_in_forecast_drift():
    evs = [policy_edge_r(z * 0.0007, 0.0007, 0.008, 60).ev_r for z in (0.0, 0.02, 0.05, 0.1, 0.2)]
    assert evs == sorted(evs)


def test_adverse_forecast_is_never_credited():
    assert policy_edge_r(-0.00005, 0.0007, 0.008, 60).ev_r < -0.01


def test_curve_cache_matches_direct_simulation_within_margin():
    """The fast path must not be more optimistic than the direct simulation by
    more than the 0.01R conservative margin."""
    for sig, stop in ((0.0007, 0.008), (0.0011, 0.005)):
        z0 = simulate_runner_ev(0, sig, stop, 0, step_min=1.0).ev_r
        for z in (0.01, 0.045, 0.12):
            direct = simulate_runner_ev(z * sig, sig, stop, 60, step_min=1.0).ev_r - z0 - 0.01
            assert policy_edge_r(z * sig, sig, stop, 60).ev_r <= direct + 0.01


def test_strong_forecast_is_clamped_not_extrapolated():
    top = policy_edge_r(float(Z_GRID[-1]) * 0.0007, 0.0007, 0.008, 60).ev_r
    assert policy_edge_r(5.0 * 0.0007, 0.0007, 0.008, 60).ev_r == pytest.approx(top)


def test_cost_model_matches_live_receipts():
    """Live Sept-2026 receipts: ~11 bps fees + ~3.4 bps slippage + spread."""
    b = CostModel().breakdown_bps(spread_bps=1.0, sigma_1m=0.0012, hold_min=60, funding_rate_8h=0.0001,
                                  is_buy=True)
    assert b["fees"] == pytest.approx(11.0)
    assert 13 <= b["total"] <= 20
    short = CostModel().breakdown_bps(1.0, 0.0012, 60, 0.0001, is_buy=False)
    assert short["funding"] < b["funding"]           # longs pay positive funding


def test_two_barrier_probability_limits():
    assert two_barrier_up_probability(0.0, 0.01, 0.01, 0.01) == pytest.approx(0.5, abs=1e-6)
    assert two_barrier_up_probability(0.0, 0.01, 0.01, 0.03) == pytest.approx(0.75, abs=1e-6)
    assert two_barrier_up_probability(0.001, 0.01, 0.01, 0.01) > 0.5


# ---- (3) volatility-aware stop with hard bounds -------------------------

def test_stop_scales_with_volatility_within_bounds():
    cfg = RiskConfig()
    lo = stop_distance(0.0003, 60, 0.0015, cfg)
    hi = stop_distance(0.0015, 60, 0.0015, cfg)
    assert lo < hi
    assert stop_distance(1e-6, 60, 0.0, cfg) == cfg.min_stop_pct
    assert stop_distance(0.05, 60, 0.0, cfg) == cfg.max_stop_pct


def test_stop_is_never_inside_the_cost_band():
    cfg = RiskConfig()
    assert stop_distance(0.0001, 1, 0.0017, cfg) >= cfg.min_stop_cost_multiple * 0.0017


# ---- (4) size by edge AND volatility, hard cap -------------------------

def test_size_fails_closed_on_unknown_equity():
    for eq in (None, 0.0, float("nan"), -5.0):
        assert not size_position(eq, 0.008, 20, 0.0007, RiskConfig()).tradeable


def test_size_never_rounds_up_to_minimum():
    d = size_position(40.0, 0.03, 3.0, 0.002, RiskConfig(), min_notional=5.0)
    assert d.notional == 0 and "never rounded up" in d.reasons[0]


def test_size_hard_caps_risk_and_notional():
    cfg = RiskConfig()
    d = size_position(1000.0, 0.004, 500.0, 0.0001, cfg)
    assert d.risk_pct <= cfg.max_risk_pct + 1e-12
    assert d.notional <= 1000.0 * cfg.max_notional_pct + 1e-9


def test_size_responds_to_measured_edge_not_confidence():
    cfg = RiskConfig()
    model_only = size_position(1000.0, 0.01, 60.0, 0.0007, cfg)
    measured_bad = size_position(1000.0, 0.01, 60.0, 0.0007, cfg, measured_edge_bps=-3.0, measured_n=50)
    assert model_only.tradeable and not measured_bad.tradeable


def test_higher_volatility_means_smaller_size():
    cfg = RiskConfig()
    calm = size_position(1000.0, 0.03, 6.0, 0.0006, cfg)
    wild = size_position(1000.0, 0.03, 6.0, 0.0016, cfg)
    assert wild.risk_pct < calm.risk_pct


# ---- (1) multi-horizon forecaster ---------------------------------------

def test_calibrator_shrinks_to_half_with_no_data():
    assert Calibrator().calibrated(0.9) == pytest.approx(0.5, abs=0.05)


def test_online_logit_learns_a_real_signal():
    rng = np.random.default_rng(0)
    m = OnlineLogit(2)
    for _ in range(4000):
        x = rng.standard_normal(2)
        y = float(rng.random() < 1 / (1 + math.exp(-2 * x[0])))
        m.update(x, y)
    assert m.predict(np.array([1.5, 0.0])) > 0.7 and m.predict(np.array([-1.5, 0.0])) < 0.3


def test_forecaster_on_noise_stays_near_half_and_has_no_skill():
    fc = MultiHorizonForecaster()
    for i, p in enumerate(walk(30000, 1, sigma=0.0003)):
        fc.observe(1_700_000_000 + 10 * i, float(p))
    for f in fc.forecast().values():
        assert abs(f.p_cal - 0.5) < 0.1
        assert f.brier_skill < 0.05


def test_overlapping_labels_do_not_inflate_sample_size():
    """A 4h label sampled every 10s overlaps itself 1440 times; it must count as
    ~1 independent outcome per 4h, not 1440."""
    fc = MultiHorizonForecaster()
    for i, p in enumerate(walk(8640, 2, sigma=0.0003)):          # 24h of 10s ticks
        fc.observe(1_700_000_000 + 10 * i, float(p))
    f = fc.forecast()
    assert f[14400].resolved <= 6
    assert f[60].resolved > 100


def test_forecaster_ignores_bad_prices():
    fc = MultiHorizonForecaster()
    for bad in (float("nan"), 0.0, -1.0, float("inf")):
        fc.observe(1_700_000_000, bad)
    assert all(f.resolved == 0 for f in fc.forecast().values())


# ---- (5) regime classifier ------------------------------------------------

def feed(rc, prices):
    for i, p in enumerate(prices):
        rc.update(1_700_000_000 + 60 * i, float(p))
    return rc.read()


def test_regime_warms_up_before_labelling():
    assert feed(RegimeClassifier(), walk(50, 3)).structure == "WARMING_UP"


def test_regime_noise_is_mostly_range():
    labels = []
    rc = RegimeClassifier()
    for i, p in enumerate(walk(3000, 4)):
        rc.update(1_700_000_000 + 60 * i, float(p))
        if i > 300 and i % 10 == 0:
            labels.append(rc.read().structure)
    trend = sum(l.startswith("TREND") for l in labels) / len(labels)
    assert trend < 0.1


def test_regime_detects_a_clean_trend():
    r = feed(RegimeClassifier(), walk(600, 5, sigma=0.0005, drift=0.0006))
    assert r.structure in ("TREND_UP", "BREAKOUT_UP")


def test_regime_one_update_per_minute():
    rc = RegimeClassifier()
    assert rc.update(1_700_000_000, 100.0)
    assert not rc.update(1_700_000_030, 101.0)


# ---- (7) learning loop -------------------------------------------------

def test_unmeasured_edge_starts_pessimistic():
    m = ConditionalEdgeModel()
    est, n = m.measured("TREND_UP", 20.0)
    assert est < 0 and n == 0


def test_three_lucky_trades_do_not_make_a_regime_profitable():
    m = ConditionalEdgeModel()
    for _ in range(100):
        m.record("RANGE", 10.0, -12.0)
    for _ in range(3):
        m.record("NEWREGIME", 10.0, +60.0)
    est, n = m.measured("NEWREGIME", 10.0)
    assert n == 3 and est < 20


def test_conditional_performance_needs_multiple_testing_correction():
    """Scanning 20 noise-only conditions: the family-wise false-positive rate
    should be ~5%, not the ~64% an uncorrected t >= 2 scan produces."""
    hits = 0
    for seed in range(40):
        rng = np.random.default_rng(seed)
        trades = [{"regime": f"R{i % 20}", "net_return_bps": float(rng.normal(0, 40))} for i in range(2000)]
        hits += any(r.significant for r in conditional_performance(trades, ["regime"]))
    assert hits <= 6


def test_edge_band_labels():
    assert edge_band(-5) != edge_band(40)


def test_quality_score_bounded_and_liquidity_graded():
    q = quality_score(100, 50, 15, "GOOD", True, 30.0)
    assert 0 <= q <= 100
    assert liquidity_grade(0.5) == "GOOD" and liquidity_grade(20) != "GOOD"
