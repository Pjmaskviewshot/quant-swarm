"""B17/B27/B28/B29 — backtest integrity.

B17: whitener state was not carried train->test, so frozen weights were applied
     to differently-scaled features. Every OOS Sharpe was invalid, including the
     one that selected params.json (which the LIVE engine reads).
B27: np.mean([]) is nan and nan is truthy, so `or 0.0` did not guard it.
B28: Sharpe/Sortino annualised with 252 while Calmar used 365.
B29: the Monte Carlo resampled the same profitable trades and reported
     P(sum>0) — ~1 by construction.
"""
import numpy as np
import pytest

from backtest import (
    summarize, _regime_stats, _robustness_analysis, ANNUALISATION_DAYS,
    BacktestAdaptiveWhitener,
)


def trades(nets, regime="TRENDING"):
    return [{"i": i, "direction": "BUY", "regime": regime, "outcome": "WIN",
             "net": n, "bars": 10} for i, n in enumerate(nets)]


# --- B28: one annualisation convention ------------------------------------

def test_annualisation_is_365_for_crypto():
    assert ANNUALISATION_DAYS == 365.0


def test_summary_reports_its_convention():
    out = summarize(trades([0.01, -0.005, 0.02]), total_minutes=1440)
    assert out["annualisation_days"] == 365.0


def test_sharpe_and_calmar_share_the_same_clock():
    """Both must scale from the same periods-per-year basis."""
    nets = [0.01, -0.004, 0.015, -0.003, 0.02]
    out = summarize(trades(nets), total_minutes=1440 * 5)
    mean, std = float(np.mean(nets)), float(np.std(nets)) + 1e-9
    tpd = len(nets) / 5.0
    expected = (mean / std) * np.sqrt(ANNUALISATION_DAYS * tpd)
    assert out["sharpe_ratio"] == pytest.approx(expected, rel=1e-6)


# --- B27: no nan in metrics -----------------------------------------------

def test_empty_regime_reports_zero_not_nan():
    stats = _regime_stats(trades([0.01]), "RANGING")
    assert stats["trades"] == 0
    assert stats["win_rate"] == 0.0
    assert not np.isnan(stats["win_rate"])


def test_no_nan_anywhere_in_summary():
    out = summarize(trades([0.01, -0.02, 0.03]), total_minutes=1440)
    for regime, stats in out["by_regime"].items():
        for k, v in stats.items():
            assert not (isinstance(v, float) and np.isnan(v)), f"nan in by_regime[{regime}][{k}]"


def test_populated_regime_win_rate_is_correct():
    stats = _regime_stats(trades([0.01, -0.01, 0.02, 0.03]), "TRENDING")
    assert stats["trades"] == 4
    assert stats["win_rate"] == pytest.approx(0.75)


# --- B29: robustness replaces the circular bootstrap -----------------------

def test_monte_carlo_p_positive_is_gone():
    out = summarize(trades([0.01, 0.02, 0.03]), total_minutes=1440)
    assert "monte_carlo_p_positive" not in out, (
        "B29: the circular P(sum>0) statistic must not be reported"
    )


def test_robustness_reports_sequence_risk():
    out = summarize(trades([0.05, -0.02, 0.03, -0.04, 0.06, -0.01]), total_minutes=1440)
    seq = out["robustness"]["sequence_risk"]
    assert "permuted_p95_max_drawdown" in seq
    assert seq["permuted_p95_max_drawdown"] >= seq["permuted_median_max_drawdown"]


def test_robustness_reports_cost_sensitivity():
    cost = _robustness_analysis(np.array([0.01, 0.02, -0.005]))["cost_sensitivity"]
    assert "breakeven_extra_cost_per_trade" in cost
    assert cost["breakeven_extra_cost_per_trade"] > 0


def test_robustness_is_deterministic():
    nets = np.array([0.01, -0.02, 0.03, -0.01, 0.04])
    a = _robustness_analysis(nets, iterations=200)
    b = _robustness_analysis(nets, iterations=200)
    assert a["sequence_risk"] == b["sequence_risk"], "reruns must be comparable"


def test_robustness_carries_its_own_caveat():
    note = _robustness_analysis(np.array([0.01, 0.02]))["note"]
    assert "NOT evidence that the edge is real" in note


def test_robustness_handles_no_trades():
    assert _robustness_analysis(np.array([]))["note"] == "no trades"


# --- B17: whitener state travels with the weights -------------------------

def test_whitener_state_is_exported_in_final_state():
    import inspect, backtest
    src = inspect.getsource(backtest.run_v40_backtest)
    for key in ("whitener_mean", "whitener_cov", "whitener_zca"):
        assert key in src, f"B17: {key} missing from the transferred state"


def test_whitener_state_is_restored_from_initial_state():
    import inspect, backtest
    src = inspect.getsource(backtest.run_v40_backtest)
    assert 'if "whitener_mean" in initial_rls_state:' in src
    assert "whitening_engine.cached_zca_matrix = initial_rls_state" in src


def test_whitener_transform_differs_before_and_after_fitting():
    """Establishes that dropping the state really would change the transform."""
    fresh = BacktestAdaptiveWhitener(dim=19)
    fitted = BacktestAdaptiveWhitener(dim=19)
    rng = np.random.default_rng(0)
    for _ in range(80):
        fitted.orthogonalize(rng.normal(5.0, 3.0, 19), 1e-5)

    probe = rng.normal(5.0, 3.0, 19)
    out_fresh = fresh.orthogonalize(probe.copy(), 1e-5).copy()
    out_fitted = fitted.orthogonalize(probe.copy(), 1e-5).copy()
    assert not np.allclose(out_fresh, out_fitted, atol=1e-6), (
        "B17: if these matched, resetting the whitener would be harmless -- "
        "they must differ for the finding to be real"
    )
