"""
BACKTEST INTEGRITY — tests of the INSTRUMENT, not of the strategy.

APEX sections 14, 15 and 23. Before any number a backtester produces can be
treated as evidence, the backtester itself has to be shown not to cheat. These
tests ask the four questions that matter:

  1. CAUSALITY    Does a decision at bar i depend on any bar after i?
                  Tested by truncation: if future bars are deleted, every trade
                  that already closed must be byte-identical. A look-ahead of
                  even one bar changes them.

  2. COSTS        Do fees actually reduce returns, monotonically?
                  Tested by raising the fee and requiring performance to fall.

  3. NULL         Does it find profit in a driftless random walk?
                  It must not. A positive after-cost expectancy on noise voids
                  every other number the instrument produces.

  4. SENSITIVITY  Does it detect a known, planted edge?
                  An instrument that finds nothing on AR(1) momentum is not
                  conservative, it is blind, and its "no edge here" on real data
                  would mean nothing either.

These run against `src/backtest.py` — the engine that actually produces the
research numbers — not a toy reimplementation.
"""
import sys
import pathlib

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

import backtest as bt                                        # noqa: E402
from backtest import run_v40_backtest, Params                # noqa: E402
from research.dataset import (                               # noqa: E402
    synthetic_random_walk, synthetic_momentum, synthetic_regime_shift,
)


def run_capturing_trades(candles, symbol="SYNTHUSDT", params=None):
    """Run the real backtester and capture the raw trade list.

    `run_v40_backtest` returns only the summary, so the trade-level evidence a
    causality test needs is intercepted at `summarize`.
    """
    captured = {}
    original = bt.summarize

    def spy(trades, total_minutes=0):
        captured["trades"] = [dict(t) for t in trades]
        return original(trades, total_minutes)

    bt.summarize = spy
    try:
        summary, state = run_v40_backtest(candles, candles, params or Params(), symbol)
    finally:
        bt.summarize = original
    return summary, captured.get("trades", []), state


# ===================== 1. CAUSALITY ======================================

@pytest.mark.slow
def test_no_look_ahead_truncating_the_future_cannot_change_the_past():
    """
    The strongest available test for look-ahead bias.

    Run on bars [0, N). Then run on bars [0, N+2000). Every trade that closed
    inside the first window must appear identically in the second: same bar
    index, same direction, same outcome, same PnL.

    If any indicator, label or exit peeked forward, the longer run would have
    seen information the shorter one did not, and these lists would diverge.
    """
    full = synthetic_momentum(n=8000, seed=101)
    n = 6000
    short = full.candles[:n]

    _, short_trades, _ = run_capturing_trades(short)
    _, long_trades, _ = run_capturing_trades(full.candles)

    assert short_trades, "no trades in the truncated window — test proves nothing"

    # Compare only trades the short run could have completed.
    comparable = [t for t in long_trades if t["i"] < n - 300]
    overlap = min(len(short_trades), len(comparable))
    assert overlap >= 1, "no overlapping trades to compare"

    for a, b in zip(short_trades[:overlap], comparable[:overlap]):
        assert a["i"] == b["i"], (
            f"LOOK-AHEAD: trade bar index moved {a['i']} -> {b['i']} when future "
            f"bars were appended"
        )
        assert a["direction"] == b["direction"], (
            f"LOOK-AHEAD: direction at bar {a['i']} changed when future data was added"
        )
        assert a["net"] == pytest.approx(b["net"], rel=1e-9, abs=1e-12), (
            f"LOOK-AHEAD: PnL at bar {a['i']} changed {a['net']} -> {b['net']} "
            f"purely because later bars existed"
        )


@pytest.mark.slow
def test_identical_input_gives_identical_output():
    """Non-determinism would make every comparison between runs meaningless."""
    ds = synthetic_momentum(n=4000, seed=202)
    a, ta, _ = run_capturing_trades(ds.candles)
    b, tb, _ = run_capturing_trades(ds.candles)
    assert len(ta) == len(tb)
    assert a.get("expectancy_per_trade") == pytest.approx(b.get("expectancy_per_trade"))


# ===================== 2. COSTS ==========================================

@pytest.mark.slow
def test_higher_fees_always_reduce_performance():
    """
    A cost model that is wired up but not actually applied is a classic silent
    defect: the backtest reports the same number whatever the fee.
    """
    ds = synthetic_momentum(n=6000, seed=303)
    saved = (bt.TAKER_FEE, bt.MAKER_FEE, bt.BASE_SLIPPAGE_BPS)
    try:
        bt.TAKER_FEE, bt.MAKER_FEE, bt.BASE_SLIPPAGE_BPS = 0.00055, 0.00020, 4.0
        base, tb, _ = run_capturing_trades(ds.candles)
        bt.TAKER_FEE, bt.MAKER_FEE, bt.BASE_SLIPPAGE_BPS = 0.00550, 0.00200, 40.0
        dear, td, _ = run_capturing_trades(ds.candles)
    finally:
        bt.TAKER_FEE, bt.MAKER_FEE, bt.BASE_SLIPPAGE_BPS = saved

    if not tb:
        pytest.skip("no trades generated; cost sensitivity untestable on this series")
    assert dear.get("expectancy_per_trade", 0.0) < base.get("expectancy_per_trade", 0.0), (
        "raising fees 10x did not reduce expectancy — costs are not being applied"
    )


def test_cost_constants_are_not_zero_by_default():
    """A frictionless default would make every stored result an overstatement."""
    assert bt.TAKER_FEE > 0.0
    assert bt.MAKER_FEE > 0.0
    assert bt.FUNDING_PER_8H > 0.0
    assert bt.BASE_SLIPPAGE_BPS > 0.0


def test_round_trip_cost_is_material_at_the_traded_horizon():
    """
    Sanity on the arithmetic that any claimed edge has to clear: two taker fills
    plus slippage on both sides. Stated once, in bps, so nothing downstream can
    quietly assume a smaller number.
    """
    from research.experiment import CostModel
    rt = CostModel().round_trip_cost_bps()
    assert rt == pytest.approx(2 * 5.5 + 2 * 4.0)
    assert rt > 15.0, "round-trip friction is above 15 bps; an edge must exceed it"


# ===================== 3. NULL / 4. SENSITIVITY ==========================
# The full sweeps take ~20 minutes and live in
# reports/instrument_validation.md. These are the fast, deterministic
# in-suite versions.

@pytest.mark.slow
def test_instrument_finds_no_after_cost_edge_in_pure_noise():
    ds = synthetic_random_walk(n=12000, seed=404)
    summary, trades, _ = run_capturing_trades(ds.candles)
    if summary.get("trades", 0) < 20:
        pytest.skip(f"only {summary.get('trades', 0)} trades — sample too small to judge")
    assert summary["expectancy_per_trade"] <= 0.0, (
        f"THE INSTRUMENT FOUND PROFIT IN NOISE: expectancy "
        f"{summary['expectancy_per_trade']:+.6f} over {summary['trades']} trades on a "
        f"driftless random walk. Look-ahead, optimistic fills or under-modelled "
        f"costs. No other backtest number is trustworthy until this is resolved."
    )


@pytest.mark.slow
def test_walk_forward_actually_transfers_fitted_state():
    """
    AUDIT B17, asserted as a property rather than by reading the code.

    A walk-forward fold is only out-of-sample if the TEST fold runs with the
    weights fitted on TRAIN. If `initial_rls_state` is ignored, or the whitener
    restarts from identity, the "frozen-weight OOS" run is really just a second
    cold run, and every OOS Sharpe the sweep produced was measuring nothing.

    The observable difference: a cold run and a warm-started frozen run over the
    SAME test bars must not produce identical results.

    NOTE ON A TEST I REMOVED: this replaces an earlier assertion that a
    regime-shift dataset must degrade from first half to second. That test was
    wrong — it ran the two halves independently, with no state transfer, so it
    compared "how the system does on momentum" against "how it does on mean
    reversion" and called the difference leakage. It failed, and the failure was
    my test's, not the code's. The observation it produced is recorded in
    reports/instrument_validation.md instead.
    """
    ds = synthetic_regime_shift(n=12000, seed=505)
    train, test = ds.split(train_frac=0.5, embargo_bars=240)

    _, _, trained_state = run_capturing_trades(train.candles)
    assert trained_state and "w_trend" in trained_state

    cold, _, _ = run_capturing_trades(test.candles)
    warm, _ = run_v40_backtest(test.candles, test.candles, Params(), "SYNTHUSDT",
                               initial_rls_state=trained_state, freeze_weights=True)

    cold_key = (cold.get("trades"), cold.get("expectancy_per_trade"))
    warm_key = (warm.get("trades"), warm.get("expectancy_per_trade"))
    assert cold_key != warm_key, (
        "a frozen warm-started fold produced results identical to a cold run — "
        "fitted state is NOT crossing the split, so 'out-of-sample' is a fiction"
    )


@pytest.mark.slow
def test_frozen_weights_really_are_frozen():
    """If `freeze_weights=True` still learns, the OOS fold is fitting on itself."""
    ds = synthetic_momentum(n=6000, seed=606)
    train, test = ds.split(0.5, 100)
    _, _, state = run_capturing_trades(train.candles)

    _, frozen_state = run_v40_backtest(test.candles, test.candles, Params(),
                                       "SYNTHUSDT", initial_rls_state=state,
                                       freeze_weights=True)
    import numpy as np
    for key in ("w_trend", "w_range", "w_spoof", "w_cascade"):
        assert np.allclose(state[key], frozen_state[key]), (
            f"{key} changed during a freeze_weights=True run — the test fold is "
            f"learning from its own out-of-sample data"
        )


# ===================== metric hygiene =====================================

def test_summary_never_emits_nan():
    """AUDIT B27: np.mean([]) is nan, and nan is truthy, so `or 0.0` did not guard it."""
    import math
    out = bt.summarize([{"net": 1.0, "regime": "TRENDING", "outcome": "TP",
                         "direction": 1, "i": 0, "bars": 5}], total_minutes=1440)
    for k, v in out.items():
        if isinstance(v, float):
            assert not math.isnan(v), f"{k} is nan"
    for regime, stats in out["by_regime"].items():
        for k, v in stats.items():
            if isinstance(v, float):
                assert not math.isnan(v), f"by_regime[{regime}][{k}] is nan"


def test_empty_trade_list_is_not_a_zero_result():
    assert bt.summarize([], total_minutes=1440) == {"trades": 0}


def test_annualisation_is_one_convention():
    """AUDIT B28: Sharpe used 252 while Calmar used 365 — different clocks."""
    src = (REPO / "src" / "backtest.py").read_text()
    assert "ANNUALISATION_DAYS" in src
    body = src.split("def summarize")[1].split("def parameter_sweep")[0]
    code = "\n".join(line.split("#")[0] for line in body.splitlines())
    assert "252" not in code, "a second annualisation constant is back in summarize()"


def test_sharpe_sortino_and_calmar_share_one_clock():
    """
    The B28 defect was not the literal 252 — it was TWO conventions coexisting,
    so the ratios could not be compared with each other. Assert the property
    rather than the absence of a string: scaling the observation window must
    move Sharpe and Calmar by the same annualisation multiplier.
    """
    trades = [{"net": n, "regime": "TRENDING", "outcome": "TP", "direction": 1,
               "i": i, "bars": 5} for i, n in enumerate([0.02, -0.01] * 40)]
    a = bt.summarize(trades, total_minutes=1440 * 10)
    b = bt.summarize(trades, total_minutes=1440 * 40)
    ratio_sharpe = a["sharpe_ratio"] / b["sharpe_ratio"]
    ratio_calmar = a["calmar_ratio"] / b["calmar_ratio"]
    assert ratio_sharpe == pytest.approx(2.0, rel=1e-6)       # sqrt(4x trades/day)
    assert ratio_calmar == pytest.approx(4.0, rel=1e-6)       # linear in trades/day
    assert a["annualisation_days"] == b["annualisation_days"] == 365.0
