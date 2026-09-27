"""
RESEARCH INFRASTRUCTURE — dataset provenance, experiment records, validation gates.

APEX section 19. The point of this layer is that a claimed improvement can be
re-derived by someone else, and that the ways a research loop normally flatters
itself are blocked mechanically rather than by discipline.

Each test below names the specific dishonesty it prevents.
"""
import json
import math

import pytest

from research.dataset import (
    Dataset, synthetic_random_walk, synthetic_momentum, synthetic_mean_reverting,
    synthetic_regime_shift, load_dataset, save_to_cache, load_from_cache,
    available_datasets,
)
from research.experiment import (
    CostModel, ZERO_COST, ExperimentRecord, build_record, run_experiment,
    new_experiment_id, compare, load_all,
)
from research import validate as V


# ===================== datasets ==========================================

def test_same_bytes_same_hash_different_bytes_different_hash():
    """Provenance: a result is pinned to the exact candles that produced it."""
    a = synthetic_momentum(n=500, seed=1)
    b = synthetic_momentum(n=500, seed=1)
    c = synthetic_momentum(n=500, seed=2)
    assert a.content_hash() == b.content_hash()
    assert a.content_hash() != c.content_hash()


def test_a_moved_window_cannot_masquerade_as_the_same_dataset():
    """
    The original backtester re-fetched on every run, so 'the same test' silently
    used different data each day. Hashing makes that impossible to miss.
    """
    full = synthetic_momentum(n=1000, seed=3)
    shifted = Dataset(full.name, full.symbol, full.interval, full.candles[10:],
                      full.source, full.meta)
    assert full.content_hash() != shifted.content_hash()


def test_generators_are_deterministic_across_calls():
    assert (synthetic_random_walk(n=200, seed=9).candles
            == synthetic_random_walk(n=200, seed=9).candles)


@pytest.mark.parametrize("gen,expected", [
    (synthetic_random_walk, "NONE"),
    (synthetic_momentum, "POSITIVE_MOMENTUM"),
    (synthetic_mean_reverting, "MEAN_REVERSION"),
    (synthetic_regime_shift, "REGIME_DEPENDENT"),
])
def test_every_generator_declares_its_ground_truth(gen, expected):
    """A synthetic series with an undeclared expectation proves nothing."""
    assert gen(n=300).meta["expected_edge"] == expected


def test_ohlc_bars_are_internally_consistent():
    for c in synthetic_momentum(n=800, seed=5).candles:
        assert c["low"] <= c["open"] <= c["high"]
        assert c["low"] <= c["close"] <= c["high"]
        assert c["low"] > 0 and c["volume"] > 0


def test_timestamps_are_strictly_increasing():
    ds = synthetic_regime_shift(n=1000, seed=6)
    ts = [c["ts"] for c in ds.candles]
    assert all(b > a for a, b in zip(ts, ts[1:])), "out-of-order bars would corrupt any backtest"


def test_split_inserts_a_real_embargo_gap():
    """
    Without the gap, a trade opened near the end of train can still be open at
    the start of test, so the two sets overlap in outcome space and the
    'out-of-sample' number is contaminated.
    """
    ds = synthetic_momentum(n=2000, seed=7)
    train, test = ds.split(train_frac=0.6, embargo_bars=240)
    assert train.end_ts < test.start_ts
    gap_bars = (test.start_ts - train.end_ts) // 60_000
    assert gap_bars >= 240
    assert len(train) + len(test) + 240 <= len(ds) + 1


def test_split_sets_do_not_share_a_single_bar():
    ds = synthetic_momentum(n=1500, seed=8)
    train, test = ds.split(0.6, 100)
    assert set(c["ts"] for c in train.candles).isdisjoint(c["ts"] for c in test.candles)


def test_momentum_and_mean_reversion_really_differ_in_autocorrelation():
    """The planted edges must actually be present, or the sensitivity test is a lie."""
    import numpy as np

    def ac1(ds):
        p = np.array([c["close"] for c in ds.candles])
        r = np.diff(np.log(p))
        return float(np.corrcoef(r[:-1], r[1:])[0, 1])

    assert ac1(synthetic_momentum(n=20000, seed=11)) > 0.15
    assert ac1(synthetic_mean_reverting(n=20000, seed=13)) < -0.15
    assert abs(ac1(synthetic_random_walk(n=20000, seed=7))) < 0.05


def test_cache_round_trip(tmp_path):
    ds = synthetic_momentum(n=100, seed=2)
    save_to_cache("BTCUSDT", "1", ds.candles, tmp_path)
    back = load_from_cache("BTCUSDT", "1", tmp_path)
    assert back.candles == ds.candles
    assert back.source == "cache"
    assert available_datasets(tmp_path) == ["BTCUSDT_1"]


def test_missing_cache_explains_how_to_get_the_data(tmp_path):
    with pytest.raises(FileNotFoundError) as e:
        load_from_cache("ETHUSDT", "1", tmp_path)
    assert "fetch_klines" in str(e.value)


def test_dataset_spec_resolution():
    assert load_dataset("synthetic:momentum:42").meta["seed"] == 42
    with pytest.raises(ValueError):
        load_dataset("synthetic:wishful_thinking")


# ===================== cost model ========================================

def test_default_cost_model_is_the_real_fee_schedule_not_free():
    """A silently frictionless default would overstate every stored result."""
    cm = CostModel()
    assert cm.taker_fee > 0 and cm.maker_fee > 0
    assert cm.funding_per_8h > 0 and cm.base_slippage_bps > 0
    assert cm.fees_applied and cm.funding_applied and cm.slippage_applied
    assert cm.costs_disabled is False if hasattr(cm, "costs_disabled") else True


def test_round_trip_cost_counts_both_sides():
    assert CostModel().round_trip_cost_bps() == pytest.approx(2 * 5.5 + 2 * 4.0)
    assert ZERO_COST.round_trip_cost_bps() == 0.0


def test_a_disabled_cost_shows_up_in_the_description():
    assert "DISABLED" in ZERO_COST.describe()
    assert "DISABLED" not in CostModel().describe()


# ===================== experiment records ================================

def _rec(tmp_path, results=None, cost=None, name="t"):
    ds = synthetic_momentum(n=200, seed=1)
    r = build_record(name, "hyp", ds, {"rr_ratio": 2.0}, cost or CostModel())
    r.results = results or {}
    return r


def test_record_captures_full_provenance(tmp_path):
    r = _rec(tmp_path)
    for field in ("commit", "branch", "dirty", "python"):
        assert field in r.revision
    assert r.dataset_hash and r.symbol == "SYNTHUSDT"
    assert r.cost_model["taker_fee"] > 0


def test_experiment_ids_are_unique():
    assert len({new_experiment_id("x") for _ in range(50)}) == 50


def test_record_round_trips_through_disk(tmp_path):
    r = _rec(tmp_path, results={"trades": 120, "sharpe_ratio": 1.2})
    p = r.save(tmp_path)
    back = ExperimentRecord.load(p)
    assert back.experiment_id == r.experiment_id
    assert back.results == r.results
    assert back.dataset_hash == r.dataset_hash


def test_a_zero_cost_record_is_flagged_as_not_profitability(tmp_path):
    r = _rec(tmp_path, cost=ZERO_COST)
    assert r.costs_disabled is True
    assert _rec(tmp_path).costs_disabled is False


def test_a_failing_experiment_is_recorded_not_discarded(tmp_path):
    """Survivorship bias in the experiment LOG is as bad as in the backtest."""
    def boom(ds, params, cm):
        raise RuntimeError("model diverged")

    r = run_experiment("bad", "will fail", synthetic_momentum(n=100), {},
                       boom, results_dir=tmp_path)
    assert r.error and "model diverged" in r.error
    assert (tmp_path / f"{r.experiment_id}.json").exists()
    assert load_all(tmp_path)[0].error == r.error


def test_run_experiment_without_an_explicit_cost_model_still_charges_fees(tmp_path):
    seen = {}

    def runner(ds, params, cm):
        seen["cm"] = cm
        return {"trades": 10}

    run_experiment("x", "h", synthetic_momentum(n=100), {}, runner, results_dir=tmp_path)
    assert seen["cm"].fees_applied is True
    assert seen["cm"].taker_fee > 0, "omitting the cost model must not mean free trading"


def test_secrets_never_reach_a_record(tmp_path, monkeypatch):
    """APEX: no record may carry credentials."""
    monkeypatch.setenv("BYBIT_API_KEY", "SECRET-KEY-12345")
    monkeypatch.setenv("BYBIT_API_SECRET", "SECRET-SECRET-67890")
    r = _rec(tmp_path, results={"trades": 5})
    blob = json.dumps(r.to_dict())
    assert "SECRET-KEY-12345" not in blob
    assert "SECRET-SECRET-67890" not in blob


# ===================== comparison guards =================================

def test_comparing_across_different_data_is_blocked(tmp_path):
    """The cherry-picked-window manoeuvre."""
    a = build_record("a", "", synthetic_momentum(n=200, seed=1), {}, CostModel())
    b = build_record("b", "", synthetic_momentum(n=200, seed=2), {}, CostModel())
    a.results = {"trades": 200, "expectancy_per_trade": 0.001}
    b.results = {"trades": 200, "expectancy_per_trade": 0.010}
    out = compare(a, b)
    assert out["comparable"] is False
    assert out["verdict"] == "NOT COMPARABLE"
    assert any("different data" in x for x in out["blockers"])


def test_comparing_across_different_cost_models_is_blocked(tmp_path):
    """The measured-more-kindly manoeuvre."""
    ds = synthetic_momentum(n=200, seed=1)
    a = build_record("a", "", ds, {}, CostModel())
    b = build_record("b", "", ds, {}, ZERO_COST)
    a.results = b.results = {"trades": 200, "expectancy_per_trade": 0.001}
    assert any("cost model" in x for x in compare(a, b)["blockers"])


def test_a_candidate_with_too_few_trades_is_blocked():
    ds = synthetic_momentum(n=200, seed=1)
    a = build_record("a", "", ds, {}, CostModel())
    b = build_record("b", "", ds, {}, CostModel())
    a.results = {"trades": 500, "expectancy_per_trade": 0.001}
    b.results = {"trades": 12, "expectancy_per_trade": 0.900}
    out = compare(a, b)
    assert out["comparable"] is False
    assert any("too few" in x for x in out["blockers"])


def test_a_genuine_like_for_like_improvement_is_allowed():
    ds = synthetic_momentum(n=200, seed=1)
    a = build_record("a", "", ds, {}, CostModel())
    b = build_record("b", "", ds, {}, CostModel())
    a.results = {"trades": 400, "expectancy_per_trade": 0.0010}
    b.results = {"trades": 380, "expectancy_per_trade": 0.0015}
    out = compare(a, b)
    assert out["comparable"] is True and out["verdict"] == "IMPROVED"
    assert out["deltas"]["expectancy_per_trade"] == pytest.approx(0.0005)


# ===================== validation gates ==================================

def test_small_samples_are_rejected():
    assert V.gate_sample_size(19).passed is False
    assert V.gate_sample_size(500).passed is True


def test_profit_in_noise_fails_the_null_gate():
    g = V.gate_null_rejection(0.0012, 300)
    assert g.passed is False
    assert "PROFIT IN NOISE" in g.detail.upper()


def test_no_edge_in_noise_passes_the_null_gate():
    assert V.gate_null_rejection(-0.0008, 300).passed is True


def test_an_edge_that_merely_clears_costs_is_not_enough():
    rt = CostModel().round_trip_cost_bps()          # 19 bps
    assert V.gate_cost_survival(rt * 1.1, rt).passed is False
    assert V.gate_cost_survival(rt * 2.5, rt).passed is True


def test_out_of_sample_sign_flip_fails():
    assert V.gate_oos_decay(0.0020, -0.0005).passed is False


def test_out_of_sample_far_exceeding_in_sample_fails_as_leakage():
    g = V.gate_oos_decay(0.0010, 0.0100)
    assert g.passed is False
    assert "leakage" in g.detail


def test_healthy_degradation_passes():
    assert V.gate_oos_decay(0.0020, 0.0012).passed is True


def test_negative_in_sample_cannot_be_validated():
    assert V.gate_oos_decay(-0.001, 0.002).passed is False


def test_deflated_sharpe_punishes_a_wide_search():
    """Reporting the best of 500 configurations without this is selection bias."""
    one = V.deflated_sharpe(2.0, n_trials=1, n_obs=200)
    many = V.deflated_sharpe(2.0, n_trials=500, n_obs=200)
    wider = V.deflated_sharpe(2.0, n_trials=5000, n_obs=200)
    assert one == 2.0
    assert wider < many < one
    assert V.deflated_sharpe(2.0, 500, 2000) > many, "more evidence, smaller haircut"


def test_the_haircut_is_expressed_in_the_units_of_the_reported_sharpe():
    """
    The subtle way this gate becomes decorative: `summarize()` reports an
    ANNUALISED Sharpe, so a per-trade haircut subtracted from it is ~sqrt(ppy)
    times too small and nothing ever fails.
    """
    per_trade = V.deflated_sharpe(8.0, 500, 200, annualisation_factor=1.0)
    annualised = V.deflated_sharpe(8.0, 500, 200, annualisation_factor=40.0)
    assert per_trade > 7.0, "per-trade haircut is small, as expected"
    assert annualised < 0.0, "annualised haircut must actually bite"
    assert V.gate_multiple_testing(8.0, 500, 200, annualisation_factor=40.0).passed is False


def test_annualisation_factor_is_recovered_from_the_summary():
    assert V.annualisation_factor_from({"periods_per_year": 1600.0}) == pytest.approx(40.0)
    assert V.annualisation_factor_from({}) == 1.0, "missing field must not crash"
    assert V.annualisation_factor_from({"periods_per_year": "junk"}) == 1.0


def test_summarize_publishes_the_annualisation_multiplier():
    """Without this field the deflation gate silently under-corrects."""
    import backtest as bt
    trades = [{"net": n, "regime": "TRENDING", "outcome": "TP", "direction": 1,
               "i": i, "bars": 5} for i, n in enumerate([0.01, -0.005] * 25)]
    out = bt.summarize(trades, total_minutes=14400)     # 50 trades over 10 days
    assert out["periods_per_year"] == pytest.approx(365.0 * (50 / 10.0))
    # The published multiplier must be the one actually used on the Sharpe.
    mean = sum(t["net"] for t in trades) / len(trades)
    import statistics
    sd = statistics.pstdev([t["net"] for t in trades]) + 1e-9
    assert out["sharpe_ratio"] == pytest.approx(
        (mean / sd) * math.sqrt(out["periods_per_year"]), rel=1e-6)


def test_an_edge_present_in_only_one_fold_is_rejected():
    assert V.gate_stability([0.01, -0.002, -0.003, -0.001, -0.002]).passed is False
    assert V.gate_stability([0.004, 0.003, -0.001, 0.005, 0.002]).passed is True


def test_missing_costs_fail_the_costs_gate():
    from dataclasses import asdict
    assert V.gate_costs_applied(asdict(ZERO_COST)).passed is False
    assert V.gate_costs_applied(asdict(CostModel())).passed is True


def test_an_unchecked_gate_is_never_reported_as_a_pass():
    """
    The quiet failure mode of every validation harness: 'we didn't test it' and
    'it passed' rendering identically.
    """
    from dataclasses import asdict
    rep = V.validate_result({"trades": 500, "sharpe_ratio": 1.5}, asdict(CostModel()))
    names = {g.name: g for g in rep.gates}
    assert names["null_rejection"].passed is False
    assert "not supplied" in names["null_rejection"].detail
    assert names["oos_decay"].passed is False
    assert names["stability"].passed is False
    assert rep.verdict() == "REJECTED"


def test_a_fully_evidenced_good_result_is_accepted():
    from dataclasses import asdict
    rep = V.validate_result(
        {"trades": 600, "sharpe_ratio": 1.4}, asdict(CostModel()),
        null_expectancy=-0.0006, null_trades=400,
        is_metric=0.0020, oos_metric=0.0014,
        n_trials=6, per_fold=[0.002, 0.001, 0.003, -0.0005, 0.0015],
    )
    assert rep.verdict() == "ACCEPTED", rep.render()


def test_report_renders_every_gate():
    from dataclasses import asdict
    rep = V.validate_result({"trades": 10}, asdict(ZERO_COST))
    text = rep.render()
    assert text.startswith("VERDICT: REJECTED")
    assert all(g.name in text for g in rep.gates)
    assert len(rep.failures) >= 4
