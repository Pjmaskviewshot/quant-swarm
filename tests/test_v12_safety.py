"""
V12 safety layer: kill-switch hierarchy (fail closed), strategy health,
journal, promotion gate, and an exit optimiser that can only RECOMMEND.
"""
import json

import numpy as np
import pytest

from core.intelligent_exit import EXIT_CONFIG
from v12.exit_optimizer import (RPath, load_approved_exit_config, mfe_mae_report, replay,
                                walk_forward_optimise, write_candidate)
from v12.guardian import Guardian, GuardianConfig, clusters_of
from v12.health import StrategyHealthMonitor
from v12.journal import TradeJournal
from v12.promotion import evaluate


def ready(mode="PAPER", **cfg):
    g = Guardian(GuardianConfig(**cfg), mode=mode)
    g.set_equity(1000.0, 0)
    g.set_reconciliation(True, "ok", 0)
    g.set_data_age("BTCUSDT", 0.5)
    return g


# ---- L5 infrastructure: fail closed ---------------------------------------

def test_fresh_guardian_blocks_everything():
    d = Guardian().evaluate_entry("BTCUSDT", "BUY", 10, now=1)
    assert not d.allowed and d.level == "L5_INFRASTRUCTURE"
    assert "equity unknown" in d.reason and "reconciliation" in d.reason


def test_ready_guardian_allows():
    assert ready().evaluate_entry("BTCUSDT", "BUY", 100, now=10).allowed


@pytest.mark.parametrize("breaker", ["nan_equity", "mismatch", "stale_recon", "stale_data", "api", "spread"])
def test_each_infrastructure_fault_blocks(breaker):
    g = ready()
    now = 10
    if breaker == "nan_equity":
        g.set_equity(float("nan"))
    elif breaker == "mismatch":
        g.set_reconciliation(False, "exchange holds ETHUSDT, bot does not", 5)
    elif breaker == "stale_recon":
        now = 10_000
        g.set_data_age("BTCUSDT", 0.5)
    elif breaker == "stale_data":
        g.set_data_age("BTCUSDT", 30.0)
    elif breaker == "api":
        for i in range(30):
            g.record_api(ok=i % 2 == 0, now=5)
    elif breaker == "spread":
        for _ in range(60):
            g.observe_spread("BTCUSDT", 1.0)
        g.observe_spread("BTCUSDT", 9.0)
    d = g.evaluate_entry("BTCUSDT", "BUY", 100, now=now)
    assert not d.allowed and d.level == "L5_INFRASTRUCTURE", d.reason


# ---- L2-L4 ---------------------------------------------------------------

def test_daily_loss_limit_blocks_until_next_day():
    g = ready(max_daily_loss_pct=0.03)
    g.set_equity(965.0, 100)
    assert g.evaluate_entry("BTCUSDT", "BUY", 50, now=100).level == "L3_DAILY_LOSS"
    g.set_equity(965.0, 86400 + 5)
    g.set_reconciliation(True, "ok", 86400 + 5)
    assert g.evaluate_entry("BTCUSDT", "BUY", 50, now=86400 + 10).allowed


def test_drawdown_pause():
    g = ready(drawdown_pause_pct=0.08, max_daily_loss_pct=0.5)
    g.set_equity(910.0, 10)
    assert g.evaluate_entry("BTCUSDT", "BUY", 50, now=10).level == "L4_DRAWDOWN"


def test_strategy_halt_and_consecutive_losses_and_operator_reset():
    g = ready()
    g.set_strategy_halt("EDGE BELOW COST")
    assert g.evaluate_entry("BTCUSDT", "BUY", 50, now=10).level == "L2_STRATEGY"
    g.reset_strategy()
    for _ in range(6):
        g.record_trade(-0.1)
    assert g.evaluate_entry("BTCUSDT", "BUY", 50, now=10).level == "L2_STRATEGY"
    g.reset_strategy()
    assert g.evaluate_entry("BTCUSDT", "BUY", 50, now=10).allowed


def test_correlated_cluster_exposure_is_capped_across_overlapping_clusters():
    g = Guardian()
    g.set_equity(77.0, 0)
    g.set_reconciliation(True, "ok", 0)
    g.set_data_age("DOGEUSDT", 0.5)
    g.set_open_positions({"SHIBUSDT": ("BUY", 19.0), "WIFUSDT": ("BUY", 19.0)})
    d = g.evaluate_entry("DOGEUSDT", "BUY", 19.0, now=10)
    assert not d.allowed and "MEME" in d.reason
    assert set(clusters_of("PEPEUSDT")) == {"ETH_ECO", "MEME"}
    # the opposite direction is a hedge, not concentration
    assert g.evaluate_entry("DOGEUSDT", "SELL", 19.0, now=10).allowed


def test_gross_exposure_cap():
    g = ready(max_gross_exposure_pct=1.0)
    g.set_open_positions({"ETHUSDT": ("BUY", 400.0), "SOLUSDT": ("SELL", 450.0)})
    assert g.evaluate_entry("BTCUSDT", "BUY", 200.0, now=10).level == "EXPOSURE"


# ---- promotion ------------------------------------------------------------

def test_live_requires_approved_promotion(tmp_path, monkeypatch):
    monkeypatch.delenv("ALLOW_UNPROMOTED_LIVE", raising=False)
    f = tmp_path / "promo.json"
    g = ready(mode="LIVE", promotion_file=str(f))
    assert g.evaluate_entry("BTCUSDT", "BUY", 50, now=10).level == "PROMOTION"
    f.write_text(json.dumps({"approved": False}))
    g.promotion = None
    assert g.evaluate_entry("BTCUSDT", "BUY", 50, now=10).level == "PROMOTION"
    f.write_text(json.dumps({"approved": True, "approved_by": "kerry"}))
    g.promotion = None
    assert g.evaluate_entry("BTCUSDT", "BUY", 50, now=10).allowed


def _trades(n, mean_bps, seed=0, sym_cycle=5):
    rng = np.random.default_rng(seed)
    out = []
    for i in range(n):
        bps = float(rng.normal(mean_bps, 60))
        out.append({"trading_mode": "PAPER", "symbol": f"S{i % sym_cycle}", "net_return_bps": bps,
                    "net_pnl": bps / 1e4 * 100, "expected_edge_bps": 20.0, "cost_bps": 16.0,
                    "fees": 0.11, "entry_price": 100.0, "qty": 1.0})
    return out


def test_promotion_refuses_small_or_losing_samples():
    assert not evaluate(_trades(50, 30), 1000)["eligible"]
    assert not evaluate(_trades(300, -5), 1000)["eligible"]


def test_promotion_accepts_a_clear_edge():
    r = evaluate(_trades(300, 20, seed=3), 1000)
    assert r["eligible"], [c for c in r["checks"] if not c["pass"]]


def test_promotion_rejects_one_lucky_coin():
    t = _trades(300, 0.0, seed=4)
    t[0]["net_pnl"] = 50.0
    assert not evaluate(t, 1000)["eligible"]


# ---- health monitor ---------------------------------------------------------

def test_health_warms_up_then_halts_on_edge_below_cost():
    h = StrategyHealthMonitor(min_trades=30)
    assert h.report().status == "WARMING_UP"
    rng = np.random.default_rng(1)
    for _ in range(40):
        b = float(rng.normal(-6, 30))
        h.record_trade(b / 100, b, b / 80, 12.0, 15.0)
    r = h.report(1000)
    assert r.status == "HALT" and any("EDGE BELOW COST" in x for x in r.reasons)
    text = r.render()
    assert text.index("Expectancy") < text.index("Win Rate")        # win rate last


def test_health_ok_on_real_edge_and_ignores_nan():
    h = StrategyHealthMonitor(min_trades=30)
    rng = np.random.default_rng(2)
    for _ in range(40):
        b = float(rng.normal(25, 30))
        h.record_trade(b / 100, b, b / 80, 12.0, 30.0, 1.5, -0.4)
    h.record_trade(float("nan"), 1, 1, 1, 1)
    r = h.report(1000)
    assert r.n == 40 and r.status == "OK"


# ---- journal ----------------------------------------------------------------

class _Dec(dict):
    def as_dict(self):
        return dict(self)


def test_journal_round_trip_and_post_exit_path(tmp_path):
    j = TradeJournal(str(tmp_path / "j.db"))
    d = _Dec(symbol="BTCUSDT", direction="BUY", stop_pct=0.01, horizon_sec=3600, net_edge_bps=12.0,
             cost_bps=16.0, regime="TREND_UP/NORMAL", decision="TRADE", reasons=[], ts=0.0)
    j.open_trade("t1", d, 0.0, 100.0, 1.0)
    for k in range(100):
        j.on_price("BTCUSDT", k * 10.0, 100.0 + k * 0.01)
    res = j.close_trade("t1", 500.0, 100.5, net_pnl=0.39, fees=0.11, exit_reason="TRAIL")
    for k in range(50, 200):
        j.on_price("BTCUSDT", k * 10.0, 100.5)
    assert res["net_return_bps"] == pytest.approx(39.0)
    assert res["r_multiple"] == pytest.approx(0.39)
    phases = {ph for _, _, ph in j.trade_path("t1")}
    assert phases == {"IN", "POST"}


def test_journal_unknown_pnl_is_not_breakeven(tmp_path):
    j = TradeJournal(str(tmp_path / "j.db"))
    j.open_trade("t2", _Dec(symbol="X", direction="SELL", stop_pct=0.01), 0.0, 10.0, 1.0)
    assert j.close_trade("t2", 60.0, 10.0, net_pnl=float("nan"), fees=None, exit_reason="?")["status"] == "UNKNOWN"
    assert j.closed_trades() == []


def test_journal_samples_rejections_but_keeps_trades(tmp_path):
    j = TradeJournal(str(tmp_path / "j.db"), decision_sample_every_sec=60)
    for k in range(10):
        j.record_decision(_Dec(symbol="X", direction="BUY", decision="NO_TRADE",
                               reasons=["EDGE DOES NOT CLEAR COST (expected 1 bps)"], ts=float(k)), 1.0)
    assert j.decision_counts() == {"EDGE DOES NOT CLEAR COST": 1}


# ---- exit optimiser: recommends only ---------------------------------------

def _rpaths(n, seed, drift=0.0):
    rng = np.random.default_rng(seed)
    out = []
    for i in range(n):
        r = np.concatenate([[0.0], np.cumsum(drift + 0.12 * rng.standard_normal(400))])
        out.append(RPath(str(i), 300, list(r), 1.0, 0.15))
    return out


def test_replay_charges_stop_overshoot():
    p = RPath("x", 3, [0.0, -0.5, -1.4, -1.4], 1.0, 0.1)
    assert replay(p, EXIT_CONFIG)[0] == pytest.approx(-1.5)


def test_optimiser_needs_enough_trades():
    r = walk_forward_optimise(_rpaths(40, 1))
    assert not r["recommend"] and "need 100" in r["reason"]


def test_optimiser_does_not_recommend_on_noise():
    r = walk_forward_optimise(_rpaths(300, 2))
    assert not r["recommend"]


def test_candidates_are_written_unapproved_and_not_loaded(tmp_path):
    r = walk_forward_optimise(_rpaths(300, 3, drift=0.01))
    f = tmp_path / "cand.json"
    write_candidate(r, str(f))
    assert json.loads(f.read_text())["approved"] is False
    assert load_approved_exit_config(str(f)) is None
    doc = json.loads(f.read_text())
    doc.update(approved=True, approved_by="kerry", best=dict(doc["current"], trail_distance_r=1.25))
    f.write_text(json.dumps(doc))
    assert load_approved_exit_config(str(f)).trail_distance_r == 1.25
    assert load_approved_exit_config(None) is None


def test_mfe_mae_report_flags_loose_profit_protection():
    paths = []
    for i in range(60):
        r = [0.0, 0.6, 1.2, 0.3, -0.5, -1.05] if i % 2 else [0.0, 0.5, 1.0, 2.0, 2.5, 2.2]
        paths.append(RPath(str(i), len(r) - 1, r, 1.0, 0.1))
    rep = mfe_mae_report(paths)
    assert rep["losers_once_above_1r"] == 1.0
    assert any("profit protection" in n for n in rep["notes"])
