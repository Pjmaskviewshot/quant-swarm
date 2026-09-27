"""
V12 wiring into the live engine: tick feed, decision gate, fills, settlement,
reconciliation, equity, API health, and the exit engine's R definition.
"""
import asyncio
import pathlib

import numpy as np
import pytest

import main as M
from core.intelligent_exit import IntelligentExitEngine, PositionExitState
from services.bybit_v5 import BybitUnifiedExecutor
from v12.runtime import V12Runtime

MAIN = (pathlib.Path(__file__).resolve().parents[1] / "src" / "main.py").read_text()


# ---- source-level wiring (the engine cannot be booted without an exchange) ----

def test_v12_gate_runs_before_capacity_is_reserved():
    gate = MAIN.index("self.v12.evaluate, symbol, action")
    reserve = MAIN.index("reserved_ok, risk_reason = await self._try_reserve_entry(")
    assert gate < reserve


def test_v12_can_only_shrink_size_and_owns_the_stop():
    assert "target_notional = min(target_notional, v12_verdict.notional)" in MAIN
    assert "sl_dist_pct = v12_verdict.stop_pct" in MAIN
    assert "tp_dist_pct = sl_dist_pct * dynamic_rr" in MAIN


def test_enforce_mode_fails_closed_when_pipeline_is_missing():
    i = MAIN.index('if self.v12_mode == "enforce" and self.v12 is None:')
    assert "return" in MAIN[i:i + 160]


def test_settlement_reconciliation_equity_and_api_are_wired():
    for needle in ("self.v12.on_settle(", "self.v12.set_reconciliation(", "self.v12.set_positions(",
                   "self.v12.set_equity(", "api_observer = self.v12.record_api", "self.v12.on_tick(",
                   "self.v12.on_fill(", "self.run_v12_reporter", "self.run_v12_warm_start"):
        assert needle in MAIN, needle


def test_unknown_equity_is_pushed_as_none():
    assert "self.v12.set_equity(None)" in MAIN


@pytest.mark.parametrize("payload,age", [({"ts": 1_700_000_000_000}, 5.0), ({"timestamp": 1_700_000_003.0}, 2.0),
                                         ({}, None), ({"ts": "junk"}, None)])
def test_orderbook_age(payload, age):
    got = M.DistributedQuantEngine._ob_age_sec(payload, 1_700_000_005.0)
    assert got == (pytest.approx(age) if age is not None else None)


# ---- executor reports transport health, not business rejections ------------

@pytest.mark.parametrize("resp,ok", [({"retCode": 0, "result": {}}, True),
                                     ({"retCode": 110043, "retMsg": "leverage not modified"}, True),
                                     ({"retCode": 10006, "retMsg": "rate limit"}, False),
                                     ({"retCode": 10016}, False), (None, False)])
def test_api_observer_classification(resp, ok):
    ex = BybitUnifiedExecutor("", "")
    seen = []
    ex.api_observer = seen.append

    async def fake(*a, **k):
        return resp
    ex._safe_api_call = fake
    asyncio.run(ex.safe_call("GET", "/v5/x"))
    assert seen == [ok]


def test_api_observer_sees_exceptions_and_reraises():
    ex = BybitUnifiedExecutor("", "")
    seen = []
    ex.api_observer = seen.append

    async def boom(*a, **k):
        raise TimeoutError("network")
    ex._safe_api_call = boom
    with pytest.raises(TimeoutError):
        asyncio.run(ex.safe_call("GET", "/v5/x"))
    assert seen == [False]


# ---- exit engine: R means the V12 stop when V12 set it -----------------------

def _latched_risk(flag):
    st = PositionExitState(position_id="t", entry_time=1_700_000_000.0, entry_price=100.0, exit_side="Sell",
                           entry_balance=1000.0, actual_qty=1.0, base_qty=1.0, last_eval_time=1_699_999_990.0)
    ctx = {"is_buy": True, "symbol": "SIM", "atr": 0.1, "initial_risk_dist": 0.8, "taker_fee_rate": 0.00055,
           "slippage_buffer_pct": 0.0004, "baseline_vol_pct": 0.005, "dynamic_rr_ratio": 3.0,
           "max_drawdown_pct": 0.15, "drawdown_pct": 0.0, "latest_tick_price": 100.0, "mark_price": 100.0,
           "last_ob": {"best_bid": 99.99, "best_ask": 100.01}, "risk_dist_from_v12": flag}
    IntelligentExitEngine.evaluate(ctx, st)
    return st.profit_state.initial_risk_dist


def test_exit_engine_uses_v12_stop_as_one_r():
    assert _latched_risk(True) == pytest.approx(0.8)
    assert _latched_risk(False) == pytest.approx(1.5)       # legacy 1.5% floor unchanged


# ---- runtime end to end ------------------------------------------------------

def _trend_ticks(minutes=2500, seed=5):
    from research.exit_lab import drifting_path
    p = drifting_path(minutes, seed=seed, sigma_per_min=0.0007, drift_strength=0.3, drift_halflife_min=240)
    return [(1_700_000_000 + 10 * i, float(x)) for i, x in enumerate(p)]


def test_runtime_trade_lifecycle_and_restart_restores_learning(tmp_path):
    db = str(tmp_path / "j.db")
    rt = V12Runtime("PAPER", mode="enforce", journal_path=db)
    rt.set_equity(1000.0)
    verdict = None
    for ts, px in _trend_ticks():
        rt.on_tick("SIM", ts, px, 1.0, 0.1)
        if ts % 60 == 0:
            rt.set_reconciliation(True, "ok")
            rt.guard.reconcile_ts = ts
            for d in ("BUY", "SELL"):
                v = rt.evaluate("SIM", d, ts, 1000.0, 1.0)
                if v.allowed:
                    verdict = (v, ts, px)
                    break
            if verdict:
                break
    assert verdict is not None, "a strong planted trend should produce a TRADE within ~40 hours"
    v, ts, px = verdict
    assert v.stop_pct > 0 and 0 < v.notional <= 250.0
    rt.on_fill("sig1", v, ts, px, v.notional / px)
    res = rt.on_settle("sig1", ts + 3600, px * 1.01, net_pnl=v.notional * 0.0085, fees=v.notional * 0.0011,
                       exit_reason="EXCHANGE_TP", mfe_r=2.0, mae_r=-0.3)
    assert res["status"] == "CLOSED" and res["net_return_bps"] == pytest.approx(85.0, rel=1e-6)
    assert rt.pipe.edge_model.n == 1 and rt.health.report().n == 1
    rt2 = V12Runtime("PAPER", mode="enforce", journal_path=db)
    assert rt2.pipe.edge_model.n == 1 and rt2.health.report().n == 1


def test_shadow_mode_never_blocks(tmp_path):
    rt = V12Runtime("PAPER", mode="shadow", journal_path=str(tmp_path / "j.db"))
    v = rt.evaluate("SIM", "BUY", 1_700_000_000, 1000.0, 1.0)
    assert v.allowed and not v.would_allow


def test_enforce_blocks_until_forecasts_are_measured(tmp_path):
    rt = V12Runtime("PAPER", mode="enforce", journal_path=str(tmp_path / "j.db"))
    for i in range(50):
        rt.on_tick("SIM", 1_700_000_000 + 10 * i, 100.0 + 0.01 * i, 1.0, 0.1)
    v = rt.evaluate("SIM", "BUY", 1_700_000_500, 1000.0, 1.0)
    assert not v.allowed and "NOT YET MEASURED" in v.reason


def test_warm_start_counts_and_never_overwrites_live_data(tmp_path):
    rt = V12Runtime("PAPER", mode="enforce", journal_path=str(tmp_path / "j.db"))
    rng = np.random.default_rng(0)
    bars = [(1_700_000_000 + 60 * i, float(100 * np.exp(0.0007 * rng.standard_normal()))) for i in range(300)]
    assert rt.warm_start("SIM", bars) == 300
    assert rt.warm_start("SIM", bars) == 0          # nothing older than what is already held


def test_live_mode_without_promotion_refuses(tmp_path, monkeypatch):
    monkeypatch.setenv("PROMOTION_FILE", str(tmp_path / "none.json"))
    monkeypatch.delenv("ALLOW_UNPROMOTED_LIVE", raising=False)
    rt = V12Runtime("LIVE", mode="enforce", journal_path=str(tmp_path / "j.db"))
    g = rt.guard.evaluate_entry("BTCUSDT", "BUY", 10.0)
    assert not g.allowed and g.level == "PROMOTION"
