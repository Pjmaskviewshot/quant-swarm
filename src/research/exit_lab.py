"""
EXIT LAB — measure what an exit policy does to the payoff distribution.

Built in response to the live session of Sept 2026, where 27 closed trades
showed a 66.7% win rate and still lost money: the average loss was 3.60x the
average win, so the system needed a 78.3% win rate just to break even.

The question this module answers is whether that shape comes from the ENTRIES
or from the EXITS. It drives the *actual live exit function*
(`IntelligentExitEngine.evaluate`) tick by tick over simulated price paths,
with a simulated clock, and records every trade's R-multiple, MFE and MAE.
Alternative exit policies run over the *identical* entries, so any difference
between them is caused by the exit rule alone.

A NOTE ON WHAT EXITS CAN AND CANNOT DO. On a driftless price path, no exit
rule has positive expectancy — by the optional stopping theorem every stopping
rule on a martingale has the same expected value, and costs make it negative.
Exits only RESHAPE the distribution: a tight profit lock buys a high win rate
by paying for it with small wins. What an exit rule CAN do is stop destroying
edge that the entries genuinely have. For a momentum entry, the edge lives in
the trades that keep running, so a rule that closes them early discards it.
That is the effect this lab measures.
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np

import core.intelligent_exit as ie
from core.intelligent_exit import IntelligentExitEngine, PositionExitState


# ----------------------------------------------------------------------------
# Price paths
# ----------------------------------------------------------------------------

def drifting_path(n_minutes: int, seed: int, sigma_per_min: float = 0.0010,
                  drift_strength: float = 0.0, drift_halflife_min: float = 60.0,
                  tick_seconds: int = 10, start: float = 100.0) -> np.ndarray:
    """
    Tick-level prices with a slowly varying hidden drift.

    drift_strength = 0 is a pure random walk: NO edge exists and no exit rule
    can create one. drift_strength > 0 plants a trend that a momentum entry
    can partially detect — the kind of edge a trend-following bot targets.
    It is expressed as the drift's stationary standard deviation relative to
    per-minute noise, so 0.10 means drift is 10% of one minute's noise.
    """
    rng = np.random.default_rng(seed)
    ticks_per_min = 60 // tick_seconds
    n = n_minutes * ticks_per_min
    dt = 1.0 / ticks_per_min                     # in minutes
    phi = 0.5 ** (dt / drift_halflife_min)
    drift_sd = drift_strength * sigma_per_min    # per minute
    innov = drift_sd * math.sqrt(1 - phi ** 2)
    mu = np.empty(n)
    m = 0.0
    shocks = rng.standard_normal(n)
    for i in range(n):
        m = phi * m + innov * shocks[i]
        mu[i] = m
    noise = rng.standard_normal(n) * sigma_per_min * math.sqrt(dt)
    log_ret = mu * dt + noise
    return start * np.exp(np.cumsum(log_ret))


# ----------------------------------------------------------------------------
# Policies
# ----------------------------------------------------------------------------

class _SimClock:
    def __init__(self):
        self.t = 1_700_000_000.0

    def time(self):
        return self.t

    def __getattr__(self, name):          # anything else the module might use
        import time as _real
        return getattr(_real, name)


class LivePolicy:
    """
    The exact exit engine the live bot runs, driven through
    IntelligentExitEngine.evaluate. `config` selects the policy: pass
    LEGACY_EXIT_CONFIG for the pre-2026-09 ladder, or leave it None for the
    engine's current default.
    """
    name = "LIVE engine"

    def __init__(self, config=None):
        self.clock = _SimClock()
        self.config = config
        if config is not None:
            self.name = "LIVE engine: " + ("LEGACY ladder" if config.legacy else "RUNNER (new default)")

    def start(self, entry_price, is_buy, atr, risk_dist, now):
        self.clock.t = now
        self.state = PositionExitState(
            position_id="sim", entry_time=now, entry_price=entry_price,
            exit_side="Sell" if is_buy else "Buy", entry_balance=1000.0,
            actual_qty=1.0, base_qty=1.0, last_eval_time=now - 10,
        )
        self.ctx = {
            "is_buy": is_buy, "symbol": "SIM", "atr": atr,
            "initial_risk_dist": risk_dist, "taker_fee_rate": 0.00055,
            "slippage_buffer_pct": 0.0004, "baseline_vol_pct": 0.005,
            "dynamic_rr_ratio": 2.0, "max_drawdown_pct": 0.15, "drawdown_pct": 0.0,
        }
        if self.config is not None:
            self.ctx["exit_config"] = self.config

    def step(self, price, now) -> Optional[str]:
        self.clock.t = now
        spread = price * 0.00005
        self.ctx["last_ob"] = {"best_bid": price - spread, "best_ask": price + spread}
        self.ctx["latest_tick_price"] = price
        self.ctx["mark_price"] = price
        saved = ie.time
        ie.time = self.clock
        try:
            d = IntelligentExitEngine.evaluate(self.ctx, self.state)
        finally:
            ie.time = saved
        if d.action in ("EXIT", "EMERGENCY"):
            return d.reason.split(" ")[0]
        if d.action == "SCALE_OUT":
            return "SCALE_OUT_1.3R"          # treat as full exit (conservative)
        return None


class BracketPolicy:
    """Fixed stop and target, nothing in between. The simplest baseline."""

    def __init__(self, tp_r: float = 2.0, max_minutes: float = 180.0):
        self.tp_r, self.max_minutes = tp_r, max_minutes
        self.name = f"BRACKET 1R/{tp_r:g}R"

    def start(self, entry_price, is_buy, atr, risk_dist, now):
        self.e, self.b, self.r, self.t0 = entry_price, is_buy, risk_dist, now

    def step(self, price, now):
        rr = ((price - self.e) if self.b else (self.e - price)) / self.r
        if rr <= -1.0:
            return "STOP"
        if rr >= self.tp_r:
            return "TARGET"
        if (now - self.t0) / 60.0 >= self.max_minutes:
            return "TIME"
        return None


class RunnerPolicy:
    """
    Proposed replacement. Same 1R stop; winners get room comparable to losers.

      * no stop movement until the trade has earned 1.0R
      * at 1.0R, stop to breakeven PLUS round-trip costs (a true scratch)
      * from 1.5R, trail the stop at (peak - trail_r)
      * hard target at tp_r
      * time stop only for trades that never earned anything
    """

    def __init__(self, be_at_r=1.0, trail_from_r=1.5, trail_r=1.0, tp_r=3.0,
                 max_minutes=240.0, cost_r=0.13):
        self.be_at_r, self.trail_from_r, self.trail_r = be_at_r, trail_from_r, trail_r
        self.tp_r, self.max_minutes, self.cost_r = tp_r, max_minutes, cost_r
        self.name = f"RUNNER BE@{be_at_r:g}R trail@{trail_from_r:g}R TP{tp_r:g}R"

    def start(self, entry_price, is_buy, atr, risk_dist, now):
        self.e, self.b, self.r, self.t0 = entry_price, is_buy, risk_dist, now
        self.stop_r, self.peak_r = -1.0, 0.0

    def step(self, price, now):
        rr = ((price - self.e) if self.b else (self.e - price)) / self.r
        self.peak_r = max(self.peak_r, rr)
        if self.peak_r >= self.be_at_r:
            self.stop_r = max(self.stop_r, self.cost_r)
        if self.peak_r >= self.trail_from_r:
            self.stop_r = max(self.stop_r, self.peak_r - self.trail_r)
        if rr <= self.stop_r:
            return "STOP" if self.stop_r < 0 else "TRAIL"
        if rr >= self.tp_r:
            return "TARGET"
        if (now - self.t0) / 60.0 >= self.max_minutes and self.peak_r < self.be_at_r:
            return "TIME"
        return None


# ----------------------------------------------------------------------------
# Simulation
# ----------------------------------------------------------------------------

@dataclass
class TradeResult:
    r: float
    pnl_pct: float
    mfe_r: float
    mae_r: float
    minutes: float
    reason: str


def _atr_pct(path: np.ndarray, i: int, ticks_per_min: int, period: int = 14,
             bar_min: int = 5) -> float:
    step = ticks_per_min * bar_min
    lo = max(0, i - step * period)
    bars = path[lo:i + 1:step]
    if len(bars) < 3:
        return 0.003
    return float(np.mean(np.abs(np.diff(bars)) / bars[:-1])) * 1.25


def run(path: np.ndarray, policy, entries: List[tuple], tick_seconds: int = 10,
        fee_rt: float = 0.0011, slippage: float = 0.0002,
        stop_floor_pct: float = 0.015, atr_mult: float = 2.5) -> List[TradeResult]:
    """
    Simulate `policy` over `entries` [(tick_index, is_buy), ...].

    The initial stop reproduces the live rule exactly:
        risk = max(atr_mult * ATR, stop_floor_pct * entry)
    Round-trip fees and exit slippage are charged on every trade.
    """
    ticks_per_min = 60 // tick_seconds
    out: List[TradeResult] = []
    for idx, is_buy in entries:
        e = float(path[idx])
        atr = _atr_pct(path, idx, ticks_per_min) * e
        risk = max(atr_mult * atr, stop_floor_pct * e)
        t0 = 1_700_000_000.0 + idx * tick_seconds
        policy.start(e, is_buy, atr, risk, t0)
        mfe = mae = 0.0
        reason, j = "END", idx
        for j in range(idx + 1, min(len(path), idx + ticks_per_min * 300)):
            p = float(path[j])
            rr = ((p - e) if is_buy else (e - p)) / risk
            mfe, mae = max(mfe, rr), min(mae, rr)
            why = policy.step(p, t0 + (j - idx) * tick_seconds)
            if why:
                reason = why
                break
        exit_p = float(path[j])
        exit_p = exit_p * (1 - slippage) if is_buy else exit_p * (1 + slippage)
        move = (exit_p - e) / e if is_buy else (e - exit_p) / e
        pnl = move - fee_rt
        out.append(TradeResult(pnl * e / risk, pnl, mfe, mae,
                               (j - idx) / ticks_per_min, reason))
    return out


def momentum_entries(path: np.ndarray, tick_seconds: int = 10, lookback_min: int = 15,
                     spacing_min: int = 300, threshold_sigma: float = 0.0,
                     sigma_per_min: float = 0.0010) -> List[tuple]:
    """
    Enter in the direction of the trailing `lookback_min` return, every
    `spacing_min` minutes, so trades never overlap. Identical entries are fed
    to every policy — only the exit differs.
    """
    tpm = 60 // tick_seconds
    lb, sp = lookback_min * tpm, spacing_min * tpm
    thr = threshold_sigma * sigma_per_min * math.sqrt(lookback_min)
    out = []
    for i in range(lb + 14 * 5 * tpm, len(path) - 300 * tpm, sp):
        ret = math.log(path[i] / path[i - lb])
        if abs(ret) > thr:
            out.append((i, ret > 0))
    return out


def summarise(trades: List[TradeResult]) -> Dict[str, float]:
    if not trades:
        return {"n": 0}
    w = [t.pnl_pct for t in trades if t.pnl_pct > 0]
    l = [t.pnl_pct for t in trades if t.pnl_pct <= 0]
    aw = statistics.mean(w) if w else 0.0
    al = statistics.mean(l) if l else 0.0
    pnls = [t.pnl_pct for t in trades]
    mean = statistics.mean(pnls)
    se = statistics.stdev(pnls) / math.sqrt(len(pnls)) if len(pnls) > 1 else float("nan")
    return {
        "n": len(trades),
        "win_rate": len(w) / len(trades),
        "avg_win_pct": aw * 100,
        "avg_loss_pct": al * 100,
        "loss_to_win": abs(al) / aw if aw > 0 else float("inf"),
        "breakeven_wr": abs(al) / (abs(al) + aw) if aw > 0 else 1.0,
        "expectancy_bps": mean * 1e4,
        "t_stat": mean / se if se and se > 0 else 0.0,
        "profit_factor": sum(w) / abs(sum(l)) if l and sum(l) != 0 else float("inf"),
        "avg_minutes": statistics.mean(t.minutes for t in trades),
        "avg_mfe_r": statistics.mean(t.mfe_r for t in trades),
    }


class NoiseStatEngine:
    """
    Stand-in for the live stat engine: order-flow, Hawkes, acceleration and
    changepoint readings that are PURE NOISE — autocorrelated, as real
    microstructure features are, but carrying no information about the future.

    The live exit engine consults these on every tick once a trade is ahead
    (EARLY_FLOW_OPPOSITION at +0.25R, ALPHA_DRIFT_INVERSION at +0.40R, ...).
    Feeding it noise measures how often those rules close a winner for no
    reason — which is exactly what happens when a signal has no real edge.
    """

    def __init__(self, seed: int, halflife_ticks: float = 18.0):
        from probability import make_estimate
        self._make = make_estimate
        self.rng = np.random.default_rng(seed)
        self.phi = 0.5 ** (1.0 / halflife_ticks)
        self.s = math.sqrt(1 - self.phi ** 2)
        self._z = np.zeros(5)
        self.kinetic_tensor = type("K", (), {"accel_z": 0.0})()
        self.tick()

    def tick(self):
        self._z = self.phi * self._z + self.s * self.rng.standard_normal(5)
        self.clean_ofi_z = float(self._z[0])
        self.marked_hawkes_z = float(self._z[1])
        self.kinetic_tensor.accel_z = float(self._z[2])
        self.changepoint_prob = float(1 / (1 + math.exp(-2.0 * self._z[3] + 2.0)))
        p_up = 0.5 + 0.15 * math.tanh(self._z[4])
        self.latest_probability = self._make(p_up, horizon_seconds=60)


class LivePolicyWithFlow(LivePolicy):
    """Live exit engine including its flow-based exits, fed with noise features."""

    def __init__(self, seed: int = 0, config=None):
        super().__init__(config)
        self.engine = NoiseStatEngine(seed)

    def start(self, *a, **k):
        super().start(*a, **k)
        self.ctx["stat_engine"] = self.engine

    def step(self, price, now):
        self.engine.tick()
        return super().step(price, now)
