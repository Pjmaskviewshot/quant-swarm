"""
V12.2 — NET-EDGE CALCULATOR AND TRADE-QUALITY CARD.

    Expected edge = P(win) x E[win] - P(loss) x E[loss] - trading costs

computed for the exits the bot ACTUALLY uses. The runner policy (breakeven+costs
at +1R, trail 1R behind the peak from +1.5R, 3R target, time rules) is not a
two-barrier bracket, so a closed-form bracket formula would misstate it. The
expectation is instead taken by simulating that policy -- vectorised, fixed
seed, common random numbers so horizons and directions are compared fairly --
under:

  * drift  = the forecaster's MEASURED expected move for the horizon, applied
             only for that horizon (beyond it there is no forecast, so none is
             assumed)
  * vol    = measured 1-minute volatility
  * costs  = taker fees both legs + half the live spread both legs + a
             volatility-scaled slippage estimate both legs + funding for the
             expected holding time

A closed-form two-barrier probability (Brownian motion with drift) is provided
to cross-check the simulator in tests.

If the expected move is 2 bps and costs are 15, the answer is
NO TRADE -- EDGE DOES NOT CLEAR COST. That is the point of this module.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

import numpy as np

_SHOCK_CACHE: Dict[tuple, 'np.ndarray'] = {}


@dataclass(frozen=True)
class CostModel:
    taker_fee: float = 0.00055
    maker_fee: float = 0.00020
    # Calibrated to the live Sept-2026 session: average slippage across 27
    # trades was ~3.4 bps per round trip at ~7 bps/min volatility.
    base_slippage_bps: float = 1.0
    slippage_per_sigma: float = 0.2      # extra slippage bps per leg, per bp of 1-minute vol

    def breakdown_bps(self, spread_bps: float, sigma_1m: float, hold_min: float,
                      funding_rate_8h: float = 0.0, is_buy: bool = True) -> Dict[str, float]:
        fees = 2 * self.taker_fee * 1e4
        spread = max(0.0, spread_bps)                   # half-spread paid on each leg
        slip = 2 * (self.base_slippage_bps + self.slippage_per_sigma * sigma_1m * 1e4)
        # longs pay positive funding, shorts receive it (and vice versa)
        funding = (1 if is_buy else -1) * funding_rate_8h * 1e4 * (hold_min / 480.0)
        return {"fees": fees, "spread": spread, "slippage": slip, "funding": funding,
                "total": fees + spread + slip + funding}


def two_barrier_up_probability(drift: float, sigma: float, up: float, down: float) -> float:
    """
    P(hit +up before -down) for X_t = drift*t + sigma*W_t from 0.
    Driftless limit: down / (up + down).
    """
    if sigma <= 0:
        return 1.0 if drift > 0 else 0.0
    k = 2.0 * drift / (sigma * sigma)
    if abs(k * (up + down)) < 1e-9:
        return down / (up + down)
    a = math.exp(min(700.0, k * down))
    b = math.exp(max(-700.0, -k * up))
    return (1.0 - a) / (b - a)


@dataclass(frozen=True)
class PolicyEV:
    ev_r: float                  # expected R per trade BEFORE costs
    p_win: float
    avg_win_r: float
    avg_loss_r: float
    exp_hold_min: float


def simulate_runner_ev(drift_per_min: float, sigma_per_min: float, stop_pct: float,
                       drift_minutes: float, *, be_trigger_r: float = 1.0,
                       trail_start_r: float = 1.5, trail_distance_r: float = 1.0,
                       target_r: float = 3.0, stagnation_min: float = 90.0,
                       stagnation_r: float = 0.35, horizon_min: float = 180.0,
                       max_min: float = 480.0, cost_r: float = 0.0,
                       n_paths: int = 1500, step_min: float = 0.5, seed: int = 12345) -> PolicyEV:
    """
    Expected outcome of the runner exit policy, in R, before costs. Paths are in
    R units (log return / stop). Stop exits are charged any overshoot past the
    stop between monitoring steps; take-profits are capped at the target. The
    discretisation bias is therefore deliberately pessimistic: on a driftless
    market this returns slightly below zero, never above.
    """
    rng = np.random.default_rng(seed)
    steps = int(max_min / step_min)
    mu = drift_per_min * step_min / stop_pct
    sd = sigma_per_min * math.sqrt(step_min) / stop_pct
    drift_steps = int(max(0.0, drift_minutes) / step_min)
    r = np.zeros(n_paths)
    peak = np.zeros(n_paths)
    stop = np.full(n_paths, -1.0)
    alive = np.ones(n_paths, bool)
    out = np.zeros(n_paths)
    hold = np.full(n_paths, max_min)
    key = (seed, steps, n_paths)
    shocks = _SHOCK_CACHE.get(key)
    if shocks is None:               # common random numbers: identical draws for every call
        shocks = rng.standard_normal((steps, n_paths))
        if len(_SHOCK_CACHE) > 8:
            _SHOCK_CACHE.clear()
        _SHOCK_CACHE[key] = shocks
    for i in range(steps):
        if not alive.any():
            break
        r = r + (mu if i < drift_steps else 0.0) + sd * shocks[i]
        t = (i + 1) * step_min
        peak = np.maximum(peak, r)
        stop = np.where(peak >= be_trigger_r, np.maximum(stop, cost_r), stop)
        stop = np.where(peak >= trail_start_r, np.maximum(stop, peak - trail_distance_r), stop)
        hit_stop = alive & (r <= stop)
        hit_tp = alive & ~hit_stop & (r >= target_r)
        time_out = alive & ~hit_stop & ~hit_tp & (
            ((t >= stagnation_min) & (r < -stagnation_r)) | ((t >= horizon_min) & (peak < be_trigger_r)))
        # A path that crosses the stop between two monitoring steps is filled
        # at the path value, not the stop level. Crediting the stop level made
        # a driftless market show +0.06R -- an OPTIMISTIC bias, the dangerous
        # kind for a gate. Charging the overshoot errs pessimistic instead.
        out = np.where(hit_stop, np.minimum(stop, r), out)
        out = np.where(hit_tp, target_r, out)
        out = np.where(time_out, r, out)
        done = hit_stop | hit_tp | time_out
        hold = np.where(done, t, hold)
        alive &= ~done
    out = np.where(alive, r, out)
    wins = out > 0
    return PolicyEV(
        ev_r=float(out.mean()),
        p_win=float(wins.mean()),
        avg_win_r=float(out[wins].mean()) if wins.any() else 0.0,
        avg_loss_r=float(out[~wins].mean()) if (~wins).any() else 0.0,
        exp_hold_min=float(hold.mean()),
    )


_ZERO_CACHE: Dict[tuple, float] = {}
_DRIFT_CACHE: Dict[tuple, 'PolicyEV'] = {}
_CURVE_CACHE: Dict[tuple, tuple] = {}

# Drift grid, in units of per-minute sigma (z = drift / sigma). z = 0 is the
# control variate. Forecasts beyond the last point are CLAMPED to it, which
# under-states very strong forecasts -- the safe direction for a gate.
Z_GRID = np.array([0.0, 0.005, 0.01, 0.015, 0.02, 0.03, 0.04, 0.05, 0.065, 0.08, 0.1, 0.125,
                   0.15, 0.2, 0.25, 0.3, 0.4, 0.5])
S_R_STEP = 0.03          # sigma/stop quantised DOWN on a 3% geometric grid
COST_R_STEP = 0.02       # cost in R quantised to 0.02R (moves the breakeven stop only)


def _curve(s_r: float, drift_steps: int, cost_r: float, target_r: float, n_paths: int,
           step_min: float, seed: int, policy: tuple) -> tuple:
    """Simulate the runner policy for EVERY z in Z_GRID at once, on common random
    numbers. Returns arrays (ev, p_win, avg_win, avg_loss, hold) over Z_GRID."""
    pol = dict(policy)
    be, ts_, td = pol.get("be_trigger_r", 1.0), pol.get("trail_start_r", 1.5), pol.get("trail_distance_r", 1.0)
    stag_min, stag_r = pol.get("stagnation_min", 90.0), pol.get("stagnation_r", 0.35)
    hz, max_min = pol.get("horizon_min", 180.0), pol.get("max_min", 480.0)
    steps = int(max_min / step_min)
    key = (seed, steps, n_paths)
    shocks = _SHOCK_CACHE.get(key)
    if shocks is None:
        shocks = np.random.default_rng(seed).standard_normal((steps, n_paths))
        if len(_SHOCK_CACHE) > 8:
            _SHOCK_CACHE.clear()
        _SHOCK_CACHE[key] = shocks
    nz = len(Z_GRID)
    sd = s_r * math.sqrt(step_min)
    mu = (Z_GRID * s_r * step_min)[:, None]          # per step, in R
    r = np.zeros((nz, n_paths))
    peak = np.zeros_like(r)
    stop = np.full_like(r, -1.0)
    alive = np.ones_like(r, dtype=bool)
    out = np.zeros_like(r)
    hold = np.full_like(r, max_min)
    for i in range(steps):
        if not alive.any():
            break
        r = r + sd * shocks[i]
        if i < drift_steps:
            r = r + mu
        t = (i + 1) * step_min
        np.maximum(peak, r, out=peak)
        stop = np.where(peak >= be, np.maximum(stop, cost_r), stop)
        stop = np.where(peak >= ts_, np.maximum(stop, peak - td), stop)
        hit_stop = alive & (r <= stop)
        hit_tp = alive & ~hit_stop & (r >= target_r)
        time_out = alive & ~hit_stop & ~hit_tp & (
            ((t >= stag_min) & (r < -stag_r)) | ((t >= hz) & (peak < be)))
        out = np.where(hit_stop, np.minimum(stop, r), out)     # overshoot charged
        out = np.where(hit_tp, target_r, out)
        out = np.where(time_out, r, out)
        done = hit_stop | hit_tp | time_out
        hold = np.where(done, t, hold)
        alive &= ~done
    out = np.where(alive, r, out)
    wins = out > 0
    nw = wins.sum(1)
    ev = out.mean(1)
    pw = wins.mean(1)
    aw = np.where(nw > 0, (out * wins).sum(1) / np.maximum(nw, 1), 0.0)
    al = np.where(nw < n_paths, (out * ~wins).sum(1) / np.maximum(n_paths - nw, 1), 0.0)
    return ev, pw, aw, al, hold.mean(1)


def policy_edge_r(drift_per_min: float, sigma_per_min: float, stop_pct: float,
                  drift_minutes: float, *, target_r: float = 3.0, cost_r: float = 0.0,
                  conservative_margin_r: float = 0.01, n_paths: int = 1500,
                  step_min: float = 1.0, seed: int = 12345, **policy) -> PolicyEV:
    """
    Control-variate estimate of the runner policy's expected R before costs.

    A driftless market has zero expectancy under ANY stopping rule, so the
    zero-drift case, simulated on the SAME random numbers, is subtracted. That
    removes both Monte-Carlo noise (+/-0.05R per seed on its own -- about
    +/-4 bps at a 0.8% stop, too close to a 5 bps gate) and discretisation
    bias. What remains is the effect of the forecast drift alone, less a small
    conservative margin: a driftless forecast comes out at exactly -margin.

    Speed: results depend on drift and volatility only through z = drift/sigma
    and s = sigma/stop. The whole z-curve is simulated once per (s, horizon,
    cost) cell and then interpolated, so a decision costs microseconds after
    the first one in a cell (was ~27 ms per call, 90% of backtest time).
    """
    if not (sigma_per_min > 0 and stop_pct > 0):
        return PolicyEV(-conservative_margin_r, 0.0, 0.0, 0.0, 0.0)
    s_r = sigma_per_min / stop_pct
    # FLOOR, not round: expected R rises with sigma/stop at a fixed z, so the
    # quantisation error always under-states the edge.
    s_q = math.exp(math.floor(math.log(s_r) / S_R_STEP) * S_R_STEP)
    c_q = round(cost_r / COST_R_STEP) * COST_R_STEP
    drift_steps = int(max(0.0, drift_minutes) / step_min)
    pol = tuple(sorted(policy.items()))
    key = (round(s_q, 9), drift_steps, round(c_q, 4), target_r, n_paths, step_min, seed, pol)
    cur = _CURVE_CACHE.get(key)
    if cur is None:
        cur = _curve(s_q, drift_steps, c_q, target_r, n_paths, step_min, seed, pol)
        if len(_CURVE_CACHE) > 4096:
            _CURVE_CACHE.clear()
        _CURVE_CACHE[key] = cur
    ev, pw, aw, al, hold = cur
    z = min(max(drift_per_min / sigma_per_min, 0.0), float(Z_GRID[-1])) if drift_per_min > 0 else 0.0

    def at(a: np.ndarray) -> float:
        return float(np.interp(z, Z_GRID, a))

    if drift_per_min < 0:
        # adverse forecast: never credited as better than no drift
        return PolicyEV(-conservative_margin_r - abs(drift_per_min) * drift_minutes / stop_pct,
                        float(pw[0]), float(aw[0]), float(al[0]), float(hold[0]))
    return PolicyEV(at(ev) - float(ev[0]) - conservative_margin_r, at(pw), at(aw), at(al), at(hold))


def liquidity_grade(spread_bps: float) -> str:
    if spread_bps <= 2.0:
        return "GOOD"
    if spread_bps <= 5.0:
        return "FAIR"
    return "POOR"


@dataclass
class TradeDecision:
    symbol: str
    direction: str
    decision: str                        # TRADE | NO_TRADE
    reasons: List[str]
    horizon: str = "-"
    horizon_sec: int = 0
    signal_score: float = 50.0
    agreement: float = 50.0
    expected_move_bps: float = 0.0       # gross expectation per trade, before costs
    cost_bps: float = 0.0
    net_edge_bps: float = 0.0
    cost_breakdown: Dict[str, float] = field(default_factory=dict)
    p_win: float = 0.0
    avg_win_r: float = 0.0
    avg_loss_r: float = 0.0
    exp_hold_min: float = 0.0
    volatility: str = "NORMAL"
    regime: str = "WARMING_UP"
    liquidity: str = "GOOD"
    spread_bps: float = 0.0
    sigma_1m: float = 0.0
    rr: float = 3.0
    stop_pct: float = 0.0
    notional: float = 0.0
    risk_pct: float = 0.0
    measured_edge_bps: Optional[float] = None
    measured_n: int = 0
    quality_score: float = 0.0
    p_cal: float = 0.5
    ts: float = 0.0

    @property
    def is_trade(self) -> bool:
        return self.decision == "TRADE"

    def as_dict(self) -> Dict[str, object]:
        return asdict(self)

    def render(self) -> str:
        lines = [
            f"{self.symbol} {self.direction}  [{self.horizon} horizon]",
            f"SIGNAL              {self.signal_score:5.0f}/100",
            f"MULTI-TF AGREEMENT  {self.agreement:5.0f}/100",
            f"EXPECTED MOVE       {self.expected_move_bps:+6.1f} bps   (P(win) {self.p_win:.0%}, "
            f"avg win {self.avg_win_r:+.2f}R / loss {self.avg_loss_r:+.2f}R)",
            f"EST. COST           {-self.cost_bps:+6.1f} bps",
            f"NET EDGE            {self.net_edge_bps:+6.1f} bps",
        ]
        if self.measured_edge_bps is not None:
            lines.append(f"MEASURED EDGE       {self.measured_edge_bps:+6.1f} bps   ({self.measured_n} similar trades)")
        lines += [
            f"VOLATILITY          {self.volatility}",
            f"REGIME              {self.regime}",
            f"LIQUIDITY           {self.liquidity} ({self.spread_bps:.1f} bps spread)",
            f"RISK/REWARD         1:{self.rr:g}   stop {self.stop_pct:.2%}",
            f"QUALITY             {self.quality_score:5.0f}/100",
            f"DECISION            {self.decision.replace('_', ' ')}",
        ]
        if self.is_trade:
            lines.append(f"SIZE                ${self.notional:.2f}  (risk {self.risk_pct:.2%} of equity)")
        if self.reasons:
            lines.append(f"REASON              {self.reasons[0]}")
            for r in self.reasons[1:4]:
                lines.append(f"                    {r}")
        return "\n".join(lines)


def quality_score(agreement: float, net_edge_bps: float, cost_bps: float,
                  liquidity: str, regime_ok: bool, measured_edge_bps: Optional[float]) -> float:
    """
    0-100 composite, for ranking and audit. Weights: net edge relative to cost
    40, multi-timeframe agreement 25, measured edge in similar conditions 20,
    liquidity 10, regime 5. It is a summary; the hard gates decide.
    """
    edge_part = max(0.0, min(1.0, net_edge_bps / max(cost_bps, 1.0)))
    agree_part = max(0.0, min(1.0, (agreement - 50.0) / 40.0))
    if measured_edge_bps is None:
        meas_part = 0.25
    else:
        meas_part = max(0.0, min(1.0, 0.5 + measured_edge_bps / 40.0))
    liq_part = {"GOOD": 1.0, "FAIR": 0.5}.get(liquidity, 0.0)
    return round(100 * (0.40 * edge_part + 0.25 * agree_part + 0.20 * meas_part
                        + 0.10 * liq_part + 0.05 * (1.0 if regime_ok else 0.0)), 1)
