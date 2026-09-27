"""
V12.5b — STRATEGY HEALTH MONITOR.

Answers continuously: is the advantage still there, after real costs?

    STRATEGY HEALTH
    ────────────────────
    Expectancy          +0.18R  (+14.2 bps)
    Profit Factor       1.34
    Net P&L             +3.21 USDT
    Max Drawdown        1.8%
    Avg R               +0.18
    Cost/Trade          14.8 bps
    MFE Captured        54%
    Avg MAE             -0.42R
    Win Rate            47%     <- deliberately last
    Model Edge          +24.0 bps predicted  /  realised 71%
    MODEL DRIFT         NORMAL
    EXECUTION           NORMAL
    STATUS              OK

Status rules, once `min_trades` exist in the rolling window:
    HALT  expectancy is reliably negative (mean + 1 standard error < 0) and PF < 0.9
    HALT  EDGE BELOW COST: realised net edge < 0 while the model predicted a positive one
    WARN  realised edge < 30% of predicted (overconfident model)
    WARN  cost/trade rose > 50% above its baseline (execution deterioration)
    WARN  mean forecaster skill turned negative (model drift)

HALT is wired to Guardian level 2: new entries stop; open positions are still
managed. It clears only by operator reset, never by itself.
"""
from __future__ import annotations

import math
import statistics as st
from collections import deque
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class TradeStat:
    net_pnl: float
    net_bps: float
    r: float
    cost_bps: float
    predicted_bps: float
    mfe_r: Optional[float]
    mae_r: Optional[float]


@dataclass
class HealthReport:
    n: int
    expectancy_r: float
    expectancy_bps: float
    profit_factor: float
    net_pnl: float
    max_drawdown_pct: float
    cost_bps: float
    mfe_captured: Optional[float]
    avg_mae_r: Optional[float]
    win_rate: float
    avg_win_r: float
    avg_loss_r: float
    model_edge_bps: float
    realisation: Optional[float]
    model_drift: str
    execution: str
    status: str
    reasons: List[str]

    def render(self, mode: str = "") -> str:
        pf = "inf" if math.isinf(self.profit_factor) else f"{self.profit_factor:.2f}"
        lines = [
            "STRATEGY HEALTH",
            "────────────────────",
            f"Expectancy          {self.expectancy_r:+.2f}R  ({self.expectancy_bps:+.1f} bps)",
            f"Profit Factor       {pf}",
            f"Net P&L             {self.net_pnl:+.4f} USDT",
            f"Max Drawdown        {self.max_drawdown_pct:.1%}",
            f"Avg Win / Loss      {self.avg_win_r:+.2f}R / {self.avg_loss_r:+.2f}R",
            f"Cost/Trade          {self.cost_bps:.1f} bps",
            f"MFE Captured        {'n/a' if self.mfe_captured is None else f'{self.mfe_captured:.0%}'}",
            f"Avg MAE             {'n/a' if self.avg_mae_r is None else f'{self.avg_mae_r:+.2f}R'}",
            f"Win Rate            {self.win_rate:.0%}",
            f"Model Edge          {self.model_edge_bps:+.1f} bps predicted"
            + ("" if self.realisation is None else f"  /  realised {self.realisation:.0%}"),
            "",
            f"MODEL DRIFT         {self.model_drift}",
            f"EXECUTION           {self.execution}",
            f"STATUS              {self.status}{(' (' + mode + ')') if mode else ''}",
        ]
        if self.reasons:
            lines.append(f"REASON              {self.reasons[0]}")
            lines += [f"                    {r}" for r in self.reasons[1:3]]
        lines.append(f"(last {self.n} trades)")
        return "\n".join(lines)


class StrategyHealthMonitor:
    def __init__(self, window: int = 50, min_trades: int = 30, baseline_trades: int = 30):
        self.window = deque(maxlen=window)
        self.all: List[TradeStat] = []
        self.min_trades = min_trades
        self.baseline_trades = baseline_trades
        self.skill: Dict[str, float] = {}
        self.equity_curve: List[float] = []

    def record_trade(self, net_pnl: float, net_bps: float, r: float, cost_bps: float,
                     predicted_bps: float, mfe_r: Optional[float] = None,
                     mae_r: Optional[float] = None) -> None:
        vals = (net_pnl, net_bps, r, cost_bps, predicted_bps)
        if not all(math.isfinite(float(v)) for v in vals):
            return
        t = TradeStat(float(net_pnl), float(net_bps), float(r), float(cost_bps), float(predicted_bps),
                      mfe_r, mae_r)
        self.window.append(t)
        self.all.append(t)
        self.equity_curve.append((self.equity_curve[-1] if self.equity_curve else 0.0) + t.net_pnl)

    def set_model_skill(self, skill_by_horizon: Dict[str, float]) -> None:
        self.skill = dict(skill_by_horizon)

    def report(self, equity: Optional[float] = None) -> HealthReport:
        w = list(self.window)
        n = len(w)
        if n == 0:
            return HealthReport(0, 0, 0, float("nan"), 0, 0, 0, None, None, 0, 0, 0, 0, None,
                                "UNKNOWN", "UNKNOWN", "WARMING_UP",
                                [f"0/{self.min_trades} trades settled"])
        rs = [t.r for t in w]
        bps = [t.net_bps for t in w]
        wins = [t for t in w if t.net_pnl > 0]
        losses = [t for t in w if t.net_pnl <= 0]
        gw, gl = sum(t.net_pnl for t in wins), abs(sum(t.net_pnl for t in losses))
        pf = gw / gl if gl > 0 else float("inf")
        exp_bps = st.mean(bps)
        se = st.stdev(bps) / math.sqrt(n) if n > 1 else float("inf")
        pred = st.mean(t.predicted_bps for t in w)
        realisation = exp_bps / pred if pred > 1e-9 and n >= 10 else None
        cost = st.mean(t.cost_bps for t in w)
        base_cost = (st.mean(t.cost_bps for t in self.all[:self.baseline_trades])
                     if len(self.all) >= self.baseline_trades else cost)
        mfes = [t for t in wins if t.mfe_r and t.mfe_r > 0]
        mfe_cap = (st.mean(t.r for t in mfes) / st.mean(t.mfe_r for t in mfes)) if mfes else None
        maes = [t.mae_r for t in w if t.mae_r is not None]
        curve = self.equity_curve[-n:]
        peak, mdd = -math.inf, 0.0
        base_eq = equity if equity and equity > 0 else None
        for v in curve:
            peak = max(peak, v)
            if base_eq:
                mdd = max(mdd, (peak - v) / base_eq)
        skills = [v for v in self.skill.values() if v != 0.0]
        drift = "NORMAL" if not skills or st.mean(skills) >= 0 else "DRIFTING"
        execution = "NORMAL" if cost <= 1.5 * base_cost else "DETERIORATING"

        status, reasons = "OK", []
        if n < self.min_trades:
            status = "WARMING_UP"
            reasons.append(f"{n}/{self.min_trades} trades settled")
        else:
            if exp_bps + se < 0 and pf < 0.9:
                status = "HALT"
                reasons.append(f"expectancy reliably negative ({exp_bps:+.1f} bps, PF {pf:.2f})")
            if pred > 0 and exp_bps < 0:
                status = "HALT"
                reasons.append(f"EDGE BELOW COST (predicted {pred:+.1f} bps, realised {exp_bps:+.1f} bps)")
            if status != "HALT":
                if realisation is not None and realisation < 0.3:
                    status = "WARN"
                    reasons.append(f"model overconfident: realises {realisation:.0%} of predicted edge")
                if execution == "DETERIORATING":
                    status = "WARN"
                    reasons.append(f"cost/trade {cost:.1f} bps vs baseline {base_cost:.1f}")
                if drift == "DRIFTING":
                    status = "WARN"
                    reasons.append("forecaster skill turned negative")
        return HealthReport(n, st.mean(rs), exp_bps, pf, sum(t.net_pnl for t in w), mdd, cost,
                            mfe_cap, st.mean(maes) if maes else None, len(wins) / n,
                            st.mean(t.r for t in wins) if wins else 0.0,
                            st.mean(t.r for t in losses) if losses else 0.0,
                            pred, realisation, drift, execution, status, reasons)
