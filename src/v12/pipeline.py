"""
V12 DECISION PIPELINE — one decision function for the live bot AND the backtester.

    forecasts (per horizon, measured) -> regime -> for each direction x horizon:
        drift = measured expected move / horizon
        stop  = f(volatility, horizon, costs)
        EV    = runner-policy expectation under that drift (control variate)
        net   = EV x stop - (fees + spread + slippage + funding)
    -> best candidate -> hard gates -> edge-and-vol size -> TradeDecision

Hard gates (ALL must pass; every failure is reported, not just the first):
    * the chosen horizon has enough INDEPENDENT scored forecasts to be trusted
    * net edge after all costs >= min_net_edge_bps
    * multi-timeframe agreement >= min_agreement
    * regime is not CHAOTIC / still warming up
    * spread within limit
    * measured edge in this regime x edge band is not negative, once enough
      similar trades exist
    * the edge-and-vol size clears the exchange minimum

The pipeline is fail-closed: missing volatility, missing forecasts, unknown
equity -> NO TRADE with the reason stated.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from core.intelligent_exit import EXIT_CONFIG, ExitPolicyConfig
from v12.edge import CostModel, TradeDecision, liquidity_grade, policy_edge_r, quality_score
from v12.horizons import HORIZONS_SEC, MultiHorizonForecaster
from v12.learning import ConditionalEdgeModel
from v12.regime import RegimeClassifier, RegimeConfig
from v12.risk import RiskConfig, size_position, stop_distance


@dataclass(frozen=True)
class PipelineConfig:
    horizons: Sequence[int] = HORIZONS_SEC
    min_resolved: int = 30
    min_net_edge_bps: float = 5.0
    min_agreement: float = 55.0
    max_spread_bps: float = 8.0
    blocked_regimes: Sequence[str] = ("CHAOTIC", "WARMING_UP")
    min_measured_for_gate: int = 30
    slippage_buffer_pct: float = 0.0004     # must match the exit engine's cost_r
    ev_paths: int = 1500
    ev_step_min: float = 1.0
    sample_every_sec: float = 10.0


class DecisionPipeline:
    def __init__(self, cfg: Optional[PipelineConfig] = None, risk: Optional[RiskConfig] = None,
                 costs: Optional[CostModel] = None, exit_cfg: Optional[ExitPolicyConfig] = None,
                 external_dim: int = 0, regime_cfg: Optional[RegimeConfig] = None):
        self.cfg = cfg or PipelineConfig()
        self.risk = risk or RiskConfig()
        self.costs = costs or CostModel()
        self.exit_cfg = exit_cfg or EXIT_CONFIG
        self.external_dim = external_dim
        self.regime_cfg = regime_cfg
        self.forecasters: Dict[str, MultiHorizonForecaster] = {}
        self.regimes: Dict[str, RegimeClassifier] = {}
        self.edge_model = ConditionalEdgeModel()

    # ------------------------------------------------------------------ data
    def _fc(self, symbol: str) -> MultiHorizonForecaster:
        f = self.forecasters.get(symbol)
        if f is None:
            f = MultiHorizonForecaster(self.cfg.horizons, self.cfg.sample_every_sec,
                                       external_dim=self.external_dim)
            self.forecasters[symbol] = f
        return f

    def _rg(self, symbol: str) -> RegimeClassifier:
        r = self.regimes.get(symbol)
        if r is None:
            r = RegimeClassifier(self.regime_cfg)
            self.regimes[symbol] = r
        return r

    def observe(self, symbol: str, now: float, price: float,
                features: Optional[Sequence[float]] = None) -> None:
        self._fc(symbol).observe(now, price, features)
        self._rg(symbol).update(now, price)

    # ------------------------------------------------------------------ decision
    def decide(self, symbol: str, now: float, equity: float, spread_bps: float,
               direction: Optional[str] = None, funding_rate_8h: float = 0.0,
               min_notional: Optional[float] = None) -> TradeDecision:
        c, xc = self.cfg, self.exit_cfg
        fc = self._fc(symbol)
        reading = self._rg(symbol).read()
        sigma = fc.sigma_1m()
        dirs = [direction.upper()] if direction else ["BUY", "SELL"]
        base = dict(symbol=symbol, ts=now, regime=reading.label, volatility=reading.volatility,
                    spread_bps=spread_bps, liquidity=liquidity_grade(spread_bps),
                    rr=xc.min_reward_r, sigma_1m=sigma)

        if not (sigma > 0 and math.isfinite(sigma)):
            return TradeDecision(direction=dirs[0], decision="NO_TRADE",
                                 reasons=["VOLATILITY NOT YET MEASURED"], **base)
        forecasts = fc.forecast()
        mature = {h: f for h, f in forecasts.items() if f.resolved >= c.min_resolved}
        if not mature:
            most = max((f.resolved for f in forecasts.values()), default=0)
            return TradeDecision(direction=dirs[0], decision="NO_TRADE",
                                 reasons=[f"FORECASTS NOT YET MEASURED ({most}/{c.min_resolved} "
                                          f"independent outcomes on the best horizon)"], **base)

        best = None
        for d in dirs:
            is_buy = d == "BUY"
            for h, f in mature.items():
                move = f.expected_move(is_buy)
                if move <= 0:
                    continue
                h_min = h / 60.0
                pre = self.costs.breakdown_bps(spread_bps, sigma, min(max(h_min, 10), 240),
                                               funding_rate_8h, is_buy)
                stop = stop_distance(sigma, h_min, pre["total"] / 1e4, self.risk)
                cost_r = (2 * self.costs.taker_fee + c.slippage_buffer_pct) / stop
                pe = policy_edge_r(move / h_min, sigma, stop, h_min, target_r=xc.min_reward_r,
                                   cost_r=cost_r, be_trigger_r=xc.be_trigger_r,
                                   trail_start_r=xc.trail_start_r, trail_distance_r=xc.trail_distance_r,
                                   stagnation_min=xc.stagnation_minutes, stagnation_r=xc.stagnation_r,
                                   horizon_min=xc.horizon_minutes, n_paths=c.ev_paths,
                                   step_min=c.ev_step_min)
                cb = self.costs.breakdown_bps(spread_bps, sigma, pe.exp_hold_min, funding_rate_8h, is_buy)
                gross = pe.ev_r * stop * 1e4
                net = gross - cb["total"]
                if best is None or net > best["net"]:
                    best = dict(d=d, h=h, f=f, stop=stop, pe=pe, cb=cb, gross=gross, net=net)

        if best is None:
            f0 = max(mature.values(), key=lambda f: abs(f.p_cal - 0.5))
            d0 = dirs[0] if direction else ("BUY" if f0.p_cal >= 0.5 else "SELL")
            return TradeDecision(direction=d0, decision="NO_TRADE",
                                 reasons=["NO MEASURED HORIZON FORECASTS A MOVE IN THIS DIRECTION"], **base)

        d, f, pe = best["d"], best["f"], best["pe"]
        is_buy = d == "BUY"
        agree = fc.agreement(is_buy, forecasts, c.min_resolved)
        q = f.p_cal if is_buy else 1 - f.p_cal
        signal = 50 + 50 * max(-1.0, min(1.0, (2 * q - 1) / 0.2))
        measured, mn = self.edge_model.measured(reading.structure, best["net"])

        reasons: List[str] = []
        if best["net"] < c.min_net_edge_bps:
            reasons.append(f"EDGE DOES NOT CLEAR COST (expected {best['gross']:+.1f} bps vs cost "
                           f"{best['cb']['total']:.1f} bps; need net >= {c.min_net_edge_bps:g})")
        if agree < c.min_agreement:
            reasons.append(f"TIMEFRAMES DISAGREE (agreement {agree:.0f}/100 < {c.min_agreement:g})")
        if reading.structure in c.blocked_regimes:
            reasons.append(f"REGIME {reading.structure} -- not traded")
        if spread_bps > c.max_spread_bps:
            reasons.append(f"SPREAD {spread_bps:.1f} bps > {c.max_spread_bps:g}")
        if mn >= c.min_measured_for_gate and measured <= 0:
            reasons.append(f"MEASURED EDGE IN {reading.structure} IS NEGATIVE "
                           f"({measured:+.1f} bps over {mn} similar trades)")

        size = size_position(equity, best["stop"], best["net"], sigma, self.risk,
                             measured_edge_bps=measured, measured_n=mn, min_notional=min_notional)
        if not reasons and not size.tradeable:
            reasons.extend(size.reasons or ["size not tradeable"])

        dec = TradeDecision(
            direction=d, decision="NO_TRADE" if reasons else "TRADE", reasons=reasons,
            horizon=f.name, horizon_sec=best["h"], signal_score=round(signal, 1),
            agreement=round(agree, 1), expected_move_bps=round(best["gross"], 2),
            cost_bps=round(best["cb"]["total"], 2), net_edge_bps=round(best["net"], 2),
            cost_breakdown={k: round(v, 2) for k, v in best["cb"].items()},
            p_win=round(pe.p_win, 3), avg_win_r=round(pe.avg_win_r, 3), avg_loss_r=round(pe.avg_loss_r, 3),
            exp_hold_min=round(pe.exp_hold_min, 1), stop_pct=best["stop"],
            notional=round(size.notional, 2) if not reasons else 0.0,
            risk_pct=size.risk_pct if not reasons else 0.0,
            measured_edge_bps=round(measured, 2) if mn > 0 else None, measured_n=mn,
            p_cal=round(f.p_cal, 4), **base)
        dec.quality_score = quality_score(agree, best["net"], best["cb"]["total"], dec.liquidity,
                                          reading.structure not in c.blocked_regimes,
                                          measured if mn >= c.min_measured_for_gate else None)
        if dec.is_trade and size.reasons:
            dec.reasons = list(size.reasons)
        return dec

    # ------------------------------------------------------------------ learning
    def record_outcome(self, decision: TradeDecision, realised_net_bps: float) -> bool:
        return self.edge_model.record(decision.regime, decision.net_edge_bps, realised_net_bps)
