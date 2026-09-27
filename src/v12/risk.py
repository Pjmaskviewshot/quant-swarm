"""
V12.3b — VOLATILITY- AND HORIZON-AWARE STOPS, EDGE-AND-VOLATILITY SIZING.

THE DEFECT IT REPLACES. The live stop was max(2.5 x ATR, 1.5% of price), and
the 1.5% floor won on essentially every trade -- "Stop Loss: 1.50%" on every
ticket, for DOGE in a quiet hour and SUI in a cascade alike. Size was set to a
fixed share of equity regardless of edge.

STOP. The noise a position must survive grows with how long it is held, so the
stop scales with volatility over the holding horizon:

    stop = stop_k * sigma_1m * sqrt(min(horizon, horizon_cap))

clipped to hard bounds, and never tighter than a multiple of the round-trip
cost (a stop inside the cost band is just a slow way to pay fees).

SIZE. Responds to MEASURED edge and to volatility -- never to confidence alone,
because a model can be confidently wrong:

    risk% = max_risk% * edge_scale * vol_scale
    edge_scale = clip(edge / reference_edge, 0, 1)
    vol_scale  = clip(target_sigma / sigma, min_vol_scale, 1)

`edge` is the edge measured on this account's settled trades in comparable
conditions when enough exist, and otherwise the model estimate HALVED. Notional
= equity * risk% / stop, capped by a hard notional ceiling. An order that would
fall below the exchange minimum is skipped -- never rounded up (the B1 failure).
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import List, Optional


def _env_float(raw: Optional[str], default: float) -> float:
    """Parse an environment value; missing, malformed or non-positive -> default."""
    try:
        v = float(raw) if raw not in (None, "") else default
    except ValueError:
        return default
    return v if math.isfinite(v) and v > 0 else default


@dataclass(frozen=True)
class RiskConfig:
    stop_k: float = 1.5
    stop_horizon_cap_min: float = 60.0
    min_stop_pct: float = 0.004
    max_stop_pct: float = 0.030
    min_stop_cost_multiple: float = 4.0
    max_risk_pct: float = 0.025          # hard cap: equity lost if the stop is hit
    max_notional_pct: float = 0.25       # hard cap: one position's notional / equity
    reference_edge_bps: float = 30.0     # edge at which a trade earns full size
    model_edge_trust: float = 0.5        # unmeasured model edge is halved
    target_sigma_1m: float = 0.0008
    min_vol_scale: float = 0.5
    min_notional: float = 5.0

    @staticmethod
    def from_env() -> "RiskConfig":
        return RiskConfig(
            max_risk_pct=_env_float(os.getenv("MAX_SINGLE_POSITION_RISK_PCT"), 0.025),
            max_notional_pct=_env_float(os.getenv("V12_MAX_NOTIONAL_PCT"), 0.25),
        )


def stop_distance(sigma_1m: float, horizon_min: float, cost_fraction: float,
                  cfg: RiskConfig) -> float:
    """Stop distance as a fraction of price."""
    s = cfg.stop_k * max(sigma_1m, 0.0) * math.sqrt(max(1.0, min(horizon_min, cfg.stop_horizon_cap_min)))
    s = max(s, cfg.min_stop_cost_multiple * max(cost_fraction, 0.0))
    return float(min(max(s, cfg.min_stop_pct), cfg.max_stop_pct))


@dataclass(frozen=True)
class SizeDecision:
    notional: float
    risk_pct: float
    edge_used_bps: float
    edge_source: str
    edge_scale: float
    vol_scale: float
    capped: bool
    reasons: List[str]

    @property
    def tradeable(self) -> bool:
        return self.notional > 0


def size_position(equity: float, stop_pct: float, model_edge_bps: float, sigma_1m: float,
                  cfg: RiskConfig, measured_edge_bps: Optional[float] = None,
                  measured_n: int = 0, min_measured: int = 30,
                  min_notional: Optional[float] = None) -> SizeDecision:
    reasons: List[str] = []
    if not (equity and equity > 0 and math.isfinite(equity)):
        return SizeDecision(0.0, 0.0, 0.0, "none", 0, 0, False, ["equity unknown -- fail closed"])
    if not (stop_pct and stop_pct > 0):
        return SizeDecision(0.0, 0.0, 0.0, "none", 0, 0, False, ["no stop distance"])

    if measured_edge_bps is not None and measured_n >= min_measured:
        edge, src = measured_edge_bps, f"measured over {measured_n} trades"
    else:
        edge, src = model_edge_bps * cfg.model_edge_trust, f"model x{cfg.model_edge_trust:g} (unmeasured)"
    edge_scale = max(0.0, min(1.0, edge / cfg.reference_edge_bps)) if cfg.reference_edge_bps > 0 else 0.0
    vol_scale = max(cfg.min_vol_scale, min(1.0, cfg.target_sigma_1m / sigma_1m)) if sigma_1m > 0 else cfg.min_vol_scale
    risk_pct = cfg.max_risk_pct * edge_scale * vol_scale
    if edge_scale <= 0:
        reasons.append(f"edge {edge:+.1f} bps ({src}) is not positive")
        return SizeDecision(0.0, 0.0, edge, src, edge_scale, vol_scale, False, reasons)

    notional = equity * risk_pct / stop_pct
    cap = equity * cfg.max_notional_pct
    capped = notional > cap
    if capped:
        notional = cap
        risk_pct = notional * stop_pct / equity
        reasons.append(f"notional capped at {cfg.max_notional_pct:.0%} of equity")
    floor = cfg.min_notional if min_notional is None else min_notional
    if notional < floor:
        reasons.append(f"size ${notional:.2f} below exchange minimum ${floor:.2f} -- skipped, never rounded up")
        return SizeDecision(0.0, 0.0, edge, src, edge_scale, vol_scale, capped, reasons)
    return SizeDecision(notional, risk_pct, edge, src, edge_scale, vol_scale, capped, reasons)
