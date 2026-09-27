"""
V12.3a — MARKET REGIME CLASSIFIER.

Deterministic and auditable: every label comes with the measurements that
produced it, so "why did it call this a breakout?" always has an answer.

Two axes:
  STRUCTURE   TREND_UP | TREND_DOWN | RANGE | BREAKOUT | MEAN_REVERSION | CHAOTIC
  VOLATILITY  LOW | NORMAL | HIGH   (vs this symbol's own recent history)

Measurements (1-minute closes):
  efficiency ratio  |P_t - P_t-n| / sum|dP|        1 = straight line, 0 = pure chop
  trend t-stat      OLS slope of log price / its standard error
  variance ratio    Var(k-min returns) / (k * Var(1-min returns))
                    > 1 persistent (trending), < 1 anti-persistent (reverting)
  vol percentile    current 30-min realised vol within the last ~24 h
  breakout          close beyond the prior 120-min range WITH volatility expansion
  vol-of-vol        dispersion of rolling volatility -- instability

Precedence: CHAOTIC > BREAKOUT > TREND > MEAN_REVERSION > RANGE.

The classifier does not decide whether to trade. The learning loop measures how
the strategy performs IN each regime and the edge model uses that.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional

import numpy as np


@dataclass(frozen=True)
class RegimeReading:
    structure: str
    volatility: str
    efficiency: float
    trend_t: float
    variance_ratio: float
    vol_percentile: float
    vol_expansion: float
    vol_of_vol: float
    bars: int

    @property
    def label(self) -> str:
        return f"{self.structure}/{self.volatility}_VOL"

    @property
    def warm(self) -> bool:
        return self.structure != "WARMING_UP"

    def as_dict(self) -> Dict[str, object]:
        return {"structure": self.structure, "volatility": self.volatility,
                "efficiency": round(self.efficiency, 3), "trend_t": round(self.trend_t, 2),
                "variance_ratio": round(self.variance_ratio, 3),
                "vol_percentile": round(self.vol_percentile, 3),
                "vol_expansion": round(self.vol_expansion, 3),
                "vol_of_vol": round(self.vol_of_vol, 3), "bars": self.bars}


@dataclass
class RegimeConfig:
    trend_window: int = 60
    range_window: int = 120
    vol_window: int = 30
    vr_k: int = 5
    history_bars: int = 1440
    min_bars: int = 150
    er_trend: float = 0.35
    t_trend: float = 3.0
    vr_revert: float = 0.65
    breakout_expansion: float = 1.5
    chaos_vov: float = 0.60
    chaos_pct: float = 0.97
    high_vol_pct: float = 0.80
    low_vol_pct: float = 0.20


class RegimeClassifier:
    """One per symbol. Feed 1-minute closes with update(ts, close)."""

    def __init__(self, cfg: Optional[RegimeConfig] = None):
        self.cfg = cfg or RegimeConfig()
        self.closes: Deque[float] = deque(maxlen=self.cfg.history_bars + self.cfg.range_window + 5)
        self.vol_hist: Deque[float] = deque(maxlen=self.cfg.history_bars)
        self.last_ts: Optional[float] = None

    def update(self, ts: float, close: float) -> bool:
        """Accepts at most one close per minute; extra calls in the same minute are ignored."""
        if not (close > 0 and math.isfinite(close)):
            return False
        minute = math.floor(ts / 60.0)
        if self.last_ts is not None and minute <= self.last_ts:
            return False
        self.last_ts = minute
        self.closes.append(float(close))
        if len(self.closes) > self.cfg.vol_window + 1:
            r = np.diff(np.log(np.asarray(list(self.closes)[-(self.cfg.vol_window + 1):])))
            self.vol_hist.append(float(np.std(r)))
        return True

    def read(self) -> RegimeReading:
        c = self.cfg
        n = len(self.closes)
        if n < c.min_bars or len(self.vol_hist) < 30:
            return RegimeReading("WARMING_UP", "NORMAL", 0, 0, 1, 0.5, 1, 0, n)
        px = np.asarray(self.closes, float)
        lp = np.log(px)

        w = lp[-c.trend_window:]
        path = float(np.sum(np.abs(np.diff(w))))
        er = abs(w[-1] - w[0]) / path if path > 0 else 0.0
        x = np.arange(len(w), dtype=float)
        x -= x.mean()
        slope = float(np.dot(x, w - w.mean()) / np.dot(x, x))
        resid = w - (w.mean() + slope * x)
        se = math.sqrt(float(np.sum(resid ** 2)) / max(1, len(w) - 2) / float(np.dot(x, x)))
        t = slope / se if se > 0 else 0.0

        r1 = np.diff(lp[-(c.range_window + 1):])
        v1 = float(np.var(r1))
        rk = lp[-(c.range_window + 1):]
        rk = rk[c.vr_k:] - rk[:-c.vr_k]
        vr = float(np.var(rk)) / (c.vr_k * v1) if v1 > 0 else 1.0

        vh = np.asarray(self.vol_hist, float)
        cur = vh[-1]
        pct = float(np.mean(vh <= cur))
        slow = float(np.std(np.diff(lp[-(c.range_window + 1):])))
        expansion = cur / slow if slow > 0 else 1.0
        recent = vh[-120:]
        vov = float(np.std(recent) / np.mean(recent)) if np.mean(recent) > 0 else 0.0

        prior = px[-(c.range_window + 1):-1]
        broke_up = px[-1] > prior.max()
        broke_dn = px[-1] < prior.min()

        vol_state = "HIGH" if pct >= c.high_vol_pct else ("LOW" if pct <= c.low_vol_pct else "NORMAL")
        if vov >= c.chaos_vov or pct >= c.chaos_pct and er < c.er_trend:
            s = "CHAOTIC"
        elif (broke_up or broke_dn) and expansion >= c.breakout_expansion:
            s = "BREAKOUT"
        elif er >= c.er_trend and abs(t) >= c.t_trend:
            s = "TREND_UP" if slope > 0 else "TREND_DOWN"
        elif vr <= c.vr_revert:
            s = "MEAN_REVERSION"
        else:
            s = "RANGE"
        return RegimeReading(s, vol_state, er, t, vr, pct, expansion, vov, n)
