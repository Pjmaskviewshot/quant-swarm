"""
V12.4a — POST-TRADE LEARNING: under what conditions does this model make money?

Every settled trade is stored with the conditions it was taken in (regime,
volatility state, horizon, predicted net edge band, direction, liquidity). Two
consumers:

1. ConditionalEdgeModel -- the MEASURED edge the risk engine sizes on and the
   pipeline gates on. Hierarchical shrinkage, so thin cells borrow strength
   without being trusted:

       global mean     shrunk toward a pessimistic prior (costs paid, no edge)
       regime mean     shrunk toward the global mean
       cell mean       shrunk toward its regime mean

   A cell with 5 lucky trades barely moves; a cell with 200 consistent trades
   speaks for itself.

2. conditional_performance() -- the report: expectancy, profit factor, t-stat
   and a multiple-testing-adjusted significance flag for every condition, so
   "trend + good liquidity makes money" can be distinguished from the one
   lucky cell out of forty.

Also tracks EDGE REALISATION: realised net return / predicted net edge. A value
well below 1 means the model is overconfident; near zero or negative means its
edge estimates are not real. The health monitor halts on it.
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

EDGE_BANDS: Tuple[float, ...] = (-1e9, 5.0, 15.0, 30.0, 1e9)


def edge_band(predicted_net_edge_bps: float) -> str:
    for lo, hi in zip(EDGE_BANDS[:-1], EDGE_BANDS[1:]):
        if lo <= predicted_net_edge_bps < hi:
            return ("<5" if lo < -1e8 else f"{lo:g}-{hi:g}") if hi < 1e8 else f">{lo:g}"
    return ">30"


@dataclass
class _Acc:
    n: int = 0
    s: float = 0.0
    s2: float = 0.0
    pred: float = 0.0

    def add(self, r: float, pred: float) -> None:
        self.n += 1
        self.s += r
        self.s2 += r * r
        self.pred += pred

    @property
    def mean(self) -> float:
        return self.s / self.n if self.n else 0.0


class ConditionalEdgeModel:
    """Measured net edge (bps, after all costs) by regime x predicted-edge band."""

    def __init__(self, prior_bps: float = -10.0, k_global: float = 30.0,
                 k_regime: float = 20.0, k_cell: float = 20.0):
        self.prior, self.kg, self.kr, self.kc = prior_bps, k_global, k_regime, k_cell
        self.g = _Acc()
        self.regimes: Dict[str, _Acc] = defaultdict(_Acc)
        self.cells: Dict[Tuple[str, str], _Acc] = defaultdict(_Acc)

    def record(self, regime: str, predicted_net_edge_bps: float, realised_net_bps: float) -> bool:
        try:
            r, p = float(realised_net_bps), float(predicted_net_edge_bps)
        except (TypeError, ValueError):
            return False
        if not (math.isfinite(r) and math.isfinite(p)):
            return False
        reg = str(regime or "UNKNOWN").split("/")[0]
        self.g.add(r, p)
        self.regimes[reg].add(r, p)
        self.cells[(reg, edge_band(p))].add(r, p)
        return True

    def _global(self) -> float:
        return (self.g.s + self.kg * self.prior) / (self.g.n + self.kg)

    def _regime(self, reg: str) -> float:
        a = self.regimes.get(reg, _Acc())
        return (a.s + self.kr * self._global()) / (a.n + self.kr)

    def measured(self, regime: str, predicted_net_edge_bps: float) -> Tuple[float, int]:
        """
        (shrunk measured edge bps, trades in the exact cell).

        The cell is shrunk toward its regime mean computed WITHOUT the cell's
        own trades. Shrinking toward a regime mean that contains them counts
        the same evidence twice: in testing, three lucky trades forming a new
        regime came out at +22 bps. Leave-cell-out gives +13 for the same data.
        """
        reg = str(regime or "UNKNOWN").split("/")[0]
        a = self.cells.get((reg, edge_band(predicted_net_edge_bps)), _Acc())
        ra = self.regimes.get(reg, _Acc())
        g_n, g_s = self.g.n - a.n, self.g.s - a.s
        glob = (g_s + self.kg * self.prior) / (g_n + self.kg)
        reg_prior = ((ra.s - a.s) + self.kr * glob) / ((ra.n - a.n) + self.kr)
        return (a.s + self.kc * reg_prior) / (a.n + self.kc), a.n

    def realisation(self) -> Optional[float]:
        """Realised / predicted net edge over all trades. None until meaningful."""
        if self.g.n < 20 or self.g.pred <= 0:
            return None
        return self.g.s / self.g.pred

    @property
    def n(self) -> int:
        return self.g.n

    def snapshot(self) -> Dict[str, object]:
        return {
            "trades": self.g.n,
            "global_bps": round(self._global(), 2),
            "realisation": None if self.realisation() is None else round(self.realisation(), 3),
            "regimes": {k: {"n": v.n, "shrunk_bps": round(self._regime(k), 2)}
                        for k, v in self.regimes.items()},
        }


@dataclass
class ConditionRow:
    dimension: str
    value: str
    n: int
    expectancy_bps: float
    profit_factor: float
    win_rate: float
    t_stat: float
    significant: bool


def conditional_performance(trades: Sequence[Dict[str, object]],
                            dimensions: Sequence[str] = ("regime", "volatility", "horizon",
                                                         "edge_band", "direction", "liquidity"),
                            return_key: str = "net_return_bps", min_n: int = 10,
                            family_alpha_t: float = 2.0) -> List[ConditionRow]:
    """
    Expectancy and significance of every condition value. The significance bar
    is Bonferroni-adjusted for the number of cells tested -- scanning 40
    conditions and quoting the best one at t = 2 is how false edges are found.
    """
    groups: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for tr in trades:
        r = tr.get(return_key)
        if r is None:
            continue
        try:
            r = float(r)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(r):
            continue
        for d in dimensions:
            v = tr.get(d)
            if d == "edge_band" and v is None and tr.get("expected_edge_bps") is not None:
                v = edge_band(float(tr["expected_edge_bps"]))
            if v is not None:
                groups[(d, str(v))].append(r)
    cells = [(k, v) for k, v in groups.items() if len(v) >= min_n]
    m = max(1, len(cells))
    # Bonferroni on a two-sided normal test: z such that P(|Z|>z) = 0.05/m
    z = _normal_quantile(1 - 0.025 / m) if m > 1 else 1.96
    rows: List[ConditionRow] = []
    for (d, val), rs in cells:
        n = len(rs)
        mean = sum(rs) / n
        var = sum((x - mean) ** 2 for x in rs) / (n - 1) if n > 1 else 0.0
        t = mean / math.sqrt(var / n) if var > 0 else 0.0
        wins = [x for x in rs if x > 0]
        losses = [x for x in rs if x <= 0]
        pf = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else float("inf")
        rows.append(ConditionRow(d, val, n, mean, pf, len(wins) / n, t, abs(t) >= max(z, family_alpha_t)))
    rows.sort(key=lambda r: (-r.expectancy_bps))
    return rows


def _normal_quantile(p: float) -> float:
    """Acklam's rational approximation to the inverse normal CDF."""
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    pl = 0.02425
    if p < pl:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    if p > 1 - pl:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    q = p - 0.5
    r = q * q
    return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)
