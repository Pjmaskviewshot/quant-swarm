"""
CALIBRATION AND EXIT-QUALITY ANALYSIS.

APEX sections 20-22. Two questions this system currently has no way to answer
about itself:

  1. When the model says p_up = 0.70, does the price actually rise 70% of the
     time? `micro_models` applies a Platt/temperature transform and calls the
     output calibrated, but nothing has ever measured whether it is. A model
     that is confidently wrong sizes aggressively into losses, because the same
     p feeds the Merton-Kelly allocator.

  2. Are exits leaving money on the table, or giving it back? MFE/MAE answers
     this directly: how far did the trade go in your favour before you closed,
     and how far against.

Neither can be run on real data in this environment. The functions are pure and
tested, so they run the moment trade records exist.

A NOTE ON WHAT CALIBRATION IS NOT. A well-calibrated model is not necessarily a
profitable one — a model that always outputs 0.50 on a fair coin is perfectly
calibrated and completely useless. Calibration and discrimination are separate
properties, and both are reported here, because improving one while quietly
destroying the other is a standard way to produce a better-looking number.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence


# ----------------------------------------------------------------------------
# Probability calibration
# ----------------------------------------------------------------------------

@dataclass
class CalibrationReport:
    n: int
    brier: float
    log_loss: float
    expected_calibration_error: float
    max_calibration_error: float
    base_rate: float
    mean_prediction: float
    bias: float
    resolution: float
    bins: List[Dict[str, Any]]

    def verdict(self) -> str:
        if self.n < 100:
            return f"INSUFFICIENT DATA ({self.n} observations; need >= 100)"
        parts = []
        if self.expected_calibration_error > 0.10:
            parts.append(f"POORLY CALIBRATED (ECE {self.expected_calibration_error:.3f})")
        elif self.expected_calibration_error > 0.05:
            parts.append(f"marginally calibrated (ECE {self.expected_calibration_error:.3f})")
        else:
            parts.append(f"well calibrated (ECE {self.expected_calibration_error:.3f})")

        if abs(self.bias) > 0.05:
            direction = "OVERCONFIDENT in the up direction" if self.bias > 0 else "UNDERCONFIDENT"
            parts.append(f"{direction} by {abs(self.bias):.3f}")

        if self.resolution < 0.01:
            parts.append(
                "NO DISCRIMINATION — predictions barely vary with outcome, so even "
                "perfect calibration would be worthless")
        return "; ".join(parts)

    def render(self) -> str:
        lines = [
            f"n={self.n}  base_rate={self.base_rate:.3f}  mean_pred={self.mean_prediction:.3f}",
            f"Brier={self.brier:.4f}  log-loss={self.log_loss:.4f}  "
            f"ECE={self.expected_calibration_error:.4f}  MCE={self.max_calibration_error:.4f}",
            f"bias={self.bias:+.4f}  resolution={self.resolution:.4f}",
            "",
            "reliability:",
            f"  {'bin':>12}  {'n':>6}  {'predicted':>9}  {'actual':>8}  {'gap':>8}",
        ]
        for b in self.bins:
            if b["n"] == 0:
                continue
            lines.append(
                f"  {b['lo']:.2f}-{b['hi']:.2f}  {b['n']:>6}  {b['mean_pred']:>9.3f}  "
                f"{b['actual']:>8.3f}  {b['gap']:>+8.3f}")
        lines += ["", f"VERDICT: {self.verdict()}"]
        return "\n".join(lines)


def calibration_report(predictions: Sequence[float], outcomes: Sequence[int],
                       n_bins: int = 10) -> CalibrationReport:
    """
    Score probabilistic predictions against binary outcomes.

    `predictions[i]` is P(up) in [0, 1]; `outcomes[i]` is 1 if it went up.

    Metrics, and what each one catches that the others do not:

      Brier       mean squared error. Overall accuracy. Decomposes into
                  reliability - resolution + uncertainty.
      log-loss    punishes confident errors far more harshly than Brier. A model
                  that says 0.99 and is wrong is barely penalised by Brier and
                  savaged by log-loss, which is the correct relative weighting
                  when the output drives position size.
      ECE         average |predicted - actual| across bins, weighted by bin
                  population. The headline calibration number.
      MCE         the WORST bin. A model can look fine on ECE while being badly
                  miscalibrated exactly in the high-confidence region where it
                  takes the largest positions.
      bias        mean prediction minus base rate. Systematic directional
                  overconfidence.
      resolution  variance of bin outcome rates around the base rate. How much
                  the predictions actually discriminate. Near zero means the
                  model says the same thing regardless of what happens.
    """
    p = [min(1.0, max(0.0, float(x))) for x in predictions]
    y = [1 if int(v) else 0 for v in outcomes]
    if len(p) != len(y):
        raise ValueError(f"length mismatch: {len(p)} predictions, {len(y)} outcomes")
    n = len(p)
    if n == 0:
        return CalibrationReport(0, float("nan"), float("nan"), float("nan"),
                                 float("nan"), float("nan"), float("nan"),
                                 float("nan"), float("nan"), [])

    base_rate = sum(y) / n
    mean_pred = sum(p) / n
    brier = sum((pi - yi) ** 2 for pi, yi in zip(p, y)) / n

    eps = 1e-12
    log_loss = -sum(
        yi * math.log(max(eps, pi)) + (1 - yi) * math.log(max(eps, 1.0 - pi))
        for pi, yi in zip(p, y)
    ) / n

    bins: List[Dict[str, Any]] = []
    ece = 0.0
    mce = 0.0
    resolution = 0.0
    for b in range(n_bins):
        lo, hi = b / n_bins, (b + 1) / n_bins
        idx = [i for i in range(n) if (lo <= p[i] < hi) or (b == n_bins - 1 and p[i] == 1.0)]
        if not idx:
            bins.append({"lo": lo, "hi": hi, "n": 0, "mean_pred": 0.0,
                         "actual": 0.0, "gap": 0.0})
            continue
        mp = sum(p[i] for i in idx) / len(idx)
        act = sum(y[i] for i in idx) / len(idx)
        gap = act - mp
        bins.append({"lo": lo, "hi": hi, "n": len(idx), "mean_pred": mp,
                     "actual": act, "gap": gap})
        w = len(idx) / n
        ece += w * abs(gap)
        mce = max(mce, abs(gap))
        resolution += w * (act - base_rate) ** 2

    return CalibrationReport(
        n=n, brier=brier, log_loss=log_loss,
        expected_calibration_error=ece, max_calibration_error=mce,
        base_rate=base_rate, mean_prediction=mean_pred,
        bias=mean_pred - base_rate, resolution=resolution, bins=bins,
    )


def brier_skill_score(predictions: Sequence[float], outcomes: Sequence[int]) -> float:
    """
    Brier score relative to always predicting the base rate.

    > 0  better than the naive constant forecast
    = 0  no better than guessing the base rate
    < 0  WORSE than a model that ignores every feature

    This is the number that matters. A Brier of 0.24 sounds respectable until
    you notice the base-rate forecast scores 0.2475, at which point the entire
    25-dimensional manifold is contributing almost nothing.
    """
    y = [1 if int(v) else 0 for v in outcomes]
    if not y:
        return float("nan")
    base = sum(y) / len(y)
    ref = sum((base - yi) ** 2 for yi in y) / len(y)
    if ref <= 0:
        return float("nan")
    model = calibration_report(predictions, outcomes).brier
    return 1.0 - (model / ref)


# ----------------------------------------------------------------------------
# Exit quality — MFE / MAE
# ----------------------------------------------------------------------------

@dataclass
class ExcursionReport:
    n: int
    mean_mfe: float
    mean_mae: float
    mean_realised: float
    capture_ratio: float
    portfolio_capture_ratio: float
    mean_giveback: float
    edge_ratio: float
    winners_mae: float
    losers_mfe: float

    def verdict(self) -> str:
        if self.n < 30:
            return f"INSUFFICIENT DATA ({self.n} trades)"
        out = []
        # Capture is judged on WINNERS. Computing it across all trades mixes in
        # the win rate -- any system with a meaningful share of losers shows a
        # depressed all-trade ratio and would be falsely flagged as exiting
        # late, when the real story is simply that some trades lose. Exit
        # quality is "of the favourable move a winning trade reached, how much
        # did we keep"; the all-trade figure is reported separately as
        # portfolio_capture_ratio.
        if self.capture_ratio < 0.40:
            out.append(
                f"EXITS ARE LATE — winners capture only {self.capture_ratio:.0%} of the "
                f"favourable excursion they reached; {self.mean_giveback:.4f} per winner "
                f"is reached and then handed back")
        elif self.capture_ratio > 0.85:
            out.append(
                f"EXITS MAY BE EARLY — winners capture {self.capture_ratio:.0%} of MFE, "
                f"which usually means trades are cut before the move completes")
        else:
            out.append(f"winner capture ratio {self.capture_ratio:.0%}")

        if self.losers_mfe > abs(self.mean_realised) * 1.5:
            out.append(
                f"LOSERS WENT PROFITABLE FIRST — mean MFE on losing trades is "
                f"{self.losers_mfe:.4f}. Winners are being turned into losers by the "
                f"exit rule, not by the entry")

        if self.edge_ratio < 1.0:
            out.append(
                f"edge ratio {self.edge_ratio:.2f} < 1.0 — trades move against the "
                f"position more than with it; the ENTRY timing is the problem, not the exit")
        return "; ".join(out)


def excursion_report(trades: Sequence[Dict[str, float]]) -> ExcursionReport:
    """
    Maximum Favourable / Adverse Excursion.

    Each trade needs `mfe` (best unrealised gain reached, >= 0), `mae` (worst
    unrealised loss reached, expressed as a POSITIVE magnitude) and `realised`.

    What it separates, which a win rate cannot: an entry problem from an exit
    problem. If trades routinely reach +2R and close at +0.3R, the signal is
    fine and the exit is the defect. If they go straight to -1R, the exit rule
    is irrelevant — the entries are wrong.
    """
    ts = [t for t in trades if t.get("mfe") is not None and t.get("mae") is not None]
    n = len(ts)
    if n == 0:
        nan = float("nan")
        return ExcursionReport(0, nan, nan, nan, nan, nan, nan, nan, nan, nan)

    mfe = [max(0.0, float(t["mfe"])) for t in ts]
    mae = [abs(float(t["mae"])) for t in ts]
    real = [float(t.get("realised", 0.0)) for t in ts]

    mean_mfe = sum(mfe) / n
    mean_mae = sum(mae) / n
    mean_real = sum(real) / n

    winners = [i for i in range(n) if real[i] > 0]
    losers = [i for i in range(n) if real[i] <= 0]

    win_mfe = (sum(mfe[i] for i in winners) / len(winners)) if winners else 0.0
    win_real = (sum(real[i] for i in winners) / len(winners)) if winners else 0.0

    return ExcursionReport(
        n=n,
        mean_mfe=mean_mfe,
        mean_mae=mean_mae,
        mean_realised=mean_real,
        # Exit quality, judged on winners only -- see verdict().
        capture_ratio=(win_real / win_mfe) if win_mfe > 0 else 0.0,
        # The all-trade figure, which mixes exit quality with the win rate.
        # Reported because it is what a portfolio actually keeps, but NOT used
        # to judge the exit rule.
        portfolio_capture_ratio=(mean_real / mean_mfe) if mean_mfe > 0 else 0.0,
        mean_giveback=win_mfe - win_real,
        edge_ratio=(mean_mfe / mean_mae) if mean_mae > 0 else float("inf"),
        winners_mae=(sum(mae[i] for i in winners) / len(winners)) if winners else 0.0,
        losers_mfe=(sum(mfe[i] for i in losers) / len(losers)) if losers else 0.0,
    )


def suggested_stop_from_mae(trades: Sequence[Dict[str, float]],
                            percentile: float = 0.90) -> Optional[float]:
    """
    The MAE level that would have survived `percentile` of WINNING trades.

    Read this as a diagnostic, not a parameter to copy in. It is fitted to the
    realised sample, so adopting it directly is in-sample optimisation of the
    stop — the exact move the overfitting defences elsewhere exist to prevent.
    Its legitimate use is spotting that the current stop sits at, say, the 40th
    percentile of winners' adverse excursion, which means the stop itself is
    killing most of the profitable trades.
    """
    winners = sorted(abs(float(t["mae"])) for t in trades
                     if t.get("mae") is not None and float(t.get("realised", 0.0)) > 0)
    if len(winners) < 20:
        return None
    k = min(len(winners) - 1, int(len(winners) * percentile))
    return winners[k]
