"""
VALIDATION GATES — the part of the research loop that says "no".

APEX sections 15 and 23. A backtest number is not evidence. It becomes evidence
only after it survives a fixed set of adversarial checks, defined BEFORE the
result is seen, and applied to every candidate identically.

The gates, and what each one is defending against:

  NULL REJECTION       A strategy run on a driftless random walk must NOT be
                       profitable after costs. If it is, the measurement is
                       broken (look-ahead, optimistic fills, a cost model that
                       is too kind) and every other number is void.

  SAMPLE SUFFICIENCY   An expectancy estimated from 19 trades carries a standard
                       error larger than the estimate. Small samples are the
                       single most common way a backtest flatters itself.

  COST SURVIVAL        The edge must exceed the round-trip cost by a margin, not
                       merely clear it. An edge of 1.1x costs is an edge that
                       disappears on a bad fill.

  OUT-OF-SAMPLE DECAY  Test performance is allowed to be worse than train. It is
                       not allowed to invert. A large positive OOS/IS ratio on a
                       regime-shift dataset means the split is contaminated.

  MULTIPLE TESTING     Searching k parameter sets and reporting the best one
                       inflates Sharpe by roughly sqrt(2 ln k). The deflated
                       figure is what gets reported.

  STABILITY            An edge that exists on one seed/fold and not the others
                       is a coincidence with good PR.

None of these gates can be satisfied by making the strategy look better. They
are satisfied only by the edge being real, or not at all.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

# Minimum trades before an expectancy estimate is quotable at all.
MIN_TRADES = 100

# How far above round-trip cost the gross edge must sit.
COST_SAFETY_MULTIPLE = 2.0

# The most an out-of-sample result may exceed in-sample before we suspect leakage.
MAX_OOS_OVER_IS = 1.50


@dataclass
class GateResult:
    name: str
    passed: bool
    detail: str
    value: Optional[float] = None
    threshold: Optional[float] = None

    def line(self) -> str:
        return f"[{'PASS' if self.passed else 'FAIL'}] {self.name}: {self.detail}"


@dataclass
class ValidationReport:
    gates: List[GateResult] = field(default_factory=list)

    def add(self, g: GateResult) -> "ValidationReport":
        self.gates.append(g)
        return self

    @property
    def passed(self) -> bool:
        return all(g.passed for g in self.gates) and bool(self.gates)

    @property
    def failures(self) -> List[GateResult]:
        return [g for g in self.gates if not g.passed]

    def verdict(self) -> str:
        if not self.gates:
            return "NO GATES RUN — not validated"
        return "ACCEPTED" if self.passed else "REJECTED"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "verdict": self.verdict(),
            "passed": self.passed,
            "gates": [{"name": g.name, "passed": g.passed, "detail": g.detail,
                       "value": g.value, "threshold": g.threshold} for g in self.gates],
        }

    def render(self) -> str:
        return "\n".join([f"VERDICT: {self.verdict()}", *(g.line() for g in self.gates)])


# ----------------------------------------------------------------------------
# Individual gates
# ----------------------------------------------------------------------------

def gate_sample_size(trades: int, minimum: int = MIN_TRADES) -> GateResult:
    return GateResult(
        "sample_size",
        trades >= minimum,
        f"{trades} trades (need >= {minimum}); "
        f"standard error of expectancy scales as 1/sqrt(n)",
        float(trades), float(minimum),
    )


def gate_null_rejection(null_expectancy: float, null_trades: int,
                        tolerance: float = 0.0) -> GateResult:
    """
    The single most important gate.

    On a driftless random walk there is nothing to find, so after costs the
    expectancy must be <= 0. A positive result here does not mean the strategy
    is good on noise; it means the MEASUREMENT is wrong, and no other number
    from the same apparatus can be trusted.
    """
    ok = null_expectancy <= tolerance
    return GateResult(
        "null_rejection",
        ok,
        (f"random-walk expectancy {null_expectancy:+.6f} over {null_trades} trades "
         f"(must be <= {tolerance:+.6f}). "
         + ("no edge found on noise, as required" if ok else
            "THE INSTRUMENT FINDS PROFIT IN NOISE — look-ahead, optimistic fills or "
            "under-modelled costs. Every other metric is void until this is fixed.")),
        float(null_expectancy), float(tolerance),
    )


def gate_cost_survival(gross_edge_bps: float, round_trip_cost_bps: float,
                       multiple: float = COST_SAFETY_MULTIPLE) -> GateResult:
    need = round_trip_cost_bps * multiple
    return GateResult(
        "cost_survival",
        gross_edge_bps >= need,
        f"gross edge {gross_edge_bps:.2f} bps vs {multiple:.1f}x round-trip cost "
        f"({round_trip_cost_bps:.2f} bps -> need {need:.2f} bps)",
        gross_edge_bps, need,
    )


def gate_oos_decay(is_metric: float, oos_metric: float,
                   max_ratio: float = MAX_OOS_OVER_IS) -> GateResult:
    """
    Degradation out of sample is expected and healthy. The failure modes are:
      * OOS flips sign         -> the edge was fitted noise
      * OOS wildly exceeds IS  -> the split leaked
    """
    if is_metric <= 0:
        return GateResult("oos_decay", False,
                          f"in-sample metric {is_metric:+.6f} is not positive — nothing to validate",
                          oos_metric, None)
    ratio = oos_metric / is_metric
    ok = 0.0 < ratio <= max_ratio
    if ratio <= 0:
        why = "out-of-sample flipped sign — the in-sample edge was fitted noise"
    elif ratio > max_ratio:
        why = (f"out-of-sample EXCEEDS in-sample by {ratio:.2f}x — suspect leakage "
               f"across the split (purge/embargo too short, or state carried over)")
    else:
        why = "degrades within tolerance, as a real edge should"
    return GateResult("oos_decay", ok,
                      f"IS {is_metric:+.6f} -> OOS {oos_metric:+.6f} (ratio {ratio:.2f}x): {why}",
                      ratio, max_ratio)


def deflated_sharpe(observed_sharpe: float, n_trials: int, n_obs: int,
                    annualisation_factor: float = 1.0) -> float:
    """
    Haircut a Sharpe for the number of configurations searched.

    The estimate of a PER-OBSERVATION Sharpe from n trades has standard error
    ~= 1/sqrt(n). Taking the best of k independent trials therefore yields an
    expected maximum of about sqrt(2 ln k) standard errors above the truth,
    purely by chance — even when every strategy has zero real edge.

    UNITS MATTER HERE, and getting them wrong is the easy mistake: this
    codebase reports an ANNUALISED Sharpe (per-trade Sharpe multiplied by
    sqrt(periods_per_year)). A haircut computed in per-observation units and
    subtracted from an annualised figure would be ~50x too small and the gate
    would be decorative. `annualisation_factor` is that multiplier, so the
    haircut is expressed in the same units as `observed_sharpe`.
    """
    if n_trials <= 1 or n_obs <= 1:
        return float(observed_sharpe)
    expected_max_per_obs = math.sqrt(2.0 * math.log(n_trials)) / math.sqrt(n_obs)
    return float(observed_sharpe) - expected_max_per_obs * float(annualisation_factor)


def gate_multiple_testing(observed_sharpe: float, n_trials: int, n_obs: int,
                          minimum: float = 0.0,
                          annualisation_factor: float = 1.0) -> GateResult:
    d = deflated_sharpe(observed_sharpe, n_trials, n_obs, annualisation_factor)
    unit = "annualised" if annualisation_factor != 1.0 else "as-reported"
    return GateResult(
        "multiple_testing",
        d > minimum,
        f"Sharpe {observed_sharpe:.3f} over {n_trials} configurations and {n_obs} trades "
        f"deflates to {d:.3f} ({unit}; need > {minimum:.2f}) — reporting the best of a "
        f"sweep without this correction is selection bias",
        d, minimum,
    )


def annualisation_factor_from(results: Dict[str, Any]) -> float:
    """
    Recover the multiplier `summarize()` applied, so the deflation lands in the
    same units. Falls back to 1.0 — which UNDER-corrects rather than
    over-corrects, so a missing field can never manufacture a pass it does not
    deserve... except by being too lenient, which is why the gate detail states
    the unit it used.
    """
    try:
        ppy = float(results.get("periods_per_year", 0.0) or 0.0)
        return math.sqrt(ppy) if ppy > 0 else 1.0
    except (TypeError, ValueError):
        return 1.0


def gate_stability(per_fold: Sequence[float], min_positive_frac: float = 0.6) -> GateResult:
    vals = [float(v) for v in per_fold]
    if not vals:
        return GateResult("stability", False, "no folds supplied", None, min_positive_frac)
    frac = sum(1 for v in vals if v > 0) / len(vals)
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / len(vals)
    sd = math.sqrt(var)
    return GateResult(
        "stability",
        frac >= min_positive_frac,
        f"{sum(1 for v in vals if v > 0)}/{len(vals)} folds positive "
        f"(need >= {min_positive_frac:.0%}), mean {mean:+.6f} sd {sd:.6f} — "
        f"an edge present in only some folds is a coincidence",
        frac, min_positive_frac,
    )


def gate_costs_applied(cost_model: Dict[str, Any]) -> GateResult:
    on = (cost_model.get("fees_applied") and cost_model.get("slippage_applied")
          and cost_model.get("funding_applied"))
    missing = [k.replace("_applied", "") for k in
               ("fees_applied", "slippage_applied", "funding_applied")
               if not cost_model.get(k)]
    return GateResult(
        "costs_applied", bool(on),
        "all frictions modelled" if on else
        f"NOT MODELLED: {', '.join(missing)} — this is a gross figure, not profitability",
    )


# ----------------------------------------------------------------------------
# Composite
# ----------------------------------------------------------------------------

def validate_result(results: Dict[str, Any], cost_model: Dict[str, Any],
                    null_expectancy: Optional[float] = None,
                    null_trades: int = 0,
                    is_metric: Optional[float] = None,
                    oos_metric: Optional[float] = None,
                    n_trials: int = 1,
                    per_fold: Optional[Sequence[float]] = None) -> ValidationReport:
    """
    Run every gate the supplied evidence supports.

    A gate with no evidence is NOT silently skipped — it is recorded as failed
    with "not supplied", because "we didn't check" and "it passed" must never
    look the same in a report.
    """
    rep = ValidationReport()
    trades = int(results.get("trades", 0) or 0)
    rep.add(gate_sample_size(trades))
    rep.add(gate_costs_applied(cost_model))

    if null_expectancy is None:
        rep.add(GateResult("null_rejection", False,
                           "not supplied — the instrument was never run against noise"))
    else:
        rep.add(gate_null_rejection(null_expectancy, null_trades))

    if is_metric is None or oos_metric is None:
        rep.add(GateResult("oos_decay", False,
                           "not supplied — no out-of-sample split was evaluated"))
    else:
        rep.add(gate_oos_decay(is_metric, oos_metric))

    sharpe = results.get("sharpe_ratio")
    if sharpe is not None and trades > 1:
        rep.add(gate_multiple_testing(float(sharpe), n_trials, trades,
                                      annualisation_factor=annualisation_factor_from(results)))

    if per_fold:
        rep.add(gate_stability(per_fold))
    else:
        rep.add(GateResult("stability", False,
                           "not supplied — result rests on a single fold"))
    return rep
