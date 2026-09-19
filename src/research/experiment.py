"""
EXPERIMENT RECORD — reproducible research runs.

APEX section 19. Every claimed improvement must be reproducible by someone
else, which means a result is worthless unless it is stored alongside enough
provenance to re-derive it:

    experiment id / commit sha / dirty-tree flag / dataset content hash /
    symbol / interval / bar range / parameters / model version /
    fee + slippage + funding assumptions / results / validation results

Two rules this module enforces mechanically, because they are the two ways a
research loop lies to itself:

  1. A result carries the COST MODEL that produced it. A Sharpe quoted without
     fees is not a Sharpe. `CostModel` is required, never defaulted silently,
     and is written into the record.

  2. A result carries the exact bytes it was computed on. `dataset_hash` is the
     content hash of the candles, so re-running with a moved window produces a
     different hash and the two records cannot be confused.

This module deliberately does NOT know anything about whether a strategy is
good. It records what happened. Judgement lives in `validate.py`.
"""

from __future__ import annotations

import dataclasses
import json
import os
import pathlib
import platform
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, List, Optional

from research.dataset import Dataset

DEFAULT_RESULTS_DIR = pathlib.Path(
    os.getenv("EXPERIMENT_DIR", pathlib.Path(__file__).resolve().parents[2] / "reports" / "experiments")
)


# ----------------------------------------------------------------------------
# Cost model — explicit, never implicit
# ----------------------------------------------------------------------------

@dataclass(frozen=True)
class CostModel:
    """
    The frictions a result was computed under.

    AUDIT: `backtest.py` hard-codes TAKER_FEE / MAKER_FEE / FUNDING_PER_8H /
    BASE_SLIPPAGE_BPS as module constants. That makes them invisible in the
    result, so a number produced under one assumption can be compared against a
    number produced under another without anyone noticing. Making them a
    recorded object is the fix.

    Defaults mirror the Bybit VIP-0 linear-perp schedule the live bot trades on
    (taker 5.5 bps, maker 2.0 bps) so a record is never silently free, but the
    defaults are still WRITTEN INTO THE RECORD rather than assumed at read time.
    """
    taker_fee: float = 0.00055
    maker_fee: float = 0.00020
    funding_per_8h: float = 0.0001
    base_slippage_bps: float = 4.0
    fees_applied: bool = True
    funding_applied: bool = True
    slippage_applied: bool = True

    def round_trip_cost_bps(self) -> float:
        """Taker in, taker out, plus slippage both sides. The honest floor."""
        fee_bps = self.taker_fee * 2 * 10_000 if self.fees_applied else 0.0
        slip_bps = self.base_slippage_bps * 2 if self.slippage_applied else 0.0
        return fee_bps + slip_bps

    def describe(self) -> str:
        parts = [f"taker={self.taker_fee * 10_000:.2f}bps",
                 f"maker={self.maker_fee * 10_000:.2f}bps",
                 f"funding={self.funding_per_8h * 10_000:.2f}bps/8h",
                 f"slippage={self.base_slippage_bps:.1f}bps"]
        off = [n for n, on in (("fees", self.fees_applied),
                               ("funding", self.funding_applied),
                               ("slippage", self.slippage_applied)) if not on]
        if off:
            parts.append("DISABLED:" + ",".join(off))
        return " ".join(parts)


ZERO_COST = CostModel(taker_fee=0.0, maker_fee=0.0, funding_per_8h=0.0,
                      base_slippage_bps=0.0, fees_applied=False,
                      funding_applied=False, slippage_applied=False)
"""A frictionless model. Legitimate for ONE purpose: proving that a measured
edge survives the transition to real costs, by showing the gross-vs-net gap.
A zero-cost record is flagged `costs_disabled` and `validate.py` refuses to
treat it as evidence of profitability."""


# ----------------------------------------------------------------------------
# Provenance
# ----------------------------------------------------------------------------

def _git(*args: str) -> Optional[str]:
    try:
        repo = pathlib.Path(__file__).resolve().parents[2]
        out = subprocess.run(["git", "-C", str(repo), *args],
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip() if out.returncode == 0 else None
    except Exception:
        return None


def code_revision() -> Dict[str, Any]:
    """
    Which code produced this result.

    `dirty` matters more than the sha: a result from an uncommitted tree cannot
    be reproduced by anyone, so it is recorded as provisional rather than
    quietly attributed to the last commit.
    """
    sha = _git("rev-parse", "HEAD")
    status = _git("status", "--porcelain")
    return {
        "commit": sha or "unknown",
        "branch": _git("rev-parse", "--abbrev-ref", "HEAD") or "unknown",
        "dirty": bool(status) if status is not None else None,
        "python": sys.version.split()[0],
        "platform": platform.platform(),
    }


def _numpy_version() -> str:
    try:
        import numpy
        return numpy.__version__
    except Exception:
        return "unknown"


# ----------------------------------------------------------------------------
# The record
# ----------------------------------------------------------------------------

@dataclass
class ExperimentRecord:
    """One run. Immutable once written."""
    experiment_id: str
    name: str
    hypothesis: str
    created_at: float
    revision: Dict[str, Any]

    dataset_name: str
    dataset_hash: str
    dataset_source: str
    symbol: str
    interval: str
    bars: int
    start_ts: int
    end_ts: int
    dataset_meta: Dict[str, Any]

    params: Dict[str, Any]
    model_version: str
    cost_model: Dict[str, Any]

    results: Dict[str, Any] = field(default_factory=dict)
    validation: Dict[str, Any] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)
    duration_sec: float = 0.0
    error: Optional[str] = None

    # --- honesty flags, computed not asserted ---------------------------------

    @property
    def costs_disabled(self) -> bool:
        cm = self.cost_model
        return not (cm.get("fees_applied") and cm.get("slippage_applied"))

    @property
    def reproducible(self) -> bool:
        """False for a dirty tree or an unknown commit — no one else can re-run it."""
        return (self.revision.get("commit") not in (None, "unknown")
                and self.revision.get("dirty") is False)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["costs_disabled"] = self.costs_disabled
        d["reproducible"] = self.reproducible
        return d

    def save(self, results_dir: Optional[pathlib.Path] = None) -> pathlib.Path:
        d = pathlib.Path(results_dir or DEFAULT_RESULTS_DIR)
        d.mkdir(parents=True, exist_ok=True)
        p = d / f"{self.experiment_id}.json"
        p.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True, default=str))
        return p

    @staticmethod
    def load(path: pathlib.Path) -> "ExperimentRecord":
        raw = json.loads(pathlib.Path(path).read_text())
        raw.pop("costs_disabled", None)
        raw.pop("reproducible", None)
        fields = {f.name for f in dataclasses.fields(ExperimentRecord)}
        return ExperimentRecord(**{k: v for k, v in raw.items() if k in fields})

    def summary_line(self) -> str:
        r = self.results or {}
        flag = "" if not self.costs_disabled else "  [COSTS DISABLED]"
        repro = "" if self.reproducible else "  [DIRTY TREE — not reproducible]"
        return (f"{self.experiment_id}  {self.name}  "
                f"n={r.get('trades', 0)}  "
                f"exp/trade={r.get('expectancy_per_trade', float('nan')):.6f}  "
                f"sharpe={r.get('sharpe_ratio', float('nan')):.3f}{flag}{repro}")


def new_experiment_id(name: str) -> str:
    stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    slug = "".join(c if c.isalnum() else "-" for c in name.lower())[:28].strip("-")
    return f"{stamp}_{slug}_{uuid.uuid4().hex[:6]}"


def build_record(name: str, hypothesis: str, dataset: Dataset,
                 params: Dict[str, Any], cost_model: CostModel,
                 model_version: str = "v50.0-apex") -> ExperimentRecord:
    return ExperimentRecord(
        experiment_id=new_experiment_id(name),
        name=name,
        hypothesis=hypothesis,
        created_at=time.time(),
        revision={**code_revision(), "numpy": _numpy_version()},
        dataset_name=dataset.name,
        dataset_hash=dataset.content_hash(),
        dataset_source=dataset.source,
        symbol=dataset.symbol,
        interval=dataset.interval,
        bars=len(dataset),
        start_ts=dataset.start_ts,
        end_ts=dataset.end_ts,
        dataset_meta=dict(dataset.meta),
        params=dict(params),
        model_version=model_version,
        cost_model=asdict(cost_model),
    )


# ----------------------------------------------------------------------------
# Runner
# ----------------------------------------------------------------------------

def run_experiment(
    name: str,
    hypothesis: str,
    dataset: Dataset,
    params: Dict[str, Any],
    runner: Callable[[Dataset, Dict[str, Any], CostModel], Dict[str, Any]],
    cost_model: Optional[CostModel] = None,
    model_version: str = "v50.0-apex",
    results_dir: Optional[pathlib.Path] = None,
    save: bool = True,
) -> ExperimentRecord:
    """
    Execute `runner` and record everything about it.

    `cost_model` is Optional in the signature only so callers can be explicit
    about wanting the production schedule; passing None yields `CostModel()`,
    which is the real Bybit schedule, NOT a frictionless one. There is no code
    path that silently drops costs.

    A raised exception is RECORDED, not swallowed: a failed experiment is a
    result, and an experiment log with survivorship bias in it is as dishonest
    as a backtest with survivorship bias in it.
    """
    cm = cost_model if cost_model is not None else CostModel()
    rec = build_record(name, hypothesis, dataset, params, cm, model_version)
    t0 = time.time()
    try:
        rec.results = dict(runner(dataset, params, cm) or {})
    except Exception as exc:                      # noqa: BLE001 — recorded deliberately
        rec.error = f"{type(exc).__name__}: {exc}"
        rec.notes.append("FAILED — recorded rather than discarded so the log has no survivorship bias.")
    finally:
        rec.duration_sec = time.time() - t0

    if rec.costs_disabled:
        rec.notes.append(
            "COSTS DISABLED — this record measures a gross signal, not a tradeable edge. "
            "It must not be quoted as profitability."
        )
    if not rec.reproducible:
        rec.notes.append(
            "NOT REPRODUCIBLE — working tree was dirty or the commit is unknown. "
            "Commit before quoting this result."
        )
    if save:
        rec.save(results_dir)
    return rec


def load_all(results_dir: Optional[pathlib.Path] = None) -> List[ExperimentRecord]:
    d = pathlib.Path(results_dir or DEFAULT_RESULTS_DIR)
    if not d.exists():
        return []
    out = []
    for p in sorted(d.glob("*.json")):
        try:
            out.append(ExperimentRecord.load(p))
        except Exception:
            continue
    return out


def compare(baseline: ExperimentRecord, candidate: ExperimentRecord) -> Dict[str, Any]:
    """
    A/B two records, refusing the comparison if it would be apples to oranges.

    The three ways this comparison is normally cheated:
      * different datasets (cherry-picked window)
      * different cost models (candidate measured more kindly)
      * a candidate with far fewer trades (noise dressed as improvement)
    Each is reported as a blocker rather than a footnote.
    """
    blockers: List[str] = []
    if baseline.dataset_hash != candidate.dataset_hash:
        blockers.append(
            f"different data: {baseline.dataset_hash} vs {candidate.dataset_hash} "
            f"— comparison is meaningless unless deliberate"
        )
    if baseline.cost_model != candidate.cost_model:
        blockers.append("different cost models — the candidate may simply be measured more kindly")

    b, c = baseline.results or {}, candidate.results or {}
    nb, nc = int(b.get("trades", 0)), int(c.get("trades", 0))
    if nc < 30:
        blockers.append(f"candidate has {nc} trades — too few to distinguish from noise")
    if nb and nc and (nc < nb * 0.5):
        blockers.append(f"candidate trades {nc} vs baseline {nb} — the samples are not comparable")

    deltas = {}
    for k in ("expectancy_per_trade", "sharpe_ratio", "profit_factor",
              "win_rate", "max_drawdown_on_margin", "total_return_on_margin"):
        if k in b and k in c:
            try:
                deltas[k] = float(c[k]) - float(b[k])
            except (TypeError, ValueError):
                continue

    return {
        "baseline": baseline.experiment_id,
        "candidate": candidate.experiment_id,
        "blockers": blockers,
        "comparable": not blockers,
        "deltas": deltas,
        "verdict": ("NOT COMPARABLE" if blockers else
                    "IMPROVED" if deltas.get("expectancy_per_trade", 0.0) > 0 else
                    "NOT IMPROVED"),
    }
