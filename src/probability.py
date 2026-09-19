"""
PROBABILITY REPRESENTATION
--------------------------------------------------------------------------------
Audit finding B4: `micro_models` appended `max(p_up, 1 - p_up)` to
`historical_probs`, and `intelligent_exit` read that value back as if it were
`p_up`:

    current_p_up      = probs[-1]                      # always >= 0.5
    continuation_prob = current_p_up if is_buy else (1 - current_p_up)

So for LONGS `continuation_prob >= 0.5` always, making EARLY_FLOW_OPPOSITION
(`< 0.38`) and ALPHA_DRIFT_INVERSION (`< 0.42`) unreachable; for SHORTS it is
`<= 0.5` always, so both fire on almost any adverse tick. Direction was being
inferred from a scalar maximum, which discards the sign.

The fix is representational, not a threshold change: carry a structured
estimate that states direction, confidence, horizon and calibration provenance
explicitly. Thresholds are untouched — with a correct p_up they simply now
evaluate the quantity they were always written against.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class ProbabilityEstimate:
    """
    A directional probability with everything needed to interpret it later.

    p_up          P(price up over `horizon_seconds`). Direction lives HERE.
    p_down        1 - p_up - p_flat.
    p_flat        mass assigned to "no meaningful move"; 0.0 for binary models.
    confidence    distance from indifference, in [0, 1]. This is the scalar the
                  old code conflated with p_up. It is explicitly NOT directional.
    horizon_seconds  what the probability is a probability OF. Recorded because
                  the audit found the model trained on 60s while trades ran
                  ~180min (S1); any consumer can now see the mismatch.
    calibrated / calibration_method / model_version  provenance, so an
                  uncalibrated score is never silently used as a probability.
    """
    p_up: float
    horizon_seconds: float
    p_flat: float = 0.0
    confidence: float = 0.0
    calibrated: bool = False
    calibration_method: str = "none"
    model_version: str = "unknown"
    created_at: float = field(default_factory=time.time)
    meta: Dict[str, Any] = field(default_factory=dict)

    @property
    def p_down(self) -> float:
        return max(0.0, 1.0 - self.p_up - self.p_flat)

    @property
    def direction(self) -> str:
        if self.p_flat >= max(self.p_up, self.p_down):
            return "FLAT"
        return "BUY" if self.p_up > self.p_down else "SELL"

    def continuation_prob(self, is_buy: bool) -> float:
        """
        P(the position's thesis continues). Correct for BOTH directions --
        this is the call site that B4 broke.
        """
        return self.p_up if is_buy else self.p_down

    def is_stale(self, now: Optional[float] = None, max_age_seconds: float = 30.0) -> bool:
        return ((now or time.time()) - self.created_at) > max_age_seconds

    def as_dict(self) -> Dict[str, Any]:
        return {
            "p_up": self.p_up, "p_down": self.p_down, "p_flat": self.p_flat,
            "confidence": self.confidence, "direction": self.direction,
            "horizon_seconds": self.horizon_seconds, "calibrated": self.calibrated,
            "calibration_method": self.calibration_method,
            "model_version": self.model_version, "created_at": self.created_at,
        }


def make_estimate(
    p_up: float,
    horizon_seconds: float,
    *,
    p_flat: float = 0.0,
    calibrated: bool = False,
    calibration_method: str = "none",
    model_version: str = "unknown",
    meta: Optional[Dict[str, Any]] = None,
) -> ProbabilityEstimate:
    """Build a bounded, self-consistent estimate. Non-finite input -> indifference."""
    if not math.isfinite(p_up):
        p_up = 0.5
    p_up = min(1.0, max(0.0, float(p_up)))
    p_flat = min(1.0 - p_up, max(0.0, float(p_flat)))
    confidence = abs(p_up - (1.0 - p_up - p_flat))
    return ProbabilityEstimate(
        p_up=p_up, p_flat=p_flat, confidence=confidence,
        horizon_seconds=float(horizon_seconds), calibrated=calibrated,
        calibration_method=calibration_method, model_version=model_version,
        meta=meta or {},
    )
