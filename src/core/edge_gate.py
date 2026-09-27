"""
EDGE GATE — take only the kinds of trade that have actually paid, after costs.

WHY. The live session of Sept 2026 ran every entry through a probability
threshold, but nothing ever checked that a trade's expected edge covered its
round-trip cost (~15 bps: two taker fees plus slippage). The ticket's "Alpha
Tensor" was hard-coded to 0.0, and the formula behind it multiplied a
60-SECOND direction probability by the full 3% take-profit distance -- about a
50x overstatement of what a 60-second forecast is worth.

WHAT IT DOES. Groups trades by the model's conviction |p - 0.5| and keeps, for
each group, the realised NET return of every settled trade (closedPnl is net
of both fees and funding). It asks one question per new entry:

    Has this conviction band made money after costs, on this account?

The estimate is shrunk toward a PESSIMISTIC prior -- "no edge, pay the costs"
(-15 bps) -- with the weight of 30 trades. A band has to earn its way out of
that prior with real results before it counts as positive. Unproven is treated
as unprofitable, never the other way round.

MODES
  off      the gate never blocks
  shadow   (default) never blocks; logs and counts every entry it WOULD block,
           so the effect is visible before it is trusted
  enforce  blocks entries in bands whose shrunk expectancy is <= 0, once at
           least `min_trades_to_enforce` settled trades exist

WHAT IT CANNOT DO. It cannot create edge; it can only stop spending money on
signal types that have none. If no band is profitable, an enforced gate stops
trading altogether -- which is the correct outcome, and is stated here so it is
not mistaken for a malfunction. A band it blocks receives no new trades and so
cannot prove itself later; `reset()` clears the history for a deliberate retry.
"""
from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger("QUANT_CORE.EDGE_GATE")


@dataclass(frozen=True)
class EdgeGateConfig:
    mode: str = "shadow"
    min_trades_to_enforce: int = 100
    prior_mean_return: float = -0.0015      # -15 bps: costs paid, no edge
    prior_strength: float = 30.0            # worth 30 trades of evidence
    band_edges: Tuple[float, ...] = (0.0, 0.05, 0.10, 0.15, 0.20, 0.51)

    @staticmethod
    def from_env() -> "EdgeGateConfig":
        mode = os.getenv("EDGE_GATE_MODE", "shadow").strip().lower()
        if mode not in ("off", "shadow", "enforce"):
            logger.error(f"EDGE_GATE_MODE={mode!r} is not off|shadow|enforce; using shadow.")
            mode = "shadow"
        try:
            n = int(os.getenv("EDGE_GATE_MIN_TRADES", "100"))
        except ValueError:
            n = 100
        return EdgeGateConfig(mode=mode, min_trades_to_enforce=max(1, n))


@dataclass
class GateDecision:
    allowed: bool
    would_block: bool
    band: str
    expectancy_bps: float
    n_band: int
    n_total: int
    reason: str


class EdgeGate:
    def __init__(self, config: Optional[EdgeGateConfig] = None):
        self.cfg = config or EdgeGateConfig()
        self._sum: List[float] = [0.0] * (len(self.cfg.band_edges) - 1)
        self._n: List[int] = [0] * (len(self.cfg.band_edges) - 1)
        self.would_block_count = 0
        self.blocked_count = 0

    # ------------------------------------------------------------------ bands
    def band_index(self, conviction: float) -> int:
        c = min(max(float(conviction), 0.0), 0.5)
        edges = self.cfg.band_edges
        for i in range(len(edges) - 1):
            if edges[i] <= c < edges[i + 1]:
                return i
        return len(edges) - 2

    def band_label(self, i: int) -> str:
        e = self.cfg.band_edges
        return f"p={0.5 + e[i]:.2f}-{min(1.0, 0.5 + e[i + 1]):.2f}"

    @property
    def n_total(self) -> int:
        return sum(self._n)

    # --------------------------------------------------------------- evidence
    def record(self, conviction: Optional[float], net_return: Optional[float]) -> bool:
        """Add one settled trade. Unknown inputs are refused, never guessed."""
        if conviction is None or net_return is None:
            return False
        try:
            c, r = float(conviction), float(net_return)
        except (TypeError, ValueError):
            return False
        if not (math.isfinite(c) and math.isfinite(r)):
            return False
        i = self.band_index(c)
        self._sum[i] += r
        self._n[i] += 1
        return True

    def load(self, trades: Iterable[Tuple[Optional[float], Optional[float]]]) -> int:
        return sum(1 for c, r in trades if self.record(c, r))

    def load_from_ledger_rows(self, rows) -> int:
        """
        Rebuild from the SQLite ledger: settled, real (non-shadow) trades with a
        stored probability. Return is normalised by the INTENDED notional, which
        understates magnitude on partial fills; the sign -- which is what the
        gate decides on -- is unaffected.
        """
        loaded = 0
        for row in rows:
            try:
                if row["is_shadow"] or not row["resolved"]:
                    continue
                if str(row["settlement_status"] or "").upper() == "UNKNOWN":
                    continue
                p, pnl, notional = row["predicted_probability"], row["net_pnl"], row["target_notional"]
                if p is None or pnl is None or not notional:
                    continue
                if self.record(abs(float(p) - 0.5), float(pnl) / float(notional)):
                    loaded += 1
            except (KeyError, IndexError, TypeError, ValueError):
                continue
        return loaded

    def reset(self) -> None:
        self._sum = [0.0] * len(self._sum)
        self._n = [0] * len(self._n)

    # --------------------------------------------------------------- estimate
    def expectancy(self, i: int) -> float:
        """Shrunk mean net return for band i. Starts at the pessimistic prior."""
        k = self.cfg.prior_strength
        return (self._sum[i] + k * self.cfg.prior_mean_return) / (self._n[i] + k)

    def decide(self, conviction: float) -> GateDecision:
        i = self.band_index(conviction)
        exp = self.expectancy(i)
        would_block = exp <= 0.0
        label = self.band_label(i)
        base = (f"{label}: expectancy {exp * 1e4:+.1f} bps after costs over "
                f"{self._n[i]} settled trades (prior {self.cfg.prior_mean_return * 1e4:+.0f} bps x "
                f"{self.cfg.prior_strength:.0f})")
        if self.cfg.mode == "off":
            return GateDecision(True, False, label, exp * 1e4, self._n[i], self.n_total, "gate off")
        if would_block:
            self.would_block_count += 1
        enforcing = (self.cfg.mode == "enforce"
                     and self.n_total >= self.cfg.min_trades_to_enforce)
        if would_block and enforcing:
            self.blocked_count += 1
            return GateDecision(False, True, label, exp * 1e4, self._n[i], self.n_total,
                                "BLOCKED -- " + base)
        if would_block:
            why = ("shadow mode" if self.cfg.mode == "shadow" else
                   f"only {self.n_total}/{self.cfg.min_trades_to_enforce} settled trades")
            return GateDecision(True, True, label, exp * 1e4, self._n[i], self.n_total,
                                f"WOULD BLOCK ({why}) -- " + base)
        return GateDecision(True, False, label, exp * 1e4, self._n[i], self.n_total, "ok -- " + base)

    def snapshot(self) -> Dict[str, object]:
        return {
            "mode": self.cfg.mode,
            "settled_trades": self.n_total,
            "enforcing": (self.cfg.mode == "enforce"
                          and self.n_total >= self.cfg.min_trades_to_enforce),
            "would_block": self.would_block_count,
            "blocked": self.blocked_count,
            "bands": {
                self.band_label(i): {"n": self._n[i],
                                     "expectancy_bps": round(self.expectancy(i) * 1e4, 2)}
                for i in range(len(self._n))
            },
        }


def horizon_edge_bps(p_up: float, is_buy: bool, atr_pct: float,
                     horizon_sec: float = 60.0, bar_minutes: float = 5.0) -> float:
    """
    What a directional probability is actually worth, in bps, over the horizon
    it forecasts.

    E[signed move] = (2q - 1) * E|r_h|, where q is the probability of moving in
    the trade's direction and E|r_h| ~ sigma_h * sqrt(2/pi). sigma is recovered
    from ATR (ATR ~ 1.25 sigma per bar), which slightly OVERstates sigma -- so
    this figure errs generous, not harsh.

    This replaces `alpha_tensor_bps`, which multiplied the same probability by
    the full take-profit distance, as if a 60-second forecast predicted the
    outcome of a trade held for hours.
    """
    q = p_up if is_buy else 1.0 - p_up
    q = min(max(float(q), 0.0), 1.0)
    sigma_bar = max(float(atr_pct), 0.0) / 1.25
    sigma_h = sigma_bar * math.sqrt(max(horizon_sec, 0.0) / 60.0 / max(bar_minutes, 1e-9))
    return (2.0 * q - 1.0) * sigma_h * math.sqrt(2.0 / math.pi) * 1e4
