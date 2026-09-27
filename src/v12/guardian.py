"""
V12.5a — GUARDIAN: the kill-switch hierarchy. Fails CLOSED.

    L1  TRADE          a bad individual trade is rejected (the decision pipeline)
    L2  STRATEGY       poor recent performance / health monitor HALT -> no new entries
    L3  DAILY LOSS     realised + unrealised loss today >= limit -> stop until next UTC day
    L4  DRAWDOWN       soft: drawdown >= pause level -> no new entries
                       hard: drawdown >= max level -> flatten (acted on by PortfolioCommander)
    L5  INFRASTRUCTURE API error rate, stale prices, abnormal spreads, position
                       mismatch with the exchange, unknown equity -> no new entries
    PROMOTION          LIVE requires a passing paper-trading promotion record

Plus exposure limits: same-direction notional per correlation cluster, and total
gross exposure. (A missing control before V12: twelve correlated alts at 2.5%
risk each is 30% directional exposure that nothing checked.)

FAIL CLOSED means: if the guardian does not KNOW that a condition is safe, it is
treated as unsafe. Equity never read -> no entries. Reconciliation never
completed -> no entries. A position the bot and the exchange disagree about ->
no entries until resolved.

Existing positions are never abandoned by the guardian: it only governs NEW
entries. Exits, stops and reconciliation keep running at every level.
"""
from __future__ import annotations

import json
import math
import os
import pathlib
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Tuple

CLUSTERS: Dict[str, Tuple[str, ...]] = {
    "BTC": ("BTCUSDT",),
    "ETH_ECO": ("ETHUSDT", "PEPEUSDT", "OPUSDT", "ARBUSDT", "LDOUSDT", "ENAUSDT", "LINKUSDT"),
    "SOL_ECO": ("SOLUSDT", "JUPUSDT", "WIFUSDT", "PYTHUSDT", "RAYUSDT", "JTOUSDT", "BONKUSDT"),
    "MEME": ("DOGEUSDT", "SHIBUSDT", "PEPEUSDT", "WIFUSDT", "BONKUSDT", "FLOKIUSDT"),
    "L1_ALT": ("ADAUSDT", "AVAXUSDT", "DOTUSDT", "NEARUSDT", "SUIUSDT", "APTUSDT", "HYPEUSDT"),
    "PAYMENTS": ("XRPUSDT", "LTCUSDT", "BCHUSDT", "XLMUSDT"),
}


def clusters_of(symbol: str) -> Tuple[str, ...]:
    """Every cluster a symbol belongs to. A symbol may sit in several (PEPE is
    both ETH-ecosystem and meme); exposure is checked against each of them.
    Unlisted symbols fall in MARKET -- crypto alts are all correlated to BTC."""
    s = symbol.upper()
    found = tuple(name for name, members in CLUSTERS.items() if s in members)
    return found or ("MARKET",)


def cluster_of(symbol: str) -> str:
    return clusters_of(symbol)[0]


def _envf(raw: Optional[str], default: float) -> float:
    """Parse an environment value; missing or malformed -> the safe default."""
    try:
        v = float(raw) if raw not in (None, "") else default
    except ValueError:
        return default
    return v if math.isfinite(v) and v > 0 else default


@dataclass(frozen=True)
class GuardianConfig:
    max_daily_loss_pct: float = 0.03
    drawdown_pause_pct: float = 0.08
    max_drawdown_pct: float = 0.15
    max_consecutive_losses: int = 6
    api_window_sec: float = 300.0
    api_error_rate_max: float = 0.30
    api_min_calls: int = 20
    max_data_age_sec: float = 5.0
    spread_anomaly_mult: float = 4.0
    max_cluster_exposure_pct: float = 0.50
    max_gross_exposure_pct: float = 1.00
    reconcile_max_age_sec: float = 120.0
    require_promotion_for_live: bool = True
    promotion_file: str = "reports/promotion/latest.json"

    @staticmethod
    def from_env() -> "GuardianConfig":
        return GuardianConfig(
            max_daily_loss_pct=_envf(os.getenv("MAX_DAILY_LOSS_PCT"), 0.03),
            drawdown_pause_pct=_envf(os.getenv("DRAWDOWN_PAUSE_PCT"), 0.08),
            max_drawdown_pct=_envf(os.getenv("MAX_DRAWDOWN_PCT"), 0.15),
            max_cluster_exposure_pct=_envf(os.getenv("MAX_CLUSTER_EXPOSURE_PCT"), 0.50),
            max_gross_exposure_pct=_envf(os.getenv("MAX_GROSS_EXPOSURE_PCT"), 1.00),
            promotion_file=os.getenv("PROMOTION_FILE", "reports/promotion/latest.json"),
        )


@dataclass
class GuardDecision:
    allowed: bool
    level: str
    reasons: List[str]

    @property
    def reason(self) -> str:
        return "; ".join(self.reasons) if self.reasons else "ok"


class Guardian:
    def __init__(self, cfg: Optional[GuardianConfig] = None, mode: str = "PAPER"):
        self.cfg = cfg or GuardianConfig()
        self.mode = mode.upper()
        self.equity: Optional[float] = None
        self.peak_equity: Optional[float] = None
        self.day_key: Optional[int] = None
        self.day_start_equity: Optional[float] = None
        self.api: Deque[Tuple[float, bool]] = deque(maxlen=5000)
        self.data_age: Dict[str, float] = {}
        self.spreads: Dict[str, Deque[float]] = {}
        self.last_spread: Dict[str, float] = {}
        self.positions: Dict[str, Tuple[str, float]] = {}
        self.reconcile_ok: Optional[bool] = None
        self.reconcile_detail = "reconciliation has not run yet"
        self.reconcile_ts = 0.0
        self.strategy_halt: Optional[str] = None
        self.consecutive_losses = 0
        self.manual_pause: Optional[str] = None
        self.promotion: Optional[Dict[str, object]] = None
        self.allow_unpromoted_live = os.getenv("ALLOW_UNPROMOTED_LIVE", "false").lower() == "true"

    # ------------------------------------------------------------------ inputs
    def set_equity(self, equity: Optional[float], now: Optional[float] = None) -> None:
        if equity is None or not math.isfinite(float(equity)):
            self.equity = None
            return
        now = time.time() if now is None else now
        e = float(equity)
        self.equity = e
        self.peak_equity = e if self.peak_equity is None else max(self.peak_equity, e)
        day = int(now // 86400)
        if day != self.day_key:
            self.day_key, self.day_start_equity = day, e

    def record_api(self, ok: bool, now: Optional[float] = None) -> None:
        self.api.append((time.time() if now is None else now, bool(ok)))

    def set_data_age(self, symbol: str, age_sec: Optional[float]) -> None:
        self.data_age[symbol] = float("inf") if age_sec is None else float(age_sec)

    def observe_spread(self, symbol: str, spread_bps: float) -> None:
        q = self.spreads.setdefault(symbol, deque(maxlen=500))
        q.append(float(spread_bps))
        self.last_spread[symbol] = float(spread_bps)

    def set_open_positions(self, positions: Dict[str, Tuple[str, float]]) -> None:
        """{symbol: (direction BUY/SELL, notional)}"""
        self.positions = dict(positions)

    def set_reconciliation(self, ok: bool, detail: str, now: Optional[float] = None) -> None:
        self.reconcile_ok = bool(ok)
        self.reconcile_detail = detail
        self.reconcile_ts = time.time() if now is None else now

    def set_strategy_halt(self, reason: Optional[str]) -> None:
        self.strategy_halt = reason

    def record_trade(self, net_pnl: float) -> None:
        self.consecutive_losses = self.consecutive_losses + 1 if net_pnl <= 0 else 0

    def pause(self, reason: str) -> None:
        self.manual_pause = reason

    def reset_strategy(self) -> None:
        """Operator action: clear L2 after reviewing why it tripped."""
        self.strategy_halt = None
        self.consecutive_losses = 0
        self.manual_pause = None

    def load_promotion(self) -> Optional[Dict[str, object]]:
        p = pathlib.Path(self.cfg.promotion_file)
        try:
            self.promotion = json.loads(p.read_text()) if p.exists() else None
        except (OSError, ValueError):
            self.promotion = None
        return self.promotion

    # ------------------------------------------------------------------ state
    def drawdown(self) -> Optional[float]:
        if self.equity is None or not self.peak_equity:
            return None
        return max(0.0, (self.peak_equity - self.equity) / self.peak_equity)

    def daily_loss(self) -> Optional[float]:
        if self.equity is None or not self.day_start_equity:
            return None
        return max(0.0, (self.day_start_equity - self.equity) / self.day_start_equity)

    def api_error_rate(self, now: float) -> Tuple[float, int]:
        recent = [ok for ts, ok in self.api if now - ts <= self.cfg.api_window_sec]
        if not recent:
            return 0.0, 0
        return 1.0 - sum(recent) / len(recent), len(recent)

    def _spread_anomalous(self, symbol: str) -> Optional[str]:
        q = self.spreads.get(symbol)
        if not q or len(q) < 30:
            return None
        s = sorted(q)
        med = s[len(s) // 2]
        cur = self.last_spread.get(symbol, med)
        if med > 0 and cur > self.cfg.spread_anomaly_mult * med:
            return f"spread {cur:.1f} bps is {cur / med:.1f}x its median {med:.1f}"
        return None

    # ------------------------------------------------------------------ decision
    def evaluate_entry(self, symbol: str, direction: str, notional: float,
                       now: Optional[float] = None) -> GuardDecision:
        now = time.time() if now is None else now
        c = self.cfg

        # PROMOTION
        if self.mode == "LIVE" and c.require_promotion_for_live:
            promo = self.promotion if self.promotion is not None else self.load_promotion()
            if not (promo and promo.get("approved") is True):
                if self.allow_unpromoted_live:
                    pass  # operator explicitly accepted the risk; logged by the caller
                else:
                    return GuardDecision(False, "PROMOTION", [
                        "LIVE trading requires an approved paper-trading promotion record "
                        f"({c.promotion_file}); run scripts/evaluate_promotion.py"])

        # L5 infrastructure -- fail closed on anything unknown
        l5: List[str] = []
        if self.equity is None:
            l5.append("equity unknown")
        if self.reconcile_ok is None:
            l5.append("reconciliation has not completed yet")
        elif not self.reconcile_ok:
            l5.append(f"position mismatch: {self.reconcile_detail}")
        elif now - self.reconcile_ts > c.reconcile_max_age_sec:
            l5.append(f"reconciliation stale ({now - self.reconcile_ts:.0f}s old)")
        age = self.data_age.get(symbol)
        if age is None or age > c.max_data_age_sec:
            l5.append(f"market data for {symbol} "
                      f"{'unknown' if age is None or age == float('inf') else f'{age:.1f}s old'}")
        rate, n = self.api_error_rate(now)
        if n >= c.api_min_calls and rate > c.api_error_rate_max:
            l5.append(f"API error rate {rate:.0%} over {n} calls")
        sp = self._spread_anomalous(symbol)
        if sp:
            l5.append(sp)
        if l5:
            return GuardDecision(False, "L5_INFRASTRUCTURE", l5)

        # L4 drawdown
        dd = self.drawdown()
        if dd is not None and dd >= c.drawdown_pause_pct:
            return GuardDecision(False, "L4_DRAWDOWN", [
                f"drawdown {dd:.1%} >= pause level {c.drawdown_pause_pct:.0%}"
                + (" (hard flatten level reached)" if dd >= c.max_drawdown_pct else "")])

        # L3 daily loss
        dl = self.daily_loss()
        if dl is not None and dl >= c.max_daily_loss_pct:
            return GuardDecision(False, "L3_DAILY_LOSS", [
                f"daily loss {dl:.1%} >= limit {c.max_daily_loss_pct:.0%}; resumes next UTC day"])

        # L2 strategy
        l2: List[str] = []
        if self.strategy_halt:
            l2.append(f"strategy halted: {self.strategy_halt}")
        if self.consecutive_losses >= c.max_consecutive_losses:
            l2.append(f"{self.consecutive_losses} consecutive losses")
        if self.manual_pause:
            l2.append(f"paused by operator: {self.manual_pause}")
        if l2:
            return GuardDecision(False, "L2_STRATEGY", l2)

        # exposure
        eq = self.equity or 0.0
        d = direction.upper()
        for cl in clusters_of(symbol):
            same = sum(n for s, (dd_, n) in self.positions.items()
                       if cl in clusters_of(s) and dd_.upper() == d and s != symbol)
            if eq > 0 and (same + notional) > c.max_cluster_exposure_pct * eq:
                return GuardDecision(False, "EXPOSURE", [
                    f"{cl} {d} exposure would be {(same + notional) / eq:.0%} of equity "
                    f"(limit {c.max_cluster_exposure_pct:.0%})"])
        gross = sum(n for s, (_, n) in self.positions.items() if s != symbol)
        if eq > 0 and (gross + notional) > c.max_gross_exposure_pct * eq:
            return GuardDecision(False, "EXPOSURE", [
                f"gross exposure would be {(gross + notional) / eq:.0%} of equity "
                f"(limit {c.max_gross_exposure_pct:.0%})"])
        return GuardDecision(True, "OK", [])

    def snapshot(self, now: Optional[float] = None) -> Dict[str, object]:
        now = time.time() if now is None else now
        rate, n = self.api_error_rate(now)
        return {
            "mode": self.mode, "equity": self.equity, "drawdown": self.drawdown(),
            "daily_loss": self.daily_loss(), "api_error_rate": rate, "api_calls": n,
            "reconcile_ok": self.reconcile_ok, "reconcile_detail": self.reconcile_detail,
            "strategy_halt": self.strategy_halt, "consecutive_losses": self.consecutive_losses,
            "manual_pause": self.manual_pause, "open_positions": len(self.positions),
            "promotion_approved": bool(self.promotion and self.promotion.get("approved")),
        }
