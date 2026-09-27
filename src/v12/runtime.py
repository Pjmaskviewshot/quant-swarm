"""
V12 RUNTIME — the single object main.py talks to.

    tick   -> on_tick()      forecaster + regime learn; journal records paths;
                             guardian sees spread and data age
    signal -> evaluate()     decision pipeline + guardian -> V12Verdict
    fill   -> on_fill()      journal opens the trade with its decision card
    close  -> on_settle()    journal, learning loop, health monitor, guardian
    timer  -> health_text()  expectancy-first dashboard for Telegram

V12_MODE
    enforce  V12 decides: an entry needs a TRADE decision AND a guardian pass;
             V12's stop and size are used (size never above the legacy size).
    shadow   V12 decides and journals everything, blocks nothing. For A/B.
    off      not constructed at all.
Default: enforce. In LIVE the guardian additionally requires an approved
paper-trading promotion record (see promotion.py); without one, LIVE entries
are refused. That is the owner's rule -- 100+ paper trades, analysed and
validated, before real capital -- made mechanical.

Thread safety: decisions run in a worker thread (a first-time EV curve costs
up to ~0.5 s and must not stall the event loop), so pipeline access is
serialised by a lock; the tick path never waits on it -- a tick that arrives
while a decision holds the lock is simply not sampled.
"""
from __future__ import annotations

import logging
import math
import os
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from core.intelligent_exit import EXIT_CONFIG
from v12.edge import TradeDecision
from v12.exit_optimizer import load_approved_exit_config
from v12.guardian import Guardian, GuardianConfig, GuardDecision
from v12.health import StrategyHealthMonitor
from v12.journal import TradeJournal
from v12.pipeline import DecisionPipeline, PipelineConfig
from v12.risk import RiskConfig

log = logging.getLogger("v12")


def resolve_mode() -> str:
    m = os.getenv("V12_MODE", "enforce").strip().lower()
    return m if m in ("enforce", "shadow", "off") else "enforce"


@dataclass
class V12Verdict:
    allowed: bool                 # may the entry proceed (always True in shadow)
    would_allow: bool             # what V12 itself concluded
    decision: TradeDecision
    guard: GuardDecision
    stop_pct: float
    notional: float
    tp_r: float
    mode: str

    @property
    def reason(self) -> str:
        if not self.decision.is_trade:
            return self.decision.reasons[0] if self.decision.reasons else "NO TRADE"
        if not self.guard.allowed:
            return f"{self.guard.level}: {self.guard.reason}"
        return "TRADE"

    def card(self) -> str:
        text = self.decision.render()
        if self.decision.is_trade and not self.guard.allowed:
            text += f"\nGUARDIAN            BLOCKED {self.guard.level}: {self.guard.reason}"
        if self.mode == "shadow":
            text += "\n(shadow mode: V12 did not control this entry)"
        return text


class V12Runtime:
    def __init__(self, trading_mode: str = "PAPER", mode: Optional[str] = None,
                 journal_path: Optional[str] = None):
        self.mode = mode or resolve_mode()
        self.trading_mode = str(trading_mode).upper()
        approved = load_approved_exit_config(os.getenv("EXIT_POLICY_FILE"))
        self.exit_cfg = approved or EXIT_CONFIG
        self.exit_policy_source = "approved candidate file" if approved else "built-in default"
        self.pipe = DecisionPipeline(PipelineConfig(), risk=RiskConfig.from_env(), exit_cfg=self.exit_cfg)
        self.guard = Guardian(GuardianConfig.from_env(), mode=self.trading_mode)
        self.health = StrategyHealthMonitor()
        self.journal = TradeJournal(journal_path, mode=self.trading_mode)
        self._lock = threading.Lock()
        self._open: Dict[str, TradeDecision] = {}
        self.counts: Dict[str, int] = {"evaluated": 0, "trade": 0, "no_trade": 0, "guard_block": 0}
        self._last_price: Dict[str, float] = {}
        self._last_resolve: Dict[str, float] = {}
        self._reload_history()

    def _reload_history(self) -> None:
        """Rebuild the learning loop and health monitor from the journal, so a
        restart does not reset measured edge to 'unknown' (or forget losses)."""
        try:
            for t in self.journal.closed_trades(mode=self.trading_mode):
                if t.get("net_return_bps") is None:
                    continue
                if t.get("expected_edge_bps") is not None:
                    self.pipe.edge_model.record(t.get("regime") or "UNKNOWN", t["expected_edge_bps"],
                                                t["net_return_bps"])
                notional = (t.get("entry_price") or 0) * (t.get("qty") or 0)
                fee_bps = (t["fees"] / notional * 1e4) if t.get("fees") is not None and notional > 0 else 0.0
                self.health.record_trade(t.get("net_pnl") or 0.0, t["net_return_bps"], t.get("r_multiple") or 0.0,
                                         fee_bps, t.get("expected_edge_bps") or 0.0, t.get("mfe_r"), t.get("mae_r"))
        except Exception as exc:     # a corrupt journal must not stop startup
            log.warning("V12 history reload failed: %s", exc)

    # ------------------------------------------------------------------ inputs
    def on_tick(self, symbol: str, now: float, price: float, spread_bps: Optional[float] = None,
                data_age_sec: Optional[float] = 0.0) -> None:
        if not (price and price > 0 and math.isfinite(price)):
            return
        # Never stall the event loop behind a decision computing a fresh EV curve
        # (up to ~0.5 s the first time a volatility cell is seen). The forecaster
        # samples every 10 s, so a skipped tick costs nothing.
        if self._lock.acquire(blocking=False):
            try:
                self.pipe.observe(symbol, now, price)
            finally:
                self._lock.release()
        else:
            self.counts["ticks_skipped_busy"] = self.counts.get("ticks_skipped_busy", 0) + 1
        self._last_price[symbol] = price
        self.journal.on_price(symbol, now, price)
        if spread_bps is not None and math.isfinite(spread_bps):
            self.guard.observe_spread(symbol, spread_bps)
        self.guard.set_data_age(symbol, data_age_sec)
        if now - self._last_resolve.get(symbol, 0.0) >= 30.0:
            self._last_resolve[symbol] = now
            self.journal.resolve_decisions(symbol, now, price)

    def warm_start(self, symbol: str, bars: List[Tuple[float, float]]) -> int:
        """Replay closed 1-minute bars [(close_ts, close)] in order. Only bars older
        than anything already observed are used, so live data is never overwritten."""
        n = 0
        with self._lock:
            fc = self.pipe._fc(symbol)
            first_live = fc.hist[0][0] if fc.hist else float("inf")
            for ts, px in bars:
                if ts >= first_live:
                    break
                # each bar stands for 6 ten-second samples; replaying the close once per
                # minute under-counts independent outcomes, which is the safe direction
                self.pipe.observe(symbol, ts, px)
                n += 1
        return n

    def set_equity(self, equity: Optional[float]) -> None:
        self.guard.set_equity(equity)

    def set_positions(self, positions: Dict[str, Tuple[str, float]]) -> None:
        self.guard.set_open_positions(positions)

    def set_reconciliation(self, ok: bool, detail: str) -> None:
        self.guard.set_reconciliation(ok, detail)

    def record_api(self, ok: bool) -> None:
        self.guard.record_api(ok)

    # ------------------------------------------------------------------ decision
    def evaluate(self, symbol: str, direction: str, now: float, equity: float, spread_bps: float,
                 funding_rate_8h: float = 0.0, min_notional: Optional[float] = None) -> V12Verdict:
        with self._lock:
            dec = self.pipe.decide(symbol, now, equity=equity, spread_bps=spread_bps, direction=direction,
                                   funding_rate_8h=funding_rate_8h, min_notional=min_notional)
        g = self.guard.evaluate_entry(symbol, direction, dec.notional if dec.is_trade else 0.0, now)
        would = dec.is_trade and g.allowed
        self.counts["evaluated"] += 1
        self.counts["trade" if dec.is_trade else "no_trade"] += 1
        if dec.is_trade and not g.allowed:
            self.counts["guard_block"] += 1
        self.journal.record_decision(dec, self._last_price.get(symbol, 0.0))
        allowed = would if self.mode == "enforce" else True
        return V12Verdict(allowed, would, dec, g, dec.stop_pct, dec.notional, self.exit_cfg.min_reward_r, self.mode)

    # ------------------------------------------------------------------ lifecycle
    def on_fill(self, signal_id: str, verdict: V12Verdict, fill_ts: float, fill_price: float,
                qty: float) -> None:
        self._open[signal_id] = verdict.decision
        self.journal.open_trade(signal_id, verdict.decision, fill_ts, fill_price, qty)

    def on_settle(self, signal_id: str, exit_ts: float, exit_price: Optional[float], net_pnl: Optional[float],
                  fees: Optional[float], exit_reason: str, mfe_r: Optional[float] = None,
                  mae_r: Optional[float] = None) -> Optional[Dict[str, object]]:
        dec = self._open.pop(signal_id, None)
        res = self.journal.close_trade(signal_id, exit_ts, exit_price or 0.0, net_pnl, fees, exit_reason,
                                       mfe_r=mfe_r, mae_r=mae_r)
        if not res or res.get("net_return_bps") is None:
            return res
        net_bps = float(res["net_return_bps"])
        pred = dec.net_edge_bps if dec else 0.0
        if dec is not None:
            with self._lock:
                self.pipe.record_outcome(dec, net_bps)
        notional_fee_bps = 0.0
        self.health.record_trade(float(net_pnl), net_bps, float(res.get("r_multiple") or 0.0),
                                 notional_fee_bps, pred, mfe_r, mae_r)
        self.guard.record_trade(float(net_pnl))
        rep = self.health.report(self.guard.equity)
        if rep.status == "HALT" and self.mode == "enforce":
            self.guard.set_strategy_halt(rep.reasons[0])
        return {**res, "health": rep.status}

    # ------------------------------------------------------------------ reporting
    def health_text(self) -> str:
        rep = self.health.report(self.guard.equity)
        c = self.counts
        head = (f"V12 {self.mode.upper()} // {self.trading_mode}\n"
                f"Decisions {c['evaluated']}: trade {c['trade']}, no-trade {c['no_trade']}, "
                f"guardian blocks {c['guard_block']}\n"
                f"Exit policy: {self.exit_policy_source}\n")
        top = self.journal.decision_counts(since=time.time() - 86400)
        tail = ""
        if top:
            tail = "\nTop rejection reasons (24h):\n" + "\n".join(f"  {v:5d}  {k}" for k, v in list(top.items())[:5])
        g = self.guard.snapshot()
        guard = (f"\nGuardian: recon {'OK' if g['reconcile_ok'] else g['reconcile_detail']}, "
                 f"API errors {g['api_error_rate']:.0%}, open {g['open_positions']}, "
                 f"promotion {'APPROVED' if g['promotion_approved'] else 'not approved'}")
        return head + rep.render(self.trading_mode) + guard + tail
