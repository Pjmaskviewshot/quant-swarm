"""
V12.5c — PAPER -> LIVE PROMOTION GATE.

The owner's rule: 100+ paper trades -> analyse -> revise -> validate -> only
then real capital. This module turns that into a checklist evaluated on the
journal. It never approves anything itself: it reports ELIGIBLE / NOT ELIGIBLE,
and scripts/evaluate_promotion.py writes an approval record only when a human
passes --approve --operator NAME on an ELIGIBLE result. The Guardian refuses
LIVE entries without that record.

Criteria (all must hold, all measured on PAPER/TESTNET trades, after costs):
    n >= 100 closed trades
    expectancy > 0 and t-stat >= 2.0 (per-trade net bps)
    profit factor >= 1.2
    max drawdown (of the trade equity curve) <= 10% of starting equity
    realised edge >= 30% of predicted edge (the model is not fantasy)
    realised cost per trade <= 1.5x modelled cost (execution assumption holds)
    no single symbol provides > 50% of net profit (not one lucky coin)
    the second half of the sample is also profitable (edge not decaying)
"""
from __future__ import annotations

import math
import statistics as st
from typing import Dict, List


def evaluate(trades: List[Dict], starting_equity: float, min_trades: int = 100) -> Dict[str, object]:
    t = [x for x in trades if x.get("net_return_bps") is not None and x.get("net_pnl") is not None
         and (x.get("trading_mode") or "PAPER") in ("PAPER", "TESTNET")]
    checks: List[Dict[str, object]] = []

    def chk(name: str, ok: bool, detail: str) -> None:
        checks.append({"check": name, "pass": bool(ok), "detail": detail})

    n = len(t)
    chk("sample size", n >= min_trades, f"{n} closed paper trades (need {min_trades})")
    if n >= 2:
        bps = [x["net_return_bps"] for x in t]
        m, sd = st.mean(bps), st.stdev(bps)
        tstat = m / (sd / math.sqrt(n)) if sd > 0 else 0.0
        chk("expectancy", m > 0 and tstat >= 2.0, f"{m:+.1f} bps/trade, t={tstat:.2f} (need > 0, t >= 2)")
        pnl = [x["net_pnl"] for x in t]
        gw, gl = sum(p for p in pnl if p > 0), -sum(p for p in pnl if p <= 0)
        pf = gw / gl if gl > 0 else float("inf")
        chk("profit factor", pf >= 1.2, f"{pf:.2f} (need >= 1.2)")
        eq, peak, mdd = starting_equity, starting_equity, 0.0
        for p in pnl:
            eq += p
            peak = max(peak, eq)
            mdd = max(mdd, (peak - eq) / starting_equity if starting_equity > 0 else 1.0)
        chk("max drawdown", mdd <= 0.10, f"{mdd:.1%} (limit 10%)")
        pred = [x.get("expected_edge_bps") for x in t if x.get("expected_edge_bps") is not None]
        if pred and st.mean(pred) > 0:
            real = m / st.mean(pred)
            chk("edge realisation", real >= 0.3, f"realised {real:.0%} of predicted {st.mean(pred):+.1f} bps")
        else:
            chk("edge realisation", False, "no predicted edge recorded")
        mc = [x.get("cost_bps") for x in t if x.get("cost_bps") is not None]
        fees = [x["fees"] / (x["entry_price"] * x["qty"]) * 1e4 for x in t
                if x.get("fees") is not None and x.get("entry_price") and x.get("qty")]
        if mc and fees:
            chk("execution cost", st.mean(fees) <= 1.5 * st.mean(mc),
                f"realised fees {st.mean(fees):.1f} bps vs modelled total cost {st.mean(mc):.1f}")
        else:
            chk("execution cost", False, "fees or modelled cost not recorded")
        by: Dict[str, float] = {}
        for x in t:
            by[x.get("symbol") or "?"] = by.get(x.get("symbol") or "?", 0.0) + x["net_pnl"]
        total = sum(pnl)
        top = max(by.items(), key=lambda kv: kv[1])
        chk("concentration", total > 0 and top[1] <= 0.5 * total,
            f"top symbol {top[0]} = {top[1]:+.4f} of total {total:+.4f}")
        second = bps[n // 2:]
        chk("stability", st.mean(second) > 0, f"second half {st.mean(second):+.1f} bps/trade")
    eligible = bool(checks) and all(c["pass"] for c in checks) and n >= min_trades
    return {"eligible": eligible, "n": n, "checks": checks}
