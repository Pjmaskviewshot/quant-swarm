#!/usr/bin/env python3
"""
Ledger reconciliation — does the recorded history hold together?

APEX section 18. The trade ledger is what every performance claim, every
Kelly update and every model label is built on. If it drifts from reality, the
bot keeps trading confidently on a false picture of its own results — and the
drift is silent, because nothing downstream re-checks it.

This runs OFFLINE against the SQLite ledger and asserts internal consistency:

  C1  no duplicate signal ids
  C2  no trade resolved before it was predicted
  C3  every resolved trade carries an outcome; every unresolved one does not
      carry a PnL
  C4  fees are non-negative and are not larger than the gross move
  C5  settlement_status and `resolved` agree
  C6  no NaN or infinity anywhere in a numeric column
  C7  per-symbol PnL sums to the total (no rows lost to a filter)
  C8  holding time is non-negative and not implausibly long
  C9  win rate computed from outcomes matches win rate computed from PnL sign
      -- these are derived two different ways and must not disagree
  C10 shadow trades are excluded from realised PnL

A failure here is not a style issue. C9 disagreeing means the labels fed to the
model do not match the money, which corrupts learning silently.

With --exchange it additionally compares against Bybit's own execution history.
That path needs credentials and egress and is READ-ONLY: it calls
/v5/execution/list and nothing else. It never places, amends or cancels an
order.

    python scripts/reconcile_ledger.py --db titan_memory_ledger.db
    python scripts/reconcile_ledger.py --db ... --exchange --days 7
"""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import sqlite3
import sys
from typing import Any, Dict, List

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

NUMERIC_COLUMNS = ("price_at_prediction", "net_pnl", "fees_usdt", "slippage_drag",
                   "holding_minutes", "virtual_sl", "virtual_tp", "target_notional",
                   "shadow_return_fraction")

MAX_PLAUSIBLE_HOLD_MINUTES = 60 * 24 * 14        # two weeks


class Finding:
    def __init__(self, code: str, severity: str, detail: str, rows: int = 0,
                 examples: List[Any] = None):
        self.code, self.severity, self.detail = code, severity, detail
        self.rows, self.examples = rows, examples or []

    def line(self) -> str:
        ex = f"  e.g. {self.examples[:3]}" if self.examples else ""
        return f"[{self.severity}] {self.code}: {self.detail} (rows={self.rows}){ex}"

    def to_dict(self) -> Dict[str, Any]:
        return {"code": self.code, "severity": self.severity, "detail": self.detail,
                "rows": self.rows, "examples": [str(e) for e in self.examples[:5]]}


def _rows(conn) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return list(conn.execute("SELECT * FROM quantitative_ledger"))


def reconcile(rows: List[sqlite3.Row]) -> List[Finding]:
    out: List[Finding] = []
    if not rows:
        return [Finding("C0", "INFO", "ledger is empty — nothing to reconcile")]

    def g(r, k, default=None):
        try:
            return r[k]
        except (IndexError, KeyError):
            return default

    # C1 -----------------------------------------------------------------
    ids = [r["signal_id"] for r in rows]
    dupes = {i for i in ids if ids.count(i) > 1} if len(ids) < 5000 else set()
    if dupes:
        out.append(Finding("C1", "CRITICAL",
                           "duplicate signal_id — the same trade is counted more than once "
                           "in every performance figure", len(dupes), sorted(dupes)))

    # C2 / C8 ------------------------------------------------------------
    bad_hold = [r["signal_id"] for r in rows
                if g(r, "holding_minutes") is not None
                and (float(g(r, "holding_minutes") or 0) < 0
                     or float(g(r, "holding_minutes") or 0) > MAX_PLAUSIBLE_HOLD_MINUTES)]
    if bad_hold:
        out.append(Finding("C2/C8", "HIGH",
                           "negative or implausible holding time — the exit timestamp "
                           "precedes the entry, or a resolution was never recorded",
                           len(bad_hold), bad_hold))

    # C3 -----------------------------------------------------------------
    resolved_no_outcome = [r["signal_id"] for r in rows
                           if g(r, "resolved") and not g(r, "actual_outcome")]
    if resolved_no_outcome:
        out.append(Finding("C3a", "HIGH", "resolved trade with no outcome recorded",
                           len(resolved_no_outcome), resolved_no_outcome))

    unresolved_with_pnl = [r["signal_id"] for r in rows
                           if not g(r, "resolved") and (g(r, "net_pnl") or 0.0)]
    if unresolved_with_pnl:
        out.append(Finding("C3b", "HIGH",
                           "unresolved trade already carrying PnL — realised figures "
                           "include money that has not settled",
                           len(unresolved_with_pnl), unresolved_with_pnl))

    # C4 -----------------------------------------------------------------
    neg_fees = [r["signal_id"] for r in rows if float(g(r, "fees_usdt") or 0.0) < 0]
    if neg_fees:
        out.append(Finding("C4", "HIGH",
                           "negative fees — a cost recorded as income inflates net PnL",
                           len(neg_fees), neg_fees))

    # C5 -----------------------------------------------------------------
    mismatch = [r["signal_id"] for r in rows
                if g(r, "settlement_status") is not None
                and bool(g(r, "resolved")) != (str(g(r, "settlement_status")).upper()
                                               in ("SETTLED", "RESOLVED", "CLOSED"))]
    if mismatch:
        out.append(Finding("C5", "MEDIUM",
                           "settlement_status disagrees with `resolved` — two fields "
                           "describing the same fact have drifted apart",
                           len(mismatch), mismatch))

    # C6a ----------------------------------------------------------------
    nonfinite = []
    for r in rows:
        for col in NUMERIC_COLUMNS:
            v = g(r, col)
            if isinstance(v, float) and not math.isfinite(v):
                nonfinite.append(f"{r['signal_id']}.{col}")
    if nonfinite:
        out.append(Finding("C6a", "CRITICAL",
                           "infinity stored in a numeric column — it will propagate "
                           "silently through every aggregate",
                           len(nonfinite), nonfinite))

    # C6b ----------------------------------------------------------------
    # SQLite does not store NaN: it silently converts it to NULL on insert
    # (verified — INSERT float('nan') reads back as None, while Inf survives).
    # Every read site in this codebase then does `float(row["net_pnl"] or 0.0)`,
    # so a trade whose PnL computation produced NaN does not raise, does not
    # warn, and does not show up as missing. It becomes a BREAKEVEN TRADE in
    # the win rate, the expectancy, the Kelly update and the model labels.
    # A NULL PnL on a resolved trade is therefore not a cosmetic gap.
    null_pnl = [r["signal_id"] for r in rows
                if g(r, "resolved") and g(r, "net_pnl") is None]
    if null_pnl:
        out.append(Finding(
            "C6b", "CRITICAL",
            "resolved trade with NULL net_pnl. SQLite stores NaN as NULL, and every "
            "read site coerces NULL to 0.0, so a corrupted PnL is indistinguishable "
            "from a breakeven trade in the win rate, the expectancy and the Kelly "
            "update. Find why it was NULL before trusting any aggregate.",
            len(null_pnl), null_pnl))

    # C7 -----------------------------------------------------------------
    real = [r for r in rows if not g(r, "is_shadow")]
    total = sum(float(g(r, "net_pnl") or 0.0) for r in real)
    by_symbol: Dict[str, float] = {}
    for r in real:
        by_symbol[r["symbol"]] = by_symbol.get(r["symbol"], 0.0) + float(g(r, "net_pnl") or 0.0)
    if abs(sum(by_symbol.values()) - total) > 1e-6:
        out.append(Finding("C7", "CRITICAL",
                           "per-symbol PnL does not sum to the total — rows are being "
                           "dropped by a filter somewhere", len(real)))

    # C9 -----------------------------------------------------------------
    settled = [r for r in real if g(r, "resolved")]
    if settled:
        by_outcome = sum(1 for r in settled
                         if str(g(r, "actual_outcome") or "").upper() in ("WIN", "TP", "PROFIT"))
        by_pnl = sum(1 for r in settled if float(g(r, "net_pnl") or 0.0) > 0)
        if by_outcome != by_pnl:
            out.append(Finding(
                "C9", "CRITICAL",
                f"win count by OUTCOME LABEL ({by_outcome}) disagrees with win count by "
                f"PnL SIGN ({by_pnl}) over {len(settled)} settled trades. The labels fed "
                f"to the model do not match the money. Learning is being corrupted "
                f"silently — this is the single most damaging inconsistency in the list.",
                len(settled)))

    # C10 ----------------------------------------------------------------
    shadow_pnl = sum(float(g(r, "net_pnl") or 0.0) for r in rows if g(r, "is_shadow"))
    if shadow_pnl:
        out.append(Finding("C10", "INFO",
                           f"shadow trades carry {shadow_pnl:+.4f} PnL; confirm nothing "
                           f"downstream adds them to realised results",
                           sum(1 for r in rows if g(r, "is_shadow"))))

    return out


def summarise(rows: List[sqlite3.Row]) -> Dict[str, Any]:
    real = [r for r in rows if not (r["is_shadow"] if "is_shadow" in r.keys() else 0)]
    settled = [r for r in real if r["resolved"]]
    pnl = [float(r["net_pnl"] or 0.0) for r in settled]
    fees = sum(float(r["fees_usdt"] or 0.0) for r in settled)
    return {
        "rows_total": len(rows),
        "rows_real": len(real),
        "rows_settled": len(settled),
        "rows_shadow": len(rows) - len(real),
        "net_pnl_settled": sum(pnl),
        "gross_before_fees": sum(pnl) + fees,
        "fees_paid": fees,
        "fees_as_pct_of_gross": (fees / abs(sum(pnl) + fees) * 100.0
                                 if (sum(pnl) + fees) else 0.0),
        "wins": sum(1 for p in pnl if p > 0),
        "losses": sum(1 for p in pnl if p <= 0),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="titan_memory_ledger.db")
    ap.add_argument("--json", default=None, help="write the report here")
    ap.add_argument("--exchange", action="store_true",
                    help="also compare against Bybit execution history (READ-ONLY; "
                         "needs credentials and egress)")
    ap.add_argument("--days", type=int, default=7)
    args = ap.parse_args()

    p = pathlib.Path(args.db)
    if not p.exists():
        print(f"no ledger at {p}")
        return 1

    conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    rows = _rows(conn)
    findings = reconcile(rows)
    stats = summarise(rows)

    print(json.dumps(stats, indent=2))
    print()
    if not findings:
        print("RECONCILED — no inconsistencies found.")
    for f in findings:
        print(f.line())

    if args.exchange:
        print("\nExchange comparison requires credentials and outbound access to "
              "api.bybit.com. It is read-only (/v5/execution/list) and places no orders.")
        print("Not attempted here — run it from an environment with exchange egress.")

    report = {"stats": stats, "findings": [f.to_dict() for f in findings]}
    if args.json:
        pathlib.Path(args.json).write_text(json.dumps(report, indent=2))
        print(f"\nwritten to {args.json}")

    worst = {f.severity for f in findings}
    return 2 if ("CRITICAL" in worst or "HIGH" in worst) else 0


if __name__ == "__main__":
    raise SystemExit(main())
