#!/usr/bin/env python3
"""
Paper -> live promotion gate. Read-only unless --approve is given.

    python scripts/evaluate_promotion.py                         # evaluate, print checklist
    python scripts/evaluate_promotion.py --approve --operator kerry   # write approval (only if ELIGIBLE)

The approval record is what the V12 guardian requires before it allows a single
LIVE entry. An ineligible result can never be approved from here.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from v12.journal import TradeJournal  # noqa: E402
from v12.promotion import evaluate  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--journal", default=None, help="path to v12_journal.db")
    ap.add_argument("--starting-equity", type=float,
                    default=float(os.getenv("PAPER_STARTING_BALANCE", "1000")))
    ap.add_argument("--approve", action="store_true")
    ap.add_argument("--operator", default="")
    ap.add_argument("--out", default=os.getenv("PROMOTION_FILE", str(ROOT / "reports/promotion/latest.json")))
    a = ap.parse_args()
    j = TradeJournal(a.journal)
    trades = j.closed_trades()
    res = evaluate(trades, a.starting_equity)
    print(f"PROMOTION CHECK over {res['n']} paper/testnet trades\n")
    for c in res["checks"]:
        print(f"  [{'PASS' if c['pass'] else 'FAIL'}] {c['check']:<18} {c['detail']}")
    print(f"\n  RESULT: {'ELIGIBLE' if res['eligible'] else 'NOT ELIGIBLE'}")
    if not a.approve:
        return 0 if res["eligible"] else 1
    if not res["eligible"]:
        print("\nRefusing to approve: the strategy has not met the criteria.")
        return 2
    if not a.operator.strip():
        print("\n--operator NAME is required: approval is a named human decision.")
        return 2
    doc = {"approved": True, "approved_by": a.operator.strip(), "approved_ts": time.time(),
           "evaluation": res, "journal": j.path}
    pathlib.Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(a.out).write_text(json.dumps(doc, indent=2, default=str))
    print(f"\nApproval written to {a.out}. LIVE mode still requires TRADING_MODE=LIVE to be set by you.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
