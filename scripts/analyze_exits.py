#!/usr/bin/env python3
"""
MFE/MAE analysis and walk-forward exit optimisation over the V12 journal.

    python scripts/analyze_exits.py [--journal PATH] [--write-candidate]

Prints how far winners run, how far they go against us first, how many losers
were once in profit, how much of the move exits capture, and what price did
after we left. Then replays a grid of exit variants TRAIN -> TEST. With
--write-candidate it writes reports/candidates/exit_policy.json -- ALWAYS
unapproved. Nothing here changes the running bot.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from v12.exit_optimizer import mfe_mae_report, to_rpath, walk_forward_optimise, write_candidate  # noqa: E402
from v12.journal import TradeJournal  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--journal", default=None)
    ap.add_argument("--write-candidate", action="store_true")
    ap.add_argument("--out", default=str(ROOT / "reports/candidates/exit_policy.json"))
    a = ap.parse_args()
    j = TradeJournal(a.journal)
    paths = []
    for t in j.closed_trades():
        rp = to_rpath(t, j.trade_path(t["trade_id"]), cost_frac=(t.get("cost_bps") or 15.0) / 1e4)
        if rp:
            paths.append(rp)
    rep = mfe_mae_report(paths)
    print("MFE / MAE ANALYSIS")
    print(json.dumps({k: v for k, v in rep.items() if k != "notes"}, indent=2, default=str))
    for n in rep.get("notes", []):
        print("  NOTE:", n)
    res = walk_forward_optimise(paths)
    print("\nWALK-FORWARD EXIT OPTIMISATION")
    print(json.dumps({k: v for k, v in res.items() if k not in ("current", "best")}, indent=2, default=str))
    if a.write_candidate:
        write_candidate(res, a.out)
        print(f"\nCandidate written (UNAPPROVED) to {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
