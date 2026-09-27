#!/usr/bin/env python3
"""
Approve a learned candidate (e.g. an exit policy) for deployment.

    python scripts/approve_candidate.py reports/candidates/exit_policy.json --operator kerry

Refuses unless the candidate itself recommended the change on its TEST split.
Approval does not deploy anything: you then set EXIT_POLICY_FILE to the file and
restart, ideally in PAPER first. learn -> evaluate -> paper test -> approve -> deploy.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("candidate")
    ap.add_argument("--operator", required=True)
    ap.add_argument("--force-not-recommended", action="store_true",
                    help="approve even though the TEST split did not show a significant improvement")
    a = ap.parse_args()
    p = pathlib.Path(a.candidate)
    doc = json.loads(p.read_text())
    if not doc.get("recommend") and not a.force_not_recommended:
        print(f"Refusing: candidate was not recommended ({doc.get('reason')}).")
        return 2
    if not a.operator.strip():
        print("--operator must name the person approving.")
        return 2
    doc.update(approved=True, approved_by=a.operator.strip(), approved_ts=time.time())
    p.write_text(json.dumps(doc, indent=2, default=str))
    print(f"Approved by {a.operator}. To deploy: EXIT_POLICY_FILE={p} (restart required).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
