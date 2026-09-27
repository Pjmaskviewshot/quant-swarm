"""
MARKET DATA QUALITY CONTRACT
--------------------------------------------------------------------------------
Audit findings B19/B20: the system traded on whatever snapshot happened to be in
memory. There was no freshness bound anywhere, and a depth-less snapshot (the
shape written by the REST fallback during a WebSocket outage) caused the slippage
estimator to report zero cost — disabling the firewall exactly when data quality
was worst.

This is a pure module on purpose: no engine, no I/O, no async. The rule that
decides whether the system may risk capital on a piece of data should be
testable without constructing the world.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

# Snapshots produced without depth ladders. Usable for display and for keeping
# trailing logic alive; never for sizing or for entry.
DEPTHLESS_SOURCES = frozenset({"REST_BBO_FALLBACK"})


def is_tradeable(
    payload: Dict[str, Any],
    now: float,
    max_age_sec: float,
    max_future_skew_sec: float = 1.0,
) -> Tuple[bool, str]:
    """
    Decide whether a market-data snapshot may support a new entry.

    Returns (ok, reason). Freshness is measured against OUR receipt clock
    (`as_of`), not the exchange timestamp, because exchange clocks drift and the
    field is sometimes absent. A snapshot with no `as_of` is rejected rather
    than assumed fresh.
    """
    if not payload:
        return False, "no order book payload"

    if payload.get("depth_available") is False or payload.get("source") in DEPTHLESS_SOURCES:
        return False, "depth-less REST fallback snapshot (no ladders to size against)"

    if not (payload.get("bids") or []) or not (payload.get("asks") or []):
        return False, "order book missing bid or ask ladder"

    as_of = payload.get("as_of")
    if as_of is None:
        return False, "snapshot carries no as_of timestamp"

    try:
        age = now - float(as_of)
    except (TypeError, ValueError):
        return False, f"unparseable as_of timestamp: {as_of!r}"

    if age < -abs(max_future_skew_sec):
        return False, f"snapshot timestamp is {abs(age):.1f}s in the future (clock skew)"
    if age > max_age_sec:
        return False, f"order book age {age:.1f}s > {max_age_sec:.1f}s limit"

    return True, "OK"
