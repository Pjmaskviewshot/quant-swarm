"""
OBSERVABILITY — counters and decision reason codes
--------------------------------------------------------------------------------
Audit finding B31: roughly forty `except: pass` / `logger.debug` handlers absorbed
exchange and database faults with no counter and no alert, so failures were
invisible. Several audit findings (B13, B14, B16) had been live for an unknown
period precisely because nothing counted what was being rejected or dropped.

This module is deliberately dependency-free and synchronous. It is a counter
registry, not a metrics backend; the values are exposed through /health so an
operator (or a scheduled check) can see them without additional infrastructure.
"""

from __future__ import annotations

import threading
import time
from collections import Counter, deque
from typing import Any, Deque, Dict


class ReasonCode:
    """Why a decision went the way it did. Strings are stable for dashboards."""

    # Entry rejections
    STALE_DATA = "ENTRY_REJECTED_STALE_DATA"
    NO_SPREAD = "ENTRY_REJECTED_NO_SPREAD"
    EV = "ENTRY_REJECTED_EV"
    CORRELATION = "ENTRY_REJECTED_CORRELATION"
    DRAWDOWN = "ENTRY_REJECTED_DRAWDOWN"
    SPREAD = "ENTRY_REJECTED_SPREAD"
    SLOT_CAP = "ENTRY_REJECTED_SLOT_CAP"
    HEAT_CAP = "ENTRY_REJECTED_HEAT_CAP"
    DUPLICATE = "ENTRY_REJECTED_DUPLICATE"
    BELOW_MIN_NOTIONAL = "ENTRY_REJECTED_BELOW_MIN_NOTIONAL"
    NOTIONAL_DEVIATION = "ENTRY_REJECTED_NOTIONAL_DEVIATION"
    SLIPPAGE = "ENTRY_REJECTED_SLIPPAGE"
    DNA_QUARANTINE = "ENTRY_REJECTED_DNA_QUARANTINE"
    GATE_PROBABILITY = "ENTRY_REJECTED_GATE_PROBABILITY"
    MOMENTUM_VETO = "ENTRY_REJECTED_MOMENTUM"
    WHIPSAW_VETO = "ENTRY_REJECTED_WHIPSAW"
    MACRO_VETO = "ENTRY_REJECTED_MACRO"
    EQUITY_FLOOR = "ENTRY_REJECTED_EQUITY_FLOOR"
    HEALTH_DEGRADED = "ENTRY_REJECTED_HEALTH_DEGRADED"

    # Exit classifications
    EXIT_TARGET = "EXIT_TARGET_REACHED"
    EXIT_STOP = "EXIT_STOP_REACHED"
    EXIT_TIME = "EXIT_TIME"
    EXIT_RISK = "EXIT_RISK"
    EXIT_THESIS_INVALIDATED = "EXIT_THESIS_INVALIDATED"
    EXIT_EXECUTION = "EXIT_EXECUTION"

    # Settlement
    SETTLE_RESOLVED = "SETTLEMENT_RESOLVED"
    SETTLE_FILLS_FALLBACK = "SETTLEMENT_FROM_FILLS"
    SETTLE_UNKNOWN = "SETTLEMENT_UNKNOWN"


class Metrics:
    """Thread-safe counters, gauges and latency samples."""

    def __init__(self, latency_window: int = 512):
        self._lock = threading.RLock()
        self._counters: Counter = Counter()
        self._gauges: Dict[str, float] = {}
        self._latencies: Dict[str, Deque[float]] = {}
        self._reasons: Counter = Counter()
        self._latency_window = latency_window
        self.started_at = time.time()

    def incr(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[name] += amount

    def gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = float(value)

    def reason(self, code: str, amount: int = 1) -> None:
        """Record a decision reason code and roll it into a total."""
        with self._lock:
            self._reasons[code] += amount
            if code.startswith("ENTRY_REJECTED"):
                self._counters["signals_rejected"] += amount

    def observe_latency(self, name: str, seconds: float) -> None:
        with self._lock:
            buf = self._latencies.get(name)
            if buf is None:
                buf = deque(maxlen=self._latency_window)
                self._latencies[name] = buf
            buf.append(float(seconds))

    def timer(self, name: str) -> "_Timer":
        return _Timer(self, name)

    def _percentile(self, buf: Deque[float], pct: float) -> float:
        if not buf:
            return 0.0
        ordered = sorted(buf)
        idx = min(len(ordered) - 1, max(0, int(round((pct / 100.0) * (len(ordered) - 1)))))
        return round(ordered[idx], 6)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            latencies = {
                name: {
                    "count": len(buf),
                    "p50": self._percentile(buf, 50),
                    "p95": self._percentile(buf, 95),
                    "p99": self._percentile(buf, 99),
                }
                for name, buf in self._latencies.items()
            }
            return {
                "uptime_seconds": round(time.time() - self.started_at, 1),
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "reason_codes": dict(self._reasons),
                "latency": latencies,
            }

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._reasons.clear()
            self._latencies.clear()
            self.started_at = time.time()


class _Timer:
    __slots__ = ("_m", "_name", "_t0")

    def __init__(self, metrics: Metrics, name: str):
        self._m = metrics
        self._name = name
        self._t0 = 0.0

    def __enter__(self) -> "_Timer":
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *exc) -> bool:
        self._m.observe_latency(self._name, time.perf_counter() - self._t0)
        return False


METRICS = Metrics()
