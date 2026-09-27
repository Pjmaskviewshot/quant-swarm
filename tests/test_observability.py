"""B31 — observability.

Roughly forty `except: pass` / `logger.debug` handlers absorbed exchange and
database faults with no counter and no alert. Several findings (B13, B14, B16)
survived unnoticed for an unknown period precisely because nothing counted what
was being rejected or dropped.
"""
import pathlib
import pytest

from observability import Metrics, ReasonCode, METRICS

REPO = pathlib.Path(__file__).resolve().parents[1]
MAIN = (REPO / "src" / "main.py").read_text()


@pytest.fixture
def m():
    return Metrics()


# --- counters --------------------------------------------------------------

def test_counters_accumulate(m):
    m.incr("entries"); m.incr("entries", 2)
    assert m.snapshot()["counters"]["entries"] == 3


def test_gauges_overwrite(m):
    m.gauge("balance", 100.0); m.gauge("balance", 250.0)
    assert m.snapshot()["gauges"]["balance"] == 250.0


def test_reason_codes_are_counted(m):
    m.reason(ReasonCode.STALE_DATA)
    m.reason(ReasonCode.STALE_DATA)
    m.reason(ReasonCode.SLOT_CAP)
    snap = m.snapshot()
    assert snap["reason_codes"][ReasonCode.STALE_DATA] == 2
    assert snap["reason_codes"][ReasonCode.SLOT_CAP] == 1


def test_entry_rejections_roll_into_a_total(m):
    m.reason(ReasonCode.STALE_DATA)
    m.reason(ReasonCode.HEAT_CAP)
    m.reason(ReasonCode.EXIT_TARGET)          # not a rejection
    assert m.snapshot()["counters"]["signals_rejected"] == 2


def test_reset_clears_everything(m):
    m.incr("x"); m.reason(ReasonCode.EV); m.gauge("g", 1.0)
    m.reset()
    snap = m.snapshot()
    assert snap["counters"] == {} and snap["reason_codes"] == {} and snap["gauges"] == {}


# --- latency ---------------------------------------------------------------

def test_latency_percentiles(m):
    for v in [0.001 * i for i in range(1, 101)]:
        m.observe_latency("eval", v)
    lat = m.snapshot()["latency"]["eval"]
    assert lat["count"] == 100
    assert lat["p50"] < lat["p95"] < lat["p99"]


def test_timer_records(m):
    with m.timer("execution"):
        pass
    assert m.snapshot()["latency"]["execution"]["count"] == 1


def test_latency_window_is_bounded():
    m = Metrics(latency_window=10)
    for i in range(50):
        m.observe_latency("x", float(i))
    assert m.snapshot()["latency"]["x"]["count"] == 10


def test_empty_latency_is_zero_not_error(m):
    m.observe_latency("k", 0.5)
    assert m.snapshot()["latency"]["k"]["p50"] >= 0.0


# --- reason code vocabulary -----------------------------------------------

@pytest.mark.parametrize("code", [
    ReasonCode.STALE_DATA, ReasonCode.EV, ReasonCode.CORRELATION,
    ReasonCode.DRAWDOWN, ReasonCode.SPREAD, ReasonCode.SLOT_CAP,
    ReasonCode.HEAT_CAP, ReasonCode.BELOW_MIN_NOTIONAL,
    ReasonCode.NOTIONAL_DEVIATION, ReasonCode.HEALTH_DEGRADED,
])
def test_required_reason_codes_exist(code):
    assert isinstance(code, str) and code.startswith("ENTRY_REJECTED")


def test_settlement_codes_exist():
    assert ReasonCode.SETTLE_UNKNOWN == "SETTLEMENT_UNKNOWN"
    assert ReasonCode.SETTLE_FILLS_FALLBACK == "SETTLEMENT_FROM_FILLS"


# --- wiring ----------------------------------------------------------------

def test_main_records_rejections():
    assert "METRICS.reason(ReasonCode.STALE_DATA)" in MAIN
    assert "METRICS.reason(ReasonCode.DNA_QUARANTINE)" in MAIN


def test_main_counts_signals_and_orders():
    for counter in ("signals_generated", "signals_executed", "orders_submitted",
                    "orders_rejected", "entries"):
        assert f'METRICS.incr("{counter}")' in MAIN, f"missing counter: {counter}"


def test_health_endpoint_exposes_metrics():
    ka = (REPO / "src" / "keep_alive.py").read_text()
    assert "_metrics_snapshot" in ka
    assert "@app.route('/metrics')" in ka


def test_global_metrics_singleton_is_usable():
    before = METRICS.snapshot()["counters"].get("smoke", 0)
    METRICS.incr("smoke")
    assert METRICS.snapshot()["counters"]["smoke"] == before + 1
