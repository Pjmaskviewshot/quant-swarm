"""B9/B10 — atomic reservations and a TTL that outlives execution.

B9: the vault check ran OUTSIDE the reservation lock, so N symbols evaluating
    concurrently could all pass against the same snapshot and all reserve.
    max_slots and the leverage ceiling were advisory, not enforced.
B10: the reservation TTL was a hardcoded 45s while a TWAP iceberg can run ~80s,
    so the lock expired mid-execution and a duplicate entry became possible.
"""
import asyncio
import time
import pytest

from main import DistributedQuantEngine
from portfolio.risk_vault import InstitutionalRiskVault
from runtime_config import RuntimeConfig, TradingMode


class Engine:
    """Exercises the real reservation logic with a minimal surrounding state."""
    _try_reserve_entry = DistributedQuantEngine._try_reserve_entry
    _execution_budget_seconds = DistributedQuantEngine._execution_budget_seconds

    def __init__(self, max_slots=5, balance=10_000.0):
        self.config = RuntimeConfig(mode=TradingMode.PAPER, build_revision="test")
        self.reservation_lock = asyncio.Lock()
        self.active_positions_map = {}
        self.in_flight_symbols = {}
        self.in_flight_notionals = {}
        self.live_params = {"LEVERAGE_CAP": 2.0}
        self.risk_vault = InstitutionalRiskVault(max_slots=max_slots)
        self.risk_vault.sync_watermarks(balance)
        self.balance = balance
        self.dispatched = []

    class _Actor:
        def __init__(self, outer): self.outer = outer
        def dispatch(self, a, t, p): self.outer.dispatched.append((a, t, p))

    @property
    def state_actor(self):
        return Engine._Actor(self)


def reserve(eng, symbol, notional=100.0):
    return asyncio.get_event_loop().run_until_complete(
        eng._try_reserve_entry(symbol, notional, 0.02, eng.balance)
    )


def run_all(coros):
    async def _g():
        return await asyncio.gather(*coros)
    return asyncio.run(_g())


# --- B9: concurrency -------------------------------------------------------

def test_concurrent_reservations_respect_slot_cap():
    """The headline B9 case: 20 symbols racing must not exceed max_slots."""
    eng = Engine(max_slots=5)
    symbols = [f"SYM{i}USDT" for i in range(20)]
    results = run_all([eng._try_reserve_entry(s, 100.0, 0.02, eng.balance) for s in symbols])
    granted = [ok for ok, _ in results if ok]
    assert len(granted) <= 5, (
        f"B9: {len(granted)} reservations granted against a cap of 5"
    )
    assert len(eng.in_flight_symbols) == len(granted)


def test_concurrent_duplicate_symbol_reserved_once():
    eng = Engine(max_slots=5)
    results = run_all([eng._try_reserve_entry("BTCUSDT", 100.0, 0.02, eng.balance)
                       for _ in range(10)])
    assert sum(1 for ok, _ in results if ok) == 1


def test_concurrent_reservations_respect_heat_cap():
    """Total committed notional must never exceed the leverage ceiling."""
    eng = Engine(max_slots=50, balance=1_000.0)   # heat cap = 1000 * 2 * 0.95 = 1900
    symbols = [f"SYM{i}USDT" for i in range(20)]
    run_all([eng._try_reserve_entry(s, 500.0, 0.02, eng.balance) for s in symbols])
    committed = sum(eng.in_flight_notionals.values())
    assert committed <= 1_900.0 + 1e-6, f"B9: heat cap breached at ${committed:.2f}"


def test_in_flight_counts_toward_slots():
    eng = Engine(max_slots=2)
    assert run_all([eng._try_reserve_entry("AUSDT", 100.0, 0.02, eng.balance)])[0][0] is True
    assert run_all([eng._try_reserve_entry("BUSDT", 100.0, 0.02, eng.balance)])[0][0] is True
    ok, reason = run_all([eng._try_reserve_entry("CUSDT", 100.0, 0.02, eng.balance)])[0]
    assert ok is False and "SLOT_CAP" in reason


def test_existing_position_blocks_duplicate():
    eng = Engine()
    eng.active_positions_map["BTCUSDT"] = "BUY"
    ok, reason = run_all([eng._try_reserve_entry("BTCUSDT", 100.0, 0.02, eng.balance)])[0]
    assert ok is False and "DUPLICATE" in reason


def test_successful_reservation_dispatches_to_state_actor():
    eng = Engine()
    run_all([eng._try_reserve_entry("BTCUSDT", 100.0, 0.02, eng.balance)])
    assert any(t == "RESERVE_IN_FLIGHT" for _, t, _ in eng.dispatched)


def test_failed_reservation_leaves_no_residue():
    eng = Engine(max_slots=1)
    run_all([eng._try_reserve_entry("AUSDT", 100.0, 0.02, eng.balance)])
    run_all([eng._try_reserve_entry("BUSDT", 100.0, 0.02, eng.balance)])
    assert "BUSDT" not in eng.in_flight_symbols
    assert "BUSDT" not in eng.in_flight_notionals


# --- B10: TTL --------------------------------------------------------------

def test_ttl_exceeds_worst_case_twap_duration():
    """8 slices x (5s peg + 8s interval) = 104s, plus retries and margin."""
    eng = Engine()
    budget = eng._execution_budget_seconds()
    worst_case_twap = 8 * (5.0 + 8.0)
    assert budget > worst_case_twap, (
        f"B10: TTL {budget}s does not cover a {worst_case_twap}s TWAP"
    )
    assert budget > 45.0, "B10: TTL is still at or below the old hardcoded 45s"


def test_reservation_uses_derived_ttl():
    eng = Engine()
    before = time.time()
    run_all([eng._try_reserve_entry("BTCUSDT", 100.0, 0.02, eng.balance)])
    expiry = eng.in_flight_symbols["BTCUSDT"]
    assert expiry - before >= eng._execution_budget_seconds() - 1.0


def test_ttl_respects_configured_floor():
    eng = Engine()
    eng.config = RuntimeConfig(mode=TradingMode.PAPER, build_revision="t",
                               execution_budget_sec=600.0)
    assert eng._execution_budget_seconds() == pytest.approx(600.0)
