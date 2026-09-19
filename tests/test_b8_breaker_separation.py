"""B8 — system health failure must not be treated as a capital emergency.

Audited behaviour: 4 CRITICAL errors in 60s engaged the global emergency lock,
which unwound the main loop into graceful_shutdown and market-flattened every
open position. Four transient exceptions in the RISK-FREE evaluation path were
enough to liquidate the portfolio. A planned restart did the same.
"""
import time

from core.fsm import SystemStateMachine, TradingState, ErrorSeverity


def fsm():
    return SystemStateMachine()


def flood(m, n, module="_eval_gate", severity=ErrorSeverity.CRITICAL):
    for _ in range(n):
        m.record_module_error(module, severity)


# --- the core separation ---------------------------------------------------

def test_error_flood_halts_entries_without_capital_emergency():
    m = fsm()
    flood(m, 5)
    assert m.is_health_degraded is True
    assert m.can_open_new_positions is False
    assert m.is_emergency_locked() is False, (
        "B8: an evaluation-path error flood escalated to a CAPITAL emergency"
    )


def test_error_flood_preserves_position_management():
    """Exits, trailing stops and reconciliation must keep working."""
    m = fsm()
    flood(m, 8)
    assert m.can_manage_positions is True, (
        "B8: health degradation must not suspend protective monitoring"
    )


def test_degraded_state_is_reported():
    m = fsm()
    flood(m, 5)
    assert m.current_state is TradingState.HEALTH_DEGRADED
    assert "_eval_gate" in m.halt_reason


def test_transient_errors_never_degrade():
    m = fsm()
    flood(m, 50, module="run_telegram_worker", severity=ErrorSeverity.TRANSIENT)
    assert m.is_health_degraded is False
    assert m.can_open_new_positions is True


def test_degraded_threshold_needs_four_criticals():
    m = fsm()
    flood(m, 3)
    assert m.is_health_degraded is False
    flood(m, 1)
    assert m.is_health_degraded is True


# --- capital emergency still works -----------------------------------------

def test_drawdown_breach_still_engages_capital_lock():
    m = fsm()
    m.trigger_global_emergency_lock(reason="Drawdown breach: 15%")
    assert m.is_emergency_locked() is True
    assert m.can_manage_positions is False
    assert m.can_open_new_positions is False


def test_capital_lock_release_clears_health_halt():
    m = fsm()
    flood(m, 5)
    m.trigger_global_emergency_lock(reason="dd")
    m.release_global_emergency_lock()
    assert m.is_emergency_locked() is False
    assert m.is_health_degraded is False
    assert m.can_open_new_positions is True


# --- recovery --------------------------------------------------------------

def test_manual_recovery_rearms_entries():
    m = fsm()
    flood(m, 5)
    m.recover_health()
    assert m.can_open_new_positions is True
    assert m.current_state is TradingState.ACTIVE_TRADING


def test_auto_recovery_waits_for_quiet_period():
    m = fsm()
    flood(m, 5)
    assert m.maybe_auto_recover_health(quiet_period_seconds=300.0) is False
    assert m.is_health_degraded is True


def test_auto_recovery_fires_after_quiet_period():
    m = fsm()
    flood(m, 5)
    # Backdate the fault window to simulate a quiet period.
    m.halt_timestamp = time.time() - 600.0
    for events in m.error_counts.values():
        for i, (ts, sev) in enumerate(events):
            events[i] = (ts - 600.0, sev)
    assert m.maybe_auto_recover_health(quiet_period_seconds=300.0) is True
    assert m.can_open_new_positions is True


def test_auto_recovery_noop_when_healthy():
    assert fsm().maybe_auto_recover_health() is False


# --- backwards compatibility ----------------------------------------------

def test_can_execute_trades_still_gates_entries():
    m = fsm()
    assert m.can_execute_trades is True
    flood(m, 5)
    assert m.can_execute_trades is False
