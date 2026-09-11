"""
V49.0 APEX QUANTUM PRIME: ASYNCHRONOUS MACRO STATE MANAGER (FSM)
--------------------------------------------------------------------------------
O(1) in-memory cache for macro regime telemetry, Sector SVD Eigenvectors,
and resilient multi-tiered hardware/software circuit breakers.

Production Hardening & Quantitative Upgrades (V49.0 Audit Resolutions):
1. Multi-Tier SRE Error Governance: Replaces monolithic error counters with 
   tiered severity classification (TRANSIENT, DEGRADED, CRITICAL) to prevent 
   non-critical telemetry/database timeouts from halting execution.
2. Auto-Classifying Module Routing: Automatically resolves caller severity tiers 
   for background daemons, preserving full backwards compatibility.
3. Clean Lock Clearance: Purges sliding-window error queues during emergency lock 
   release, eliminating instant re-lock deadlocks.
4. Active Expiration Cleaning: Lazily prunes expired asset micro-locks in O(1) time.
"""

import logging
import time
from enum import Enum
from typing import Dict, Any, Optional, Tuple
from collections import deque

logger = logging.getLogger("QUANT_CORE.FSM")


class TradingState(Enum):
    BOOTSTRAPPING = "BOOTSTRAPPING"
    CALIBRATING = "SWARM_CALIBRATING"
    ACTIVE_TRADING = "HUNTING_ACTIVE"
    EMERGENCY_LOCK = "EMERGENCY_LOCK"
    ABSORPTION_COOLDOWN = "ABSORPTION_COOLDOWN"  
    SECTOR_MISALIGNMENT = "SECTOR_MISALIGNMENT"


class ErrorSeverity(Enum):
    TRANSIENT = "TRANSIENT"  # Non-blocking: Telegram, Supabase logging, shadow forensics
    DEGRADED = "DEGRADED"    # Non-fatal: WebSockets reconnects, klines, scanner sweeps
    CRITICAL = "CRITICAL"    # Fatal: Position lifecycle, SOR execution, exchange rejections


class SystemStateMachine:
    """
    State Manager and Circuit Breaker Nexus.
    Manages O(1) in-memory telemetry, sector alignment, and localized/global halts.
    """
    def __init__(self):
        self.current_state = TradingState.BOOTSTRAPPING
        
        # O(1) In-memory caches for off-path predictions and sector alignments
        self.ai_macro_cache: Dict[str, Dict[str, Any]] = {}
        self.sector_macro_cache: Dict[str, Dict[str, Any]] = {}
        
        # Single Source of Truth for Swarm-level execution locks
        self.global_emergency_lock = False
        self.lock_reason: str = ""
        self.lock_timestamp: float = 0.0
        
        # Per-Asset Micro-Locks (Timestamp expiration)
        self.asset_locks: Dict[str, float] = {}

        # SRE Tiered Module Anomaly Tracking
        self.error_counts: Dict[str, deque] = {}

        # Automatic severity resolution for unannotated callers
        self._module_severity_defaults: Dict[str, ErrorSeverity] = {
            "run_telegram_worker": ErrorSeverity.TRANSIENT,
            "run_shadow_resolution_daemon": ErrorSeverity.TRANSIENT,
            "run_dna_prewarmer": ErrorSeverity.TRANSIENT,
            "stream_manager_loop": ErrorSeverity.DEGRADED,
            "run_omni_swarm_director": ErrorSeverity.DEGRADED,
            "run_correlation_engine": ErrorSeverity.DEGRADED,
            "_universe_refresher_loop": ErrorSeverity.DEGRADED,
            "run_fast_state_invariant_reconciliation": ErrorSeverity.DEGRADED,
            "run_system_heartbeat": ErrorSeverity.DEGRADED,
            "_eval_gate": ErrorSeverity.CRITICAL,
            "_position_lifecycle_daemon": ErrorSeverity.CRITICAL,
            "state_actor": ErrorSeverity.CRITICAL,
            "_execute_flash_strike": ErrorSeverity.CRITICAL,
            "_execute_emergency_market_strike": ErrorSeverity.CRITICAL,
            "_state_settle_trade": ErrorSeverity.CRITICAL
        }
        
        logger.info("⚡ FSM Core Upgraded to V49.0: Multi-Tier SRE Breakers & Active Asset Locks Online.")

    # =====================================================================
    # MACRO & SECTOR STATE CACHING
    # =====================================================================

    def update_ai_macro_state(self, symbol: str, action: str, confidence_multiplier: float):
        """Updates an asset's off-path macro bias without blocking execution."""
        self.ai_macro_cache[symbol] = {
            "action": action.upper(),
            "confidence_multiplier": max(0.5, min(2.0, confidence_multiplier)), 
            "last_updated": time.time()
        }
        logger.info(f"🧠 AI MACRO CACHED // {symbol}: {action.upper()} (Mult: {self.ai_macro_cache[symbol]['confidence_multiplier']:.2f}x)")

    def get_ai_macro_state(self, symbol: str, staleness_limit_seconds: float = 900.0) -> Dict[str, Any]:
        """O(1) lookup for macro direction. Reverts to neutral HOLD if lagging."""
        state = self.ai_macro_cache.get(symbol)
        if not state or (time.time() - state["last_updated"] > staleness_limit_seconds):
            return {"action": "HOLD", "confidence_multiplier": 1.0}
        return state

    def update_sector_state(self, target_symbol: str, impulse_score: float, correlation: float):
        """Caches the SVD Sector Eigenvector impulse for execution verification."""
        self.sector_macro_cache[target_symbol] = {
            "impulse_score": impulse_score,
            "correlation": correlation,
            "last_updated": time.time()
        }

    def get_sector_state(self, target_symbol: str, staleness_limit_seconds: float = 300.0) -> Dict[str, Any]:
        """O(1) lookup for Sector SVD alignments."""
        state = self.sector_macro_cache.get(target_symbol)
        if not state or (time.time() - state["last_updated"] > staleness_limit_seconds):
            return {"impulse_score": 0.0, "correlation": 0.0}
        return state

    # =====================================================================
    # SRE & EXCEPTION CIRCUIT BREAKERS
    # =====================================================================

    def record_module_error(self, module_name: str, severity: Optional[ErrorSeverity] = None):
        """
        Tiered SRE Anomaly Governor.
        Tracks subsystem exceptions by severity. Non-critical background errors log warnings 
        without triggering false-positive system halts.
        """
        now = time.time()
        
        # Auto-classify severity if not explicitly provided
        if severity is None:
            severity = self._module_severity_defaults.get(module_name, ErrorSeverity.DEGRADED)

        if module_name not in self.error_counts:
            self.error_counts[module_name] = deque(maxlen=100)
            
        self.error_counts[module_name].append((now, severity))
        
        # TRANSIENT errors (telemetry, background DB flushes) never halt execution
        if severity == ErrorSeverity.TRANSIENT:
            logger.warning(f"[SRE TRANSIENT] Handled background fault in {module_name}. Execution unaffected.")
            return

        # Prune events outside the 60-second observation window
        recent_events = [event for event in self.error_counts[module_name] if now - event[0] <= 60.0]
        
        crit_count = sum(1 for _, sev in recent_events if sev == ErrorSeverity.CRITICAL)
        degraded_count = sum(1 for _, sev in recent_events if sev == ErrorSeverity.DEGRADED)

        # Fault thresholds: 4 Critical faults or 25 Degraded faults in 60 seconds
        should_lock = (crit_count >= 4) or (degraded_count >= 25)

        if should_lock and not self.global_emergency_lock:
            reason = (
                f"Anomaly flood in {module_name} "
                f"({crit_count} Critical, {degraded_count} Degraded within 60s)"
            )
            logger.critical(f"🛑 [SRE ALERT] {reason}. Engaging Global Emergency Lock.")
            self.trigger_global_emergency_lock(reason=reason)

    # =====================================================================
    # GLOBAL & LOCAL CIRCUIT BREAKERS
    # =====================================================================

    def trigger_asset_lock(self, symbol: str, duration_seconds: float, reason: str = "ABSORPTION_WALL"):
        """Isolates specific assets experiencing high adverse selection or toxic spreads."""
        expiration = time.time() + duration_seconds
        self.asset_locks[symbol] = expiration
        logger.warning(f"⏸️ ASSET MICRO-LOCK ENGAGED // {symbol} isolated for {duration_seconds:.1f}s. Reason: {reason}")

    def is_asset_locked(self, symbol: str) -> bool:
        """O(1) check to see if an asset is currently in a cooldown window."""
        if self.global_emergency_lock:
            return True
            
        expiration = self.asset_locks.get(symbol, 0.0)
        if expiration > 0.0:
            if time.time() < expiration:
                return True
            # Clean up expired lock
            self.asset_locks.pop(symbol, None)
            
        return False

    def trigger_global_emergency_lock(self, reason: str = "UNSPECIFIED_ANOMALY"):
        """Locks all execution pathways across the swarm."""
        self.global_emergency_lock = True
        self.lock_reason = reason
        self.lock_timestamp = time.time()
        self.current_state = TradingState.EMERGENCY_LOCK
        logger.critical(f"🛑 FSM GLOBAL EMERGENCY LOCK ENGAGED. Reason: {reason}. ALL EXECUTIONS HALTED.")

    def release_global_emergency_lock(self):
        """Restores execution pathways and purges historical error queues."""
        self.global_emergency_lock = False
        self.lock_reason = ""
        self.lock_timestamp = 0.0
        self.error_counts.clear()
        self.current_state = TradingState.ACTIVE_TRADING
        logger.warning("🔓 FSM GLOBAL EMERGENCY LOCK LIFTED. Error counters cleared. Swarm re-armed.")

    def is_emergency_locked(self) -> bool:
        """Explicit check used by main orchestrator daemons."""
        return self.global_emergency_lock

    @property
    def emergency_locked(self) -> bool:
        """Property wrapper for centralized circuit-breaker verification."""
        return self.global_emergency_lock

    @property
    def can_execute_trades(self) -> bool:
        """Gatekeeper check to verify execution availability."""
        return not self.global_emergency_lock