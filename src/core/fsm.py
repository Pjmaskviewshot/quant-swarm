"""
âš¡ V1.0 TITANIUM APEX: ASYNCHRONOUS MACRO STATE MANAGER (FSM)
--------------------------------------------------------------
Serves as the O(1) in-memory cache for macro regime analysis, Sector Eigenvector 
momentum states, and the single source of truth for Swarm-level circuit breakers.
Upgraded with Per-Asset Micro-Locks for localized Exhaustion/Absorption isolation.
"""

import logging
import time
from enum import Enum
from typing import Dict, Any

logger = logging.getLogger("QUANT_CORE.FSM")

class TradingState(Enum):
    BOOTSTRAPPING = "BOOTSTRAPPING"
    CALIBRATING = "SWARM_CALIBRATING"
    ACTIVE_TRADING = "DECENTRALIZED_ACTIVE"
    ACTIVE_MEAN_REVERSION = "DECENTRALIZED_MEAN_REV"
    EMERGENCY_LOCK = "EMERGENCY_LOCK"
    AI_MACRO_BULL = "AI_MACRO_BULL"
    AI_MACRO_BEAR = "AI_MACRO_BEAR"
    # ðŸš€ V1.0 NEW STATES
    ABSORPTION_COOLDOWN = "ABSORPTION_COOLDOWN"  
    SECTOR_MISALIGNMENT = "SECTOR_MISALIGNMENT"

class SystemStateMachine:
    """
    âš¡ V1.0 APEX UPGRADE: DYNAMIC MACRO & SECTOR STATE MANAGER
    Serves as the O(1) in-memory cache for Off-Path AI Debate, Sector SVD Eigenvectors,
    and granular localized/global Circuit Breakers.
    """
    def __init__(self, accuracy_threshold: float = 0.60, warmup_epochs: int = 150):
        self.current_state = TradingState.BOOTSTRAPPING
        
        # O(1) Caches for off-path predictions and sector SVDs (Eliminates execution latency)
        self.ai_macro_cache: Dict[str, Dict[str, Any]] = {}
        self.sector_macro_cache: Dict[str, Dict[str, Any]] = {}
        
        # Single Source of Truth for Swarm-level hardware locks
        self.global_emergency_lock = False
        
        # ðŸš€ V1.0 NEW: Per-Asset Micro-Locks (Timestamp expiration)
        self.asset_locks: Dict[str, float] = {}
        
        logger.info("âš¡ FSM Core Upgraded to V1.0: Now serving Granular Asset Locks & Sector Eigenvector Cache.")

    # =====================================================================
    # MACRO & SECTOR STATE CACHING
    # =====================================================================

    def update_ai_macro_state(self, symbol: str, action: str, confidence_multiplier: float):
        """Updates the asset's macro state without blocking the HFT WebSocket feed."""
        self.ai_macro_cache[symbol] = {
            "action": action.upper(),
            "confidence_multiplier": max(0.5, min(2.0, confidence_multiplier)), 
            "last_updated": time.time()
        }
        logger.info(f"ðŸ§  AI MACRO CACHED // {symbol}: {action.upper()} (Mult: {self.ai_macro_cache[symbol]['confidence_multiplier']:.2f}x)")

    def get_ai_macro_state(self, symbol: str, staleness_limit_seconds: float = 900.0) -> Dict[str, Any]:
        """O(1) lookup for the SOR pipeline. Reverts to neutral safety if LLM is lagging."""
        state = self.ai_macro_cache.get(symbol)
        if not state or (time.time() - state["last_updated"] > staleness_limit_seconds):
            return {"action": "HOLD", "confidence_multiplier": 1.0}
        return state

    def update_sector_state(self, target_symbol: str, impulse_score: float, correlation: float):
        """
        ðŸš€ V1.0 UPGRADE: Caches the SVD Sector Eigenvector impulse.
        Allows the execution router to verify sector tailwinds in O(1) time.
        """
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
    # GLOBAL & LOCAL CIRCUIT BREAKERS
    # =====================================================================

    def trigger_asset_lock(self, symbol: str, duration_seconds: float, reason: str = "ABSORPTION_WALL"):
        """
        ðŸš€ V1.0 UPGRADE: Isolates specific assets that have hit an absorption wall 
        or extreme slippage, freezing them without taking down the entire Swarm.
        """
        expiration = time.time() + duration_seconds
        self.asset_locks[symbol] = expiration
        logger.warning(f"â¸ï¸ ASSET MICRO-LOCK ENGAGED // {symbol} isolated for {duration_seconds:.1f}s. Reason: {reason}")

    def is_asset_locked(self, symbol: str) -> bool:
        """O(1) check to see if an asset is currently in a micro-lock cooldown."""
        if self.global_emergency_lock:
            return True
            
        expiration = self.asset_locks.get(symbol, 0.0)
        if time.time() < expiration:
            return True
            
        # Clean up expired lock
        if expiration > 0.0:
            self.asset_locks.pop(symbol, None)
            
        return False

    def trigger_global_emergency_lock(self):
        """Instantly locks all swarm execution pathways across all nodes."""
        self.global_emergency_lock = True
        self.current_state = TradingState.EMERGENCY_LOCK
        logger.critical("ðŸ›‘ FSM GLOBAL EMERGENCY LOCK ENGAGED. ALL NEW EXECUTIONS HALTED.")

    def release_global_emergency_lock(self):
        """Restores swarm execution pathways."""
        self.global_emergency_lock = False
        self.current_state = TradingState.ACTIVE_TRADING
        logger.warning("ðŸ”“ FSM GLOBAL EMERGENCY LOCK LIFTED. SWARM RE-ARMED.")

    def is_emergency_locked(self) -> bool:
        """Explicit boolean check used by main.py to stop executions."""
        return self.global_emergency_lock

    @property
    def emergency_locked(self) -> bool:
        """Property wrapper for centralized circuit-breaker verification."""
        return self.global_emergency_lock

    def process_state_transition(self, rolling_accuracy: float, total_resolved: int, market_regime: str = "TRENDING") -> TradingState:
        """
        Legacy method preserved to prevent crashes if called by older modules.
        Now dynamically intercepts execution if the global hardware lock is engaged.
        """
        if self.global_emergency_lock:
            return TradingState.EMERGENCY_LOCK
        return self.current_state

    @property
    def can_execute_trades(self) -> bool:
        """Intercepts the gatekeeper boolean to halt the swarm during systemic failure."""
        return not self.global_emergency_lock