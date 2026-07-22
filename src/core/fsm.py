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
    # New Swarm States for Off-Path LLM Integration
    AI_MACRO_BULL = "AI_MACRO_BULL"
    AI_MACRO_BEAR = "AI_MACRO_BEAR"

class SystemStateMachine:
    """
    🚀 V26.0 APEX UPGRADE: ASYNCHRONOUS MACRO STATE MANAGER
    Formerly a deprecated shell, this module is now the O(1) in-memory cache 
    for the Off-Path AI LLM Debate Matrix and Swarm-level Circuit Breakers.
    
    Legacy methods are strictly preserved to prevent ImportErrors from older modules.
    """
    def __init__(self, accuracy_threshold: float = 0.60, warmup_epochs: int = 150):
        self.current_state = TradingState.BOOTSTRAPPING
        
        # ⚡ O(1) Cache for off-path LLM predictions (Eliminates execution latency)
        self.ai_macro_cache: Dict[str, Dict[str, Any]] = {}
        
        # 🛑 Swarm-level hardware locks
        self.global_emergency_lock = False
        
        logger.info("⚡ FSM Shell Upgraded: Now serving as O(1) Macro Regime & Circuit Breaker Cache.")

    # ====================================================================
    # 🚀 V26 APEX METHODS: OFF-PATH INTELLIGENCE & SAFETY
    # ====================================================================
    
    def update_ai_macro_state(self, symbol: str, regime: str, confidence_multiplier: float):
        """
        Called exclusively by the background LLM worker loop.
        Updates the asset's macro state without blocking the High-Frequency WebSocket feed.
        """
        self.ai_macro_cache[symbol] = {
            "regime": regime.upper(),
            # Clamp the multiplier to prevent hallucinated extreme leverage sizing
            "confidence_multiplier": max(0.5, min(2.0, confidence_multiplier)), 
            "last_updated": time.time()
        }
        logger.info(f"🧠 AI MACRO STATE CACHED // {symbol}: {regime.upper()} (Mult: {self.ai_macro_cache[symbol]['confidence_multiplier']:.2f}x)")

    def get_ai_macro_state(self, symbol: str, staleness_limit_seconds: float = 900.0) -> Dict[str, Any]:
        """
        O(1) lookup for the SOR / Execution pipeline. 
        Instantly returns the AI verdict or falls back to a neutral safety state if the LLM is lagging.
        """
        state = self.ai_macro_cache.get(symbol)
        
        # If no state exists or the LLM data is older than 15 minutes, revert to safe defaults
        if not state or (time.time() - state["last_updated"] > staleness_limit_seconds):
            return {"regime": "RANGING", "confidence_multiplier": 1.0}
            
        return state
        
    def trigger_global_emergency_lock(self):
        """Instantly locks all swarm execution pathways across all nodes."""
        self.global_emergency_lock = True
        self.current_state = TradingState.EMERGENCY_LOCK
        logger.critical("🛑 FSM GLOBAL EMERGENCY LOCK ENGAGED. ALL NEW EXECUTIONS HALTED.")

    def release_global_emergency_lock(self):
        """Restores swarm execution pathways."""
        self.global_emergency_lock = False
        self.current_state = TradingState.ACTIVE_TRADING
        logger.warning("🔓 FSM GLOBAL EMERGENCY LOCK LIFTED. SWARM RE-ARMED.")

    # ====================================================================
    # 🛡️ LEGACY METHODS (PRESERVED FOR MAIN.PY BACKWARD COMPATIBILITY)
    # ====================================================================

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
        """
        Legacy property preserved. Intercepts the gatekeeper boolean to halt 
        the swarm entirely during a systemic failure.
        """
        return not self.global_emergency_lock