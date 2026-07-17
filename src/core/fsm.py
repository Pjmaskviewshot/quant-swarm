from enum import Enum
import logging

class TradingState(Enum):
    BOOTSTRAPPING = "BOOTSTRAPPING"
    CALIBRATING = "CALIBRATING"
    ACTIVE_TRADING = "ACTIVE_TRADING"
    ACTIVE_MEAN_REVERSION = "ACTIVE_MEAN_REVERSION"  # 🚀 UPGRADE: New Offensive State
    EMERGENCY_LOCK = "EMERGENCY_LOCK"

logger = logging.getLogger("QUANT_CORE.FSM")

class SystemStateMachine:
    def __init__(self, accuracy_threshold: float = 0.60, warmup_epochs: int = 150):
        self.current_state = TradingState.BOOTSTRAPPING
        self.accuracy_threshold = accuracy_threshold
        self.warmup_epochs = warmup_epochs
        logger.info(f"FSM Initialized in state: {self.current_state.value}")

    def process_state_transition(self, rolling_accuracy: float, total_resolved: int, market_regime: str = "TRENDING") -> TradingState:
        """
        🚀 UPGRADE: REGIME-AWARE EVALUATION WITH HYSTERESIS
        Evaluates operational metrics and transitions system state boundaries contextually based on the market regime.
        Includes a 2.5% deadband to prevent rapid state oscillation (flapping).
        """
        old_state = self.current_state
        deadband = 0.025  # 2.5% Hysteresis Buffer

        # 1. Warmup Evaluation Phase
        if total_resolved < self.warmup_epochs:
            self.current_state = TradingState.CALIBRATING
            if old_state != self.current_state:
                logger.critical(f"STATE TRANSITION DETECTED: {old_state.value} ➡️ {self.current_state.value} | Warmup: {total_resolved}/{self.warmup_epochs}")
            return self.current_state

        # 2. Dynamic Threshold Calculation (Hysteresis Logic)
        # If we are already trading, make it slightly harder to lock down (Threshold - 2.5%)
        # If we are locked down, make it harder to unlock (Threshold + 2.5%)
        is_active = self.current_state in [TradingState.ACTIVE_TRADING, TradingState.ACTIVE_MEAN_REVERSION]
        effective_threshold = (self.accuracy_threshold - deadband) if is_active else (self.accuracy_threshold + deadband)

        # 3. Peak Performance Boundary (Edge is Mathematically Verified)
        if rolling_accuracy >= effective_threshold:
            # Route into the correct execution mode based on current macro regime
            if market_regime == "RANGING":
                self.current_state = TradingState.ACTIVE_MEAN_REVERSION
            else:
                self.current_state = TradingState.ACTIVE_TRADING
                
        # 4. Underperformance / Regime Shift Response
        else:
            # 🛑 CRITICAL LOGIC FIX: The FSM must protect capital.
            # Previously, the bot would stay ACTIVE_MEAN_REVERSION simply because the market was RANGING,
            # even if rolling accuracy dropped to catastrophic levels. Now, if edge is lost, it locks down unconditionally.
            self.current_state = TradingState.EMERGENCY_LOCK

        if old_state != self.current_state:
            logger.critical(f"STATE TRANSITION DETECTED: {old_state.value} ➡️ {self.current_state.value} | Accuracy: {rolling_accuracy:.2%} | Target: {effective_threshold:.2%} | Regime: {market_regime}")
            
        return self.current_state

    @property
    def can_execute_trades(self) -> bool:
        """Gatekeeper attribute checking execution safety bounds."""
        # 🚀 UPGRADE: Both ACTIVE_TRADING and ACTIVE_MEAN_REVERSION are cleared for live capital.
        return self.current_state in [TradingState.ACTIVE_TRADING, TradingState.ACTIVE_MEAN_REVERSION]