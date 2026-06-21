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
    def __init__(self, accuracy_threshold: float = 0.65, warmup_epochs: int = 10):
        self.current_state = TradingState.BOOTSTRAPPING
        self.accuracy_threshold = accuracy_threshold
        self.warmup_epochs = warmup_epochs
        logger.info(f"FSM Initialized in state: {self.current_state.value}")

    def process_state_transition(self, rolling_accuracy: float, total_resolved: int, market_regime: str = "TRENDING") -> TradingState:
        """
        🚀 UPGRADE: REGIME-AWARE EVALUATION
        Evaluates operational metrics and transitions system state boundaries contextually based on the market regime.
        """
        old_state = self.current_state

        # 1. Warmup Evaluation Phase
        if total_resolved < self.warmup_epochs:
            self.current_state = TradingState.CALIBRATING
            
        # 2. Peak Performance Boundary (Breakout Mode Authorized)
        elif rolling_accuracy >= self.accuracy_threshold:
            self.current_state = TradingState.ACTIVE_TRADING
            
        # 3. Underperformance / Regime Shift Response
        else:
            # If the breakout strategy is losing but the market is visibly chopping sideways,
            # we don't lock down—we deploy the Accumulation/Mean Reversion strategy.
            if market_regime == "RANGING":
                self.current_state = TradingState.ACTIVE_MEAN_REVERSION
            else:
                # If the market is trending but we are still losing, the logic is compromised.
                # Trigger a hard safety decoupling lock.
                self.current_state = TradingState.EMERGENCY_LOCK

        if old_state != self.current_state:
            logger.critical(f"STATE TRANSITION DETECTED: {old_state.value} ➡️ {self.current_state.value} | Accuracy: {rolling_accuracy:.2%} | Regime: {market_regime}")
            
        return self.current_state

    @property
    def can_execute_trades(self) -> bool:
        """Gatekeeper attribute checking execution safety bounds."""
        # 🚀 UPGRADE: Both ACTIVE_TRADING and ACTIVE_MEAN_REVERSION are cleared for live capital.
        return self.current_state in [TradingState.ACTIVE_TRADING, TradingState.ACTIVE_MEAN_REVERSION]