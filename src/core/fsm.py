from enum import Enum
import logging

class TradingState(Enum):
    BOOTSTRAPPING = "BOOTSTRAPPING"
    CALIBRATING = "CALIBRATING"
    ACTIVE_TRADING = "ACTIVE_TRADING"
    EMERGENCY_LOCK = "EMERGENCY_LOCK"

logger = logging.getLogger("QUANT_CORE.FSM")

class SystemStateMachine:
    def __init__(self, accuracy_threshold: float = 0.65, warmup_epochs: int = 10):
        self.current_state = TradingState.BOOTSTRAPPING
        self.accuracy_threshold = accuracy_threshold
        self.warmup_epochs = warmup_epochs
        logger.info(f"FSM Initialized in state: {self.current_state.value}")

    def process_state_transition(self, rolling_accuracy: float, total_resolved: int) -> TradingState:
        """Evaluates operational metrics and transitions system state boundaries."""
        old_state = self.current_state

        # 1. Warmup Evaluation Phase
        if total_resolved < self.warmup_epochs:
            self.current_state = TradingState.CALIBRATING
            
        # 2. Performance Boundary Evaluation
        elif rolling_accuracy >= self.accuracy_threshold:
            if self.current_state in [TradingState.CALIBRATING, TradingState.EMERGENCY_LOCK, TradingState.BOOTSTRAPPING]:
                self.current_state = TradingState.ACTIVE_TRADING
        else:
            # Underperformance triggers an immediate safety decoupling lock
            if self.current_state == TradingState.ACTIVE_TRADING:
                self.current_state = TradingState.EMERGENCY_LOCK

        if old_state != self.current_state:
            logger.critical(f"STATE TRANSITION DETECTED: {old_state.value} ➡️ {self.current_state.value} | Accuracy: {rolling_accuracy:.2%}")
            
        return self.current_state

    @property
    def can_execute_trades(self) -> bool:
        """Gatekeeper attribute checking execution safety bounds."""
        return self.current_state == TradingState.ACTIVE_TRADING