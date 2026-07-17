import logging
from enum import Enum

logger = logging.getLogger("QUANT_CORE.FSM")

class TradingState(Enum):
    BOOTSTRAPPING = "BOOTSTRAPPING"
    CALIBRATING = "SWARM_CALIBRATING"
    ACTIVE_TRADING = "DECENTRALIZED_ACTIVE"
    ACTIVE_MEAN_REVERSION = "DECENTRALIZED_MEAN_REV"
    EMERGENCY_LOCK = "EMERGENCY_LOCK"

class SystemStateMachine:
    """
    🚀 V4 DEPRECATION NOTICE:
    The monolithic global FSM has been officially annihilated and replaced by 
    Decentralized Bayesian Node Grading inside main.py.
    
    This class is preserved strictly as a lightweight, passive shell to prevent 
    ImportErrors and legacy dependency crashes. It has zero authority over live capital.
    """
    def __init__(self, accuracy_threshold: float = 0.60, warmup_epochs: int = 150):
        self.current_state = TradingState.BOOTSTRAPPING
        logger.info("FSM Shell Initialized: Global logic bypassed in favor of Per-Node Swarm mechanics.")

    def process_state_transition(self, rolling_accuracy: float, total_resolved: int, market_regime: str = "TRENDING") -> TradingState:
        """
        Legacy method preserved to prevent crashes if called by older modules.
        Returns static state because V4 decentralized nodes handle their own state transitions.
        """
        return self.current_state

    @property
    def can_execute_trades(self) -> bool:
        """
        Legacy property preserved. Always returns True because the actual execution 
        gatekeeping is now handled securely by the Bayesian Edge matrix in main.py.
        """
        return True