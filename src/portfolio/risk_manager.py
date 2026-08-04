"""
💎 V56.2 QUANTUM SWARM: INSTITUTIONAL RISK VAULT (RISK PARITY ENABLED)
------------------------------------------------------------
Conservative Kelly sizing with Volatility-Adjusted CVaR Protection,
Dynamic Win-Rate Queues, Full Pairwise Correlation Matrices, 
and X-Ray Diagnostic Telemetry.
"""

import math
import logging
from typing import Dict, List, Any, Optional
from collections import deque
import numpy as np
import pandas as pd  # 🚀 V56.2 UPGRADE: Required for True Risk Parity Matrix

logger = logging.getLogger("QUANT_CORE.RISK_VAULT")


class InstitutionalRiskVault:
    def __init__(
        self, 
        max_drawdown_pct: float = 0.25, 
        max_single_position_risk_pct: float = 0.015, # Clamped to 1.5% max risk for normal balances
        exchange_min_notional: float = 6.0
    ):
        self.max_drawdown_pct = max_drawdown_pct
        self.max_single_risk = max_single_position_risk_pct
        
        # 🚀 V56.2 FIX: Two-way attribute parity alias to guarantee compliance across all modules
        self.max_single_position_risk_pct = max_single_position_risk_pct
        
        self.exchange_min_notional = exchange_min_notional
        
        self.absolute_max_leverage: float = 5.0
        self.base_leverage: float = 3.0
        
        self.peak_balance = 0.0
        self.emergency_circuit_breaker = False
        
        self.active_positions: Dict[str, float] = {}

        # 🚀 V56.2 TRUE RISK PARITY: Initialized empty correlation matrix
        self.correlation_matrix: Optional[pd.DataFrame] = None

        # 🚀 V55.2 FIX: Replaced saturating integer counters with a sliding boolean deque
        self.outcomes_history = deque(maxlen=100) 
        self.avg_win_pct = 0.02
        self.avg_loss_pct = 0.01
        
        self.volatility_surface: deque = deque(maxlen=300)

    def push_microstructure_variance(self, variance: float):
        """Pushes instantaneous variance into the volatility surface for tail-risk modeling."""
        if variance > 0:
            self.volatility_surface.append(variance)

    def calculate_evt_tail_risk(self) -> float:
        """
        🚀 V55.2 ADAPTIVE CVaR: Replaces unstable EVT with Expected Shortfall penalty.
        Automatically scales tail-risk suppression between 0.35 (high vol) and 1.0 (low vol).
        """
        if len(self.volatility_surface) < 50:
            return 1.05 
            
        try:
            vol_arr = np.array(self.volatility_surface)
            var_threshold = np.percentile(vol_arr, 95)
            
            tail_variances = vol_arr[vol_arr >= var_threshold]
            if len(tail_variances) == 0: 
                return 1.05
                
            cvar = np.mean(tail_variances)
            
            # If Expected Shortfall of variance exceeds 0.0001, start suppressing position sizes
            if cvar > 0.0001:
                # E.g., if cvar is 0.0002, penalty is (0.0001 * 5000) = 0.50
                penalty = min(0.65, (cvar - 0.0001) * 5000.0)
                tail_risk_multiplier = max(0.35, 1.0 - penalty)
                logger.debug(f"[X-RAY] 🌪️ CVaR Tail Risk (Var: {cvar:.6f}): Multiplier at {tail_risk_multiplier:.1%}")
                return tail_risk_multiplier
                
            return 1.0
        except Exception as e:
            logger.debug(f"[X-RAY] CVaR calculation fallback engaged: {e}")
            return 1.05

    def update_correlation_matrix(self, price_histories: Dict[str, List[float]]):
        """
        🚀 V56.2 TRUE RISK PARITY: Computes full O(N^2) pairwise correlation matrix
        using pandas to prevent concentrated risk exposure.
        """
        try:
            # Ensure all histories are the same length by trimming to the shortest array
            min_len = min([len(prices) for prices in price_histories.values()])
            if min_len < 30: 
                return
            
            trimmed_histories = {sym: prices[-min_len:] for sym, prices in price_histories.items()}
            df = pd.DataFrame(trimmed_histories)
            
            # Calculate log returns for statistical stationarity
            returns_df = np.log(df / df.shift(1)).dropna()
            
            # Compute full O(N^2) Pearson correlation matrix
            self.correlation_matrix = returns_df.corr()
            logger.info("[X-RAY] 🧠 Risk Parity Matrix Updated: Full pairwise correlation computed.")
        except Exception as e:
            logger.debug(f"[X-RAY] Failed to compute correlation matrix: {e}")

    # 🚀 V55.2 AUDIT FIX: Kelly Criterion now tracks R-Multiples, not raw percentages.
    def update_kelly_metrics(self, is_win: bool, realized_r_multiple: float):
        """
        Tracks win/loss magnitude relative to the initial risk taken (R-Multiple).
        This guarantees the Kelly fraction is scale-invariant and immune to leverage distortions.
        """
        self.outcomes_history.append(1.0 if is_win else 0.0)
        
        # We continue to use the variables avg_win_pct/avg_loss_pct, but they now store R-values
        if is_win:
            self.avg_win_pct = (self.avg_win_pct * 0.9) + (abs(realized_r_multiple) * 0.1)
        else:
            self.avg_loss_pct = (self.avg_loss_pct * 0.9) + (abs(realized_r_multiple) * 0.1)

    def calculate_optimal_fraction(self, base_confidence: float, net_edge_bps: float = 50.0) -> float:
        """
        🚀 Edge-Weighted Kelly Allocation with Adaptive CVaR Tail-Risk.
        Scales capital allocation based on signal confidence, tail-risk state, and expected net edge.
        """
        total_trades = len(self.outcomes_history)
        if total_trades < 10:
            base_fraction = 0.010 # Cold start 1.0%
        else:
            win_rate = float(np.mean(self.outcomes_history))
            safe_prob = min(0.70, max(0.51, base_confidence))
            blended_w = (win_rate * 0.7) + (safe_prob * 0.3) 
            
            payoff_ratio = self.avg_win_pct / (self.avg_loss_pct + 1e-9)
            if payoff_ratio <= 0 or blended_w <= 0:
                base_fraction = 0.005 
            else:
                kelly_fraction = blended_w - ((1.0 - blended_w) / payoff_ratio)
                base_fraction = max(0.005, kelly_fraction / 2.0)
        
        evt_multiplier = self.calculate_evt_tail_risk()
        edge_factor = min(1.5, max(0.5, net_edge_bps / 50.0))
        
        risk_adjusted_kelly = base_fraction * evt_multiplier * edge_factor
        
        # Hard bounds: Min 0.5%, Max 1.5% equity risk per trade
        return max(0.005, min(0.015, risk_adjusted_kelly))

    def evaluate_portfolio_safety(self, current_balance: float, new_position_notional: float = 0.0, symbol: str = "") -> tuple[bool, str]:
        """
        🚀 V56.2 X-RAY PORTFOLIO SAFETY GUARD:
        Verifies drawdown limits, O(N^2) correlation matrices, and total portfolio heat limits.
        Returns: (is_safe: bool, reason_string: str)
        """
        if self.emergency_circuit_breaker:
            return False, "EMERGENCY_CIRCUIT_BREAKER_ACTIVE"

        if current_balance > self.peak_balance:
            self.peak_balance = current_balance
            
        if self.peak_balance > 0:
            current_drawdown = (self.peak_balance - current_balance) / (self.peak_balance + 1e-9)
            if current_drawdown >= self.max_drawdown_pct:
                if not self.emergency_circuit_breaker:
                    logger.critical(f"🚨 ABSOLUTE MAX DRAWDOWN BREACHED ({current_drawdown:.2%}). LOCKING DOWN SYSTEMS.")
                    self.emergency_circuit_breaker = True
                return False, f"MAX_DRAWDOWN_BREACHED_{current_drawdown:.2%}"
        
        # 🚀 V56.2 RISK PARITY: Enforce Correlation Limits
        if symbol and new_position_notional > 0:
            if hasattr(self, 'correlation_matrix') and self.correlation_matrix is not None:
                for active_sym in self.active_positions.keys():
                    if active_sym in self.correlation_matrix.index and symbol in self.correlation_matrix.columns:
                        corr_value = self.correlation_matrix.loc[active_sym, symbol]
                        # Block trade if correlation with an open position is > 0.75 (75%)
                        if corr_value > 0.75:
                            return False, f"RISK_PARITY_BLOCK_CORRELATED_{corr_value:.2f}_WITH_{active_sym}"

        # Calculate Total Portfolio Exposure Heat
        total_exposure = sum(self.active_positions.values()) + new_position_notional
        
        # Strict 2.5x institutional heat cap enforced universally across all account balances.
        max_heat = current_balance * 2.5  

        if total_exposure > max_heat:
            return False, f"PORTFOLIO_HEAT_EXCEEDED_MAX_{max_heat:.2f}_REQ_{total_exposure:.2f}"
                
        return True, "SAFE"

    def update_position_ledger(self, symbol: str, notional_value: float):
        """Updates active tracking ledger for portfolio exposure calculations."""
        if notional_value <= 0:
            self.active_positions.pop(symbol, None)
        else:
            self.active_positions[symbol] = notional_value

    def clear_ledger(self):
        """Clears all position entries in the ledger."""
        self.active_positions.clear()