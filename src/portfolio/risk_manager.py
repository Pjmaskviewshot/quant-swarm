"""
💎 V61.2 APEX NEURAL: UNIVERSAL INSTITUTIONAL RISK VAULT
------------------------------------------------------------
Scale-Invariant Kelly Sizing with Volatility-Adjusted CVaR Protection,
Continuous Logarithmic Micro-Account Adaptation ($5 to $1,000,000+),
Volatility-Elastic Correlation Matrices, and Drawdown-Modulated Heat Limits.
Upgraded with Asymmetric Risk Aversion to eradicate Kelly Death Spirals.
"""

import math
import logging
from typing import Dict, List, Any, Optional, Tuple
from collections import deque
import numpy as np
import pandas as pd

logger = logging.getLogger("QUANT_CORE.RISK_VAULT")


class InstitutionalRiskVault:
    def __init__(
        self, 
        max_drawdown_pct: float = 0.30, 
        max_single_position_risk_pct: float = 0.015,
        exchange_min_notional: float = 6.50
    ):
        self.max_drawdown_pct = max_drawdown_pct
        self.max_single_risk = max_single_position_risk_pct
        self.exchange_min_notional = exchange_min_notional
        
        self.absolute_max_leverage: float = 5.0
        self.base_leverage: float = 3.0
        
        self.peak_balance = 0.0
        self.current_drawdown_state = 0.0
        self.emergency_circuit_breaker = False
        
        self.active_positions: Dict[str, float] = {}
        self.correlation_matrix: Optional[pd.DataFrame] = None

        # Empirical Bayesian Priors for Kelly
        self.outcomes_history = deque(maxlen=100) 
        self.avg_win_r = 1.5   
        self.avg_loss_r = 1.0  
        
        # High-Frequency Volatility Surface
        self.volatility_surface: deque = deque(maxlen=300)

    def push_microstructure_variance(self, variance: float):
        if variance > 0:
            self.volatility_surface.append(variance)

    def calculate_evt_tail_risk(self) -> float:
        """
        🚀 V61.2: Conditional Value at Risk (CVaR) Engine.
        Analyzes the 95th percentile of the microstructure volatility surface.
        Returns a continuous penalty multiplier (0.35 to 1.05) to choke risk during tail events.
        """
        if len(self.volatility_surface) < 50:
            return 1.0  # Neutral baseline prior to surface mapping
            
        try:
            vol_arr = np.array(self.volatility_surface)
            var_threshold = np.percentile(vol_arr, 95)
            
            tail_variances = vol_arr[vol_arr >= var_threshold]
            if len(tail_variances) == 0: 
                return 1.0
                
            cvar = np.mean(tail_variances)
            
            # Map CVaR to a compression multiplier
            if cvar > 0.0001:
                penalty = min(0.65, (cvar - 0.0001) * 5000.0)
                tail_risk_multiplier = max(0.35, 1.0 - penalty)
                return tail_risk_multiplier
                
            return 1.05 # Slight sizing premium in ultra-stable (low CVaR) regimes
        except Exception as e:
            logger.debug(f"[X-RAY] CVaR calculation fallback engaged: {e}")
            return 1.0

    def update_correlation_matrix(self, price_histories: Dict[str, List[float]]):
        """Updates the pairwise correlation matrix to prevent over-exposure to a single sector."""
        try:
            min_len = min([len(prices) for prices in price_histories.values()])
            if min_len < 30: 
                return
            
            trimmed_histories = {sym: prices[-min_len:] for sym, prices in price_histories.items()}
            df = pd.DataFrame(trimmed_histories)
            
            returns_df = np.log(df / df.shift(1)).dropna()
            self.correlation_matrix = returns_df.corr()
            logger.info("[X-RAY] 🧠 Risk Parity Matrix Updated: Full pairwise correlation computed.")
        except Exception as e:
            logger.debug(f"[X-RAY] Failed to compute correlation matrix: {e}")

    def update_kelly_metrics(self, is_win: bool, realized_r_multiple: float):
        self.outcomes_history.append(1.0 if is_win else 0.0)
        
        if is_win:
            capped_r = min(5.0, max(0.1, abs(realized_r_multiple)))
            self.avg_win_r = (self.avg_win_r * 0.95) + (capped_r * 0.05)
        else:
            capped_r = min(2.5, max(0.1, abs(realized_r_multiple)))
            self.avg_loss_r = (self.avg_loss_r * 0.95) + (capped_r * 0.05)

    def calculate_optimal_fraction(self, base_confidence: float, net_edge_bps: float = 50.0) -> float:
        """
        🧬 V61.2: Asymmetric Drawdown-Modulated Half-Kelly.
        Calculates optimal allocation fraction, exponentially penalized by current portfolio drawdown.
        Erases the 'Death Spiral' by making the risk floor scale-invariant.
        """
        total_trades = len(self.outcomes_history)
        
        # 1. Calculate the Raw Kelly Fraction
        if total_trades < 10:
            raw_kelly = 0.010 # Flat 1% bootstrap prior
        else:
            p = float(np.mean(self.outcomes_history))
            p = min(0.75, max(0.25, p)) # Bound probability bounds
            q = 1.0 - p
            b = self.avg_win_r / (self.avg_loss_r + 1e-9)
            
            if b <= 0:
                raw_kelly = 0.001
            else:
                f_star = (p * b - q) / b
                raw_kelly = max(0.001, f_star / 2.0) # Standard Half-Kelly
        
        # 2. Extract Sub-Matrix Risk Multipliers
        evt_multiplier = self.calculate_evt_tail_risk()
        edge_factor = min(1.5, max(0.5, net_edge_bps / 50.0))
        
        # 3. 🛡️ ASYMMETRIC RISK AVERSION (Drawdown Penalty)
        # If drawdown is 10%, multiplier drops to 0.70. If 20%, multiplier drops to 0.40.
        drawdown_penalty = max(0.10, 1.0 - (self.current_drawdown_state * 3.0))
        
        # 4. 🚀 CONTINUOUS MICRO-FLOOR (No Hard Limits)
        # $14 account = ~1.4% floor. $1,000 account = 0.1% floor.
        current_bal = self.peak_balance * (1.0 - self.current_drawdown_state)
        dynamic_operational_floor = max(0.001, 0.015 * math.exp(-current_bal / 200.0))
        
        # Combine vectors
        risk_adjusted_kelly = raw_kelly * evt_multiplier * edge_factor * drawdown_penalty
        
        # Enforce ceilings and dynamic floors
        final_fraction = max(dynamic_operational_floor, min(self.max_single_risk, risk_adjusted_kelly))
        
        return final_fraction

    def evaluate_portfolio_safety(self, current_balance: float, new_position_notional: float = 0.0, symbol: str = "") -> Tuple[bool, str]:
        """
        🚀 V61.2: Volatility-Elastic Scale-Invariant Safety Gates.
        """
        if self.emergency_circuit_breaker:
            return False, "EMERGENCY_CIRCUIT_BREAKER_ACTIVE"

        # 1. Update Peak Equity and Drawdown State
        if current_balance > self.peak_balance:
            self.peak_balance = current_balance
            self.current_drawdown_state = 0.0
        elif self.peak_balance > 0:
            self.current_drawdown_state = (self.peak_balance - current_balance) / self.peak_balance
            
            if self.current_drawdown_state >= self.max_drawdown_pct:
                if not self.emergency_circuit_breaker:
                    logger.critical(f"🚨 ABSOLUTE MAX DRAWDOWN BREACHED ({self.current_drawdown_state:.2%}). LOCKING DOWN SYSTEMS.")
                    self.emergency_circuit_breaker = True
                return False, f"MAX_DRAWDOWN_BREACHED_{self.current_drawdown_state:.2%}"

        # 2. Deduplicate Symbols
        if symbol in self.active_positions:
            return False, f"DUPLICATE_SYMBOL_LOCK ({symbol})"

        # 3. 🧬 VOLATILITY-ELASTIC CORRELATION GATE
        # In stable markets, allow up to 80% correlation.
        # In high tail-risk (CVaR) or drawdown regimes, choke correlation down to 40%.
        evt_multiplier = self.calculate_evt_tail_risk()
        dynamic_corr_threshold = max(0.40, min(0.85, 0.85 * evt_multiplier - (self.current_drawdown_state * 1.5)))

        if symbol and new_position_notional > 0:
            if hasattr(self, 'correlation_matrix') and self.correlation_matrix is not None:
                for active_sym in self.active_positions.keys():
                    if active_sym in self.correlation_matrix.index and symbol in self.correlation_matrix.columns:
                        corr_value = self.correlation_matrix.loc[active_sym, symbol]
                        if corr_value > dynamic_corr_threshold:
                            return False, f"RISK_PARITY_BLOCK // {symbol} correlates {corr_value:.2f} with {active_sym} (Max allowed: {dynamic_corr_threshold:.2f})"

        # 4. 🚀 CONTINUOUS SIDNA HEAT MAP
        # Eradicates the $50 hard cliff. Uses an asymptotic curve to determine max concurrent exposure.
        # $14 -> allows ~80% heat (to fit $6.50 trades). $1000 -> allows ~25% heat. $10k+ -> 10% heat.
        logistic_heat_allowance = 0.10 + 0.85 * math.exp(-current_balance / 100.0)
        
        # Calculate max allowable notional in dollars
        max_heat_dollars = max(self.exchange_min_notional * 2.5, current_balance * logistic_heat_allowance)
        total_exposure = sum(self.active_positions.values()) + new_position_notional

        if total_exposure > max_heat_dollars:
            return False, f"SIDNA_HEAT_CAP_EXCEEDED // Req: ${total_exposure:.2f} > Max: ${max_heat_dollars:.2f}"
                
        return True, "SAFE"

    def update_position_ledger(self, symbol: str, notional_value: float):
        if notional_value <= 0:
            self.active_positions.pop(symbol, None)
        else:
            self.active_positions[symbol] = notional_value

    def clear_ledger(self):
        self.active_positions.clear()