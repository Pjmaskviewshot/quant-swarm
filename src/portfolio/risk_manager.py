"""
💎 V58.0 TITANIUM APEX: UNIVERSAL INSTITUTIONAL RISK VAULT
------------------------------------------------------------
Scale-Invariant Kelly Sizing with Volatility-Adjusted CVaR Protection,
Micro-Account Notional Adaptation ($5 to $1,000,000+),
Full Pairwise Correlation Matrices, and Dynamic Drawdown Circuit Breakers.
Upgraded with Minimum Operational Risk Floors to eradicate Kelly Death Spirals.
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
        max_drawdown_pct: float = 0.25, 
        max_single_position_risk_pct: float = 0.015,
        exchange_min_notional: float = 6.0
    ):
        self.max_drawdown_pct = max_drawdown_pct
        self.max_single_risk = max_single_position_risk_pct
        self.max_single_position_risk_pct = max_single_position_risk_pct
        self.exchange_min_notional = exchange_min_notional
        
        self.absolute_max_leverage: float = 5.0
        self.base_leverage: float = 3.0
        
        self.peak_balance = 0.0
        self.emergency_circuit_breaker = False
        
        self.active_positions: Dict[str, float] = {}
        self.correlation_matrix: Optional[pd.DataFrame] = None

        self.outcomes_history = deque(maxlen=100) 
        self.avg_win_r = 1.5   
        self.avg_loss_r = 1.0  
        
        self.volatility_surface: deque = deque(maxlen=300)

    def push_microstructure_variance(self, variance: float):
        if variance > 0:
            self.volatility_surface.append(variance)

    def calculate_evt_tail_risk(self) -> float:
        """
        Calculates the Conditional Value at Risk (CVaR) of the volatility surface.
        Penalizes position sizing during extreme volatility clustering.
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
            
            if cvar > 0.0001:
                penalty = min(0.65, (cvar - 0.0001) * 5000.0)
                tail_risk_multiplier = max(0.35, 1.0 - penalty)
                return tail_risk_multiplier
                
            return 1.0
        except Exception as e:
            logger.debug(f"[X-RAY] CVaR calculation fallback engaged: {e}")
            return 1.05

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
        🚀 V58.0 UPGRADE: Scale-Invariant Kelly Sizing with Anti-Starvation Floor.
        Calculates the optimal portfolio fraction to risk based on rolling performance.
        """
        total_trades = len(self.outcomes_history)
        
        # 🚀 FIX: Minimum Operational Risk Floor (0.3% Account Risk)
        min_operational_floor = 0.003
        
        if total_trades < 10:
            base_fraction = 0.010 
        else:
            p = float(np.mean(self.outcomes_history))
            p = min(0.75, p)
            q = 1.0 - p
            
            b = self.avg_win_r / (self.avg_loss_r + 1e-9)
            
            if b <= 0:
                base_fraction = min_operational_floor
            else:
                kelly_fraction = (p * b - q) / b
                
                if kelly_fraction <= 0:
                    base_fraction = min_operational_floor
                else:
                    base_fraction = max(min_operational_floor, kelly_fraction / 2.0) # Half-Kelly
        
        evt_multiplier = self.calculate_evt_tail_risk()
        edge_factor = min(1.5, max(0.5, net_edge_bps / 50.0))
        
        risk_adjusted_kelly = base_fraction * evt_multiplier * edge_factor
        
        # Guarantee we never return 0.0 unless the edge is completely invalid upstream
        return max(min_operational_floor, min(self.max_single_risk, risk_adjusted_kelly))

    def evaluate_portfolio_safety(self, current_balance: float, new_position_notional: float = 0.0, symbol: str = "") -> Tuple[bool, str]:
        """Evaluates if a new position breaches systemic portfolio constraints."""
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

        # 🚀 V58.0 SCALE-INVARIANT DYNAMIC NOTIONAL ADAPTER (SIDNA)
        # Bypasses percentage caps for micro-accounts (< $50) so $6.00-$6.50 minimum orders can clear.
        if current_balance < 50.0:
            active_count = len(self.active_positions)
            if active_count >= 3:  # Max 3 concurrent micro-account trades
                return False, f"MICRO_ACCOUNT_MAX_POSITIONS_REACHED ({active_count}/3)"
            
            if symbol in self.active_positions:
                return False, f"DUPLICATE_SYMBOL_LOCK ({symbol})"
                
            return True, "SAFE_MICRO_ACCOUNT"

        # INSTITUTIONAL PORTFOLIO CHECKS ($50+ Balance)
        if symbol and new_position_notional > 0:
            if hasattr(self, 'correlation_matrix') and self.correlation_matrix is not None:
                for active_sym in self.active_positions.keys():
                    if active_sym in self.correlation_matrix.index and symbol in self.correlation_matrix.columns:
                        corr_value = self.correlation_matrix.loc[active_sym, symbol]
                        # Veto trades that are > 75% correlated with an active position
                        if corr_value > 0.75:
                            return False, f"RISK_PARITY_BLOCK_CORRELATED_{corr_value:.2f}_WITH_{active_sym}"

        total_exposure = sum(self.active_positions.values()) + new_position_notional
        max_heat = current_balance * 0.60  # Max 60% total portfolio heat

        if total_exposure > max_heat:
            return False, f"PORTFOLIO_HEAT_EXCEEDED_MAX_{max_heat:.2f}_REQ_{total_exposure:.2f}"
                
        return True, "SAFE"

    def update_position_ledger(self, symbol: str, notional_value: float):
        if notional_value <= 0:
            self.active_positions.pop(symbol, None)
        else:
            self.active_positions[symbol] = notional_value

    def clear_ledger(self):
        self.active_positions.clear()