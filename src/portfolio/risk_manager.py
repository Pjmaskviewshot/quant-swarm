"""
💎 V69.0 APEX NEURAL: STRUCTURAL DYNAMICS RISK VAULT
------------------------------------------------------------
Features Natural Slot Allocation (Max 5, bounded strictly by margin math),
Recalibrated Sigmoidal Conviction Gates, Net Expected Value (EV) Edge Multipliers,
and Organic Leveraged Heat Caps (1.8x Multiplier).
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

        self.outcomes_history = deque(maxlen=100) 
        self.avg_win_r = 1.5   
        self.avg_loss_r = 1.0  
        self.volatility_surface: deque = deque(maxlen=300)

    def push_microstructure_variance(self, variance: float):
        if variance > 0:
            self.volatility_surface.append(variance)

    def calculate_evt_tail_risk(self) -> float:
        if len(self.volatility_surface) < 50:
            return 1.0  
            
        try:
            vol_arr = np.array(self.volatility_surface)
            var_threshold = np.percentile(vol_arr, 95)
            
            tail_variances = vol_arr[vol_arr >= var_threshold]
            if len(tail_variances) == 0: 
                return 1.0
                
            cvar = np.mean(tail_variances)
            
            if cvar > 0.0001:
                penalty = min(0.65, (cvar - 0.0001) * 5000.0)
                return max(0.35, 1.0 - penalty)
                
            return 1.05 
        except Exception as e:
            logger.debug(f"[X-RAY] CVaR calculation fallback engaged: {e}")
            return 1.0

    def update_correlation_matrix(self, price_histories: Dict[str, List[float]]):
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

    def get_dynamic_conviction_threshold(self, balance: float, net_edge_bps: float = 50.0) -> float:
        """
        🚀 V69.0 RECALIBRATED SIGMOIDAL THRESHOLD
        Sub-$20 balance requires ~57.5% base win probability.
        High Net EV (>140 bps) further lowers required conviction by up to 2.0%.
        """
        sigmoid = 1.0 / (1.0 + math.exp((balance - 30.0) / 7.0))
        base_threshold = 0.525 + (0.060 * sigmoid)  
        
        # High Expected Value (EV) Discount
        ev_discount = min(0.020, max(0.0, (net_edge_bps - 100.0) / 5000.0))
        
        return max(0.515, base_threshold - ev_discount)

    def get_max_allowed_slots(self, balance: float) -> int:
        """
        🚀 V69.0 NATURAL SLOT CAP
        Universal Max of 5. The engine natively restricts trades 
        if the real-time margin/heat cap math rejects the sizing.
        """
        return 5

    def calculate_optimal_fraction(self, base_confidence: float, net_edge_bps: float = 50.0, current_balance: float = 100.0) -> float:
        """
        🧬 V69.0 DYNAMIC EV-CONFLUENCE SIZING
        """
        # 1. Sigmoidal EV Conviction Check
        min_required_conviction = self.get_dynamic_conviction_threshold(current_balance, net_edge_bps)
        if base_confidence < min_required_conviction:
            return 0.0  # Rejected by Dynamic EV Gate

        # 2. Hybrid Allocation Sizing
        total_trades = len(self.outcomes_history)
        if total_trades < 10:
            raw_kelly = 0.010 
        else:
            p = float(np.mean(self.outcomes_history))
            p_win = max(0.01, min(0.99, p)) 
            b = self.avg_win_r / (self.avg_loss_r + 1e-9)
            
            if b <= 0:
                raw_kelly = 0.001
            else:
                kelly_fraction = p_win - ((1.0 - p_win) / b)
                if kelly_fraction <= 0:
                    return 0.0 
                raw_kelly = kelly_fraction / 2.0 
        
        evt_multiplier = self.calculate_evt_tail_risk()
        edge_factor = min(1.5, max(0.5, net_edge_bps / 50.0))
        drawdown_penalty = max(0.10, 1.0 - (self.current_drawdown_state * 3.0))
        
        risk_adjusted_kelly = raw_kelly * evt_multiplier * edge_factor * drawdown_penalty
        return max(0.001, min(self.max_single_risk, risk_adjusted_kelly))

    def evaluate_portfolio_safety(self, current_balance: float, new_position_notional: float = 0.0, symbol: str = "") -> Tuple[bool, str]:
        if self.emergency_circuit_breaker:
            return False, "EMERGENCY_CIRCUIT_BREAKER_ACTIVE"

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

        if symbol in self.active_positions:
            return False, f"DUPLICATE_SYMBOL_LOCK ({symbol})"

        # 🚀 Natural Slot Limit Check
        max_slots = self.get_max_allowed_slots(current_balance)
        if len(self.active_positions) >= max_slots:
            return False, f"DYNAMIC_SLOT_CAP_REACHED // Active: {len(self.active_positions)} >= Max Allowed: {max_slots}"

        evt_multiplier = self.calculate_evt_tail_risk()
        dynamic_corr_threshold = max(0.40, min(0.85, 0.85 * evt_multiplier - (self.current_drawdown_state * 1.5)))

        if symbol and new_position_notional > 0:
            if hasattr(self, 'correlation_matrix') and self.correlation_matrix is not None:
                for active_sym in self.active_positions.keys():
                    if active_sym in self.correlation_matrix.index and symbol in self.correlation_matrix.columns:
                        corr_value = self.correlation_matrix.loc[active_sym, symbol]
                        if corr_value > dynamic_corr_threshold:
                            return False, f"RISK_PARITY_BLOCK // {symbol} correlates {corr_value:.2f} with {active_sym} (Max allowed: {dynamic_corr_threshold:.2f})"

        # 🚀 V69.0 NATURAL HEAT MAP (Organic Scaling)
        # Allows an organic 1.8x leveraged equity multiplier to efficiently use available margin
        max_heat_dollars = max(self.exchange_min_notional * 2.5, current_balance * 1.8)
        
        total_exposure = sum(self.active_positions.values()) + new_position_notional

        if total_exposure > max_heat_dollars:
            return False, f"NATURAL_HEAT_CAP_EXCEEDED // Req: ${total_exposure:.2f} > Max Margin Available: ${max_heat_dollars:.2f}"
                
        return True, "SAFE"

    def update_position_ledger(self, symbol: str, notional_value: float):
        if notional_value <= 0:
            self.active_positions.pop(symbol, None)
        else:
            self.active_positions[symbol] = notional_value

    def clear_ledger(self):
        self.active_positions.clear()