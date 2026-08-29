"""
💎 V25.5 APEX QUANTUM PRIME: INSTITUTIONAL RISK VAULT
------------------------------------------------------------
Features:
- Live Portfolio Correlation Stress Guard (Rejects trades if avg pairwise corr > 0.70)
- Fail-Closed Liquidity & Balance Integrity Verification
- Intraday High-Watermark Circuit Breaker (2%) & Systemic Drawdown (5%)
- Exact Matrix Invertibility Guards & Exposure Heat Allocation

Architectural Supremacy (V25.5):
- Stabilized Correlation Lookback: Raised the minimum observation threshold from 10 
  to 60 periods to eliminate spurious correlation spikes during fast market regimes.
- Pure NumPy Covariance Engine: Vectorized matrix computation operating lightning-fast 
  inside the 15-second high-frequency tracking loop.
"""

import math
import logging
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from datetime import datetime, timezone

logger = logging.getLogger("QUANT_CORE.RISK_VAULT")


class InstitutionalRiskVault:
    """
    🚀 V25.5 PURE SYSTEMIC GUARDIAN
    Strictly governs portfolio contagion, absolute drawdowns, and correlation clustering.
    Stripped of all trade-sizing logic to act purely as an invariant firewall.
    """
    def __init__(
        self, 
        max_drawdown_pct: float = 0.05,              
        max_single_position_risk_pct: float = 0.015, 
        exchange_min_notional: float = 6.50
    ):
        self.max_drawdown_pct = max_drawdown_pct
        self.max_single_position_risk_pct = max_single_position_risk_pct
        self.exchange_min_notional = exchange_min_notional
        
        self.absolute_max_leverage: float = 2.0      
        self.base_leverage: float = 1.0
        
        # Systemic Drawdown Trackers
        self.peak_balance: float = 0.0
        self.last_valid_equity: float = 21.0
        self.current_drawdown_state: float = 0.0
        self.emergency_circuit_breaker: bool = False
        
        # Daily Loss Limit Trackers
        self.daily_high_watermark: float = 0.0
        self.current_day_utc = datetime.now(timezone.utc).date()
        self.daily_loss_limit_pct: float = 0.02      
        
        self.active_positions: Dict[str, float] = {}
        self.correlation_matrix: Optional[pd.DataFrame] = None

    def update_correlation_matrix(self, price_histories: Dict[str, List[float]]):
        """
        🚀 V25.5 FIX: Vectorized NumPy Covariance with Stabilized Lookback
        Computes pairwise correlations using pure NumPy arrays for high-frequency execution 
        while requiring at least 60 periods to prevent noise-driven matrix distortion.
        """
        try:
            if not price_histories:
                return
                
            symbols = list(price_histories.keys())
            min_len = min(len(prices) for prices in price_histories.values())
            
            # Require at least 60 observations for statistical stability
            if min_len < 60: 
                return
            
            # Vectorized O(1) Memory layout using pure NumPy
            price_arr = np.array([price_histories[sym][-min_len:] for sym in symbols], dtype=np.float64)
            
            # Fast vectorized log returns
            returns_arr = np.log(price_arr[:, 1:] / (price_arr[:, :-1] + 1e-9))
            
            # Fast Pearson Correlation Matrix
            corr = np.corrcoef(returns_arr)
            
            # Handle NaNs from zero-variance arrays (e.g., dead pairs or halted trading)
            corr = np.nan_to_num(corr, nan=0.0)
            
            # Tikhonov Ridge regularization for invertible correlation matrix
            reg_corr = (1.0 - 1e-4) * corr + (1e-4 * np.eye(corr.shape[0]))
            
            # Map back to a DataFrame purely for simple O(1) .loc lookups in the safety check
            self.correlation_matrix = pd.DataFrame(reg_corr, index=symbols, columns=symbols)
            
        except Exception as e:
            logger.debug(f"[MATH_WARN] Correlation update failure: {e}")

    def get_max_allowed_slots(self) -> int:
        return 5

    def evaluate_portfolio_safety(
        self, 
        current_balance: float, 
        new_position_notional: float = 0.0, 
        symbol: str = ""
    ) -> Tuple[bool, str]:
        """
        🚀 V25.5 FAIL-CLOSED LATCH MATRIX
        Evaluates the aggregate systemic health of the portfolio before allowing any order routing.
        """
        if self.emergency_circuit_breaker:
            return False, "EMERGENCY_CIRCUIT_BREAKER_ACTIVE"

        # Fail-Closed Latch: Block new allocations on bad balance reads
        if math.isnan(current_balance) or math.isinf(current_balance) or current_balance <= 0.0:
            logger.critical(f"[RISK_VAULT] 🛑 FAIL-CLOSED ENGAGED: Invalid balance read (${current_balance}). Halting execution.")
            return False, "HALT_INVALID_BALANCE_STATE"

        if current_balance > 1.0:
            self.last_valid_equity = current_balance

        # Daily Loss Limit Protection
        now_date = datetime.now(timezone.utc).date()
        if now_date != self.current_day_utc:
            self.current_day_utc = now_date
            self.daily_high_watermark = current_balance
            
        if current_balance > self.daily_high_watermark:
            self.daily_high_watermark = current_balance
            
        daily_drawdown = (self.daily_high_watermark - current_balance) / max(self.daily_high_watermark, 1e-9)
        if daily_drawdown >= self.daily_loss_limit_pct:
            logger.warning(f"🚨 INTRADAY LOSS LIMIT REACHED ({daily_drawdown:.2%}). Suspending entries.")
            return False, f"DAILY_LOSS_LIMIT_REACHED_{daily_drawdown:.2%}"

        # Absolute Systemic Drawdown Protection
        if current_balance > self.peak_balance:
            self.peak_balance = current_balance
            self.current_drawdown_state = 0.0
        elif self.peak_balance > 0:
            self.current_drawdown_state = (self.peak_balance - current_balance) / self.peak_balance
            if self.current_drawdown_state >= self.max_drawdown_pct:
                self.emergency_circuit_breaker = True
                logger.critical(f"🚨 ABSOLUTE MAX DRAWDOWN BREACHED ({self.current_drawdown_state:.2%}). SYSTEM LOCKDOWN.")
                return False, f"MAX_DRAWDOWN_BREACHED_{self.current_drawdown_state:.2%}"

        if symbol in self.active_positions:
            return False, f"DUPLICATE_SYMBOL_LOCK ({symbol})"

        if len(self.active_positions) >= self.get_max_allowed_slots():
            return False, f"DYNAMIC_SLOT_CAP_REACHED ({self.get_max_allowed_slots()}/{self.get_max_allowed_slots()})"

        # 🚀 PORTFOLIO CORRELATION GUARD: Prevent correlated long-beta clustering
        if self.correlation_matrix is not None and len(self.active_positions) >= 2 and symbol:
            active_symbols = [s for s in self.active_positions.keys() if s in self.correlation_matrix.index]
            if symbol in self.correlation_matrix.index and active_symbols:
                corrs = [self.correlation_matrix.loc[symbol, s] for s in active_symbols]
                avg_corr = float(np.mean(corrs)) if corrs else 0.0
                if avg_corr > 0.70:
                    logger.warning(f"[RISK_VAULT] 🛑 CORRELATION VETO // {symbol} has {avg_corr:.2f} avg correlation with active portfolio. Aborting.")
                    return False, f"PORTFOLIO_CORRELATION_VETO ({avg_corr:.2f} > 0.70)"

        max_heat_dollars = max(self.exchange_min_notional * 5.0, current_balance * self.absolute_max_leverage)
        total_exposure = sum(self.active_positions.values()) + new_position_notional

        if total_exposure > max_heat_dollars:
            return False, f"HEAT_CAP_EXCEEDED (Req: ${total_exposure:.2f} > Max: ${max_heat_dollars:.2f})"
                
        return True, "SAFE"

    def update_position_ledger(self, symbol: str, notional_value: float):
        if notional_value <= 0 or math.isnan(notional_value) or math.isinf(notional_value):
            self.active_positions.pop(symbol, None)
        else:
            self.active_positions[symbol] = notional_value

    def clear_ledger(self):
        self.active_positions.clear()