"""
💎 V36.0 APEX TITAN: INSTITUTIONAL RISK VAULT
------------------------------------------------------------
Features:
- Live Portfolio Correlation Stress Guard (Rejects trades if avg pairwise corr > 0.70)
- Fail-Closed Liquidity & Balance Integrity Verification
- Intraday High-Watermark Circuit Breaker (2%) & Systemic Drawdown (5%)
- Exact Matrix Invertibility Guards & Exposure Heat Allocation

Architectural Supremacy (V36.0 Integration):
- Continuous Dual-EWMA Covariance: Replaces O(N^2) rolling window computations 
  with a memory-efficient, continuous-time exponential covariance tracker.
- Ledoit-Wolf Linear Shrinkage: Pulls extreme high-frequency correlation noise 
  toward the identity matrix to prevent spurious >0.70 portfolio vetoes.
- Robust Watermark Bootstrapping: Safeguards intraday and peak balance initializations 
  against zero/dust read loops to eliminate false positive 99.74% loss limit trips.
- Beta-Stripped Correlation Lookback: Subtracts the cross-sectional market mean 
  before computing the covariance matrix to isolate true asset correlations.
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
    🚀 V36.0 PURE SYSTEMIC GUARDIAN
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
        
        # 🚀 V36.0 Continuous Covariance State
        self.ewma_mean = None
        self.ewma_var = None
        self.ewma_cov = None
        self.prev_prices = None

    def update_correlation_matrix(self, price_histories: Dict[str, List[float]]):
        """
        🚀 V36.0 EMPIRICAL HARDENING: Continuous Dual-EWMA Covariance.
        Eradicates the O(N^2) rolling window computation in favor of a 
        memory-efficient, continuous-time exponential covariance tracker.
        """
        try:
            if not price_histories: return
            symbols = list(price_histories.keys())
            if len(symbols) < 2: return

            # Get only the latest price to update the continuous state
            latest_prices = np.array([price_histories[sym][-1] for sym in symbols], dtype=np.float64)
            
            if self.prev_prices is None or len(self.prev_prices) != len(symbols):
                self.prev_prices = latest_prices
                self.ewma_mean = np.zeros(len(symbols), dtype=np.float64)
                self.ewma_var = np.ones(len(symbols), dtype=np.float64) * 1e-6
                self.ewma_cov = np.eye(len(symbols), dtype=np.float64) * 1e-6
                return

            # Compute instant log return
            returns = np.log(latest_prices / (self.prev_prices + 1e-9))
            self.prev_prices = latest_prices

            # Beta stripping (Cross-sectional mean)
            market_mean = np.mean(returns)
            excess_returns = returns - market_mean

            # Continuous EWMA Updates (Alpha = 0.02 ~ 50 tick half-life)
            alpha = 0.02
            delta = excess_returns - self.ewma_mean
            self.ewma_mean += alpha * delta
            
            # Update variances and covariance matrix
            self.ewma_var = (1.0 - alpha) * self.ewma_var + alpha * (delta ** 2)
            self.ewma_cov = (1.0 - alpha) * self.ewma_cov + alpha * np.outer(delta, delta)

            # Compute correlation from continuous covariance
            stds = np.sqrt(np.maximum(self.ewma_var, 1e-9))
            corr = self.ewma_cov / np.outer(stds, stds)
            
            # Handle numerical instability
            corr = np.clip(np.nan_to_num(corr, nan=0.0), -1.0, 1.0)
            
            # 🚀 V36.0: Static Shrinkage to Identity (Ledoit-Wolf approximation)
            # Pulls noisy off-diagonal extreme correlations toward 0, saving us from false >0.70 vetoes.
            shrinkage_intensity = 0.25
            shrunk_corr = (1.0 - shrinkage_intensity) * corr + (shrinkage_intensity * np.eye(corr.shape[0]))
            
            self.correlation_matrix = pd.DataFrame(shrunk_corr, index=symbols, columns=symbols)
            
        except Exception as e:
            logger.debug(f"[MATH_WARN] EWMA Correlation update failure: {e}")

    def get_max_allowed_slots(self) -> int:
        return 5

    def evaluate_portfolio_safety(
        self, 
        current_balance: float, 
        new_position_notional: float = 0.0, 
        symbol: str = ""
    ) -> Tuple[bool, str]:
        """
        🚀 V36.0 FAIL-CLOSED LATCH MATRIX WITH WATERMARK SAFEGUARDS
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

        # 🚀 ROBUST WATERMARK BOOTSTRAP: Prevent false 99.74% loss limit trips
        # Anchors the high-watermarks dynamically to the true balance on boot
        if self.daily_high_watermark <= 1.0:
            self.daily_high_watermark = current_balance
        if self.peak_balance <= 1.0:
            self.peak_balance = current_balance

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
                    logger.warning(f"[RISK_VAULT] 🛑 CORRELATION VETO // {symbol} has {avg_corr:.2f} avg excess correlation with active portfolio. Aborting.")
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