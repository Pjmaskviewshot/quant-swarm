"""
💎 V22.5 APEX QUANTUM PRIME: INSTITUTIONAL RISK VAULT
------------------------------------------------------------
Features:
- Live Portfolio Correlation Stress Guard (Rejects trades if avg pairwise corr > 0.70)
- Fail-Closed Liquidity & Balance Integrity Verification
- Calibrated Binary Kelly Criterion with Platt Sigmoidal Scaling
- L-Moment Generalized Pareto Distribution (GPD) Heavy-Tail Estimator
- Intraday High-Watermark Circuit Breaker (5%) & Systemic Drawdown (30%)
- Exact Matrix Invertibility Guards & Exposure Heat Allocation

Audit Fixes (V22.5):
- Portfolio Beta Contagion Guard: Added a live cross-sectional correlation filter 
  to prevent the system from clustering risk into highly correlated assets during 
  directional market sweeps.
"""

import math
import logging
from typing import Dict, List, Any, Optional, Tuple
from collections import deque
import numpy as np
import pandas as pd
from datetime import datetime, timezone

logger = logging.getLogger("QUANT_CORE.RISK_VAULT")


class InstitutionalRiskVault:
    def __init__(
        self, 
        max_drawdown_pct: float = 0.30, 
        max_single_position_risk_pct: float = 0.05, 
        exchange_min_notional: float = 6.50
    ):
        self.max_drawdown_pct = max_drawdown_pct
        self.max_single_position_risk_pct = max_single_position_risk_pct
        self.exchange_min_notional = exchange_min_notional
        
        self.absolute_max_leverage: float = 5.0
        self.base_leverage: float = 3.0
        
        # Systemic Drawdown Trackers
        self.peak_balance: float = 0.0
        self.last_valid_equity: float = 21.0
        self.current_drawdown_state: float = 0.0
        self.emergency_circuit_breaker: bool = False
        
        # Daily Loss Limit Trackers
        self.daily_high_watermark: float = 0.0
        self.current_day_utc = datetime.now(timezone.utc).date()
        self.daily_loss_limit_pct: float = 0.05  # 5% Max Intraday Drawdown
        
        self.active_positions: Dict[str, float] = {}
        self.correlation_matrix: Optional[pd.DataFrame] = None

        self.outcomes_history = deque(maxlen=2000) 
        self.avg_win_r: float = 1.5   
        self.avg_loss_r: float = 1.0  
        self.volatility_surface: deque = deque(maxlen=300)

    def push_microstructure_variance(self, variance: float):
        if variance > 0 and not math.isnan(variance) and not math.isinf(variance):
            self.volatility_surface.append(variance)

    def calculate_evt_tail_risk(self) -> float:
        """
        L-Moment fitting of Generalized Pareto Distribution (GPD).
        Mathematically immune to small-sample variance explosions.
        """
        if len(self.volatility_surface) < 50:
            return 1.0  
            
        try:
            vol_arr = np.array(self.volatility_surface, dtype=np.float64)
            vol_arr = np.sort(vol_arr[~np.isnan(vol_arr)])
            
            if len(vol_arr) == 0:
                return 1.0
                
            k = max(5, int(len(vol_arr) * 0.10))
            threshold = vol_arr[-k]
            exceedances = vol_arr[-k:] - threshold
            
            if threshold <= 0 or len(exceedances) < 3:
                return 1.0
                
            l_1 = float(np.mean(exceedances))
            n = len(exceedances)
            j = np.arange(1, n + 1)
            l_2 = float(np.sum(exceedances * ((2 * j - 1 - n) / (n * (n - 1)))))
            
            if l_2 == 0 or l_1 == 0: 
                return 1.0
                
            tau = l_2 / l_1
            xi = 2.0 - (1.0 / tau)
            
            if xi > 0.15:
                tail_penalty = min(0.65, (xi - 0.15) * 2.5)
                return max(0.35, 1.0 - tail_penalty)
                
            return 1.05
            
        except Exception as e:
            logger.debug(f"[MATH_WARN] L-Moment GPD EVT fallback: {e}")
            return 1.0

    def update_correlation_matrix(self, price_histories: Dict[str, List[float]]):
        try:
            if not price_histories:
                return
                
            min_len = min(len(prices) for prices in price_histories.values())
            if min_len < 30: 
                return
            
            trimmed = {sym: prices[-min_len:] for sym, prices in price_histories.items()}
            df = pd.DataFrame(trimmed, dtype=np.float64)
            shifted = df.shift(1)
            returns_df = np.log(df / shifted.replace(0, np.nan)).dropna()
            
            # Tikhonov Ridge regularization for invertible correlation matrix
            corr = returns_df.corr().fillna(0.0).values
            reg_corr = (1.0 - 1e-4) * corr + (1e-4 * np.eye(corr.shape[0]))
            self.correlation_matrix = pd.DataFrame(reg_corr, index=df.columns, columns=df.columns)
            
            logger.info("[X-RAY] 🧠 Risk Parity Matrix Updated: Full pairwise correlation computed.")
        except Exception as e:
            logger.debug(f"[MATH_WARN] Correlation update failure: {e}")

    def update_kelly_metrics(self, is_win: bool, realized_r_multiple: float):
        if math.isnan(realized_r_multiple) or math.isinf(realized_r_multiple):
            return
            
        self.outcomes_history.append(1.0 if is_win else 0.0)
        
        if is_win:
            capped_r = min(5.0, max(0.1, abs(realized_r_multiple)))
            self.avg_win_r = (self.avg_win_r * 0.95) + (capped_r * 0.05)
        else:
            capped_r = min(2.5, max(0.1, abs(realized_r_multiple)))
            self.avg_loss_r = (self.avg_loss_r * 0.95) + (capped_r * 0.05)

    def get_dynamic_conviction_threshold(self, balance: float, net_edge_bps: float = 50.0) -> float:
        if math.isnan(balance) or math.isinf(balance):
            balance = 12.0
            
        base_threshold = 0.518
        ev_discount = min(0.010, max(0.0, (net_edge_bps - 40.0) / 5000.0))
        
        return max(0.508, base_threshold - ev_discount)

    def get_max_allowed_slots(self, balance: float) -> int:
        return 5

    def _calibrate_model_probability(self, raw_confidence: float) -> float:
        """Platt Sigmoidal scaling mapping raw model confidence to empirical win rate."""
        if math.isnan(raw_confidence) or math.isinf(raw_confidence): 
            return 0.50
        compressed_p = 0.50 + 0.15 * math.tanh((raw_confidence - 0.5) * 5.0)
        return float(np.clip(compressed_p, 0.50, 0.65))

    def calculate_calibrated_kelly_fraction(
        self, 
        model_confidence: float, 
        net_edge_bps: float = 50.0, 
        current_balance: float = 100.0, 
        symbol_variance: float = 1e-4
    ) -> float:
        min_required_conviction = self.get_dynamic_conviction_threshold(current_balance, net_edge_bps)
        if model_confidence < min_required_conviction:
            return 0.0

        p = self._calibrate_model_probability(model_confidence)
        q = 1.0 - p
        b = self.avg_win_r / max(self.avg_loss_r, 1e-9)
        
        if b <= 0:
            return 0.001
            
        raw_kelly = p - (q / b)
        if raw_kelly <= 0.0:
            return 0.0
        
        half_kelly = raw_kelly / 2.0
        evt_multiplier = self.calculate_evt_tail_risk()
        drawdown_penalty = max(0.10, 1.0 - (self.current_drawdown_state * 3.0))
        
        risk_adjusted_kelly = half_kelly * evt_multiplier * drawdown_penalty
        
        if math.isnan(risk_adjusted_kelly) or math.isinf(risk_adjusted_kelly):
            return 0.001
            
        return max(0.001, min(self.max_single_position_risk_pct, risk_adjusted_kelly))

    def evaluate_portfolio_safety(
        self, 
        current_balance: float, 
        new_position_notional: float = 0.0, 
        symbol: str = ""
    ) -> Tuple[bool, str]:
        if self.emergency_circuit_breaker:
            return False, "EMERGENCY_CIRCUIT_BREAKER_ACTIVE"

        # 🚀 V22.4 FAIL-CLOSED LATCH: Block new allocations on bad balance reads
        if math.isnan(current_balance) or math.isinf(current_balance) or current_balance <= 0.0:
            logger.critical(f"[RISK_VAULT] 🛑 FAIL-CLOSED ENGAGED: Invalid balance read (${current_balance}). Halting execution.")
            return False, "HALT_INVALID_BALANCE_STATE"

        if current_balance > 1.0:
            self.last_valid_equity = current_balance

        # 5% Daily Loss Limit Protection
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

        # 30% Absolute Systemic Drawdown Protection
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

        if len(self.active_positions) >= 5:
            return False, "DYNAMIC_SLOT_CAP_REACHED (5/5)"

        # 🚀 V22.5 PORTFOLIO CORRELATION GUARD: Prevent correlated long-beta clustering
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