"""
💎 V50.0 APEX: INSTITUTIONAL RISK VAULT (NANO-CORE ENABLED)
------------------------------------------------------------
Conservative Kelly sizing with Volatility-Adjusted EVT Tail-Risk Protection,
Micro-Account ($17 Balance) Nano-Sizing, and X-Ray Diagnostic Telemetry.
"""

import math
import logging
from typing import Dict, List, Any, Optional
from collections import deque
import numpy as np

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
        self.exchange_min_notional = exchange_min_notional
        
        self.absolute_max_leverage: float = 5.0
        self.base_leverage: float = 3.0
        
        self.peak_balance = 0.0
        self.emergency_circuit_breaker = False
        
        self.active_positions: Dict[str, float] = {}

        self.correlation_groups = {
            "DYNAMIC_BTC_COVARIANCE": ["BTCUSDT"] 
        }

        self.rolling_wins = 0
        self.rolling_losses = 0
        self.avg_win_pct = 0.02
        self.avg_loss_pct = 0.01
        
        self.volatility_surface: deque = deque(maxlen=300)

    def push_microstructure_variance(self, variance: float):
        """Pushes instantaneous variance into the volatility surface for EVT modeling."""
        if variance > 0:
            self.volatility_surface.append(variance)

    def calculate_evt_tail_risk(self) -> float:
        """
        🚀 V50.0 ADAPTIVE EVT: Volatility-Adjusted EVT Multiplier with Dynamic Floor.
        Automatically scales tail-risk suppression between 0.35 (high vol) and 0.50 (low vol).
        """
        if len(self.volatility_surface) < 30:
            return 1.05  
            
        try:
            vol_arr = np.array(self.volatility_surface)
            threshold = np.percentile(vol_arr, 95)
            exceedances = vol_arr[vol_arr > threshold] - threshold
            
            if len(exceedances) < 5 or np.mean(exceedances) <= 1e-9:
                return 1.05
                
            log_exceedances = np.log(exceedances + 1e-9)
            xi_estimator = float(np.mean(log_exceedances) - np.log(threshold + 1e-9))
            xi_clamped = max(0.0, min(1.5, xi_estimator))
            
            if xi_clamped <= 0.3:
                return 1.05

            raw_suppression = min(0.65, (xi_clamped - 0.3) * 1.2)
            
            # Dynamic floor: Higher in calm markets (0.50), lower in volatile markets (0.35)
            current_variance = self.volatility_surface[-1] if self.volatility_surface else 0.0
            vol_scalar = min(1.0, current_variance * 1000.0)
            dynamic_floor = 0.35 + 0.15 * (1.0 - vol_scalar)
            
            tail_risk_multiplier = max(dynamic_floor, 1.0 - raw_suppression)
            logger.debug(f"[X-RAY] 🌪️ EVT Tail Risk (Xi: {xi_clamped:.3f}): Multiplier at {tail_risk_multiplier:.1%}")
            
            return tail_risk_multiplier
        except Exception as e:
            logger.debug(f"[X-RAY] EVT calculation fallback engaged: {e}")
            return 1.05

    def update_correlation_matrix(self, price_histories: Dict[str, List[float]], base_asset: str = "BTCUSDT", threshold: float = 0.75):
        """Dynamically clusters assets correlated with BTC to enforce portfolio exposure limits."""
        if base_asset not in price_histories or len(price_histories[base_asset]) < 30:
            return
            
        base_prices = np.array(price_histories[base_asset][-1440:]) 
        base_returns = np.diff(base_prices) / (base_prices[:-1] + 1e-9)
        
        restricted_group = [base_asset]
        
        for symbol, prices in price_histories.items():
            if symbol == base_asset or len(prices) < len(base_prices):
                continue
                
            sym_prices = np.array(prices[-len(base_prices):])
            sym_returns = np.diff(sym_prices) / (sym_prices[:-1] + 1e-9)
            
            if np.std(sym_returns) < 1e-9 or np.std(base_returns) < 1e-9:
                continue
                
            correlation = np.corrcoef(base_returns, sym_returns)[0, 1]
            if correlation >= threshold:
                restricted_group.append(symbol)
                
        self.correlation_groups["DYNAMIC_BTC_COVARIANCE"] = restricted_group

    def update_kelly_metrics(self, is_win: bool, pnl_pct: float):
        """Updates rolling win rate and win/loss payoff ratios for Fractional Kelly calculation."""
        if is_win:
            self.rolling_wins = min(100, self.rolling_wins + 1)
            self.avg_win_pct = (self.avg_win_pct * 0.9) + (abs(pnl_pct) * 0.1)
        else:
            self.rolling_losses = min(100, self.rolling_losses + 1)
            self.avg_loss_pct = (self.avg_loss_pct * 0.9) + (abs(pnl_pct) * 0.1)

    def calculate_optimal_fraction(self, base_confidence: float, net_edge_bps: float = 50.0) -> float:
        """
        🚀 Edge-Weighted Kelly Allocation with Adaptive EVT Tail-Risk.
        Scales capital allocation based on signal confidence, tail-risk state, and expected net edge.
        """
        total_trades = self.rolling_wins + self.rolling_losses
        if total_trades < 10:
            base_fraction = 0.010 # Cold start 1.0%
        else:
            win_rate = self.rolling_wins / total_trades
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

    def evaluate_portfolio_safety(self, current_balance: float, new_position_notional: float = 0.0, symbol: str = "") -> bool:
        """
        🚀 V50.0 X-RAY PORTFOLIO SAFETY GUARD:
        Verifies drawdown limits, correlation group caps, and total portfolio heat limits.
        """
        if self.emergency_circuit_breaker:
            logger.warning("[X-RAY] 🚫 SAFETY BLOCK // Emergency circuit breaker is ACTIVE.")
            return False

        if current_balance > self.peak_balance:
            self.peak_balance = current_balance
            
        if self.peak_balance > 0:
            current_drawdown = (self.peak_balance - current_balance) / (self.peak_balance + 1e-9)
            if current_drawdown >= self.max_drawdown_pct:
                if not self.emergency_circuit_breaker:
                    logger.critical(f"🚨 ABSOLUTE MAX DRAWDOWN BREACHED ({current_drawdown:.2%}). LOCKING DOWN SYSTEMS.")
                    self.emergency_circuit_breaker = True
                return False
        
        # Check Correlation Cluster Caps
        if symbol and new_position_notional > 0:
            for group_name, asset_list in self.correlation_groups.items():
                if symbol in asset_list:
                    active_correlated_nodes = [active_sym for active_sym in self.active_positions.keys() if active_sym in asset_list and active_sym != symbol]
                    if len(active_correlated_nodes) >= 2:
                        logger.warning(f"[X-RAY] 🛡️ CORRELATION GUARD // {symbol} rejected. Too many correlated positions active ({active_correlated_nodes}).")
                        return False

        # Calculate Total Portfolio Exposure Heat
        total_exposure = sum(self.active_positions.values()) + new_position_notional
        
        # Nano-Core Micro-Account Heat Bypass (< $50 USDT)
        if current_balance < 50.0:
            max_heat = current_balance * 3.5  # Allow up to 3.5x portfolio heat for micro balances
        else:
            max_heat = current_balance * 2.5  # Standard institutional 2.5x cap

        if total_exposure > max_heat:
            logger.warning(f"[X-RAY] ⚠️ LEVERAGE HEAT CAP // Total exposure (${total_exposure:.2f}) exceeds max allowed (${max_heat:.2f}). Rejected {symbol}.")
            return False
                
        return True

    def update_position_ledger(self, symbol: str, notional_value: float):
        """Updates active tracking ledger for portfolio exposure calculations."""
        if notional_value <= 0:
            self.active_positions.pop(symbol, None)
        else:
            self.active_positions[symbol] = notional_value

    def clear_ledger(self):
        """Clears all position entries in the ledger."""
        self.active_positions.clear()

    def calculate_dynamic_leverage(
        self, 
        notional_position_usdt: float, 
        account_balance: float, 
        base_leverage: int = 3, 
        hard_cap: int = 5, 
        sl_distance_pct: Optional[float] = None
    ) -> int:
        """
        🚀 Dynamic Leverage Calculation with Division-by-Zero Safety & EVT Protection.
        """
        if account_balance <= 0 or notional_position_usdt <= 0:
            return 1

        safe_base = min(base_leverage, int(self.base_leverage))
        safe_cap = min(hard_cap, int(self.absolute_max_leverage))
        
        # EVT Volatility Compression
        evt_multiplier = self.calculate_evt_tail_risk()
        if evt_multiplier < 0.5:
            safe_cap = max(1, int(safe_cap * evt_multiplier))
            
        # 🚀 V50.0 FIX: Division-by-Zero / Near-Zero Safety Guard on Stop-Loss Distance
        if sl_distance_pct and sl_distance_pct > 0.0001:
            max_safe_leverage = int(1.0 / (sl_distance_pct * 1.5))
            safe_cap = min(safe_cap, max(1, max_safe_leverage))

        margin_required = account_balance * 0.20
        calculated_leverage = math.ceil(notional_position_usdt / (margin_required + 1e-9))
        final_leverage = int(min(max(safe_base, calculated_leverage), safe_cap))
        
        return max(1, final_leverage)