import math
import numpy as np
import logging
from typing import Dict, List

logger = logging.getLogger("QUANT_CORE.RISK_VAULT")

class InstitutionalRiskVault:
    def __init__(
        self, 
        max_drawdown_pct: float = 0.25, 
        max_single_position_risk_pct: float = 0.15, 
        exchange_min_notional: float = 5.0
    ):
        """
        💎 V34.0 APEX: ASYMMETRIC KELLY ENGINE
        Dynamically scales capital allocation based on live win rate, payoff ratios,
        and realized account equity. Limits drawdown using half-Kelly damping.
        """
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

        # 🚀 V34.0 FIX: Kelly Calibration State
        self.rolling_wins = 0
        self.rolling_losses = 0
        self.avg_win_pct = 0.02
        self.avg_loss_pct = 0.01

    def update_correlation_matrix(self, price_histories: Dict[str, List[float]], base_asset: str = "BTCUSDT", threshold: float = 0.75):
        if base_asset not in price_histories or len(price_histories[base_asset]) < 30:
            return
            
        base_prices = np.array(price_histories[base_asset][-150:]) 
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
        logger.debug(f"🕸️ COVARIANCE MATRIX UPDATED: {len(restricted_group)} assets locked in high-correlation with {base_asset}.")

    def update_kelly_metrics(self, is_win: bool, pnl_pct: float):
        """🚀 V34.0 FIX: Updates the rolling parameters required for the Kelly formula."""
        if is_win:
            self.rolling_wins = min(100, self.rolling_wins + 1)
            self.avg_win_pct = (self.avg_win_pct * 0.9) + (abs(pnl_pct) * 0.1)
        else:
            self.rolling_losses = min(100, self.rolling_losses + 1)
            self.avg_loss_pct = (self.avg_loss_pct * 0.9) + (abs(pnl_pct) * 0.1)

    def calculate_optimal_fraction(self, base_confidence: float) -> float:
        """
        🚀 V34.0 FIX: Calculates the Asymmetric Half-Kelly fraction.
        K = W - ((1 - W) / R)
        """
        total_trades = self.rolling_wins + self.rolling_losses
        if total_trades < 10:
            # Fallback for cold-start (prioritizes survival on low balance)
            return 0.015 
            
        win_rate = self.rolling_wins / total_trades
        # Blend actual win rate with model confidence for live tuning
        blended_w = (win_rate * 0.6) + (base_confidence * 0.4) 
        
        payoff_ratio = self.avg_win_pct / (self.avg_loss_pct + 1e-9)
        
        if payoff_ratio <= 0 or blended_w <= 0:
            return 0.005 # Absolute minimum survival risk
            
        # The core Kelly Formula
        kelly_fraction = blended_w - ((1.0 - blended_w) / payoff_ratio)
        
        # Apply Half-Kelly (institutional standard for reducing volatility)
        half_kelly = kelly_fraction / 2.0
        
        # Hard bounds: Never risk less than 0.5% or more than 5% of pure equity per trade
        return max(0.005, min(0.05, half_kelly))

    def evaluate_portfolio_safety(self, current_balance: float, new_position_notional: float = 0.0, symbol: str = "") -> bool:
        if self.emergency_circuit_breaker:
            logger.critical("🚨 RISK VAULT CURRENTLY LOCKED OUT. SUBMISSIONS REJECTED.")
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
        
        active_correlated_count = 0
        if symbol and new_position_notional > 0:
            for group_name, asset_list in self.correlation_groups.items():
                if symbol in asset_list:
                    active_correlated_nodes = [active_sym for active_sym in self.active_positions.keys() if active_sym in asset_list and active_sym != symbol]
                    active_correlated_count = len(active_correlated_nodes)
                    if active_correlated_count > 0:
                        logger.warning(
                            f"🛡️ CORRELATION GUARD BLOCK // Node {symbol} rejected. "
                            f"High-covariance trade already open in group [{group_name}]: {active_correlated_nodes}. Over-exposure aborted."
                        )
                        return False

        if symbol:
            current_node_exposure = self.active_positions.get(symbol, 0.0)
            absolute_max_notional_per_asset = current_balance * self.absolute_max_leverage
            
            if (current_node_exposure + new_position_notional) > absolute_max_notional_per_asset:
                logger.warning(f"⚠️ Single asset concentration risk limit reached for {symbol}. Cap: {absolute_max_notional_per_asset:.2f} USDT.")
                return False

        total_exposure = sum(self.active_positions.values()) + new_position_notional
        
        correlation_penalty = 1.0 - (min(active_correlated_count, 3) * 0.15)
        global_max_notional = current_balance * self.absolute_max_leverage * correlation_penalty
        
        if total_exposure > global_max_notional:
            logger.warning(f"⚠️ Global exposure limit reached: Current {sum(self.active_positions.values()):.2f} + New {new_position_notional:.2f} exceeds Global Cap ({global_max_notional:.2f}).")
            return False
                
        return True

    def update_position_ledger(self, symbol: str, notional_value: float):
        if notional_value <= 0:
            self.active_positions.pop(symbol, None)
        else:
            self.active_positions[symbol] = notional_value
        logger.info(f"💼 PORTFOLIO LEDGER UPDATED: {symbol} exposure is now {notional_value:.2f} USDT")

    def clear_ledger(self):
        self.active_positions.clear()
        logger.info("💼 PORTFOLIO LEDGER PURGED SATELLITE MATRIX CLEAR.")

    def calculate_dynamic_leverage(
        self, 
        notional_position_usdt: float, 
        account_balance: float, 
        base_leverage: int = 3, 
        hard_cap: int = 5, 
        sl_distance_pct: float = None
    ) -> int:
        safe_base = min(base_leverage, self.base_leverage)
        safe_cap = min(hard_cap, self.absolute_max_leverage)
        
        if account_balance <= 0 or notional_position_usdt <= 0:
            return 1
            
        if sl_distance_pct and sl_distance_pct > 0:
            max_safe_leverage = int(1.0 / (sl_distance_pct * 1.5))
            safe_cap = min(safe_cap, max_safe_leverage)

        margin_required = account_balance * 0.20
        calculated_leverage = math.ceil(notional_position_usdt / (margin_required + 1e-9))
        
        final_leverage = int(min(max(safe_base, calculated_leverage), safe_cap))
        
        return max(1, final_leverage)