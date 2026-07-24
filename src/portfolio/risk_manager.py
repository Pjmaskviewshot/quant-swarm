import logging
import math
import numpy as np
from typing import Dict, List

logger = logging.getLogger("QUANT_CORE.RISK_MANAGER")

class InstitutionalRiskVault:
    def __init__(
        self, 
        max_drawdown_pct: float = 0.25, 
        max_single_position_risk_pct: float = 0.15, 
        exchange_min_notional: float = 5.0
    ):
        """
        🛡️ V31.4 APEX: INSTITUTIONAL RISK VAULT
        Hardened with Single Source of Truth leverage caps and dynamic correlation margin guards.
        """
        self.max_drawdown_pct = max_drawdown_pct
        self.max_single_risk = max_single_position_risk_pct
        self.exchange_min_notional = exchange_min_notional
        
        # 🚀 V31.4 FIX: Single Source of Truth Leverage Caps
        self.absolute_max_leverage: float = 5.0
        self.base_leverage: float = 3.0
        
        self.peak_balance = 0.0
        self.emergency_circuit_breaker = False
        
        # --- GLOBAL PORTFOLIO LEDGER ---
        self.active_positions: Dict[str, float] = {}

        # Cross-asset correlation groups to prevent structural systemic risk
        self.correlation_groups = {
            "DYNAMIC_BTC_COVARIANCE": ["BTCUSDT"] # Updated dynamically by the math engine
        }

    def update_correlation_matrix(self, price_histories: Dict[str, List[float]], base_asset: str = "BTCUSDT", threshold: float = 0.75):
        """
        🚀 STRUCTURAL UPGRADE: DYNAMIC PEARSON COVARIANCE MATRIX
        Calculates rolling Pearson correlation coefficients against a base asset (e.g., BTCUSDT).
        Automatically classifies high-beta altcoins into a restricted group during risk-off events.
        """
        if base_asset not in price_histories or len(price_histories[base_asset]) < 30:
            return
            
        # Use recent history (last 150 periods) to gauge current market stress
        base_prices = np.array(price_histories[base_asset][-150:]) 
        base_returns = np.diff(base_prices) / (base_prices[:-1] + 1e-9)
        
        restricted_group = [base_asset]
        
        for symbol, prices in price_histories.items():
            if symbol == base_asset or len(prices) < len(base_prices):
                continue
                
            # Align sequence lengths for mathematical parity
            sym_prices = np.array(prices[-len(base_prices):])
            sym_returns = np.diff(sym_prices) / (sym_prices[:-1] + 1e-9)
            
            # Prevent division by zero anomalies in flat/illiquid micro-caps
            if np.std(sym_returns) < 1e-9 or np.std(base_returns) < 1e-9:
                continue
                
            # Calculate linear correlation
            correlation = np.corrcoef(base_returns, sym_returns)[0, 1]
            
            if correlation >= threshold:
                restricted_group.append(symbol)
                
        self.correlation_groups["DYNAMIC_BTC_COVARIANCE"] = restricted_group
        logger.debug(f"🕸️ COVARIANCE MATRIX UPDATED: {len(restricted_group)} assets locked in high-correlation with {base_asset}.")

    def evaluate_portfolio_safety(self, current_balance: float, new_position_notional: float = 0.0, symbol: str = "") -> bool:
        """
        Enforces a rigid hardware circuit breaker if portfolio trailing drawdown parameters are breached,
        guards against asset concentration, and applies strict cross-asset correlation rules.
        """
        # 1. Emergency Circuit Breaker Check
        if self.emergency_circuit_breaker:
            logger.critical("🚨 RISK VAULT CURRENTLY LOCKED OUT. SUBMISSIONS REJECTED.")
            return False

        # 2. Update Trailing Drawdown Peak
        if current_balance > self.peak_balance:
            self.peak_balance = current_balance
            
        # 3. Check Absolute Drawdown Breach
        if self.peak_balance > 0:
            current_drawdown = (self.peak_balance - current_balance) / (self.peak_balance + 1e-9)
            if current_drawdown >= self.max_drawdown_pct:
                if not self.emergency_circuit_breaker:
                    logger.critical(f"🚨 ABSOLUTE MAX DRAWDOWN BREACHED ({current_drawdown:.2%}). LOCKING DOWN SYSTEMS.")
                    self.emergency_circuit_breaker = True
                return False
        
        # 4. Cross-Asset Correlation Guard
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

        # 5. Check Node-Specific Allocation Cap (Concentration Risk Mitigation)
        if symbol:
            current_node_exposure = self.active_positions.get(symbol, 0.0)
            
            # An asset can never exceed a notional size equivalent to the account balance * max leverage limit.
            absolute_max_notional_per_asset = current_balance * self.absolute_max_leverage
            
            if (current_node_exposure + new_position_notional) > absolute_max_notional_per_asset:
                logger.warning(f"⚠️ Single asset concentration risk limit reached for {symbol}. Cap: {absolute_max_notional_per_asset:.2f} USDT.")
                return False

        # 6. Check Global Exposure (The Swarm Central Banker)
        total_exposure = sum(self.active_positions.values()) + new_position_notional
        
        # 🚀 V31.4 FIX: Dynamically shrink the global exposure limit based on active correlated risk
        # If we have 2 uncorrelated assets open, we allow normal margin usage. 
        # But if the entire market is correlated, we forcibly shrink the ceiling.
        correlation_penalty = 1.0 - (min(active_correlated_count, 3) * 0.15)
        
        global_max_notional = current_balance * self.absolute_max_leverage * correlation_penalty
        
        if total_exposure > global_max_notional:
            logger.warning(f"⚠️ Global exposure limit reached: Current {sum(self.active_positions.values()):.2f} + New {new_position_notional:.2f} exceeds Global Cap ({global_max_notional:.2f}).")
            return False
                
        return True

    def update_position_ledger(self, symbol: str, notional_value: float):
        """Updates the internal ledger with the current notional value of an asset."""
        if notional_value <= 0:
            self.active_positions.pop(symbol, None)
        else:
            self.active_positions[symbol] = notional_value
        logger.info(f"💼 PORTFOLIO LEDGER UPDATED: {symbol} exposure is now {notional_value:.2f} USDT")

    def clear_ledger(self):
        """Resets all position trackers within the asset universe ledger."""
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
        """
        🚀 CENTRALIZED LEVERAGE AUTHORITY
        Dynamically scales leverage required to execute the ideal Kelly fraction while strictly adhering to safety bounds.
        Contains un-bypassable micro-account scaling rules and a Liquidation Reality Check.
        """
        # Override any passed-in caps with the Absolute Master limits
        safe_base = min(base_leverage, self.base_leverage)
        safe_cap = min(hard_cap, self.absolute_max_leverage)
        
        if account_balance <= 0 or notional_position_usdt <= 0:
            return 1
            
        # 🛑 LIQUIDATION REALITY CHECK
        # Guarantees the leverage applied will never put the liquidation price inside the Stop Loss bracket.
        if sl_distance_pct and sl_distance_pct > 0:
            # Force the liquidation price to be at least 1.5x further away than the Stop Loss
            max_safe_leverage = int(1.0 / (sl_distance_pct * 1.5))
            safe_cap = min(safe_cap, max_safe_leverage)

        # Target consuming a maximum of 20% of the free balance as margin per trade (5x leverage equivalent)
        margin_required = account_balance * 0.20
        
        calculated_leverage = math.ceil(notional_position_usdt / (margin_required + 1e-9))
        
        # Apply institutional safety bounds to prevent liquidation cascades
        final_leverage = int(min(max(safe_base, calculated_leverage), safe_cap))
        
        return max(1, final_leverage)