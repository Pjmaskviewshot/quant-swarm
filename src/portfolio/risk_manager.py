import logging
import math
import numpy as np
from typing import Dict, Any, List

logger = logging.getLogger("QUANT_CORE.RISK_MANAGER")

class InstitutionalRiskVault:
    def __init__(self, max_drawdown_pct: float = 0.25, max_single_position_risk_pct: float = 0.15, exchange_min_notional: float = 5.0, max_single_asset_leverage_limit: float = 10.0):
        """
        Risk engine initialized with baseline protections and Phase 2 advanced safety rings.
        Optimized dynamically to handle both micro-balances and large account scaling seamlessly.
        
        Parameters:
            max_drawdown_pct (float): Maximum trailing drawdown limit before circuit breaking.
            max_single_position_risk_pct (float): Baseline risk fraction per position.
            exchange_min_notional (float): Minimum order cost required by the exchange interface.
            max_single_asset_leverage_limit (float): Maximum total leverage exposure allowed for a single asset node.
        """
        self.max_drawdown_pct = max_drawdown_pct
        self.max_single_risk = max_single_position_risk_pct
        self.exchange_min_notional = exchange_min_notional
        # 🚀 FIX: Raised to 10.0 so the Kelly Criterion can function on micro-accounts without being artificially clamped
        self.max_single_asset_leverage_limit = max_single_asset_leverage_limit
        self.peak_balance = 0.0
        self.emergency_circuit_breaker = False
        
        # --- GLOBAL PORTFOLIO LEDGER ---
        self.active_positions: Dict[str, float] = {}

        # ====================================================================
        # 🚀 PHASE 2 UPGRADES: DYNAMIC CAPITAL HARDENING LAYERS
        # ====================================================================
        # Cross-asset correlation groups to prevent structural systemic risk
        self.correlation_groups = {
            "DYNAMIC_BTC_COVARIANCE": ["BTCUSDT"] # Will be updated dynamically by the math engine
        }

    def update_correlation_matrix(self, price_histories: Dict[str, List[float]], base_asset: str = "BTCUSDT", threshold: float = 0.75):
        """
        🚀 STRUCTURAL UPGRADE: DYNAMIC PEARSON COVARIANCE MATRIX
        Calculates rolling Pearson correlation coefficients against a base asset (e.g., BTCUSDT).
        Automatically classifies high-beta altcoins into a restricted group during risk-off events.
        """
        if base_asset not in price_histories or len(price_histories[base_asset]) < 30:
            return
            
        # Use recent history (e.g., last 150 periods) to gauge current market stress
        base_prices = np.array(price_histories[base_asset][-150:]) 
        base_returns = np.diff(base_prices) / base_prices[:-1]
        
        restricted_group = [base_asset]
        
        for symbol, prices in price_histories.items():
            if symbol == base_asset or len(prices) < len(base_prices):
                continue
                
            # Align sequence lengths for mathematical parity
            sym_prices = np.array(prices[-len(base_prices):])
            sym_returns = np.diff(sym_prices) / sym_prices[:-1]
            
            # Prevent division by zero anomalies in flat/illiquid micro-caps
            if np.std(sym_returns) == 0 or np.std(base_returns) == 0:
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
            current_drawdown = (self.peak_balance - current_balance) / self.peak_balance
            if current_drawdown >= self.max_drawdown_pct:
                if not self.emergency_circuit_breaker:
                    logger.critical(f"🚨 ABSOLUTE MAX DRAWDOWN BREACHED ({current_drawdown:.2%}). LOCKING DOWN SYSTEMS.")
                    self.emergency_circuit_breaker = True
                return False
        
        # 4. Cross-Asset Correlation Guard
        if symbol and new_position_notional > 0:
            for group_name, asset_list in self.correlation_groups.items():
                if symbol in asset_list:
                    active_correlated_nodes = [active_sym for active_sym in self.active_positions.keys() if active_sym in asset_list and active_sym != symbol]
                    if active_correlated_nodes:
                        logger.warning(
                            f"🛡️ CORRELATION GUARD BLOCK // Node {symbol} rejected. "
                            f"High-covariance trade already open in group [{group_name}]: {active_correlated_nodes}. Over-exposure aborted."
                        )
                        return False

        # 5. Check Node-Specific Allocation Cap (Concentration Risk Mitigation)
        if symbol:
            current_node_exposure = self.active_positions.get(symbol, 0.0)
            if (current_node_exposure + new_position_notional) > (current_balance * self.max_single_asset_leverage_limit):
                logger.warning(f"⚠️ Single asset concentration risk limit reached for {symbol}: Exceeds limit of {self.max_single_asset_leverage_limit}x balance.")
                return False

        # 6. Check Global Exposure (The Swarm Central Banker)
        total_exposure = sum(self.active_positions.values()) + new_position_notional
        # 🚀 FIX: Raised to 5.0 to allow 2-3 simultaneous micro-account positions
        if total_exposure > (current_balance * 5.0):
            logger.warning(f"⚠️ Global exposure limit reached: Current {sum(self.active_positions.values()):.2f} + New {new_position_notional:.2f} exceeds capacity.")
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

    def calculate_dynamic_leverage(self, notional_position_usdt: float, account_balance: float, base_leverage: int = 5, hard_cap: int = 15, sl_distance_pct: float = None) -> int:
        """
        🚀 CENTRALIZED LEVERAGE AUTHORITY
        Dynamically scales leverage required to execute the ideal Kelly fraction while strictly adhering to safety bounds.
        Contains un-bypassable micro-account scaling rules and a Liquidation Reality Check.
        """
        if account_balance <= 0 or notional_position_usdt <= 0:
            return 1
            
        # 🛑 P1-6 FIX: LIQUIDATION REALITY CHECK
        # Guarantees the leverage applied will never put the liquidation price inside the Stop Loss bracket.
        if sl_distance_pct and sl_distance_pct > 0:
            # Force the liquidation price to be at least 1.5x further away than the Stop Loss
            max_safe_leverage = int(1.0 / (sl_distance_pct * 1.5))
            hard_cap = min(hard_cap, max_safe_leverage)

        # 🛑 MICRO-ACCOUNT SURVIVAL CLAMP
        # A tiny account cannot handle 15x leverage without imminent liquidation risk from standard volatility.
        if account_balance < 10.0:
            hard_cap = min(hard_cap, 2)  # Ultra-safe mode for sub-$10 accounts
        elif account_balance < 50.0:
            hard_cap = min(hard_cap, 3)  # Safe mode for sub-$50 accounts
            
        # Target consuming a maximum of 12% of the free balance as margin per trade
        margin_required = account_balance * 0.12
        calculated_leverage = math.ceil(notional_position_usdt / margin_required)
        
        # Apply institutional safety bounds to prevent liquidation cascades
        return int(min(max(1, calculated_leverage), hard_cap))