import logging
from typing import Dict, Any, List

logger = logging.getLogger("QUANT_CORE.RISK_MANAGER")

class InstitutionalRiskVault:
    def __init__(self, max_drawdown_pct: float = 0.25, max_single_position_risk_pct: float = 0.15, exchange_min_notional: float = 5.0, max_single_asset_leverage_limit: float = 1.5):
        """
        Risk engine initialized with baseline protections and Phase 2 advanced safety rings.
        Optimized for small account balances to meet exchange limits while safeguarding capital.
        
        Parameters:
            max_drawdown_pct (float): Maximum trailing drawdown limit before circuit breaking.
            max_single_position_risk_pct (float): Baseline risk fraction per position.
            exchange_min_notional (float): Minimum order cost required by the exchange interface.
            max_single_asset_leverage_limit (float): Maximum total leverage exposure allowed for a single asset node.
        """
        self.max_drawdown_pct = max_drawdown_pct
        self.max_single_risk = max_single_position_risk_pct
        self.exchange_min_notional = exchange_min_notional
        self.max_single_asset_leverage_limit = max_single_asset_leverage_limit
        self.peak_balance = 0.0
        self.emergency_circuit_breaker = False
        
        # --- GLOBAL PORTFOLIO LEDGER ---
        # Tracks current notional exposure for all assets in the swarm to prevent over-leverage
        self.active_positions: Dict[str, float] = {}

        # ====================================================================
        # 🚀 PHASE 2 UPGRADES: RISK HARDENING LAYERS
        # ====================================================================
        # 1. CAPITAL HARVESTING VAULT BASELINE
        # Any profits generated above this threshold are locked away from active margin calculations.
        self.harvesting_baseline_usdt = 100.0
        
        # 2. CROSS-ASSET CORRELATION MATRIX GROUPS
        # Map highly covariant tickers to prevent simultaneous exposure to macro flash-crashes.
        self.correlation_groups = {
            "L1_HIGH_COVARIANCE": ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        }

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
        
        # 🚀 PHASE 2 ADVANCEMENT: CROSS-ASSET CORRELATION GUARD
        # Scans current active trades to prevent holding overlapping high-covariance cluster positions.
        if symbol and new_position_notional > 0:
            for group_name, asset_list in self.correlation_groups.items():
                if symbol in asset_list:
                    # Check if any other asset in this identical cluster group is currently active
                    active_correlated_nodes = [active_sym for active_sym in self.active_positions.keys() if active_sym in asset_list and active_sym != symbol]
                    
                    if active_correlated_nodes:
                        logger.warning(
                            f"🛡️ CORRELATION GUARD BLOCK // Node {symbol} rejected. "
                            f"High-covariance trade already open in group [{group_name}]: {active_correlated_nodes}. Over-exposure aborted."
                        )
                        return False

        # 4. Check Node-Specific Allocation Cap (Concentration Risk Mitigation)
        if symbol:
            current_node_exposure = self.active_positions.get(symbol, 0.0)
            if (current_node_exposure + new_position_notional) > (current_balance * self.max_single_asset_leverage_limit):
                logger.warning(f"⚠️ Single asset concentration risk limit reached for {symbol}: Exceeds limit of {self.max_single_asset_leverage_limit}x balance.")
                return False

        # 5. Check Global Exposure (The Swarm Central Banker)
        # Sum of current positions + proposed position must not exceed 300% of balance
        total_exposure = sum(self.active_positions.values()) + new_position_notional
        if total_exposure > (current_balance * 3.0):
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

    def compute_variance_adjusted_kelly(self, account_balance: float, win_rate: float, win_loss_ratio: float, asset_volatility_atr: float, current_price: float, ai_confidence: float = 0.5, market_regime: str = "TRENDING") -> Dict[str, Any]:
        """
        Calculates position sizes using the Kelly Criterion, adjusted for market variance, AI confidence, 
        macroeconomic regime states, and isolates compounded returns into a harvesting chamber.
        """
        if self.emergency_circuit_breaker:
            return {"approved": False, "size": 0.0, "recommended_leverage": 1, "allocated_value_usdt": 0.0, "target_fraction": 0.0}

        # Safety bounds guard for input parsing
        if current_price <= 0 or asset_volatility_atr < 0:
            logger.error("Invalid pricing vectors passed to Kelly optimization module.")
            return {"approved": False, "size": 0.0, "recommended_leverage": 1, "allocated_value_usdt": 0.0, "target_fraction": 0.0}

        # 🚀 PHASE 2 ADVANCEMENT: DYNAMIC CAPITAL HARVESTING VAULT
        # If the account balance compounds past our target baseline limit, the engine captures and locks 
        # away the excess, restricting Kelly sizing inputs strictly to the baseline amount to insulate cash.
        effective_balance = account_balance
        if account_balance > self.harvesting_baseline_usdt:
            harvested_yield = account_balance - self.harvesting_baseline_usdt
            effective_balance = self.harvesting_baseline_usdt
            logger.info(
                f"💰 HARVESTING VAULT ENGAGED // Total Wallet: ${account_balance:.4f} USDT | "
                f"Isolated Secured Yield: ${harvested_yield:.4f} USDT | Kelly Operating Baseline: ${effective_balance:.2f} USDT"
            )

        # Standard Kelly Formula: f = p - (q / b)
        p = max(0.0, min(1.0, win_rate))
        q = 1.0 - p
        b = win_loss_ratio
        
        raw_kelly = p - (q / b) if b > 0 else 0.0
        
        if raw_kelly <= 0:
            return {"approved": False, "size": 0.0, "recommended_leverage": 1, "allocated_value_usdt": 0.0, "target_fraction": 0.0}

        # --- DYNAMIC CEILING CONFIGURATION ---
        dynamic_risk_ceiling = self.max_single_risk + (0.13 * ai_confidence)
        dynamic_risk_ceiling = min(dynamic_risk_ceiling, 0.15)

        # Apply institutional fraction modifier (Quarter-Kelly) along with variance scaling
        variance_penalty_factor = 1.0 - (asset_volatility_atr / current_price)
        safe_fraction = raw_kelly * 0.25 * max(0.1, variance_penalty_factor)
        
        # Scale sizing fraction directly based on AI context conviction
        safe_fraction = safe_fraction * (1.0 + max(0.0, ai_confidence))
        
        # REGIME-SPECIFIC RISK CONTRACTION
        if market_regime == "RANGING":
            safe_fraction = safe_fraction * 0.50
            dynamic_risk_ceiling = dynamic_risk_ceiling * 0.60
            logger.debug("🛡️ RANGING REGIME DETECTED: Risk parameters actively compressed.")
            
        safe_fraction = min(safe_fraction, dynamic_risk_ceiling)
        
        # Calculate market structural distance thresholds
        stop_loss_distance_ticks = asset_volatility_atr * 2.0
        risk_per_token_pct = stop_loss_distance_ticks / current_price
        
        # Volatility-derived cap to prevent liquidation before your stop loss is triggered
        max_safe_leverage_by_vol = int(1.0 / max(0.01, risk_per_token_pct))
        
        # REGIME-SPECIFIC LEVERAGE CLAMP
        if market_regime == "RANGING":
            leverage_cap = min(8, max(2, max_safe_leverage_by_vol))
        else:
            leverage_cap = min(15, max(2, max_safe_leverage_by_vol))
            
        base_leverage = min(5, leverage_cap)
        
        # Initial target capital deployment (margin size calculated using effective safe harvesting balance)
        margin_allocated = effective_balance * safe_fraction
        calculated_notional = margin_allocated * base_leverage
        
        # --- MICRO-ACCOUNT ADAPTIVE LEVERAGE ENGINE ---
        if calculated_notional < self.exchange_min_notional:
            recommended_leverage = leverage_cap
            required_margin = self.exchange_min_notional / recommended_leverage
            required_fraction = required_margin / effective_balance
            
            # Safety Check
            if required_fraction <= dynamic_risk_ceiling and required_margin < (effective_balance * self.max_drawdown_pct):
                logger.info(
                    f"🔄 Micro-balance optimization active. Scaling parameters -> "
                    f"Margin: {required_fraction:.2%} (${required_margin:.2f}) | Leverage: {recommended_leverage}x"
                )
                safe_fraction = required_fraction
                notional_position_usdt = self.exchange_min_notional
            else:
                logger.warning(
                    f"⚠️ Scale-to-minimum rejected: Required risk fraction ({required_fraction:.2%}) "
                    f"or required margin (${required_margin:.2f}) breaches absolute safety bounds."
                )
                return {"approved": False, "size": 0.0, "recommended_leverage": 1, "allocated_value_usdt": 0.0, "target_fraction": 0.0}
        else:
            recommended_leverage = base_leverage
            notional_position_usdt = calculated_notional

        token_quantity = notional_position_usdt / current_price

        return {
            "approved": True,
            "target_fraction": safe_fraction,
            "recommended_leverage": recommended_leverage,
            "size": round(token_quantity, 4),
            "allocated_value_usdt": round(notional_position_usdt, 2)
        }