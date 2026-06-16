import logging
from typing import Dict, Any

logger = logging.getLogger("QUANT_CORE.RISK_MANAGER")

class InstitutionalRiskVault:
    def __init__(self, max_drawdown_pct: float = 0.10, max_single_position_risk_pct: float = 0.02, exchange_min_notional: float = 5.0, max_single_asset_leverage_limit: float = 1.5):
        """
        Risk engine initialized with baseline protections.
        Note: For small account balances (e.g., $7), max_drawdown_pct has been optimized 
        to 10% to allow the micro-account engine room to hit exchange minimum requirements.
        
        Parameters:
            max_drawdown_pct (float): Maximum trailing drawdown limit before circuit breaking.
            max_single_position_risk_pct (float): Baseline risk fraction per position.
            exchange_min_notional (float): Minimum order cost required by the exchange interface.
            max_single_asset_leverage_limit (float): Maximum total leverage exposure allowed for a single asset node (e.g., 1.5x account balance).
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

    def evaluate_portfolio_safety(self, current_balance: float, new_position_notional: float = 0.0, symbol: str = "") -> bool:
        """
        Enforces a rigid hardware circuit breaker if portfolio trailing drawdown parameters are breached,
        and ensures aggregate global exposure does not exceed safe leverage limits.
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

    def compute_variance_adjusted_kelly(self, account_balance: float, win_rate: float, win_loss_ratio: float, asset_volatility_atr: float, current_price: float, ai_confidence: float = 0.5) -> Dict[str, Any]:
        """
        Calculates position sizes using the Kelly Criterion, adjusted for market variance and AI confidence.
        Dynamically scales leverage multipliers and margin allocation constraints to satisfy exchange 
        minimum order parameters for micro-balances.
        """
        if self.emergency_circuit_breaker:
            return {"approved": False, "size": 0.0, "recommended_leverage": 1, "allocated_value_usdt": 0.0, "target_fraction": 0.0}

        # Safety bounds guard for input parsing
        if current_price <= 0 or asset_volatility_atr < 0:
            logger.error("Invalid pricing vectors passed to Kelly optimization module.")
            return {"approved": False, "size": 0.0, "recommended_leverage": 1, "allocated_value_usdt": 0.0, "target_fraction": 0.0}

        # Standard Kelly Formula: f = p - (q / b)
        p = max(0.0, min(1.0, win_rate))
        q = 1.0 - p
        b = win_loss_ratio
        
        raw_kelly = p - (q / b) if b > 0 else 0.0
        
        if raw_kelly <= 0:
            return {"approved": False, "size": 0.0, "recommended_leverage": 1, "allocated_value_usdt": 0.0, "target_fraction": 0.0}

        # --- DYNAMIC CEILING CONFIGURATION ---
        # Allows the risk cap to safely expand from your baseline up to 15% under high-conviction signals
        dynamic_risk_ceiling = self.max_single_risk + (0.13 * ai_confidence)
        dynamic_risk_ceiling = min(dynamic_risk_ceiling, 0.15)

        # Apply institutional fraction modifier (Quarter-Kelly) along with variance scaling
        variance_penalty_factor = 1.0 - (asset_volatility_atr / current_price)
        safe_fraction = raw_kelly * 0.25 * max(0.1, variance_penalty_factor)
        
        # Scale sizing fraction directly based on AI context conviction
        safe_fraction = safe_fraction * (1.0 + max(0.0, ai_confidence))
        safe_fraction = min(safe_fraction, dynamic_risk_ceiling)
        
        # Calculate market structural distance thresholds
        stop_loss_distance_ticks = asset_volatility_atr * 2.0
        risk_per_token_pct = stop_loss_distance_ticks / current_price
        
        # Volatility-derived cap to prevent liquidation before your stop loss is triggered
        max_safe_leverage_by_vol = int(1.0 / max(0.01, risk_per_token_pct))
        leverage_cap = min(15, max(2, max_safe_leverage_by_vol))
        
        # Baseline leverage selection for steady-state conditions
        base_leverage = min(5, leverage_cap)
        
        # Initial target capital deployment (margin size)
        margin_allocated = account_balance * safe_fraction
        calculated_notional = margin_allocated * base_leverage
        
        # --- MICRO-ACCOUNT ADAPTIVE LEVERAGE ENGINE ---
        # If the base calculation cannot meet the exchange minimum rules due to low capital,
        # the engine scales up leverage and margin in tandem to pass the minimum threshold.
        if calculated_notional < self.exchange_min_notional:
            # Force leverage to the maximum structurally safe ceiling to preserve raw margin
            recommended_leverage = leverage_cap
            
            # Recalculate required margin at this maximized leverage configuration
            required_margin = self.exchange_min_notional / recommended_leverage
            required_fraction = required_margin / account_balance
            
            # Safety Check: Ensure forced parameters do not cross absolute risk bounds
            if required_fraction <= dynamic_risk_ceiling and required_margin < (account_balance * self.max_drawdown_pct):
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
            # Capital footprint is sufficient to clear limits using baseline leverage
            recommended_leverage = base_leverage
            notional_position_usdt = calculated_notional
        # -----------------------------------------------

        token_quantity = notional_position_usdt / current_price

        return {
            "approved": True,
            "target_fraction": safe_fraction,
            "recommended_leverage": recommended_leverage,
            "size": round(token_quantity, 4),
            "allocated_value_usdt": round(notional_position_usdt, 2)
        }