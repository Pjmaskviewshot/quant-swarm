import logging
from typing import Dict, Any

logger = logging.getLogger("QUANT_CORE.RISK_MANAGER")

class InstitutionalRiskVault:
    def __init__(self, max_drawdown_pct: float = 0.03, max_single_position_risk_pct: float = 0.01):
        self.max_drawdown_pct = max_drawdown_pct
        self.max_single_risk = max_single_position_risk_pct
        self.peak_balance = 0.0
        self.emergency_circuit_breaker = False

    def evaluate_portfolio_safety(self, current_balance: float) -> bool:
        """Enforces a rigid hardware circuit breaker if portfolio trailing drawdown parameters are breached."""
        if current_balance > self.peak_balance:
            self.peak_balance = current_balance
            
        if self.peak_balance > 0:
            current_drawdown = (self.peak_balance - current_balance) / self.peak_balance
            if current_drawdown >= self.max_drawdown_pct:
                if not self.emergency_circuit_breaker:
                    logger.critical(f"🚨 ABSOLUTE MAX DRAWDOWN BREACHED ({current_drawdown:.2%}). LOCKING DOWN SYSTEMS.")
                    self.emergency_circuit_breaker = True
                return False
                
        return not self.emergency_circuit_breaker

    def compute_variance_adjusted_kelly(self, account_balance: float, win_rate: float, win_loss_ratio: float, asset_volatility_atr: float, current_price: float) -> Dict[str, Any]:
        """
        Calculates position sizes using the Kelly Criterion, adjusted for market variance.
        Reduces size proportionally if the asset's volatility expands rapidly.
        """
        if self.emergency_circuit_breaker:
            return {"approved": False, "size": 0.0}

        # Standard Kelly Formula: f = p - (q / b)
        p = win_rate
        q = 1.0 - p
        b = win_loss_ratio
        
        raw_kelly = p - (q / b) if b > 0 else 0.0
        
        if raw_kelly <= 0:
            return {"approved": False, "size": 0.0}

        # Apply institutional fraction modifier (Quarter-Kelly) along with variance scaling
        variance_penalty_factor = 1.0 - (asset_volatility_atr / current_price)
        safe_fraction = raw_kelly * 0.25 * max(0.1, variance_penalty_factor)
        
        # Absolute structural risk allocation ceiling constraint matching capital preservation boundaries
        safe_fraction = min(safe_fraction, self.max_single_risk)
        
        target_capital_at_risk = account_balance * safe_fraction
        stop_loss_distance_ticks = asset_volatility_atr * 2.0
        risk_per_token_pct = stop_loss_distance_ticks / current_price
        
        allocated_position_usdt = target_capital_at_risk / risk_per_token_pct
        token_quantity = allocated_position_usdt / current_price

        return {
            "approved": True,
            "target_fraction": safe_fraction,
            "size": round(token_quantity, 4),
            "allocated_value_usdt": round(allocated_position_usdt, 2)
        }