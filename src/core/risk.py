import numpy as np
import logging
from typing import Dict, Any, List

logger = logging.getLogger("QUANT_CORE.RISK")

class MathematicalRiskEngine:
    def __init__(self, kelly_fraction: float = 0.25, max_portfolio_risk: float = 0.05):
        self.kelly_fraction = kelly_fraction
        self.max_portfolio_risk = max_portfolio_risk  # Global capital isolation protection ceiling

    @staticmethod
    def calculate_average_true_range(klines: List[List[str]], period: int = 14) -> float:
        """Calculates historical systemic market volatility using True Range arrays."""
        # Bybit kline sequence layout: [0]Timestamp, [1]Open, [2]High, [3]Low, [4]Close, [5]Volume
        highs = np.array([float(candle[2]) for candle in klines])
        lows = np.array([float(candle[3]) for candle in klines])
        closes = np.array([float(candle[4]) for candle in klines])
        
        # Shift close prices to align with preceding intervals
        shifted_closes = np.roll(closes, 1)
        shifted_closes[0] = float(klines[1][1]) # Prevent zero delta wrapping index boundary
        
        tr1 = highs - lows
        tr2 = np.abs(highs - shifted_closes)
        tr3 = np.abs(lows - shifted_closes)
        
        true_range = np.maximum.reduce([tr1, tr2, tr3])
        atr = float(np.mean(true_range[:period]))
        return atr

    def evaluate_position_matrix(self, account_balance: float, current_price: float, atr: float, confidence: float, direction: str) -> Dict[str, Any]:
        """
        Executes Fractional Kelly Asset Allocation & Dynamic ATR Stop Bound Selection.
        Uses structural risk reward odds b = 2.0 (Target Take Profit boundary is double the dynamic Stop Loss range)
        """
        if confidence <= 0.50 or direction not in ["BUY", "SELL"]:
            return {"executable": False, "size": 0.0, "leverage": 1, "tp": 0.0, "sl": 0.0}

        # 1. Structural Risk Boundaries (Dynamic volatility buffering)
        stop_loss_distance = atr * 1.5
        take_profit_distance = atr * 3.0

        if direction == "BUY":
            sl_price = current_price - stop_loss_distance
            tp_price = current_price + take_profit_distance
        else:
            sl_price = current_price + stop_loss_distance
            tp_price = current_price - take_profit_distance

        # 2. Mathematical Position Sizing via Kelly Criterion Framework
        b = 2.0  # Systemic Risk/Reward Odds ratio
        p = confidence
        q = 1.0 - p
        
        kelly_optimal_fraction = p - (q / b)
        
        if kelly_optimal_fraction <= 0:
            return {"executable": False, "size": 0.0, "leverage": 1, "tp": 0.0, "sl": 0.0}
            
        # Apply fractional buffer scale to minimize tail-risk events
        safe_allocation_fraction = kelly_optimal_fraction * self.kelly_fraction
        
        # Enforce absolute global capital allocation ceiling guardrails
        safe_allocation_fraction = min(safe_allocation_fraction, self.max_portfolio_risk)
        
        target_risk_capital = account_balance * safe_allocation_fraction
        risk_per_token_pct = stop_loss_distance / current_price
        
        position_magnitude_usdt = target_risk_capital / risk_per_token_pct
        
        # Dynamically define leverage target bounded by safety thresholds
        calculated_leverage = int(min(max(int(position_magnitude_usdt / account_balance), 1), 10))
        
        return {
            "executable": True,
            "size": round(position_magnitude_usdt / current_price, 4),
            "leverage": calculated_leverage,
            "tp": round(tp_price, 2),
            "sl": round(sl_price, 2)
        }