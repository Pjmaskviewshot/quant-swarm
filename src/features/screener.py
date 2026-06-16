import logging
import numpy as np
from typing import Dict, List, Any

logger = logging.getLogger("QUANT_CORE.SCREENER")

class DynamicAssetScreener:
    def __init__(self, target_symbols: List[str], window_size: int = 60):
        self.symbols = target_symbols
        self.window_size = window_size
        
        # Historical memory buffers for tracking baselines
        self.volume_history: Dict[str, List[float]] = {symbol: [] for symbol in target_symbols}
        self.price_history: Dict[str, List[float]] = {symbol: [] for symbol in target_symbols}

    def update_ticker_metrics(self, symbol: str, current_price: float, current_volume: float) -> Dict[str, Any]:
        """Updates rolling baseline matrices and checks for volume/volatility anomalies."""
        if symbol not in self.symbols:
            return {"qualified": False}

        v_history = self.volume_history[symbol]
        p_history = self.price_history[symbol]

        # Append new data points
        v_history.append(current_volume)
        p_history.append(current_price)

        # Maintain sliding window memory limits
        if len(v_history) > self.window_size:
            v_history.pop(0)
        if len(p_history) > self.window_size:
            p_history.pop(0)

        # Warmup guardrail
        if len(v_history) < 15:
            return {"qualified": False, "reason": "Warming up buffers"}

        # Calculate standard deviation metrics for volatility (Z-Score)
        returns = np.diff(np.log(p_history)) if len(p_history) > 1 else [0.0]
        mean_return = np.mean(returns) if len(returns) > 0 else 0.0
        std_return = np.std(returns) if len(returns) > 0 else 1e-6
        
        current_return = returns[-1] if len(returns) > 0 else 0.0
        z_score_volatility = abs((current_return - mean_return) / std_return) if std_return > 0 else 0.0

        # Calculate volume spike ratio
        mean_historical_volume = np.mean(v_history[:-1]) if len(v_history) > 1 else 1.0
        volume_multiplier = current_volume / mean_historical_volume if mean_historical_volume > 0 else 1.0

        # Verification logic: Qualify asset if it experiences a 2x volume anomaly OR an extreme price deviation
        is_qualified = volume_multiplier >= 2.2 or z_score_volatility >= 2.5

        return {
            "symbol": symbol,
            "qualified": is_qualified,
            "volume_multiplier": volume_multiplier,
            "volatility_z": z_score_volatility,
            "price": current_price
        }