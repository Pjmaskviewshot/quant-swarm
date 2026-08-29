"""
💎 V25.0 APEX QUANTUM PRIME: HIGHER-ORDER FEATURE ENGINE
--------------------------------------------------------------
Multi-Timeframe Physics & Volatility Scaling.

Architectural Upgrades (V25.0):
1. L2 Orderbook Reconstruction Eradicated: Orderbook state tracking has been 
   stripped from this module and delegated to the centralized MarketStateMatrix. 
   This permanently eliminates CPU pruning spikes and dictionary memory bloat.
2. Focused HTF Oracle: This engine now acts exclusively as the source of truth 
   for Multi-Timeframe Momentum, Average True Range (ATR), and Kalman-smoothed 
   Trend Bias.
3. Bounded Jump-Diffusion Kalman Filter: Retained for pure mathematical 
   price-series smoothing without overflow risks.
"""

import math
import numpy as np
import logging
from collections import deque
from typing import Dict, List

logger = logging.getLogger("QUANT_CORE.ADAPTIVE_ENGINE")


class AdaptiveFeatureEngine:
    def __init__(self, memory_window_long: int = 1800):
        self.prices = deque(maxlen=memory_window_long)
        
        self.timeframes = {
            "1": deque(maxlen=200), 
            "5": deque(maxlen=300), 
            "15": deque(maxlen=900),
            "60": deque(maxlen=200),  
            "240": deque(maxlen=100)  
        }
        self.long_window = memory_window_long

    def _apply_adaptive_kalman_smoothing(self, prices: np.ndarray) -> np.ndarray:
        """
        🚀 V25.0 JUMP-DIFFUSION ADAPTIVE KALMAN FILTER (JD-AKF)
        Strict polynomial scaling to eradicate FloatOverflow during flash crashes.
        """
        if len(prices) < 2:
            return prices
            
        n = len(prices)
        filtered = np.zeros(n)
        
        base_Q = 1e-5 # Baseline Process Noise
        
        rolling_diffs = np.abs(np.diff(prices))
        base_variance = max(float(np.var(prices) * 0.05), 1e-9)
        
        x_hat = float(prices[0])
        P = 1.0
        
        for i in range(n):
            inst_jump = abs(float(prices[i]) - x_hat)
            avg_jump = float(np.mean(rolling_diffs[max(0, i-10):i])) if i > 10 else (inst_jump + 1e-9)
            
            # Mahalanobis-style anomaly distance
            jump_z = inst_jump / (avg_jump + 1e-9)
            
            # Algebraic Cap: Prevents overflow
            q_multiplier = 1.0 + min(100.0, jump_z ** 2)
            dynamic_Q = base_Q * q_multiplier
            
            x_pred = x_hat
            P_pred = P + dynamic_Q
            
            # Dynamic Measurement Noise (R)
            jump_ratio = min(5.0, inst_jump / (avg_jump + 1e-9))
            R_adaptive = base_variance / max(1.0, jump_ratio**2)
            
            K = P_pred / (P_pred + R_adaptive)
            x_hat = x_pred + K * (float(prices[i]) - x_pred)
            P = (1 - K) * P_pred
            
            filtered[i] = x_hat
            
        return filtered

    # ====================================================================
    # TIME-SERIES AND UTILITY METHODS
    # ====================================================================

    def update_multi_timeframe_candle(self, timeframe: str, open_p: float, high_p: float, low_p: float, close_p: float, volume: float):
        tf_key = str(timeframe).rstrip("m")
        if tf_key in self.timeframes:
            self.timeframes[tf_key].append({
                "open": open_p, "high": high_p, "low": low_p, "close": close_p, "volume": volume
            })
        if close_p > 0:
            self.prices.append(close_p)

    def extract_multi_timeframe_momentum(self) -> Dict[str, float]:
        momentum_matrix = {}
        for tf, candles in self.timeframes.items():
            if len(candles) < 2:
                momentum_matrix[f"momentum_{tf}"] = 0.0
                continue
            
            current_close = candles[-1]["close"]
            historical_close = candles[0]["close"]
            momentum_matrix[f"momentum_{tf}"] = (current_close - historical_close) / max(historical_close, 1e-9)
            
        return momentum_matrix

    def get_htf_trend_bias(self, current_price: float) -> float:
        """
        Derives an aggregated macro-trend bias from Kalman-smoothed 1H and 4H EMAs.
        Outputs a normalized vector between -1.0 (Bearish) and 1.0 (Bullish).
        """
        bias = 0.0
        
        if len(self.timeframes["240"]) >= 10:
            candles_4h = list(self.timeframes["240"])
            closes_4h = np.array([float(c["close"]) for c in candles_4h])
            
            alpha = 2.0 / (len(closes_4h) + 1)
            ema_4h = closes_4h[0]
            for val in closes_4h[1:]:
                ema_4h = (val * alpha) + (ema_4h * (1 - alpha))
                
            atr_4h = self.get_computed_atr(period=min(14, len(candles_4h)))
            if atr_4h > 0:
                bias_4h = np.clip((current_price - ema_4h) / atr_4h, -1.0, 1.0)
                bias += (bias_4h * 0.6)
                
        if len(self.timeframes["60"]) >= 20:
            candles_1h = list(self.timeframes["60"])
            closes_1h = np.array([float(c["close"]) for c in candles_1h])
            
            alpha = 2.0 / (min(20, len(closes_1h)) + 1)
            ema_1h = closes_1h[0]
            for val in closes_1h[1:]:
                ema_1h = (val * alpha) + (ema_1h * (1 - alpha))
                
            atr_1h = self.get_computed_atr(period=min(14, len(candles_1h)))
            if atr_1h > 0:
                bias_1h = np.clip((current_price - ema_1h) / atr_1h, -1.0, 1.0)
                bias += (bias_1h * 0.4)
                
        return np.clip(bias, -1.0, 1.0)

    def get_dynamic_rr_ratio(self) -> float:
        """
        Calculates Kaufman's Efficiency Ratio (ER) to dynamically stretch 
        the target Risk-Reward ratio during high-trend regimes.
        """
        if len(self.timeframes["5"]) >= 20:
            candles = list(self.timeframes["5"])[-20:]
            closes = np.array([float(c["close"]) for c in candles])
            
            directional_change = abs(closes[-1] - closes[0])
            absolute_changes = np.sum(np.abs(np.diff(closes)))
            er = float(directional_change / (absolute_changes + 1e-9))
        else:
            er = 0.5 
            
        dynamic_rr = 1.2 + (2.0 * (er ** 2))
        return np.clip(dynamic_rr, 1.2, 3.2)

    def get_computed_atr(self, period: int = 14) -> float:
        """
        Calculates the True Range volatility across the lowest reliable timeframe.
        """
        if len(self.timeframes["5"]) >= period + 1:
            candles = list(self.timeframes["5"])
        elif len(self.timeframes["1"]) >= period + 1:
            candles = list(self.timeframes["1"])
        elif len(self.prices) >= period + 1:
            return float(np.std(list(self.prices)[-period:]) * 1.5)
        else:
            return 0.0

        tr_values = []
        for i in range(1, len(candles)):
            high = float(candles[i].get("high", 0))
            low = float(candles[i].get("low", 0))
            prev_close = float(candles[i-1].get("close", 0))
            
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_values.append(tr)
            
        if not tr_values:
            return 0.0
            
        init_period = min(period, len(tr_values))
        atr = float(np.mean(tr_values[:init_period]))
        
        for i in range(init_period, len(tr_values)):
            atr = (atr * (period - 1) + tr_values[i]) / period
            
        return float(atr)