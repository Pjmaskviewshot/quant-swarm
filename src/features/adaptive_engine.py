import numpy as np
from collections import deque
from typing import Dict, List, Any

class AdaptiveFeatureEngine:
    def __init__(self, memory_window_short: int = 500, memory_window_long: int = 1800):
        # Increased windows to support 100-period lookbacks
        self.obi_history = deque(maxlen=memory_window_short)
        self.spread_history = deque(maxlen=memory_window_short)
        self.tfi_history = deque(maxlen=memory_window_short)
        
        # Multi-Timeframe memory
        self.timeframes = {"1m": deque(maxlen=100), "5m": deque(maxlen=300), "15m": deque(maxlen=900)}
        
        self._latest_mid = 0.0
        self._orderbook_snapshot = {"bids": [], "asks": []}
        self.ema_spread = 0.0
        self.spread_alpha = 0.15

    def get_computed_atr(self, period: int = 14) -> float:
        """
        🚀 STRUCTURAL UPGRADE: Wilder's Smoothing ATR (14-period).
        Eliminates noise by using a statistically significant window.
        """
        candles = list(self.timeframes["5m"]) if len(self.timeframes["5m"]) > period else list(self.timeframes["1m"])
        
        if len(candles) < period + 1:
            return 0.0

        tr_values = []
        for i in range(1, len(candles)):
            high = float(candles[i]["high"])
            low = float(candles[i]["low"])
            prev_close = float(candles[i-1]["close"])
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_values.append(tr)
        
        # Wilder's Smoothing Implementation
        atr = sum(tr_values[:period]) / period
        for i in range(period, len(tr_values)):
            atr = (atr * (period - 1) + tr_values[i]) / period
            
        return float(atr)

    def detect_market_regime(self) -> str:
        """
        🚀 STRUCTURAL UPGRADE: Robust Kaufman Efficiency Ratio.
        """
        candles = list(self.timeframes["5m"]) if len(self.timeframes["5m"]) >= 45 else list(self.timeframes["1m"])
        lookback = min(len(candles), 45)
        
        if lookback < 20: return "RANGING"
        
        closes = np.array([float(c["close"]) for c in candles[-lookback:]])
        directional_change = abs(closes[-1] - closes[0])
        absolute_changes = np.sum(np.abs(np.diff(closes)))
        
        er = directional_change / absolute_changes if absolute_changes > 0 else 0.0
        
        # Squeeze detection
        sma = np.mean(closes)
        std_dev = np.std(closes)
        bb_width = (4 * std_dev) / sma if sma > 0 else 0.0
        
        return "TRENDING" if (er >= 0.35 and bb_width >= 0.004) else "RANGING"

    def push_orderbook_tick(self, bids: List[List[str]], asks: List[List[str]]) -> Dict[str, Any]:
        """
        🚀 STRUCTURAL UPGRADE: Robust Z-Score (Median + MAD)
        """
        if not bids or not asks: return {"valid": False}

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        mid_price = (best_bid + best_ask) / 2.0
        
        # EMA Spread
        raw_spread = best_ask - best_bid
        self.ema_spread = (raw_spread * self.spread_alpha) + (self.ema_spread * (1.0 - self.spread_alpha)) if self.ema_spread > 0 else raw_spread
        
        self._latest_mid = mid_price
        
        v_b = sum(float(tier[1]) for tier in bids[:5])
        v_a = sum(float(tier[1]) for tier in asks[:5])
        obi = (v_b - v_a) / (v_b + v_a) if (v_b + v_a) > 0 else 0.0
        
        self.obi_history.append(obi)
        
        # 🚀 ROBUST Z-SCORE USING MAD (Median Absolute Deviation)
        # This is immune to price wicks that kill standard deviation
        obi_z_score = 0.0
        if len(self.obi_history) >= 100:
            obi_array = np.array(self.obi_history)
            median = np.median(obi_array)
            mad = np.median(np.abs(obi_array - median))
            # 1.4826 is the scaling factor for Normal Distribution consistency
            obi_z_score = (obi - median) / (mad * 1.4826 + 1e-6)

        current_tfi = self.tfi_history[-1] if len(self.tfi_history) > 0 else 0.0

        return {
            "valid": True,
            "mid_price": mid_price,
            "adaptive_obi_z": round(obi_z_score, 4),
            "market_regime": self.detect_market_regime(),
            "bid_ask_spread": round(self.ema_spread, 6)
        }