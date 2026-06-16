import numpy as np
import time
import logging
from collections import deque
from typing import Dict, Any, Tuple, List

logger = logging.getLogger("QUANT_CORE.ADAPTIVE_ENGINE")

class AdaptiveFeatureEngine:
    def __init__(self, memory_window_short: int = 300, memory_window_long: int = 1800):
        # Rolling high-frequency buffers (Tick/Update-based memory)
        self.obi_history = deque(maxlen=memory_window_short)
        self.spread_history = deque(maxlen=memory_window_short)
        
        # Multi-Timeframe micro-aggregates (1m, 5m, 15m)
        self.timeframes = {"1m": deque(maxlen=60), "5m": deque(maxlen=300), "15m": deque(maxlen=900)}
        self.long_window = memory_window_long

    def push_orderbook_tick(self, bids: List[List[str]], asks: List[List[str]]) -> Dict[str, Any]:
        """
        Consumes raw level 2 structural updates.
        Computes rolling statistical parameters and extracts ML-ready feature arrays.
        """
        if not bids or not asks:
            return {"valid": False}

        try:
            # 1. Extract Microstructure Absolute Bounds
            best_bid = float(bids[0][0])
            best_ask = float(asks[0][0])
            mid_price = (best_bid + best_ask) / 2.0
            bid_ask_spread = best_ask - best_bid

            # 2. Compute Volume-Weighted Order Book Imbalance (OBI)
            # Sample across top 5 high-density institutional liquidity tiers
            v_b = sum(float(tier[1]) for tier in bids[:5])
            v_a = sum(float(tier[1]) for tier in asks[:5])
            
            obi = (v_b - v_a) / (v_b + v_a) if (v_b + v_a) > 0 else 0.0

            # Update structural historical data vaults
            self.obi_history.append(obi)
            self.spread_history.append(bid_ask_spread)

            # 3. Adaptive Threshold Engine (Dynamic Z-Score Generation)
            obi_z_score = 0.0
            if len(self.obi_history) > 30:
                obi_array = np.array(self.obi_history)
                mean_obi = np.mean(obi_array)
                std_obi = np.std(obi_array)
                # Prevent zero-division wrap errors in static markets
                obi_z_score = (obi - mean_obi) / std_obi if std_obi > 0 else 0.0

            # 4. Machine Learning Feature Matrix Payload Extraction
            features = {
                "valid": True,
                "timestamp": time.time(),
                "mid_price": mid_price,
                "bid_ask_spread": bid_ask_spread,
                "raw_obi": round(obi, 4),
                "adaptive_obi_z": round(obi_z_score, 4),
                "micro_volatility_z": round(self._calculate_spread_volatility_z(), 4),
                "liquidity_density_ratio": round(v_b / v_a if v_a > 0 else 1.0, 4)
            }

            return features

        except Exception as e:
            logger.error(f"Failed to process microstructure feature sequence vector: {e}")
            return {"valid": False}

    def update_multi_timeframe_candle(self, timeframe: str, open_p: float, high_p: float, low_p: float, close_p: float, volume: float):
        """Injects micro-candle snapshots to maintain multi-timeframe synchronization."""
        if timeframe in self.timeframes:
            self.timeframes[timeframe].append({
                "open": open_p, "high": high_p, "low": low_p, "close": close_p, "volume": volume
            })

    def extract_multi_timeframe_momentum(self) -> Dict[str, float]:
        """Computes structural cross-timeframe velocity deltas for ML prediction gating."""
        momentum_matrix = {}
        for tf, candles in self.timeframes.items():
            if len(candles) < 2:
                momentum_matrix[f"momentum_{tf}"] = 0.0
                continue
            
            current_close = candles[-1]["close"]
            historical_close = candles[0]["close"]
            # Log returning percentage returns across lookback boundaries
            momentum_matrix[f"momentum_{tf}"] = (current_close - historical_close) / historical_close
            
        return momentum_matrix

    def _calculate_spread_volatility_z(self) -> float:
        """Measures high-frequency spread expansion to detect impending toxicity events."""
        if len(self.spread_history) < 10:
            return 0.0
        spreads = np.array(self.spread_history)
        current_spread = spreads[-1]
        return float((current_spread - np.mean(spreads)) / (np.std(spreads) + 1e-6))