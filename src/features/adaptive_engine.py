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
        
        # 🛡️ State trackers for FSM and execution pipeline
        self._latest_mid = 0.0
        self._orderbook_snapshot = {"bids": [], "asks": []}
        
        # 🚀 INSTITUTIONAL UPGRADE: Low-pass Filter for Order Book Jitter
        self.ema_spread = 0.0
        self.spread_alpha = 0.15  # Slower adjustment locks out microsecond flash spikes

    def detect_market_regime(self) -> str:
        """
        🚀 LIGHTWEIGHT STATISTICAL REGIME CLASSIFIER
        Uses Kaufman's Efficiency Ratio (ER) and Volatility Squeeze metrics to 
        mathematically classify the market state without heavy ML dependencies.
        """
        # Prioritize 5m timeframe for structural clarity, fallback to 1m
        # Wait for at least 45 candles before making a judgment
        target_tf = "5m" if "5m" in self.timeframes and len(self.timeframes["5m"]) > 45 else "1m"

        # EXTENDED LOOKBACK TO 45 CANDLES (~3.75 HOURS)
        if target_tf in self.timeframes and len(self.timeframes[target_tf]) > 45:
            candles = list(self.timeframes[target_tf])[-45:]
            closes = np.array([float(c["close"]) for c in candles])

            # 1. Kaufman's Efficiency Ratio (ER)
            # ER = Directional Change / Sum of Absolute Changes (Noise)
            directional_change = abs(closes[-1] - closes[0])
            absolute_changes = np.sum(np.abs(np.diff(closes)))

            efficiency_ratio = directional_change / absolute_changes if absolute_changes > 0 else 0.0

            # 2. Volatility Contraction / Bollinger Band Squeeze
            sma = np.mean(closes)
            std_dev = np.std(closes)
            # Approximate Bollinger Band Width percentage
            bb_width = (4 * std_dev) / sma if sma > 0 else 0.0

            # 3. Regime Matrix Logic
            # If the market is too noisy (ER < 0.35) or too tightly compressed (BBW < 0.4%)
            if efficiency_ratio < 0.35 or bb_width < 0.004:
                return "RANGING"
            else:
                return "TRENDING"
                
        # Defensive fallback: assume ranging to protect capital if data is warming up
        return "RANGING"

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
            raw_spread = best_ask - best_bid
            
            # 🚀 INSTITUTIONAL UPGRADE: Smooth Raw Spread via EMA to Filter Websocket Jitter
            if self.ema_spread == 0.0:
                self.ema_spread = raw_spread
            else:
                self.ema_spread = (raw_spread * self.spread_alpha) + (self.ema_spread * (1.0 - self.spread_alpha))
            
            # 🛡️ Store latest tick data for orchestrator access
            self._latest_mid = mid_price
            self._orderbook_snapshot = {"bids": bids[:5], "asks": asks[:5]}

            # 2. Compute Volume-Weighted Order Book Imbalance (OBI)
            # Sample across top 5 high-density institutional liquidity tiers
            v_b = sum(float(tier[1]) for tier in bids[:5])
            v_a = sum(float(tier[1]) for tier in asks[:5])
            
            obi = (v_b - v_a) / (v_b + v_a) if (v_b + v_a) > 0 else 0.0

            # 🚀 MIEG UPGRADE: Capture the previous tick's imbalance baseline before appending
            prev_obi = self.obi_history[-1] if len(self.obi_history) > 0 else obi

            # Update structural historical data vaults
            self.obi_history.append(obi)
            self.spread_history.append(raw_spread)  # Keep raw spread here to preserve raw volatility flags

            # 3. Adaptive Threshold Engine (Dynamic Z-Score Generation)
            obi_z_score = 0.0
            if len(self.obi_history) > 30:
                obi_array = np.array(self.obi_history)
                mean_obi = np.mean(obi_array)
                std_obi = np.std(obi_array)
                # Prevent zero-division wrap errors in static markets
                obi_z_score = (obi - mean_obi) / std_obi if std_obi > 0 else 0.0

            # 🚀 MIEG UPGRADE: Microstructure Imbalance Exhaustion Gate Evaluation
            # Checks if extreme buying/selling clusters are rolling over toward zero
            mieg_confirmed = False
            if obi_z_score >= 2.4 and obi < prev_obi:
                mieg_confirmed = True  # Overbought peak has physically rolled over (Short Entry Armed)
            elif obi_z_score <= -2.4 and obi > prev_obi:
                mieg_confirmed = True  # Oversold trough has physically bottomed out (Long Entry Armed)

            # 4. Machine Learning Feature Matrix Payload Extraction
            features = {
                "valid": True,
                "timestamp": time.time(),
                "mid_price": mid_price,
                "raw_spread": round(raw_spread, 6),
                "bid_ask_spread": round(self.ema_spread, 6),
                "raw_obi": round(obi, 4),
                "adaptive_obi_z": round(obi_z_score, 4),
                # 🚀 Pass the zero-lag exhaustion gate validation status to the orchestrator loop
                "mieg_confirmed": mieg_confirmed,
                "micro_volatility_z": round(self._calculate_spread_volatility_z(), 4),
                "liquidity_density_ratio": round(v_b / v_a if v_a > 0 else 1.0, 4),
                "market_regime": self.detect_market_regime()
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
        """Measures high-frequency raw spread expansion to detect impending toxicity events."""
        if len(self.spread_history) < 10:
            return 0.0
        spreads = np.array(self.spread_history)
        current_spread = spreads[-1]
        return float((current_spread - np.mean(spreads)) / (np.std(spreads) + 1e-6))

    # =================================================================
    # 🛡️ EXPOSED METHODS FOR ORCHESTRATOR COMMUNICATION
    # =================================================================

    def get_latest_mid(self) -> float:
        """Returns the most recent mid-price from the fast websocket stream."""
        return getattr(self, '_latest_mid', 0.0)

    def get_orderbook_snapshot(self) -> Dict[str, List]:
        """Returns the current order book snapshot for Iceberg execution."""
        return getattr(self, '_orderbook_snapshot', {"bids": [], "asks": []})

    def get_computed_atr(self) -> float:
        """Calculates dynamic True Range based on recent 1m or 5m candles."""
        # Try to use 5m candles first, fallback to 1m
        target_tf = "5m" if "5m" in self.timeframes and len(self.timeframes["5m"]) > 10 else "1m"
        
        if target_tf in self.timeframes and len(self.timeframes[target_tf]) > 10:
            candles = list(self.timeframes[target_tf])
            
            # Simple ATR approximation using max of High-Low, High-PrevClose, Low-PrevClose
            tr_values = []
            for i in range(1, len(candles)):
                high = float(candles[i].get("high", 0))
                low = float(candles[i].get("low", 0))
                prev_close = float(candles[i-1].get("close", 0))
                
                tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                tr_values.append(tr)
                
            if tr_values:
                return sum(tr_values) / len(tr_values)
                
        return 0.0  # Will trigger the safety fallback in main.py if not enough data