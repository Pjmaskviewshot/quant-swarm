import numpy as np
import time
import logging
from collections import deque
from typing import Dict, Any, Tuple, List

logger = logging.getLogger("QUANT_CORE.ADAPTIVE_ENGINE")

class AdaptiveFeatureEngine:
    def __init__(self, memory_window_short: int = 500, memory_window_long: int = 1800):
        # Rolling high-frequency buffers (Tick/Update-based memory)
        self.obi_history = deque(maxlen=memory_window_short)
        self.spread_history = deque(maxlen=memory_window_short)
        
        # 🚀 TFI UPGRADE: Rolling memory for Aggressive Trade Flow Imbalance
        self.tfi_history = deque(maxlen=memory_window_short)
        
        # Multi-Timeframe micro-aggregates (1m, 5m, 15m)
        self.timeframes = {"1m": deque(maxlen=100), "5m": deque(maxlen=300), "15m": deque(maxlen=900)}
        self.long_window = memory_window_long
        
        # 🛡️ State trackers for FSM and execution pipeline
        self._latest_mid = 0.0
        self._orderbook_snapshot = {"bids": [], "asks": []}
        
        # 🚀 INSTITUTIONAL UPGRADE: Low-pass Filter for Order Book Jitter
        self.ema_spread = 0.0
        self.spread_alpha = 0.15  # Slower adjustment locks out microsecond flash spikes

    def detect_market_regime(self) -> str:
        """
        🚀 DYNAMIC STATISTICAL REGIME CLASSIFIER (COLD-BOOT RESILIENT)
        Uses Kaufman's Efficiency Ratio (ER) and Volatility Squeeze metrics.
        Dynamically scales the lookback window so the bot doesn't fly blind upon boot.
        """
        # 1. Tiered Data Degradation: Use 5m if we have decent data, fallback to 1m, or hard fail.
        if len(self.timeframes["5m"]) >= 45:
            candles = list(self.timeframes["5m"])
        elif len(self.timeframes["1m"]) >= 20:
            candles = list(self.timeframes["1m"])
        else:
            return "RANGING"  # Absolute cold-boot fallback

        # 2. Extract closes up to the maximum optimal window (45 candles)
        lookback = min(len(candles), 45)
        recent_candles = candles[-lookback:]
        closes = np.array([float(c["close"]) for c in recent_candles])

        # 3. Kaufman's Efficiency Ratio (ER)
        directional_change = abs(closes[-1] - closes[0])
        absolute_changes = np.sum(np.abs(np.diff(closes)))
        
        efficiency_ratio = directional_change / absolute_changes if absolute_changes > 0 else 0.0

        # 4. Volatility Contraction / Bollinger Band Squeeze
        sma = np.mean(closes)
        std_dev = np.std(closes)
        bb_width = (4 * std_dev) / sma if sma > 0 else 0.0

        # 5. Regime Matrix Logic
        if efficiency_ratio < 0.35 or bb_width < 0.004:
            return "RANGING"
        else:
            return "TRENDING"

    def push_trade_tick(self, trades: List[Dict[str, Any]]):
        """
        🚀 TFI UPGRADE: Aggressive Trade Flow Imbalance (The Tape Reader)
        Ingests real-time market execution prints to calculate actual aggression.
        """
        if not trades:
            return

        buy_vol = 0.0
        sell_vol = 0.0

        for trade in trades:
            # Bybit trade sides: "Buy" = Aggressive buyer crossed the spread
            # "Sell" = Aggressive seller crossed the spread
            side = trade.get("S") 
            qty = float(trade.get("v", 0.0))
            
            if side == "Buy":
                buy_vol += qty
            elif side == "Sell":
                sell_vol += qty

        # Calculate Trade Flow Imbalance (-1.0 to 1.0)
        tfi = (buy_vol - sell_vol) / (buy_vol + sell_vol) if (buy_vol + sell_vol) > 0 else 0.0
        self.tfi_history.append(tfi)

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

            # Update structural historical data vaults
            self.obi_history.append(obi)
            self.spread_history.append(raw_spread)  # Keep raw spread here to preserve raw volatility flags

            # 🚀 STRUCTURAL UPGRADE: Robust Z-Score Generation using MAD
            obi_z_score = 0.0
            if len(self.obi_history) >= 100:
                obi_array = np.array(self.obi_history)
                median = np.median(obi_array)
                mad = np.median(np.abs(obi_array - median))
                # Prevent zero-division wrap errors in static markets
                obi_z_score = (obi - median) / (mad * 1.4826 + 1e-6)

            # 4. Machine Learning Feature Matrix Payload Extraction
            features = {
                "valid": True,
                "timestamp": time.time(),
                "mid_price": mid_price,
                "raw_spread": round(raw_spread, 6),
                "bid_ask_spread": round(self.ema_spread, 6),
                "raw_obi": round(obi, 4),
                "adaptive_obi_z": round(obi_z_score, 4),
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

    def get_computed_atr(self, period: int = 14) -> float:
        """
        🚀 STRUCTURAL UPGRADE: Calculates dynamic True Range using Wilder's Smoothing.
        Filters out noise by relying on a statistically significant 14-period lookback.
        """
        # Tiered Data Degradation for Volatility
        if len(self.timeframes["5m"]) >= period + 1:
            candles = list(self.timeframes["5m"])
        elif len(self.timeframes["1m"]) >= period + 1:
            candles = list(self.timeframes["1m"])
        else:
            return 0.0  # Failsafe: Triggers default ATR fallback in main.py

        tr_values = []
        for i in range(1, len(candles)):
            high = float(candles[i].get("high", 0))
            low = float(candles[i].get("low", 0))
            prev_close = float(candles[i-1].get("close", 0))
            
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_values.append(tr)
            
        if not tr_values:
            return 0.0
            
        # Use first TR as initial ATR
        atr = tr_values[0]
        
        # Wilder's Smoothing (Exponential)
        for i in range(1, min(period, len(tr_values))):
            atr = (atr * (period - 1) + tr_values[i]) / period
            
        return float(atr)

    def get_orderbook_snapshot(self) -> Dict[str, List]:
        """Returns the current order book snapshot for Iceberg execution."""
        return getattr(self, '_orderbook_snapshot', {"bids": [], "asks": []})