import numpy as np
import time
import logging
from collections import deque
from typing import Dict, Any, Tuple, List

logger = logging.getLogger("QUANT_CORE.ADAPTIVE_ENGINE")

class AdaptiveFeatureEngine:
    def __init__(self, memory_window_short: int = 500, memory_window_long: int = 1800):
        # 🚀 STRUCTURAL UPGRADE: Local Orderbook Reconstruction Cache
        self.local_bids: Dict[float, float] = {}
        self.local_asks: Dict[float, float] = {}

        # Rolling high-frequency buffers (Tick/Update-based memory)
        self.obi_history = deque(maxlen=memory_window_short)
        self.spread_history = deque(maxlen=memory_window_short)
        
        # 🚀 TFI UPGRADE: Rolling memory for Aggressive Trade Flow Imbalance
        self.tfi_history = deque(maxlen=memory_window_short)
        
        # Multi-Timeframe micro-aggregates (keys match Bybit topic intervals: "1", "5", "15")
        # 🛑 FIX: previously keyed "1m"/"5m"/"15m" while main.py passes "1"/"5"/"15",
        # which silently starved regime detection and ATR of all candle data.
        self.timeframes = {"1": deque(maxlen=100), "5": deque(maxlen=300), "15": deque(maxlen=900)}
        self.long_window = memory_window_long
        
        # 🛡️ State trackers for FSM and execution pipeline
        self._latest_mid = 0.0
        
        # 🚀 INSTITUTIONAL UPGRADE: Low-pass Filter for Order Book Jitter
        self.ema_spread = 0.0
        self.spread_alpha = 0.15  # Slower adjustment locks out microsecond flash spikes

    def _prune_book(self):
        """
        🛑 CRITICAL FIX: Memory Leak Prevention
        Removes deep out-of-the-money levels to prevent infinite RAM bloat over long container uptimes.
        """
        if len(self.local_bids) > 1000:
            # Keep only the top 500 bids closest to the spread
            sorted_bids = sorted(self.local_bids.items(), key=lambda x: x[0], reverse=True)
            self.local_bids = dict(sorted_bids[:500])
            
        if len(self.local_asks) > 1000:
            # Keep only the top 500 asks closest to the spread
            sorted_asks = sorted(self.local_asks.items(), key=lambda x: x[0])
            self.local_asks = dict(sorted_asks[:500])

    def detect_market_regime(self) -> str:
        """
        🚀 DYNAMIC STATISTICAL REGIME CLASSIFIER (COLD-BOOT RESILIENT)
        Uses Kaufman's Efficiency Ratio (ER) and Volatility Squeeze metrics.
        Dynamically scales the lookback window so the bot doesn't fly blind upon boot.
        """
        # 1. Tiered Data Degradation: Use 5m if we have decent data, fallback to 1m, or hard fail.
        # 🛑 CRITICAL KEY FIX: Ensure we use the exact string keys ("5" and "1") initialized above.
        if len(self.timeframes["5"]) >= 45:
            candles = list(self.timeframes["5"])
        elif len(self.timeframes["1"]) >= 20:
            candles = list(self.timeframes["1"])
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

    def push_orderbook_tick(self, bids: List[List[str]], asks: List[List[str]], is_snapshot: bool = False) -> Dict[str, Any]:
        """
        Consumes raw level 2 structural updates.
        Computes rolling statistical parameters and extracts ML-ready feature arrays.
        🚀 UPGRADE: Intelligently rebuilds the orderbook state from deltas.
        """
        if is_snapshot:
            self.local_bids.clear()
            self.local_asks.clear()

        try:
            # 🛑 CRITICAL FIX: Initialization Race Condition Failsafe
            if not self.local_bids and not self.local_asks and bids and asks:
                for price_str, size_str in bids:
                    self.local_bids[float(price_str)] = float(size_str)
                for price_str, size_str in asks:
                    self.local_asks[float(price_str)] = float(size_str)
            else:
                # 1. Reconstruct Local Dictionary State from Deltas
                for price_str, size_str in bids:
                    price, size = float(price_str), float(size_str)
                    if size == 0.0:
                        self.local_bids.pop(price, None) # Delta instruction to delete level
                    else:
                        self.local_bids[price] = size

                for price_str, size_str in asks:
                    price, size = float(price_str), float(size_str)
                    if size == 0.0:
                        self.local_asks.pop(price, None) # Delta instruction to delete level
                    else:
                        self.local_asks[price] = size

            # Memory Manager: Prevent RAM bloat
            self._prune_book()

            # 2. Sort to find true Top of Book
            sorted_bids = sorted(self.local_bids.items(), key=lambda x: x[0], reverse=True)
            sorted_asks = sorted(self.local_asks.items(), key=lambda x: x[0])

            if not sorted_bids or not sorted_asks:
                return {"valid": False}

            # 3. Extract Microstructure Absolute Bounds
            best_bid = sorted_bids[0][0]
            best_ask = sorted_asks[0][0]
            
            # Failsafe: Crossed book glitch from websocket delay
            if best_bid >= best_ask:
                 return {"valid": False}
                 
            mid_price = (best_bid + best_ask) / 2.0
            raw_spread = best_ask - best_bid
            
            # 🚀 INSTITUTIONAL UPGRADE: Smooth Raw Spread via EMA to Filter Websocket Jitter
            if self.ema_spread == 0.0:
                self.ema_spread = raw_spread
            else:
                self.ema_spread = (raw_spread * self.spread_alpha) + (self.ema_spread * (1.0 - self.spread_alpha))
            
            # 🛡️ Store latest tick data for the execution engine
            self._latest_mid = mid_price

            # 4. Compute True Volume-Weighted Order Book Imbalance (OBI)
            v_b = sum(s for p, s in sorted_bids[:5])
            v_a = sum(s for p, s in sorted_asks[:5])
            
            obi = (v_b - v_a) / (v_b + v_a) if (v_b + v_a) > 0 else 0.0

            # Update structural historical data vaults
            self.obi_history.append(obi)
            self.spread_history.append(raw_spread)  

            # 🚀 STRUCTURAL UPGRADE: Robust Z-Score Generation using MAD
            obi_z_score = 0.0
            if len(self.obi_history) >= 100:
                obi_array = np.array(self.obi_history)
                median = np.median(obi_array)
                mad = np.median(np.abs(obi_array - median))
                obi_z_score = (obi - median) / (mad * 1.4826 + 1e-6)

            # 5. Machine Learning Feature Matrix Payload Extraction
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
        tf_key = str(timeframe).rstrip("m")
        if tf_key in self.timeframes:
            self.timeframes[tf_key].append({
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
        # 🛑 CRITICAL KEY FIX: Ensure we use the exact string keys ("5" and "1") initialized above.
        if len(self.timeframes["5"]) >= period + 1:
            candles = list(self.timeframes["5"])
        elif len(self.timeframes["1"]) >= period + 1:
            candles = list(self.timeframes["1"])
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
        """
        🛑 CRITICAL FIX: Live Dynamic Execution Slicing
        Extracts the precise top 5 tiers directly from the active sorted cache for the Smart Order Router.
        """
        if hasattr(self, 'local_bids') and self.local_bids and self.local_asks:
            sorted_bids = sorted(self.local_bids.items(), key=lambda x: x[0], reverse=True)
            sorted_asks = sorted(self.local_asks.items(), key=lambda x: x[0])
            
            return {
                "bids": [[str(p), str(s)] for p, s in sorted_bids[:5]],
                "asks": [[str(p), str(s)] for p, s in sorted_asks[:5]]
            }
            
        return {"bids": [], "asks": []}

    def get_book_depth_metrics(self) -> Dict[str, float]:
        """
        📊 BOOK DEPTH STATISTICS
        Returns liquidity metrics for risk assessment to prevent "Hollow Book" slippage on execution.
        """
        if not self.local_bids or not self.local_asks:
            return {}
            
        sorted_bids = sorted(self.local_bids.items(), key=lambda x: x[0], reverse=True)
        sorted_asks = sorted(self.local_asks.items(), key=lambda x: x[0])
        
        # Calculate volume depth across top 10 tiers
        bid_depth = sum(s for _, s in sorted_bids[:10])
        ask_depth = sum(s for _, s in sorted_asks[:10])
        total_depth = bid_depth + ask_depth
        
        return {
            "bid_depth_10": float(bid_depth),
            "ask_depth_10": float(ask_depth),
            "total_depth_10": float(total_depth),
            "depth_imbalance": float((bid_depth - ask_depth) / (total_depth + 1e-6)),
            "top_bid": float(sorted_bids[0][0]) if sorted_bids else 0.0,
            "top_ask": float(sorted_asks[0][0]) if sorted_asks else 0.0
        }