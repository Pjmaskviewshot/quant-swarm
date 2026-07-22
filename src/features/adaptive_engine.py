import numpy as np
import time
import heapq
import logging
from collections import deque
from typing import Dict, Any, Tuple, List

logger = logging.getLogger("QUANT_CORE.ADAPTIVE_ENGINE")

class AdaptiveFeatureEngine:
    """
    🔬 V27.0 SIGNAL APEX: HIGH-SPEED MICROSTRUCTURE CACHE
    Upgraded to reconstruct and cache the Deep Book (Top 10 Levels) for MLOFI.
    Features O(N log K) Heap Extraction and Epsilon Zero-Division Guards 
    to guarantee mathematical stability during liquidity vacuums.
    """
    def __init__(self, memory_window_short: int = 500, memory_window_long: int = 1800):
        # Local Orderbook Reconstruction Cache
        self.local_bids: Dict[float, float] = {}
        self.local_asks: Dict[float, float] = {}

        # 🚀 O(1) SNAPSHOT OPTIMIZATION CACHE (String format for API/SOR compatibility)
        self._cached_snapshot: Dict[str, List[List[str]]] = {"bids": [], "asks": []}
        
        # 🚀 V27.0 EXPORT CACHE: Pre-cast floats for Zero-Latency MLOFI Math
        self._cached_floats: Dict[str, List[List[float]]] = {"bids": [], "asks": []}

        # Rolling memory for Aggressive Trade Flow Imbalance (Tape Reader Pipeline)
        self.tfi_history = deque(maxlen=memory_window_short)
        
        # Multi-Timeframe micro-aggregates
        self.timeframes = {"1": deque(maxlen=100), "5": deque(maxlen=300), "15": deque(maxlen=900)}
        self.long_window = memory_window_long
        
        self._latest_mid = 0.0

    def _prune_book(self):
        """
        Memory Leak Prevention: Truncates deep out-of-the-money liquidity levels.
        ⚡ V26/V27 UPGRADE: Replaced O(N log N) sorting with O(N log K) Heap Queues.
        """
        if len(self.local_bids) > 1000:
            top_bids = heapq.nlargest(500, self.local_bids.items(), key=lambda x: x[0])
            self.local_bids = dict(top_bids)
            
        if len(self.local_asks) > 1000:
            top_asks = heapq.nsmallest(500, self.local_asks.items(), key=lambda x: x[0])
            self.local_asks = dict(top_asks)

    def detect_market_regime(self) -> str:
        """
        Kaufman's Efficiency Ratio (ER) Market Regime Classifier.
        ⚡ V26 UPGRADE: Mathematical Epsilon Guards against Zero-Division.
        """
        if len(self.timeframes["5"]) >= 45:
            candles = list(self.timeframes["5"])
        elif len(self.timeframes["1"]) >= 20:
            candles = list(self.timeframes["1"])
        else:
            return "RANGING"

        lookback = min(len(candles), 45)
        recent_candles = candles[-lookback:]
        closes = np.array([float(c["close"]) for c in recent_candles])

        directional_change = abs(closes[-1] - closes[0])
        absolute_changes = np.sum(np.abs(np.diff(closes)))
        
        # Epsilon guard added
        efficiency_ratio = directional_change / (absolute_changes + 1e-9)

        sma = np.mean(closes)
        std_dev = np.std(closes)
        
        # Epsilon guard added
        bb_width = (4 * std_dev) / (sma + 1e-9)

        if efficiency_ratio < 0.35 or bb_width < 0.004:
            return "RANGING"
        else:
            return "TRENDING"

    def push_trade_tick(self, trades: List[Dict[str, Any]]):
        """Ingests real-time market execution prints to calculate actual trade aggression."""
        if not trades:
            return

        buy_vol = 0.0
        sell_vol = 0.0

        for trade in trades:
            side = trade.get("side", trade.get("S")) 
            qty = float(trade.get("size", trade.get("v", 0.0)))
            
            if side == "Buy":
                buy_vol += qty
            elif side == "Sell":
                sell_vol += qty

        # Calculate Trade Flow Imbalance (-1.0 to 1.0) guarded by epsilon
        tfi = (buy_vol - sell_vol) / ((buy_vol + sell_vol) + 1e-9)
        self.tfi_history.append(tfi)

    def push_orderbook_tick(self, bids: List[List[str]], asks: List[List[str]], is_snapshot: bool = False) -> None:
        """Consumes raw Level 2 updates and immediately builds an optimized top-of-book lookup."""
        if is_snapshot:
            self.local_bids.clear()
            self.local_asks.clear()

        try:
            if not self.local_bids and not self.local_asks and bids and asks:
                for price_str, size_str in bids:
                    self.local_bids[float(price_str)] = float(size_str)
                for price_str, size_str in asks:
                    self.local_asks[float(price_str)] = float(size_str)
            else:
                for price_str, size_str in bids:
                    price, size = float(price_str), float(size_str)
                    if size == 0.0:
                        self.local_bids.pop(price, None)
                    else:
                        self.local_bids[price] = size

                for price_str, size_str in asks:
                    price, size = float(price_str), float(size_str)
                    if size == 0.0:
                        self.local_asks.pop(price, None)
                    else:
                        self.local_asks[price] = size

            self._prune_book()

            # 🚀 V27.0 UPGRADE: Extract TOP 10 Levels for Deep-Book MLOFI calculations
            if self.local_bids and self.local_asks:
                best_bids = heapq.nlargest(10, self.local_bids.items(), key=lambda x: x[0])
                best_asks = heapq.nsmallest(10, self.local_asks.items(), key=lambda x: x[0])

                if best_bids and best_asks:
                    best_bid_price = best_bids[0][0]
                    best_ask_price = best_asks[0][0]
                    
                    if best_bid_price < best_ask_price:
                        self._latest_mid = (best_bid_price + best_ask_price) / 2.0
                    
                    # Update high-speed execution cache array instantly
                    self._cached_snapshot = {
                        "bids": [[str(p), str(s)] for p, s in best_bids],
                        "asks": [[str(p), str(s)] for p, s in best_asks]
                    }
                    
                    # ⚡ Pre-cast floats for zero-latency MLOFI array math
                    self._cached_floats = {
                        "bids": [[float(p), float(s)] for p, s in best_bids],
                        "asks": [[float(p), float(s)] for p, s in best_asks]
                    }

        except Exception as e:
            logger.error(f"Microstructure local cache reconstruction failure: {e}")

    def update_multi_timeframe_candle(self, timeframe: str, open_p: float, high_p: float, low_p: float, close_p: float, volume: float):
        tf_key = str(timeframe).rstrip("m")
        if tf_key in self.timeframes:
            self.timeframes[tf_key].append({
                "open": open_p, "high": high_p, "low": low_p, "close": close_p, "volume": volume
            })

    def extract_multi_timeframe_momentum(self) -> Dict[str, float]:
        momentum_matrix = {}
        for tf, candles in self.timeframes.items():
            if len(candles) < 2:
                momentum_matrix[f"momentum_{tf}"] = 0.0
                continue
            
            current_close = candles[-1]["close"]
            historical_close = candles[0]["close"]
            # ⚡ V26 UPGRADE: Epsilon guard
            momentum_matrix[f"momentum_{tf}"] = (current_close - historical_close) / max(historical_close, 1e-9)
            
        return momentum_matrix

    # =================================================================
    # 🛡️ INTERFACE CHANNELS FOR EXPOSED ENGINE CALLS
    # =================================================================

    def get_latest_mid(self) -> float:
        return getattr(self, '_latest_mid', 0.0)

    def get_latest_tfi(self) -> float:
        """Exposes Tape Reader pipeline to the central orchestrator path."""
        return self.tfi_history[-1] if self.tfi_history else 0.0

    def get_orderbook_snapshot(self) -> Dict[str, List[List[str]]]:
        """Instantaneous O(1) layout return for the SOR engine (String Format)."""
        return self._cached_snapshot
        
    def get_deep_book_floats(self) -> Tuple[List[List[float]], List[List[float]]]:
        """
        🚀 V27.0 MLOFI EXPORT
        Returns the reconstructed L2 deep book as raw floats, bypassing string-parsing 
        overhead for the Microstructure Edge Gate.
        """
        return self._cached_floats["bids"], self._cached_floats["asks"]

    def get_computed_atr(self, period: int = 14) -> float:
        """Wilder's Smoothed True Range calculation for volatility-adjusted stop realignments."""
        if len(self.timeframes["5"]) >= period + 1:
            candles = list(self.timeframes["5"])
        elif len(self.timeframes["1"]) >= period + 1:
            candles = list(self.timeframes["1"])
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
            
        atr = tr_values[0]
        for i in range(1, min(period, len(tr_values))):
            atr = (atr * (period - 1) + tr_values[i]) / period
            
        return float(atr)

    def get_book_depth_metrics(self) -> Dict[str, float]:
        """Helper diagnostics for shallow liquidity scanning."""
        snapshot = self._cached_floats
        if not snapshot["bids"] or not snapshot["asks"]:
            return {}
            
        # 🚀 V27.0 UPGRADE: Depth metrics now cover the Top 10 levels for better resistance modeling
        bid_depth = sum(level[1] for level in snapshot["bids"])
        ask_depth = sum(level[1] for level in snapshot["asks"])
        total_depth = bid_depth + ask_depth
        
        return {
            "bid_depth_10": float(bid_depth),
            "ask_depth_10": float(ask_depth),
            "total_depth_10": float(total_depth),
            "depth_imbalance": float((bid_depth - ask_depth) / (total_depth + 1e-9)),
            "top_bid": float(snapshot["bids"][0][0]),
            "top_ask": float(snapshot["asks"][0][0])
        }