import numpy as np
import time
import logging
from collections import deque
from typing import Dict, Any, Tuple, List

logger = logging.getLogger("QUANT_CORE.ADAPTIVE_ENGINE")

class AdaptiveFeatureEngine:
    """
    🔬 V20.2 APEX ENGINE: HIGH-SPEED MICROSTRUCTURE CACHE
    Redundant OBI/MAD math completely purged. 
    Features institutional-grade O(1) top-of-book caching for the Smart Order Router.
    """
    def __init__(self, memory_window_short: int = 500, memory_window_long: int = 1800):
        # Local Orderbook Reconstruction Cache
        self.local_bids: Dict[float, float] = {}
        self.local_asks: Dict[float, float] = {}

        # 🚀 O(1) SNAPSHOT OPTIMIZATION CACHE
        self._cached_snapshot: Dict[str, List[List[str]]] = {"bids": [], "asks": []}

        # Rolling memory for Aggressive Trade Flow Imbalance (Tape Reader Pipeline)
        self.tfi_history = deque(maxlen=memory_window_short)
        
        # Multi-Timeframe micro-aggregates
        self.timeframes = {"1": deque(maxlen=100), "5": deque(maxlen=300), "15": deque(maxlen=900)}
        self.long_window = memory_window_long
        
        self._latest_mid = 0.0

    def _prune_book(self):
        """Memory Leak Prevention: Truncates deep out-of-the-money liquidity levels."""
        if len(self.local_bids) > 1000:
            sorted_bids = sorted(self.local_bids.items(), key=lambda x: x[0], reverse=True)
            self.local_bids = dict(sorted_bids[:500])
            
        if len(self.local_asks) > 1000:
            sorted_asks = sorted(self.local_asks.items(), key=lambda x: x[0])
            self.local_asks = dict(sorted_asks[:500])

    def detect_market_regime(self) -> str:
        """Kaufman's Efficiency Ratio (ER) Market Regime Classifier."""
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
        efficiency_ratio = directional_change / absolute_changes if absolute_changes > 0 else 0.0

        sma = np.mean(closes)
        std_dev = np.std(closes)
        bb_width = (4 * std_dev) / sma if sma > 0 else 0.0

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

        # Calculate Trade Flow Imbalance (-1.0 to 1.0)
        tfi = (buy_vol - sell_vol) / (buy_vol + sell_vol) if (buy_vol + sell_vol) > 0 else 0.0
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

            # 🚀 SORT ONCE ON HOT PATH AND CACHE IMMEDIATELY
            sorted_bids = sorted(self.local_bids.items(), key=lambda x: x[0], reverse=True)
            sorted_asks = sorted(self.local_asks.items(), key=lambda x: x[0])

            if sorted_bids and sorted_asks:
                best_bid = sorted_bids[0][0]
                best_ask = sorted_asks[0][0]
                
                if best_bid < best_ask:
                    self._latest_mid = (best_bid + best_ask) / 2.0
                
                # Update high-speed execution cache array
                self._cached_snapshot = {
                    "bids": [[str(p), str(s)] for p, s in sorted_bids[:5]],
                    "asks": [[str(p), str(s)] for p, s in sorted_asks[:5]]
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
            momentum_matrix[f"momentum_{tf}"] = (current_close - historical_close) / historical_close
            
        return momentum_matrix

    # =================================================================
    # 🛡️ INTERFACE CHANNELS FOR EXPOSED ENGINE CALLS
    # =================================================================

    def get_latest_mid(self) -> float:
        return getattr(self, '_latest_mid', 0.0)

    def get_latest_tfi(self) -> float:
        """🚀 V20.2 CHANNELS: Exposes Tape Reader pipeline to the central orchestrator path."""
        return self.tfi_history[-1] if self.tfi_history else 0.0

    def get_orderbook_snapshot(self) -> Dict[str, List[List[str]]]:
        """🚀 ULTRA-LOW LATENCY FIX: Instantaneous O(1) layout return for the SOR engine."""
        return self._cached_snapshot

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
        snapshot = self._cached_snapshot
        if not snapshot["bids"] or not snapshot["asks"]:
            return {}
            
        bid_depth = sum(float(level[1]) for level in snapshot["bids"])
        ask_depth = sum(float(level[1]) for level in snapshot["asks"])
        total_depth = bid_depth + ask_depth
        
        return {
            "bid_depth_5": float(bid_depth),
            "ask_depth_5": float(ask_depth),
            "total_depth_5": float(total_depth),
            "depth_imbalance": float((bid_depth - ask_depth) / (total_depth + 1e-6)),
            "top_bid": float(snapshot["bids"][0][0]),
            "top_ask": float(snapshot["asks"][0][0])
        }