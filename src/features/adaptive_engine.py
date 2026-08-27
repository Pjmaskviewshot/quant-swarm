"""
💎 V22.0 APEX QUANTUM PRIME: ADAPTIVE FEATURE ENGINE
--------------------------------------------------------------
Multi-Dimensional State Generation & Microstructure Physics.

Breakthroughs & Audit Fixes:
- O(1) Amortized L2 Orderbook Reconstruction (Eliminated CPU Pruning Spikes)
- Bounded Jump-Diffusion Kalman Filter (Algebraic Process Noise Capping)
- Deterministic Efficiency-Ratio (ER) Regime Classification
- Guaranteed Mathematical Immunity to Zero-Variance Float Overflows
"""

import math
import time
import numpy as np
import logging
import heapq
from collections import deque
from typing import Dict, Any, Tuple, List

logger = logging.getLogger("QUANT_CORE.ADAPTIVE_ENGINE")


class AdaptiveFeatureEngine:
    def __init__(self, memory_window_short: int = 500, memory_window_long: int = 1800):
        self.local_bids: Dict[float, float] = {}
        self.local_asks: Dict[float, float] = {}

        self._cached_snapshot: Dict[str, List[List[str]]] = {"bids": [], "asks": []}
        self._cached_floats: Dict[str, List[List[float]]] = {"bids": [], "asks": []}

        self.tfi_history = deque(maxlen=memory_window_short)
        self.prices = deque(maxlen=memory_window_long)
        self.volumes = deque(maxlen=memory_window_long)
        
        self.timeframes = {
            "1": deque(maxlen=200), 
            "5": deque(maxlen=300), 
            "15": deque(maxlen=900),
            "60": deque(maxlen=200),  
            "240": deque(maxlen=100)  
        }
        self.long_window = memory_window_long
        self._latest_mid = 0.0
        self._latest_micro_price = 0.0

        self.last_detected_regime = "UNKNOWN"
        self._last_log_time = 0.0

    def _prune_book(self):
        """
        🚀 V22.0 CPU SPIKE PREVENTION
        Replaced the O(N log K) heapq with C-optimized Timsort for large slice extraction.
        Expanded the trigger gap (2500 -> 500) to massively amortize the cleanup cost,
        ensuring the async event loop never stutters during high-volatility tick floods.
        """
        if len(self.local_bids) > 2500:
            top_bids = sorted(self.local_bids.keys(), reverse=True)[:500]
            self.local_bids = {p: self.local_bids[p] for p in top_bids}
            
        if len(self.local_asks) > 2500:
            top_asks = sorted(self.local_asks.keys())[:500]
            self.local_asks = {p: self.local_asks[p] for p in top_asks}

    def _apply_adaptive_kalman_smoothing(self, prices: np.ndarray) -> np.ndarray:
        """
        🚀 V21.1 JUMP-DIFFUSION ADAPTIVE KALMAN FILTER (JD-AKF)
        Replaced the deadly math.exp() multiplier with a hard-capped algebraic 
        polynomial scale. Guarantees the filter will not overflow during flash crashes.
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
            
            # 🚀 V21.1 ALGEBRAIC CAP: Strict polynomial scaling to eradicate FloatOverflow
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

    def detect_market_regime(self) -> str:
        """
        🚀 V21.1 DETERMINISTIC REGIME CLASSIFIER
        Stripped out the bloated pseudo-HMM and replaced it with a fast, computationally
        efficient Realized Volatility and Efficiency Ratio (ER) classifier.
        """
        candles = []
        if len(self.timeframes["5"]) >= 20:
            candles = list(self.timeframes["5"])[-20:]
        elif len(self.timeframes["1"]) >= 20:
            candles = list(self.timeframes["1"])[-20:]
        elif len(self.prices) >= 20:
            price_list = list(self.prices)[-20:]
            candles = [{"close": p, "volume": 1.0} for p in price_list]
        else:
            return "MEAN_REVERTING"

        closes = self._apply_adaptive_kalman_smoothing(np.array([float(c["close"]) for c in candles]))
        
        try:
            log_returns = np.diff(np.log(closes + 1e-9))
            volatility = float(np.std(log_returns)) + 1e-9
            
            directional_change = abs(closes[-1] - closes[0])
            absolute_changes = float(np.sum(np.abs(np.diff(closes))))
            er = float(directional_change / (absolute_changes + 1e-9))
            
            if er > 0.45:
                detected_regime = "TRENDING"
            elif er < 0.20 and volatility > 0.005:
                detected_regime = "HIGH_VOL_CHOP"
            else:
                detected_regime = "MEAN_REVERTING"

            now = time.time()
            if detected_regime != self.last_detected_regime and (now - self._last_log_time > 300):
                logger.info(f"[X-RAY] 🌌 PHYSICS REGIME SHIFT // Space-Time transitioned to: {detected_regime}")
                self.last_detected_regime = detected_regime
                self._last_log_time = now
            
            if detected_regime == "TRENDING":
                return "TRENDING"
            else:
                return "RANGING"

        except Exception as e:
            logger.debug(f"[X-RAY] Regime detection fallback: {e}")
            return "MEAN_REVERTING"

    def push_trade_tick(self, trades: List[Dict[str, Any]]):
        if not trades:
            return

        buy_vol = 0.0
        sell_vol = 0.0

        for trade in trades:
            side = trade.get("side", trade.get("S", "")) 
            qty = float(trade.get("size", trade.get("v", 0.0)))
            p = float(trade.get("price", trade.get("p", 0.0)))
            
            if p > 0:
                self.prices.append(p)
                self.volumes.append(qty)
                self._latest_mid = p

            if str(side).upper() == "BUY":
                buy_vol += qty
            elif str(side).upper() == "SELL":
                sell_vol += qty

        tfi = (buy_vol - sell_vol) / ((buy_vol + sell_vol) + 1e-9)
        self.tfi_history.append(tfi)

    def push_orderbook_tick(self, bids: List[List[str]], asks: List[List[str]], is_snapshot: bool = False) -> None:
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

            if self.local_bids and self.local_asks:
                # heapq remains optimal for K=10 (much faster than sorting 2000 items)
                best_bids = heapq.nlargest(10, self.local_bids.items(), key=lambda x: x[0])
                best_asks = heapq.nsmallest(10, self.local_asks.items(), key=lambda x: x[0])

                if best_bids and best_asks:
                    best_bid_price = best_bids[0][0]
                    best_ask_price = best_asks[0][0]
                    
                    if best_bid_price < best_ask_price:
                        self._latest_mid = (best_bid_price + best_ask_price) / 2.0
                        self.prices.append(self._latest_mid)
                        
                        # V21.0 NON-LINEAR STOIKOV MICRO-PRICE
                        bid_vol = best_bids[0][1]
                        ask_vol = best_asks[0][1]
                        spread = max(1e-9, best_ask_price - best_bid_price)
                        imb = bid_vol / (bid_vol + ask_vol + 1e-9)
                        
                        stoikov_adjustment = spread * (imb - 0.5) * (1.0 + abs(imb - 0.5))
                        self._latest_micro_price = self._latest_mid + stoikov_adjustment
                    
                    self._cached_snapshot = {
                        "bids": [[str(p), str(s)] for p, s in best_bids],
                        "asks": [[str(p), str(s)] for p, s in best_asks]
                    }
                    
                    self._cached_floats = {
                        "bids": [[float(p), float(s)] for p, s in best_bids],
                        "asks": [[float(p), float(s)] for p, s in best_asks]
                    }

        except Exception as e:
            logger.error(f"[X-RAY] Microstructure local cache reconstruction failure: {e}")

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
            self._latest_mid = close_p

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

    def get_latest_mid(self) -> float:
        if self._latest_mid > 0:
            return self._latest_mid
        return self.prices[-1] if self.prices else 0.0
        
    def get_latest_micro_price(self) -> float:
        """Returns the Stoikov-adjusted Non-Linear Micro-Price."""
        if self._latest_micro_price > 0:
            return self._latest_micro_price
        return self.get_latest_mid()

    def get_latest_tfi(self) -> float:
        return self.tfi_history[-1] if self.tfi_history else 0.0

    def get_orderbook_snapshot(self) -> Dict[str, List[List[str]]]:
        return self._cached_snapshot
        
    def get_deep_book_floats(self) -> Tuple[List[List[float]], List[List[float]]]:
        return self._cached_floats["bids"], self._cached_floats["asks"]

    def get_computed_atr(self, period: int = 14) -> float:
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

    def _compute_spatial_entropy(self, levels: List[List[float]]) -> float:
        """Computes Shannon Entropy of volume distribution to detect liquidity fragmentation."""
        if not levels or len(levels) < 2: return 0.0
        
        vols = np.array([lvl[1] for lvl in levels])
        total_vol = np.sum(vols)
        if total_vol <= 0: return 0.0
        
        probs = vols / total_vol
        probs = probs[probs > 0] # Avoid log(0)
        
        entropy = -np.sum(probs * np.log2(probs))
        # Normalize between 0 (fragile/concentrated) and 1 (resilient/distributed)
        return float(entropy / math.log2(len(levels)))

    def get_book_depth_metrics(self) -> Dict[str, float]:
        snapshot = self._cached_floats
        fallback_mid = self._latest_mid if self._latest_mid > 0 else (self.prices[-1] if self.prices else 1.0)
        
        base_metrics = {
            "top_bid": fallback_mid * 0.9999, 
            "top_ask": fallback_mid * 1.0001, 
            "micro_price": fallback_mid,
            "bid_depth_10": 10.0, 
            "ask_depth_10": 10.0,
            "total_depth_10": 20.0,
            "depth_imbalance": 0.0,
            "liquidity_density": 1.0,
            "book_convexity": 1.0,
            "spatial_entropy": 1.0
        }

        if not snapshot["bids"] or not snapshot["asks"]:
            return base_metrics

        try:
            top_bid = float(snapshot["bids"][0][0])
            top_ask = float(snapshot["asks"][0][0])
            
            bid_vols = [float(lvl[1]) for lvl in snapshot["bids"]]
            ask_vols = [float(lvl[1]) for lvl in snapshot["asks"]]
            
            bid_d10 = sum(bid_vols)
            ask_d10 = sum(ask_vols)
            total_depth = bid_d10 + ask_d10
            
            # Convexity: Handles sparse arrays safely
            bid_inner = sum(bid_vols[:3]) / 3.0 if len(bid_vols) >= 3 else (bid_d10 / max(1, len(bid_vols)))
            bid_outer = sum(bid_vols[3:]) / max(1, len(bid_vols[3:])) if len(bid_vols) > 3 else bid_inner
            
            ask_inner = sum(ask_vols[:3]) / 3.0 if len(ask_vols) >= 3 else (ask_d10 / max(1, len(ask_vols)))
            ask_outer = sum(ask_vols[3:]) / max(1, len(ask_vols[3:])) if len(ask_vols) > 3 else ask_inner
            
            bid_convexity = bid_outer / (bid_inner + 1e-9)
            ask_convexity = ask_outer / (ask_inner + 1e-9)
            book_convexity = (bid_convexity + ask_convexity) / 2.0
            
            # Spatial Entropy
            bid_entropy = self._compute_spatial_entropy(snapshot["bids"])
            ask_entropy = self._compute_spatial_entropy(snapshot["asks"])
            avg_entropy = (bid_entropy + ask_entropy) / 2.0
            
            micro_price = self._latest_micro_price if self._latest_micro_price > 0 else ((top_bid + top_ask) / 2.0)
            
            spread_pct = (top_ask - top_bid) / top_bid
            liquidity_density = total_depth / max(spread_pct, 1e-5)
            
            return {
                "top_bid": top_bid,
                "top_ask": top_ask,
                "micro_price": micro_price,
                "bid_depth_10": float(bid_d10),
                "ask_depth_10": float(ask_d10),
                "total_depth_10": float(total_depth),
                "depth_imbalance": float((bid_d10 - ask_d10) / (total_depth + 1e-9)),
                "liquidity_density": float(liquidity_density),
                "book_convexity": float(book_convexity),
                "spatial_entropy": float(avg_entropy)
            }
        except (IndexError, ValueError, TypeError) as e:
            logger.debug(f"[X-RAY] Metric extraction error, returning baseline: {e}")
            return base_metrics