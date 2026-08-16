"""
💎 V6.0 ULTRA-APEX NEURAL: ADAPTIVE FEATURE ENGINE
--------------------------------------------------------------
Dynamically Calibrated Hidden Markov Model (HMM) for Regime Detection.
Upgraded with 1D Kalman Filtering on the price stream to eradicate 
micro-whipsaws and stabilize order routing decisions. 

CRITICAL FIX: Fully initialized price buffers, self.prices deque, 
and safe fallback in get_book_depth_metrics to prevent AttributeError crashes.
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
            "60": deque(maxlen=200),  # 1 Hour
            "240": deque(maxlen=100)  # 4 Hour
        }
        self.long_window = memory_window_long
        self._latest_mid = 0.0

        # ====================================================================
        # 🚀 HMM STATE PRIORS & TRANSITIONS
        # ====================================================================
        self.regimes = [
            "TRENDING_BULL", 
            "TRENDING_BEAR", 
            "HIGH_VOL_CHOP", 
            "MEAN_REVERTING", 
            "LIQUIDITY_VACUUM"
        ]
        
        self.state_probs = np.array([0.2, 0.2, 0.2, 0.2, 0.2], dtype=np.float64)
        self.transition_matrix = np.array([
            [0.75, 0.05, 0.10, 0.08, 0.02], # From BULL
            [0.05, 0.75, 0.10, 0.08, 0.02], # From BEAR
            [0.10, 0.10, 0.70, 0.05, 0.05], # From CHOP
            [0.05, 0.05, 0.05, 0.80, 0.05], # From MEAN_REVERTING
            [0.05, 0.05, 0.15, 0.05, 0.70]  # From VACUUM
        ], dtype=np.float64)
        
        self.last_detected_regime = "UNKNOWN"
        self._last_log_time = 0.0

    def _prune_book(self):
        """Memory Leak Prevention: Truncates deep out-of-the-money liquidity levels."""
        if len(self.local_bids) > 1000:
            top_bids = heapq.nlargest(500, self.local_bids.items(), key=lambda x: x[0])
            self.local_bids = dict(top_bids)
            
        if len(self.local_asks) > 1000:
            top_asks = heapq.nsmallest(500, self.local_asks.items(), key=lambda x: x[0])
            self.local_asks = dict(top_asks)

    def _log_gaussian_pdf(self, x: float, mean: float, std: float) -> float:
        """Computes ln(P) to eliminate float underflow when multiplying probabilities."""
        var = float(std)**2 + 1e-9
        return -0.5 * math.log(2 * math.pi * var) - ((float(x) - float(mean))**2 / (2 * var))

    def _apply_kalman_smoothing(self, prices: np.ndarray) -> np.ndarray:
        """
        🚀 1D Kalman Filter
        Strips high-frequency microstructure noise from the raw price feed.
        """
        if len(prices) < 2:
            return prices
            
        n = len(prices)
        filtered = np.zeros(n)
        
        Q = 1e-5
        R = np.var(prices) * 0.05 + 1e-9 
        
        x_hat = prices[0]
        P = 1.0
        
        for i in range(n):
            x_pred = x_hat
            P_pred = P + Q
            
            K = P_pred / (P_pred + R)
            x_hat = x_pred + K * (prices[i] - x_pred)
            P = (1 - K) * P_pred
            
            filtered[i] = x_hat
            
        return filtered

    def detect_market_regime(self) -> str:
        """Dynamically Calibrated HMM without look-ahead bias."""
        if len(self.timeframes["5"]) >= 100:
            candles = list(self.timeframes["5"])[-100:]
        elif len(self.timeframes["1"]) >= 100:
            candles = list(self.timeframes["1"])[-100:]
        elif len(self.timeframes["5"]) >= 20: 
            candles = list(self.timeframes["5"])[-20:]
        elif len(self.prices) >= 20:
            raw_p = np.array(list(self.prices)[-60:])
            return "TRENDING" if abs(raw_p[-1] - raw_p[0]) > np.std(raw_p) * 2 else "RANGING"
        else:
            return "MEAN_REVERTING" 

        raw_closes = np.array([float(c["close"]) for c in candles])
        volumes = np.array([float(c["volume"]) for c in candles])
        
        closes = self._apply_kalman_smoothing(raw_closes)
        
        try:
            log_returns = np.diff(np.log(closes + 1e-9))
            mu_ret = float(np.mean(log_returns))
            volatility = float(np.std(log_returns)) + 1e-9
            
            directional_change = abs(closes[-1] - closes[0])
            absolute_changes = np.sum(np.abs(np.diff(closes)))
            er = float(directional_change / (absolute_changes + 1e-9))
            
            avg_vol = float(np.mean(volumes))
            vol_baseline = float(np.median(volumes)) + 1e-9
            
            base_vol = max(0.001, np.median([np.std(log_returns[max(0, i-5):i]) for i in range(5, len(log_returns)+1, 5)]))
            
            archetypes = {
                "TRENDING_BULL":    {"ret": (base_vol * 1.5, base_vol), "vol": (base_vol * 1.2, base_vol), "er": (0.7, 0.2)},
                "TRENDING_BEAR":    {"ret": (-base_vol * 1.5, base_vol), "vol": (base_vol * 1.2, base_vol), "er": (0.7, 0.2)},
                "HIGH_VOL_CHOP":    {"ret": (0.0, base_vol * 2.5), "vol": (base_vol * 3.0, base_vol * 1.5), "er": (0.2, 0.15)},
                "MEAN_REVERTING":   {"ret": (0.0, base_vol * 0.8), "vol": (base_vol * 1.0, base_vol * 0.5), "er": (0.3, 0.15)},
                "LIQUIDITY_VACUUM": {"ret": (0.0, base_vol * 0.5), "vol": (base_vol * 0.5, base_vol * 0.3), "er": (0.5, 0.2)}
            }
            
            log_emissions = np.zeros(5)
            for i, regime in enumerate(self.regimes):
                arch = archetypes[regime]
                log_p_ret = self._log_gaussian_pdf(mu_ret, arch["ret"][0], arch["ret"][1])
                log_p_vol = self._log_gaussian_pdf(volatility, arch["vol"][0], arch["vol"][1])
                log_p_er  = self._log_gaussian_pdf(er, arch["er"][0], arch["er"][1])
                
                log_emission = log_p_ret + log_p_vol + log_p_er
                if regime == "LIQUIDITY_VACUUM" and avg_vol < vol_baseline * 0.5:
                    log_emission += math.log(2.0) 
                    
                log_emissions[i] = log_emission
                
            prior = np.dot(self.state_probs, self.transition_matrix)
            prior_log = np.log(prior + 1e-9)
            
            unnormalized_log_posterior = log_emissions + prior_log
            max_log = np.max(unnormalized_log_posterior)
            posterior = np.exp(unnormalized_log_posterior - max_log)
            self.state_probs = posterior / np.sum(posterior)
            
            best_state_idx = int(np.argmax(self.state_probs))
            detected_regime = self.regimes[best_state_idx]
            
            now = time.time()
            if detected_regime != self.last_detected_regime and (now - self._last_log_time > 300):
                logger.info(f"[X-RAY] 🌌 HMM REGIME SHIFT // State transitioned to: {detected_regime}")
                self.last_detected_regime = detected_regime
                self._last_log_time = now
            
            if detected_regime in ["TRENDING_BULL", "TRENDING_BEAR"]:
                return "TRENDING"
            else:
                return "RANGING"

        except Exception as e:
            logger.debug(f"[X-RAY] HMM Regime detection fallback: {e}")
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
                best_bids = heapq.nlargest(10, self.local_bids.items(), key=lambda x: x[0])
                best_asks = heapq.nsmallest(10, self.local_asks.items(), key=lambda x: x[0])

                if best_bids and best_asks:
                    best_bid_price = best_bids[0][0]
                    best_ask_price = best_asks[0][0]
                    
                    if best_bid_price < best_ask_price:
                        self._latest_mid = (best_bid_price + best_ask_price) / 2.0
                        self.prices.append(self._latest_mid)
                    
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

    def get_book_depth_metrics(self) -> Dict[str, float]:
        snapshot = self._cached_floats
        fallback_mid = self._latest_mid if self._latest_mid > 0 else (self.prices[-1] if self.prices else 1.0)
        
        if not snapshot["bids"] or not snapshot["asks"]:
            return {
                "top_bid": fallback_mid * 0.9999, 
                "top_ask": fallback_mid * 1.0001, 
                "bid_depth_10": 10.0, 
                "ask_depth_10": 10.0,
                "total_depth_10": 20.0,
                "depth_imbalance": 0.0
            }

        try:
            top_bid = float(snapshot["bids"][0][0])
            top_ask = float(snapshot["asks"][0][0])
            bid_d10 = sum(float(level[1]) for level in snapshot["bids"])
            ask_d10 = sum(float(level[1]) for level in snapshot["asks"])
            total_depth = bid_d10 + ask_d10
            
            return {
                "top_bid": top_bid,
                "top_ask": top_ask,
                "bid_depth_10": float(bid_d10),
                "ask_depth_10": float(ask_d10),
                "total_depth_10": float(total_depth),
                "depth_imbalance": float((bid_d10 - ask_d10) / (total_depth + 1e-9))
            }
        except (IndexError, ValueError, TypeError):
            return {
                "top_bid": fallback_mid * 0.9999, 
                "top_ask": fallback_mid * 1.0001, 
                "bid_depth_10": 10.0, 
                "ask_depth_10": 10.0,
                "total_depth_10": 20.0,
                "depth_imbalance": 0.0
            }