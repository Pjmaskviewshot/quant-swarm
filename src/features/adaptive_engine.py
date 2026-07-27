import math
import time
import numpy as np
import logging
import heapq
from collections import deque
from typing import Dict, Any, Tuple, List

logger = logging.getLogger("QUANT_CORE.ADAPTIVE_ENGINE")

class AdaptiveFeatureEngine:
    """
    🔬 V36.2 APEX: HIDDEN MARKOV MODEL (HMM) REGIME ENGINE
    Upgraded to track 5 distinct non-linear market states using Log-Space Gaussian emission probabilities.
    Prevents float underflow when multiplying tiny pdf values.
    Maintains O(N log K) Heap Extraction for Top-10 Deep Book reconstruction.
    Matrix aligned with backtest for perfect predictive parity.
    """
    def __init__(self, memory_window_short: int = 500, memory_window_long: int = 1800):
        # Local Orderbook Reconstruction Cache
        self.local_bids: Dict[float, float] = {}
        self.local_asks: Dict[float, float] = {}

        # 🚀 O(1) SNAPSHOT OPTIMIZATION CACHE
        self._cached_snapshot: Dict[str, List[List[str]]] = {"bids": [], "asks": []}
        
        # 🚀 O(1) MLOFI FLOAT EXPORT CACHE
        self._cached_floats: Dict[str, List[List[float]]] = {"bids": [], "asks": []}

        # Aggressive Trade Flow Imbalance
        self.tfi_history = deque(maxlen=memory_window_short)
        
        # Multi-Timeframe micro-aggregates
        self.timeframes = {"1": deque(maxlen=100), "5": deque(maxlen=300), "15": deque(maxlen=900)}
        self.long_window = memory_window_long
        
        self._latest_mid = 0.0

        # ====================================================================
        # 🚀 V36.2 APEX: HIDDEN MARKOV MODEL (HMM) STATE PRIORS
        # ====================================================================
        self.regimes = [
            "TRENDING_BULL", 
            "TRENDING_BEAR", 
            "HIGH_VOL_CHOP", 
            "MEAN_REVERTING", 
            "LIQUIDITY_VACUUM"
        ]
        
        # Current belief state probabilities (Uniform initial prior)
        self.state_probs = np.array([0.2, 0.2, 0.2, 0.2, 0.2])
        
        # Transition Matrix P(State_t | State_{t-1})
        # 🚀 V36.2 FIX: Matrix strictly aligned to backtest (0.75 self-persistence)
        self.transition_matrix = np.array([
            [0.75, 0.05, 0.10, 0.08, 0.02], # From BULL
            [0.05, 0.75, 0.10, 0.08, 0.02], # From BEAR
            [0.10, 0.10, 0.70, 0.05, 0.05], # From CHOP
            [0.05, 0.05, 0.05, 0.80, 0.05], # From MEAN_REVERTING
            [0.05, 0.05, 0.15, 0.05, 0.70]  # From VACUUM
        ])

    def _prune_book(self):
        """
        Memory Leak Prevention: Truncates deep out-of-the-money liquidity levels.
        """
        if len(self.local_bids) > 1000:
            top_bids = heapq.nlargest(500, self.local_bids.items(), key=lambda x: x[0])
            self.local_bids = dict(top_bids)
            
        if len(self.local_asks) > 1000:
            top_asks = heapq.nsmallest(500, self.local_asks.items(), key=lambda x: x[0])
            self.local_asks = dict(top_asks)

    def _log_gaussian_pdf(self, x: float, mean: float, std: float) -> float:
        """
        🚀 V36.2 FIX: Log-Space HMM Emission Probability.
        Computes ln(P) to completely eliminate float underflow when multiplying.
        """
        var = float(std)**2 + 1e-9
        return -0.5 * math.log(2 * math.pi * var) - ((float(x) - float(mean))**2 / (2 * var))

    def detect_market_regime(self) -> str:
        """
        🚀 V36.2 APEX: Log-Space HMM Gaussian Classifier.
        Matrix Parity aligned with Backtest. Stable probability extraction.
        """
        if len(self.timeframes["5"]) >= 20:
            candles = list(self.timeframes["5"])[-20:]
        elif len(self.timeframes["1"]) >= 20:
            candles = list(self.timeframes["1"])[-20:]
        else:
            return "MEAN_REVERTING" # Safe fallback

        closes = np.array([float(c["close"]) for c in candles])
        volumes = np.array([float(c["volume"]) for c in candles])
        
        # 1. Calculate Emission Features
        log_returns = np.diff(np.log(closes + 1e-9))
        mu_ret = float(np.mean(log_returns))
        volatility = float(np.std(log_returns)) + 1e-9
        
        directional_change = abs(closes[-1] - closes[0])
        absolute_changes = np.sum(np.abs(np.diff(closes)))
        er = float(directional_change / (absolute_changes + 1e-9))
        
        avg_vol = float(np.mean(volumes))
        
        # 2. Define Regime Expected Archetypes (Mean, StdDev) for Emission P(Obs | State)
        archetypes = {
            "TRENDING_BULL":    {"ret": (0.001, 0.0005), "vol": (0.002, 0.001), "er": (0.8, 0.15)},
            "TRENDING_BEAR":    {"ret": (-0.001, 0.0005), "vol": (0.002, 0.001), "er": (0.8, 0.15)},
            "HIGH_VOL_CHOP":    {"ret": (0.0, 0.002), "vol": (0.008, 0.002), "er": (0.3, 0.15)},
            "MEAN_REVERTING":   {"ret": (0.0, 0.0005), "vol": (0.0015, 0.0005), "er": (0.2, 0.1)},
            "LIQUIDITY_VACUUM": {"ret": (0.0, 0.001), "vol": (0.004, 0.002), "er": (0.5, 0.2)}
        }
        
        # 3. Calculate Log-Emission Probabilities
        log_emissions = np.zeros(5)
        for i, regime in enumerate(self.regimes):
            arch = archetypes[regime]
            log_p_ret = self._log_gaussian_pdf(mu_ret, arch["ret"][0], arch["ret"][1])
            log_p_vol = self._log_gaussian_pdf(volatility, arch["vol"][0], arch["vol"][1])
            log_p_er  = self._log_gaussian_pdf(er, arch["er"][0], arch["er"][1])
            
            # Summing in log space is multiplying in linear space
            log_emission = log_p_ret + log_p_vol + log_p_er
            
            # Liquidity Vacuum uniquely keys off volume drops
            if regime == "LIQUIDITY_VACUUM" and avg_vol < np.percentile(volumes, 25):
                log_emission += math.log(2.0) 
                
            log_emissions[i] = log_emission
            
        # 4. HMM Forward Algorithm: Update Belief State using Log-Sum-Exp Trick
        prior = np.dot(self.transition_matrix.T, self.state_probs)
        prior_log = np.log(prior + 1e-9)
        
        unnormalized_log_posterior = log_emissions + prior_log
        max_log = np.max(unnormalized_log_posterior)
        posterior = np.exp(unnormalized_log_posterior - max_log)
        self.state_probs = posterior / np.sum(posterior)
        
        # 5. Extract Maximum Likelihood Regime
        best_state_idx = int(np.argmax(self.state_probs))
        detected_regime = self.regimes[best_state_idx]
        
        # Map back to legacy binary tags for main.py execution pipeline backward compatibility
        if detected_regime in ["TRENDING_BULL", "TRENDING_BEAR"]:
            return "TRENDING"
        elif detected_regime in ["HIGH_VOL_CHOP", "MEAN_REVERTING", "LIQUIDITY_VACUUM"]:
            return "RANGING"
            
        return "RANGING"

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

            if self.local_bids and self.local_asks:
                best_bids = heapq.nlargest(10, self.local_bids.items(), key=lambda x: x[0])
                best_asks = heapq.nsmallest(10, self.local_asks.items(), key=lambda x: x[0])

                if best_bids and best_asks:
                    best_bid_price = best_bids[0][0]
                    best_ask_price = best_asks[0][0]
                    
                    if best_bid_price < best_ask_price:
                        self._latest_mid = (best_bid_price + best_ask_price) / 2.0
                    
                    self._cached_snapshot = {
                        "bids": [[str(p), str(s)] for p, s in best_bids],
                        "asks": [[str(p), str(s)] for p, s in best_asks]
                    }
                    
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
            momentum_matrix[f"momentum_{tf}"] = (current_close - historical_close) / max(historical_close, 1e-9)
            
        return momentum_matrix

    def get_latest_mid(self) -> float:
        return getattr(self, '_latest_mid', 0.0)

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
        if not snapshot["bids"] or not snapshot["asks"]:
            return {}
            
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