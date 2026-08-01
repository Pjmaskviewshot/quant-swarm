"""
💎 V55.2 QUANTUM SWARM: MACRO-AWARE TENSOR MATRIX
-------------------------------------------------
Computes real-time cross-asset impulse propagation (BTC -> Alts).
Uses Millisecond-Precise Event-Time Alignment to eradicate Look-Ahead Bias.
Includes X-Ray Telemetry for macro-signal detection.
"""

import math
import time
import numpy as np
import logging
from collections import deque
from typing import Dict, Any, Tuple, List

logger = logging.getLogger("QUANT_CORE.TENSOR_ORACLE")

class CrossAssetTensorOracle:
    def __init__(self, history_len: int = 1000):
        # Stores tuples of (millisecond_timestamp, price)
        self.btc_prices = deque(maxlen=history_len)
        self.alt_prices = {}
        self.history_len = history_len
        
        # X-Ray Log Throttling to prevent console spam
        self._last_log_time = {}

    def ingest_tick(self, symbol: str, price: float, exchange_timestamp: float):
        """Stores real-time tick prices with raw millisecond precision."""
        if symbol == "BTCUSDT":
            self.btc_prices.append((exchange_timestamp, price))
        else:
            if symbol not in self.alt_prices:
                self.alt_prices[symbol] = deque(maxlen=self.history_len)
            self.alt_prices[symbol].append((exchange_timestamp, price))

    def compute_lead_lag_signal(self, target_symbol: str) -> float:
        """
        🚀 V55.2 FIX: Sub-Second Asynchronous Merge Alignment
        Calculates cross-covariance tensor using exact millisecond timestamps.
        Strictly maps BTC[t-1] to ALT[t] to guarantee no future data leakage.
        """
        if target_symbol == "BTCUSDT" or target_symbol not in self.alt_prices:
            return 0.0
            
        btc_p = list(self.btc_prices)
        alt_p = list(self.alt_prices[target_symbol])
        
        if len(btc_p) < 30 or len(alt_p) < 30: 
            return 0.0
            
        # 1. Align time series based on exact exchange timestamps (Millisecond Pointer Scan)
        aligned_b = []
        aligned_a = []
        
        # Fast pointer to avoid O(N^2) searches
        btc_idx = 0
        btc_len = len(btc_p)
        
        for i in range(1, len(alt_p)):
            alt_ts, alt_price = alt_p[i]
            prev_alt_price = alt_p[i-1][1]
            
            # 🛡️ Look-ahead Bias Prevention: Find the latest BTC trade that occurred STRICTLY BEFORE alt_ts
            # This is a pure Python implementation of pandas.merge_asof(direction='backward')
            lagged_btc_price = None
            prev_lagged_btc_price = None
            
            # Move the pointer forward until we hit the future, then step back one
            while btc_idx < btc_len and btc_p[btc_idx][0] < alt_ts:
                btc_idx += 1
                
            if btc_idx > 1:
                lagged_btc_price = btc_p[btc_idx - 1][1]
                prev_lagged_btc_price = btc_p[btc_idx - 2][1]
            
            if lagged_btc_price is not None and prev_lagged_btc_price is not None:
                try:
                    a_ret = math.log(alt_price / (prev_alt_price + 1e-9))
                    b_ret = math.log(lagged_btc_price / (prev_lagged_btc_price + 1e-9))
                    
                    aligned_a.append(a_ret)
                    aligned_b.append(b_ret)
                except ValueError:
                    continue
                
        if len(aligned_a) < 20:
            return 0.0

        # 2. Compute true lagged Pearson correlation (Guarded against Zero-Variance)
        try:
            # Ignore Numpy warnings if price is flat and variance is 0
            with np.errstate(divide='ignore', invalid='ignore'):
                correlation = np.corrcoef(aligned_b, aligned_a)[0, 1]
                
            if np.isnan(correlation):
                return 0.0
        except Exception:
            return 0.0
            
        # 3. Compute leading momentum vector from BTC
        btc_momentum = np.mean(aligned_b[-10:])
        
        # 4. Signal Generation & X-Ray Logging
        if abs(btc_momentum) > 0.0002 and correlation > 0.60:
            alpha_signal = np.sign(btc_momentum) * min(1.0, abs(correlation))
            
            # X-Ray Telemetry Throttler (Alert once per 60 seconds per coin)
            now = time.time()
            if now - self._last_log_time.get(target_symbol, 0.0) > 60.0:
                direction = "BULLISH" if alpha_signal > 0 else "BEARISH"
                logger.info(f"[X-RAY] 🌌 TENSOR STRIKE // {target_symbol} following BTC {direction} wave. Correlation: {correlation:.2f}")
                self._last_log_time[target_symbol] = now
                
            return float(alpha_signal)
            
        return 0.0