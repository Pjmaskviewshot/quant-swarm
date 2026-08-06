"""
💎 V58.0 TITANIUM APEX: MACRO-AWARE CROSS-ASSET TENSOR ORACLE
-------------------------------------------------------------
Computes real-time cross-asset impulse propagation (BTC/ETH/SOL -> Alts).
Uses Millisecond-Precise Event-Time Backward Pointer Alignment to eradicate 
Look-Ahead Bias in sub-second real-time. Bounded correlation dampening 
prevents false alpha triggers during un-correlated altcoin divergence.
"""

import math
import time
import numpy as np
import logging
from collections import deque
from typing import Dict, Any, Tuple, List, Optional

logger = logging.getLogger("QUANT_CORE.TENSOR_ORACLE")

class CrossAssetTensorOracle:
    """
    🚀 V58.0 APEX: Asynchronous Cross-Asset Lead-Lag Tensor Oracle
    Tracks real-time impulse propagation vectors from primary market anchors 
    (BTC, ETH, SOL) to target altcoins without future-data leakage.
    """
    def __init__(self, history_len: int = 1000):
        # Stores tuples of (exchange_timestamp_sec, price)
        self.benchmark_prices: Dict[str, deque] = {
            "BTCUSDT": deque(maxlen=history_len),
            "ETHUSDT": deque(maxlen=history_len),
            "SOLUSDT": deque(maxlen=history_len)
        }
        self.alt_prices: Dict[str, deque] = {}
        self.history_len = history_len
        
        # X-Ray Log Throttling to prevent console spam
        self._last_log_time: Dict[str, float] = {}

    def ingest_tick(self, symbol: str, price: float, exchange_timestamp: float):
        """
        Ingests real-time tick prices with raw millisecond precision in O(1) time.
        
        Args:
            symbol: Ticker symbol (e.g. "BTCUSDT", "JUPUSDT")
            price: Executed tick price
            exchange_timestamp: Millisecond or second timestamp from exchange
        """
        if price <= 0:
            return

        if symbol in self.benchmark_prices:
            self.benchmark_prices[symbol].append((exchange_timestamp, price))
        else:
            if symbol not in self.alt_prices:
                self.alt_prices[symbol] = deque(maxlen=self.history_len)
            self.alt_prices[symbol].append((exchange_timestamp, price))

    def compute_lead_lag_signal(self, target_symbol: str, benchmark_symbol: str = "BTCUSDT") -> float:
        """
        🚀 V58.0 UPGRADE: Sub-Second Asynchronous Pointer Alignment.
        Calculates cross-covariance tensor using exact millisecond timestamps.
        Strictly maps Benchmark[t-1] to Alt[t] to guarantee zero look-ahead bias.
        
        Returns:
            float: Lead-lag alpha signal bounded between -1.0 and +1.0.
        """
        if target_symbol in self.benchmark_prices or target_symbol not in self.alt_prices:
            return 0.0
            
        benchmark_ticks = list(self.benchmark_prices.get(benchmark_symbol, []))
        alt_ticks = list(self.alt_prices[target_symbol])
        
        if len(benchmark_ticks) < 30 or len(alt_ticks) < 30: 
            return 0.0
            
        aligned_benchmark = []
        aligned_alt = []
        
        # Fast pointer scan to avoid O(N^2) searches (Equivalent to pandas.merge_asof backward)
        bench_idx = 0
        bench_len = len(benchmark_ticks)
        
        for i in range(1, len(alt_ticks)):
            alt_ts, alt_price = alt_ticks[i]
            prev_alt_price = alt_ticks[i-1][1]
            
            lagged_bench_price = None
            prev_lagged_bench_price = None
            
            # Move the pointer forward until we cross alt_ts, then take the prior trade
            while bench_idx < bench_len and benchmark_ticks[bench_idx][0] < alt_ts:
                bench_idx += 1
                
            if bench_idx > 1:
                lagged_bench_price = benchmark_ticks[bench_idx - 1][1]
                prev_lagged_bench_price = benchmark_ticks[bench_idx - 2][1]
            
            if lagged_bench_price is not None and prev_lagged_bench_price is not None:
                try:
                    alt_ret = math.log(alt_price / (prev_alt_price + 1e-9))
                    bench_ret = math.log(lagged_bench_price / (prev_lagged_bench_price + 1e-9))
                    
                    aligned_alt.append(alt_ret)
                    aligned_benchmark.append(bench_ret)
                except ValueError:
                    continue
                
        if len(aligned_alt) < 20:
            return 0.0

        # Compute true lagged Pearson correlation (Guarded against Zero-Variance)
        try:
            with np.errstate(divide='ignore', invalid='ignore'):
                correlation = float(np.corrcoef(aligned_benchmark, aligned_alt)[0, 1])
                
            if np.isnan(correlation):
                return 0.0
        except Exception:
            return 0.0
            
        # Compute leading momentum vector from Benchmark
        bench_momentum = float(np.mean(aligned_benchmark[-10:]))
        
        # 🚀 V58.0 NOISE FILTER: Require minimum correlation threshold (0.45)
        if abs(bench_momentum) > 0.00015 and correlation > 0.45:
            # Bound alpha signal between -1.0 and +1.0
            alpha_signal = math.copysign(min(1.0, abs(correlation)), bench_momentum)
            
            # X-Ray Telemetry Throttler (Alert once per 60 seconds per coin)
            now = time.time()
            if now - self._last_log_time.get(target_symbol, 0.0) > 60.0:
                direction = "BULLISH" if alpha_signal > 0 else "BEARISH"
                logger.info(
                    f"[X-RAY] 🌌 TENSOR STRIKE // {target_symbol} following {benchmark_symbol} "
                    f"{direction} wave. Correlation: {correlation:.2f} | Momentum: {bench_momentum*10000:.1f} bps"
                )
                self._last_log_time[target_symbol] = now
                
            return float(alpha_signal)
            
        return 0.0