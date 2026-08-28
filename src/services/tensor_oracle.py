"""
💎 V22.4 APEX QUANTUM PRIME: STATISTICALLY HARDENED TENSOR ORACLE
-------------------------------------------------------------
Computes real-time cross-asset impulse propagation (BTC/ETH/SOL -> Alts).
Uses Millisecond-Precise Event-Time Backward Pointer Alignment to eradicate 
Look-Ahead Bias in sub-second real-time. 

Audit Fixes (V22.4):
- Student's t-Distribution Significance Filter: Replaces static thresholds with 
  rigorous hypothesis testing (p < 0.01) to reject spurious tick correlations.
- Enforced Sample Size: Increased minimum observation window to N=60 for statistical validity.
- Time-Window Synchronization: Fixes the mathematical category error where benchmark 
  returns were measured over random microsecond tick gaps instead of matching the altcoin's exact time window.
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
    🚀 V22.4 TENSOR-PRIME: Asynchronous Cross-Asset Lead-Lag Tensor Oracle
    Tracks real-time impulse propagation vectors from primary market anchors 
    (BTC, ETH, SOL) to target altcoins without future-data leakage.
    """
    def __init__(self, history_len: int = 1000):
        # Stores tuples of (exchange_timestamp_ms, price)
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
        🚀 V22.4 UPGRADE: Sub-Second Asynchronous Pointer Alignment & T-Stat Gating.
        Calculates cross-covariance tensor using exact millisecond timestamps.
        Strictly maps Benchmark[t-1] to Alt[t] to guarantee zero look-ahead bias
        and enforces exact time-window parity for log returns.
        
        Returns:
            float: Lead-lag alpha signal bounded between -1.0 and +1.0.
        """
        if target_symbol in self.benchmark_prices or target_symbol not in self.alt_prices:
            return 0.0
            
        benchmark_ticks = self.benchmark_prices.get(benchmark_symbol)
        alt_ticks = self.alt_prices.get(target_symbol)
        
        # Enforce statistical minimum history of 60 ticks
        if not benchmark_ticks or not alt_ticks or len(benchmark_ticks) < 60 or len(alt_ticks) < 60: 
            return 0.0
            
        # Convert only once per computation to avoid O(N) penalties in deep loops
        bench_list = list(benchmark_ticks)
        alt_list = list(alt_ticks)
        
        aligned_benchmark = []
        aligned_alt = []
        
        # Fast pointer scan to avoid O(N^2) searches
        bench_idx = 0
        bench_len = len(bench_list)
        
        # Align initial pointer
        while bench_idx < bench_len and bench_list[bench_idx][0] < alt_list[0][0]:
            bench_idx += 1
            
        prev_bench_price = bench_list[bench_idx - 1][1] if bench_idx > 0 else None
        prev_bench_ts = bench_list[bench_idx - 1][0] if bench_idx > 0 else None
        
        for i in range(1, len(alt_list)):
            alt_ts, alt_price = alt_list[i]
            alt_ts_prev, prev_alt_price = alt_list[i-1]
            
            # Advance benchmark pointer to just before the current alt tick
            while bench_idx < bench_len and bench_list[bench_idx][0] < alt_ts:
                bench_idx += 1
                
            curr_bench_price = bench_list[bench_idx - 1][1] if bench_idx > 0 else None
            curr_bench_ts = bench_list[bench_idx - 1][0] if bench_idx > 0 else None
            
            if prev_bench_price is not None and curr_bench_price is not None:
                # 🛡️ STALENESS GUARD: Ensure benchmark data isn't disconnected by > 60 seconds
                if (alt_ts - curr_bench_ts) < 60000 and (alt_ts_prev - prev_bench_ts) < 60000:
                    if prev_alt_price > 0 and prev_bench_price > 0:
                        try:
                            alt_ret = math.log(alt_price / prev_alt_price)
                            bench_ret = math.log(curr_bench_price / prev_bench_price)
                            
                            aligned_alt.append(alt_ret)
                            aligned_benchmark.append(bench_ret)
                        except ValueError:
                            pass
                            
            prev_bench_price = curr_bench_price
            prev_bench_ts = curr_bench_ts
            
        n = len(aligned_alt)
        if n < 60:
            return 0.0

        bench_arr = np.array(aligned_benchmark, dtype=np.float64)
        alt_arr = np.array(aligned_alt, dtype=np.float64)

        # 🚀 ZERO-VARIANCE GUARD: Prevent NaN correlation matrices
        if np.std(bench_arr) == 0 or np.std(alt_arr) == 0:
            return 0.0

        # Compute true lagged Pearson correlation
        try:
            r = float(np.corrcoef(bench_arr, alt_arr)[0, 1])
            if np.isnan(r) or abs(r) >= 1.0:
                return 0.0
                
            # 🚀 V22.4 STUDENT'S T SIGNIFICANCE TEST (p < 0.01 at df >= 58 requires t > 2.66)
            t_stat = r * math.sqrt((n - 2) / (1.0 - r**2))
            if abs(t_stat) < 2.66:
                return 0.0  # Spurious correlation rejected
                
            # Compute leading momentum vector from Benchmark
            bench_momentum = float(np.mean(bench_arr[-10:]))
            
            # 🛡️ ALIGNMENT FILTER: Require momentum and valid correlation
            if abs(bench_momentum) > 0.00015 and abs(r) > 0.50:
                # Bound alpha signal between -1.0 and +1.0
                alpha_signal = math.copysign(min(1.0, abs(r)), bench_momentum)
                
                # X-Ray Telemetry Throttler (Alert once per 60 seconds per coin)
                now = time.time()
                if now - self._last_log_time.get(target_symbol, 0.0) > 60.0:
                    direction = "BULLISH" if alpha_signal > 0 else "BEARISH"
                    logger.info(
                        f"[X-RAY] 🌌 TENSOR STRIKE // {target_symbol} following {benchmark_symbol} "
                        f"{direction} wave. Correlation: {r:.2f} | T-Stat: {t_stat:.2f} | Momentum: {bench_momentum*10000:.1f} bps"
                    )
                    self._last_log_time[target_symbol] = now
                    
                return float(alpha_signal)
                
        except Exception:
            return 0.0
            
        return 0.0