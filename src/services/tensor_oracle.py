"""
💎 V25.0 APEX QUANTUM PRIME: STATISTICALLY HARDENED TENSOR ORACLE
-------------------------------------------------------------
Computes real-time cross-asset impulse propagation (BTC/ETH/SOL -> Alts).
Uses Millisecond-Precise Event-Time Backward Pointer Alignment to eradicate 
Look-Ahead Bias in sub-second real-time. 

Architectural Supremacy (V25.0):
1. Stateless Pure-Async Execution: Eradicated tick-by-tick `deque` ingestion 
   and state memory bloat. The Oracle now accepts tick arrays directly from 
   the centralized `MarketStateMatrix`.
2. CPU-Bound Thread Isolation: The O(N) backward-pointer alignment and Pearson 
   correlation are mathematically CPU-bound. These have been aggressively offloaded 
   to an `asyncio.to_thread` worker to prevent HFT event loop starvation.
3. Strict Zero-Variance & T-Stat Gating: Retains rigorous hypothesis testing 
   (p < 0.01) to reject spurious tick correlations during low-liquidity chop.
"""

import math
import numpy as np
import logging
import asyncio
from typing import List, Tuple

logger = logging.getLogger("QUANT_CORE.TENSOR_ORACLE")

def _compute_lead_lag_core(
    target_symbol: str, 
    benchmark_symbol: str, 
    bench_list: List[Tuple[float, float]], 
    alt_list: List[Tuple[float, float]]
) -> float:
    """
    CPU-bound Sub-Second Asynchronous Pointer Alignment & T-Stat Gating.
    Isolated from the asyncio event loop to preserve execution latency.
    """
    if len(bench_list) < 60 or len(alt_list) < 60: 
        return 0.0
        
    aligned_benchmark = []
    aligned_alt = []
    
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
            
        # 🚀 V25.0 STUDENT'S T SIGNIFICANCE TEST (p < 0.01 at df >= 58 requires t > 2.66)
        t_stat = r * math.sqrt((n - 2) / (1.0 - r**2))
        if abs(t_stat) < 2.66:
            return 0.0  # Spurious correlation rejected
            
        # Compute leading momentum vector from Benchmark
        bench_momentum = float(np.mean(bench_arr[-10:]))
        
        # 🛡️ ALIGNMENT FILTER: Require momentum and valid correlation
        if abs(bench_momentum) > 0.00015 and abs(r) > 0.50:
            # Bound alpha signal between -1.0 and +1.0
            return float(math.copysign(min(1.0, abs(r)), bench_momentum))
            
    except Exception as e:
        logger.debug(f"[MATH_WARN] Lead-lag computation fault: {e}")
        
    return 0.0


class CrossAssetTensorOracle:
    """
    🚀 V25.0 TENSOR-PRIME: Stateless Cross-Asset Lead-Lag Tensor Oracle
    Tracks real-time impulse propagation vectors from primary market anchors 
    (BTC, ETH, SOL) to target altcoins without future-data leakage.
    """
    
    @staticmethod
    async def compute_lead_lag_signal(
        target_symbol: str, 
        benchmark_symbol: str, 
        benchmark_ticks: List[Tuple[float, float]], 
        alt_ticks: List[Tuple[float, float]]
    ) -> float:
        """
        Asynchronously calculates cross-covariance tensor using exact millisecond timestamps.
        Strictly maps Benchmark[t-1] to Alt[t] to guarantee zero look-ahead bias
        and enforces exact time-window parity for log returns.
        """
        if not benchmark_ticks or not alt_ticks:
            return 0.0
            
        # 🚀 V25.0 ASYNC OFFLOADING
        # Passes the raw tick arrays to a background thread to prevent GIL lock
        alpha_signal = await asyncio.to_thread(
            _compute_lead_lag_core, 
            target_symbol, 
            benchmark_symbol, 
            benchmark_ticks, 
            alt_ticks
        )
        
        if alpha_signal != 0.0:
            direction = "BULLISH" if alpha_signal > 0 else "BEARISH"
            logger.info(f"[X-RAY] 🌌 TENSOR STRIKE // {target_symbol} following {benchmark_symbol} {direction} wave.")
            
        return alpha_signal