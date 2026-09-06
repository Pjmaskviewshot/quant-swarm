"""
💎 V36.5 APEX TITAN: THREAD-SAFE SECTOR EIGEN ORACLE
--------------------------------------------------------
O(1) Cached Cross-Asset Principal Component Analysis.

Architectural Supremacy (V36.5 Stability Patch):
- LAPACK Heap-Corruption Eradication: Purged fragile C-level `np.linalg.svd`
  which causes NTStatus 0xC0000374 heap corruption during concurrent execution.
- Von Mises Power Iteration: Extracts the dominant eigenvector (PC1) in 8 pure
  dot products without touching LAPACK workspace buffers.
- High-Speed Atomic Caching: Caches global eigen-decomposition with a 1.0s TTL
  to prevent redundant recalculations across 20+ concurrent asset feeds.
- Thread-Safe Memory Bounds: Guards against zero-variance, singular covariance,
  and mismatched array lengths during cold-boot periods.
"""

import time
import math
import asyncio
import logging
import numpy as np
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger("QUANT_CORE.SECTOR_ORACLE")


class SectorEigenOracle:
    """
    🚀 V36.5 HIGH-PERFORMANCE SECTOR EIGENVECTOR ORACLE
    Extracts sector momentum and market-wide beta impulses using pure
    thread-safe linear algebra and lock-free time-bounded caching.
    """
    _cache_lock = asyncio.Lock()
    _last_calc_time: float = 0.0
    _cached_eigenvector: Optional[np.ndarray] = None
    _cached_symbols: List[str] = []
    _cached_market_impulse: float = 0.0
    _cached_asset_correlations: Dict[str, float] = {}

    @classmethod
    def _power_iteration_pc1(cls, cov_matrix: np.ndarray, num_simulations: int = 8) -> np.ndarray:
        """
        Computes the dominant eigenvector (PC1) using pure matrix-vector multiplication.
        100% thread-safe; completely bypasses OpenBLAS LAPACK SVD workspace corruption.
        """
        n = cov_matrix.shape[0]
        # Start with a normalized uniform vector
        b_k = np.ones(n, dtype=np.float64) / math.sqrt(n)

        for _ in range(num_simulations):
            # Matrix-vector multiplication
            b_k1 = cov_matrix @ b_k
            norm = np.linalg.norm(b_k1)
            if norm < 1e-9:
                break
            b_k = b_k1 / norm

        return b_k

    @classmethod
    def _svd_compute_core(cls, cluster_returns: Dict[str, List[float]]) -> Tuple[float, Dict[str, float]]:
        """
        Pure-math compute routine: Standardizes returns, forms the covariance
        matrix, and extracts the top eigen-vector using power iteration.
        """
        valid_symbols = [s for s, rets in cluster_returns.items() if len(rets) >= 30]
        if len(valid_symbols) < 3:
            return 0.0, {}

        # Align length to the shortest return series
        min_len = min(len(cluster_returns[s]) for s in valid_symbols)
        matrix_rows = []
        for s in valid_symbols:
            matrix_rows.append(cluster_returns[s][-min_len:])

        R = np.array(matrix_rows, dtype=np.float64)

        # Standardize returns (Zero-mean, unit-variance)
        means = np.mean(R, axis=1, keepdims=True)
        stds = np.std(R, axis=1, keepdims=True) + 1e-9
        norm_R = (R - means) / stds

        # Covariance Matrix: (N x N)
        T_steps = norm_R.shape[1]
        cov_matrix = (norm_R @ norm_R.T) / max(1, T_steps - 1)

        # Extract top eigenvector (PC1) via Power Iteration
        pc1 = cls._power_iteration_pc1(cov_matrix, num_simulations=8)

        # Compute instantaneous Market Factor: Projection of latest returns onto PC1
        latest_returns = norm_R[:, -1]
        market_factor = float(np.dot(pc1, latest_returns))
        market_impulse = float(np.clip(market_factor / math.sqrt(len(valid_symbols)), -5.0, 5.0))

        # Asset correlations with PC1
        asset_correlations = {}
        for idx, s in enumerate(valid_symbols):
            asset_correlations[s] = float(np.clip(pc1[idx], -1.0, 1.0))

        return market_impulse, asset_correlations

    @classmethod
    async def compute_sector_impulse(
        cls, 
        symbol: str, 
        cluster_returns: Dict[str, List[float]]
    ) -> Tuple[float, float]:
        """
        🚀 O(1) TIME-BOUNDED INTERFACE
        Returns (sector_impulse, asset_correlation). Recomputes at most once
        every 1.0 second, eliminating redundant compute cycles across all assets.
        """
        now = time.time()

        # 1. Fast Lock-Free Read from Cache (<1.0s staleness)
        if now - cls._last_calc_time < 1.0 and cls._cached_asset_correlations:
            corr = cls._cached_asset_correlations.get(symbol, 0.0)
            impulse = cls._cached_market_impulse * (1.0 if corr >= 0 else -1.0)
            return impulse, corr

        # 2. Re-compute Cache under Async Lock
        async with cls._cache_lock:
            # Double check condition inside lock
            if now - cls._last_calc_time < 1.0 and cls._cached_asset_correlations:
                corr = cls._cached_asset_correlations.get(symbol, 0.0)
                impulse = cls._cached_market_impulse * (1.0 if corr >= 0 else -1.0)
                return impulse, corr

            try:
                loop = asyncio.get_running_loop()
                market_impulse, asset_corrs = await loop.run_in_executor(
                    None, cls._svd_compute_core, cluster_returns
                )
                cls._cached_market_impulse = market_impulse
                cls._cached_asset_correlations = asset_corrs
                cls._last_calc_time = now

                corr = asset_corrs.get(symbol, 0.0)
                impulse = market_impulse * (1.0 if corr >= 0 else -1.0)
                return impulse, corr

            except Exception as e:
                logger.debug(f"[X-RAY] Sector Eigen compute error: {e}")
                return 0.0, 0.0