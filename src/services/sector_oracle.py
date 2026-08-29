"""
💎 V25.0 APEX QUANTUM PRIME: SECTOR EIGENVECTOR ORACLE
-------------------------------------------------
Tracks cross-asset sector impulse propagation using Dynamic SVD Decomposition.

Architectural Supremacy (V25.0):
1. Stateless Pure-Async Execution: Eradicated tick-by-tick `deque` memory buffers. 
   The Oracle now ingests matrix slices directly from the centralized MarketStateMatrix.
2. SVD Thread Isolation: Singular Value Decomposition (SVD) is mathematically 
   CPU-bound. The eigenvector extraction is now aggressively offloaded to an 
   `asyncio.to_thread` worker to prevent the HFT event loop from freezing.
3. NaN/Inf Matrix Guards: Hardened matrix standardization to prevent LAPACK 
   singular matrix crashes during violent flash cascades.
"""

import math
import numpy as np
import logging
import asyncio
from typing import Dict, List, Tuple

logger = logging.getLogger("QUANT_CORE.SECTOR_ORACLE")

def _svd_compute_core(target_symbol: str, valid_symbols: List[str], returns_data: List[List[float]]) -> Tuple[float, float]:
    """
    CPU-bound SVD computation isolated from the async event loop.
    """
    try:
        # R is an M x T matrix (M = assets, T = time ticks)
        R = np.array(returns_data, dtype=np.float64)

        # Standardize the returns matrix (Z-Score normalization per asset)
        with np.errstate(divide='ignore', invalid='ignore'):
            stds = np.std(R, axis=1, keepdims=True) + 1e-9
            R_norm = (R - np.mean(R, axis=1, keepdims=True)) / stds

        # 🚀 SINGULAR VALUE DECOMPOSITION (SVD)
        # U: Left-singular vectors, S: Singular values, Vt: Right-singular vectors
        U, S, Vt = np.linalg.svd(R_norm, full_matrices=False)
        
        # The top right-singular vector represents the primary Sector Trend Factor
        sector_factor = Vt[0, :] 

        target_idx = valid_symbols.index(target_symbol)
        target_rets = R_norm[target_idx, 1:]  # Current returns
        sector_lagged = sector_factor[:-1]    # Lagged sector factor

        if len(target_rets) < 20:
            return 0.0, 0.0

        # 🚀 LAGGED PEARSON CORRELATION
        with np.errstate(divide='ignore', invalid='ignore'):
            corr = float(np.corrcoef(sector_lagged, target_rets)[0, 1])

        if np.isnan(corr) or np.isinf(corr):
            corr = 0.0

        # Compute the leading momentum of the sector itself
        sector_momentum = float(np.mean(sector_factor[-5:]))
        
        # The Impulse Score combines correlation strength with the direction of the sector
        if abs(sector_momentum) > 0.0001:
            impulse_score = math.copysign(min(1.0, abs(corr)), sector_momentum)
        else:
            impulse_score = 0.0

        return impulse_score, corr

    except Exception as e:
        # Swallow matrix singularity or convergence faults gracefully
        logger.debug(f"[X-RAY] Sector SVD computation fault for {target_symbol}: {e}")
        return 0.0, 0.0


class SectorEigenOracle:
    """
    🚀 V25.0 STATELESS TENSOR ORACLE
    Computes real-time lead-lag impulse coupling by projecting target asset 
    returns against the dominant eigenvector of its sector cohort.
    """
    
    @staticmethod
    async def compute_sector_impulse(target_symbol: str, cluster_returns: Dict[str, List[float]]) -> Tuple[float, float]:
        """
        Extracts the sector PC1 via SVD and computes the target asset's correlation
        and leading impulse score against it.
        
        Args:
            target_symbol: The asset triggering a local edge gate signal.
            cluster_returns: Centralized mapping of symbol -> log-returns history.
            
        Returns:
            Tuple[float, float]: (Impulse_Score, Sector_Correlation)
        """
        if not cluster_returns:
            return 0.0, 0.0

        # Filter for symbols with sufficient historical data for a stable covariance matrix
        valid_symbols = [s for s, rets in cluster_returns.items() if len(rets) >= 30]
        
        # Require at least 3 assets to form a meaningful sector eigenvector
        if target_symbol not in valid_symbols or len(valid_symbols) < 3:
            return 0.0, 0.0

        # Align lengths of all return arrays to the shortest available history
        min_len = min(len(cluster_returns[s]) for s in valid_symbols)
        matrix_rows = [list(cluster_returns[s])[-min_len:] for s in valid_symbols]
        
        # 🚀 V25.0 ASYNC SVD OFFLOADING
        # Passes the sanitized matrix to a background thread to prevent GIL lock
        impulse_score, corr = await asyncio.to_thread(
            _svd_compute_core, 
            target_symbol, 
            valid_symbols, 
            matrix_rows
        )
        
        return impulse_score, corr