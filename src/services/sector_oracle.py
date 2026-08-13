"""
ðŸ’Ž V1.0 TITANIUM APEX: SECTOR EIGENVECTOR ORACLE
-------------------------------------------------
Tracks cross-asset sector impulse propagation using Dynamic SVD Decomposition.
Extracts the Principal Component (PC1) of a given asset cluster to determine 
if the overarching sector momentum supports a microscopic breakout signal.
"""

import math
import numpy as np
import logging
from collections import deque
from typing import Dict, List, Tuple

logger = logging.getLogger("QUANT_CORE.SECTOR_ORACLE")


class SectorEigenOracle:
    """
    Computes real-time lead-lag impulse coupling by projecting target asset 
    returns against the dominant eigenvector of its sector cohort.
    """
    def __init__(self, history_len: int = 120):
        self.history_len = history_len
        
        # Stores log-returns and last known prices for asset clustering
        self.asset_returns: Dict[str, deque] = {}
        self.asset_prices: Dict[str, float] = {}

    def ingest_price(self, symbol: str, price: float):
        """
        Ingests real-time pricing, converting raw ticks into rolling log-returns.
        Must be called continuously from the websocket ingestion loop.
        """
        if symbol not in self.asset_returns:
            self.asset_returns[symbol] = deque(maxlen=self.history_len)
            self.asset_prices[symbol] = price
            return

        prev_p = self.asset_prices[symbol]
        
        if prev_p > 0 and price > 0:
            ret = math.log(price / prev_p)
            self.asset_returns[symbol].append(ret)

        self.asset_prices[symbol] = price

    def compute_sector_impulse(self, target_symbol: str, cluster_symbols: List[str]) -> Tuple[float, float]:
        """
        Extracts the sector PC1 via SVD and computes the target asset's correlation
        and leading impulse score against it.
        
        Args:
            target_symbol: The asset triggering a local edge gate signal.
            cluster_symbols: The macro sector basket (e.g., ["SOLUSDT", "JUPUSDT", "RAYUSDT"]).
            
        Returns:
            Tuple[float, float]: (Impulse_Score, Sector_Correlation)
        """
        # Filter for symbols with sufficient historical data for a stable covariance matrix
        valid_symbols = [s for s in cluster_symbols if s in self.asset_returns and len(self.asset_returns[s]) >= 30]
        
        # Require at least 3 assets to form a meaningful sector eigenvector
        if target_symbol not in valid_symbols or len(valid_symbols) < 3:
            return 0.0, 0.0

        # Align lengths of all return arrays to the shortest available history
        min_len = min(len(self.asset_returns[s]) for s in valid_symbols)
        matrix_rows = [list(self.asset_returns[s])[-min_len:] for s in valid_symbols]
        
        # R is an M x T matrix (M = assets, T = time ticks)
        R = np.array(matrix_rows)

        # Standardize the returns matrix (Z-Score normalization per asset)
        stds = np.std(R, axis=1, keepdims=True) + 1e-9
        R_norm = (R - np.mean(R, axis=1, keepdims=True)) / stds

        try:
            # ðŸš€ SINGULAR VALUE DECOMPOSITION (SVD)
            # U: Left-singular vectors, S: Singular values, Vt: Right-singular vectors
            U, S, Vt = np.linalg.svd(R_norm, full_matrices=False)
            
            # The top right-singular vector represents the primary Sector Trend Factor
            sector_factor = Vt[0, :] 

            target_idx = valid_symbols.index(target_symbol)
            target_rets = R_norm[target_idx, 1:]  # Current returns
            sector_lagged = sector_factor[:-1]    # Lagged sector factor

            if len(target_rets) < 20:
                return 0.0, 0.0

            # ðŸš€ LAGGED PEARSON CORRELATION
            with np.errstate(divide='ignore', invalid='ignore'):
                corr = float(np.corrcoef(sector_lagged, target_rets)[0, 1])

            if np.isnan(corr):
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