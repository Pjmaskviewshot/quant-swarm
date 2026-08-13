"""
ðŸŒŒ V1.0 TITANIUM APEX: OMNI-SWARM CROSS-SECTIONAL SCANNER
----------------------------------------------------------
Scans Bybit perpetual universe using Stabilized 60-Bar PCA Beta-Stripping.
Upgraded with Relaxed Hot-Swap Thresholds to eradicate liquidity stagnation,
ensuring the Swarm continuously rotates into high-RVOL, high-Alpha nodes.
Features Titanium Blocklist filtering and Adaptive Session Volume Thresholds.
"""

import math
import time
import numpy as np
import logging
from typing import List, Dict, Tuple, Set, Optional

from features.micro_models import AdaptiveSessionClock

logger = logging.getLogger("QUANT_CORE.OMNI_SWARM")


def compute_pca_residual_alpha(price_matrix: np.ndarray) -> np.ndarray:
    """
    ðŸš€ V1.0 QUANTUM MICRO-CORE: 60-Bar PCA Eigenvector Beta-Stripping
    Computes the top Principal Component (PC1) representing the global market factor
    across a stable 60-period return window, returning idiosyncratic alpha residuals.
    """
    # Require at least 2 assets and 30 time steps for a stable covariance decomposition
    if price_matrix.shape[0] < 2 or price_matrix.shape[1] < 30:
        return np.zeros(price_matrix.shape[0])
        
    with np.errstate(divide='ignore', invalid='ignore'):
        stds = np.std(price_matrix, axis=1, keepdims=True) + 1e-9
        norm_matrix = (price_matrix - np.mean(price_matrix, axis=1, keepdims=True)) / stds
    
    try:
        # Singular Value Decomposition on N x T matrix
        U, S, Vt = np.linalg.svd(norm_matrix, full_matrices=False)
        market_factor = Vt[0, :]  # Top right-singular vector (Global Market PC1)
        
        factor_norm = np.dot(market_factor, market_factor) + 1e-9
        residuals = []
        
        for i in range(norm_matrix.shape[0]):
            beta = np.dot(norm_matrix[i, :], market_factor) / factor_norm
            residual = norm_matrix[i, -1] - (beta * market_factor[-1])
            residuals.append(residual)
            
        return np.array(residuals) * 10000.0  # Return residuals in bps
    except Exception as e:
        logger.debug(f"[X-RAY] PCA SVD Computation failed (Matrix Singularity): {e}")
        return np.zeros(price_matrix.shape[0])


class GlobalOmniScanner:
    """
    ðŸŒŒ V1.0 OMNI-SWARM CROSS-SECTIONAL SCANNER
    Scans Bybit perpetual universe with live microstructure spread gating and 
    logarithmic liquidity weighting. Enforces a strict 30-minute swap cooldown.
    """
    def __init__(self, executor):
        self.executor = executor
        self.market_memory: Dict[str, Dict[str, list]] = {}
        self.btc_returns = []
        self.last_btc_price = 0.0
        self.last_swap_time = 0.0  

    async def _fetch_global_tickers(self) -> dict:
        try:
            res = await self.executor.safe_call(self.executor.client.get_tickers, category="linear")
            return {item['symbol']: item for item in res.get("result", {}).get("list", []) if item['symbol'].endswith('USDT')}
        except Exception as e:
            logger.error(f"[X-RAY] âŒ Global ticker fetch failed during Omni-Scan: {e}")
            return {}

    async def scan_and_rank_universe(
        self, 
        current_basket: List[str], 
        protected_symbols: Optional[Set[str]] = None
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Calculates RVOL and 60-Bar PCA Beta-Stripped Alpha using true log-returns. 
        Returns (Symbol_To_Drop, Symbol_To_Add) if a severe alpha anomaly is detected.
        """
        if protected_symbols is None:
            protected_symbols = set()

        # Enforce 30-minute cooldown between hot-swaps to prevent matrix thrashing
        if time.time() - self.last_swap_time < 1800.0:
            return None, None

        tickers = await self._fetch_global_tickers()
        if not tickers: 
            return None, None

        scoring_matrix = []
        valid_symbols = []
        return_matrix_rows = []
        turnover_map = {}

        # Track Global Benchmark (BTC)
        btc_data = tickers.get("BTCUSDT")
        if btc_data:
            current_btc_price = float(btc_data.get('lastPrice', 0))
            if self.last_btc_price > 0 and current_btc_price > 0:
                btc_ret = math.log(current_btc_price / self.last_btc_price)
                self.btc_returns.append(btc_ret)
                if len(self.btc_returns) > 120: 
                    self.btc_returns.pop(0)
            self.last_btc_price = current_btc_price

        # ðŸš€ V1.0 TITANIUM BLOCKLIST
        banned_keywords = ["SOXL", "SPCX", "SKHY", "SNDK", "BANK", "MUUSDT", "BEAT", "MSTR", "ESPUSDT", "DEXE", "PUMP", "EUL", "XAU", "XAG", "BTCUSDT"]
        
        # ðŸš€ ADAPTIVE SESSION CLOCK
        min_turnover = AdaptiveSessionClock.get_turnover_threshold()

        for sym, data in tickers.items():
            try:
                if any(b in sym for b in banned_keywords):
                    continue

                current_price = float(data.get('lastPrice', 0))
                turnover24h = float(data.get('turnover24h', 0))
                bid = float(data.get('bid1Price', 0) or 0)
                ask = float(data.get('ask1Price', 0) or 0)
                
                if bid <= 0 or ask <= 0 or ask <= bid:
                    continue
                    
                spread_bps = ((ask - bid) / bid) * 10000.0
                
                # Filter out illiquid or untradeable assets
                if current_price < 0.01 or turnover24h < min_turnover or spread_bps > 12.0:
                    continue

                vol = float(data.get('volume24h', 0))
                
                if sym not in self.market_memory:
                    self.market_memory[sym] = {"vol": [], "returns": [], "last_price": current_price}
                    continue 
                    
                prev_price = self.market_memory[sym]["last_price"]
                if prev_price > 0:
                    sym_ret = math.log(current_price / prev_price)
                else:
                    sym_ret = 0.0
                
                self.market_memory[sym]["last_price"] = current_price
                self.market_memory[sym]["vol"].append(vol)
                self.market_memory[sym]["returns"].append(sym_ret)
                
                # Maintain up to 120 historical observations
                if len(self.market_memory[sym]["vol"]) > 120:
                    self.market_memory[sym]["vol"].pop(0)
                    self.market_memory[sym]["returns"].pop(0)

                # Require at least 60 return observations for PCA stability
                if len(self.market_memory[sym]["returns"]) >= 60:
                    valid_symbols.append(sym)
                    return_matrix_rows.append(self.market_memory[sym]["returns"][-60:])
                    turnover_map[sym] = turnover24h

            except Exception:
                continue

        if not valid_symbols:
            return None, None

        price_matrix = np.array(return_matrix_rows)
        pca_alphas = compute_pca_residual_alpha(price_matrix)

        for idx, sym in enumerate(valid_symbols):
            try:
                vol_array = np.array(self.market_memory[sym]["vol"])
                mu_v = np.mean(vol_array)
                sig_v = np.std(vol_array) + 1e-9
                rvol_z = (vol_array[-1] - mu_v) / sig_v
                
                idiosyncratic_alpha = pca_alphas[idx]
                
                # Logarithmic Liquidity Weighting
                turnover_weight = math.log10(max(turnover_map[sym], 1e-9)) / 10.0 
                
                # Scoring: 60% RVOL Z-Score + 40% Idiosyncratic Alpha
                swarm_score = ((rvol_z * 0.6) + (abs(idiosyncratic_alpha) * 0.4)) * turnover_weight
                scoring_matrix.append((swarm_score, sym, rvol_z))
            except Exception:
                continue

        scoring_matrix.sort(key=lambda x: x[0], reverse=True)

        if not scoring_matrix: 
            return None, None

        top_score, top_sym, top_z = scoring_matrix[0]
        
        # ðŸš€ V1.0 ANTI-STARVATION UPGRADE: Relaxed Hot-Swap Trigger
        # Lowered Z-Score requirement from 3.0 to 2.0. Lowered base score from 2500 to 1500.
        if top_sym not in current_basket and top_z > 2.0 and top_score > 1500.0:
            basket_scores = [
                item for item in scoring_matrix 
                if item[1] in current_basket 
                and item[1] != "BTCUSDT" 
                and item[1] not in protected_symbols
            ]
            
            if basket_scores:
                deadest_score, deadest_sym, deadest_z = basket_scores[-1]
                
                # Only execute swap if the candidate is 3x stronger than the weakest active symbol (Was 5x)
                if top_score > (deadest_score * 3.0):
                    logger.critical(
                        f"[X-RAY] ðŸŒªï¸ OMNI-SWARM HOT-SWAP TRIGGERED: Dropping {deadest_sym} (Score: {deadest_score:.2f}) -> "
                        f"Injecting Pure-Alpha Asset {top_sym} (Score: {top_score:.2f} | RVOL-Z: {top_z:.1f})"
                    )
                    self.last_swap_time = time.time()  # Lock the matrix for 30 minutes
                    return deadest_sym, top_sym

        return None, None