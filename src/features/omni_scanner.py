import math
import time
import numpy as np
import logging
from typing import List, Dict, Tuple, Set, Optional
import asyncio

logger = logging.getLogger("QUANT_CORE.OMNI_SWARM")

def compute_pca_residual_alpha(price_matrix: np.ndarray) -> np.ndarray:
    """
    🚀 V37.0 APEX: PCA Eigenvector Beta-Stripping
    Computes the top Principal Component (PC1) representing the global market beta,
    and returns pure idiosyncratic alpha residuals for each asset.
    """
    if price_matrix.shape[0] < 2 or price_matrix.shape[1] < 10:
        return np.zeros(price_matrix.shape[0])
        
    stds = np.std(price_matrix, axis=1, keepdims=True) + 1e-9
    norm_matrix = (price_matrix - np.mean(price_matrix, axis=1, keepdims=True)) / stds
    
    try:
        U, S, Vt = np.linalg.svd(norm_matrix, full_matrices=False)
        market_factor = Vt[0, :] 
        
        residuals = []
        for i in range(norm_matrix.shape[0]):
            beta = np.dot(norm_matrix[i, :], market_factor) / (np.dot(market_factor, market_factor) + 1e-9)
            residual = norm_matrix[i, -1] - (beta * market_factor[-1])
            residuals.append(residual)
            
        return np.array(residuals) * 10000.0  
    except Exception as e:
        logger.debug(f"PCA SVD Computation failed: {e}")
        return np.zeros(price_matrix.shape[0])


class GlobalOmniScanner:
    """
    🌌 V37.0 OMNI-SWARM CROSS-SECTIONAL SCANNER
    Scans Bybit 250+ perpetual universe every 10 seconds.
    Upgraded with Logarithmic Liquidity Weighting to filter out "Junk Alpha".
    Enforces a strict 30-minute swap cooldown to preserve matrix stability.
    """
    def __init__(self, executor):
        self.executor = executor
        self.market_memory: Dict[str, Dict[str, list]] = {}
        self.btc_returns = []
        self.last_btc_price = 0.0
        self.last_swap_time = 0.0  # 🚀 V37.0: Global matrix churn prevention

    async def _fetch_global_tickers(self) -> dict:
        try:
            res = await self.executor.safe_call(self.executor.client.get_tickers, category="linear")
            return {item['symbol']: item for item in res.get("result", {}).get("list", []) if item['symbol'].endswith('USDT')}
        except Exception as e:
            logger.error(f"Global ticker fetch failed: {e}")
            return {}

    async def scan_and_rank_universe(
        self, 
        current_basket: List[str], 
        protected_symbols: Optional[Set[str]] = None
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Calculates RVOL and PCA Beta-Stripped Alpha using true log-returns. 
        Returns (Symbol_To_Drop, Symbol_To_Add) if a severe anomaly is detected.
        """
        if protected_symbols is None:
            protected_symbols = set()

        # 🚀 V37.0 FIX: Hard 30-Minute Cooldown on Swaps
        if time.time() - self.last_swap_time < 1800.0:
            return None, None

        tickers = await self._fetch_global_tickers()
        if not tickers: 
            return None, None

        scoring_matrix = []
        valid_symbols = []
        return_matrix_rows = []
        turnover_map = {}

        btc_data = tickers.get("BTCUSDT")
        if btc_data:
            current_btc_price = float(btc_data.get('lastPrice', 0))
            if self.last_btc_price > 0 and current_btc_price > 0:
                btc_ret = math.log(current_btc_price / self.last_btc_price)
                self.btc_returns.append(btc_ret)
                if len(self.btc_returns) > 100: 
                    self.btc_returns.pop(0)
            else:
                btc_ret = 0.0
            self.last_btc_price = current_btc_price

        for sym, data in tickers.items():
            try:
                current_price = float(data.get('lastPrice', 0))
                turnover24h = float(data.get('turnover24h', 0))
                
                # 🚀 V37.0 FIX: Raised baseline turnover to $25M to kill micro-cap noise
                if current_price < 0.05 or turnover24h < 25_000_000.0:
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
                
                if len(self.market_memory[sym]["vol"]) > 100:
                    self.market_memory[sym]["vol"].pop(0)
                    self.market_memory[sym]["returns"].pop(0)

                if len(self.market_memory[sym]["vol"]) >= 10:
                    valid_symbols.append(sym)
                    return_matrix_rows.append(self.market_memory[sym]["returns"][-10:])
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
                
                # 🚀 V37.0 FIX: Logarithmic Liquidity Weighting
                # Punishes assets with low real liquidity to prevent "Junk Alpha" traps
                turnover_weight = math.log10(max(turnover_map[sym], 1e-9)) / 10.0 
                
                swarm_score = ((rvol_z * 0.6) + (abs(idiosyncratic_alpha) * 0.4)) * turnover_weight
                scoring_matrix.append((swarm_score, sym, rvol_z))
            except Exception:
                continue

        scoring_matrix.sort(key=lambda x: x[0], reverse=True)

        if not scoring_matrix: 
            return None, None

        top_score, top_sym, top_z = scoring_matrix[0]
        
        if top_sym not in current_basket and top_z > 3.0 and top_score > 2500.0:
            basket_scores = [
                item for item in scoring_matrix 
                if item[1] in current_basket 
                and item[1] != "BTCUSDT" 
                and item[1] not in protected_symbols
            ]
            
            if basket_scores:
                deadest_score, deadest_sym, deadest_z = basket_scores[-1]
                
                if top_score > (deadest_score * 5.0):
                    logger.critical(
                        f"🌪️ OMNI-SWARM HOT-SWAP TRIGGERED: Dropping {deadest_sym} (Score: {deadest_score:.2f}) -> "
                        f"Injecting Pure-Alpha Asset {top_sym} (Score: {top_score:.2f} | RVOL-Z: {top_z:.1f})"
                    )
                    self.last_swap_time = time.time()  # Lock the matrix for 30 minutes
                    return deadest_sym, top_sym

        return None, None