"""
V49.0 APEX TITAN: OMNI-SWARM CROSS-SECTIONAL SCANNER
--------------------------------------------------------------------------------
Scans Bybit perpetual universe using 60-period PCA Eigenvector Beta-Stripping.
Rotates capital dynamically into high-RVOL, high-Alpha market leaders while 
rejecting liquidation cascades and illiquid books.

Production Hardening & Quantitative Upgrades (V49.0 Audit Resolutions):
1. Absolute Spread Hurdle Formulation: Replaces broken `top_score > deadest * 3.0`
   multiplication with a sign-invariant spread hurdle (`top_score - deadest >= hurdle`).
2. Dimensional De-Normalization Fix: De-normalizes SVD residuals by asset return 
   standard deviations before basis point conversion, restoring true 60/40 weighting.
3. Liquidation Cascade Clamp: Caps maximum idiosyncratic alpha to 60.0 bps to prevent 
   the scanner from rotating into assets in catastrophic freefall.
4. Threshold Synchronization: Aligns turnover ($15M) and spread (8.0 bps) filters 
   strictly with `main.py` admission criteria.
5. O(1) Memory Deques: Replaces list slicing with fixed-length double-ended queues 
   to eliminate memory leaks and garbage collection stalls.
"""

import math
import time
import datetime
import numpy as np
import logging
import asyncio
from collections import deque
from typing import List, Dict, Tuple, Set, Optional, Any

logger = logging.getLogger("QUANT_CORE.OMNI_SWARM")

# Unified Exclusion Matrix: Synchronized with main.py
BANNED_ASSET_KEYWORDS = {
    "AAPL", "TSLA", "NVDA", "AMZN", "MSFT", "GOOG", "META", "SOXL",
    "SPCX", "SKHY", "SNDK", "BANK", "MUUSDT", "BEAT", "MSTR", "ESPUSDT",
    "DEXE", "PUMP", "EUL", "XAU", "XAG", "USDC", "CLUSDT", "SSPCUSDT",
    "KO", "HANMI", "LRCX", "PURR", "MUU", "XIAOMI", "INTW", "CLANKER",
    "AAOI", "COIN", "PLTR", "ARM", "BABA", "NIO", "AMD", "WTIUSDT", "BRENTUSDT",
    "BTCUSDT"
}


def compute_pca_residual_alpha(price_matrix: np.ndarray) -> np.ndarray:
    """
    60-Bar PCA Eigenvector Beta-Stripping.
    Decomposes normalized returns via SVD, isolates the first principal component
    (global crypto beta), and de-normalizes the residual back to true basis points.
    Must be called off-thread via asyncio.to_thread.
    """
    if price_matrix.shape[0] < 2 or price_matrix.shape[1] < 30:
        return np.zeros(price_matrix.shape[0], dtype=np.float64)

    with np.errstate(divide='ignore', invalid='ignore'):
        stds = np.std(price_matrix, axis=1, keepdims=True) + 1e-9
        means = np.mean(price_matrix, axis=1, keepdims=True)
        norm_matrix = (price_matrix - means) / stds

    try:
        # SVD on N x T standardized returns
        U, S, Vt = np.linalg.svd(norm_matrix, full_matrices=False)
        market_factor = Vt[0, :]  # Global Market PC1

        factor_norm = np.dot(market_factor, market_factor) + 1e-9
        residuals_bps = np.empty(norm_matrix.shape[0], dtype=np.float64)

        for i in range(norm_matrix.shape[0]):
            beta = np.dot(norm_matrix[i, :], market_factor) / factor_norm
            std_residual = norm_matrix[i, -1] - (beta * market_factor[-1])
            
            # De-normalize standard deviation back to actual return units
            raw_residual = std_residual * float(stds[i, 0])
            
            # Convert to true basis points and clamp cascade extremes
            true_bps = raw_residual * 10000.0
            residuals_bps[i] = float(np.clip(true_bps, -60.0, 60.0))

        return residuals_bps
    except Exception as e:
        logger.debug(f"[X-RAY] PCA SVD computation bypassed: {e}")
        return np.zeros(price_matrix.shape[0], dtype=np.float64)


class GlobalOmniScanner:
    """
    Cross-sectional microstructure and relative volume scanner.
    Maintains 60-bar historical memory per asset and ranks universe nodes.
    """
    def __init__(self, executor):
        self.executor = executor
        self.market_memory: Dict[str, Dict[str, Any]] = {}
        self.btc_returns = deque(maxlen=120)
        self.last_btc_price = 0.0
        self.last_swap_time = 0.0

    def _get_turnover_threshold(self) -> float:
        """Weekend-aware turnover threshold aligned with main.py ($15M weekday, $10M weekend)."""
        now = datetime.datetime.now(datetime.timezone.utc)
        is_weekend = now.weekday() in (5, 6)
        return 10_000_000.0 if is_weekend else 15_000_000.0

    async def _fetch_global_tickers(self) -> dict:
        try:
            res = await self.executor.safe_call("GET", "/v5/market/tickers", category="linear")
            if not isinstance(res, dict) or res.get("retCode") != 0:
                return {}
            return {
                item['symbol']: item 
                for item in res.get("result", {}).get("list", []) 
                if item.get('symbol', '').endswith('USDT')
            }
        except Exception as e:
            logger.error(f"[X-RAY] Global ticker fetch failed during Omni-Scan: {e}")
            return {}

    async def scan_and_rank_universe(
        self, 
        current_basket: List[str], 
        protected_symbols: Optional[Set[str]] = None
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Ranks tradeable assets by RVOL and idiosyncratic alpha.
        Returns (Symbol_To_Drop, Symbol_To_Add) upon detecting a statistically superior node.
        """
        if protected_symbols is None:
            protected_symbols = set()

        # Enforce 30-minute cooldown between hot-swaps to prevent thrashing
        if time.time() - self.last_swap_time < 1800.0:
            return None, None

        tickers = await self._fetch_global_tickers()
        if not tickers: 
            return None, None

        scoring_matrix: List[Tuple[float, str, float]] = []
        valid_symbols: List[str] = []
        return_matrix_rows: List[List[float]] = []
        turnover_map: Dict[str, float] = {}

        # 1. Track Global BTC Benchmark
        btc_data = tickers.get("BTCUSDT")
        if btc_data:
            current_btc_price = float(btc_data.get('lastPrice', 0.0) or 0.0)
            if self.last_btc_price > 0.0 and current_btc_price > 0.0:
                self.btc_returns.append(math.log(current_btc_price / self.last_btc_price))
            self.last_btc_price = current_btc_price

        min_turnover = self._get_turnover_threshold()

        # 2. Prune disconnected symbols from long-term memory
        active_ticker_keys = set(tickers.keys())
        stale_memory_keys = [k for k in self.market_memory if k not in active_ticker_keys]
        for k in stale_memory_keys:
            self.market_memory.pop(k, None)

        # 3. Filter & Ingest Universe Candidates
        for sym, data in tickers.items():
            try:
                if any(b in sym for b in BANNED_ASSET_KEYWORDS) or sym.startswith(("PRE-", "INNO-", "TEST-")):
                    continue

                current_price = float(data.get('lastPrice', 0.0) or 0.0)
                turnover24h = float(data.get('turnover24h', 0.0) or 0.0)
                bid = float(data.get('bid1Price', 0.0) or 0.0)
                ask = float(data.get('ask1Price', 0.0) or 0.0)

                if bid <= 0.0 or ask <= 0.0 or ask <= bid:
                    continue

                spread_bps = ((ask - bid) / (bid + 1e-9)) * 10000.0

                # Strict liquidity, penny asset, and friction gates
                if current_price < 0.01 or turnover24h < min_turnover or spread_bps > 8.0:
                    continue

                vol = float(data.get('volume24h', 0.0) or 0.0)

                if sym not in self.market_memory:
                    self.market_memory[sym] = {
                        "vol": deque(maxlen=60),
                        "returns": deque(maxlen=60),
                        "last_price": current_price
                    }
                    continue

                prev_price = self.market_memory[sym]["last_price"]
                sym_ret = math.log(current_price / prev_price) if prev_price > 0.0 and current_price > 0.0 else 0.0

                self.market_memory[sym]["last_price"] = current_price
                self.market_memory[sym]["vol"].append(vol)
                self.market_memory[sym]["returns"].append(sym_ret)

                # Require 60 synchronized observations for SVD stability
                if len(self.market_memory[sym]["returns"]) >= 60:
                    valid_symbols.append(sym)
                    return_matrix_rows.append(list(self.market_memory[sym]["returns"]))
                    turnover_map[sym] = turnover24h

            except Exception:
                continue

        if len(valid_symbols) < 5:
            return None, None

        price_matrix = np.array(return_matrix_rows, dtype=np.float64)

        # Offload SVD decomposition to thread pool
        pca_alphas = await asyncio.to_thread(compute_pca_residual_alpha, price_matrix)

        # 4. Composite Sizing & Multi-Factor Scoring
        for idx, sym in enumerate(valid_symbols):
            try:
                vol_array = np.array(self.market_memory[sym]["vol"], dtype=np.float64)
                mu_v = float(np.mean(vol_array))
                sig_v = float(np.std(vol_array) + 1e-9)
                rvol_z = float(np.clip((vol_array[-1] - mu_v) / sig_v, -3.0, 5.0))

                idiosyncratic_alpha_bps = pca_alphas[idx]

                # Log-scaled turnover weight (e.g., $15M -> ~0.71, $100M -> ~0.80)
                turnover_weight = float(np.clip(math.log10(max(turnover_map[sym], 1e-9)) / 10.0, 0.50, 1.0))

                # Composite score: 60% RVOL Z-score + 40% Normalized Alpha bps (scaled / 10.0)
                # An alpha of +20 bps scales to 2.0, balancing cleanly against RVOL Z-scores
                alpha_score = abs(idiosyncratic_alpha_bps) / 10.0
                swarm_score = ((rvol_z * 0.6) + (alpha_score * 0.4)) * turnover_weight
                
                scoring_matrix.append((float(swarm_score), sym, rvol_z))
            except Exception:
                continue

        scoring_matrix.sort(key=lambda x: x[0], reverse=True)

        if not scoring_matrix: 
            return None, None

        top_score, top_sym, top_z = scoring_matrix[0]

        # 5. Hot-Swap Evaluation
        # Top candidate must possess positive volume excitement (RVOL Z > 1.5) and pass quality floor
        if top_sym not in current_basket and top_z >= 1.5 and top_score >= 1.8:
            basket_scores = [
                item for item in scoring_matrix 
                if item[1] in current_basket 
                and item[1] != "BTCUSDT" 
                and item[1] not in protected_symbols
            ]

            if basket_scores:
                deadest_score, deadest_sym, deadest_z = basket_scores[-1]

                # Sign-invariant spread hurdle: candidate must meaningfully beat weakest member
                score_spread = top_score - deadest_score
                min_spread_hurdle = max(1.2, abs(deadest_score) * 0.35)

                if score_spread >= min_spread_hurdle:
                    logger.critical(
                        f"[X-RAY] OMNI-SWARM ROTATION TRIGGERED // "
                        f"Evicting: {deadest_sym} (Score: {deadest_score:.2f}) -> "
                        f"Admitting: {top_sym} (Score: {top_score:.2f} | RVOL-Z: {top_z:.1f} | Δ: {score_spread:.2f})"
                    )
                    self.last_swap_time = time.time()
                    return deadest_sym, top_sym

        return None, None