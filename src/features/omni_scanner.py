import math
import time
import numpy as np
import logging
from typing import List, Dict, Tuple
import asyncio

logger = logging.getLogger("QUANT_CORE.OMNI_SWARM")

class GlobalOmniScanner:
    """
    🌌 V33.1 OMNI-SWARM CROSS-SECTIONAL SCANNER (TIER-1 ONLY)
    Scans the Bybit 250+ perpetual universe every 10 seconds.
    Isolates Idiosyncratic Alpha (true price returns) while strictly 
    banning illiquid micro-caps to prevent spread-bleed on small accounts.
    """
    def __init__(self, executor):
        self.executor = executor
        self.market_memory: Dict[str, Dict[str, list]] = {}
        self.btc_returns = []
        self.last_btc_price = 0.0

    async def _fetch_global_tickers(self) -> dict:
        try:
            # Query the entire universe in a single API call
            res = await self.executor.safe_call(self.executor.client.get_tickers, category="linear")
            return {item['symbol']: item for item in res.get("result", {}).get("list", []) if item['symbol'].endswith('USDT')}
        except Exception as e:
            logger.error(f"Global ticker fetch failed: {e}")
            return {}

    async def scan_and_rank_universe(self, current_basket: List[str]) -> Tuple[str, str]:
        """
        Calculates RVOL and Beta-Stripped Alpha using true log-returns. 
        Returns (Symbol_To_Drop, Symbol_To_Add) if a severe anomaly is detected.
        """
        tickers = await self._fetch_global_tickers()
        if not tickers: return None, None

        current_time = time.time()
        scoring_matrix = []

        # 1. Update Memory & Extract TRUE BTC Log Returns
        btc_data = tickers.get("BTCUSDT")
        if btc_data:
            current_btc_price = float(btc_data.get('lastPrice', 0))
            if self.last_btc_price > 0 and current_btc_price > 0:
                btc_ret = math.log(current_btc_price / self.last_btc_price)
                self.btc_returns.append(btc_ret)
                if len(self.btc_returns) > 100: self.btc_returns.pop(0)
            else:
                btc_ret = 0.0
            self.last_btc_price = current_btc_price

        for sym, data in tickers.items():
            try:
                current_price = float(data.get('lastPrice', 0))
                turnover24h = float(data.get('turnover24h', 0))
                
                # 🚀 V33.1 FIX: LIQUIDITY GATE (Ban Micro-Caps)
                # Ban assets priced under $0.50 OR with less than $50M 24h turnover
                # This prevents the bot from trading garbage like ZAMAUSDT or HOMEUSDT
                if current_price < 0.50 or turnover24h < 50_000_000.0:
                    continue

                vol = float(data.get('volume24h', 0))
                
                if sym not in self.market_memory:
                    self.market_memory[sym] = {"vol": [], "returns": [], "last_price": current_price}
                    continue # Need a previous price to calculate first return
                    
                # Calculate True Log Return for this Altcoin
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

                # Need at least 10 samples to calculate standard deviation
                if len(self.market_memory[sym]["vol"]) < 10:
                    continue

                # 2. Calculate Cross-Sectional RVOL Z-Score
                vol_array = np.array(self.market_memory[sym]["vol"])
                mu_v = np.mean(vol_array)
                sig_v = np.std(vol_array) + 1e-9
                rvol_z = (vol - mu_v) / sig_v

                # 3. True Idiosyncratic Alpha Approximation (Beta-Stripped Price Returns)
                if len(self.btc_returns) > 10 and len(self.market_memory[sym]["returns"]) > 10:
                    # Match array lengths
                    min_len = min(len(self.btc_returns), len(self.market_memory[sym]["returns"]))
                    btc_arr = np.array(self.btc_returns[-min_len:])
                    sym_arr = np.array(self.market_memory[sym]["returns"][-min_len:])
                    
                    # Calculate true price covariance to find Beta
                    covariance = np.cov(btc_arr, sym_arr)[0][1]
                    btc_variance = np.var(btc_arr) + 1e-9
                    beta = covariance / btc_variance
                    
                    # Strip BTC's gravity from the altcoin's current return
                    expected_return = beta * btc_ret
                    idiosyncratic_alpha = sym_ret - expected_return
                    
                    # Scale alpha up so it has comparable weighting to RVOL Z-score
                    scaled_alpha = idiosyncratic_alpha * 10000.0 
                else:
                    scaled_alpha = 0.0

                # Calculate final Swarm Score
                swarm_score = (rvol_z * 0.6) + (abs(scaled_alpha) * 0.4)
                scoring_matrix.append((swarm_score, sym, rvol_z))

            except Exception:
                continue

        # Sort matrix highest score to lowest
        scoring_matrix.sort(key=lambda x: x[0], reverse=True)

        if not scoring_matrix: return None, None

        # 4. Identify Hot-Swap Candidates
        top_score, top_sym, top_z = scoring_matrix[0]
        
        # If the most explosive coin isn't in our current socket basket
        if top_sym not in current_basket and top_z > 3.0:
            
            # Find the absolute "deadest" coin currently in our basket to drop
            basket_scores = [item for item in scoring_matrix if item[1] in current_basket and item[1] != "BTCUSDT"]
            if basket_scores:
                deadest_score, deadest_sym, deadest_z = basket_scores[-1]
                
                # Only swap if the Alpha differential is massive
                if top_score > (deadest_score * 3.0):
                    logger.critical(f"🌪️ OMNI-SWARM HOT-SWAP TRIGGERED: Dropping {deadest_sym} (Score: {deadest_score:.2f}) -> Injecting Tier-1 Asset {top_sym} (Score: {top_score:.2f} | RVOL-Z: {top_z:.1f})")
                    return deadest_sym, top_sym

        return None, None