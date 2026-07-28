"""
💎 V50.0 QUANTUM SWARM: PARALLELIZED UNIFIED API EXECUTOR
--------------------------------------------------------
Features Token-Bucket Rate Limiting, Thread-Isolated Dispatch, Smart Leverage Caching,
Titanium Ticker Filtering, Multi-Field Wallet Parsing, and X-Ray Telemetry.
"""

import os
import time
import math
import asyncio
import logging
import functools
import concurrent.futures
from typing import Dict, Any, List, Optional
from pybit.unified_trading import HTTP

logger = logging.getLogger("QUANT_CORE.EXECUTION")

class TokenBucketRateLimiter:
    """
    🚀 TOKEN-BUCKET RATE LIMITER
    Throttles outbound API calls to strictly respect Bybit's private endpoint limits
    and prevent HTTP 429 rate-limit bans.
    """
    def __init__(self, capacity: int = 10, fill_rate: float = 5.0):
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.fill_rate = float(fill_rate)  # Tokens regenerated per second
        self.last_fill_time = time.time()
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                now = time.time()
                elapsed = now - self.last_fill_time
                self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)
                self.last_fill_time = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
            await asyncio.sleep(0.05)


class BybitUnifiedExecutor:
    """
    🚀 V50.0 PARALLELIZED UNIFIED API EXECUTOR
    Thread-isolated wrapper for Pybit V5 with automated rate-limiting, leverage caching,
    secrets scrubbing, and titanium ticker filtering.
    """
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, max_workers: int = 8):
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        
        self.client = HTTP(
            testnet=testnet,
            api_key=api_key,
            api_secret=api_secret
        )
        
        self.rate_limiter = TokenBucketRateLimiter(capacity=10, fill_rate=5.0)
        self._api_thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, 
            thread_name_prefix="BybitIsolator"
        )
        self._leverage_cache: Dict[str, int] = {}

    async def _safe_api_call(self, func, *args, **kwargs) -> Any:
        """
        🛡️ UNIFIED API GATEWAY
        Enforces rate-limiting, thread isolation, automatic retries on server load,
        fail-fast on parameter faults (10002), and token/secret scrubbing.
        """
        await self.rate_limiter.acquire()
        loop = asyncio.get_running_loop()
        bound_func = functools.partial(func, *args, **kwargs)
        
        for attempt in range(3):
            try:
                response = await loop.run_in_executor(self._api_thread_pool, bound_func)
                ret_code = response.get("retCode") if isinstance(response, dict) else 0
                
                # Fail Fast on Parameter Error (10002)
                if ret_code == 10002:
                    error_msg = f"[X-RAY] ❌ 10002 Parameter Fault: {response.get('retMsg', 'Unknown')}. Failing fast."
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Backoff on server load or rate limits
                if ret_code in [10006, 10016]: 
                    logger.warning(f"[X-RAY] ⚠️ Bybit System Load/Rate Limit (Code: {ret_code}). Backing off...")
                    await asyncio.sleep(2.0)
                    continue
                
                return response
                
            except Exception as e:
                error_str = str(e)
                # Scrub API credentials from output logs
                if self.api_key and self.api_key in error_str:
                    error_str = error_str.replace(self.api_key, "********")
                if self.api_secret and self.api_secret in error_str:
                    error_str = error_str.replace(self.api_secret, "********")
                
                if "10002 Parameter Fault" in error_str:
                    raise ValueError(error_str)
                    
                if attempt == 2:
                    logger.error(f"[X-RAY] ❌ Bybit API call failed after 3 attempts: {error_str}")
                    raise Exception(error_str)
                await asyncio.sleep(1.0)

    async def safe_call(self, func, *args, **kwargs) -> Any:
        """Public async gateway for executing raw pybit methods safely."""
        return await self._safe_api_call(func, *args, **kwargs)

    async def get_wallet_balance_usdt(self) -> float:
        """
        🚀 V50.0 MULTI-FIELD WALLET BALANCE PARSER
        Fetches available margin balance from Unified Trading Account with multi-field
        fallbacks (`walletBalance`, `totalAvailableBalance`, `equity`) and 95% buffer.
        """
        try:
            response = await self._safe_api_call(
                self.client.get_wallet_balance,
                accountType="UNIFIED",
                coin="USDT"
            )
            
            account_data = response["result"]["list"][0]
            for coin_info in account_data.get("coin", []):
                if coin_info.get("coin") == "USDT":
                    # Multi-field fallback parsing for Unified Account formats
                    raw_balance_str = (
                        coin_info.get("walletBalance") or 
                        coin_info.get("availableToWithdraw") or 
                        coin_info.get("equity") or "0.0"
                    )
                    raw_balance = float(raw_balance_str)
                    
                    # 95% Buffer leaves 5% margin to absorb limit-order fee holds
                    return raw_balance * 0.95
                    
            return 0.0
        except Exception as e:
            logger.error(f"[X-RAY] Failed to fetch Bybit wallet balance metrics: {e}")
            return 0.0

    async def adjust_leverage(self, symbol: str, target_leverage: int) -> bool:
        """
        🚀 Smart Leverage Caching with Error 110043 & 110013 Guards.
        Only sends API updates when leverage differs from current exchange state.
        """
        try:
            if self._leverage_cache.get(symbol) == target_leverage:
                return True
                
            pos_info = await self._safe_api_call(
                self.client.get_positions,
                category="linear",
                symbol=symbol
            )
            
            positions = pos_info.get("result", {}).get("list", [])
            if positions:
                current_leverage = int(float(positions[0].get("leverage", 1)))
                self._leverage_cache[symbol] = current_leverage
                if current_leverage == target_leverage:
                    return True

            # Push state update to Bybit
            res = await self._safe_api_call(
                self.client.set_leverage,
                category="linear",
                symbol=symbol,
                buyLeverage=str(target_leverage),
                sellLeverage=str(target_leverage)
            )
            
            self._leverage_cache[symbol] = target_leverage
            logger.info(f"[X-RAY] ⚙️ AUTO-SCALED LEVERAGE: {symbol} is now set to {target_leverage}x")
            return True
            
        except Exception as e:
            error_msg = str(e)
            
            # Code 110043: Leverage not modified (already at target value)
            if "110043" in error_msg or "not modified" in error_msg.lower():
                self._leverage_cache[symbol] = target_leverage
                return True
                
            # Code 110013: Requested leverage exceeds symbol's max risk tier limit
            if "110013" in error_msg:
                try:
                    info = await self._safe_api_call(
                        self.client.get_instruments_info,
                        category="linear",
                        symbol=symbol
                    )
                    max_allowed = int(float(info["result"]["list"][0]["leverageFilter"]["maxLeverage"]))
                    logger.warning(f"[X-RAY] ⚠️ Risk Cap hit for {symbol}. Auto-clamping leverage from {target_leverage}x to {max_allowed}x.")
                    
                    await self._safe_api_call(
                        self.client.set_leverage,
                        category="linear",
                        symbol=symbol,
                        buyLeverage=str(max_allowed),
                        sellLeverage=str(max_allowed)
                    )
                    self._leverage_cache[symbol] = max_allowed
                    return True
                except Exception:
                    logger.error(f"[X-RAY] Leverage auto-clamping failed for {symbol}.")
                    return False
                    
            logger.error(f"[X-RAY] ❌ Failed to synchronize leverage matrix for {symbol}: {error_msg}")
            return False

    async def get_top_volatile_assets(self, limit: int = 18, min_turnover: float = 30_000_000.0) -> List[str]:
        """
        🚀 TITANIUM TICKER DISCOVERY RADAR
        Scrapes exchange tickers, applies Titanium Blocklist (rejecting stock perps/leveraged ETFs),
        filters for USDT perps with sufficient turnover, and ranks by volatility.
        """
        banned_keywords = ["SOXL", "SPCX", "SKHY", "SNDK", "BANK", "MUUSDT", "BEAT", "MSTR", "ESPUSDT", "DEXE", "PUMP", "EUL", "XAU", "XAG"]
        
        try:
            response = await self._safe_api_call(
                self.client.get_tickers,
                category="linear"
            )
            
            tickers = response.get("result", {}).get("list", [])
            valid_assets = []
            
            for t in tickers:
                symbol = t.get("symbol", "")
                if not symbol.endswith("USDT"): continue
                if any(b in symbol for b in banned_keywords): continue
                    
                turnover = float(t.get("turnover24h", 0.0) or 0.0)
                if turnover < min_turnover: continue
                    
                high = float(t.get("highPrice24h", 0.0) or 0.0)
                low = float(t.get("lowPrice24h", 0.0) or 0.0)
                last = float(t.get("lastPrice", 0.0) or 1.0)
                
                if last == 0 or low == 0: continue
                volatility = (high - low) / (last + 1e-9)
                
                valid_assets.append({
                    "symbol": symbol,
                    "volatility": volatility,
                    "turnover": turnover
                })
                
            valid_assets.sort(key=lambda x: x["volatility"], reverse=True)
            top_symbols = [asset["symbol"] for asset in valid_assets[:limit]]
            
            logger.info(f"[X-RAY] 📡 TITANIUM RADAR DISCOVERED {len(top_symbols)} VOLATILE ASSETS.")
            return top_symbols
            
        except Exception as e:
            logger.error(f"[X-RAY] ❌ Failed to fetch global market tickers: {e}")
            return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOGEUSDT"]