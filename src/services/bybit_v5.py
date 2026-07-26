import os
import asyncio
import logging
import time
import concurrent.futures
import functools
from typing import Dict, Any, List
from pybit.unified_trading import HTTP

logger = logging.getLogger("QUANT_CORE.EXECUTION")

class TokenBucketRateLimiter:
    """
    🚀 PHASE 4 ADVANCEMENT: TOKEN-BUCKET RATE LIMITER
    Prevents HTTP 429 Too Many Requests bans by actively throttling outbound API calls 
    to strictly respect the exchange's private endpoint throughput limits.
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
                # Regenerate tokens based on time elapsed
                self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)
                self.last_fill_time = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
            # Backoff sleep to prevent CPU spinning while waiting for token regeneration
            await asyncio.sleep(0.05)


class BybitUnifiedExecutor:
    """
    🚀 V34.3 APEX: PARALLELIZED UNIFIED API EXECUTOR
    Upgraded with strict Leverage Caching and a 95% Sizing Buffer to eliminate 
    API rejections (110043 & 110007) and guarantee Maker-Peg order routing.
    Legacy dead code removed to prevent position index conflicts with SOR.
    """
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, max_workers: int = 8):
        # Store keys for error-scrubbing purposes
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        
        # Instantiate the official V5 client
        self.client = HTTP(
            testnet=testnet,
            api_key=api_key,
            api_secret=api_secret
        )
        
        # 🛡️ Initialize the global API rate limiter (10 burst, 5 per sec sustained)
        self.rate_limiter = TokenBucketRateLimiter(capacity=10, fill_rate=5.0)
        
        # 🚀 V26 UPGRADE: Expanded Multi-Thread Pool
        self._api_thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="BybitIsolator")
        
        # 🚀 V34.3 UPGRADE: Local Leverage Cache prevents 110043 API Rejections
        self._leverage_cache: Dict[str, int] = {}

    async def _safe_api_call(self, func, *args, **kwargs) -> Any:
        """
        🛡️ UNIFIED API GATEWAY
        All exchange interactions pass through here to ensure rate-limiting, 
        automatic retries on system load errors, thread isolation, fail-fast on parameter errors, and credential scrubbing.
        """
        await self.rate_limiter.acquire()
        loop = asyncio.get_running_loop()
        
        # Thread-safe kwarg dispatch
        bound_func = functools.partial(func, *args, **kwargs)
        
        for attempt in range(3):
            try:
                # Execute the synchronous pybit function inside the parallelized thread pool
                response = await loop.run_in_executor(self._api_thread_pool, bound_func)
                
                ret_code = response.get("retCode") if isinstance(response, dict) else 0
                
                # Fail Fast on Parameter Error (10002). Never retry malformed requests.
                if ret_code == 10002:
                    error_msg = f"❌ 10002 Parameter Fault: {response.get('retMsg', 'Unknown')}. Failing fast."
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Check for Bybit-specific system load/rate limit codes natively
                if ret_code in [10006, 10016]: 
                    logger.warning(f"⚠️ Bybit System Load/Rate Limit (Code: {ret_code}). Backoff...")
                    await asyncio.sleep(2.0)
                    continue
                
                return response
                
            except Exception as e:
                # 🛑 SECRETS HYGIENE
                error_str = str(e)
                if self.api_key and self.api_key in error_str:
                    error_str = error_str.replace(self.api_key, "********")
                if self.api_secret and self.api_secret in error_str:
                    error_str = error_str.replace(self.api_secret, "********")
                
                # Immediately raise if it was a forced fail-fast on 10002
                if "10002 Parameter Fault" in error_str:
                    raise ValueError(error_str)
                    
                if attempt == 2:
                    logger.error(f"❌ Bybit API call failed after 3 attempts: {error_str}")
                    raise Exception(error_str)
                await asyncio.sleep(1.0)

    async def safe_call(self, func, *args, **kwargs) -> Any:
        """🚀 Public async wrapper allowing external modules to safely dispatch raw client calls"""
        return await self._safe_api_call(func, *args, **kwargs)

    async def get_wallet_balance_usdt(self) -> float:
        """Fetches available margin balance from the Unified Trading Account."""
        try:
            response = await self._safe_api_call(
                self.client.get_wallet_balance,
                accountType="UNIFIED",
                coin="USDT"
            )
            
            account_data = response["result"]["list"][0]
            for coin_info in account_data.get("coin", []):
                if coin_info.get("coin") == "USDT":
                    # 🚀 V34.3 FIX: 95% Sizing Buffer
                    # Leaves 5% free margin locally to absorb limit-order fee holds
                    # and prevent 'ab not enough for new order' (110007) errors.
                    raw_balance = float(coin_info.get("walletBalance", 0.0))
                    return raw_balance * 0.95
            return 0.0
        except Exception:
            logger.error(f"Failed to fetch Bybit wallet balance metrics.")
            return 0.0

    async def adjust_leverage(self, symbol: str, target_leverage: int) -> bool:
        """
        🚀 V34.3: Smart Leverage Caching
        Only pushes an API update if the symbol's current leverage on Bybit 
        is different from the target. Saves API calls & latency.
        """
        try:
            # Check local cache first
            if self._leverage_cache.get(symbol) == target_leverage:
                return True
                
            # If not in cache, verify actual state on exchange to prevent 110043 error
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
                    logger.debug(f"Leverage for {symbol} is already perfectly set at {target_leverage}x.")
                    return True

            # State is out of sync. Push the update.
            await self._safe_api_call(
                self.client.set_leverage,
                category="linear",
                symbol=symbol,
                buyLeverage=str(target_leverage),
                sellLeverage=str(target_leverage)
            )
            
            self._leverage_cache[symbol] = target_leverage
            logger.info(f"⚙️ AUTO-SCALED LEVERAGE: {symbol} is now set to {target_leverage}x")
            return True
            
        except Exception as e:
            error_msg = str(e)
            
            # ErrCode 110013: Requested leverage exceeds Bybit's hard risk limit for this specific altcoin
            if "110013" in error_msg:
                try:
                    info = await self._safe_api_call(
                        self.client.get_instruments_info,
                        category="linear",
                        symbol=symbol
                    )
                    
                    max_allowed_str = info["result"]["list"][0]["leverageFilter"]["maxLeverage"]
                    max_allowed = int(float(max_allowed_str))
                    
                    logger.warning(f"⚠️ Exchange Risk Cap hit for {symbol}. Auto-clamping leverage from {target_leverage}x down to {max_allowed}x.")
                    
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
                    logger.error(f"Leverage auto-clamping failed for {symbol}.")
                    return False
                    
            logger.error(f"❌ Failed to synchronize leverage matrix for {symbol}.")
            return False

    async def get_top_volatile_assets(self, limit: int = 15, min_turnover: float = 50_000_000) -> list:
        """
        Fetches the global ticker list and filters for the highest volatility USDT pairs.
        Acts as the engine's autonomous global satellite radar.
        """
        try:
            response = await self._safe_api_call(
                self.client.get_tickers,
                category="linear"
            )
            
            tickers = response.get("result", {}).get("list", [])
            valid_assets = []
            
            for t in tickers:
                symbol = t.get("symbol", "")
                
                # Safety Filter: Only track perpetual USDT instruments
                if not symbol.endswith("USDT"):
                    continue
                    
                # Liquidity Guard: Filter out low-volume, high-risk assets
                turnover = float(t.get("turnover24h", 0))
                if turnover < min_turnover:
                    continue
                    
                high = float(t.get("highPrice24h", 0))
                low = float(t.get("lowPrice24h", 0))
                last = float(t.get("lastPrice", 1))
                
                if last == 0 or low == 0:
                    continue
                    
                # ⚡ V26 UPGRADE: Volatility Scaling Engine with Epsilon Guard
                volatility = (high - low) / (last + 1e-9)
                
                valid_assets.append({
                    "symbol": symbol,
                    "volatility": volatility,
                    "turnover": turnover
                })
                
            # Mathematical Matrix Sorting (Highest alpha velocity at top)
            valid_assets.sort(key=lambda x: x["volatility"], reverse=True)
            
            # Slice and isolate the target list
            top_symbols = [asset["symbol"] for asset in valid_assets[:limit]]
            return top_symbols
            
        except Exception:
            logger.error(f"❌ Failed to fetch global market tickers.")
            return []