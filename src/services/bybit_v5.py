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
    🚀 V26.1 APEX: PARALLELIZED UNIFIED API EXECUTOR
    Upgraded with an expanded 8-worker thread pool to eliminate thread-serialization 
    bottlenecks across multi-asset swarm deployments, paired with strict token-bucket rate limiting.
    Includes strict Fail-Fast logic for malformed 10002 responses to prevent 
    Token-Bucket limits from being burned on guaranteed-fail requests.
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
        
        # 🚀 V26 UPGRADE: Expanded Multi-Thread Pool (Fixes Thread Serialization Bottleneck)
        # Prevents concurrent requests across different asset nodes from blocking each other in single-file queues
        self._api_thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="BybitIsolator")

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
                
                # 🚀 V26.1 FIX: Fail Fast on Parameter Error (10002). Never retry malformed requests.
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
                # Scrub API key and Secret from any network errors before standard output
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
                    return float(coin_info.get("walletBalance", 0.0))
            return 0.0
        except Exception:
            logger.error(f"Failed to fetch Bybit wallet balance metrics.")
            return 0.0

    async def adjust_leverage(self, symbol: str, leverage: int) -> bool:
        """
        Safely modifies isolated/cross leverage thresholds before order dispatch.
        Attempts to set leverage, gracefully clamping to exchange maximums if rejected.
        """
        try:
            await self._safe_api_call(
                self.client.set_leverage,
                category="linear",
                symbol=symbol,
                buyLeverage=str(leverage),
                sellLeverage=str(leverage)
            )
            logger.info(f"⚙️ AUTO-SCALED LEVERAGE: {symbol} is now set to {leverage}x")
            return True
            
        except Exception as e:
            error_msg = str(e)
            
            # Capture Bybit API error code 110043 ('Leverage not modified') 
            if "110043" in error_msg or "not modified" in error_msg.lower():
                logger.debug(f"Leverage for {symbol} is already safely configured at {leverage}x.")
                return True
                
            # ErrCode 110013: Requested leverage exceeds Bybit's hard risk limit for this specific altcoin
            if "110013" in error_msg:
                try:
                    # Deterministic API Query
                    info = await self._safe_api_call(
                        self.client.get_instruments_info,
                        category="linear",
                        symbol=symbol
                    )
                    
                    # Safely extract exact maximum allowed leverage directly from the exchange specifications
                    max_allowed_str = info["result"]["list"][0]["leverageFilter"]["maxLeverage"]
                    max_allowed = int(float(max_allowed_str))
                    
                    logger.warning(f"⚠️ Exchange Risk Cap hit for {symbol}. Auto-clamping leverage from {leverage}x down to {max_allowed}x.")
                    
                    # Immediately retry the exchange request with the safely capped maximum
                    await self._safe_api_call(
                        self.client.set_leverage,
                        category="linear",
                        symbol=symbol,
                        buyLeverage=str(max_allowed),
                        sellLeverage=str(max_allowed)
                    )
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

    async def dispatch_market_order(self, symbol: str, direction: str, qty: float, tp: float, sl: float) -> str:
        """Signs and executes automated market orders with bracketed protection constraints."""
        side = "Buy" if direction == "BUY" else "Sell"
        
        try:
            order_payload = await self._safe_api_call(
                self.client.place_order,
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Market",
                qty=str(qty),
                takeProfit=str(tp),
                stopLoss=str(sl),
                tpslMode="Full",
                positionIdx=0  # Resolves exchange rejection error 10001
            )
            
            order_id = order_payload["result"].get("orderId", "UNKNOWN_ID")
            logger.critical(f"🚀 ORDER DISPATCHED SUCCESSFUL // ID: {order_id} | Side: {side} | Qty: {qty}")
            return order_id
            
        except Exception:
            logger.error(f"Order routing execution failed at exchange interface level.")
            return ""

    async def dispatch_limit_order(self, symbol: str, direction: str, qty: float, price: float, tp: float, sl: float) -> str:
        """
        Signs and executes passive Post-Only Limit Orders for Mean Reversion regimes to capture the spread.
        """
        side = "Buy" if direction == "BUY" else "Sell"
        
        try:
            order_payload = await self._safe_api_call(
                self.client.place_order,
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Limit",
                qty=str(qty),
                price=str(price),
                takeProfit=str(tp),
                stopLoss=str(sl),
                tpslMode="Full",
                timeInForce="PostOnly", # Forces the order to be a maker, avoiding taker fees
                positionIdx=0
            )
            
            order_id = order_payload["result"].get("orderId", "UNKNOWN_ID")
            logger.critical(f"🕸️ PASSIVE LIMIT NET DEPLOYED // ID: {order_id} | Side: {side} | Qty: {qty} @ {price}")
            return order_id
            
        except Exception:
            logger.error(f"Limit order routing execution failed at exchange interface level.")
            return ""

    async def check_recent_settlement(self, symbol: str, lookback_seconds: int = 60) -> Dict[str, Any]:
        """
        Queries the exchange's closed PnL ledger to see if a bracket order executed.
        Returns formatted trade metrics if a trade closed within the lookback window.
        """
        try:
            response = await self._safe_api_call(
                self.client.get_closed_pnl,
                category="linear",
                symbol=symbol,
                limit=1
            )
            
            pnl_list = response.get("result", {}).get("list", [])
            if not pnl_list:
                return {"closed": False}
                
            latest_trade = pnl_list[0]
            
            # Convert exchange millisecond timestamp to seconds
            updated_time = int(latest_trade.get("updatedTime", 0)) / 1000
            current_time = time.time()
            
            # Verify if this trade closure happened recently
            if (current_time - updated_time) <= lookback_seconds:
                pnl = float(latest_trade.get("closedPnl", 0.0))
                side = latest_trade.get("side", "UNKNOWN")
                qty = float(latest_trade.get("qty", 0.0))
                entry_price = float(latest_trade.get("avgEntryPrice", 0.0))
                exit_price = float(latest_trade.get("avgExitPrice", 0.0))
                
                outcome = "🟢 PROFIT" if pnl > 0 else "🔴 LOSS"
                
                return {
                    "closed": True,
                    "symbol": symbol,
                    "outcome": outcome,
                    "pnl": round(pnl, 4),
                    "side": side,
                    "qty": qty,
                    "entry": entry_price,
                    "exit": exit_price
                }
                
            return {"closed": False}
            
        except Exception:
            logger.error(f"Failed to pull closed PnL metrics for {symbol}.")
            return {"closed": False}