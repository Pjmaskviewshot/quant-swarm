import asyncio
import logging
import time
from typing import Dict, Any
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
        self.fill_rate = fill_rate  # Tokens regenerated per second
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
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        # Instantiate the official V5 client
        self.client = HTTP(
            testnet=testnet,
            api_key=api_key,
            api_secret=api_secret
        )
        
        # 🛡️ UPGRADE: Initialize the global API rate limiter (10 burst, 5 per sec sustained)
        self.rate_limiter = TokenBucketRateLimiter(capacity=10, fill_rate=5.0)

    async def get_wallet_balance_usdt(self) -> float:
        """Fetches available margin balance from the Unified Trading Account."""
        try:
            await self.rate_limiter.acquire()
            # Shift synchronous network call to a background thread pool
            response = await asyncio.to_thread(
                self.client.get_wallet_balance,
                accountType="UNIFIED",
                coin="USDT"
            )
            
            account_data = response["result"]["list"][0]
            for coin_info in account_data.get("coin", []):
                if coin_info.get("coin") == "USDT":
                    return float(coin_info.get("walletBalance", 0.0))
            return 0.0
        except Exception as e:
            logger.error(f"Failed to fetch Bybit wallet balance metrics: {e}")
            return 0.0

    async def adjust_leverage(self, symbol: str, leverage: int) -> bool:
        """
        Safely modifies isolated/cross leverage thresholds before order dispatch.
        Attempts to set leverage, gracefully clamping to exchange maximums if rejected.
        """
        try:
            await self.rate_limiter.acquire()
            await asyncio.to_thread(
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
                    # 🛑 CRITICAL FIX: Deterministic API Query instead of fragile Regex parsing
                    await self.rate_limiter.acquire()
                    info = await asyncio.to_thread(
                        self.client.get_instruments_info,
                        category="linear",
                        symbol=symbol
                    )
                    
                    # Safely extract exact maximum allowed leverage directly from the exchange specifications
                    max_allowed_str = info["result"]["list"][0]["leverageFilter"]["maxLeverage"]
                    max_allowed = int(float(max_allowed_str))
                    
                    logger.warning(f"⚠️ Exchange Risk Cap hit for {symbol}. Auto-clamping leverage from {leverage}x down to {max_allowed}x.")
                    
                    # Immediately retry the exchange request with the safely capped maximum
                    await self.rate_limiter.acquire()
                    await asyncio.to_thread(
                        self.client.set_leverage,
                        category="linear",
                        symbol=symbol,
                        buyLeverage=str(max_allowed),
                        sellLeverage=str(max_allowed)
                    )
                    return True
                except Exception as fallback_err:
                    logger.error(f"Leverage auto-clamping failed for {symbol}: {fallback_err}")
                    return False
                    
            logger.error(f"❌ Failed to synchronize leverage matrix for {symbol}: {error_msg}")
            return False

    async def get_top_volatile_assets(self, limit: int = 15, min_turnover: float = 50_000_000) -> list:
        """
        Fetches the global ticker list and filters for the highest volatility USDT pairs.
        Acts as the engine's autonomous global satellite radar.
        """
        try:
            await self.rate_limiter.acquire()
            # 1. Fetch 24-hour statistics for all tickers on the exchange via thread pool
            response = await asyncio.to_thread(
                self.client.get_tickers,
                category="linear"
            )
            
            tickers = response.get("result", {}).get("list", [])
            valid_assets = []
            
            for t in tickers:
                symbol = t.get("symbol", "")
                
                # 2. Safety Filter: Only track perpetual USDT instruments
                if not symbol.endswith("USDT"):
                    continue
                    
                # 3. Liquidity Guard: Filter out low-volume, high-risk assets
                turnover = float(t.get("turnover24h", 0))
                if turnover < min_turnover:
                    continue
                    
                high = float(t.get("highPrice24h", 0))
                low = float(t.get("lowPrice24h", 0))
                last = float(t.get("lastPrice", 1))
                
                if last == 0 or low == 0:
                    continue
                    
                # 4. Volatility Scaling Engine
                volatility = (high - low) / last
                
                valid_assets.append({
                    "symbol": symbol,
                    "volatility": volatility,
                    "turnover": turnover
                })
                
            # 5. Mathematical Matrix Sorting (Highest alpha velocity at top)
            valid_assets.sort(key=lambda x: x["volatility"], reverse=True)
            
            # Slice and isolate the target list
            top_symbols = [asset["symbol"] for asset in valid_assets[:limit]]
            return top_symbols
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch global market tickers: {e}")
            return []

    async def dispatch_market_order(self, symbol: str, direction: str, qty: float, tp: float, sl: float) -> str:
        """Signs and executes automated market orders with bracketed protection constraints."""
        side = "Buy" if direction == "BUY" else "Sell"
        
        try:
            await self.rate_limiter.acquire()
            # Order payload parameters matching string schemas for the V5 specification
            order_payload = await asyncio.to_thread(
                self.client.place_order,
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Market",
                qty=str(qty),
                takeProfit=str(tp),
                stopLoss=str(sl),
                tpslMode="Full",
                timeInForce="IOC",
                positionIdx=0  # Fixed: Resolves exchange rejection error 10001
            )
            
            order_id = order_payload["result"].get("orderId", "UNKNOWN_ID")
            logger.critical(f"🚀 ORDER DISPATCHED SUCCESSFUL // ID: {order_id} | Side: {side} | Qty: {qty}")
            return order_id
            
        except Exception as e:
            logger.error(f"Order routing execution failed at exchange interface level: {e}")
            return ""

    async def dispatch_limit_order(self, symbol: str, direction: str, qty: float, price: float, tp: float, sl: float) -> str:
        """
        🚀 PHASE 1 UPGRADE (INTEGRATION): 
        Signs and executes passive Post-Only Limit Orders for Mean Reversion regimes to capture the spread.
        """
        side = "Buy" if direction == "BUY" else "Sell"
        
        try:
            await self.rate_limiter.acquire()
            order_payload = await asyncio.to_thread(
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
            
        except Exception as e:
            logger.error(f"Limit order routing execution failed at exchange interface level: {e}")
            return ""

    async def check_recent_settlement(self, symbol: str, lookback_seconds: int = 60) -> Dict[str, Any]:
        """
        Queries the exchange's closed PnL ledger to see if a bracket order executed.
        Returns formatted trade metrics if a trade closed within the lookback window.
        """
        try:
            await self.rate_limiter.acquire()
            response = await asyncio.to_thread(
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
            
        except Exception as e:
            logger.error(f"Failed to pull closed PnL metrics for {symbol}: {e}")
            return {"closed": False}