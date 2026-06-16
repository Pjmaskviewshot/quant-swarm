import asyncio
import logging
from typing import Dict, Any
from pybit.unified_trading import HTTP

logger = logging.getLogger("QUANT_CORE.EXECUTION")

class BybitUnifiedExecutor:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        # Instantiate the official V5 client
        self.client = HTTP(
            testnet=testnet,
            api_key=api_key,
            api_secret=api_secret
        )

    async def get_wallet_balance_usdt(self) -> float:
        """Fetches available margin balance from the Unified Trading Account."""
        try:
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
        Returns True if successful or if leverage is already set to target.
        """
        try:
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
            # to avoid blocking execution for redundant updates.
            if "110043" in error_msg or "not modified" in error_msg.lower():
                logger.debug(f"Leverage for {symbol} is already safely configured at {leverage}x.")
                return True
                
            logger.error(f"❌ Failed to synchronize leverage matrix for {symbol}: {error_msg}")
            return False

    async def get_top_volatile_assets(self, limit: int = 15, min_turnover: float = 50_000_000) -> list:
        """
        Fetches the global ticker list and filters for the highest volatility USDT pairs.
        Acts as the engine's autonomous global satellite radar.
        """
        try:
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
                timeInForce="IOC"
            )
            
            order_id = order_payload["result"].get("orderId", "UNKNOWN_ID")
            logger.critical(f"🚀 ORDER DISPATCHED SUCCESSFUL // ID: {order_id} | Side: {side} | Qty: {qty}")
            return order_id
            
        except Exception as e:
            logger.error(f"Order routing execution failed at exchange interface level: {e}")
            return ""