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

    async def adjust_leverage(self, symbol: str, leverage: int):
        """Safely modifies isolated/cross leverage thresholds before order dispatch."""
        try:
            await asyncio.to_thread(
                self.client.set_leverage,
                category="linear",
                symbol=symbol,
                buyLeverage=str(leverage),
                sellLeverage=str(leverage)
            )
            logger.info(f"Leverage configuration successfully synchronized to {leverage}x for {symbol}.")
        except Exception as e:
            # Capture and suppress 'Leverage not modified' API error codes (110043)
            if "not modified" not in str(e):
                logger.error(f"Failed to synchronize leverage matrix: {e}")

    async def dispatch_market_order(self, symbol: str, direction: str, qty: float, tp: float, sl: float) -> str:
        """Signs and executes automated market orders with bracketed protection constraints."""
        side = "Buy" if direction == "BUY" else "Sell"
        
        try:
            # Set target leverage boundaries before entering the market
            # Execution params must match string schemas for the V5 specification
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
            logger.critical(f"ORDER DISPATCHED SUCCESSFUL // ID: {order_id} | Side: {side} | Qty: {qty}")
            return order_id
            
        except Exception as e:
            logger.error(f"Order routing execution failed at exchange interface level: {e}")
            return ""