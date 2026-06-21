import asyncio
import random
import logging
import time
from typing import Dict, Any
from services.bybit_v5 import BybitUnifiedExecutor

logger = logging.getLogger("QUANT_CORE.SOR")

class SmartOrderRouter:
    def __init__(self, executor: BybitUnifiedExecutor, max_slippage_pct: float = 0.0050):
        self.executor = executor
        self.max_slippage_pct = max_slippage_pct

    def _clean_lot_size(self, qty: float, target_symbol: str) -> float:
        """Enforces strict asset lot precision guidelines to eradicate order size rounding leaks."""
        if any(target_symbol.startswith(token) for token in ["BTC", "ETH"]):
            return round(qty, 3)
        elif any(target_symbol.startswith(token) for token in ["SOL", "AVAX", "NEAR", "WLD", "XRP", "HYPE", "RE", "SPCX", "ZEC"]):
            return round(qty, 2)
        return round(qty, 1)

    def _format_price_precision(self, price: float, target_symbol: str) -> float:
        """Dynamically rounds target boundary prices to prevent decimal step rejections."""
        if target_symbol.startswith("BTC") or target_symbol.startswith("ETH"):
            return round(price, 2)
        elif target_symbol.startswith("SOL") or target_symbol.startswith("AVAX") or target_symbol.startswith("ZEC"):
            return round(price, 3)
        if price < 1.0:
            return round(price, 4)
        return round(price, 2)

    async def execute_iceberg_block(
        self, symbol: str, direction: str, total_qty: float, current_mid_price: float,
        stop_loss: float = None, take_profit: float = None, **kwargs
    ) -> bool:
        """
        🚀 OFFENSIVE MODE: TRENDING MARKETS
        Aggressive market-order block execution to guarantee fill during explosive breakouts.
        """
        depth_snapshot = kwargs.get("depth_snapshot", {})
        if depth_snapshot and "bids" in depth_snapshot and "asks" in depth_snapshot:
            try:
                best_bid = float(depth_snapshot["bids"][0][0])
                best_ask = float(depth_snapshot["asks"][0][0])
                live_spread = (best_ask - best_bid) / best_bid
                if live_spread > self.max_slippage_pct:
                    logger.warning(f"❌ SPREAD VIOLATION // {symbol} Market Friction: {live_spread:.4%} > Safety Limit: {self.max_slippage_pct:.4%}.")
                    return False
            except (IndexError, ValueError, TypeError) as e:
                logger.debug(f"Depth parsing bypassed for {symbol}: {e}")

        cleaned_qty = self._clean_lot_size(total_qty, symbol)
        if (cleaned_qty * current_mid_price) < 5.0:
            cleaned_qty = self._clean_lot_size(5.1 / current_mid_price, symbol)

        logger.info(f"🚀 TRENDING ROUTER ONLINE // Target: {symbol} | Dir: {direction} | Qty: {cleaned_qty}")

        try:
            # Standard Market Order Dispatch
            order_id = await self.executor.dispatch_market_order(
                symbol=symbol,
                direction=direction,
                qty=cleaned_qty,
                tp=self._format_price_precision(take_profit, symbol) if take_profit else 0.0,
                sl=self._format_price_precision(stop_loss, symbol) if stop_loss else 0.0
            )
            if order_id:
                logger.critical(f"🎯 BREAKOUT BLOCK FILLED // Ticket: {order_id[:12]}...")
                return True
            return False
        except Exception as e:
            logger.error(f"Critical execution fault inside SOR market block allocation: {e}")
            return False


    async def execute_mean_reversion_bracket(
        self, symbol: str, direction: str, total_qty: float, current_mid_price: float,
        stop_loss: float = None, take_profit: float = None, **kwargs
    ) -> bool:
        """
        🛡️ ACCUMULATION MODE: RANGING MARKETS
        Passive Limit Order execution designed to capture the spread and buy support/sell resistance.
        """
        depth_snapshot = kwargs.get("depth_snapshot", {})
        limit_entry_price = current_mid_price

        # Extract the absolute top of the order book to place our passive limit order
        if depth_snapshot and "bids" in depth_snapshot and "asks" in depth_snapshot:
            try:
                best_bid = float(depth_snapshot["bids"][0][0])
                best_ask = float(depth_snapshot["asks"][0][0])
                
                # If buying, sit exactly on the best bid (Maker). If selling, sit on best ask.
                if direction.upper() == "BUY":
                    limit_entry_price = best_bid
                else:
                    limit_entry_price = best_ask
            except (IndexError, ValueError, TypeError):
                pass

        cleaned_qty = self._clean_lot_size(total_qty, symbol)
        if (cleaned_qty * limit_entry_price) < 5.0:
            cleaned_qty = self._clean_lot_size(5.1 / limit_entry_price, symbol)
            
        limit_entry_price = self._format_price_precision(limit_entry_price, symbol)

        logger.info(f"🕸️ MEAN REVERSION ROUTER ONLINE // Target: {symbol} | Passive Entry Limit: {limit_entry_price}")

        try:
            # We attempt to dispatch a passive limit order to avoid taker fees. 
            # If your executor doesn't have a specific limit dispatch yet, this uses the raw client.
            if hasattr(self.executor, 'dispatch_limit_order'):
                order_id = await self.executor.dispatch_limit_order(
                    symbol=symbol,
                    direction=direction,
                    qty=cleaned_qty,
                    price=limit_entry_price,
                    tp=self._format_price_precision(take_profit, symbol) if take_profit else 0.0,
                    sl=self._format_price_precision(stop_loss, symbol) if stop_loss else 0.0
                )
            else:
                # Fallback directly to Bybit client if dispatch_limit_order is not defined in bybit_v5.py
                side = "Buy" if direction.upper() == "BUY" else "Sell"
                order_response = await asyncio.to_thread(
                    self.executor.client.place_order,
                    category="linear", symbol=symbol, side=side, orderType="Limit", 
                    qty=str(cleaned_qty), price=str(limit_entry_price),
                    takeProfit=str(self._format_price_precision(take_profit, symbol)),
                    stopLoss=str(self._format_price_precision(stop_loss, symbol)),
                    timeInForce="PostOnly" # Ensures we ONLY pay maker fees
                )
                order_id = order_response.get("result", {}).get("orderId")

            if order_id:
                logger.critical(f"🕸️ PASSIVE LIMIT NET DEPLOYED // Ticket: {order_id[:12]}...")
                return True
            return False
        except Exception as e:
            logger.error(f"Critical execution fault inside SOR limit block allocation: {e}")
            return False