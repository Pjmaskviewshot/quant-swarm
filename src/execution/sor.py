import asyncio
import logging
import math
from typing import Dict, Any
from services.bybit_v5 import BybitUnifiedExecutor

logger = logging.getLogger("QUANT_CORE.SOR")

class SmartOrderRouter:
    def __init__(self, executor: BybitUnifiedExecutor, max_slippage_pct: float = 0.0050):
        self.executor = executor
        self.max_slippage_pct = max_slippage_pct
        # 🧠 DYNAMIC CACHE: Stores Bybit's exact lot limits and step sizes for any coin it encounters
        self.instrument_cache: Dict[str, Dict[str, float]] = {}

    def _get_precision(self, step_size: float) -> int:
        """Mathematically determines the number of decimal places required for a given step size."""
        if step_size >= 1.0:
            return 0
        return abs(int(math.floor(math.log10(step_size))))

    async def _fetch_exchange_limits(self, symbol: str):
        """Dynamically queries Bybit for the absolute minimum order sizes and precision steps."""
        if symbol in self.instrument_cache:
            return

        try:
            # Ping Bybit's V5 instruments endpoint dynamically
            info = await asyncio.to_thread(
                self.executor.client.get_instruments_info,
                category="linear",
                symbol=symbol
            )
            
            lot_filter = info["result"]["list"][0]["lotSizeFilter"]
            price_filter = info["result"]["list"][0]["priceFilter"]
            
            self.instrument_cache[symbol] = {
                "min_qty": float(lot_filter["minOrderQty"]),
                "qty_step": float(lot_filter["qtyStep"]),
                "tick_size": float(price_filter["tickSize"])
            }
            logger.info(f"📡 DYNAMIC LIMITS ACQUIRED // {symbol} | Min Qty: {self.instrument_cache[symbol]['min_qty']} | Step: {self.instrument_cache[symbol]['qty_step']}")
            
        except Exception as e:
            logger.error(f"Failed to fetch strict limits for {symbol}, using safe defaults: {e}")
            self.instrument_cache[symbol] = {"min_qty": 1.0, "qty_step": 1.0, "tick_size": 0.01}

    def _apply_dynamic_exchange_limits(self, qty: float, price: float, target_symbol: str) -> float:
        """Calculates the required lot size using Bybit's dynamically fetched constraints."""
        limits = self.instrument_cache.get(target_symbol, {"min_qty": 1.0, "qty_step": 1.0})
        
        required_min_qty = limits["min_qty"]
        qty_step = limits["qty_step"]
        
        # 1. Enforce Bybit's Absolute Dollar Minimum (ErrCode 110094)
        if (qty * price) < 6.0:
            qty = 6.0 / price
            
        # 2. Enforce Contract Quantity Limit (ErrCode 10001)
        if qty < required_min_qty:
            qty = required_min_qty
            
        # 3. Dynamic Precision Rounding using Bybit's exact step size
        # We use floor division so we never accidentally round UP into a margin deficit
        stepped_qty = math.floor(qty / qty_step) * qty_step
        precision = self._get_precision(qty_step)
        
        return round(stepped_qty, precision)

    def _format_dynamic_price(self, price: float, target_symbol: str) -> float:
        """Rounds target prices to Bybit's exact allowed tick size."""
        limits = self.instrument_cache.get(target_symbol, {"tick_size": 0.01})
        tick_size = limits["tick_size"]
        
        stepped_price = round(price / tick_size) * tick_size
        precision = self._get_precision(tick_size)
        
        return round(stepped_price, precision)

    async def execute_iceberg_block(
        self, symbol: str, direction: str, total_qty: float, current_mid_price: float,
        stop_loss: float = None, take_profit: float = None, **kwargs
    ) -> bool:
        """
        🚀 OFFENSIVE MODE: TRENDING MARKETS
        """
        # 🛡️ Download constraints from Bybit before calculating sizes
        await self._fetch_exchange_limits(symbol)

        depth_snapshot = kwargs.get("depth_snapshot", {})
        if depth_snapshot and "bids" in depth_snapshot and "asks" in depth_snapshot:
            try:
                best_bid = float(depth_snapshot["bids"][0][0])
                best_ask = float(depth_snapshot["asks"][0][0])
                live_spread = (best_ask - best_bid) / best_bid
                if live_spread > self.max_slippage_pct:
                    logger.warning(f"❌ SPREAD VIOLATION // {symbol} Friction: {live_spread:.4%} > {self.max_slippage_pct:.4%}.")
                    return False
            except (IndexError, ValueError, TypeError):
                pass

        # 🚀 Route through the dynamic exchange enforcer
        cleaned_qty = self._apply_dynamic_exchange_limits(total_qty, current_mid_price, symbol)

        logger.info(f"🚀 TRENDING ROUTER ONLINE // Target: {symbol} | Dir: {direction} | Qty: {cleaned_qty}")

        try:
            order_id = await self.executor.dispatch_market_order(
                symbol=symbol, direction=direction, qty=cleaned_qty,
                tp=self._format_dynamic_price(take_profit, symbol) if take_profit else 0.0,
                sl=self._format_dynamic_price(stop_loss, symbol) if stop_loss else 0.0
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
        """
        # 🛡️ Download constraints from Bybit before calculating sizes
        await self._fetch_exchange_limits(symbol)

        depth_snapshot = kwargs.get("depth_snapshot", {})
        limit_entry_price = current_mid_price

        if depth_snapshot and "bids" in depth_snapshot and "asks" in depth_snapshot:
            try:
                best_bid = float(depth_snapshot["bids"][0][0])
                best_ask = float(depth_snapshot["asks"][0][0])
                limit_entry_price = best_bid if direction.upper() == "BUY" else best_ask
            except (IndexError, ValueError, TypeError):
                pass

        # 🚀 Route through the dynamic exchange enforcer
        cleaned_qty = self._apply_dynamic_exchange_limits(total_qty, limit_entry_price, symbol)
        limit_entry_price = self._format_dynamic_price(limit_entry_price, symbol)

        logger.info(f"🕸️ MEAN REVERSION ROUTER ONLINE // Target: {symbol} | Limit: {limit_entry_price} | Qty: {cleaned_qty}")

        try:
            if hasattr(self.executor, 'dispatch_limit_order'):
                order_id = await self.executor.dispatch_limit_order(
                    symbol=symbol, direction=direction, qty=cleaned_qty, price=limit_entry_price,
                    tp=self._format_dynamic_price(take_profit, symbol) if take_profit else 0.0,
                    sl=self._format_dynamic_price(stop_loss, symbol) if stop_loss else 0.0
                )
            else:
                side = "Buy" if direction.upper() == "BUY" else "Sell"
                order_response = await asyncio.to_thread(
                    self.executor.client.place_order,
                    category="linear", symbol=symbol, side=side, orderType="Limit", 
                    qty=str(cleaned_qty), price=str(limit_entry_price),
                    takeProfit=str(self._format_dynamic_price(take_profit, symbol)),
                    stopLoss=str(self._format_dynamic_price(stop_loss, symbol)),
                    timeInForce="PostOnly" 
                )
                order_id = order_response.get("result", {}).get("orderId")

            if order_id:
                logger.critical(f"🕸️ PASSIVE LIMIT NET DEPLOYED // Ticket: {order_id[:12]}...")
                return True
            return False
        except Exception as e:
            logger.error(f"Critical execution fault inside SOR limit block allocation: {e}")
            return False