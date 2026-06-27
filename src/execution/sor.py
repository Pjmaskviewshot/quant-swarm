import asyncio
import logging
import math
import time
from typing import Dict, Any
from decimal import Decimal, ROUND_HALF_UP
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

    async def _execute_flash_strike(self, symbol: str, direction: str, qty: float, current_mid_price: float, sl: float, tp: float):
        """
        ⚡ THE FLASH STRIKE
        IOC (Immediate-Or-Cancel) for violent Z-OBI anomalies.
        Crosses the spread to guarantee instant execution before price escapes.
        """
        logger.critical(f"⚡ FLASH STRIKE AUTHORIZED // {symbol} is experiencing severe structural fracture. Crossing spread with IOC.")
        
        slip_buffer = current_mid_price * self.max_slippage_pct
        exec_price = current_mid_price + slip_buffer if direction.upper() == "BUY" else current_mid_price - slip_buffer
        
        cleaned_qty = self._apply_dynamic_exchange_limits(qty, current_mid_price, symbol)
        final_price = self._format_dynamic_price(exec_price, symbol)
        final_sl = self._format_dynamic_price(sl, symbol) if sl else 0.0
        final_tp = self._format_dynamic_price(tp, symbol) if tp else 0.0
        
        side = "Buy" if direction.upper() == "BUY" else "Sell"

        try:
            response = await asyncio.to_thread(
                self.executor.client.place_order,
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Limit", 
                qty=str(cleaned_qty), 
                price=str(final_price), 
                timeInForce="IOC", 
                stopLoss=str(final_sl) if final_sl else None,
                takeProfit=str(final_tp) if final_tp else None
            )
            
            if response.get("retCode") == 0:
                order_id = response.get("result", {}).get("orderId", "UNKNOWN")
                logger.critical(f"✅ FLASH STRIKE SUCCESS // {symbol} filled instantly on extreme volatility. ID: {order_id}")
                return True
            else:
                logger.error(f"❌ Flash Strike failed or rejected by exchange: {response.get('retMsg')}")
                return False
                
        except Exception as e:
            logger.error(f"⚠️ Network/API Exception during Flash Strike for {symbol}: {e}")
            return False

    async def _execute_dynamic_maker_peg(self, symbol: str, direction: str, qty: float, sl: float, tp: float, feature_engine=None, timeout: int = 60):
        """
        🛡️ DYNAMIC MAKER-PEGGING (The Order Chase)
        Places PostOnly limit orders and constantly amends them to the top of the book.
        Ensures 0% Taker Fees and protects against stale limit traps.
        """
        logger.info(f"🛡️ MAKER-PEGGING INITIATED // {symbol}. Chasing top of the book via Zero-Latency Local RAM.")
        
        start_time = time.time()
        current_order_id = None
        
        side = "Buy" if direction.upper() == "BUY" else "Sell"
        
        final_sl = self._format_dynamic_price(sl, symbol) if sl else 0.0
        final_tp = self._format_dynamic_price(tp, symbol) if tp else 0.0

        while time.time() - start_time < timeout:
            try:
                # 🚀 1. ZERO-LATENCY RAM CHECK: Fetch live top of book from memory
                best_bid, best_ask = 0.0, 0.0
                
                if feature_engine and hasattr(feature_engine, 'get_orderbook_snapshot'):
                    ob_data = feature_engine.get_orderbook_snapshot()
                    try:
                        best_bid = float(ob_data.get("bids", [[0]])[0][0])
                        best_ask = float(ob_data.get("asks", [[0]])[0][0])
                    except (IndexError, ValueError):
                        await asyncio.sleep(1.5)
                        continue
                else:
                    # Fallback to REST only if memory fails
                    ob_response = await asyncio.to_thread(self.executor.client.get_orderbook, category="linear", symbol=symbol)
                    ob_data = ob_response.get("result", {})
                    try:
                        best_bid = float(ob_data.get("b", [[0]])[0][0])
                        best_ask = float(ob_data.get("a", [[0]])[0][0])
                    except (IndexError, ValueError):
                        await asyncio.sleep(1.5)
                        continue
                
                target_price = best_bid if direction.upper() == "BUY" else best_ask
                
                cleaned_qty = self._apply_dynamic_exchange_limits(qty, target_price, symbol)
                final_target_price = self._format_dynamic_price(target_price, symbol)
                
                # 2. Place order if we don't have one active
                if not current_order_id:
                    place_response = await asyncio.to_thread(
                        self.executor.client.place_order,
                        category="linear", symbol=symbol, side=side, orderType="Limit",
                        qty=str(cleaned_qty), price=str(final_target_price), timeInForce="PostOnly", 
                        stopLoss=str(final_sl) if final_sl else None, 
                        takeProfit=str(final_tp) if final_tp else None
                    )
                    if place_response.get("retCode") == 0:
                        current_order_id = place_response["result"]["orderId"]
                    else:
                        # Rejection likely means PostOnly crossed the spread. Loop will retry instantly.
                        await asyncio.sleep(0.5)
                        continue
                
                # 3. Assess and Amend Order
                if current_order_id:
                    status_response = await asyncio.to_thread(
                        self.executor.client.get_open_orders,
                        category="linear", symbol=symbol, orderId=current_order_id
                    )
                    order_list = status_response.get("result", {}).get("list", [])
                    
                    if not order_list:
                        # Order is gone from open orders. Check if it became a live position.
                        pos_response = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                        pos_size = float(pos_response.get("result", {}).get("list", [{}])[0].get("size", 0))
                        if pos_size > 0:
                            logger.critical(f"✅ MAKER PEG SECURED // {symbol} filled completely at optimal Maker fees.")
                            return True
                        else:
                            current_order_id = None 
                            continue
                            
                    order_info = order_list[0]
                    order_status = order_info.get("orderStatus")
                    current_peg_price = float(order_info.get("price"))
                    
                    if order_status in ["Filled"]:
                        logger.critical(f"✅ MAKER PEG SECURED // {symbol} filled completely at optimal Maker fees.")
                        return True
                    elif order_status in ["Cancelled", "Rejected"]:
                        current_order_id = None 
                    elif order_status in ["New", "PartiallyFilled"]:
                        # 4. The Chase: If the book moved away, pull our order and move it to the new top
                        if final_target_price != current_peg_price:
                            await asyncio.to_thread(
                                self.executor.client.amend_order,
                                category="linear", symbol=symbol, orderId=current_order_id,
                                price=str(final_target_price)
                            )

            except Exception as e:
                logger.debug(f"Maker peg cycle variance for {symbol}: {e}")
                
            await asyncio.sleep(1.5) 

        # 5. Stale Timeout: If market escaped completely, cancel and surrender
        if current_order_id:
            logger.warning(f"⏳ MAKER CHASE TIMEOUT // Market escaped {symbol} peg range. Canceling to protect capital.")
            try:
                await asyncio.to_thread(self.executor.client.cancel_order, category="linear", symbol=symbol, orderId=current_order_id)
            except Exception:
                pass
        return False

    async def execute_iceberg_block(
        self, symbol: str, direction: str, total_qty: float, current_mid_price: float,
        stop_loss: float = None, take_profit: float = None, vol_z: float = 0.0, vol_mult: float = 1.0, **kwargs
    ) -> bool:
        """
        🚀 OFFENSIVE MODE: TRENDING MARKETS
        """
        await self._fetch_exchange_limits(symbol)

        # Allow fallback to kwargs just in case the execution routing from main changes
        v_z = kwargs.get("vol_z", vol_z)
        v_m = kwargs.get("vol_mult", vol_mult)
        fe = kwargs.get("feature_engine") # 🚀 Extracts local RAM from Main

        # The Mathematical Flash Gate
        if abs(v_z) >= 3.0 and v_m >= 2.5:
            return await self._execute_flash_strike(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit)
        else:
            return await self._execute_dynamic_maker_peg(symbol, direction, total_qty, stop_loss, take_profit, feature_engine=fe)

    async def execute_mean_reversion_bracket(
        self, symbol: str, direction: str, total_qty: float, current_mid_price: float,
        stop_loss: float = None, take_profit: float = None, vol_z: float = 0.0, vol_mult: float = 1.0, **kwargs
    ) -> bool:
        """
        🛡️ ACCUMULATION MODE: RANGING MARKETS
        """
        await self._fetch_exchange_limits(symbol)

        v_z = kwargs.get("vol_z", vol_z)
        v_m = kwargs.get("vol_mult", vol_mult)
        fe = kwargs.get("feature_engine") # 🚀 Extracts local RAM from Main

        # The Mathematical Flash Gate
        if abs(v_z) >= 3.0 and v_m >= 2.5:
            return await self._execute_flash_strike(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit)
        else:
            return await self._execute_dynamic_maker_peg(symbol, direction, total_qty, stop_loss, take_profit, feature_engine=fe)