import asyncio
import logging
import math
import time
from typing import Dict, Any, List
from decimal import Decimal, ROUND_HALF_UP
from services.bybit_v5 import BybitUnifiedExecutor

logger = logging.getLogger("QUANT_CORE.SOR")

class SmartOrderRouter:
    def __init__(self, executor: BybitUnifiedExecutor, max_slippage_pct: float = 0.0050):
        self.executor = executor
        self.max_slippage_pct = max_slippage_pct
        self.instrument_cache: Dict[str, Dict[str, float]] = {}

    async def _safe_api_call(self, func, *args, **kwargs) -> Any:
        """
        🛡️ RATE-LIMIT WRAPPER
        Catches HTTP 429 and Bybit RetCode 10006 (Too Many Requests).
        """
        for attempt in range(4):
            try:
                response = await asyncio.to_thread(func, *args, **kwargs)
                if isinstance(response, dict):
                    ret_code = response.get("retCode")
                    if ret_code in [10006, 10002, 10016]:
                        sleep_time = 0.5 * (1.5 ** attempt)
                        logger.warning(f"⚠️ Bybit API Limit/Load (Code: {ret_code}). Throttling SOR for {sleep_time:.2f}s... (Attempt {attempt+1})")
                        await asyncio.sleep(sleep_time)
                        continue
                return response
            except Exception as e:
                error_msg = str(e).lower()
                if "rate limit" in error_msg or "429" in error_msg or "timeout" in error_msg:
                    sleep_time = 0.5 * (1.5 ** attempt)
                    logger.warning(f"⚠️ Bybit Network Rate Limit/Timeout. Throttling SOR for {sleep_time:.2f}s... (Attempt {attempt+1})")
                    await asyncio.sleep(sleep_time)
                    continue
                if attempt == 3:
                    logger.error(f"❌ SOR API Call failed permanently after 4 attempts: {e}")
                    raise e
                await asyncio.sleep(0.5)
        return {}

    def _get_precision(self, step_size: float) -> int:
        if step_size >= 1.0: return 0
        return abs(int(math.floor(math.log10(step_size))))

    async def _fetch_exchange_limits(self, symbol: str):
        if symbol in self.instrument_cache: return
        try:
            info = await self._safe_api_call(self.executor.client.get_instruments_info, category="linear", symbol=symbol)
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
        limits = self.instrument_cache.get(target_symbol, {"min_qty": 1.0, "qty_step": 1.0})
        required_min_qty, qty_step = limits["min_qty"], limits["qty_step"]
        if (qty * price) < 6.0: qty = 6.0 / price
        if qty < required_min_qty: qty = required_min_qty
        stepped_qty = math.floor(qty / qty_step) * qty_step
        return round(stepped_qty, self._get_precision(qty_step))

    def _format_dynamic_price(self, price: float, target_symbol: str) -> float:
        tick_size = self.instrument_cache.get(target_symbol, {"tick_size": 0.01})["tick_size"]
        stepped_price = round(price / tick_size) * tick_size
        return round(stepped_price, self._get_precision(tick_size))

    def _get_meaningful_tob(self, ob_data: Dict, side: str, min_notional: float = 5.0) -> float:
        """
        🚀 HFT ANTI-SPOOFING FILTER
        V20.3 FIX: Adjusted min_notional down to $5.0. 
        Allows micro-account Maker pegging to securely touch the absolute Top of Book
        instead of burying orders behind deep whale walls.
        """
        levels = ob_data.get("bids" if side == "BUY" else "asks", [[0, 0]])
        for level in levels:
            try:
                price = float(level[0])
                qty = float(level[1])
                if price * qty >= min_notional:
                    return price
            except (IndexError, ValueError):
                continue
        # Fallback to absolute top if the book is totally hollow
        return float(levels[0][0]) if levels else 0.0

    async def _fetch_rest_tob(self, symbol: str, side: str) -> float:
        """Fallback REST query if Local RAM is offline."""
        ob_response = await self._safe_api_call(self.executor.client.get_orderbook, category="linear", symbol=symbol)
        ob_data = ob_response.get("result", {})
        return self._get_meaningful_tob({"bids": ob_data.get("b", []), "asks": ob_data.get("a", [])}, side)

    async def _execute_flash_strike(self, symbol: str, direction: str, qty: float, current_mid_price: float, sl: float, tp: float):
        """
        ⚡ THE FLASH STRIKE
        Aggressive multi-step IOC escalation. Used strictly during breakouts.
        """
        logger.critical(f"⚡ FLASH STRIKE AUTHORIZED // {symbol} is experiencing severe structural fracture. Executing aggressive escalation.")
        
        cleaned_qty = self._apply_dynamic_exchange_limits(qty, current_mid_price, symbol)
        final_sl = self._format_dynamic_price(sl, symbol) if sl else 0.0
        final_tp = self._format_dynamic_price(tp, symbol) if tp else 0.0
        side = "Buy" if direction.upper() == "BUY" else "Sell"

        for attempt in range(3):
            escalation_factor = 0.002 * (2 ** attempt)
            
            if side == "Buy":
                target_price = current_mid_price * (1.0 + escalation_factor)
            else:
                target_price = current_mid_price * (1.0 - escalation_factor)
                
            final_price = self._format_dynamic_price(target_price, symbol)

            try:
                response = await self._safe_api_call(
                    self.executor.client.place_order,
                    category="linear", symbol=symbol, side=side, orderType="Limit", 
                    qty=str(cleaned_qty), price=str(final_price), timeInForce="IOC", 
                    stopLoss=str(final_sl) if final_sl else None,
                    takeProfit=str(final_tp) if final_tp else None
                )
                
                if response.get("retCode") == 0:
                    order_id = response.get("result", {}).get("orderId", "UNKNOWN")
                    logger.critical(f"✅ FLASH STRIKE SUCCESS // {symbol} filled instantly on attempt {attempt+1}. ID: {order_id}")
                    return True
                else:
                    logger.warning(f"⚠️ Flash Strike IOC miss (Attempt {attempt+1}): {response.get('retMsg')}")
                    await asyncio.sleep(0.1) 
                    
            except Exception as e:
                logger.error(f"⚠️ Network Exception during Flash Strike for {symbol}: {e}")
                
        logger.error(f"❌ Flash Strike failed permanently after 3 escalation attempts. Order book evaporated.")
        return False

    async def _execute_dynamic_maker_peg(self, symbol: str, direction: str, qty: float, sl: float, tp: float, feature_engine=None, depth_snapshot: dict=None, timeout: int = 60):
        """
        🛡️ HIGH-FREQUENCY MAKER PEGGING
        Bypasses spoofing dust and dynamically throttles its own loop speed based on RAM availability.
        """
        logger.info(f"🛡️ HFT MAKER-PEGGING INITIATED // {symbol}. Engaging Anti-Spoofing Scanners.")
        
        start_time = time.time()
        current_order_id = None
        side = "Buy" if direction.upper() == "BUY" else "Sell"
        final_sl = self._format_dynamic_price(sl, symbol) if sl else 0.0
        final_tp = self._format_dynamic_price(tp, symbol) if tp else 0.0

        anchor_price = None
        max_chase_deviation = 0.0075 

        while time.time() - start_time < timeout:
            loop_delay = 1.5 # Default to safe REST throttling limit

            try:
                # 🚀 V20.2 FIX: Use depth_snapshot efficiently instead of discarding it
                target_price = 0.0
                if depth_snapshot and "bids" in depth_snapshot and "asks" in depth_snapshot:
                    target_price = self._get_meaningful_tob(depth_snapshot, side)
                    if target_price > 0.0: loop_delay = 0.2
                    
                if target_price <= 0.0 and feature_engine and hasattr(feature_engine, 'get_orderbook_snapshot'):
                    ob_data = feature_engine.get_orderbook_snapshot()
                    target_price = self._get_meaningful_tob(ob_data, side)
                    if target_price > 0.0: loop_delay = 0.2
                    
                if target_price <= 0.0:
                    target_price = await self._fetch_rest_tob(symbol, side)
                    
                if target_price <= 0:
                    await asyncio.sleep(loop_delay); continue

                if anchor_price is None: anchor_price = target_price

                # 2. CHASE BOUNDARY CHECK
                if direction.upper() == "BUY" and target_price > anchor_price * (1 + max_chase_deviation):
                    logger.warning(f"🏃 CHASE ABORTED // {symbol} ran +{max_chase_deviation:.2%} beyond signal anchor. Surrendering peg.")
                    break
                if direction.upper() == "SELL" and target_price < anchor_price * (1 - max_chase_deviation):
                    logger.warning(f"🏃 CHASE ABORTED // {symbol} ran -{max_chase_deviation:.2%} beyond signal anchor. Surrendering peg.")
                    break

                cleaned_qty = self._apply_dynamic_exchange_limits(qty, target_price, symbol)
                final_target_price = self._format_dynamic_price(target_price, symbol)
                
                # 3. INITIAL ORDER PLACEMENT
                if not current_order_id:
                    place_response = await self._safe_api_call(
                        self.executor.client.place_order, category="linear", symbol=symbol, side=side, orderType="Limit",
                        qty=str(cleaned_qty), price=str(final_target_price), timeInForce="PostOnly", 
                        stopLoss=str(final_sl) if final_sl else None, takeProfit=str(final_tp) if final_tp else None
                    )
                    if place_response.get("retCode") == 0: 
                        current_order_id = place_response["result"]["orderId"]
                    else:
                        await asyncio.sleep(loop_delay); continue
                
                # 4. ORDER STATUS & RE-PEGGING
                if current_order_id:
                    status_response = await self._safe_api_call(self.executor.client.get_open_orders, category="linear", symbol=symbol, orderId=current_order_id)
                    order_list = status_response.get("result", {}).get("list", [])
                    
                    if not order_list:
                        pos_response = await self._safe_api_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                        pos_size = float(pos_response.get("result", {}).get("list", [{}])[0].get("size", 0))
                        if pos_size > 0:
                            logger.critical(f"✅ MAKER PEG SECURED // {symbol} filled completely at optimal Maker fees.")
                            return True
                        else:
                            current_order_id = None; continue
                            
                    order_info = order_list[0]
                    order_status = order_info.get("orderStatus")
                    current_peg_price = float(order_info.get("price"))
                    
                    if order_status in ["Filled"]:
                        logger.critical(f"✅ MAKER PEG SECURED // {symbol} filled completely at optimal Maker fees.")
                        return True
                    elif order_status in ["Cancelled", "Rejected"]: 
                        current_order_id = None 
                    elif order_status in ["New", "PartiallyFilled"]:
                        if final_target_price != current_peg_price:
                            await self._safe_api_call(self.executor.client.amend_order, category="linear", symbol=symbol, orderId=current_order_id, price=str(final_target_price))

            except Exception as e: 
                logger.debug(f"Maker peg cycle variance for {symbol}: {e}")
                
            await asyncio.sleep(loop_delay) 

        if current_order_id:
            logger.warning(f"⏳ MAKER CHASE TIMEOUT // Market escaped {symbol} peg range. Canceling to protect capital.")
            try: await self._safe_api_call(self.executor.client.cancel_order, category="linear", symbol=symbol, orderId=current_order_id)
            except Exception: pass
        return False

    async def execute_iceberg_block(self, symbol: str, direction: str, total_qty: float, current_mid_price: float, stop_loss: float = None, take_profit: float = None, depth_snapshot: dict = None, vol_z: float = 0.0, vol_mult: float = 1.0, feature_engine: Any = None, **kwargs) -> bool:
        """
        🚀 V20.2 REGIME FIX: TRENDING
        Lower thresholds for crossing the spread. Shorter timeout for maker pegging.
        """
        await self._fetch_exchange_limits(symbol)
        
        logger.info(f"🚀 TRENDING REGIME ROUTING // {symbol} {direction}")
        
        if abs(vol_z) >= 1.5 or vol_mult >= 1.5:
            return await self._execute_flash_strike(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit)
        else:
            return await self._execute_dynamic_maker_peg(symbol, direction, total_qty, stop_loss, take_profit, feature_engine=feature_engine, depth_snapshot=depth_snapshot, timeout=30)

    async def execute_mean_reversion_bracket(self, symbol: str, direction: str, total_qty: float, current_mid_price: float, stop_loss: float = None, take_profit: float = None, depth_snapshot: dict = None, vol_z: float = 0.0, vol_mult: float = 1.0, feature_engine: Any = None, **kwargs) -> bool:
        """
        🕸️ V20.2 REGIME FIX: RANGING
        Forced Maker peg to harvest the spread. Longer timeout patience.
        """
        await self._fetch_exchange_limits(symbol)
        
        logger.info(f"🕸️ RANGING REGIME ROUTING // Forcing Maker Peg on {symbol}")
        
        # Strict passive maker execution to harvest spread
        return await self._execute_dynamic_maker_peg(symbol, direction, total_qty, stop_loss, take_profit, feature_engine=feature_engine, depth_snapshot=depth_snapshot, timeout=60)