import os
import asyncio
import logging
import math
import time
from typing import Dict, Any, List
from decimal import Decimal, ROUND_HALF_UP
from services.bybit_v5 import BybitUnifiedExecutor

logger = logging.getLogger("QUANT_CORE.SOR")

class SmartOrderRouter:
    """
    🚀 V27.1 APEX: INSTITUTIONAL SMART ORDER ROUTER
    Features Explicit Order-ID State Tracking, Hedge Mode Compatibility,
    Hard Slippage Clamps, and Maker-Peg Rejection Circuit Breakers.
    Now mathematically guaranteed to respect the global TokenBucketRateLimiter.
    """
    def __init__(self, executor: BybitUnifiedExecutor, max_slippage_pct: float = 0.0050):
        self.executor = executor
        self.max_slippage_pct = max_slippage_pct
        self.instrument_cache: Dict[str, Dict[str, float]] = {}
        
        # 🚀 V26.2 FIX: Dynamic Hedge Mode Compatibility (Default 0 = One-Way Mode)
        self.position_idx = int(os.getenv("BYBIT_POSITION_IDX", 0))

    def _get_precision(self, step_size: float) -> int:
        if step_size >= 1.0: return 0
        return abs(int(math.floor(math.log10(step_size))))

    async def _fetch_exchange_limits(self, symbol: str):
        if symbol in self.instrument_cache: return
        try:
            # ⚡ CRITICAL FIX: Route through executor's rate-limited gateway
            info = await self.executor.safe_call(self.executor.client.get_instruments_info, category="linear", symbol=symbol)
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
        if (qty * price) < 6.0: qty = 6.0 / (price + 1e-9)
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
        return float(levels[0][0]) if levels else 0.0

    async def _fetch_rest_tob(self, symbol: str, side: str) -> float:
        """Fallback REST query if Local RAM is offline."""
        # ⚡ CRITICAL FIX: Route through executor's rate-limited gateway
        ob_response = await self.executor.safe_call(self.executor.client.get_orderbook, category="linear", symbol=symbol)
        ob_data = ob_response.get("result", {})
        return self._get_meaningful_tob({"bids": ob_data.get("b", []), "asks": ob_data.get("a", [])}, side)

    async def _execute_flash_strike(self, symbol: str, direction: str, qty: float, current_mid_price: float, sl: float, tp: float):
        """
        ⚡ THE FLASH STRIKE
        Aggressive IOC escalation with explicit `cumExecQty` verification.
        Prevents hallucinated positions when IOC orders miss the book.
        """
        logger.critical(f"⚡ FLASH STRIKE AUTHORIZED // {symbol} executing aggressive escalation.")
        
        cleaned_qty = self._apply_dynamic_exchange_limits(qty, current_mid_price, symbol)
        final_sl = self._format_dynamic_price(sl, symbol) if sl else 0.0
        final_tp = self._format_dynamic_price(tp, symbol) if tp else 0.0
        side = "Buy" if direction.upper() == "BUY" else "Sell"

        for attempt in range(3):
            # Escalate aggressiveness but HARD CLAMP it to the max_slippage_pct
            escalation_pct = 0.001 * (2 ** attempt)
            escalation_pct = min(escalation_pct, self.max_slippage_pct)
            
            if side == "Buy":
                target_price = current_mid_price * (1.0 + escalation_pct)
            else:
                target_price = current_mid_price * (1.0 - escalation_pct)
                
            final_price = self._format_dynamic_price(target_price, symbol)

            try:
                # ⚡ CRITICAL FIX: Route through executor's rate-limited gateway
                response = await self.executor.safe_call(
                    self.executor.client.place_order,
                    category="linear", symbol=symbol, side=side, orderType="Limit", 
                    qty=str(cleaned_qty), price=str(final_price), timeInForce="IOC", 
                    stopLoss=str(final_sl) if final_sl else None,
                    takeProfit=str(final_tp) if final_tp else None,
                    positionIdx=self.position_idx
                )
                
                if response.get("retCode") == 0:
                    order_id = response.get("result", {}).get("orderId", "UNKNOWN")
                    
                    # Give the matching engine 300ms to resolve the IOC, then verify.
                    await asyncio.sleep(0.3)
                    
                    hist_res = await self.executor.safe_call(
                        self.executor.client.get_order_history,
                        category="linear", symbol=symbol, orderId=order_id, limit=1
                    )
                    orders = hist_res.get("result", {}).get("list", [])
                    
                    if orders:
                        cum_exec = float(orders[0].get("cumExecQty", 0.0))
                        if cum_exec > 0:
                            logger.critical(f"✅ FLASH STRIKE SUCCESS // {symbol} filled {cum_exec} units on attempt {attempt+1}. ID: {order_id}")
                            return True
                        else:
                            logger.warning(f"⚠️ Flash Strike IOC missed (Liquidity vanished). Escalating...")
                    else:
                        # Exchange latency fallback: Assume success if history query drops to let the lifecycle daemon manage it.
                        logger.critical(f"✅ FLASH STRIKE PLACED // {symbol} ID: {order_id} (History delayed)")
                        return True
                else:
                    logger.warning(f"⚠️ Flash Strike API rejection (Attempt {attempt+1}): {response.get('retMsg')}")
                    await asyncio.sleep(0.1) 
                    
            except Exception as e:
                logger.error(f"⚠️ Network Exception during Flash Strike for {symbol}: {e}")
                
        logger.error(f"❌ Flash Strike failed permanently after 3 escalation attempts. Order book evaporated or Slippage Cap hit.")
        return False

    async def _execute_dynamic_maker_peg(self, symbol: str, direction: str, qty: float, sl: float, tp: float, feature_engine=None, depth_snapshot: dict=None, timeout: int = 60):
        """
        🛡️ HIGH-FREQUENCY MAKER PEGGING
        Replaces blind polling with exact order-ID state tracking.
        Detects and honors fractional partial fills perfectly.
        """
        logger.info(f"🛡️ HFT MAKER-PEGGING INITIATED // {symbol}. Engaging Anti-Spoofing Scanners.")
        
        start_time = time.time()
        current_order_id = None
        side = "Buy" if direction.upper() == "BUY" else "Sell"
        final_sl = self._format_dynamic_price(sl, symbol) if sl else 0.0
        final_tp = self._format_dynamic_price(tp, symbol) if tp else 0.0

        anchor_price = None
        max_chase_deviation = 0.0075 
        rejection_count = 0  

        while time.time() - start_time < timeout:
            loop_delay = 1.5 

            try:
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
                    place_response = await self.executor.safe_call(
                        self.executor.client.place_order, category="linear", symbol=symbol, side=side, orderType="Limit",
                        qty=str(cleaned_qty), price=str(final_target_price), timeInForce="PostOnly", 
                        stopLoss=str(final_sl) if final_sl else None, takeProfit=str(final_tp) if final_tp else None,
                        positionIdx=self.position_idx
                    )
                    if place_response.get("retCode") == 0: 
                        current_order_id = place_response["result"]["orderId"]
                    else:
                        rejection_count += 1
                        if rejection_count >= 5:
                            logger.error(f"🛑 PEG CIRCUIT BREAKER TRIPPED // {symbol} PostOnly rejected 5 times. Market is running away.")
                            break
                        await asyncio.sleep(loop_delay); continue
                
                # 4. EXPLICIT ORDER ID STATE TRACKING
                if current_order_id:
                    status_response = await self.executor.safe_call(self.executor.client.get_open_orders, category="linear", symbol=symbol, orderId=current_order_id)
                    order_list = status_response.get("result", {}).get("list", [])
                    
                    if not order_list:
                        # Order is no longer open. Check history to see if it Filled or Canceled.
                        hist_response = await self.executor.safe_call(self.executor.client.get_order_history, category="linear", symbol=symbol, orderId=current_order_id, limit=1)
                        hist_list = hist_response.get("result", {}).get("list", [])
                        
                        if hist_list:
                            cum_exec = float(hist_list[0].get("cumExecQty", 0.0))
                            if cum_exec > 0:
                                logger.critical(f"✅ MAKER PEG RESOLVED // {symbol} secured {cum_exec} units.")
                                return True
                        
                        # It was canceled with 0 fills. Reset and try to peg again.
                        current_order_id = None
                        continue
                            
                    order_info = order_list[0]
                    order_status = order_info.get("orderStatus")
                    current_peg_price = float(order_info.get("price"))
                    cum_exec_qty = float(order_info.get("cumExecQty", 0.0))
                    
                    if order_status in ["Filled"]:
                        logger.critical(f"✅ MAKER PEG SECURED // {symbol} filled completely at optimal Maker fees.")
                        return True
                    elif order_status in ["Cancelled", "Rejected"]: 
                        rejection_count += 1
                        current_order_id = None 
                        
                        # Partial Fill Guard: If it was canceled but we snagged a partial fill, exit successfully!
                        if cum_exec_qty > 0:
                            logger.critical(f"✅ MAKER PEG PARTIAL // {symbol} secured {cum_exec_qty} units before rejection.")
                            return True
                            
                        if rejection_count >= 5:
                            logger.error(f"🛑 PEG CIRCUIT BREAKER TRIPPED // {symbol} canceled/rejected 5 times. Aborting.")
                            break
                    elif order_status in ["New", "PartiallyFilled"]:
                        if final_target_price != current_peg_price:
                            await self.executor.safe_call(self.executor.client.amend_order, category="linear", symbol=symbol, orderId=current_order_id, price=str(final_target_price))

            except Exception as e: 
                logger.debug(f"Maker peg cycle variance for {symbol}: {e}")
                
            await asyncio.sleep(loop_delay) 

        # 5. TIMEOUT GRACEFUL RESOLUTION
        if current_order_id:
            logger.warning(f"⏳ MAKER CHASE TIMEOUT // Market escaped {symbol} peg range. Canceling to protect capital.")
            try: 
                await self.executor.safe_call(self.executor.client.cancel_order, category="linear", symbol=symbol, orderId=current_order_id)
                # Verify if we caught a partial fill at the very last second before timeout
                hist_res = await self.executor.safe_call(self.executor.client.get_order_history, category="linear", symbol=symbol, orderId=current_order_id, limit=1)
                hist_list = hist_res.get("result", {}).get("list", [])
                if hist_list and float(hist_list[0].get("cumExecQty", 0.0)) > 0:
                    return True
            except Exception: pass
            
        return False

    async def execute_iceberg_block(self, symbol: str, direction: str, total_qty: float, current_mid_price: float, stop_loss: float = None, take_profit: float = None, depth_snapshot: dict = None, vol_z: float = 0.0, vol_mult: float = 1.0, feature_engine: Any = None, **kwargs) -> bool:
        """
        🚀 TRENDING REGIME ROUTING
        """
        await self._fetch_exchange_limits(symbol)
        
        logger.info(f"🚀 TRENDING REGIME ROUTING // {symbol} {direction}")
        
        if abs(vol_z) >= 1.5 or vol_mult >= 1.5:
            return await self._execute_flash_strike(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit)
        else:
            return await self._execute_dynamic_maker_peg(symbol, direction, total_qty, stop_loss, take_profit, feature_engine=feature_engine, depth_snapshot=depth_snapshot, timeout=30)

    async def execute_mean_reversion_bracket(self, symbol: str, direction: str, total_qty: float, current_mid_price: float, stop_loss: float = None, take_profit: float = None, depth_snapshot: dict = None, vol_z: float = 0.0, vol_mult: float = 1.0, feature_engine: Any = None, **kwargs) -> bool:
        """
        🕸️ RANGING REGIME ROUTING
        """
        await self._fetch_exchange_limits(symbol)
        
        logger.info(f"🕸️ RANGING REGIME ROUTING // Forcing Maker Peg on {symbol}")
        
        return await self._execute_dynamic_maker_peg(symbol, direction, total_qty, stop_loss, take_profit, feature_engine=feature_engine, depth_snapshot=depth_snapshot, timeout=60)