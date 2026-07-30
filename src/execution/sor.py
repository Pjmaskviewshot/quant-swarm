"""
💎 V55.2 QUANTUM SWARM: INSTITUTIONAL SMART ORDER ROUTER
--------------------------------------------------------
Features X-Ray Diagnostic Telemetry, Maker-Grid Spread Capture,
Strict Slippage Clamps, PostOnly Pegging, Adverse Selection Protection, 
Null-Guard Parity, and Reduced Exposure Timeouts.
"""

import os
import asyncio
import logging
import math
import time
from typing import Dict, Any, List, Tuple, Optional
from decimal import Decimal, ROUND_HALF_UP

from services.bybit_v5 import BybitUnifiedExecutor

logger = logging.getLogger("QUANT_CORE.SOR")

class SmartOrderRouter:
    def __init__(self, executor: BybitUnifiedExecutor, max_slippage_pct: float = 0.0012):
        self.executor = executor
        self.max_slippage_pct = max_slippage_pct
        self.instrument_cache: Dict[str, Dict[str, float]] = {}
        self.position_idx = int(os.getenv("BYBIT_POSITION_IDX", 0))

    def _get_precision(self, step_size: float) -> int:
        if step_size >= 1.0: return 0
        return abs(int(math.floor(math.log10(step_size))))

    async def _fetch_exchange_limits(self, symbol: str):
        if symbol in self.instrument_cache: return
        try:
            info = await self.executor.safe_call(self.executor.client.get_instruments_info, category="linear", symbol=symbol)
            lot_filter = info["result"]["list"][0]["lotSizeFilter"]
            price_filter = info["result"]["list"][0]["priceFilter"]
            self.instrument_cache[symbol] = {
                "min_qty": float(lot_filter["minOrderQty"]),
                "qty_step": float(lot_filter["qtyStep"]),
                "tick_size": float(price_filter["tickSize"])
            }
            logger.info(f"[X-RAY] 📡 DYNAMIC LIMITS ACQUIRED // {symbol} | Min Qty: {self.instrument_cache[symbol]['min_qty']} | Step: {self.instrument_cache[symbol]['qty_step']}")
        except Exception as e:
            logger.error(f"[X-RAY] Failed to fetch strict limits for {symbol}, using safe defaults: {e}")
            self.instrument_cache[symbol] = {"min_qty": 1.0, "qty_step": 1.0, "tick_size": 0.01}

    def _apply_dynamic_exchange_limits(self, qty: float, price: float, target_symbol: str) -> float:
        limits = self.instrument_cache.get(target_symbol, {"min_qty": 1.0, "qty_step": 1.0})
        required_min_qty, qty_step = limits["min_qty"], limits["qty_step"]
        
        # Ensure we always meet Bybit's ~$5-6 minimum notional value constraint
        if (qty * price) < 6.0: 
            qty = 6.0 / (price + 1e-9)
            
        if qty < required_min_qty: 
            qty = required_min_qty
            
        stepped_qty = math.floor(qty / qty_step) * qty_step
        return round(stepped_qty, self._get_precision(qty_step))

    def _format_dynamic_price(self, price: float, target_symbol: str) -> float:
        tick_size = self.instrument_cache.get(target_symbol, {"tick_size": 0.01})["tick_size"]
        stepped_price = round(price / tick_size) * tick_size
        return round(stepped_price, self._get_precision(tick_size))

    def _get_meaningful_tob(self, ob_data: Dict, side: str, min_notional: float = 5.0) -> float:
        """
        Scans the orderbook to find the true top-of-book, ignoring dust/dust-spam orders
        that aren't large enough to provide actual liquidity.
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
        ob_response = await self.executor.safe_call(self.executor.client.get_orderbook, category="linear", symbol=symbol)
        ob_data = ob_response.get("result", {})
        return self._get_meaningful_tob({"bids": ob_data.get("b", []), "asks": ob_data.get("a", [])}, side)

    async def cancel_order_safe(self, symbol: str, order_id: str) -> bool:
        """
        Queries open orders before canceling to eliminate Bybit Error 110001 (Order does not exist).
        """
        for attempt in range(3):
            try:
                open_orders = await self.executor.safe_call(
                    self.executor.client.get_open_orders, 
                    category="linear", symbol=symbol, orderId=order_id
                )
                open_list = open_orders.get("result", {}).get("list", [])
                
                # If order is not in open list, it either filled or was already canceled
                if not open_list:
                    return True

                res = await self.executor.safe_call(
                    self.executor.client.cancel_order, 
                    category="linear", symbol=symbol, orderId=order_id
                )
                if res.get("retCode") == 0:
                    return True
            except Exception as e:
                err_str = str(e)
                if "110001" in err_str or "not exists" in err_str or "too late" in err_str:
                    logger.debug(f"[X-RAY] Safe cancel verified: Order {order_id} no longer active.")
                    return True
                await asyncio.sleep(0.5)
        return False

    async def _execute_flash_strike(self, symbol: str, direction: str, qty: float, current_mid_price: float, sl: Optional[float] = None, tp: Optional[float] = None) -> Tuple[bool, float, float]:
        """
        Aggressive IOC Execution with robust NoneType and empty-string guards for price/qty parameters.
        Escalates price through orderbook depth to guarantee fill during extreme momentum, 
        capped at a strict max slippage limit.
        """
        logger.critical(f"[X-RAY] ⚡ FLASH STRIKE AUTHORIZED // {symbol} executing aggressive momentum escalation.")
        
        # Defensive fallback if stop loss or take profit are missing/None
        if sl is None or tp is None or sl == tp:
            implied_sl_dist = current_mid_price * 0.008
            implied_tp_dist = implied_sl_dist * 2.0
            if direction.upper() == "BUY":
                sl = current_mid_price - implied_sl_dist
                tp = current_mid_price + implied_tp_dist
            else:
                sl = current_mid_price + implied_sl_dist
                tp = current_mid_price - implied_tp_dist

        implied_sl_dist = abs(tp - sl) / 3.0 if (tp and sl and tp != sl) else (current_mid_price * 0.008)
        implied_tp_dist = implied_sl_dist * 2.0
        
        if direction.upper() == "BUY":
            if sl >= current_mid_price: sl = current_mid_price - implied_sl_dist
            if tp <= current_mid_price: tp = current_mid_price + implied_tp_dist
        else:
            if sl <= current_mid_price: sl = current_mid_price + implied_sl_dist
            if tp >= current_mid_price: tp = current_mid_price - implied_tp_dist
            
        # --- SAFETY CLAMP FOR FLASH STRIKE ---
        if direction.upper() == "BUY":
            safe_sl = min(sl, current_mid_price * 0.999) if sl else 0.0
            safe_tp = max(tp, current_mid_price * 1.001) if tp else 0.0
        else:
            safe_sl = max(sl, current_mid_price * 1.001) if sl else 0.0
            safe_tp = min(tp, current_mid_price * 0.999) if tp else 0.0
            
        cleaned_qty = self._apply_dynamic_exchange_limits(qty, current_mid_price, symbol)
        final_sl = self._format_dynamic_price(safe_sl, symbol) if safe_sl else 0.0
        final_tp = self._format_dynamic_price(safe_tp, symbol) if safe_tp else 0.0
        side = "Buy" if direction.upper() == "BUY" else "Sell"

        for attempt in range(3):
            escalation_base = 0.0002
            escalation_pct = escalation_base * (2 ** attempt)
            escalation_pct = min(escalation_pct, self.max_slippage_pct)
            
            if side == "Buy": target_price = current_mid_price * (1.0 + escalation_pct)
            else: target_price = current_mid_price * (1.0 - escalation_pct)
                
            final_price = self._format_dynamic_price(target_price, symbol)

            logger.info(f"[X-RAY] ⚡ Flash Strike Attempt {attempt+1}/3 // {side} {cleaned_qty} {symbol} at {final_price}")

            try:
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
                    await asyncio.sleep(0.3) # Allow matching engine to settle
                    
                    hist_res = await self.executor.safe_call(
                        self.executor.client.get_order_history,
                        category="linear", symbol=symbol, orderId=order_id, limit=1
                    )
                    orders = hist_res.get("result", {}).get("list", [])
                    
                    if orders:
                        raw_exec = orders[0].get("cumExecQty")
                        raw_avg = orders[0].get("avgPrice")
                        
                        cum_exec = float(raw_exec) if (raw_exec is not None and str(raw_exec).strip() != "") else 0.0
                        avg_price = float(raw_avg) if (raw_avg is not None and str(raw_avg).strip() != "") else current_mid_price

                        if cum_exec > 0:
                            logger.critical(f"✅ FLASH STRIKE SUCCESS // {symbol} filled {cum_exec} units at {avg_price} on attempt {attempt+1}.")
                            return True, avg_price, cum_exec
                        else:
                            logger.warning(f"[X-RAY] ⚠️ Flash Strike IOC missed (Liquidity vanished before execution). Escalating...")
                    else:
                        logger.warning(f"[X-RAY] ⚠️ API history delayed. Cannot verify fill for ID: {order_id}. Assuming missed.")
                else:
                    logger.warning(f"[X-RAY] ⚠️ API rejection (Attempt {attempt+1}): {response.get('retMsg')}")
                    await asyncio.sleep(0.1) 
                    
            except Exception as e:
                logger.error(f"[X-RAY] ⚠️ Network Exception during Flash Strike for {symbol}: {e}")
                
        logger.error(f"[X-RAY] ❌ Flash Strike failed permanently after 3 escalation attempts. Order book evaporated or Slippage Cap hit.")
        return False, 0.0, 0.0

    async def _execute_dynamic_maker_peg(self, symbol: str, direction: str, qty: float, sl: Optional[float], tp: Optional[float], feature_engine=None, depth_snapshot: dict=None, timeout: int = 12) -> Tuple[bool, float, float]:
        """
        🚀 V55.2 MAKER-GRID SPREAD CAPTURE:
        Tighter chasing rules. Drops timeout to 12s and max rejections to 3.
        If the market breaks away, it gracefully surrenders the peg to avoid taking a late, poor entry.
        """
        logger.info(f"🛡️ HFT MAKER-PEGGING INITIATED // {symbol}. Engaging Spread Capture & Anti-Spoofing Scanners.")
        
        start_time = time.time()
        current_order_id = None
        side = "Buy" if direction.upper() == "BUY" else "Sell"
        
        anchor_price = None
        max_chase_deviation = 0.005  # 🚀 V55.2: Tighter chase abort (0.5% max drift from anchor)
        rejection_count = 0  

        tick_size = self.instrument_cache.get(symbol, {"tick_size": 0.01})["tick_size"]

        # Default SL/TP safety fallbacks if None passed
        if sl is None or tp is None:
            current_mid = depth_snapshot.get("bids", [[100, 1]])[0][0] if depth_snapshot else 100.0
            sl = current_mid * 0.99 if side == "Buy" else current_mid * 1.01
            tp = current_mid * 1.02 if side == "Buy" else current_mid * 0.98

        while time.time() - start_time < timeout:
            loop_delay = 1.0

            try:
                # 1. Check for Orderbook Toxicity (Adverse Selection Guard)
                if feature_engine and hasattr(feature_engine, 'get_book_depth_metrics'):
                    depth_metrics = feature_engine.get_book_depth_metrics()
                    imbalance = depth_metrics.get("depth_imbalance", 0.0)
                    
                    if side == "Buy" and imbalance < -0.80:
                        logger.warning(f"[X-RAY] 🛡️ ADVERSE SELECTION GUARD // {symbol} book toxic (sell wall detected: {imbalance:.2f}). Aborting peg to prevent getting dumped on.")
                        break
                    elif side == "Sell" and imbalance > 0.80:
                        logger.warning(f"[X-RAY] 🛡️ ADVERSE SELECTION GUARD // {symbol} book toxic (buy wall detected: {imbalance:.2f}). Aborting peg to prevent getting squeezed.")
                        break

                # 2. Fetch LIVE Orderbook Snapshot
                target_price = 0.0
                fresh_ob = feature_engine.get_orderbook_snapshot() if feature_engine and hasattr(feature_engine, 'get_orderbook_snapshot') else None
                
                if fresh_ob and "bids" in fresh_ob and "asks" in fresh_ob and len(fresh_ob["bids"]) > 0:
                    target_price = self._get_meaningful_tob(fresh_ob, side)
                    if target_price > 0.0: loop_delay = 0.5
                else:
                    target_price = await self._fetch_rest_tob(symbol, side)
                    
                if target_price <= 0:
                    await asyncio.sleep(loop_delay); continue

                if anchor_price is None: anchor_price = target_price

                # 3. Check Chase Deviation Limits
                if direction.upper() == "BUY" and target_price > anchor_price * (1 + max_chase_deviation):
                    logger.warning(f"[X-RAY] 🏃 CHASE ABORTED // {symbol} ran +{max_chase_deviation:.2%} beyond signal anchor. Surrendering peg.")
                    break
                if direction.upper() == "SELL" and target_price < anchor_price * (1 - max_chase_deviation):
                    logger.warning(f"[X-RAY] 🏃 CHASE ABORTED // {symbol} ran -{max_chase_deviation:.2%} beyond signal anchor. Surrendering peg.")
                    break

                # 4. Format SL/TP cleanly based on actual target price
                implied_sl_dist = abs(tp - sl) / 3.0 if (tp and sl and tp != sl) else (target_price * 0.008)
                implied_tp_dist = implied_sl_dist * 2.0
                
                current_sl, current_tp = sl, tp
                if direction.upper() == "BUY":
                    if current_sl >= target_price: current_sl = target_price - implied_sl_dist
                    if current_tp <= target_price: current_tp = target_price + implied_tp_dist
                else:
                    if current_sl <= target_price: current_sl = target_price + implied_sl_dist
                    if current_tp >= target_price: current_tp = target_price - implied_tp_dist

                cleaned_qty = self._apply_dynamic_exchange_limits(qty, target_price, symbol)
                final_target_price = self._format_dynamic_price(target_price, symbol)
                final_sl = self._format_dynamic_price(current_sl, symbol) if current_sl else 0.0
                final_tp = self._format_dynamic_price(current_tp, symbol) if current_tp else 0.0
                
                # 5. Place or Amend Order
                if not current_order_id:
                    logger.info(f"[X-RAY] 🛡️ Placing initial PostOnly Maker Peg for {symbol} at {final_target_price}")
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
                        logger.warning(f"[X-RAY] PostOnly placement rejected: {place_response.get('retMsg')}")
                        # 🚀 V55.2: Trip circuit breaker after 3 rejections instead of 5
                        if rejection_count >= 3:
                            logger.error(f"[X-RAY] 🛑 PEG CIRCUIT BREAKER TRIPPED // {symbol} PostOnly rejected 3 times. Market is likely running away.")
                            break
                        await asyncio.sleep(loop_delay); continue
                
                if current_order_id:
                    status_response = await self.executor.safe_call(self.executor.client.get_open_orders, category="linear", symbol=symbol, orderId=current_order_id)
                    order_list = status_response.get("result", {}).get("list", [])
                    
                    if not order_list:
                        # Order is gone from active list. Verify if it filled.
                        await asyncio.sleep(0.5) 
                        hist_response = await self.executor.safe_call(self.executor.client.get_order_history, category="linear", symbol=symbol, orderId=current_order_id, limit=1)
                        hist_list = hist_response.get("result", {}).get("list", [])
                        
                        if hist_list:
                            raw_exec = hist_list[0].get("cumExecQty")
                            raw_avg = hist_list[0].get("avgPrice")
                            
                            cum_exec = float(raw_exec) if (raw_exec is not None and str(raw_exec).strip() != "") else 0.0
                            avg_price = float(raw_avg) if (raw_avg is not None and str(raw_avg).strip() != "") else final_target_price

                            if cum_exec > 0:
                                logger.critical(f"✅ MAKER PEG RESOLVED // {symbol} secured {cum_exec} units at {avg_price}. Spread Captured!")
                                return True, avg_price, cum_exec
                        
                        # If not in history, it was likely canceled. Reset to try again.
                        current_order_id = None
                        continue
                            
                    order_info = order_list[0]
                    order_status = order_info.get("orderStatus")
                    
                    raw_price = order_info.get("price")
                    raw_exec_qty = order_info.get("cumExecQty")
                    raw_avg = order_info.get("avgPrice")

                    current_peg_price = float(raw_price) if (raw_price is not None and str(raw_price).strip() != "") else final_target_price
                    cum_exec_qty = float(raw_exec_qty) if (raw_exec_qty is not None and str(raw_exec_qty).strip() != "") else 0.0
                    avg_price = float(raw_avg) if (raw_avg is not None and str(raw_avg).strip() != "") else current_peg_price
                    
                    if order_status in ["Filled"]:
                        logger.critical(f"✅ MAKER PEG SECURED // {symbol} filled completely. Earned Maker Rebates.")
                        return True, avg_price, cum_exec_qty
                        
                    elif order_status in ["Cancelled", "Rejected"]: 
                        rejection_count += 1
                        current_order_id = None 
                        logger.warning(f"[X-RAY] ⚠️ Maker Peg Cancelled by Exchange (Likely spread cross). Retrying with fresh TOB.")
                        if cum_exec_qty > 0:
                            logger.critical(f"✅ MAKER PEG PARTIAL // {symbol} secured {cum_exec_qty} units before rejection.")
                            return True, avg_price, cum_exec_qty
                            
                        # 🚀 V55.2: Trip circuit breaker after 3 rejections
                        if rejection_count >= 3:
                            logger.error(f"[X-RAY] 🛑 PEG CIRCUIT BREAKER TRIPPED // {symbol} canceled/rejected 3 times. Aborting.")
                            break
                            
                    elif order_status in ["New", "PartiallyFilled"]:
                        # Only amend if price drifted significantly (> 1 tick)
                        if abs(final_target_price - current_peg_price) >= tick_size:
                            logger.info(f"[X-RAY] 🔄 Amending Maker Peg from {current_peg_price} to new Top-of-Book {final_target_price}")
                            await self.executor.safe_call(self.executor.client.amend_order, category="linear", symbol=symbol, orderId=current_order_id, price=str(final_target_price))

            except Exception as e: 
                logger.debug(f"[X-RAY] Maker peg cycle variance for {symbol}: {e}")
                
            await asyncio.sleep(loop_delay) 

        # Timeout Handler
        if current_order_id:
            logger.warning(f"[X-RAY] ⏳ MAKER CHASE TIMEOUT // {timeout}s elapsed. Market escaped {symbol} peg range. Canceling to protect capital.")
            cancel_success = await self.cancel_order_safe(symbol, current_order_id)
            
            if not cancel_success:
                logger.critical(f"🛑 ORPHAN ORDER ALERT // Failed to cancel peg order {current_order_id} for {symbol}. Manual intervention may be needed.")
                
            try:
                hist_res = await self.executor.safe_call(self.executor.client.get_order_history, category="linear", symbol=symbol, orderId=current_order_id, limit=1)
                hist_list = hist_res.get("result", {}).get("list", [])
                if hist_list:
                    raw_exec = hist_list[0].get("cumExecQty")
                    raw_avg = hist_list[0].get("avgPrice")
                    
                    cum_exec = float(raw_exec) if (raw_exec is not None and str(raw_exec).strip() != "") else 0.0
                    avg_price = float(raw_avg) if (raw_avg is not None and str(raw_avg).strip() != "") else anchor_price
                    if cum_exec > 0:
                        return True, avg_price, cum_exec
            except Exception: pass
            
        return False, 0.0, 0.0

    async def _execute_twap_iceberg(self, symbol: str, direction: str, total_qty: float, current_mid_price: float, sl: float, tp: float, slices: int = 4, slice_interval_sec: float = 5.0) -> Tuple[bool, float, float]:
        """
        Time-Weighted Iceberg Execution with Patched SL/TP propagation.
        Slices massive institutional orders into undetectable smaller chunks executed sequentially.
        """
        logger.critical(f"[X-RAY] 🧊 ICEBERG ENGAGED // {symbol} large notional size detected. Slicing order into {slices} TWAP chunks.")
        
        slice_qty = total_qty / slices
        total_executed_qty = 0.0
        weighted_notional_sum = 0.0
        
        for i in range(slices):
            logger.info(f"[X-RAY] 🧊 TWAP SLICE [{i+1}/{slices}] // Routing {slice_qty:.4f} {symbol}")
            
            # 🚀 V55.2: Iceberg peg chunks use even tighter 8-second timeouts to avoid staggering
            success, fill_price, fill_qty = await self._execute_dynamic_maker_peg(
                symbol=symbol, direction=direction, qty=slice_qty, sl=sl, tp=tp, timeout=8
            )
            
            if not success or fill_qty == 0:
                logger.warning(f"[X-RAY] 🧊 TWAP SLICE FAILED // Maker Peg rejected. Escalating slice to Flash Strike with valid SL/TP floats.")
                success, fill_price, fill_qty = await self._execute_flash_strike(
                    symbol=symbol, direction=direction, qty=slice_qty, current_mid_price=current_mid_price, sl=sl, tp=tp
                )
                
            if fill_qty > 0:
                total_executed_qty += fill_qty
                weighted_notional_sum += (fill_price * fill_qty)
                
            if i < slices - 1:
                await asyncio.sleep(slice_interval_sec)
                
        if total_executed_qty > 0:
            avg_fill_price = weighted_notional_sum / total_executed_qty
            side = "Buy" if direction.upper() == "BUY" else "Sell"
            tick_size = self.instrument_cache.get(symbol, {"tick_size": 0.01})["tick_size"]
            def align_price(p: float) -> str: return str(Decimal(str(p)).quantize(Decimal(str(tick_size)), rounding=ROUND_HALF_UP))
            
            sl_dist_pct = abs(sl - current_mid_price) / (current_mid_price + 1e-9)
            tp_dist_pct = abs(tp - current_mid_price) / (current_mid_price + 1e-9)
            
            if side == "Buy":
                actual_sl = avg_fill_price * (1.0 - sl_dist_pct)
                actual_tp = avg_fill_price * (1.0 + tp_dist_pct)
            else:
                actual_sl = avg_fill_price * (1.0 + sl_dist_pct)
                actual_tp = avg_fill_price * (1.0 - tp_dist_pct)
            
            try:
                await self.executor.safe_call(
                    self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=self.position_idx, 
                    takeProfit=align_price(actual_tp), stopLoss=align_price(actual_sl)
                )
                logger.info(f"[X-RAY] 🛡️ Bracket synchronized to Avg Fill {avg_fill_price:.5f} | SL: {actual_sl:.5f} | TP: {actual_tp:.5f}")
            except Exception as e:
                logger.warning(f"[X-RAY] 🧊 Failed to reattach bracket to TWAP position: {e}")
                
            logger.critical(f"✅ ICEBERG COMPLETE // {symbol} secured {total_executed_qty:.4f} total units at avg price {avg_fill_price:.4f}.")
            return True, avg_fill_price, total_executed_qty
            
        logger.error(f"[X-RAY] ❌ ICEBERG FAILED // {symbol} could not secure any slices. Evaporated liquidity.")
        return False, 0.0, 0.0

    async def execute_iceberg_block(self, symbol: str, direction: str, total_qty: float, current_mid_price: float, stop_loss: float = None, take_profit: float = None, depth_snapshot: dict = None, vol_z: float = 0.0, vol_mult: float = 1.0, feature_engine: Any = None, **kwargs) -> Tuple[bool, float, float]:
        await self._fetch_exchange_limits(symbol)
        
        is_large_order = False
        if depth_snapshot and "bids" in depth_snapshot and "asks" in depth_snapshot:
            top_bid_vol = sum(float(l[1]) for l in depth_snapshot["bids"][:3])
            top_ask_vol = sum(float(l[1]) for l in depth_snapshot["asks"][:3])
            avg_tob_vol = (top_bid_vol + top_ask_vol) / 2.0
            if avg_tob_vol > 0 and total_qty > (avg_tob_vol * 0.05):
                is_large_order = True
        
        if is_large_order:
            logger.info(f"[X-RAY] 🐋 WHALE ROUTING // {symbol} size > 5% of Top-of-Book depth. Triggering Iceberg Protocol.")
            return await self._execute_twap_iceberg(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit)
        
        logger.info(f"[X-RAY] 🚀 TRENDING REGIME ROUTING // Initiating high-speed dispatch for {symbol} {direction}")
        if abs(vol_z) >= 1.5 or vol_mult >= 1.5:
            return await self._execute_flash_strike(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit)
        else:
            return await self._execute_dynamic_maker_peg(symbol, direction, total_qty, stop_loss, take_profit, feature_engine=feature_engine, depth_snapshot=depth_snapshot, timeout=12)

    async def execute_mean_reversion_bracket(self, symbol: str, direction: str, total_qty: float, current_mid_price: float, stop_loss: float = None, take_profit: float = None, depth_snapshot: dict = None, vol_z: float = 0.0, vol_mult: float = 1.0, feature_engine: Any = None, **kwargs) -> Tuple[bool, float, float]:
        await self._fetch_exchange_limits(symbol)
        
        is_large_order = False
        if depth_snapshot and "bids" in depth_snapshot and "asks" in depth_snapshot:
            top_bid_vol = sum(float(l[1]) for l in depth_snapshot["bids"][:3])
            top_ask_vol = sum(float(l[1]) for l in depth_snapshot["asks"][:3])
            avg_tob_vol = (top_bid_vol + top_ask_vol) / 2.0
            if avg_tob_vol > 0 and total_qty > (avg_tob_vol * 0.05):
                is_large_order = True
                
        if is_large_order:
            logger.info(f"[X-RAY] 🐋 WHALE ROUTING // {symbol} size > 5% of Top-of-Book depth. Triggering Iceberg Protocol.")
            return await self._execute_twap_iceberg(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit)

        logger.info(f"[X-RAY] 🕸️ RANGING REGIME ROUTING // Forcing Maker-Grid Peg on {symbol} to capture spread edge.")
        # 🚀 V55.2 FIX: Strict 12s timeout for Maker Grids. No bad chases.
        return await self._execute_dynamic_maker_peg(symbol, direction, total_qty, stop_loss, take_profit, feature_engine=feature_engine, depth_snapshot=depth_snapshot, timeout=12)