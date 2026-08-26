"""
💎 V5.2 APEX NEURAL: INSTITUTIONAL SMART ORDER ROUTER
--------------------------------------------------------
Features Atomic Probability Routing, Arrival Price Caching (IS Tracking),
Maker-Grid Spread Capture, Dynamic Slippage Firewalls, PostOnly Pegging, 
and Null-Guard Parity.
Upgraded with V5.2 Elimination of Adverse Selection Fallback Chasing.
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
    """
    Dynamically routes executions across Flash Strike (Taker), Maker Peg (PostOnly), 
    and TWAP Iceberg slices to minimize execution drag and slippage.
    """
    def __init__(self, executor: BybitUnifiedExecutor, max_slippage_pct: float = 0.0012):
        self.executor = executor
        self.base_max_slippage_pct = max_slippage_pct
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
        if (qty * price) < 6.50: 
            qty = 6.50 / (price + 1e-9)
            
        if qty < required_min_qty: 
            qty = required_min_qty
            
        stepped_qty = math.floor(qty / qty_step) * qty_step
        final_qty = round(stepped_qty, self._get_precision(qty_step))
        
        # Secondary Verification
        if (final_qty * price) < 5.50:
            final_qty += qty_step
            final_qty = round(final_qty, self._get_precision(qty_step))

        return final_qty

    def compute_dynamic_slippage_cap_bps(self, symbol: str, regime: str, live_spread_bps: float) -> float:
        """
        🚀 V5.1 DYNAMIC SLIPPAGE CAP CALCULATOR
        Adapts allowable slippage based on asset class, live spread, and regime.
        """
        is_major = symbol in ["BTCUSDT", "ETHUSDT"]
        is_high_cap = symbol in ["SOLUSDT", "SUIUSDT", "AVAXUSDT", "LINKUSDT", "NEARUSDT", "APTUSDT"]
        
        # Base allowance derived from live spread
        spread_multiplier = 2.0 if is_major else 2.5
        calculated_cap = max(8.0, live_spread_bps * spread_multiplier)
        
        # Regime expansion: Trending market breakouts grant additional buffer
        if regime == "TRENDING":
            calculated_cap += 5.0
            
        # Hard asset class ceilings
        if is_major:
            return min(15.0, calculated_cap)      # BTC/ETH: Max 15 bps
        elif is_high_cap:
            return min(25.0, calculated_cap)      # SOL/SUI/AVAX: Max 25 bps
        else:
            return min(40.0, calculated_cap)      # Dynamic Alts: Max 40 bps

    def estimate_orderbook_slippage_bps(self, depth_snapshot: Dict, side: str, qty: float, current_mid: float) -> float:
        """
        🛡️ PRE-TRADE L2 SLIPPAGE FIREWALL
        Simulates walking the orderbook for `qty` to calculate expected fill price.
        Returns expected slippage in basis points.
        """
        if not depth_snapshot or "bids" not in depth_snapshot or "asks" not in depth_snapshot:
            return 0.0

        levels = depth_snapshot.get("asks" if side.upper() == "BUY" else "bids", [])
        if not levels: return 0.0

        accumulated_qty = 0.0
        accumulated_cost = 0.0

        for level in levels:
            try:
                p = float(level[0])
                v = float(level[1])
                needed = qty - accumulated_qty
                
                if v >= needed:
                    accumulated_cost += (needed * p)
                    accumulated_qty += needed
                    break
                else:
                    accumulated_cost += (v * p)
                    accumulated_qty += v
            except (IndexError, ValueError):
                continue

        if accumulated_qty < qty or accumulated_qty == 0:
            return 999.0 # Depleted depth = infinite slippage

        avg_expected_price = accumulated_cost / accumulated_qty
        if side.upper() == "BUY":
            slippage_bps = ((avg_expected_price - current_mid) / current_mid) * 10000.0
        else:
            slippage_bps = ((current_mid - avg_expected_price) / current_mid) * 10000.0

        return max(0.0, slippage_bps)

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
        Escalates price through orderbook depth to guarantee fill during extreme momentum.
        """
        logger.critical(f"[X-RAY] ⚡ FLASH STRIKE AUTHORIZED // {symbol} executing aggressive momentum escalation.")
        
        # 🚀 V5.1 AUDIT FIX: Defensive fallback if stop loss or take profit are missing/None.
        if sl is None or tp is None or sl == tp:
            implied_sl_dist = current_mid_price * 0.025
            implied_tp_dist = implied_sl_dist * 2.0
            if direction.upper() == "BUY":
                sl = current_mid_price - implied_sl_dist
                tp = current_mid_price + implied_tp_dist
            else:
                sl = current_mid_price + implied_sl_dist
                tp = current_mid_price - implied_tp_dist

        cleaned_qty = self._apply_dynamic_exchange_limits(qty, current_mid_price, symbol)
        final_sl = self._format_dynamic_price(sl, symbol) if sl else 0.0
        final_tp = self._format_dynamic_price(tp, symbol) if tp else 0.0
        side = "Buy" if direction.upper() == "BUY" else "Sell"

        for attempt in range(3):
            escalation_base = 0.0002
            escalation_pct = escalation_base * (2 ** attempt)
            escalation_pct = min(escalation_pct, self.base_max_slippage_pct * 2.0)
            
            if side == "Buy": target_price = current_mid_price * (1.0 + escalation_pct)
            else: target_price = current_mid_price * (1.0 - escalation_pct)
                
            final_price = self._format_dynamic_price(target_price, symbol)

            logger.info(f"[X-RAY] ⚡ Flash Strike Attempt {attempt+1}/3 // {side} {cleaned_qty} {symbol} at {final_price}")

            try:
                # Place limit IOC order (Stops attached post-fill to avoid immediate rejection)
                response = await self.executor.safe_call(
                    self.executor.client.place_order,
                    category="linear", symbol=symbol, side=side, orderType="Limit", 
                    qty=str(cleaned_qty), price=str(final_price), timeInForce="IOC", 
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
                            
                            # Attach TP/SL exactly to the average fill price
                            if final_sl or final_tp:
                                try:
                                    await self.executor.safe_call(
                                        self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=self.position_idx, 
                                        stopLoss=str(final_sl) if final_sl else None, takeProfit=str(final_tp) if final_tp else None
                                    )
                                    logger.info(f"[X-RAY] 🛡️ Stops successfully attached to Flash Strike: SL {final_sl} | TP {final_tp}")
                                except Exception as e:
                                    logger.error(f"[X-RAY] 🛑 FATAL: Failed to attach stops to Flash Strike for {symbol}: {e}")
                                    
                            return True, avg_price, cum_exec
                        else:
                            logger.warning(f"[X-RAY] ⚠️ Flash Strike IOC missed (Liquidity vanished before execution). Escalating...")
                    else:
                        logger.warning(f"[X-RAY] ⚠️ API history delayed. Cannot verify fill for ID: {order_id}. Assuming missed.")
                else:
                    logger.warning(f"[X-RAY] ⚠️ API rejection (Attempt {attempt+1}): {response.get('retMsg')}")
                    await asyncio.sleep(0.1) 
                    
            except Exception as e:
                error_str = str(e)
                logger.error(f"[X-RAY] ⚠️ Network Exception during Flash Strike for {symbol}: {error_str}")
                
                # 🚀 V5.1 INSTANT SHATTER: Break out of Flash Strike loops on fatal blocks
                if any(fatal in error_str for fatal in ["110126", "INNOVATION ZONE", "10002", "10001"]):
                    logger.error(f"[X-RAY] 🛑 FATAL BLOCK // {symbol} is banned or invalid. Shattering Flash Strike loop instantly.")
                    break
                
        logger.error(f"[X-RAY] ❌ Flash Strike failed permanently after 3 escalation attempts. Order book evaporated or Slippage Cap hit.")
        return False, 0.0, 0.0

    async def _execute_dynamic_maker_peg(self, symbol: str, direction: str, qty: float, sl: Optional[float], tp: Optional[float], feature_engine=None, depth_snapshot: dict=None, timeout: int = 12) -> Tuple[bool, float, float]:
        """
        🛡️ MAKER-GRID SPREAD CAPTURE:
        Tries to capture spread rebates with PostOnly limit orders. Drops execution if 
        adverse selection is detected (Micro-price absorbing against us).
        """
        logger.info(f"🛡️ HFT MAKER-PEGGING INITIATED // {symbol}. Engaging Spread Capture & Anti-Spoofing Scanners. (Timeout: {timeout}s)")
        
        start_time = time.time()
        current_order_id = None
        side = "Buy" if direction.upper() == "BUY" else "Sell"
        
        anchor_price = None
        max_chase_deviation = 0.005  
        rejection_count = 0  

        tick_size = self.instrument_cache.get(symbol, {"tick_size": 0.01})["tick_size"]

        if sl is None or tp is None:
            current_mid = depth_snapshot.get("bids", [[100, 1]])[0][0] if depth_snapshot else 100.0
            sl = current_mid * 0.975 if side == "Buy" else current_mid * 1.025
            tp = current_mid * 1.050 if side == "Buy" else current_mid * 0.950

        while time.time() - start_time < timeout:
            loop_delay = 1.0

            try:
                # 1. Check for Orderbook Toxicity (Adverse Selection Guard)
                if feature_engine and hasattr(feature_engine, 'get_book_depth_metrics'):
                    depth_metrics = feature_engine.get_book_depth_metrics()
                    imbalance = depth_metrics.get("depth_imbalance", 0.0)
                    
                    if direction.upper() == "BUY" and imbalance > 0.80:
                        logger.warning(f"[X-RAY] 🚫 ADVERSE SELECTION // {symbol} Bid wall collapsing. Aborting peg to prevent bad entry.")
                        break
                    elif direction.upper() == "SELL" and imbalance < -0.80:
                        logger.warning(f"[X-RAY] 🚫 ADVERSE SELECTION // {symbol} Ask wall collapsing. Aborting peg to prevent bad entry.")
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
                implied_sl_dist = abs(tp - sl) / 3.0 if (tp and sl and tp != sl) else (target_price * 0.025)
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
                            
                        if rejection_count >= 3:
                            logger.error(f"[X-RAY] 🛑 PEG CIRCUIT BREAKER TRIPPED // {symbol} canceled/rejected 3 times. Aborting.")
                            break
                            
                    elif order_status in ["New", "PartiallyFilled"]:
                        # Only amend if price drifted significantly (> 1 tick)
                        if abs(final_target_price - current_peg_price) >= tick_size:
                            logger.info(f"[X-RAY] 🔄 Amending Maker Peg from {current_peg_price} to new Top-of-Book {final_target_price}")
                            await self.executor.safe_call(self.executor.client.amend_order, category="linear", symbol=symbol, orderId=current_order_id, price=str(final_target_price))

            except Exception as e: 
                error_str = str(e)
                logger.warning(f"[X-RAY] ⚠️ Maker peg cycle variance for {symbol}: {error_str}")
                
                # 🚀 V5.1 INSTANT SHATTER: Do not wait timeout seconds if the coin is banned
                if any(fatal in error_str for fatal in ["110126", "INNOVATION ZONE", "10002", "10001"]):
                    logger.error(f"[X-RAY] 🛑 FATAL BLOCK // {symbol} is banned or invalid. Shattering Maker Peg loop instantly.")
                    break
                    
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
        
        # Dynamic Maker Timeouts for Iceberg Chunks
        is_major_asset = symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        chunk_timeout = 8.0 if is_major_asset else 15.0
        
        for i in range(slices):
            logger.info(f"[X-RAY] 🧊 TWAP SLICE [{i+1}/{slices}] // Routing {slice_qty:.4f} {symbol}")
            
            success, fill_price, fill_qty = await self._execute_dynamic_maker_peg(
                symbol=symbol, direction=direction, qty=slice_qty, sl=sl, tp=tp, timeout=chunk_timeout
            )
            
            # CRITICAL FIX: If a slice misses its peg, DO NOT flash strike it. Record zero fill and move on.
            if not success or fill_qty == 0:
                logger.warning(f"[X-RAY] 🧊 TWAP SLICE FAILED // Maker Peg rejected. Skipping slice to prevent adverse selection.")
            else:
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

    async def execute_iceberg_block(self, symbol: str, direction: str, total_qty: float, current_mid_price: float, stop_loss: float = None, take_profit: float = None, depth_snapshot: dict = None, vol_z: float = 0.0, vol_mult: float = 1.0, feature_engine: Any = None, regime: str = "TRENDING", **kwargs) -> Tuple[bool, float, float, float]:
        await self._fetch_exchange_limits(symbol)
        arrival_price = current_mid_price
        
        # 🛡️ V5.1 DYNAMIC SLIPPAGE FIREWALL
        ob = depth_snapshot or {}
        best_bid = float(ob.get("bids", [[current_mid_price, 1]])[0][0])
        best_ask = float(ob.get("asks", [[current_mid_price, 1]])[0][0])
        live_spread_bps = ((best_ask - best_bid) / (best_bid + 1e-9)) * 10000.0 if best_bid > 0 else 1.0
        
        dynamic_cap_bps = self.compute_dynamic_slippage_cap_bps(symbol, regime, live_spread_bps)
        est_slippage = self.estimate_orderbook_slippage_bps(depth_snapshot, direction, total_qty, current_mid_price)
        
        if est_slippage > dynamic_cap_bps:
            logger.warning(f"[X-RAY] 🛑 DYNAMIC FIREWALL REJECT // {symbol} est. slippage {est_slippage:.1f} bps > Cap {dynamic_cap_bps:.1f} bps. Aborting.")
            return False, arrival_price, 0.0, 0.0

        is_large_order = False
        if depth_snapshot and "bids" in depth_snapshot and "asks" in depth_snapshot:
            top_bid_vol = sum(float(l[1]) for l in depth_snapshot["bids"][:3])
            top_ask_vol = sum(float(l[1]) for l in depth_snapshot["asks"][:3])
            avg_tob_vol = (top_bid_vol + top_ask_vol) / 2.0
            if avg_tob_vol > 0 and total_qty > (avg_tob_vol * 0.05):
                is_large_order = True
        
        if is_large_order:
            logger.info(f"[X-RAY] 🐋 WHALE ROUTING // {symbol} size > 5% of Top-of-Book depth. Triggering Iceberg Protocol.")
            success, f_price, f_qty = await self._execute_twap_iceberg(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit)
            return success, arrival_price, f_price, f_qty
        
        logger.info(f"[X-RAY] 🚀 TRENDING REGIME ROUTING // Initiating high-speed dispatch for {symbol} {direction}")
        if abs(vol_z) >= 1.5 or vol_mult >= 1.5:
            success, f_price, f_qty = await self._execute_flash_strike(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit)
            return success, arrival_price, f_price, f_qty
        else:
            is_major_asset = symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
            dynamic_timeout = 12.0 if is_major_asset else 25.0
            
            success, f_price, f_qty = await self._execute_dynamic_maker_peg(
                symbol, direction, total_qty, stop_loss, take_profit, 
                feature_engine=feature_engine, depth_snapshot=depth_snapshot, timeout=dynamic_timeout
            )
            
            # CRITICAL FIX: Abort instead of chasing.
            if not success or f_qty == 0:
                logger.info(f"[X-RAY] 🛑 MAKER PEG UNFILLED // Safely canceling order for {symbol}. Trade aborted.")
                return False, arrival_price, 0.0, 0.0
                
            return success, arrival_price, f_price, f_qty

    async def execute_mean_reversion_bracket(self, symbol: str, direction: str, total_qty: float, current_mid_price: float, stop_loss: float = None, take_profit: float = None, depth_snapshot: dict = None, vol_z: float = 0.0, vol_mult: float = 1.0, feature_engine: Any = None, regime: str = "MEAN_REVERTING", **kwargs) -> Tuple[bool, float, float, float]:
        """
        Deterministic Router: Executes pure Maker Peg with cancellation on drift,
        or pure Flash Strike IOC during high volatility. NEVER escalates failed makers to market orders.
        Returns Tuple[Success, Arrival_Price, Fill_Price, Filled_Qty]
        """
        await self._fetch_exchange_limits(symbol)
        arrival_price = current_mid_price
        
        # 🛡️ V5.1 DYNAMIC SLIPPAGE FIREWALL
        ob = depth_snapshot or {}
        best_bid = float(ob.get("bids", [[current_mid_price, 1]])[0][0])
        best_ask = float(ob.get("asks", [[current_mid_price, 1]])[0][0])
        live_spread_bps = ((best_ask - best_bid) / (best_bid + 1e-9)) * 10000.0 if best_bid > 0 else 1.0
        
        dynamic_cap_bps = self.compute_dynamic_slippage_cap_bps(symbol, regime, live_spread_bps)
        est_slippage = self.estimate_orderbook_slippage_bps(depth_snapshot, direction, total_qty, current_mid_price)
        
        if est_slippage > dynamic_cap_bps:
            logger.warning(f"[X-RAY] 🛑 DYNAMIC FIREWALL REJECT // {symbol} est. slippage {est_slippage:.1f} bps > Cap {dynamic_cap_bps:.1f} bps. Aborting.")
            return False, arrival_price, 0.0, 0.0

        is_large_order = False
        if depth_snapshot and "bids" in depth_snapshot and "asks" in depth_snapshot:
            top_bid_vol = sum(float(l[1]) for l in depth_snapshot["bids"][:3])
            top_ask_vol = sum(float(l[1]) for l in depth_snapshot["asks"][:3])
            avg_tob_vol = (top_bid_vol + top_ask_vol) / 2.0
            if avg_tob_vol > 0 and total_qty > (avg_tob_vol * 0.05):
                is_large_order = True
                
        if is_large_order:
            logger.info(f"[X-RAY] 🐋 WHALE ROUTING // {symbol} size > 5% of Top-of-Book depth. Triggering Iceberg Protocol.")
            success, f_price, f_qty = await self._execute_twap_iceberg(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit)
            return success, arrival_price, f_price, f_qty

        # 🚀 ATOMIC LATENCY-AWARE ROUTING
        # Bypass Maker Peg entirely if orderbook metrics indicate adverse selection probability
        bid_liquidity = sum(float(b[1]) for b in ob.get("bids", [])[:3]) if ob.get("bids") else 1.0
        ask_liquidity = sum(float(a[1]) for a in ob.get("asks", [])[:3]) if ob.get("asks") else 1.0
        book_skew = bid_liquidity / (ask_liquidity + 1e-9)

        urgent_taker = False
        if direction.upper() == "BUY" and (book_skew < 0.35 or vol_z > 2.2):
            urgent_taker = True
        elif direction.upper() == "SELL" and (book_skew > 2.8 or vol_z < -2.2):
            urgent_taker = True

        if urgent_taker or abs(vol_z) >= 1.8 or vol_mult >= 2.0 or regime == "TRENDING":
            logger.info(f"[X-RAY] ⚡ ATOMIC TAKER IOC // {symbol} Micro-book collapsing or momentum breaking. Routing immediate IOC to prevent queue delay.")
            success, f_price, f_qty = await self._execute_flash_strike(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit)
            return success, arrival_price, f_price, f_qty

        is_major_asset = symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        dynamic_timeout = 8.0 if is_major_asset else 15.0  
        
        logger.info(f"[X-RAY] 🕸️ RANGING REGIME ROUTING // Attempting Maker-Grid Peg on {symbol} ({dynamic_timeout}s timeout).")
        success, f_price, f_qty = await self._execute_dynamic_maker_peg(
            symbol, direction, total_qty, stop_loss, take_profit, 
            feature_engine=feature_engine, depth_snapshot=depth_snapshot, timeout=dynamic_timeout
        )

        # CRITICAL FIX: DO NOT chase with a market order if maker peg fails.
        if not success or f_qty == 0:
            logger.info(f"[X-RAY] 🛑 MAKER PEG UNFILLED // Safely canceling order for {symbol}. Trade aborted.")
            return False, arrival_price, 0.0, 0.0

        return True, arrival_price, f_price, f_qty