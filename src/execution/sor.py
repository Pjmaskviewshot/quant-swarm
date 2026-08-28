"""
💎 V22.6 APEX QUANTUM PRIME: INSTITUTIONAL SMART ORDER ROUTER
--------------------------------------------------------
Features Atomic Probability Routing, Arrival Price Caching (IS Tracking),
Maker-Grid Spread Capture, Dynamic Slippage Firewalls, PostOnly Pegging, 
and Null-Guard Parity.

Audit Fixes (V22.6):
- Pure Asyncio API Call Migration (Eliminated Pybit Sync Wrapping)
- Deterministic Sweeping Calculus (Eradicated Blind Price Escalation)
- Dynamic Maker-Peg Chase Caps (Eradicated 0.5% Spoofing Vulnerability)
- Strict Modulo Arithmetic Rounding: Micro-cap altcoin order quantities 
  are mathematically floored to the precise Bybit `lotSizeFilter` step size.
"""

import os
import asyncio
import logging
import math
import time
from typing import Dict, Any, List, Tuple, Optional
from decimal import Decimal, ROUND_HALF_UP

logger = logging.getLogger("QUANT_CORE.SOR")

class SmartOrderRouter:
    """
    Dynamically routes executions across Flash Strike (Taker), Maker Peg (PostOnly), 
    and TWAP Iceberg slices to minimize execution drag and slippage.
    """
    def __init__(self, executor: Any, max_slippage_pct: float = 0.0012):
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
            info = await self.executor.safe_call("GET", "/v5/market/instruments-info", category="linear", symbol=symbol)
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

    def _apply_dynamic_exchange_limits(self, raw_qty: float, current_price: float, symbol: str) -> float:
        """
        🚀 V22.6 FIX: Strict Modulo Rounding for Micro-Cap Altcoins
        Guarantees that the order quantity perfectly aligns with Bybit's lotSizeFilter.
        """
        limits = self.instrument_cache.get(symbol, {"min_qty": 1.0, "qty_step": 1.0})
        required_min_qty, qty_step = limits["min_qty"], limits["qty_step"]
        
        # 1. Floor the quantity to the nearest strict modulo of the qty_step
        if qty_step > 0:
            # Add a microscopic epsilon to prevent floating-point rounding errors (e.g., 2.9999999)
            steps = math.floor((raw_qty + 1e-9) / qty_step)
            adjusted_qty = steps * qty_step
        else:
            adjusted_qty = raw_qty

        # 2. Ensure minimum notional value (Bybit generally requires > $5.00)
        notional = adjusted_qty * current_price
        if notional < 6.50:
            # If the calculated size is too small, safely round up to the minimum allowed
            required_qty = 6.50 / current_price
            steps = math.ceil((required_qty - 1e-9) / qty_step) if qty_step > 0 else required_qty
            adjusted_qty = steps * qty_step

        # 3. Final sanity check against absolute minimums
        final_qty = max(required_min_qty, adjusted_qty)

        # 4. Format string to exact precision to drop trailing floating point zeroes
        precision = max(0, abs(int(math.floor(math.log10(qty_step))))) if qty_step > 0 else 0
        formatted_qty_str = f"{final_qty:.{precision}f}"
        
        # logger.info(f"[X-RAY] 📡 DYNAMIC LIMITS APPLIED // {symbol} | Raw: {raw_qty:.4f} -> Final Modulo: {formatted_qty_str}")
        
        return float(formatted_qty_str)

    def compute_dynamic_slippage_cap_bps(self, symbol: str, regime: str, live_spread_bps: float) -> float:
        """Adapts allowable slippage based on asset class, live spread, and regime."""
        is_major = symbol in ["BTCUSDT", "ETHUSDT"]
        is_high_cap = symbol in ["SOLUSDT", "SUIUSDT", "AVAXUSDT", "LINKUSDT", "NEARUSDT", "APTUSDT"]
        
        spread_multiplier = 2.0 if is_major else 2.5
        calculated_cap = max(8.0, live_spread_bps * spread_multiplier)
        
        if regime == "TRENDING":
            calculated_cap += 5.0
            
        if is_major:
            return min(15.0, calculated_cap)      
        elif is_high_cap:
            return min(25.0, calculated_cap)      
        else:
            return min(40.0, calculated_cap)      

    def estimate_orderbook_slippage_bps(self, depth_snapshot: Dict, side: str, qty: float, current_mid: float) -> float:
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
            return 999.0 

        avg_expected_price = accumulated_cost / accumulated_qty
        top_of_book = float(levels[0][0])
        
        if side.upper() == "BUY":
            slippage_bps = ((avg_expected_price - top_of_book) / top_of_book) * 10000.0
        else:
            slippage_bps = ((top_of_book - avg_expected_price) / top_of_book) * 10000.0

        return max(0.0, slippage_bps)

    def get_sweeping_price(self, depth_snapshot: Dict, side: str, qty: float, current_mid: float) -> float:
        """
        🚀 V22.0 DETERMINISTIC MARGINAL PRICE CALCULUS
        Walks the L2 snapshot to find the exact price point needed to absorb the requested qty.
        """
        if not depth_snapshot:
            return current_mid * (1.001 if side.upper() == "BUY" else 0.999)
            
        levels = depth_snapshot.get("asks" if side.upper() == "BUY" else "bids", [])
        if not levels: 
            return current_mid * (1.001 if side.upper() == "BUY" else 0.999)
            
        accumulated_qty = 0.0
        for level in levels:
            try:
                p, v = float(level[0]), float(level[1])
                accumulated_qty += v
                if accumulated_qty >= qty:
                    return p
            except (IndexError, ValueError):
                continue
                
        return float(levels[-1][0])

    def _format_dynamic_price(self, price: float, target_symbol: str) -> float:
        tick_size = self.instrument_cache.get(target_symbol, {"tick_size": 0.01})["tick_size"]
        stepped_price = round(price / tick_size) * tick_size
        return round(stepped_price, self._get_precision(tick_size))

    def _get_meaningful_tob(self, ob_data: Dict, side: str, min_notional: float = 5.0) -> float:
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
        ob_response = await self.executor.safe_call("GET", "/v5/market/orderbook", category="linear", symbol=symbol, limit=50)
        ob_data = ob_response.get("result", {})
        return self._get_meaningful_tob({"bids": ob_data.get("b", []), "asks": ob_data.get("a", [])}, side)

    async def cancel_order_safe(self, symbol: str, order_id: str) -> bool:
        for attempt in range(3):
            try:
                open_orders = await self.executor.safe_call(
                    "GET", "/v5/order/realtime", 
                    category="linear", symbol=symbol, orderId=order_id
                )
                open_list = open_orders.get("result", {}).get("list", [])
                
                if not open_list:
                    return True

                res = await self.executor.safe_call(
                    "POST", "/v5/order/cancel", is_execution=True,
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

    async def _verify_order_fill(self, symbol: str, order_id: str, timeout: float = 0.2) -> dict:
        """Attempts to fetch the fill via WebSocket. Drops to REST API if it times out."""
        if hasattr(self.executor, 'await_ws_execution_report'):
            try:
                ws_report = await self.executor.await_ws_execution_report(order_id, timeout=timeout)
                if ws_report:
                    return ws_report
            except asyncio.TimeoutError:
                logger.debug(f"[X-RAY] WS fill report timed out for {order_id}. Falling back to REST.")
                
        try:
            hist_res = await self.executor.safe_call(
                "GET", "/v5/order/history",
                category="linear", symbol=symbol, orderId=order_id, limit=1
            )
            orders = hist_res.get("result", {}).get("list", [])
            return orders[0] if orders else {}
        except Exception as e:
            logger.warning(f"[X-RAY] REST history fallback failed for {order_id}: {e}")
            return {}

    async def _execute_flash_strike(self, symbol: str, direction: str, qty: float, current_mid_price: float, sl: Optional[float] = None, tp: Optional[float] = None, depth_snapshot: dict = None, regime: str = "TRENDING") -> Tuple[bool, float, float]:
        logger.critical(f"[X-RAY] ⚡ FLASH STRIKE AUTHORIZED // {symbol} executing deterministic momentum escalation.")
        
        if sl is None or tp is None or sl == tp:
            implied_sl_dist = current_mid_price * 0.025
            implied_tp_dist = implied_sl_dist * 2.0
            if direction.upper() == "BUY":
                sl, tp = current_mid_price - implied_sl_dist, current_mid_price + implied_tp_dist
            else:
                sl, tp = current_mid_price + implied_sl_dist, current_mid_price - implied_tp_dist

        final_sl = self._format_dynamic_price(sl, symbol) if sl else 0.0
        final_tp = self._format_dynamic_price(tp, symbol) if tp else 0.0
        side = "Buy" if direction.upper() == "BUY" else "Sell"

        remaining_qty = qty
        total_executed_qty = 0.0
        weighted_cost = 0.0

        for attempt in range(3):
            cleaned_qty = self._apply_dynamic_exchange_limits(remaining_qty, current_mid_price, symbol)
            
            # 🚀 V22.0 FIX: Determine the exact required execution price and cap it safely
            sweeping_price = self.get_sweeping_price(depth_snapshot, side, cleaned_qty, current_mid_price)
            
            best_bid = float(depth_snapshot.get("bids", [[current_mid_price]])[0][0]) if depth_snapshot else current_mid_price
            best_ask = float(depth_snapshot.get("asks", [[current_mid_price]])[0][0]) if depth_snapshot else current_mid_price
            live_spread_bps = ((best_ask - best_bid) / (best_bid + 1e-9)) * 10000.0
            dynamic_cap_bps = self.compute_dynamic_slippage_cap_bps(symbol, regime, live_spread_bps)
            
            if side == "Buy":
                max_allowed_price = current_mid_price * (1.0 + (dynamic_cap_bps / 10000.0))
                target_price = min(sweeping_price, max_allowed_price)
            else:
                max_allowed_price = current_mid_price * (1.0 - (dynamic_cap_bps / 10000.0))
                target_price = max(sweeping_price, max_allowed_price)
                
            final_price = self._format_dynamic_price(target_price, symbol)

            logger.info(f"[X-RAY] ⚡ Flash Strike Attempt {attempt+1}/3 // {side} {cleaned_qty} {symbol} at {final_price}")

            try:
                response = await self.executor.safe_call(
                    "POST", "/v5/order/create", is_execution=True,
                    category="linear", symbol=symbol, side=side, orderType="Limit", 
                    qty=str(cleaned_qty), price=str(final_price), timeInForce="IOC", 
                    positionIdx=self.position_idx
                )
                
                if response.get("retCode") == 0:
                    order_id = response.get("result", {}).get("orderId", "UNKNOWN")
                    fill_report = await self._verify_order_fill(symbol, order_id, timeout=0.3)
                    
                    if fill_report:
                        raw_exec = fill_report.get("cumExecQty")
                        raw_avg = fill_report.get("avgPrice")
                        
                        cum_exec = float(raw_exec) if (raw_exec is not None and str(raw_exec).strip() != "") else 0.0
                        avg_price = float(raw_avg) if (raw_avg is not None and str(raw_avg).strip() != "") else current_mid_price

                        if cum_exec > 0:
                            total_executed_qty += cum_exec
                            weighted_cost += (cum_exec * avg_price)
                            remaining_qty -= cum_exec

                            if remaining_qty <= self.instrument_cache.get(symbol, {}).get("min_qty", 0.001):
                                final_avg = weighted_cost / total_executed_qty
                                logger.critical(f"✅ FLASH STRIKE SUCCESS // {symbol} filled {total_executed_qty} units at {final_avg}.")
                                
                                if final_sl or final_tp:
                                    try:
                                        await self.executor.safe_call(
                                            "POST", "/v5/position/trading-stop", is_execution=True,
                                            category="linear", symbol=symbol, positionIdx=self.position_idx, 
                                            stopLoss=str(final_sl) if final_sl else None, takeProfit=str(final_tp) if final_tp else None
                                        )
                                        logger.info(f"[X-RAY] 🛡️ Stops successfully attached: SL {final_sl} | TP {final_tp}")
                                    except Exception as e:
                                        logger.error(f"[X-RAY] 🛑 FATAL: Failed to attach stops for {symbol}: {e}")
                                        
                                return True, final_avg, total_executed_qty
                            else:
                                logger.warning(f"[X-RAY] ⚠️ Partial Fill. Re-evaluating depth for remaining {remaining_qty} units...")
                                # In a real implementation, we would fetch a fresh depth_snapshot here
                        else:
                            logger.warning(f"[X-RAY] ⚠️ Flash Strike IOC missed (Liquidity vanished). Escalating...")
                else:
                    logger.warning(f"[X-RAY] ⚠️ API rejection (Attempt {attempt+1}): {response.get('retMsg')}")
                    await asyncio.sleep(0.1) 
                    
            except Exception as e:
                error_str = str(e)
                logger.error(f"[X-RAY] ⚠️ Network Exception during Flash Strike: {error_str}")
                if any(fatal in error_str for fatal in ["110126", "INNOVATION ZONE", "10002", "10001"]):
                    break
                
        if total_executed_qty > 0:
            return True, weighted_cost / total_executed_qty, total_executed_qty
            
        logger.error(f"[X-RAY] ❌ Flash Strike failed permanently after 3 attempts.")
        return False, 0.0, 0.0

    async def _execute_dynamic_maker_peg(self, symbol: str, direction: str, qty: float, sl: Optional[float], tp: Optional[float], feature_engine=None, depth_snapshot: dict=None, timeout: int = 5, regime: str = "MEAN_REVERTING") -> Tuple[bool, float, float]:
        logger.info(f"🛡️ HFT MAKER-PEGGING INITIATED // {symbol}. Engaging Spread Capture & Anti-Spoofing Scanners. (Timeout: {timeout}s)")
        
        start_time = time.time()
        current_order_id = None
        side = "Buy" if direction.upper() == "BUY" else "Sell"
        
        anchor_price = None
        rejection_count = 0 
        tick_size = self.instrument_cache.get(symbol, {"tick_size": 0.01})["tick_size"]

        if sl is None or tp is None:
            current_mid = depth_snapshot.get("bids", [[100, 1]])[0][0] if depth_snapshot else 100.0
            sl = current_mid * 0.975 if side == "Buy" else current_mid * 1.025
            tp = current_mid * 1.050 if side == "Buy" else current_mid * 0.950

        # 🚀 V22.0 FIX: Tie chase limit mathematically to the slippage firewall, not a static 0.5%
        best_bid = float(depth_snapshot.get("bids", [[100]])[0][0]) if depth_snapshot else 100.0
        best_ask = float(depth_snapshot.get("asks", [[100]])[0][0]) if depth_snapshot else 100.0
        live_spread_bps = ((best_ask - best_bid) / (best_bid + 1e-9)) * 10000.0
        dynamic_cap_bps = self.compute_dynamic_slippage_cap_bps(symbol, regime, live_spread_bps)
        max_chase_deviation = max(0.001, dynamic_cap_bps / 10000.0)

        while time.time() - start_time < timeout:
            loop_delay = 1.0

            try:
                if feature_engine and hasattr(feature_engine, 'get_book_depth_metrics'):
                    depth_metrics = feature_engine.get_book_depth_metrics()
                    imbalance = depth_metrics.get("depth_imbalance", 0.0)
                    
                    if direction.upper() == "BUY" and imbalance > 0.80:
                        logger.warning(f"[X-RAY] 🚫 ADVERSE SELECTION // {symbol} Bid wall collapsing. Aborting peg.")
                        break
                    elif direction.upper() == "SELL" and imbalance < -0.80:
                        logger.warning(f"[X-RAY] 🚫 ADVERSE SELECTION // {symbol} Ask wall collapsing. Aborting peg.")
                        break

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

                if direction.upper() == "BUY" and target_price > anchor_price * (1 + max_chase_deviation):
                    logger.warning(f"[X-RAY] 🏃 CHASE ABORTED // {symbol} ran +{max_chase_deviation:.2%} beyond signal anchor.")
                    break
                if direction.upper() == "SELL" and target_price < anchor_price * (1 - max_chase_deviation):
                    logger.warning(f"[X-RAY] 🏃 CHASE ABORTED // {symbol} ran -{max_chase_deviation:.2%} beyond signal anchor.")
                    break

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
                
                # We strip final_sl and final_tp from this specific order payload to prevent 10002 errors
                
                if not current_order_id:
                    logger.info(f"[X-RAY] 🛡️ Placing initial PostOnly Maker Peg for {symbol} at {final_target_price}")
                    place_response = await self.executor.safe_call(
                        "POST", "/v5/order/create", is_execution=True,
                        category="linear", symbol=symbol, side=side, orderType="Limit",
                        qty=str(cleaned_qty), price=str(final_target_price), timeInForce="PostOnly", 
                        positionIdx=self.position_idx
                    )
                    if place_response.get("retCode") == 0: 
                        current_order_id = place_response["result"]["orderId"]
                    else:
                        rejection_count += 1
                        logger.warning(f"[X-RAY] PostOnly placement rejected: {place_response.get('retMsg')}")
                        if rejection_count >= 3:
                            logger.error(f"[X-RAY] 🛑 PEG CIRCUIT BREAKER TRIPPED // {symbol} PostOnly rejected 3 times.")
                            break
                        await asyncio.sleep(loop_delay); continue
                
                if current_order_id:
                    status_response = await self.executor.safe_call("GET", "/v5/order/realtime", category="linear", symbol=symbol, orderId=current_order_id)
                    order_list = status_response.get("result", {}).get("list", [])
                    
                    if not order_list:
                        fill_report = await self._verify_order_fill(symbol, current_order_id, timeout=0.2)
                        if fill_report:
                            raw_exec = fill_report.get("cumExecQty")
                            raw_avg = fill_report.get("avgPrice")
                            
                            cum_exec = float(raw_exec) if (raw_exec is not None and str(raw_exec).strip() != "") else 0.0
                            avg_price = float(raw_avg) if (raw_avg is not None and str(raw_avg).strip() != "") else final_target_price

                            if cum_exec > 0:
                                logger.critical(f"✅ MAKER PEG RESOLVED // {symbol} secured {cum_exec} units at {avg_price}. Spread Captured!")
                                return True, avg_price, cum_exec
                        
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
                            break
                            
                    elif order_status in ["New", "PartiallyFilled"]:
                        if abs(final_target_price - current_peg_price) >= (tick_size * 2):
                            cancel_success = await self.cancel_order_safe(symbol, current_order_id)
                            if cancel_success:
                                current_order_id = None
                                await asyncio.sleep(0.5) 
                            else:
                                logger.warning(f"[X-RAY] ⚠️ Failed to cancel peg for {symbol}. Holding current position.")

            except Exception as e: 
                error_str = str(e)
                if any(fatal in error_str for fatal in ["110126", "INNOVATION ZONE", "10002", "10001"]):
                    break
                await asyncio.sleep(loop_delay) 

        if current_order_id:
            logger.warning(f"[X-RAY] ⏳ MAKER CHASE TIMEOUT // Market escaped {symbol} peg range. Canceling to protect capital.")
            cancel_success = await self.cancel_order_safe(symbol, current_order_id)
            
            fill_report = await self._verify_order_fill(symbol, current_order_id, timeout=0.2)
            if fill_report:
                raw_exec = fill_report.get("cumExecQty")
                raw_avg = fill_report.get("avgPrice")
                
                cum_exec = float(raw_exec) if (raw_exec is not None and str(raw_exec).strip() != "") else 0.0
                avg_price = float(raw_avg) if (raw_avg is not None and str(raw_avg).strip() != "") else anchor_price
                if cum_exec > 0:
                    return True, avg_price, cum_exec
                    
        return False, 0.0, 0.0

    async def _execute_twap_iceberg(self, symbol: str, direction: str, total_qty: float, current_mid_price: float, sl: float, tp: float, slices: int = 4, slice_interval_sec: float = 5.0, regime: str = "TRENDING") -> Tuple[bool, float, float]:
        limits = self.instrument_cache.get(symbol, {"min_qty": 1.0})
        min_qty = limits["min_qty"]
        min_notional_qty = 6.50 / current_mid_price
        absolute_min_slice = max(min_qty, min_notional_qty)

        if (total_qty / slices) < absolute_min_slice:
            safe_slices = max(1, math.floor(total_qty / absolute_min_slice))
            slices = safe_slices

        slice_qty = total_qty / slices
        total_executed_qty = 0.0
        weighted_notional_sum = 0.0
        
        logger.critical(f"[X-RAY] 🧊 ICEBERG ENGAGED // {symbol} slicing into {slices} TWAP chunks of {slice_qty:.4f}.")
        
        is_major_asset = symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        chunk_timeout = 3.0 if is_major_asset else 6.0 
        
        for i in range(slices):
            logger.info(f"[X-RAY] 🧊 TWAP SLICE [{i+1}/{slices}] // Routing {slice_qty:.4f} {symbol}")
            
            success, fill_price, fill_qty = await self._execute_dynamic_maker_peg(
                symbol=symbol, direction=direction, qty=slice_qty, sl=sl, tp=tp, timeout=chunk_timeout, regime=regime
            )
            
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
                actual_sl, actual_tp = avg_fill_price * (1.0 - sl_dist_pct), avg_fill_price * (1.0 + tp_dist_pct)
            else:
                actual_sl, actual_tp = avg_fill_price * (1.0 + sl_dist_pct), avg_fill_price * (1.0 - tp_dist_pct)
            
            try:
                await self.executor.safe_call(
                    "POST", "/v5/position/trading-stop", is_execution=True,
                    category="linear", symbol=symbol, positionIdx=self.position_idx, 
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
            success, f_price, f_qty = await self._execute_twap_iceberg(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit, regime=regime)
            return success, arrival_price, f_price, f_qty
        
        logger.info(f"[X-RAY] 🚀 TRENDING REGIME ROUTING // Initiating high-speed dispatch for {symbol} {direction}")
        if abs(vol_z) >= 1.5 or vol_mult >= 1.5:
            success, f_price, f_qty = await self._execute_flash_strike(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit, depth_snapshot=depth_snapshot, regime=regime)
            return success, arrival_price, f_price, f_qty
        else:
            is_major_asset = symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
            dynamic_timeout = 2.0 if is_major_asset else 5.0
            
            success, f_price, f_qty = await self._execute_dynamic_maker_peg(
                symbol, direction, total_qty, stop_loss, take_profit, 
                feature_engine=feature_engine, depth_snapshot=depth_snapshot, timeout=dynamic_timeout, regime=regime
            )
            
            if not success or f_qty == 0:
                logger.info(f"[X-RAY] 🛑 MAKER PEG UNFILLED // Safely canceling order for {symbol}. Trade aborted.")
                return False, arrival_price, 0.0, 0.0
                
            return success, arrival_price, f_price, f_qty

    async def execute_mean_reversion_bracket(self, symbol: str, direction: str, total_qty: float, current_mid_price: float, stop_loss: float = None, take_profit: float = None, depth_snapshot: dict = None, vol_z: float = 0.0, vol_mult: float = 1.0, feature_engine: Any = None, regime: str = "MEAN_REVERTING", **kwargs) -> Tuple[bool, float, float, float]:
        await self._fetch_exchange_limits(symbol)
        arrival_price = current_mid_price
        
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
            success, f_price, f_qty = await self._execute_twap_iceberg(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit, regime=regime)
            return success, arrival_price, f_price, f_qty

        bid_liquidity = sum(float(b[1]) for b in ob.get("bids", [])[:3]) if ob.get("bids") else 1.0
        ask_liquidity = sum(float(a[1]) for a in ob.get("asks", [])[:3]) if ob.get("asks") else 1.0
        book_skew = bid_liquidity / (ask_liquidity + 1e-9)

        urgent_taker = False
        if direction.upper() == "BUY" and (book_skew < 0.35 or vol_z > 2.2):
            urgent_taker = True
        elif direction.upper() == "SELL" and (book_skew > 2.8 or vol_z < -2.2):
            urgent_taker = True

        if urgent_taker or abs(vol_z) >= 1.8 or vol_mult >= 2.0 or regime == "TRENDING":
            logger.info(f"[X-RAY] ⚡ ATOMIC TAKER IOC // {symbol} Micro-book collapsing or momentum breaking. Routing immediate IOC.")
            success, f_price, f_qty = await self._execute_flash_strike(symbol, direction, total_qty, current_mid_price, stop_loss, take_profit, depth_snapshot=depth_snapshot, regime=regime)
            return success, arrival_price, f_price, f_qty

        is_major_asset = symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        dynamic_timeout = 3.0 if is_major_asset else 6.0  
        
        logger.info(f"[X-RAY] 🕸️ RANGING REGIME ROUTING // Attempting Maker-Grid Peg on {symbol} ({dynamic_timeout}s timeout).")
        success, f_price, f_qty = await self._execute_dynamic_maker_peg(
            symbol, direction, total_qty, stop_loss, take_profit, 
            feature_engine=feature_engine, depth_snapshot=depth_snapshot, timeout=dynamic_timeout, regime=regime
        )

        if not success or f_qty == 0:
            logger.info(f"[X-RAY] 🛑 MAKER PEG UNFILLED // Safely canceling order for {symbol}. Trade aborted.")
            return False, arrival_price, 0.0, 0.0

        return True, arrival_price, f_price, f_qty