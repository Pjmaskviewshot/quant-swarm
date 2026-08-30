"""
💎 V36.0 APEX TITAN: DIRECT-DRIVE SMART ORDER ROUTER
--------------------------------------------------------
Features Atomic Probability Routing, Arrival Price Caching (IS Tracking),
Maker-Grid Spread Capture, Dynamic Slippage Firewalls, PostOnly Pegging, 
and Null-Guard Parity.

Architectural Supremacy (V36.0 Integration):
- True Precision Routing: Uses native Decimal string quantization at the API 
  boundary to eradicate Bybit 10001 (Qty) and 10002 (Price) precision rejections.
- Advanced Trailing Synchronization: Handles exchange-side "Not Modified" 
  rejections gracefully, allowing the Kinetic Take-Profit Compressor to track 
  prices perfectly without loop termination.
- Maker Queue Preservation: Amends PostOnly bounds exclusively at 1.5x tick deviation 
  to prevent queue-priority reset spam during microscopic spread adjustments.
- Adaptive Sweep-to-Peg: Eradicates the dangerous 3-attempt IOC loop. Fires a single 
  deep IOC up to the slippage cap, and seamlessly hands off any unfilled remainder 
  to the Maker Peg engine to capture the reversion.
- HFT Throttle Fix & Jitter: Reduced trailing stop amend latency floor to 0.08s 
  and applied micro-jitter to trace flash crashes without deterministic API bans.
"""

import os
import asyncio
import logging
import math
import time
import random  
import numpy as np
from typing import Dict, Any, List, Tuple, Optional
from decimal import Decimal, ROUND_HALF_UP

logger = logging.getLogger("QUANT_CORE.SOR")

class SmartOrderRouter:
    """
    🚀 V36.0 DIRECT-DRIVE EXECUTION NEXUS
    Dynamically sizes (Fixed Fractional) and routes executions across Sweep-to-Peg (Taker/Maker Hybrid), 
    Maker Peg (PostOnly), and TWAP Iceberg slices to minimize execution drag and slippage.
    """
    def __init__(self, executor: Any, max_slippage_pct: float = 0.0012, core_engine: Any = None):
        self.executor = executor
        self.core_engine = core_engine  
        self.base_max_slippage_pct = max_slippage_pct
        self.instrument_cache: Dict[str, Dict[str, float]] = {}
        self.position_idx = int(os.getenv("BYBIT_POSITION_IDX", 0))
        self._last_amend_time: Dict[str, float] = {}

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
        except Exception as e:
            logger.error(f"[X-RAY] Failed to fetch strict limits for {symbol}, using safe defaults: {e}")
            self.instrument_cache[symbol] = {"min_qty": 1.0, "qty_step": 1.0, "tick_size": 0.01}

    def _apply_dynamic_exchange_limits(self, raw_qty: float, current_price: float, symbol: str) -> float:
        limits = self.instrument_cache.get(symbol, {"min_qty": 1.0, "qty_step": 1.0})
        required_min_qty, qty_step = limits["min_qty"], limits["qty_step"]
        
        if qty_step > 0:
            steps = math.floor((raw_qty + 1e-9) / qty_step)
            adjusted_qty = steps * qty_step
        else:
            adjusted_qty = raw_qty

        notional = adjusted_qty * current_price
        # Absolute Bybit linear minimum notional is $5.00. We pad to $6.50 to avoid slippage rejections.
        if notional < 6.50:
            required_qty = 6.50 / current_price
            steps = math.ceil((required_qty - 1e-9) / qty_step) if qty_step > 0 else required_qty
            adjusted_qty = steps * qty_step

        return max(required_min_qty, adjusted_qty)

    def _format_qty_str(self, raw_qty: float, symbol: str) -> str:
        qty_step = self.instrument_cache.get(symbol, {"qty_step": 1.0})["qty_step"]
        precision = max(0, abs(int(math.floor(math.log10(qty_step))))) if qty_step > 0 else 0
        return f"{raw_qty:.{precision}f}"

    def _format_price_str(self, price: float, target_symbol: str) -> str:
        tick_size = self.instrument_cache.get(target_symbol, {"tick_size": 0.01})["tick_size"]
        if tick_size <= 0: return str(price)
        precision = self._get_precision(tick_size)
        stepped = Decimal(str(price)).quantize(Decimal(str(tick_size)), rounding=ROUND_HALF_UP)
        return f"{stepped:.{precision}f}"

    def calculate_risk_adjusted_notional(self, prob_success: float, exec_weight: float, sl_pct: float, tp_pct: float, current_balance: float, inst_var: float) -> float:
        base_risk_pct = 0.01 
        vol_scalar = 1.0 / (1.0 + (inst_var * 1000.0))
        confidence_scalar = float(np.clip((prob_success - 0.5) * 2.0, 0.5, 1.0))
        
        final_risk_pct = base_risk_pct * vol_scalar * confidence_scalar * exec_weight
        final_risk_pct = min(0.015, final_risk_pct) 
        
        trade_risk_dollars = current_balance * final_risk_pct
        target_notional = trade_risk_dollars / (sl_pct + 1e-9)
        
        max_leverage_cap = 2.0 
        max_permitted_notional = current_balance * max_leverage_cap
        
        return float(np.clip(target_notional, 6.50, max_permitted_notional))

    def compute_dynamic_slippage_cap_bps(self, symbol: str, regime: str, live_spread_bps: float) -> float:
        is_major = symbol in ["BTCUSDT", "ETHUSDT"]
        is_high_cap = symbol in ["SOLUSDT", "SUIUSDT", "AVAXUSDT", "LINKUSDT", "NEARUSDT", "APTUSDT"]
        
        spread_multiplier = 2.0 if is_major else 2.5
        calculated_cap = max(8.0, live_spread_bps * spread_multiplier)
        if regime == "TRENDING": calculated_cap += 5.0
            
        if is_major: return min(15.0, calculated_cap)       
        elif is_high_cap: return min(25.0, calculated_cap)      
        else: return min(40.0, calculated_cap)      

    def estimate_orderbook_slippage_bps(self, depth_snapshot: Dict, side: str, qty: float, current_mid: float) -> float:
        if not depth_snapshot or "bids" not in depth_snapshot or "asks" not in depth_snapshot: return 0.0
        levels = depth_snapshot.get("asks" if side.upper() == "BUY" else "bids", [])
        if not levels: return 0.0

        accumulated_qty, accumulated_cost = 0.0, 0.0
        for level in levels:
            try:
                p, v = float(level[0]), float(level[1])
                needed = qty - accumulated_qty
                if v >= needed:
                    accumulated_cost += (needed * p)
                    accumulated_qty += needed
                    break
                else:
                    accumulated_cost += (v * p)
                    accumulated_qty += v
            except (IndexError, ValueError): continue

        if accumulated_qty < qty or accumulated_qty == 0: return 999.0 
        avg_expected_price = accumulated_cost / accumulated_qty
        top_of_book = float(levels[0][0])
        
        if side.upper() == "BUY": slippage_bps = ((avg_expected_price - top_of_book) / top_of_book) * 10000.0
        else: slippage_bps = ((top_of_book - avg_expected_price) / top_of_book) * 10000.0
        return max(0.0, slippage_bps)

    def get_sweeping_price(self, depth_snapshot: Dict, side: str, qty: float, current_mid: float) -> float:
        if not depth_snapshot: return current_mid * (1.001 if side.upper() == "BUY" else 0.999)
        levels = depth_snapshot.get("asks" if side.upper() == "BUY" else "bids", [])
        if not levels: return current_mid * (1.001 if side.upper() == "BUY" else 0.999)
            
        accumulated_qty = 0.0
        for level in levels:
            try:
                p, v = float(level[0]), float(level[1])
                accumulated_qty += v
                if accumulated_qty >= qty: return p
            except (IndexError, ValueError): continue
        return float(levels[-1][0])

    def _get_meaningful_tob(self, ob_data: Dict, side: str, min_notional: float = 5.0) -> float:
        levels = ob_data.get("bids" if side == "BUY" else "asks", [[0, 0]])
        for level in levels:
            try:
                price, qty = float(level[0]), float(level[1])
                if price * qty >= min_notional: return price
            except (IndexError, ValueError): continue
        return float(levels[0][0]) if levels else 0.0

    async def cancel_order_safe(self, symbol: str, order_id: str) -> bool:
        for attempt in range(3):
            try:
                open_orders = await self.executor.safe_call("GET", "/v5/order/realtime", category="linear", symbol=symbol, orderId=order_id)
                if not open_orders.get("result", {}).get("list", []): return True
                res = await self.executor.safe_call("POST", "/v5/order/cancel", is_execution=True, category="linear", symbol=symbol, orderId=order_id)
                if res.get("retCode") == 0: return True
            except Exception as e:
                err_str = str(e)
                if "110001" in err_str or "not exists" in err_str or "too late" in err_str: return True
                await asyncio.sleep(0.5)
        return False

    async def _verify_order_fill(self, symbol: str, order_id: str, timeout: float = 0.2) -> dict:
        if hasattr(self.executor, 'await_ws_execution_report'):
            try:
                ws_report = await self.executor.await_ws_execution_report(order_id, timeout=timeout)
                if ws_report: return ws_report
            except asyncio.TimeoutError: pass
        try:
            hist_res = await self.executor.safe_call("GET", "/v5/order/history", category="linear", symbol=symbol, orderId=order_id, limit=1)
            orders = hist_res.get("result", {}).get("list", [])
            return orders[0] if orders else {}
        except Exception: return {}

    async def _amend_trailing_stop(self, symbol: str, new_sl: float, new_tp: float) -> bool:
        """🚀 V36.0 FIX: Reduced throttle to 0.08s for flash-crash reactivity with jitter."""
        now = time.time()
        
        throttle_window = 0.08 + random.uniform(0.0, 0.05)
        if now - self._last_amend_time.get(symbol, 0.0) < throttle_window:
            return False

        sl_str = self._format_price_str(new_sl, symbol)
        tp_str = self._format_price_str(new_tp, symbol)

        try:
            res = await self.executor.safe_call(
                "POST", "/v5/position/trading-stop", is_execution=True,
                category="linear", symbol=symbol, positionIdx=self.position_idx,
                takeProfit=tp_str, stopLoss=sl_str,
                tpTriggerBy="LastPrice", slTriggerBy="LastPrice"
            )
            
            ret_code = res.get("retCode")
            ret_msg = res.get("retMsg", "").lower()

            if ret_code == 0 or "not modified" in ret_msg or "same" in ret_msg:
                self._last_amend_time[symbol] = now
                return True
            return False
            
        except Exception as e:
            err_str = str(e).lower()
            if "not modified" in err_str or "same" in err_str:
                self._last_amend_time[symbol] = now
                return True
                
            logger.debug(f"[X-RAY] Trailing stop amend failed for {symbol}: {e}")
            return False

    async def _execute_flash_strike(self, symbol: str, direction: str, qty: float, current_mid_price: float, sl: Optional[float] = None, tp: Optional[float] = None, depth_snapshot: dict = None, regime: str = "TRENDING") -> Tuple[bool, float, float]:
        """
        🚀 V36.0 EMPIRICAL HARDENING: Adaptive Sweep-to-Peg.
        Fires a single deep IOC. Any unfilled remainder instantly converts to a Maker Peg.
        """
        logger.critical(f"[X-RAY] ⚡ SWEEP-TO-PEG AUTHORIZED // {symbol} executing deterministic momentum escalation.")
        
        side = "Buy" if direction.upper() == "BUY" else "Sell"
        cleaned_qty = self._apply_dynamic_exchange_limits(qty, current_mid_price, symbol)
        qty_str = self._format_qty_str(cleaned_qty, symbol)
        
        # 1. Calculate Maximum Permissible Deep Sweep Price
        best_bid = float(depth_snapshot.get("bids", [[current_mid_price]])[0][0]) if depth_snapshot else current_mid_price
        best_ask = float(depth_snapshot.get("asks", [[current_mid_price]])[0][0]) if depth_snapshot else current_mid_price
        live_spread_bps = ((best_ask - best_bid) / (best_bid + 1e-9)) * 10000.0
        
        dynamic_cap_bps = self.compute_dynamic_slippage_cap_bps(symbol, regime, live_spread_bps)
        sweeping_price = self.get_sweeping_price(depth_snapshot, side, cleaned_qty, current_mid_price)
        
        if side == "Buy":
            max_allowed_price = current_mid_price * (1.0 + (dynamic_cap_bps / 10000.0))
            target_price = min(sweeping_price, max_allowed_price)
        else:
            max_allowed_price = current_mid_price * (1.0 - (dynamic_cap_bps / 10000.0))
            target_price = max(sweeping_price, max_allowed_price)
            
        final_price_str = self._format_price_str(target_price, symbol)

        # 2. Fire Single Deep IOC
        try:
            response = await self.executor.safe_call(
                "POST", "/v5/order/create", is_execution=True,
                category="linear", symbol=symbol, side=side, orderType="Limit", 
                qty=qty_str, price=final_price_str, timeInForce="IOC", 
                positionIdx=self.position_idx
            )
            
            total_executed_qty, avg_price = 0.0, current_mid_price
            
            if response.get("retCode") == 0:
                order_id = response.get("result", {}).get("orderId", "UNKNOWN")
                fill_report = await self._verify_order_fill(symbol, order_id, timeout=0.2)
                
                if fill_report:
                    raw_exec, raw_avg = fill_report.get("cumExecQty"), fill_report.get("avgPrice")
                    total_executed_qty = float(raw_exec) if (raw_exec is not None and str(raw_exec).strip() != "") else 0.0
                    avg_price = float(raw_avg) if (raw_avg is not None and str(raw_avg).strip() != "") else current_mid_price
            else:
                logger.warning(f"[X-RAY] ⚠️ Primary IOC Strike rejected: {response.get('retMsg')}")

        except Exception as e:
            logger.error(f"[X-RAY] ❌ Primary IOC Strike API fault for {symbol}: {e}")
            total_executed_qty = 0.0

        # 3. Seamless Reversion Capture (The Peg Handoff)
        remainder = cleaned_qty - total_executed_qty
        if remainder > self.instrument_cache.get(symbol, {}).get("min_qty", 0.001):
            logger.warning(f"[X-RAY] ⚠️ Partial/Missed Sweep ({total_executed_qty:.4f}/{cleaned_qty:.4f}). Handing off remainder to Maker Peg.")
            
            # Immediately launch passive peg to catch the reversion
            peg_success, peg_price, peg_qty = await self._execute_dynamic_maker_peg(
                symbol, direction, remainder, sl, tp, depth_snapshot, timeout=4, regime=regime
            )
            
            if peg_success and peg_qty > 0:
                # Blended average pricing
                total_cost = (total_executed_qty * avg_price) + (peg_qty * peg_price)
                total_executed_qty += peg_qty
                avg_price = total_cost / total_executed_qty

        if total_executed_qty > 0:
            logger.critical(f"✅ SWEEP-TO-PEG SUCCESS // {symbol} filled {total_executed_qty:.4f} units at blended avg {avg_price:.4f}.")
            
            if sl or tp:
                final_sl = self._format_price_str(sl, symbol) if sl else None
                final_tp = self._format_price_str(tp, symbol) if tp else None
                try:
                    await self.executor.safe_call(
                        "POST", "/v5/position/trading-stop", is_execution=True,
                        category="linear", symbol=symbol, positionIdx=self.position_idx, 
                        stopLoss=final_sl, takeProfit=final_tp,
                        tpTriggerBy="LastPrice", slTriggerBy="LastPrice"
                    )
                except Exception as e: 
                    logger.error(f"[X-RAY] 🛑 Failed to attach stops for {symbol}: {e}")
                    
            return True, avg_price, total_executed_qty

        logger.error(f"[X-RAY] ❌ Sweep-to-Peg failed entirely. Liquidity evaporated.")
        return False, 0.0, 0.0

    async def _execute_dynamic_maker_peg(self, symbol: str, direction: str, qty: float, sl: Optional[float], tp: Optional[float], depth_snapshot: dict=None, timeout: int = 5, regime: str = "MEAN_REVERTING") -> Tuple[bool, float, float]:
        logger.info(f"🛡️ HFT MAKER-PEGGING INITIATED // {symbol}. Engaging In-Flight Spread Amendment. (Timeout: {timeout}s)")
        
        start_time = time.time()
        current_order_id = None
        side = "Buy" if direction.upper() == "BUY" else "Sell"
        
        anchor_price = None
        tick_size = self.instrument_cache.get(symbol, {"tick_size": 0.01})["tick_size"]
        current_peg_price = 0.0

        if sl is None or tp is None:
            current_mid = depth_snapshot.get("bids", [[100, 1]])[0][0] if depth_snapshot else 100.0
            sl = current_mid * 0.975 if side == "Buy" else current_mid * 1.025
            tp = current_mid * 1.050 if side == "Buy" else current_mid * 0.950

        best_bid = float(depth_snapshot.get("bids", [[100]])[0][0]) if depth_snapshot else 100.0
        best_ask = float(depth_snapshot.get("asks", [[100]])[0][0]) if depth_snapshot else 100.0
        live_spread_bps = ((best_ask - best_bid) / (best_bid + 1e-9)) * 10000.0
        dynamic_cap_bps = self.compute_dynamic_slippage_cap_bps(symbol, regime, live_spread_bps)
        max_chase_deviation = max(0.001, dynamic_cap_bps / 10000.0)

        cleaned_qty = self._apply_dynamic_exchange_limits(qty, best_bid, symbol)
        qty_str = self._format_qty_str(cleaned_qty, symbol)

        while time.time() - start_time < timeout:
            loop_delay = 0.5

            try:
                is_toxic = False
                
                if self.core_engine and hasattr(self.core_engine, 'orderbook_snapshots'):
                    fresh_ob = self.core_engine.orderbook_snapshots.get(symbol, depth_snapshot)
                else:
                    fresh_ob = depth_snapshot 
                
                if fresh_ob and "bids" in fresh_ob and fresh_ob["bids"]:
                    bid_vols = sum(float(l[1]) for l in fresh_ob["bids"][:3])
                    ask_vols = sum(float(l[1]) for l in fresh_ob["asks"][:3])
                    imbalance = (bid_vols - ask_vols) / (bid_vols + ask_vols + 1e-9)

                    if direction.upper() == "BUY" and imbalance > 0.80: is_toxic = True
                    elif direction.upper() == "SELL" and imbalance < -0.80: is_toxic = True

                    live_best_bid = float(fresh_ob["bids"][0][0])
                    live_best_ask = float(fresh_ob["asks"][0][0])
                    
                    if side == "Buy":
                        ideal_target = self._get_meaningful_tob(fresh_ob, side)
                        if is_toxic: ideal_target = live_best_bid - (tick_size * 2)
                        safe_target = min(ideal_target, live_best_ask - tick_size)
                    else:
                        ideal_target = self._get_meaningful_tob(fresh_ob, side)
                        if is_toxic: ideal_target = live_best_ask + (tick_size * 2)
                        safe_target = max(ideal_target, live_best_bid + tick_size)

                    target_price_str = self._format_price_str(safe_target, symbol)
                    target_price_float = float(target_price_str)
                    
                    if anchor_price is None: anchor_price = target_price_float

                    if side == "Buy" and target_price_float > anchor_price * (1 + max_chase_deviation):
                        logger.warning(f"[X-RAY] 🏃 CHASE ABORTED // {symbol} ran +{max_chase_deviation:.2%} beyond signal anchor.")
                        break
                    if side == "Sell" and target_price_float < anchor_price * (1 - max_chase_deviation):
                        logger.warning(f"[X-RAY] 🏃 CHASE ABORTED // {symbol} ran -{max_chase_deviation:.2%} beyond signal anchor.")
                        break

                if not current_order_id:
                    logger.info(f"[X-RAY] 🛡️ Dispatching Maker Peg for {symbol} clamped at {target_price_str}")
                    place_response = await self.executor.safe_call(
                        "POST", "/v5/order/create", is_execution=True,
                        category="linear", symbol=symbol, side=side, orderType="Limit",
                        qty=qty_str, price=target_price_str, timeInForce="PostOnly", 
                        positionIdx=self.position_idx
                    )
                    if place_response.get("retCode") == 0: 
                        current_order_id = place_response["result"]["orderId"]
                        current_peg_price = target_price_float
                    else:
                        logger.warning(f"[X-RAY] Peg placement rejected: {place_response.get('retMsg')}")
                        await asyncio.sleep(loop_delay); continue
                
                if current_order_id:
                    status_response = await self.executor.safe_call("GET", "/v5/order/realtime", category="linear", symbol=symbol, orderId=current_order_id)
                    order_list = status_response.get("result", {}).get("list", [])
                    
                    if not order_list:
                        fill_report = await self._verify_order_fill(symbol, current_order_id, timeout=0.2)
                        if fill_report:
                            raw_exec, raw_avg = fill_report.get("cumExecQty"), fill_report.get("avgPrice")
                            cum_exec = float(raw_exec) if (raw_exec is not None and str(raw_exec).strip() != "") else 0.0
                            avg_price = float(raw_avg) if (raw_avg is not None and str(raw_avg).strip() != "") else target_price_float

                            if cum_exec > 0:
                                logger.critical(f"✅ MAKER PEG RESOLVED // {symbol} secured {cum_exec} units at {avg_price}. Spread Captured!")
                                return True, avg_price, cum_exec
                        
                        current_order_id = None
                        continue
                            
                    order_info = order_list[0]
                    order_status = order_info.get("orderStatus")
                    
                    raw_exec_qty, raw_avg = order_info.get("cumExecQty"), order_info.get("avgPrice")
                    cum_exec_qty = float(raw_exec_qty) if (raw_exec_qty is not None and str(raw_exec_qty).strip() != "") else 0.0
                    avg_price = float(raw_avg) if (raw_avg is not None and str(raw_avg).strip() != "") else current_peg_price
                    
                    if order_status in ["Filled"]:
                        logger.critical(f"✅ MAKER PEG SECURED // {symbol} filled completely. Earned Maker Rebates.")
                        return True, avg_price, cum_exec_qty
                        
                    elif order_status in ["Cancelled", "Rejected"]: 
                        current_order_id = None 
                        if cum_exec_qty > 0:
                            logger.critical(f"✅ MAKER PEG PARTIAL // {symbol} secured {cum_exec_qty} units before rejection.")
                            return True, avg_price, cum_exec_qty
                            
                    elif order_status in ["New", "PartiallyFilled"]:
                        # 🚀 V36.0 FIX: Require 1.5x tick deviation to amend to preserve Queue Priority
                        if abs(target_price_float - current_peg_price) >= (tick_size * 1.5):
                            logger.debug(f"[X-RAY] 🔄 Amending {symbol} Maker Peg: {current_peg_price} -> {target_price_str}")
                            amend_res = await self.executor.safe_call(
                                "POST", "/v5/order/amend", is_execution=True, 
                                category="linear", symbol=symbol, orderId=current_order_id, 
                                price=target_price_str
                            )
                            if amend_res.get("retCode") == 0:
                                current_peg_price = target_price_float
                            else:
                                err_msg = amend_res.get("retMsg", "")
                                if "not modified" not in err_msg.lower():
                                    logger.warning(f"[X-RAY] Amend Failed: {err_msg}")

            except Exception as e: 
                error_str = str(e)
                if any(fatal in error_str for fatal in ["110126", "INNOVATION ZONE", "10002", "10001"]): break
                await asyncio.sleep(loop_delay) 

        if current_order_id:
            logger.warning(f"[X-RAY] ⏳ MAKER CHASE TIMEOUT // Canceling active {symbol} peg.")
            await self.cancel_order_safe(symbol, current_order_id)
            
            fill_report = await self._verify_order_fill(symbol, current_order_id, timeout=0.2)
            if fill_report:
                raw_exec, raw_avg = fill_report.get("cumExecQty"), fill_report.get("avgPrice")
                cum_exec = float(raw_exec) if (raw_exec is not None and str(raw_exec).strip() != "") else 0.0
                avg_price = float(raw_avg) if (raw_avg is not None and str(raw_avg).strip() != "") else anchor_price
                if cum_exec > 0: return True, avg_price, cum_exec
                    
        return False, 0.0, 0.0

    async def _execute_twap_iceberg(self, symbol: str, direction: str, total_qty: float, current_mid_price: float, sl: float, tp: float, slices: int = 4, slice_interval_sec: float = 5.0, regime: str = "TRENDING") -> Tuple[bool, float, float]:
        limits = self.instrument_cache.get(symbol, {"min_qty": 1.0})
        min_qty = limits["min_qty"]
        min_notional_qty = 6.50 / current_mid_price
        absolute_min_slice = max(min_qty, min_notional_qty)

        if (total_qty / slices) < absolute_min_slice:
            slices = max(1, math.floor(total_qty / absolute_min_slice))

        slice_qty = total_qty / slices
        total_executed_qty, weighted_notional_sum = 0.0, 0.0
        
        logger.critical(f"[X-RAY] 🧊 ICEBERG ENGAGED // {symbol} slicing into {slices} TWAP chunks of {slice_qty:.4f}.")
        
        is_major_asset = symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        chunk_timeout = 3.0 if is_major_asset else 6.0 
        
        for i in range(slices):
            logger.info(f"[X-RAY] 🧊 TWAP SLICE [{i+1}/{slices}] // Routing {slice_qty:.4f} {symbol}")
            success, fill_price, fill_qty = await self._execute_dynamic_maker_peg(
                symbol=symbol, direction=direction, qty=slice_qty, sl=sl, tp=tp, timeout=chunk_timeout, regime=regime
            )
            
            if success and fill_qty > 0:
                total_executed_qty += fill_qty
                weighted_notional_sum += (fill_price * fill_qty)
            else:
                logger.warning(f"[X-RAY] 🧊 TWAP SLICE FAILED // Maker Peg escaped. Skipping slice to prevent adverse selection.")
                
            if i < slices - 1: await asyncio.sleep(slice_interval_sec)
                
        if total_executed_qty > 0:
            avg_fill_price = weighted_notional_sum / total_executed_qty
            
            side = "Buy" if direction.upper() == "BUY" else "Sell"
            sl_dist_pct = abs(sl - current_mid_price) / (current_mid_price + 1e-9)
            tp_dist_pct = abs(tp - current_mid_price) / (current_mid_price + 1e-9)
            
            if side == "Buy":
                actual_sl, actual_tp = avg_fill_price * (1.0 - sl_dist_pct), avg_fill_price * (1.0 + tp_dist_pct)
            else:
                actual_sl, actual_tp = avg_fill_price * (1.0 + sl_dist_pct), avg_fill_price * (1.0 - tp_dist_pct)
            
            actual_sl_str = self._format_price_str(actual_sl, symbol)
            actual_tp_str = self._format_price_str(actual_tp, symbol)
            
            try:
                await self.executor.safe_call(
                    "POST", "/v5/position/trading-stop", is_execution=True,
                    category="linear", symbol=symbol, positionIdx=self.position_idx, 
                    takeProfit=actual_tp_str, stopLoss=actual_sl_str,
                    tpTriggerBy="LastPrice", slTriggerBy="LastPrice"
                )
            except Exception as e:
                logger.warning(f"[X-RAY] 🧊 Failed to reattach bracket to TWAP position: {e}")
                
            logger.critical(f"✅ ICEBERG COMPLETE // {symbol} secured {total_executed_qty:.4f} total units at avg price {avg_fill_price:.4f}.")
            return True, avg_fill_price, total_executed_qty
            
        logger.error(f"[X-RAY] ❌ ICEBERG FAILED // {symbol} could not secure any slices. Evaporated liquidity.")
        return False, 0.0, 0.0

    async def execute_alpha_signal(
        self, 
        symbol: str, 
        direction: str, 
        prob_success: float,
        exec_weight: float,
        current_mid_price: float, 
        sl_price: float,
        tp_price: float,
        inst_var: float,
        depth_snapshot: dict, 
        regime: str = "TRENDING"
    ) -> Tuple[bool, float, float]:
        """
        🚀 V36.0 DIRECT-DRIVE EXECUTION NEXUS
        Combines Risk Sizing, Firewall Assessment, and Topology Routing into a 
        single high-speed passthrough to eliminate Python Call-Stack overhead.
        """
        # 1. Capital & Risk Sizing
        try:
            raw_bal = await self.executor.get_wallet_balance_usdt()
            current_bal = max(1.0, raw_bal)
        except Exception:
            current_bal = 10.0

        sl_pct = abs(current_mid_price - sl_price) / current_mid_price
        tp_pct = abs(current_mid_price - tp_price) / current_mid_price
        
        target_notional = self.calculate_risk_adjusted_notional(
            prob_success, exec_weight, sl_pct, tp_pct, current_bal, inst_var
        )

        await self._fetch_exchange_limits(symbol)
        total_qty = self._apply_dynamic_exchange_limits(target_notional / current_mid_price, current_mid_price, symbol)

        if (total_qty * current_mid_price) < 6.0:
            logger.warning(f"[X-RAY] 🛑 INSUFFICIENT NOTIONAL // {symbol} Target Notional < Exchange Min. Aborting.")
            return False, current_mid_price, 0.0

        # 2. Dynamic Slippage Firewall
        arrival_price = current_mid_price
        ob = depth_snapshot or {}
        best_bid = float(ob.get("bids", [[current_mid_price, 1]])[0][0])
        best_ask = float(ob.get("asks", [[current_mid_price, 1]])[0][0])
        live_spread_bps = ((best_ask - best_bid) / (best_bid + 1e-9)) * 10000.0 if best_bid > 0 else 1.0
        
        dynamic_cap_bps = self.compute_dynamic_slippage_cap_bps(symbol, regime, live_spread_bps)
        est_slippage = self.estimate_orderbook_slippage_bps(ob, direction, total_qty, current_mid_price)
        
        if est_slippage > dynamic_cap_bps:
            logger.warning(f"[X-RAY] 🛑 DYNAMIC FIREWALL REJECT // {symbol} est. slippage {est_slippage:.1f} bps > Cap {dynamic_cap_bps:.1f} bps. Aborting.")
            return False, arrival_price, 0.0

        # 3. Whale Routing (Iceberg Protocol)
        top_bid_vol = sum(float(l[1]) for l in ob.get("bids", [])[:3])
        top_ask_vol = sum(float(l[1]) for l in ob.get("asks", [])[:3])
        avg_tob_vol = (top_bid_vol + top_ask_vol) / 2.0
        
        if avg_tob_vol > 0 and total_qty > (avg_tob_vol * 0.05):
            logger.info(f"[X-RAY] 🐋 WHALE ROUTING // {symbol} size > 5% of Top-of-Book depth. Triggering Iceberg Protocol.")
            return await self._execute_twap_iceberg(symbol, direction, total_qty, current_mid_price, sl_price, tp_price, regime=regime)

        # 4. Urgent Routing (Flash Strike IOC)
        book_skew = top_bid_vol / (top_ask_vol + 1e-9)
        urgent_taker = False
        
        if direction.upper() == "BUY" and (book_skew < 0.35 or exec_weight > 1.3): urgent_taker = True
        elif direction.upper() == "SELL" and (book_skew > 2.8 or exec_weight > 1.3): urgent_taker = True

        if urgent_taker or regime == "TRENDING":
            logger.info(f"[X-RAY] ⚡ ATOMIC TAKER IOC // {symbol} High conviction or momentum breaking. Routing immediate IOC.")
            return await self._execute_flash_strike(symbol, direction, total_qty, current_mid_price, sl_price, tp_price, depth_snapshot=ob, regime=regime)

        # 5. Passive Routing (Maker-Peg PostOnly)
        is_major_asset = symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        dynamic_timeout = 3.0 if is_major_asset else 6.0  
        
        logger.info(f"[X-RAY] 🕸️ PASSIVE REGIME ROUTING // Attempting Maker-Grid Peg on {symbol} ({dynamic_timeout}s timeout).")
        return await self._execute_dynamic_maker_peg(
            symbol, direction, total_qty, sl_price, tp_price, 
            depth_snapshot=ob, timeout=dynamic_timeout, regime=regime
        )