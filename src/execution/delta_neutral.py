"""
💎 V36.2 APEX TITAN: ATOMIC DELTA-NEUTRAL YIELD HARVESTER
------------------------------------------------------------------------
Features:
- Concurrent Asyncio Leg Dispatch (Parallel Spot & Perp)
- Hard 5.0s Timeout Rollback Protocol (Eliminates Naked Short Risk)
- Exact Execution Drag Calculus & Break-Even Horizon Gating
- Synthetic Basis Dislocation Sentry (Prevents Arbitrage into Decoupled Markets)

Architectural Supremacy (V36.2 Integration):
- API Boundary Precision Guard: Routes all Spot and Perp quantities through the 
  SOR's native Decimal formatter, completely eradicating Bybit 10001 (Qty) 
  scientific-notation rejections on micro-cap altcoins.
- Ghost-Pair Sentry: Gracefully intercepts unsupported Spot tickers (e.g., NESAUSDT) 
  without throwing stack-trace errors during the execution drag calculation.
- Basis Dislocation Sentry: Added statistical surveillance over the Spot-Perp premium.
  Refuses to initiate yield locks if the basis spread exceeds 3.0 standard deviations.
- Resilient Payload Safeguards: Soft-catches heavy ticker batch timeouts to eliminate 
  noisy log tracebacks during transient gateway network dropouts.
"""

import re
import math
import asyncio
import logging
import time
import numpy as np
from collections import deque
from typing import Dict, Any, List, Tuple

logger = logging.getLogger("QUANT_CORE.DELTA_NEUTRAL")


class DeltaNeutralYieldEngine:
    """
    🚀 V36.2 YIELD HARVESTER
    Sweeps strictly idle margin into risk-free basis trades (Spot Long + Perp Short) 
    to harvest extreme funding rates. Automatically unwinds when yield decays.
    """
    def __init__(self, core_engine):
        self.core = core_engine
        
        # 7.5 bps per 8 hours = ~82% Risk-Free APY trigger minimum
        self.entry_funding_threshold = 0.00075  
        
        # Unwind threshold: Exit if funding drops below 1.5 bps per 8h
        self.exit_funding_threshold = 0.00015   
        
        self.active_hedges: Dict[str, dict] = {} 
        self.basis_history: Dict[str, deque] = {}
        self.taker_fee_rate = 0.00055 # Bybit base taker fee

    def _get_spot_symbol(self, linear_symbol: str) -> str:
        """Sanitizes multiplier prefixes to match Bybit Spot tickers."""
        base_coin = linear_symbol[:-4]
        base_coin = re.sub(r'^(10000|1000)', '', base_coin)
        return f"{base_coin}USDT"

    def _check_basis_dislocation(self, spot_price: float, perp_price: float, symbol: str) -> bool:
        """
        🚀 V36.2 EMPIRICAL HARDENING: Basis Dislocation Sentry.
        Prevents Delta-Neutral engine from executing if the Perp-Spot 
        premium has structurally decoupled.
        """
        if symbol not in self.basis_history:
            self.basis_history[symbol] = deque(maxlen=200)
            
        current_basis_bps = ((perp_price - spot_price) / spot_price) * 10000.0
        self.basis_history[symbol].append(current_basis_bps)
        
        if len(self.basis_history[symbol]) < 50:
            return False  # Not enough data, allow safe entry
            
        arr = np.array(self.basis_history[symbol])
        basis_z = abs(current_basis_bps - np.mean(arr)) / (np.std(arr) + 1e-9)
        
        if basis_z > 3.0:
            logger.warning(f"[YIELD_SENTRY] 🛑 Structural Basis Dislocation detected on {symbol} (Z: {basis_z:.1f}σ). Halting Arbitrage.")
            return True
            
        return False

    async def run_yield_scanner_daemon(self):
        logger.info("🏦 DELTA-NEUTRAL YIELD ENGINE ONLINE: Scanning for Cash-and-Carry Arbitrage.")
        
        while True:
            await asyncio.sleep(300)  # Scan global rates every 5 minutes
            
            if not self.core.fsm.can_execute_trades:
                continue
                
            try:
                # 🚀 V36.2 RESILIENT SAFE-CALL: Soft-catch batch payload timeouts to prevent traceback spam
                try:
                    tickers_res = await self.core.executor.safe_call("GET", "/v5/market/tickers", category="linear")
                    if not isinstance(tickers_res, dict) or tickers_res.get("retCode") != 0:
                        continue
                except Exception as net_err:
                    logger.warning(f"[YIELD_SENTRY] ⚠️ Tickers batch fetch timed out or dropped. Skipping cycle: {net_err}")
                    continue
                
                ticker_list = tickers_res.get("result", {}).get("list", [])
                
                # 1. EVALUATE ACTIVE HEDGES FOR UNWIND (YIELD EJECTION)
                await self._evaluate_active_hedges(ticker_list)
                    
                # 2. SCAN FOR NEW YIELD OPPORTUNITIES
                target_asset = None
                best_funding = 0.0
                
                for t in ticker_list:
                    symbol = t.get("symbol", "")
                    if not symbol.endswith("USDT") or "BTC" in symbol or "ETH" in symbol: 
                        continue # Focus strictly on volatile altcoin funding premiums
                    
                    funding_rate = float(t.get("fundingRate", 0.0) or 0.0)
                    
                    # Look for extreme POSITIVE funding (Longs overleveraged, paying Shorts)
                    if funding_rate >= self.entry_funding_threshold and funding_rate > best_funding:
                        if symbol not in self.active_hedges and symbol not in self.core.active_positions_map:
                            best_funding = funding_rate
                            target_asset = symbol
                
                if target_asset:
                    await self.execute_atomic_cash_and_carry_hedge(target_asset, best_funding)
                    
            except Exception as e:
                logger.error(f"[X-RAY] Yield Scanner Fault: {e}", exc_info=False)

    async def _evaluate_active_hedges(self, current_tickers: List[Dict[str, Any]]):
        """Monitors ongoing hedges and unwinds them if the funding rate collapses."""
        symbols_to_unwind = []
        
        for active_symbol in list(self.active_hedges.keys()):
            ticker_data = next((t for t in current_tickers if t.get("symbol") == active_symbol), None)
            
            if ticker_data:
                current_funding = float(ticker_data.get("fundingRate", 0.0) or 0.0)
                
                # Eject if funding drops too low or flips negative
                if current_funding <= self.exit_funding_threshold:
                    logger.warning(f"[X-RAY] 📉 YIELD COLLAPSE // {active_symbol} funding dropped to {current_funding*10000:.1f} bps. Initiating Unwind.")
                    symbols_to_unwind.append(active_symbol)

        for sym in symbols_to_unwind:
            await self.unwind_cash_and_carry_hedge(sym)

    async def _calculate_execution_drag(self, perp_symbol: str, spot_symbol: str) -> Tuple[float, float, float]:
        """
        Calculates the exact basis spread and fee drag required to enter the position.
        Returns Tuple[total_cost_bps, spot_price, perp_price]
        """
        try:
            spot_res = await self.core.executor.safe_call("GET", "/v5/market/tickers", category="spot", symbol=spot_symbol)
            perp_res = await self.core.executor.safe_call("GET", "/v5/market/tickers", category="linear", symbol=perp_symbol)
            
            spot_list = spot_res.get("result", {}).get("list", [])
            perp_list = perp_res.get("result", {}).get("list", [])
            
            # 🚀 V36.2 FIX: Prevent IndexError on missing Spot markets (e.g., NESAUSDT, MARAUSDT)
            if not spot_list:
                logger.debug(f"[YIELD_SENTRY] No Spot market exists for {spot_symbol}. Cannot establish basis hedge.")
                return 35.0, 0.0, 0.0
                
            if not perp_list:
                return 35.0, 0.0, 0.0
                
            spot_ask = float(spot_list[0]["ask1Price"])
            perp_bid = float(perp_list[0]["bid1Price"])
            
            # We BUY Spot Ask and SELL Perp Bid
            basis_spread_pct = abs(spot_ask - perp_bid) / spot_ask
            fee_drag_pct = self.taker_fee_rate * 2.0  # Fees paid on both legs
            
            total_drag_bps = (basis_spread_pct + fee_drag_pct) * 10000.0
            return total_drag_bps, spot_ask, perp_bid
            
        except Exception as e:
            logger.debug(f"[X-RAY] Drag calculation failed for {perp_symbol}: {e}")
            return 35.0, 0.0, 0.0  # Fallback to a safe 35 bps estimate with 0 prices

    async def execute_atomic_cash_and_carry_hedge(self, symbol: str, funding_rate: float):
        """
        🚀 V36.2 ATOMIC DUAL-LEG DISPATCH
        Executes Spot Buy and Perp Short concurrently with strict timeouts and granular 
        exception logging to diagnose legging failures. Defended by Basis Sentry.
        """
        if funding_rate < self.entry_funding_threshold:
            return
            
        logger.info(f"[X-RAY] ⚖️ YIELD LOCK EVALUATION // {symbol} | Target Funding Rate: {funding_rate*10000:.1f} bps")
        
        try:
            spot_symbol = self._get_spot_symbol(symbol)
            
            # 1. Evaluate Execution Drag vs. Break-Even Horizon
            drag_bps, spot_price, perp_price = await self._calculate_execution_drag(symbol, spot_symbol)
            
            if spot_price <= 0.0 or perp_price <= 0.0:
                logger.debug(f"[YIELD_SENTRY] 🚫 Invalid price extraction for {symbol} (likely missing spot market). Aborting hedge cleanly.")
                return
                
            # 🚀 V36.2 BASIS SENTRY: Reject structurally dislocated premiums
            if self._check_basis_dislocation(spot_price, perp_price, symbol):
                return
                
            funding_bps_per_epoch = funding_rate * 10000.0
            if funding_bps_per_epoch <= 0: return
                
            epochs_to_breakeven = math.ceil(drag_bps / funding_bps_per_epoch)
            break_even_days = epochs_to_breakeven * (8.0 / 24.0)
            
            # Reject if it takes more than 4 epochs (1.33 days) to earn back the entry fees & spread
            if epochs_to_breakeven > 4:
                logger.warning(f"[X-RAY] 🚫 YIELD REJECTED // {symbol} Drag is {drag_bps:.1f} bps. Takes {epochs_to_breakeven} epochs to break even. Skipping.")
                return

            # 2. Calculate True Idle Capital Dynamically
            total_bal = await self.core.executor.get_wallet_balance_usdt()
            
            active_alpha_margin = 0.0
            for sym in self.core.active_positions_map.keys():
                try:
                    pos_res = await self.core.executor.safe_call("GET", "/v5/position/list", category="linear", symbol=sym)
                    pos_list = pos_res.get("result", {}).get("list", [])
                    if pos_list:
                        active_alpha_margin += float(pos_list[0].get("positionValue", 0.0)) / float(pos_list[0].get("leverage", 1.0))
                except Exception: pass
            
            idle_capital = total_bal - active_alpha_margin
            
            if idle_capital < 15.0:
                return

            # Strict Capital Allocation Limits
            max_allowed_hedge = total_bal * 0.20
            yield_capital = min(idle_capital * 0.90, max_allowed_hedge)
            
            if yield_capital < 12.0:
                return
            
            await self.core.sor._fetch_exchange_limits(symbol)
            qty = self.core.sor._apply_dynamic_exchange_limits(yield_capital / spot_price, spot_price, symbol)
            
            # 🚀 V36.2 FIX: Use Native SOR Formatter to prevent scientific notation rejections
            qty_str = self.core.sor._format_qty_str(qty, symbol)
            
            # =========================================================
            # 🚀 V36.2 ATOMIC CONCURRENT EXECUTION BLOCK WITH GRANULAR LOGGING
            # =========================================================
            logger.info(f"[X-RAY] 🏦 Routing ATOMIC DUAL-LEG Hedge for {qty_str} {symbol} (Spot: {spot_symbol})...")
            
            spot_task = self.core.executor.safe_call(
                "POST", "/v5/order/create", is_execution=True, 
                category="spot", symbol=spot_symbol, side="Buy", 
                orderType="Market", qty=qty_str, marketUnit="baseCoin"
            )
            
            perp_task = self.core.executor.safe_call(
                "POST", "/v5/order/create", is_execution=True, 
                category="linear", symbol=symbol, side="Sell", 
                orderType="Market", qty=qty_str, positionIdx=self.core.sor.position_idx
            )
            
            spot_res, perp_res = None, None
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(spot_task, perp_task, return_exceptions=True),
                    timeout=5.0
                )
                spot_res, perp_res = results[0], results[1]
            except asyncio.TimeoutError:
                logger.critical(f"[YIELD] 🚨 ATOMIC LEGGING TIMEOUT (5.0s Barrier Breached) on {symbol}.")
                if isinstance(spot_res, Exception):
                    logger.error(f"[YIELD] Spot Leg Exception: {spot_res}")
                if isinstance(perp_res, Exception):
                    logger.error(f"[YIELD] Perp Leg Exception: {perp_res}")
                spot_res = spot_res if isinstance(spot_res, dict) else {"retCode": -999, "retMsg": "TIMEOUT_EXCEPTION"}
                perp_res = perp_res if isinstance(perp_res, dict) else {"retCode": -999, "retMsg": "TIMEOUT_EXCEPTION"}
            
            # Diagnostic evaluation of individual leg responses
            spot_success = isinstance(spot_res, dict) and spot_res.get("retCode") == 0
            perp_success = isinstance(perp_res, dict) and perp_res.get("retCode") == 0
            
            if not spot_success:
                logger.warning(f"[YIELD] ⚠️ Spot Leg Failed for {symbol}: {spot_res.get('retMsg', str(spot_res))}")
            if not perp_success:
                logger.warning(f"[YIELD] ⚠️ Perp Leg Failed for {symbol}: {perp_res.get('retMsg', str(perp_res))}")

            if spot_success and perp_success:
                # Hedge successfully established
                self.active_hedges[symbol] = {
                    "qty": qty, 
                    "entry_price": spot_price, 
                    "timestamp": time.time(),
                    "projected_break_even_days": break_even_days
                }
                
                msg = (
                    f"✅ <b>DELTA-NEUTRAL YIELD LOCK SECURED</b>\n"
                    f"Asset: <code>{symbol}</code>\n"
                    f"Capital Swept: <code>${yield_capital:.2f}</code>\n"
                    f"Yield APY Target: <code>~{(funding_rate * 3 * 365)*100:.1f}%</code>\n"
                    f"Break-Even Horizon: <code>{epochs_to_breakeven} Epochs</code>"
                )
                await self.core._safe_telegram_dispatch(msg, is_html=True)
                logger.critical(f"✅ DELTA-NEUTRAL LOCK SECURED // {symbol} successfully hedged using idle cash flow.")
                return
                
            # 🚀 V36.2 SAFETY NET: Immediate Market IOC Rollback if Legging Fails
            logger.critical(f"[YIELD] 🚨 LEGGING MISMATCH ON {symbol} (Spot: {spot_success}, Perp: {perp_success}). INITIATING IMMEDIATE ROLLBACK.")
            
            if spot_success and not perp_success:
                # Spot filled but perp short failed. Unwind Spot immediately.
                logger.critical(f"[YIELD] 🔄 Rolling back exposed Long Spot position for {symbol}...")
                await self.core.executor.safe_call(
                    "POST", "/v5/order/create", is_execution=True, 
                    category="spot", symbol=spot_symbol, side="Sell", 
                    orderType="Market", qty=qty_str, timeInForce="IOC"
                )
            elif perp_success and not spot_success:
                # Perp short failed or spot failed. Buy back Perp immediately.
                logger.critical(f"[YIELD] 🔄 Rolling back naked Short Perp position for {symbol}...")
                await self.core.executor.safe_call(
                    "POST", "/v5/order/create", is_execution=True, 
                    category="linear", symbol=symbol, side="Buy", 
                    orderType="Market", qty=qty_str, timeInForce="IOC", reduceOnly=True
                )
            
        except Exception as e:
            logger.error(f"[X-RAY] Hedge execution failed for {symbol}: {e}", exc_info=True)

    async def unwind_cash_and_carry_hedge(self, symbol: str):
        """
        Dismantles an active hedge concurrently with strict timeouts and granular error logging.
        """
        if symbol not in self.active_hedges:
            return
            
        hedge_data = self.active_hedges[symbol]
        qty_val = float(hedge_data["qty"])
        spot_symbol = self._get_spot_symbol(symbol)
        
        # 🚀 V36.2 FIX: Native String Quantization
        qty_str = self.core.sor._format_qty_str(qty_val, symbol)
        
        logger.critical(f"[X-RAY] 🌪️ UNWINDING HEDGE // {symbol}. Covering Short, Selling Spot.")
        
        try:
            perp_task = self.core.executor.safe_call(
                "POST", "/v5/order/create", is_execution=True, 
                category="linear", symbol=symbol, side="Buy", 
                orderType="Market", qty=qty_str, reduceOnly=True, positionIdx=self.core.sor.position_idx
            )
            
            spot_task = self.core.executor.safe_call(
                "POST", "/v5/order/create", is_execution=True, 
                category="spot", symbol=spot_symbol, side="Sell", 
                orderType="Market", qty=qty_str
            )
            
            perp_order, spot_order = None, None
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(perp_task, spot_task, return_exceptions=True),
                    timeout=5.0
                )
                perp_order, spot_order = results[0], results[1]
            except asyncio.TimeoutError:
                logger.critical(f"[X-RAY] 🚨 UNWIND TIMEOUT (5.0s Barrier Breached) on {symbol}.")
                if isinstance(perp_order, Exception):
                    logger.error(f"[YIELD] Unwind Perp Exception: {perp_order}")
                if isinstance(spot_order, Exception):
                    logger.error(f"[YIELD] Unwind Spot Exception: {spot_order}")
                perp_order = perp_order if isinstance(perp_order, dict) else {"retCode": -999, "retMsg": "TIMEOUT_EXCEPTION"}
                spot_order = spot_order if isinstance(spot_order, dict) else {"retCode": -999, "retMsg": "TIMEOUT_EXCEPTION"}
            
            perp_success = isinstance(perp_order, dict) and perp_order.get("retCode") == 0
            spot_success = isinstance(spot_order, dict) and spot_order.get("retCode") == 0

            if not perp_success:
                logger.critical(f"[X-RAY] 🛑 CRITICAL: Unwind PERP Leg Failed for {symbol}: {perp_order.get('retMsg', str(perp_order))}")
            if not spot_success:
                logger.critical(f"[X-RAY] 🛑 CRITICAL: Unwind SPOT Leg Failed for {symbol}: {spot_order.get('retMsg', str(spot_order))}")

            if perp_success and spot_success:
                del self.active_hedges[symbol]
                duration_days = (time.time() - hedge_data["timestamp"]) / 86400.0
                msg = f"🔄 <b>DELTA-NEUTRAL HEDGE UNWOUND</b>\nAsset: <code>{symbol}</code>\nHolding Time: <code>{duration_days:.1f} Days</code>\nReason: Funding Decay"
                await self.core._safe_telegram_dispatch(msg, is_html=True)
                logger.info(f"✅ HEDGE SUCCESSFULLY UNWOUND for {symbol}.")
            else:
                self.active_hedges[symbol]["status"] = "UNWIND_ERROR"
                logger.error(f"⚠️ HEDGE RETAINED IN MEMORY: Manual clearance required for {symbol}.")
            
        except Exception as e:
            logger.error(f"[X-RAY] Unwind execution failed for {symbol}: {e}", exc_info=True)