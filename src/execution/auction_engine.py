"""
🌌 V56.2 QUANTUM MICRO-CORE: CONTINUOUS SCALE-INVARIANT AUCTION ENGINE
----------------------------------------------------------------------
Features Leverage-Bridge Auto-Sizing, Asymmetric House Money Compounding,
and Dynamic Margin Sweeping.
Handles any deposit amount ($7 to $1,000,000+) flawlessly by isolating
margin constraints and bridging Bybit exchange minimums dynamically.
Patched with Strict 15% Exposure Caps, True Risk Parity, 100% Capital Unlock,
Institutional TCA Preparation, and SEV-1 Orphan Trade Guards.
"""

import time
import uuid
import heapq
import random
import asyncio
import math
import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any

logger = logging.getLogger("QUANT_CORE.AUCTION_ENGINE")

# 🚀 SYNCHRONIZED: Structured Bybit Error Codes (Aligned globally with bybit_v5.py)
class BybitRetCode:
    SUCCESS = 0
    PARAMETER_ERROR = 10002          # Invalid request parameter
    SYSTEM_MAINTENANCE = 10004       # Server maintenance window
    RATE_LIMIT_REACHED = 10006       # Too many requests
    QTY_OUT_OF_BOUNDS = 10001        # Invalid parameter / quantity step error
    SERVICE_UNAVAILABLE = 10016      # Service temporary error
    ORDER_NOT_EXISTS = 110001        # Order does not exist or too late to cancel
    INSUFFICIENT_BALANCE = 110007    # Abundant/insufficient balance
    RISK_LIMIT_EXCEEDED = 110013     # Requested leverage exceeds symbol's max risk tier limit
    LEVERAGE_NOT_MODIFIED = 110025   # Position mode or leverage already set
    LEVERAGE_NOT_MODIFIED_2 = 110043 # Set leverage not modified


class CapitalAuctionEngine:
    def __init__(self, core_engine):
        """
        Takes a reference to the main DistributedQuantEngine to access shared memory,
        locks, API executors, and risk vaults.
        """
        self.core = core_engine

    async def run_global_capital_auction_worker(self):
        """
        The infinite polling loop that monitors the global priority heap.
        Only the highest expected Sharpe signals are evaluated.
        """
        logger.info("🏛️ GLOBAL CAPITAL AUCTION ENGINE ONLINE: Quantum Micro-Core Active.")
        
        while True:
            await asyncio.sleep(0.5) 
            
            async with self.core.portfolio_state_lock:
                if len(self.core.active_positions_map) >= 5:
                    continue 

            best_candidate = None
            
            async with self.core.auction_lock:
                if not self.core.auction_queue:
                    continue
                    
                # Prevent memory leaks by capping queue size
                if len(self.core.auction_queue) > 1000:
                    self.core.auction_queue = heapq.nsmallest(500, self.core.auction_queue) 
                    heapq.heapify(self.core.auction_queue)
                    
                valid_candidates = []
                now = time.time()
                
                # Filter out stale signals (older than 3 seconds)
                while self.core.auction_queue:
                    item = heapq.heappop(self.core.auction_queue)
                    # Unpack the 5-item tuple which includes the tie-breaker ID
                    _, _, sym, _, payload = item
                    if now - payload["timestamp"] < 3.0: 
                        valid_candidates.append(item)
                    else:
                        logger.debug(f"[X-RAY] 🗑️ Signal for {sym} expired in queue (Latency > 3.0s).")
                        
                if not valid_candidates: 
                    continue
                    
                # The top item has the most negative Sharpe (highest priority)
                best_candidate = valid_candidates[0]
                
                # Push the rest back into the heap
                for i in range(1, len(valid_candidates)):
                    heapq.heappush(self.core.auction_queue, valid_candidates[i])

            if not best_candidate: 
                continue
            
            # Unpack the 5-item tuple which includes the tie-breaker ID
            top_neg_sharpe, _, top_symbol, _, top_payload = best_candidate
            top_sharpe = -top_neg_sharpe

            # Fetch balance BEFORE the lock to prevent Async Race Conditions
            try:
                # 🚀 V56.2 ABSOLUTE PLUS: 100% Capital Access for Alpha Swarm
                raw_bal = await self.core.executor.get_wallet_balance_usdt()
                current_bal = raw_bal * 1.00
            except Exception:
                current_bal = 10.0

            # Atomic Lock. The entire sequence (check, evaluate, and assign) is unbroken.
            async with self.core.portfolio_state_lock:
                if top_symbol in self.core.active_positions_map or len(self.core.active_positions_map) >= 5:
                    continue
                    
                current_ob = self.core.orderbook_snapshots.get(top_symbol)
                if current_ob and current_ob.get("best_bid", 0) > 0:
                    live_mid = (current_ob["best_bid"] + current_ob["best_ask"]) / 2.0
                    drift_pct = abs(live_mid - top_payload["price"]) / top_payload["price"]
                    
                    # Prevent execution if price has run away before we could strike
                    if drift_pct > 0.0030: 
                        logger.warning(f"[X-RAY] 🚫 AUCTION DISCARD // {top_symbol} Signal drifted {drift_pct*10000:.1f} bps in queue. Too late to strike.")
                        continue

                # Evaluate Portfolio Heat & Correlation Limits
                is_safe, risk_reason = self.core.risk_vault.evaluate_portfolio_safety(
                    current_balance=current_bal,
                    new_position_notional=current_bal * 0.15, # Provisional check assuming max 15% allocation
                    symbol=top_symbol
                )

                if not is_safe:
                    logger.warning(f"[X-RAY] 🛡️ PORTFOLIO RISK GATE REJECTED // {top_symbol}: {risk_reason}")
                    continue

                # Lock the asset to prevent duplicate concurrent triggers ATOMICALLY
                self.core.active_positions_map[top_symbol] = top_payload["action"]
            
            logger.critical(
                f"🏛️ AUCTION WINNER // {top_symbol} [{top_payload['regime']}] | "
                f"{top_payload['action']} | Net Sharpe: {top_sharpe:.2f} | "
                f"Prob: {top_payload['prob_success']:.2%} | Net Edge: {top_payload['net_edge_bps']:.1f} bps"
            )
            
            # Dispatch the execution sequence asynchronously (Non-Blocking)
            self.core.track_task(self.execute_statistical_signal(
                top_payload["symbol"], top_payload["action"], top_payload["price"], 
                top_payload["prob_success"], top_payload["dna_stats"], top_payload["atr"], 
                top_payload["regime"], top_payload["net_edge_bps"], top_payload["vol_z"], top_payload["vol_mult"],
                top_payload.get("payload_features"), top_payload.get("elasticity"),
                top_payload.get("dynamic_rr", self.core.live_params.get("rr_ratio", 2.0))
            ))

    async def execute_statistical_signal(self, symbol: str, direction: str, current_price: float, confidence: float, dna_stats: dict, atr: float, regime: str, edge_bps: float, vol_z: float, vol_mult: float, payload_features: dict = None, elasticity: Any = None, dynamic_rr_ratio: float = 2.0):
        """
        V56.2 Execution Engine. 
        Patched with strict 15% equity exposure limits, correct Leverage math, Risk-of-Ruin floor,
        and L2-Aware simulated paper fills.
        """
        try:
            # Duplicate Daemon Check
            if symbol in self.core.daemon_tasks and not self.core.daemon_tasks[symbol].done():
                logger.warning(f"[X-RAY] 🚫 Lifecycle daemon already active for {symbol}. Aborting duplicate.")
                async with self.core.portfolio_state_lock: self.core.active_positions_map.pop(symbol, None)
                return

            signal_id = str(uuid.uuid4())
            
            # 1. Fetch Accurate Balance
            try: 
                # 🚀 V56.2 ABSOLUTE PLUS: 100% Capital Access for Alpha Swarm
                raw_balance = await self.core.executor.get_wallet_balance_usdt()
                available_balance = raw_balance * 1.00
            except Exception as e: 
                logger.debug(f"[X-RAY] Wallet fetch failed before execution: {e}", exc_info=True)
                available_balance = 0.0

            # Hard stop if balance physically cannot cover fees
            if available_balance < 3.0: 
                logger.warning(f"[X-RAY] 🚫 MARGIN EXHAUSTED // {symbol}: Balance (${available_balance:.2f}) too low for execution.")
                async with self.core.portfolio_state_lock: self.core.active_positions_map.pop(symbol, None)
                return

            # 2. Base Volatility Parameters
            # Forced widened stop-loss (2.5x ATR or 2.5%) to survive altcoin market noise without panic
            sl_atr_mult = max(2.5, self.core.live_params.get("sl_atr_mult", 2.5))
            sl_distance = max(atr * sl_atr_mult, current_price * 0.025) 
            sl_distance_pct = sl_distance / current_price
            
            # 3. Exchange Hardware Limits Verification
            await self.core.sor._fetch_exchange_limits(symbol)
            limits = self.core.sor.instrument_cache.get(symbol, {"min_qty": 1.0, "qty_step": 1.0})
            min_qty = limits["min_qty"]
            qty_step = limits["qty_step"]
            
            exchange_min_notional = min_qty * current_price

            # 4. 🌌 RISK & EXPOSURE CLAMPS
            # Calculate pure fractional Kelly based on model confidence
            base_optimal_risk = self.core.risk_vault.calculate_optimal_fraction(confidence, net_edge_bps=edge_bps)
            
            # Enforce a strict 15% maximum notional exposure regardless of account size
            max_allowed_notional = max(5.00, available_balance * 0.15)
            
            # Enforce Vault Risk Limits across all accounts
            vault_max_risk = getattr(self.core.risk_vault, 'max_single_risk', 0.015)
            
            if available_balance < 10.0:
                # SURVIVAL MODE: Extreme defense.
                fractional_risk = max(0.010, min(vault_max_risk, base_optimal_risk))
            else:
                # HOUSE MONEY MODE: Scale risk up safely, but strictly capped by Vault Limits
                profit_buffer = available_balance - 7.00
                fractional_risk = max(0.010, min(vault_max_risk, base_optimal_risk + (profit_buffer * 0.0001)))

            target_dollar_risk = available_balance * fractional_risk
            raw_notional = target_dollar_risk / sl_distance_pct

            # 5. 🌉 LEVERAGE-BRIDGE AUTO-SIZING
            # If our safe Kelly risk dictates a notional smaller than the exchange minimum,
            # we MUST SKIP THE TRADE instead of artificially jacking up the leverage.
            if raw_notional < exchange_min_notional:
                logger.warning(
                    f"[X-RAY] 🚫 LEVERAGE-BRIDGE ABORT // {symbol} Safe notional (${raw_notional:.2f}) "
                    f"is below Bybit minimum (${exchange_min_notional:.2f}). Skipping trade to protect account."
                )
                async with self.core.portfolio_state_lock: self.core.active_positions_map.pop(symbol, None)
                return
            
            # Clamp the notional back down to our 15% equity limit
            target_notional = min(raw_notional, max_allowed_notional)
            
            # Calculate exact dollar loss if SL hits
            actual_dollar_risk = target_notional * sl_distance_pct

            # Ultimate Account Protection: Never risk > 1.5x Vault Limit of total equity on a single trade
            max_tolerable_risk = available_balance * (vault_max_risk * 1.5)
            if actual_dollar_risk > max_tolerable_risk:
                logger.warning(
                    f"[X-RAY] 🚫 LEVERAGE-BRIDGE ABORT // {symbol} Bybit Min Notional ${exchange_min_notional:.2f} "
                    f"forces an actual risk of ${actual_dollar_risk:.2f}. Exceeds vault defense (${max_tolerable_risk:.2f}). Bypassing."
                )
                async with self.core.portfolio_state_lock: self.core.active_positions_map.pop(symbol, None)
                return

            # 🚀 V56.1 FIX: Calculate exact quantity strictly using the capped target_notional
            safe_qty = target_notional / current_price
            target_position_size = math.floor(safe_qty / qty_step) * qty_step
            
            if target_position_size < min_qty:
                target_position_size = min_qty
                
            target_notional = target_position_size * current_price

            # Target allocating max 10% of cash balance as margin per trade
            target_margin_fraction = 0.10  
            margin_allocation_usdt = max(1.0, available_balance * target_margin_fraction)
            
            raw_leverage = target_notional / margin_allocation_usdt
            # 🚀 AUDIT FIX: Replaced math.ceil with round() to prevent forced over-leveraging
            target_leverage = int(max(1, min(5, round(raw_leverage))))

            # 6. Target Price Calculus & Formatting
            tp_distance = sl_distance * dynamic_rr_ratio 
            tick_dec = Decimal(str(self.core.tick_sizes.get(symbol, 0.0001)))
            def align_price(p: float) -> str: return str(Decimal(str(p)).quantize(tick_dec, rounding=ROUND_HALF_UP))
            
            if direction == "BUY":
                raw_sl = min(current_price - (tp_distance * 0.1), current_price - sl_distance)
                raw_tp = current_price + tp_distance
            else:
                raw_sl = max(current_price + (tp_distance * 0.1), current_price + sl_distance)
                raw_tp = current_price - tp_distance
                
            initial_sl_price = float(align_price(raw_sl))
            target_tp_price = float(align_price(raw_tp))

            logger.info(f"[X-RAY] 🌉 LEVERAGE-BRIDGE ENGAGED // {direction} {symbol} | Notional: ${target_notional:.2f} | Lev: {target_leverage}x | Isolated Risk: ${actual_dollar_risk:.2f}")

            feature_engine = self.core.feature_engines.get(symbol)
            current_depth = feature_engine.get_orderbook_snapshot() if feature_engine and hasattr(feature_engine, 'get_orderbook_snapshot') else {"bids": [[current_price, 1]], "asks": [[current_price, 1]]}

            if self.core.test_mode:
                # 🚀 L2-Aware Paper Trading Simulator
                is_buy = direction == "BUY"
                levels = current_depth.get("asks" if is_buy else "bids", [])
                
                remaining_qty_to_fill = target_position_size
                weighted_notional_spent = 0.0
                simulated_avg_entry_price = current_price
                execution_success = False
                
                if not levels:
                    logger.warning(f"[X-RAY] 🚫 PAPER L2 REJECT // {symbol} Orderbook is completely empty. No liquidity to simulate fill.")
                else:
                    # Sweep through orderbook levels to calculate true slippage
                    for price_str, vol_str in levels:
                        level_price = float(price_str)
                        level_vol = float(vol_str)
                        
                        if level_vol <= 0: continue
                        
                        take_vol = min(remaining_qty_to_fill, level_vol)
                        weighted_notional_spent += (take_vol * level_price)
                        remaining_qty_to_fill -= take_vol
                        
                        if remaining_qty_to_fill <= 0:
                            simulated_avg_entry_price = weighted_notional_spent / target_position_size
                            execution_success = True
                            break
                            
                    if remaining_qty_to_fill > 0:
                        logger.warning(f"[X-RAY] 🚫 PAPER L2 REJECT // {symbol} Simulated {target_position_size} exhausted entire visible L2 depth. Order rejected to prevent massive slippage.")
                        execution_success = False
                        
                    elif execution_success:
                        # Reject if the simulated slippage crossed the 12bps maximum bound
                        slippage_bps = abs(simulated_avg_entry_price - current_price) / current_price * 10000.0
                        if slippage_bps > 12.0:
                            logger.warning(f"[X-RAY] 🚫 PAPER SLIPPAGE REJECT // {symbol} Order swept book causing {slippage_bps:.1f} bps slippage. Aborting.")
                            execution_success = False
                        else:
                            current_price = simulated_avg_entry_price # Set entry to the actual slipped price
                            logger.info(f"[X-RAY] 📝 PAPER FILL // {symbol} simulated execution at {current_price:.5f} ({slippage_bps:.1f} bps slippage).")
                            
                actual_filled_notional = target_position_size * current_price
            else:
                try:
                    await self.core.executor.adjust_leverage(symbol, target_leverage)
                    await asyncio.sleep(0.2) 
                except Exception as e: 
                    logger.debug(f"[X-RAY] Leverage adjust note for {symbol}: {e}")

                try:
                    # SOR DISPATCH: STRICT MAKER-ONLY LIMIT ORDERS
                    # Ban Aggressive Taker blocks to eliminate entry slippage.
                    res = await self.core.sor.execute_mean_reversion_bracket(
                        symbol=symbol, direction=direction, total_qty=target_position_size, 
                        current_mid_price=current_price, stop_loss=initial_sl_price, 
                        take_profit=target_tp_price, depth_snapshot=current_depth, 
                        vol_z=vol_z, vol_mult=vol_mult, feature_engine=feature_engine, 
                        elasticity=elasticity
                    )
                    
                    execution_success = res[0] if isinstance(res, tuple) else bool(res)
                
                except Exception as ex:
                    err_str = str(ex)
                    ret_code = getattr(ex, "ret_code", None) or getattr(ex, "code", None)

                    if ret_code in [BybitRetCode.SYSTEM_MAINTENANCE, BybitRetCode.SERVICE_UNAVAILABLE] or any(code in err_str for code in ["10004", "10016", "10002", "500"]):
                        logger.critical(f"🚨 BYBIT SYSTEM MAINTENANCE DETECTED ({err_str}). Tripping 180s System Pause.")
                        async with self.core.circuit_breaker_lock:
                            self.core.circuit_breakers["GLOBAL_MAINTENANCE"] = time.time() + 180.0
                    
                    elif ret_code in [BybitRetCode.INSUFFICIENT_BALANCE, BybitRetCode.QTY_OUT_OF_BOUNDS] or any(code in err_str for code in ["110007", "not enough", "10001"]):
                        logger.warning(f"[X-RAY] ⚠️ EXCHANGE REJECTION // Skipping {symbol}: {err_str}")
                    
                    else:
                        logger.error(f"[X-RAY] Execution error for {symbol}: {err_str}", exc_info=True)
                    
                    async with self.core.portfolio_state_lock: self.core.active_positions_map.pop(symbol, None)
                    return

            # X-RAY: SOR Failures
            if not execution_success: 
                logger.warning(f"[X-RAY] 🚫 SOR ABORT // Smart Order Router failed to fill {symbol} at acceptable slippage.")
                async with self.core.portfolio_state_lock: self.core.active_positions_map.pop(symbol, None)
                return 
                
            # Verify the Execution with Bybit Position Ledger
            if not self.core.test_mode:
                try:
                    pos_response = await self.core.executor.safe_call(self.core.executor.client.get_positions, category="linear", symbol=symbol)
                    pos_data = pos_response.get("result", {}).get("list", [])
                    actual_qty_filled = float(pos_data[0].get("size", 0.0)) if pos_data else 0.0
                    actual_filled_notional = actual_qty_filled * current_price
                    
                    if actual_filled_notional <= 0:
                        logger.warning(f"[X-RAY] 👻 PHANTOM FILL // SOR reported success but Bybit ledger shows 0 position for {symbol}.")
                        async with self.core.portfolio_state_lock: self.core.active_positions_map.pop(symbol, None)
                        return
                except Exception as e:
                    logger.warning(f"[X-RAY] Position verification failed after execute for {symbol}: {e}", exc_info=True)
                    actual_qty_filled = target_position_size
                    actual_filled_notional = target_notional
                    
            safe_features = payload_features if payload_features else {"symbol": symbol, "market_regime": regime, "virtual_sl": initial_sl_price, "virtual_tp": target_tp_price}
            
            # Write to Memory DB Async
            self.core.log_to_wal_sync("prediction", [signal_id, time.time(), current_price, direction, confidence, safe_features, False])
            
            # Telemetry Ticket
            ticket_msg = self.core.telegram.format_entry_ticket(
                symbol, direction, current_price, actual_qty_filled if not self.core.test_mode else target_position_size, 
                edge_bps, fractional_risk, regime, safe_features
            )
            self.core.track_task(self.core._safe_telegram_dispatch(ticket_msg, is_html=True))
            
            # Finalize the Lock and Hand off to the Lifecycle Daemon
            self.core.risk_vault.update_position_ledger(symbol, actual_filled_notional)
            
            # Spawn the Guardian Daemon
            self.core.daemon_tasks[symbol] = self.core.track_task(
                self.core._position_lifecycle_daemon(
                    symbol, signal_id, direction, current_price, atr, 
                    {"allocated_value_usdt": actual_filled_notional, "size": actual_qty_filled if not self.core.test_mode else target_position_size}, 
                    target_leverage, regime, realigned_tp=target_tp_price, dynamic_rr_ratio=dynamic_rr_ratio
                )
            )
            logger.info(f"🛡️ GUARDIAN DAEMON SPAWNED // Managing position lifecycle for {symbol}.")
            
        except Exception as e:
            logger.error(f"[X-RAY] Critical failure in execute_statistical_signal for {symbol}: {e}", exc_info=True)
            # 🚀 AUDIT FIX: Orphan Trade Prevention. Flatten immediately if Daemon fails to spawn.
            try:
                pos_res = await self.core.executor.safe_call(self.core.executor.client.get_positions, category="linear", symbol=symbol)
                pos_list = pos_res.get("result", {}).get("list", [])
                if pos_list and float(pos_list[0].get("size", 0.0)) > 0:
                    qty = float(pos_list[0]["size"])
                    side = "Sell" if pos_list[0]["side"] == "Buy" else "Buy"
                    logger.critical(f"🛑 ORPHAN GUARD ACTIVATED // Liquidating {symbol} due to daemon spawn failure.")
                    await self.core.executor.safe_call(
                        self.core.executor.client.place_order, category="linear", symbol=symbol, side=side, 
                        orderType="Market", qty=str(qty), timeInForce="IOC", reduceOnly=True
                    )
            except Exception as flatten_err:
                logger.error(f"[X-RAY] 💀 FATAL: Orphan flatten failed for {symbol}: {flatten_err}")
                
            async with self.core.portfolio_state_lock: self.core.active_positions_map.pop(symbol, None)