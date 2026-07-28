"""
V50.0 QUANTUM SWARM: Capital Auction & Dispatch Engine
------------------------------------------------------
This module acts as the "Sniper" of the architecture. It polls the highly-concurrent 
auction heap, evaluates X-Ray diagnostics, handles Nano-Core sizing for micro-accounts,
and safely dispatches asynchronous orders to the Smart Order Router without blocking 
the main L2 WebSocket ingestion threads.
"""

import time
import uuid
import heapq
import random
import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any

logger = logging.getLogger("QUANT_CORE.AUCTION_ENGINE")

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
        logger.info("🏛️ GLOBAL CAPITAL AUCTION ENGINE ONLINE: Processing Priority Matrix & X-Ray Diagnostics.")
        
        while True:
            await asyncio.sleep(0.5) 
            
            async with self.core.portfolio_state_lock:
                if len(self.core.active_positions_map) >= 5:
                    continue # Max portfolio capacity reached

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
                    _, _, sym, payload = item
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
            
            top_neg_sharpe, _, top_symbol, top_payload = best_candidate
            top_sharpe = -top_neg_sharpe

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

                # Lock the asset to prevent duplicate concurrent triggers
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
        V50.0 Execution Engine. Handles Nano-Core sizing, safety validations, X-Ray telemetry,
        and dispatching to the Smart Order Router.
        """
        try:
            # Duplicate Daemon Check
            if symbol in self.core.daemon_tasks and not self.core.daemon_tasks[symbol].done():
                logger.warning(f"[X-RAY] 🚫 Lifecycle daemon already active for {symbol}. Aborting duplicate.")
                async with self.core.portfolio_state_lock: self.core.active_positions_map.pop(symbol, None)
                return

            signal_id = str(uuid.uuid4())
            sl_atr_mult = self.core.live_params.get("sl_atr_mult", 1.5)
            
            # 🚀 BUG FIX: Widen Stop-Loss structural floor to survive noise (0.80% -> 1.80%)
            sl_distance = max(atr * sl_atr_mult, current_price * 0.018)
            # Take-Profit distance scaled accordingly to maintain R:R
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

            # Fetch Accurate Balance
            try: 
                available_balance = await self.core.executor.get_wallet_balance_usdt()
            except Exception as e: 
                logger.debug(f"[X-RAY] Wallet fetch failed before execution: {e}", exc_info=True)
                available_balance = 0.0

            # X-RAY: Hard Balance Floor
            if available_balance < 5.0: 
                logger.warning(f"[X-RAY] 🚫 MARGIN EXHAUSTED // Skipping {symbol}: Available margin ({available_balance:.2f} USDT) too low for Bybit minimums.")
                async with self.core.portfolio_state_lock: self.core.active_positions_map.pop(symbol, None)
                return
                
            # 🚀 V50.0 NANO-CORE MICRO-SIZER
            if available_balance < 50.0:
                logger.info(f"[NANO-CORE] 🔬 Micro-Account Detected (${available_balance:.2f}). Scaling to Absolute Minimum Notional.")
                target_position_size = 6.00 / (current_price + 1e-9) # Fixed ~$6 minimum Bybit contract size
                fractional_risk = 0.05
                target_leverage = 5
            else:
                fractional_risk = self.core.risk_vault.calculate_optimal_fraction(confidence, net_edge_bps=edge_bps)
                dollar_risk = available_balance * fractional_risk
                target_position_size = max(dollar_risk / sl_distance, 6.00 / (current_price + 1e-9))
                target_leverage = self.core.risk_vault.calculate_dynamic_leverage(target_position_size * current_price, available_balance, sl_distance_pct=(sl_distance / current_price))
                
            target_notional = target_position_size * current_price

            # X-RAY: Portfolio Safety Validation
            if available_balance >= 50.0 and not self.core.risk_vault.evaluate_portfolio_safety(available_balance, target_notional, symbol): 
                logger.warning(f"[X-RAY] 🚫 PORTFOLIO SAFETY VIOLATION // Risk Vault rejected {symbol} to prevent over-exposure.")
                async with self.core.portfolio_state_lock: self.core.active_positions_map.pop(symbol, None)
                return

            logger.info(f"[X-RAY] 🎯 PRE-TRADE DISPATCH // {direction} {symbol} | Notional: ${target_notional:.2f} | Lev: {target_leverage}x | Risk: {fractional_risk:.2%}")

            # Paper Trading Bypass
            if self.core.test_mode:
                execution_success = random.random() < 0.85
                actual_filled_notional = target_notional
            else:
                try:
                    await self.core.executor.adjust_leverage(symbol, target_leverage)
                    await asyncio.sleep(0.2) 
                except Exception as e: 
                    logger.debug(f"[X-RAY] Leverage adjust note for {symbol}: {e}")

                feature_engine = self.core.feature_engines.get(symbol)
                current_depth = feature_engine.get_orderbook_snapshot() if feature_engine and hasattr(feature_engine, 'get_orderbook_snapshot') else {"bids": [[current_price, 1]], "asks": [[current_price, 1]]}

                try:
                    # 🚀 SOR DISPATCH: Switch between Aggressive Taker Block and Passive Maker Bracket
                    if regime in ["TRENDING_BULL", "TRENDING_BEAR", "TRENDING"]:
                        res = await self.core.sor.execute_iceberg_block(symbol=symbol, direction=direction, total_qty=target_position_size, current_mid_price=current_price, stop_loss=initial_sl_price, take_profit=target_tp_price, depth_snapshot=current_depth, vol_z=vol_z, vol_mult=vol_mult, feature_engine=feature_engine)
                    else:
                        res = await self.core.sor.execute_mean_reversion_bracket(symbol=symbol, direction=direction, total_qty=target_position_size, current_mid_price=current_price, stop_loss=initial_sl_price, take_profit=target_tp_price, depth_snapshot=current_depth, vol_z=vol_z, vol_mult=vol_mult, feature_engine=feature_engine, elasticity=elasticity)
                    
                    execution_success = res[0] if isinstance(res, tuple) else bool(res)
                
                except Exception as ex:
                    err_str = str(ex)
                    if any(code in err_str for code in ["10004", "10016", "10002", "500"]):
                        logger.critical(f"🚨 BYBIT SYSTEM MAINTENANCE DETECTED ({err_str}). Tripping 180s System Pause.")
                        async with self.core.circuit_breaker_lock:
                            self.core.circuit_breakers["GLOBAL_MAINTENANCE"] = time.time() + 180.0
                    elif "110007" in err_str or "not enough" in err_str or "10001" in err_str:
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
                    symbol, direction, current_price, actual_qty_filled, 
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
            async with self.core.portfolio_state_lock: self.core.active_positions_map.pop(symbol, None)