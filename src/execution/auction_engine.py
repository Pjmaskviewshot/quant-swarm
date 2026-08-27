"""
💎 V22.0 APEX TITANIUM OMEGA: DYNAMIC EV AUCTION & ROUTING ENGINE
-----------------------------------------------------------------
Fuses Multi-Scale Probabilities, Order Book Convexity, and Intelligent
Exits into an execution pipeline with Zero-Discard Stop Compression.

Audit Fixes (V22.0):
- Atomic Reservation Locks (Eradicates Double-Exposure Race Condition)
- Precision Passthrough (Eradicates 10001 Step Size Order Rejections)
- Calibrated Probability Routing (Protects Kelly Criterion Math)
"""

import time
import uuid
import heapq
import asyncio
import math
import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any

logger = logging.getLogger("QUANT_CORE.AUCTION_ENGINE")

class BybitRetCode:
    SUCCESS = 0
    PARAMETER_ERROR = 10002          
    SYSTEM_MAINTENANCE = 10004       
    RATE_LIMIT_REACHED = 10006       
    QTY_OUT_OF_BOUNDS = 10001        
    SERVICE_UNAVAILABLE = 10016      
    ORDER_NOT_EXISTS = 110001        
    INSUFFICIENT_BALANCE = 110007    
    RISK_LIMIT_EXCEEDED = 110013     
    LEVERAGE_NOT_MODIFIED = 110025   
    LEVERAGE_NOT_MODIFIED_2 = 110043 


class CapitalAuctionEngine:
    def __init__(self, core_engine):
        self.core = core_engine
        self._heap_counter = 0
        self._last_rejection_log = {}

    def get_next_heap_id(self) -> int:
        self._heap_counter += 1
        return self._heap_counter

    def _throttled_reject_log(self, symbol: str, reason: str):
        """Prevents the terminal from being spammed by the same rejection reason."""
        now = time.time()
        log_key = f"{symbol}_{reason}"
        if now - self._last_rejection_log.get(log_key, 0.0) > 15.0:
            logger.warning(f"[X-RAY] 🚫 AUCTION REJECT // {symbol}: {reason}")
            self._last_rejection_log[log_key] = now

    async def run_global_capital_auction_worker(self):
        logger.info("🏛️ V22.0 TITANIUM DYNAMIC EV AUCTION ENGINE ONLINE.")
        
        while True:
            await asyncio.sleep(0.4) 
            
            async with self.core.portfolio_state_lock:
                if len(self.core.active_positions_map) >= 5:
                    continue 

            best_candidate = None
            
            async with self.core.auction_lock:
                if not self.core.auction_queue:
                    continue
                    
                if len(self.core.auction_queue) > 1000:
                    self.core.auction_queue = heapq.nsmallest(500, self.core.auction_queue) 
                    heapq.heapify(self.core.auction_queue)
                    
                valid_candidates = []
                now = time.time()
                
                while self.core.auction_queue:
                    item = heapq.heappop(self.core.auction_queue)
                    _, _, sym, _, payload = item
                    
                    if hasattr(self.core, 'auction_queue_symbols'):
                        self.core.auction_queue_symbols.discard(sym)
                    
                    if now - payload["timestamp"] > 3.0: 
                        continue
                        
                    if self.core.fsm.is_asset_locked(sym):
                        continue
                        
                    try:
                        sector_state = self.core.fsm.get_sector_state(sym)
                        impulse = sector_state.get("impulse_score", 0.0)
                    except Exception:
                        impulse = 0.0
                    
                    if payload["action"] == "BUY" and impulse < -0.40:
                        continue
                    if payload["action"] == "SELL" and impulse > 0.40:
                        continue
                        
                    if payload.get("prob_success", 0.0) < 0.515:
                        continue

                    valid_candidates.append(item)
                        
                if not valid_candidates: 
                    continue
                    
                best_candidate = valid_candidates[0]
                
                for i in range(1, len(valid_candidates)):
                    heapq.heappush(self.core.auction_queue, valid_candidates[i])
                    if hasattr(self.core, 'auction_queue_symbols'):
                        self.core.auction_queue_symbols.add(valid_candidates[i][2])

            if not best_candidate: 
                continue
            
            top_neg_sharpe, _, top_symbol, _, top_payload = best_candidate
            top_sharpe = -top_neg_sharpe

            # 🚀 V22.0 AUDIT FIX: Atomic Reservation
            # Lock the portfolio state and claim the in-flight ticket instantly.
            # Eradicates the double-exposure race condition.
            async with self.core.portfolio_state_lock:
                if top_symbol in self.core.active_positions_map or top_symbol in self.core.in_flight_symbols:
                    continue
                self.core.in_flight_symbols[top_symbol] = time.time() + 60.0

            try:
                try:
                    raw_bal = await self.core.executor.get_wallet_balance_usdt()
                    current_bal = max(1.0, raw_bal)
                except Exception:
                    current_bal = 10.0

                current_ob = self.core.orderbook_snapshots.get(top_symbol)
                if current_ob and current_ob.get("best_bid", 0) > 0:
                    stat_engine = self.core.stat_engines.get(top_symbol)
                    if stat_engine and getattr(stat_engine, 'true_micro_price', 0.0) > 0:
                        live_price = stat_engine.true_micro_price
                    else:
                        live_price = (current_ob["best_bid"] + current_ob["best_ask"]) / 2.0
                        
                    signal_price = top_payload["price"]
                    action = top_payload["action"]
                    
                    if action == "BUY":
                        drift_pct = (live_price - signal_price) / signal_price
                    else:
                        drift_pct = (signal_price - live_price) / signal_price
                    
                    if drift_pct > 0.0150 or drift_pct < -0.0150: 
                        self._throttled_reject_log(top_symbol, f"Micro-Price drifted {drift_pct*10000:.1f} bps from signal.")
                        self.core.in_flight_symbols.pop(top_symbol, None)
                        continue

                # 🚀 V22.0 AUDIT FIX: Calibrated Kelly Call
                stat_engine = self.core.stat_engines.get(top_symbol)
                inst_var = getattr(stat_engine, 'inst_variance', 1e-4) if stat_engine else 1e-4

                fractional_risk = self.core.risk_vault.calculate_calibrated_kelly_fraction(
                    model_confidence=top_payload["prob_success"], 
                    net_edge_bps=top_payload["net_edge_bps"], 
                    current_balance=current_bal,
                    symbol_variance=inst_var
                )

                if fractional_risk <= 0.0:
                    self._throttled_reject_log(top_symbol, "Calibrated EV/Kelly Sizing returned 0.0")
                    self.core.in_flight_symbols.pop(top_symbol, None)
                    continue

                sl_atr_mult = max(2.0, self.core.live_params.get("sl_atr_mult", 2.5))
                sl_distance = max(top_payload["atr"] * sl_atr_mult, signal_price * 0.020) 
                sl_distance_pct = sl_distance / signal_price

                max_leverage_cap = 5.0
                max_permitted_notional = current_bal * max_leverage_cap
                
                target_risk_dollars = current_bal * fractional_risk
                target_risk_dollars = min(target_risk_dollars, current_bal * 0.05)
                
                exchange_min_notional = 6.50
                raw_notional = target_risk_dollars / sl_distance_pct if target_risk_dollars > 0.0 else exchange_min_notional
                exact_target_notional = max(exchange_min_notional, min(raw_notional, max_permitted_notional))

                is_safe, risk_reason = self.core.risk_vault.evaluate_portfolio_safety(
                    current_balance=current_bal,
                    new_position_notional=exact_target_notional,
                    symbol=top_symbol
                )

                if not is_safe:
                    self._throttled_reject_log(top_symbol, f"Risk Vault Veto: {risk_reason}")
                    self.core.in_flight_symbols.pop(top_symbol, None)
                    continue
            
                self.core.state_actor.dispatch(top_symbol, "RESERVE_IN_FLIGHT", {})
                
                logger.critical(
                    f"🏛️ AUCTION WINNER // {top_symbol} [{top_payload['regime']}] | "
                    f"{top_payload['action']} | Net Sharpe: {top_sharpe:.2f} | "
                    f"Prob: {top_payload['prob_success']:.2%} | Net Edge: {top_payload['net_edge_bps']:.1f} bps"
                )
                
                self.core.track_task(self.execute_statistical_signal(
                    top_payload["symbol"], top_payload["action"], top_payload["price"], 
                    top_payload["prob_success"], top_payload["dna_stats"], top_payload["atr"], 
                    top_payload["regime"], top_payload["net_edge_bps"], top_payload["vol_z"], top_payload["vol_mult"],
                    exact_target_notional, fractional_risk, sl_distance, sl_distance_pct,
                    top_payload.get("payload_features"), top_payload.get("elasticity"),
                    top_payload.get("dynamic_rr", self.core.live_params.get("rr_ratio", 2.0))
                ))

            except Exception as e:
                self.core.in_flight_symbols.pop(top_symbol, None)
                logger.error(f"[X-RAY] Auction evaluation fault for {top_symbol}: {e}", exc_info=True)


    async def execute_statistical_signal(
        self, 
        symbol: str, 
        direction: str, 
        current_price: float, 
        confidence: float, 
        dna_stats: dict, 
        atr: float, 
        regime: str, 
        edge_bps: float, 
        vol_z: float, 
        vol_mult: float, 
        target_notional: float,
        fractional_risk: float,
        sl_distance: float,
        sl_distance_pct: float,
        payload_features: dict = None, 
        elasticity: Any = None, 
        dynamic_rr_ratio: float = 2.0
    ):
        try:
            if symbol in self.core.daemon_tasks and not self.core.daemon_tasks[symbol].done():
                self.core.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})
                return

            signal_id = str(uuid.uuid4())
            
            await self.core.sor._fetch_exchange_limits(symbol)
            limits = self.core.sor.instrument_cache.get(symbol, {"min_qty": 1.0, "qty_step": 1.0})
            
            min_qty = limits["min_qty"]
            qty_step = limits["qty_step"]
            
            exchange_min_notional = max(6.50, min_qty * current_price)

            safe_qty = target_notional / current_price
            
            # Use strict qty_step for calculation, never min_qty.
            stepped_qty = math.floor(safe_qty / qty_step) * qty_step

            if (stepped_qty * current_price) < exchange_min_notional:
                stepped_qty = math.ceil(exchange_min_notional / (current_price * qty_step)) * qty_step
                
            target_position_size = max(min_qty, stepped_qty)
            
            actual_target_notional = target_position_size * current_price
            trade_risk_dollars = actual_target_notional * sl_distance_pct

            safe_max_lev = math.floor(1.0 / (sl_distance_pct * 1.5))
            target_leverage = int(max(1, min(5, safe_max_lev)))

            tp_distance = sl_distance * dynamic_rr_ratio 
            tick_dec = Decimal(str(self.core.tick_sizes.get(symbol, 0.0001)))
            def align_price(p: float) -> str: return str(Decimal(str(p)).quantize(tick_dec, rounding=ROUND_HALF_UP))
            
            raw_sl = (current_price - sl_distance) if direction == "BUY" else (current_price + sl_distance)
            raw_tp = (current_price + tp_distance) if direction == "BUY" else (current_price - tp_distance)
                
            initial_sl_price = float(align_price(raw_sl))
            target_tp_price = float(align_price(raw_tp))

            logger.info(f"[X-RAY] 🌉 HYBRID EV EXECUTION // {direction} {symbol} | Notional: ${actual_target_notional:.2f} | Lev: {target_leverage}x | Risk: ${trade_risk_dollars:.2f}")

            feature_engine = self.core.feature_engines.get(symbol)
            current_depth = feature_engine.get_orderbook_snapshot() if feature_engine and hasattr(feature_engine, 'get_orderbook_snapshot') else {"bids": [[current_price, 1]], "asks": [[current_price, 1]]}

            if not self.core.test_mode:
                try:
                    await self.core.executor.adjust_leverage(symbol, target_leverage)
                    await asyncio.sleep(0.1) 
                except Exception: 
                    pass

                res = await self.core.sor.execute_mean_reversion_bracket(
                    symbol=symbol, direction=direction, total_qty=target_position_size, 
                    current_mid_price=current_price, stop_loss=initial_sl_price, 
                    take_profit=target_tp_price, depth_snapshot=current_depth, 
                    vol_z=vol_z, vol_mult=vol_mult, feature_engine=feature_engine, 
                    elasticity=elasticity
                )
                
                if isinstance(res, tuple) and len(res) >= 4:
                    execution_success = bool(res[0])
                    arrival_price = float(res[1])
                    fill_price = float(res[2])
                    actual_qty_filled = float(res[3])
                elif isinstance(res, tuple) and len(res) >= 2:
                    execution_success = bool(res[0])
                    arrival_price = float(res[1])
                    fill_price = current_price
                    actual_qty_filled = target_position_size if execution_success else 0.0
                else:
                    execution_success = bool(res) if not isinstance(res, tuple) else bool(res[0])
                    arrival_price = current_price
                    fill_price = current_price
                    actual_qty_filled = target_position_size if execution_success else 0.0
            else:
                execution_success = True
                arrival_price = current_price
                fill_price = current_price
                actual_qty_filled = target_position_size

            if not execution_success or actual_qty_filled <= 0: 
                self.core.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})
                return 
                
            actual_filled_notional = actual_qty_filled * (fill_price if fill_price > 0 else current_price)
                    
            safe_features = payload_features if payload_features else {"symbol": symbol, "market_regime": regime}
            await self.core.memory.commit_prediction(signal_id, time.time(), current_price, direction, confidence, safe_features, False)
            
            ticket_msg = self.core.telegram.format_entry_ticket(
                symbol, direction, current_price, actual_qty_filled, 
                edge_bps, fractional_risk, regime, safe_features
            )
            self.core.track_task(self.core._safe_telegram_dispatch(ticket_msg, is_html=True))
            
            self.core.daemon_tasks[symbol] = self.core.track_task(
                self.core._position_lifecycle_daemon(
                    symbol, signal_id, direction, current_price, atr, 
                    {
                        "allocated_value_usdt": actual_filled_notional, 
                        "size": actual_qty_filled,
                        "arrival_price": arrival_price,
                        "qty_step": qty_step  # 🚀 V22.0 AUDIT FIX: Explicitly pass step to prevent 10001 reject
                    }, 
                    target_leverage, regime, realigned_tp=target_tp_price, dynamic_rr_ratio=dynamic_rr_ratio,
                    realigned_sl=initial_sl_price
                )
            )
            logger.info(f"🛡️ GUARDIAN DAEMON SPAWNED // Managing position lifecycle for {symbol}.")
            
        except Exception as e:
            logger.error(f"[X-RAY] Critical failure in execution for {symbol}: {e}", exc_info=True)
            self.core.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})