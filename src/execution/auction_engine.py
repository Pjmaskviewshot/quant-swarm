"""
💎 V17.1 APEX TITANIUM OMEGA: DYNAMIC EV AUCTION & ROUTING ENGINE
-----------------------------------------------------------------
Fuses Multi-Scale Probabilities, Order Book Convexity, and Intelligent
Exits into an execution pipeline with Zero-Discard Stop Compression.

Upgraded with V17.1 Continuous Kelly Integration, Destructive Stop 
Compression Eradication, True Implementation Shortfall (IS) Tracking, 
and Atomic StateActor Disruptor Queue mutations.
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
        logger.info("🏛️ V17.1 TITANIUM DYNAMIC EV AUCTION ENGINE ONLINE.")
        
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
                        # 🚀 FIX: Use discard() to avoid raising KeyError on duplicate symbol pops
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
                        
                    # 🚀 V17.1 CONVICTION FLOOR
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

            try:
                raw_bal = await self.core.executor.get_wallet_balance_usdt()
                current_bal = max(1.0, raw_bal)
            except Exception:
                current_bal = 10.0

            # 🚀 AUDIT FIX: Lock state, evaluate risk, and immediately reserve capacity to prevent race conditions
            async with self.core.portfolio_state_lock:
                # Shield against race conditions checking both maps
                if top_symbol in self.core.active_positions_map or top_symbol in self.core.in_flight_symbols:
                    continue
                    
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
                        continue

                # Provisional reservation hold (Will be trued-up in the execution function)
                provisional_notional = max(6.50, current_bal * 0.35)
                is_safe, risk_reason = self.core.risk_vault.evaluate_portfolio_safety(
                    current_balance=current_bal,
                    new_position_notional=provisional_notional,
                    symbol=top_symbol
                )

                if not is_safe:
                    self._throttled_reject_log(top_symbol, f"Risk Vault Veto: {risk_reason}")
                    continue

                # 🛡️ INSTANT HEAT CAP RESERVATION (Prevents double-entry on identical tick)
                self.core.in_flight_symbols.add(top_symbol)
            
            # Delegate true ledger tracking to the lock-free State Actor
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
                top_payload.get("payload_features"), top_payload.get("elasticity"),
                top_payload.get("dynamic_rr", self.core.live_params.get("rr_ratio", 2.0))
            ))

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
        payload_features: dict = None, 
        elasticity: Any = None, 
        dynamic_rr_ratio: float = 2.0
    ):
        try:
            if confidence < 0.515:
                self.core.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})
                return

            if symbol in self.core.daemon_tasks and not self.core.daemon_tasks[symbol].done():
                self.core.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})
                return

            signal_id = str(uuid.uuid4())
            
            try: 
                raw_balance = await self.core.executor.get_wallet_balance_usdt()
                available_balance = max(1.0, raw_balance)
            except Exception: 
                available_balance = 12.0

            if available_balance < 3.0: 
                self.core.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})
                return

            sl_atr_mult = max(2.0, self.core.live_params.get("sl_atr_mult", 2.5))
            sl_distance = max(atr * sl_atr_mult, current_price * 0.020) 
            sl_distance_pct = sl_distance / current_price
            
            await self.core.sor._fetch_exchange_limits(symbol)
            limits = self.core.sor.instrument_cache.get(symbol, {"min_qty": 1.0, "qty_step": 1.0})
            min_qty = limits["min_qty"]
            qty_step = limits["qty_step"]
            
            exchange_min_notional = max(6.50, min_qty * current_price)

            # 🚀 AUDIT FIX: Supply instantaneous variance to the continuous Kelly formula
            stat_engine = self.core.stat_engines.get(symbol)
            inst_var = getattr(stat_engine, 'inst_variance', 1e-4) if stat_engine else 1e-4

            fractional_risk = self.core.risk_vault.calculate_optimal_fraction(
                base_confidence=confidence, 
                net_edge_bps=edge_bps, 
                current_balance=available_balance,
                symbol_variance=inst_var
            )

            if fractional_risk <= 0.0:
                logger.warning(f"[X-RAY] 🚫 EV REJECT // {symbol} EV is negative or Kelly returned 0.0.")
                self.core.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})
                return

            # 🚀 V17.1 AUDIT FIX: Eradicate destructive Stop Compression
            # Stop loss is ALWAYS derived from market physics (ATR), never artificially squeezed
            # Maintain viable position size while capping maximum margin leverage
            raw_notional = (available_balance * fractional_risk) / sl_distance_pct if fractional_risk > 0 else exchange_min_notional
            target_notional = max(exchange_min_notional, min(raw_notional, available_balance * 0.30))

            safe_qty = target_notional / current_price
            stepped_qty = math.floor(safe_qty / qty_step) * qty_step

            if (stepped_qty * current_price) < exchange_min_notional:
                stepped_qty = math.ceil(exchange_min_notional / (current_price * qty_step)) * qty_step
                
            target_position_size = max(min_qty, stepped_qty)
            target_notional = target_position_size * current_price
            trade_risk_dollars = target_notional * sl_distance_pct

            # Dynamic leverage derived safely from stop distance
            safe_max_lev = math.floor(1.0 / (sl_distance_pct * 1.5))
            target_leverage = int(max(1, min(5, safe_max_lev)))

            tp_distance = sl_distance * dynamic_rr_ratio 
            tick_dec = Decimal(str(self.core.tick_sizes.get(symbol, 0.0001)))
            def align_price(p: float) -> str: return str(Decimal(str(p)).quantize(tick_dec, rounding=ROUND_HALF_UP))
            
            raw_sl = (current_price - sl_distance) if direction == "BUY" else (current_price + sl_distance)
            raw_tp = (current_price + tp_distance) if direction == "BUY" else (current_price - tp_distance)
                
            initial_sl_price = float(align_price(raw_sl))
            target_tp_price = float(align_price(raw_tp))

            logger.info(f"[X-RAY] 🌉 HYBRID EV EXECUTION // {direction} {symbol} | Notional: ${target_notional:.2f} | Lev: {target_leverage}x | Risk: ${trade_risk_dollars:.2f}")

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
                
                # 🚀 FIX: Extract all 4 elements returned by SOR for True Implementation Shortfall
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

            # 🚀 FIX: Abort correctly if zero quantity was filled
            if not execution_success or actual_qty_filled <= 0: 
                self.core.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})
                return 
                
            actual_filled_notional = actual_qty_filled * (fill_price if fill_price > 0 else current_price)
                    
            safe_features = payload_features if payload_features else {"symbol": symbol, "market_regime": regime}
            self.core.log_to_wal_sync("prediction", [signal_id, time.time(), current_price, direction, confidence, safe_features, False])
            
            ticket_msg = self.core.telegram.format_entry_ticket(
                symbol, direction, current_price, actual_qty_filled, 
                edge_bps, fractional_risk, regime, safe_features
            )
            self.core.track_task(self.core._safe_telegram_dispatch(ticket_msg, is_html=True))
            
            # Spawn the Intelligent FSM Guardian WITH Arrival Price
            # Guardian Daemon handles REGISTER_POSITION mutation inside its initialization
            self.core.daemon_tasks[symbol] = self.core.track_task(
                self.core._position_lifecycle_daemon(
                    symbol, signal_id, direction, current_price, atr, 
                    {
                        "allocated_value_usdt": actual_filled_notional, 
                        "size": actual_qty_filled,
                        "arrival_price": arrival_price  
                    }, 
                    target_leverage, regime, realigned_tp=target_tp_price, dynamic_rr_ratio=dynamic_rr_ratio,
                    realigned_sl=initial_sl_price
                )
            )
            logger.info(f"🛡️ GUARDIAN DAEMON SPAWNED // Managing position lifecycle for {symbol}.")
            
        except Exception as e:
            logger.error(f"[X-RAY] Critical failure in execution for {symbol}: {e}", exc_info=True)
            self.core.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})