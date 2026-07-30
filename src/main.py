"""
🌌 V55.1 OMNI-STATE: QUANTUM MICRO-CORE ORCHESTRATOR
------------------------------------------------------
The Apex Execution Engine. Features Dynamic Micro-Universe Filtering,
Limit-Only Escapes, 3-Minute Immunity Windows, and 2.5x ATR Survival Armor.
"""

import os
import sys
import time
import math
import asyncio
import logging
import uuid
import json
import tempfile
import datetime
import heapq
import numpy as np
import aiosqlite  
from collections import deque
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Any
from dotenv import load_dotenv

# Core & Feature Modules
from core.fsm import SystemStateMachine
from core.memory import MemoryBank
from core.edge_gate import MicrostructureEdgeGate
from core.micro_elasticity import MicroElasticityEngine 
from features.adaptive_engine import AdaptiveFeatureEngine
from features.vpin_clock import VolumeSynchronizedClock
from features.omni_scanner import GlobalOmniScanner  
from features.micro_models import ContinuousMicrostructureEngine, AdaptiveSessionClock

# Execution & Risk
from execution.sor import SmartOrderRouter
from execution.auction_engine import CapitalAuctionEngine
from portfolio.risk_manager import InstitutionalRiskVault  

# External Connectors
from ingestion.multi_feed import HighVelocityMultiFeed
from services.bybit_v5 import BybitUnifiedExecutor
from services.telegram_ops import AsyncTelegramReporter
from services.data_feed import AsynchronousDataFeed
from services.tensor_oracle import CrossAssetTensorOracle

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(name)s] - [%(levelname)s] - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("QUANT_CORE.V55.1_OMNI_STATE")


class DistributedQuantEngine:
    def __init__(self):
        load_dotenv()
        self.test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
        
        if self.test_mode: logger.critical("⚠️ TEST MODE: Paper Trading Armed.")
        else: logger.critical("🌌 LIVE MODE: V55.1 OMNI-STATE ACTIVE (QUANTUM MICRO-CORE).")
        
        self.asset_basket: List[str] = []
        self.timeframe = os.getenv("TRADING_TIMEFRAME", "15")
        self.shadow_basket: List[str] = []
        
        self.db_semaphore = asyncio.Semaphore(5)
        self.eval_semaphore = asyncio.Semaphore(10)
        self.wal_db_path = "quant_swarm_wal.db"
        self.wal_batch_queue = []
        self.wal_lock = asyncio.Lock()  
        
        self.tick_error_counts: Dict[str, List[float]] = {}
        self.circuit_breakers: Dict[str, float] = {}
        self.circuit_breaker_lock = asyncio.Lock() 
        
        self.stream_restart_event = asyncio.Event()
        self.force_dna_refresh = asyncio.Event() 
        
        # 🚀 MICROSERVICES
        self.fsm = SystemStateMachine()
        self.memory = MemoryBank()
        self.risk_vault = InstitutionalRiskVault(max_drawdown_pct=0.25, max_single_position_risk_pct=0.015)
        self.tensor_oracle = CrossAssetTensorOracle()
        self.auction_engine = CapitalAuctionEngine(self)
        
        self.stat_engines: Dict[str, ContinuousMicrostructureEngine] = {} 
        self.vpin_clocks: Dict[str, VolumeSynchronizedClock] = {}
        self.feature_engines: Dict[str, AdaptiveFeatureEngine] = {}
        self.edge_gates: Dict[str, MicrostructureEdgeGate] = {}
        self.elasticity_engines: Dict[str, MicroElasticityEngine] = {} 
        
        self.screener_memory, self.screener_metrics, self.orderbook_snapshots, self.ram_dna_cache = {}, {}, {}, {}
        self.volatility_baseline: Dict[str, float] = {}
        self.portfolio_state_lock = asyncio.Lock()
        self.active_positions_map: Dict[str, str] = {}  
        
        self.symbol_locks, self.eval_semaphores, self.daemon_tasks, self.last_eval_time = {}, {}, {}, {}
        self._active_tasks = set()
        self.auction_queue: List[tuple] = []  
        self.auction_lock = asyncio.Lock()
        
        self.tick_sizes: Dict[str, float] = {}
        self.hardware_min_qty: Dict[str, float] = {} # V55.0 Hardware Limits Cache
        self.global_state_cache = {"last_updated": 0.0}
        self.live_params = self._load_live_params()
        self.last_socket_reconnect = 0.0 
        self.funding_rates, self.open_interests, self.spread_history = {}, {}, {}
        
        self.telegram = AsyncTelegramReporter(token=os.getenv("TELEGRAM_BOT_TOKEN"), chat_id=os.getenv("TELEGRAM_CHAT_ID"))
        self.executor = BybitUnifiedExecutor(api_key=os.getenv("BYBIT_API_KEY"), api_secret=os.getenv("BYBIT_API_SECRET"), testnet=self.test_mode, max_workers=8)
        self.sor = SmartOrderRouter(executor=self.executor, max_slippage_pct=0.0012)
        self.omni_scanner = GlobalOmniScanner(self.executor)
        self.stream_feed_instance = None  

    def _get_vpin_bucket_size(self, symbol: str) -> float:
        if "BTC" in symbol: return 1_000_000.0
        if "ETH" in symbol: return 500_000.0
        if "SOL" in symbol: return 250_000.0
        return 100_000.0

    def _on_task_done(self, task):
        self._active_tasks.discard(task)
        if not task.cancelled() and task.exception():
            logger.error(f"[X-RAY] ❌ BACKGROUND TASK CRASHED: {task.exception()}", exc_info=task.exception())

    def track_task(self, coro: Any):
        task = asyncio.create_task(coro)
        self._active_tasks.add(task)
        task.add_done_callback(self._on_task_done)
        return task

    def _load_live_params(self) -> dict:
        default_params = {"sl_atr_mult": 2.5, "rr_ratio": 2.0}
        try:
            if os.path.exists("params.json"):
                with open("params.json", "r") as f: return {**default_params, **json.load(f)}
        except Exception: pass
        return default_params

    async def _prune_dead_symbols(self):
        async with self.portfolio_state_lock:
            active_set = set(self.asset_basket + self.shadow_basket + list(self.active_positions_map.keys()))
            for key in list(self.stat_engines.keys()):
                if key not in active_set:
                    self.stat_engines.pop(key, None)
                    self.vpin_clocks.pop(key, None)
                    self.feature_engines.pop(key, None)
                    self.edge_gates.pop(key, None)
                    self.elasticity_engines.pop(key, None)
                    self.spread_history.pop(key, None)
                    self.symbol_locks.pop(key, None)
                    self.eval_semaphores.pop(key, None)
                    self.orderbook_snapshots.pop(key, None)

    async def _save_sgd_state(self):
        state_snapshot = {}
        async with self.portfolio_state_lock:
            for sym, engine in self.stat_engines.items():
                state_snapshot[sym] = {
                    "weights_trending": engine.weights_trending.copy().tolist(), "weights_ranging": engine.weights_ranging.copy().tolist(),
                    "P_trending": engine.P_trending.copy().tolist(), "P_ranging": engine.P_ranging.copy().tolist(), "rls_updates": engine.rls_updates
                }
        def _write_file():
            try:
                target_path = "sgd_state.json"
                fd, path = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(target_path)) or ".")
                with os.fdopen(fd, 'w') as f: json.dump(state_snapshot, f)
                os.replace(path, target_path)
            except Exception as e: logger.debug(f"[X-RAY] Failed RLS disk serialization: {e}")
        await asyncio.to_thread(_write_file)

    def _load_sgd_state(self):
        try:
            if not os.path.exists("sgd_state.json"): return
            with open("sgd_state.json", "r") as f: state = json.load(f)
            for sym, data in state.items():
                if sym in self.stat_engines:
                    self.stat_engines[sym].weights_trending = np.array(data.get("weights_trending", self.stat_engines[sym].weights_trending))
                    self.stat_engines[sym].weights_ranging = np.array(data.get("weights_ranging", self.stat_engines[sym].weights_ranging))
                    self.stat_engines[sym].P_trending = np.array(data.get("P_trending", self.stat_engines[sym].P_trending))
                    self.stat_engines[sym].P_ranging = np.array(data.get("P_ranging", self.stat_engines[sym].P_ranging))
            logger.info("🧠 KALMAN RLS MEMORY LOADED: Recovered Covariance Tensors from disk.")
        except Exception as e: logger.debug(f"[X-RAY] Failed to load RLS state: {e}", exc_info=True)

    def _initialize_symbol_structures(self, symbols: List[str]):
        for s in symbols:
            if s not in self.stat_engines: self.stat_engines[s] = ContinuousMicrostructureEngine(symbol=s)
            if s not in self.vpin_clocks: self.vpin_clocks[s] = VolumeSynchronizedClock(bucket_volume=self._get_vpin_bucket_size(s), symbol=s)
            if s not in self.feature_engines: self.feature_engines[s] = AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600)
            if s not in self.edge_gates: self.edge_gates[s] = MicrostructureEdgeGate(window_size=100)
            if s not in self.elasticity_engines: self.elasticity_engines[s] = MicroElasticityEngine() 
            if s not in self.symbol_locks: self.symbol_locks[s] = asyncio.Lock()
            if s not in self.eval_semaphores: self.eval_semaphores[s] = asyncio.Semaphore(1)
            if s not in self.spread_history: self.spread_history[s] = deque(maxlen=50)
            if s not in self.screener_memory: self.screener_memory[s] = {"prices": deque(maxlen=1440), "highs": deque(maxlen=150), "lows": deque(maxlen=150), "volumes": deque(maxlen=1440), "last_update_time": 0.0}
            if s not in self.screener_metrics: self.screener_metrics[s] = {"vol_mult": 1.0}
            if s not in self.volatility_baseline: self.volatility_baseline[s] = 0.0
            if s not in self.ram_dna_cache: self.ram_dna_cache[s] = {"is_armed": True, "win_rate": 0.50}
            if s not in self.last_eval_time: self.last_eval_time[s] = 0.0 
            if s not in self.orderbook_snapshots: self.orderbook_snapshots[s] = {"best_bid": 0.0, "best_ask": 0.0}

    async def _safe_telegram_dispatch(self, message: str, is_html: bool = True, message_type: str = "SUCCESS"):
        if not os.getenv("TELEGRAM_BOT_TOKEN") or len(os.getenv("TELEGRAM_BOT_TOKEN", "")) < 5: return
        if hasattr(self, '_telegram_blocked_until') and time.time() < self._telegram_blocked_until: return
        for attempt in range(2):
            try:
                if is_html: await self.telegram.send_html_report(message)
                else: await self.telegram.log_message(message, message_type)
                return
            except Exception: await asyncio.sleep(2 ** attempt)
        self._telegram_blocked_until = time.time() + 3600
        logger.warning("[X-RAY] ⚠️ Telegram unreachable. Disabling telemetry dispatcher temporarily for 1 hour.")

    async def _fetch_exchange_tick_sizes(self):
        """V55.0 Maps both tick sizes and minimum hardware order quantities for dynamic filtering"""
        try:
            info = await self.executor.safe_call(self.executor.client.get_instruments_info, category="linear")
            for item in info.get("result", {}).get("list", []):
                sym = item.get("symbol")
                self.tick_sizes[sym] = float(item.get("priceFilter", {}).get("tickSize", "0.0001"))
                self.hardware_min_qty[sym] = float(item.get("lotSizeFilter", {}).get("minOrderQty", "1.0"))
        except Exception as e: logger.error(f"[X-RAY] Failed fetching exchange info: {e}", exc_info=True)

    async def _get_max_affordable_notional(self):
        """Calculates the absolute maximum notional value the wallet can support using Continuous Logistics"""
        try: 
            available_balance = await self.executor.get_wallet_balance_usdt()
        except Exception: 
            available_balance = 7.00
        min_exposure_ratio = 0.40 
        max_exposure_ratio = 0.10
        k_slope = 0.05
        mid_point = 100.0
        logistic_exp = min_exposure_ratio + (max_exposure_ratio - min_exposure_ratio) / (1.0 + math.exp(-k_slope * (available_balance - mid_point)))
        return max(5.00, available_balance * logistic_exp)

    async def synchronize_exchange_state(self):
        try:
            pos_response = await self.executor.safe_call(self.executor.client.get_positions, category="linear", settleCoin="USDT")
            active_orphans = [p for p in pos_response.get("result", {}).get("list", []) if float(p.get("size", 0.0)) > 0]
            if not active_orphans: return
            logger.critical(f"⚠️ RECOVERY ENGAGED: Found {len(active_orphans)} active trades left open.")
            
            for pos in active_orphans:
                symbol = pos["symbol"]
                self._initialize_symbol_structures([symbol]) 
                qty, entry_price = float(pos["size"]), float(pos["avgPrice"])
                direction = "BUY" if pos["side"].upper() == "BUY" else "SELL"
                atr = entry_price * 0.015 
                
                async with self.portfolio_state_lock: self.active_positions_map[symbol] = direction
                risk_matrix = {"allocated_value_usdt": qty * entry_price, "size": qty, "recommended_leverage": 8}
                self.daemon_tasks[symbol] = self.track_task(self._position_lifecycle_daemon(symbol, str(uuid.uuid4()), direction, entry_price, atr, risk_matrix, 3, "RANGING", is_recovery=True))
        except Exception as e: logger.error(f"[X-RAY] Failed synchronizing exchange state: {e}", exc_info=True)

    async def cleanup_stale_locks(self):
        while True:
            await asyncio.sleep(300) 
            try:
                symbols_to_check = []
                async with self.portfolio_state_lock: symbols_to_check = list(self.active_positions_map.keys())
                for symbol in symbols_to_check:
                    if (active_daemon := self.daemon_tasks.get(symbol)) and not active_daemon.done(): continue 
                    if not hasattr(self.risk_vault, 'active_positions') or symbol not in self.risk_vault.active_positions:
                        pos_response = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                        if pos_response.get("retCode") == 0 and not any(float(p.get("size", 0.0)) > 0 for p in pos_response.get("result", {}).get("list", [])):
                            async with self.portfolio_state_lock: self.active_positions_map.pop(symbol, None)
            except Exception as e: logger.error(f"[X-RAY] Failed stale lock cleanup: {e}", exc_info=True)

    async def run_crowded_trade_oracle(self):
        logger.info("🔮 CROWDED-TRADE ORACLE ONLINE: Polling Derivatives Tickers.")
        while True:
            try:
                tickers_res = await self.executor.safe_call(self.executor.client.get_tickers, category="linear")
                if tickers_res.get("retCode") == 0:
                    for t in tickers_res.get("result", {}).get("list", []):
                        sym = t.get("symbol")
                        if sym in self.asset_basket or sym in self.shadow_basket:
                            self.funding_rates[sym] = float(t.get("fundingRate", 0.0) or 0.0)
                            self.open_interests[sym] = float(t.get("openInterestValue", 0.0) or 0.0)
            except Exception as e: logger.debug(f"[X-RAY] Crowded-Trade Oracle fault: {e}", exc_info=True)
            await asyncio.sleep(120) 

    async def run_system_heartbeat(self):
        start_time = time.time()
        loop_counter = 0
        while True:
            await asyncio.sleep(60) 
            loop_counter += 1
            uptime_hours = (time.time() - start_time) / 3600

            if loop_counter % 5 == 0:
                self.global_state_cache["last_updated"] = time.time()
                await self._save_sgd_state()
                try: 
                    current_vault_balance = await self.executor.get_wallet_balance_usdt()
                except Exception as e: 
                    logger.debug(f"[X-RAY] Heartbeat balance fetch failed: {e}", exc_info=True)
                    continue

                if "wallet_baseline" not in self.global_state_cache: self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01)
                
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                current_day = now_utc.strftime("%Y-%m-%d")
                if self.global_state_cache.get("current_day") != current_day:
                    self.global_state_cache["current_day"] = current_day
                    self.global_state_cache["start_of_day_balance"] = current_vault_balance
                    self.fsm.release_global_emergency_lock()

                today_start_iso = now_utc.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                try:
                    def _fetch(): return self.memory.get_forensic_execution_summary(today_start_iso)
                    execution_stats = await asyncio.wait_for(asyncio.to_thread(_fetch), timeout=5.0)
                except Exception as e: 
                    logger.debug(f"[X-RAY] Heartbeat DB forensic fetch failed: {e}", exc_info=True)
                    execution_stats = {} 

                actual_net_pnl = current_vault_balance - self.global_state_cache.get("start_of_day_balance", current_vault_balance)
                baseline = self.global_state_cache["wallet_baseline"]
                if current_vault_balance > baseline:
                    self.global_state_cache["wallet_baseline"] = current_vault_balance
                    baseline = current_vault_balance
                    
                drawdown_pct = max(0.0, (baseline - current_vault_balance) / baseline)
                
                if drawdown_pct >= 0.25:
                    self.fsm.trigger_global_emergency_lock()
                    await self._safe_telegram_dispatch(f"🚨 <b>EMERGENCY DRAWDOWN BREAKER TRIPPED</b>\nDrawdown: {drawdown_pct:.2%}. Engine shutting down.", is_html=True)
                    await asyncio.sleep(2) 
                    await self.graceful_shutdown()
                    sys.exit(0)
                
                filled_blocks = min(10, int(drawdown_pct * 10))
                dd_bar = "🟢" * (10 - filled_blocks) + "🔴" * filled_blocks
                self.global_state_cache.update({"drawdown_bar": dd_bar, "actual_net_pnl": actual_net_pnl, "current_vault_balance": current_vault_balance, "drawdown_pct": drawdown_pct})

            if loop_counter % 10 == 0:
                cv = self.global_state_cache.get("current_vault_balance", 0.0)
                actual = self.global_state_cache.get("actual_net_pnl", 0.0)
                dd = self.global_state_cache.get("drawdown_pct", 0.0)
                dd_bar = self.global_state_cache.get("drawdown_bar", "🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢")
                
                live_count = len(self.asset_basket)
                shadow_count = len(self.shadow_basket)

                report = self.telegram.format_mission_control_dashboard(
                    uptime_hours, live_count, shadow_count, cv, actual, dd, dd_bar, execution_stats
                )
                self.track_task(self._safe_telegram_dispatch(report))

    async def handle_incoming_trade(self, trade_data: Dict[str, Any]):
        symbol = trade_data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return
        
        now = time.time()
        async with self.circuit_breaker_lock:
            if self.circuit_breakers.get(symbol, 0.0) > now or self.circuit_breakers.get("GLOBAL_MAINTENANCE", 0.0) > now: return
            
        if not self.fsm.can_execute_trades or (time.time() - self.last_socket_reconnect < 30.0): return

        try:
            price = float(trade_data.get("price", 0.0))
            if price < 0.000001: return
            
            volume, is_buy = float(trade_data.get("size", 0.0)), (str(trade_data.get("side", "")).upper() == "BUY")
            exchange_timestamp = float(trade_data.get("timestamp", now * 1000)) / 1000.0
            
            self.tensor_oracle.ingest_tick(symbol, price, exchange_timestamp) 
            if edge_gate := self.edge_gates.get(symbol): edge_gate.update_trade_flow(volume, is_buy)
            if feature_engine := self.feature_engines.get(symbol): feature_engine.push_trade_tick([trade_data])

            stat_engine = self.stat_engines.get(symbol)
            clock = self.vpin_clocks.get(symbol)
            if not stat_engine or not clock: return
            
            stat_engine.update_trades(price, volume, is_buy, exchange_timestamp)
            self.risk_vault.push_microstructure_variance(stat_engine.inst_variance)
            
            manifests = clock.process_tick(price, volume, not is_buy)
            valid_manifests = [m for m in manifests if m.get("valid")]
            vol_z = stat_engine.hawkes_z if stat_engine else 0.0
            
            if valid_manifests: 
                vpin_z = float(valid_manifests[-1].get("vpin_z_score", 0.0))
                stat_engine.vpin_z = vpin_z
            elif clock.vpin_history:
                hist = np.array(list(clock.vpin_history)[-200:])
                if len(hist) >= 20 and np.std(hist) > 0:
                    vpin_z = float((clock.vpin_history[-1] - np.mean(hist)) / (np.std(hist) + 1e-9))
                    vol_z = vpin_z 
                    stat_engine.vpin_z = vpin_z
                else: vpin_z = 0.0
            else: vpin_z = 0.0
        
            if now - self.last_eval_time.get(symbol, 0.0) < (0.2 if abs(vpin_z) > 1.5 else 1.0): return
            self.last_eval_time[symbol] = now
            
            ob = self.orderbook_snapshots.get(symbol)
            if not ob or "bid_size" not in ob: return
            spread_cost = abs(ob["best_ask"] - ob["best_bid"]) / (price + 1e-9) if price > 0 else 0.001
            vol_mult = self.screener_metrics.get(symbol, {}).get("vol_mult", 1.0)
            
            async with self.eval_semaphores[symbol]:
                raw_atr = feature_engine.get_computed_atr() if feature_engine and hasattr(feature_engine, 'get_computed_atr') else 0.0
                atr = raw_atr if raw_atr > 0 else price * 0.005
                
                sl_atr_mult = max(2.5, self.live_params.get("sl_atr_mult", 2.5))
                sl_dist_pct = max((atr * sl_atr_mult) / (price + 1e-9), 0.025)
                dynamic_rr_ratio = feature_engine.get_dynamic_rr_ratio() if feature_engine and hasattr(feature_engine, 'get_dynamic_rr_ratio') else self.live_params.get("rr_ratio", 2.0)
                tp_dist_pct = sl_dist_pct * dynamic_rr_ratio
                
                sgd_state = stat_engine.extract_statistical_state(price, vpin_z, self.tensor_oracle.compute_lead_lag_signal(symbol), sl_dist_pct, tp_dist_pct, exchange_timestamp)
                action, prob_success = sgd_state["action_dir"], max(sgd_state["p_up"], sgd_state["p_down"])
                
                structural_verdict = edge_gate.evaluate_structural_edge(symbol, vpin_z, intended_direction=action)
                if structural_verdict["action"] == "HOLD": return
                if structural_verdict["action"] != action:
                    action, prob_success = structural_verdict["action"], max(prob_success, 0.58) 
                
                regime = feature_engine.detect_market_regime() if feature_engine else "TRENDING"
                if spread_cost > 0.0004: 
                    structural_verdict["routing"] = "MAKER_ONLY"
                    regime = "MEAN_REVERTING"
                    
                routing_mode = structural_verdict.get("routing", "STANDARD")
                net_ev_pct = (prob_success * tp_dist_pct) - ((1.0 - prob_success) * sl_dist_pct) - (spread_cost if routing_mode != "MAKER_ONLY" else -spread_cost * 0.2) - (0.0002 if routing_mode == "MAKER_ONLY" else 0.0005)

                ev_floor = AdaptiveSessionClock.get_ev_floor(routing_mode)
                if net_ev_pct < ev_floor: 
                    if prob_success >= 0.55: logger.info(f"[X-RAY] ℹ️ NEAR-MISS // {symbol} | Prob: {prob_success:.1%} | Routing: {routing_mode} | Net EV: {net_ev_pct*10000:.1f} bps < Floor {ev_floor*10000:.1f} bps (Spread: {spread_cost*10000:.1f} bps)")
                    return
                    
                dynamic_gate = sgd_state.get("dynamic_gate", 0.50) - (0.08 if routing_mode == "MAKER_ONLY" else 0.0)

                async with self.portfolio_state_lock: dna_stats = self.ram_dna_cache.get(symbol, {"is_armed": True, "win_rate": 0.50})
                if prob_success < max(dynamic_gate, dna_stats.get("cluster_win_rate", dna_stats.get("win_rate", 0.50))): 
                    return

                funding_rate = self.funding_rates.get(symbol, 0.0)
                if funding_rate > 0.00025: prob_success += 0.05 if action == "SELL" else -0.05
                elif funding_rate < -0.00025: prob_success += 0.05 if action == "BUY" else -0.05
                    
                if feature_engine and hasattr(feature_engine, 'get_htf_trend_bias'):
                    prob_success += (feature_engine.get_htf_trend_bias(price) * 0.04) if action == "BUY" else -(feature_engine.get_htf_trend_bias(price) * 0.04)
                    
                prob_success = stat_engine.calibrate_confidence(prob_success, regime, stat_engine.ewma_mse)

                try:
                    payload = {
                        "symbol": symbol, "action": action, "price": price, 
                        "prob_success": prob_success, "dna_stats": dna_stats, 
                        "atr": atr, "regime": regime, "net_edge_bps": net_ev_pct * 10000.0, 
                        "vol_z": vol_z, "vol_mult": vol_mult, "timestamp": time.time(),
                        "payload_features": {"symbol": symbol, "market_regime": regime, "virtual_sl": sgd_state["virtual_sl"], "virtual_tp": sgd_state["virtual_tp"], "adaptive_obi_z": stat_engine.ofi_fast_z, "liquidity_density_ratio": vol_mult, "bid_ask_spread": spread_cost, "reasoning": structural_verdict.get("reasoning", "MICROSTRUCTURE_ALPHA"), "ai_verdict": "DIRECT_MICROSTRUCTURE_ALPHA"},
                        "elasticity": self.elasticity_engines.get(symbol),
                        "dynamic_rr": dynamic_rr_ratio 
                    }
                    async with self.auction_lock: heapq.heappush(self.auction_queue, (-(net_ev_pct / (sl_dist_pct + 1e-9)), time.time(), symbol, payload))
                except Exception as ex_payload: logger.error(f"[X-RAY] Failed to build auction payload for {symbol}: {ex_payload}", exc_info=True)
                
        except Exception as e: logger.error(f"[X-RAY] Trade processing fault for {symbol}: {e}", exc_info=True)

    async def log_to_wal_async(self, action_type: str, args: list):
        async with self.wal_lock:
            if len(self.wal_batch_queue) > 10000: self.wal_batch_queue.pop(0)
            self.wal_batch_queue.append((str(uuid.uuid4()), action_type, json.dumps(args), time.time()))

    def log_to_wal_sync(self, action_type: str, args: list):
        self.track_task(self.log_to_wal_async(action_type, args))

    async def _batch_wal_flush_loop(self):
        while True:
            await asyncio.sleep(5.0)
            async with self.wal_lock:
                if not self.wal_batch_queue: continue
                batch_to_process, self.wal_batch_queue = self.wal_batch_queue[:], []
            try:
                async with aiosqlite.connect(self.wal_db_path) as db:
                    await db.executemany("INSERT INTO pending_wal (id, action_type, payload, created_at) VALUES (?, ?, ?, ?)", batch_to_process)
                    await db.commit()
            except Exception as e:
                logger.error(f"[X-RAY] Failed SQLite WAL write: {e}", exc_info=True)
                async with self.wal_lock: self.wal_batch_queue = batch_to_process + self.wal_batch_queue

    async def run_db_wal_worker(self):
        logger.info("🖲️ SQLITE WAL ENGINE ONLINE: High-Throughput disk logging.")
        try:
            async with aiosqlite.connect(self.wal_db_path) as db:
                await db.execute("""CREATE TABLE IF NOT EXISTS pending_wal (id TEXT PRIMARY KEY, action_type TEXT, payload TEXT, created_at REAL)""")
                await db.commit()
        except Exception as e:
            logger.critical(f"FATAL: SQLite WAL Initialization Failed: {e}", exc_info=True)
            return

        while True:
            try:
                async with aiosqlite.connect(self.wal_db_path) as db:
                    await db.execute("DELETE FROM pending_wal WHERE id IN (SELECT id FROM pending_wal ORDER BY created_at ASC LIMIT -1 OFFSET 50000)")
                    await db.commit()
                async with aiosqlite.connect(self.wal_db_path) as db:
                    async with db.execute("SELECT id, action_type, payload FROM pending_wal ORDER BY created_at ASC LIMIT 100") as cursor:
                        rows = await cursor.fetchall()
                for item_id, action_type, payload_str in rows:
                    args = json.loads(payload_str)
                    try:
                        async with self.db_semaphore:
                            if action_type == "prediction": await asyncio.wait_for(asyncio.to_thread(self.memory.commit_prediction, *args), timeout=10.0)
                            elif action_type == "settlement": await asyncio.wait_for(asyncio.to_thread(self.memory.log_live_execution_result, *args), timeout=10.0)
                        async with aiosqlite.connect(self.wal_db_path) as db:
                            await db.execute("DELETE FROM pending_wal WHERE id = ?", (item_id,))
                            await db.commit()
                    except asyncio.TimeoutError: break 
                    except Exception as e: logger.error(f"[X-RAY] WAL processing error for item {item_id}: {e}", exc_info=True)
            except Exception as e: logger.error(f"[X-RAY] WAL Worker Loop Error: {e}", exc_info=True)
            await asyncio.sleep(1.0)

    async def run_dna_prewarmer(self):
        logger.info("🔥 RAM PRE-WARMER ONLINE: Pre-fetching database edge logic.")
        while True:
            try:
                await asyncio.wait_for(self.force_dna_refresh.wait(), timeout=300.0)
                self.force_dna_refresh.clear()
            except asyncio.TimeoutError: pass
            try:
                async def _safe_fetch(sym, dna):
                    try:
                        async with self.db_semaphore:
                            res = await asyncio.wait_for(asyncio.to_thread(self.memory.compute_latent_dna_edge, dna, 30), timeout=5.0)
                            if res and isinstance(res, dict): res["is_armed"] = True
                            return res
                    except Exception: return {"is_armed": True, "win_rate": 0.50} 

                fetch_tasks = {sym: _safe_fetch(sym, {"vol_mult": self.screener_metrics.get(sym, {}).get("vol_mult", 1.0), "z_obi": self.stat_engines.get(sym).ofi_fast_z if self.stat_engines.get(sym) else 0.0, "spread_pct": (self.orderbook_snapshots.get(sym, {}).get("best_ask", 1) - self.orderbook_snapshots.get(sym, {}).get("best_bid", 1)) / max(self.orderbook_snapshots.get(sym, {}).get("best_bid", 1), 1e-9), "symbol": sym}) for sym in list(self.asset_basket)}
                if not fetch_tasks: continue
                results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
                
                async with self.portfolio_state_lock:
                    for sym, result in zip(list(fetch_tasks.keys()), results):
                        if isinstance(result, Exception): self.ram_dna_cache[sym] = {"is_armed": True, "win_rate": 0.50}
                        else:
                            if isinstance(result, dict): result["is_armed"] = True 
                            self.ram_dna_cache[sym] = result
            except Exception as e: logger.error(f"[X-RAY] DNA Prewarmer error: {e}", exc_info=True)

    async def run_shadow_resolution_daemon(self):
        logger.info("👻 GHOST FORENSICS ONLINE: Vectorized resolution engine activated.")
        interval_mins = float(self.timeframe)
        while True:
            await asyncio.sleep(300) 
            try:
                current_prices = {sym: {"prices": list(self.screener_memory[sym]["prices"]), "highs": list(self.screener_memory[sym].get("highs", [])), "lows": list(self.screener_memory[sym].get("lows", []))} for sym in self.asset_basket + self.shadow_basket if self.screener_memory.get(sym) and self.screener_memory[sym].get("prices")}
                if current_prices:
                    async with self.db_semaphore:
                        try: await asyncio.wait_for(asyncio.to_thread(self.memory.resolve_batch_historical_predictions, list(current_prices.keys()), current_prices, 60.0, interval_mins), timeout=15.0)
                        except Exception as e: logger.debug(f"[X-RAY] Shadow resolution batch timeout: {e}", exc_info=True)
            except Exception as e: logger.error(f"[X-RAY] Shadow resolution daemon error: {e}", exc_info=True)

    async def handle_incoming_orderbook_tick(self, depth_data: Dict[str, Any]):
        symbol = depth_data.get("s")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return

        bids, asks = depth_data.get("b", []), depth_data.get("a", [])
        is_snapshot = depth_data.get("type") == "snapshot"
        
        feature_engine = self.feature_engines.get(symbol)
        if feature_engine:
            feature_engine.push_orderbook_tick(bids, asks, is_snapshot=is_snapshot)
            book_metrics = feature_engine.get_book_depth_metrics()
            if book_metrics and "top_bid" in book_metrics and "top_ask" in book_metrics:
                best_bid, best_ask = book_metrics["top_bid"], book_metrics["top_ask"]
                top_bid_size, top_ask_size = float(bids[0][1]) if bids else (book_metrics.get("bid_depth_10", 10.0) / 10.0), float(asks[0][1]) if asks else (book_metrics.get("ask_depth_10", 10.0) / 10.0)
                
                if best_bid > 0 and best_ask > best_bid:
                    self.orderbook_snapshots[symbol] = {"best_bid": best_bid, "bid_size": top_bid_size, "best_ask": best_ask, "ask_size": top_ask_size, "bids": bids, "asks": asks}
                    stat_engine = self.stat_engines.get(symbol)
                    now, spread_val = time.time(), (best_ask - best_bid) / (best_bid + 1e-9)
                    
                    if (spread_hist := self.spread_history.get(symbol)) is not None:
                        spread_hist.append(spread_val)
                        async with self.circuit_breaker_lock:
                            if len(spread_hist) >= 30 and self.circuit_breakers.get(symbol, 0.0) <= now:
                                med_spread = np.median(spread_hist)
                                if spread_val > med_spread * (4.0 if symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"] else 5.0) and spread_val > (0.0015 if symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"] else 0.0050):
                                    logger.warning(f"[X-RAY] ⚠️ LIQUIDITY FRACTURE // {symbol} Spread spiked to {spread_val*10000:.1f} bps. Tripping 60s Circuit Breaker.")
                                    self.circuit_breakers[symbol] = now + 60.0
                    
                    if stat_engine: 
                        stat_engine.update_orderbook_pressure(best_bid, top_bid_size, best_ask, top_ask_size)
                        if symbol == "BTCUSDT": self.global_btc_ofi_z = stat_engine.ofi_fast_z
                        
                    if (elasticity := self.elasticity_engines.get(symbol)) and stat_engine:
                        elasticity.update_depth_state(best_bid, top_bid_size, best_ask, top_ask_size, stat_engine.ofi_fast_z, float(depth_data.get("ts", time.time() * 1000)) / 1000.0)

            if (edge_gate := self.edge_gates.get(symbol)) and bids and asks:
                try:
                    f_bids, f_asks = feature_engine.get_deep_book_floats()
                    edge_gate.update_orderbook_state(symbol, f_bids, f_asks, (book_metrics["top_bid"] + book_metrics["top_ask"]) / 2.0)
                except Exception as e: logger.debug(f"[X-RAY] Edge gate update error for {symbol}: {e}")

    async def run_omni_swarm_director(self):
        logger.info("🌪️ OMNI-SWARM DIRECTOR ONLINE: Monitoring Global Vectors.")
        banned_keywords = ["SOXL", "SPCX", "SKHY", "SNDK", "BANK", "MUUSDT", "BEAT", "MSTR", "ESPUSDT", "DEXE", "PUMP", "EUL", "XAU", "XAG"]
        while True:
            await asyncio.sleep(15) 
            try:
                protected_symbols = set()
                async with self.portfolio_state_lock: protected_symbols = set(self.active_positions_map.keys())
                dead_sym, hot_sym = await self.omni_scanner.scan_and_rank_universe(self.asset_basket, protected_symbols=protected_symbols)
                
                if dead_sym and hot_sym and not any(b in hot_sym for b in banned_keywords):
                    tick_res = await self.executor.safe_call(self.executor.client.get_tickers, category="linear", symbol=hot_sym)
                    if tick_res.get("retCode") == 0 and tick_res.get("result", {}).get("list"):
                        t_data = tick_res["result"]["list"][0]
                        bid, ask, turnover = float(t_data.get("bid1Price", 0.0) or 0.0), float(t_data.get("ask1Price", 0.0) or 0.0), float(t_data.get("turnover24h", 0.0) or 0.0)
                        
                        if bid > 0 and ask > bid and turnover >= AdaptiveSessionClock.get_turnover_threshold() and (((ask - bid) / bid) * 10000.0) <= 12.0:
                            max_notional = await self._get_max_affordable_notional()
                            if (self.hardware_min_qty.get(hot_sym, 1.0) * bid) <= max_notional:
                                async with self.portfolio_state_lock:
                                    if dead_sym in self.asset_basket: self.asset_basket.remove(dead_sym)
                                    if hot_sym not in self.asset_basket: self.asset_basket.append(hot_sym)
                                self._initialize_symbol_structures([hot_sym])
                                await self._prune_dead_symbols() 
                                if self.stream_feed_instance and hasattr(self.stream_feed_instance, 'hot_swap_socket_stream'):
                                    await self.stream_feed_instance.hot_swap_socket_stream(dead_sym, hot_sym)
                                logger.critical(f"[X-RAY] 🚀 {hot_sym} PASSED DYNAMIC GATE AND INJECTED INTO QUANT MATRIX.")
            except Exception as e: logger.error(f"[X-RAY] Omni-Swarm Director iteration failed: {e}", exc_info=True)

    async def handle_incoming_kline_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return
        self._initialize_symbol_structures([symbol]) 
        interval, candle = str(data["interval"]), data["candle_data"]
        c_open, c_high, c_low, c_close, c_vol = map(float, [candle.get("open", 0), candle.get("high", 0), candle.get("low", 0), candle.get("close", 0), candle.get("volume", 0)])

        async with self.symbol_locks[symbol]:
            if feature_engine := self.feature_engines.get(symbol):
                feature_engine.update_multi_timeframe_candle(timeframe=interval, open_p=c_open, high_p=c_high, low_p=c_low, close_p=c_close, volume=c_vol)
                if str(interval) == str(self.timeframe) and symbol in self.screener_memory:
                    self.screener_memory[symbol].setdefault("highs", deque(maxlen=150)).append(c_high)
                    self.screener_memory[symbol].setdefault("lows", deque(maxlen=150)).append(c_low)
                    self.screener_memory[symbol].setdefault("prices", deque(maxlen=1440)).append(c_close)
                    self.screener_memory[symbol]["last_update_time"] = time.time()

    async def handle_incoming_basket_screener_update(self, data: Dict[str, Any]):
        if (symbol := data.get("symbol")) not in self.asset_basket and symbol not in self.shadow_basket: return
        try:
            if "turnover24h" in (raw_data := data.get("raw_data", {})):
                turnover = float(raw_data["turnover24h"])
                if symbol not in self.screener_metrics: self.screener_metrics[symbol] = {}
                baseline = self.volatility_baseline.get(symbol, turnover)
                if baseline > 0: self.screener_metrics[symbol]["vol_mult"] = min(10.0, max(0.1, turnover / baseline))
                self.volatility_baseline[symbol] = (baseline * 0.99) + (turnover * 0.01)
        except Exception as e: logger.debug(f"[X-RAY] Screener update parse failed for {symbol}: {e}")

    async def run_universe_refresher(self):
        try:
            logger.info("🌌 V55.1 MICRO-UNIVERSE SCAN: Probing exchange for affordable liquid nodes...")
            await self._fetch_exchange_tick_sizes()
            max_notional = await self._get_max_affordable_notional()
            
            tickers_res = await self.executor.safe_call(self.executor.client.get_tickers, category="linear")
            full_market, min_turnover = [], AdaptiveSessionClock.get_turnover_threshold()
            
            if tickers_res.get("retCode") == 0:
                for t in tickers_res.get("result", {}).get("list", []):
                    if not (symbol := t.get("symbol", "")).endswith("USDT"): continue
                    turnover, bid, ask = float(t.get("turnover24h", 0.0) or 0.0), float(t.get("bid1Price", 0.0) or 0.0), float(t.get("ask1Price", 0.0) or 0.0)
                    if bid > 0 and ask > bid and turnover >= min_turnover and (((ask - bid) / bid) * 10000.0) <= 12.0:
                        if (self.hardware_min_qty.get(symbol, 1.0) * bid) <= max_notional:
                            full_market.append((turnover, symbol))
                        
                full_market.sort(key=lambda x: x[0], reverse=True)
                full_market = [item[1] for item in full_market]
                
            if len(full_market) < 24: 
                logger.warning(f"[X-RAY] Only {len(full_market)} coins pass the affordable micro-cap filter. Adjusting matrix.")
                
        except Exception as e:
            logger.error(f"[X-RAY] Universe refresher failed fetching assets: {e}", exc_info=True)
            full_market = []
            
        banned_keywords = ["SOXL", "SPCX", "SKHY", "SNDK", "BANK", "MUUSDT", "BEAT", "MSTR", "ESPUSDT", "DEXE", "PUMP", "EUL", "XAU", "XAG", "BTCUSDT"]
        full_market = [s for s in full_market if not any(b in s for b in banned_keywords)]
        
        new_core_basket = []
        
        async with self.portfolio_state_lock:
            for s in self.active_positions_map.keys():
                if s not in new_core_basket: new_core_basket.append(s)
                
        for sym in full_market:
            if sym not in new_core_basket and len(new_core_basket) < 24: new_core_basket.append(sym)
                
        async with self.portfolio_state_lock:
            self.asset_basket = new_core_basket
            self.shadow_basket = [s for s in full_market if s not in self.asset_basket][:6]
            
        await self._prune_dead_symbols() 
        self._initialize_symbol_structures(self.asset_basket + self.shadow_basket)
        
        try:
            if historical_data := {sym: list(self.screener_memory[sym]["prices"]) for sym in self.asset_basket if self.screener_memory.get(sym) and len(self.screener_memory[sym].get("prices", [])) > 30}:
                if len(historical_data) >= 2: self.risk_vault.update_correlation_matrix(historical_data)
        except Exception as e: logger.error(f"[X-RAY] Correlation matrix update failed: {e}", exc_info=True)

        self.stream_restart_event.set()
        self.force_dna_refresh.set() 

    async def _universe_refresher_loop(self):
        while True:
            await asyncio.sleep(14400)
            await self.run_universe_refresher()

    async def stream_manager_loop(self):
        while True:
            stream_feed = HighVelocityMultiFeed(
                basket=self.asset_basket + self.shadow_basket[:6], 
                intervals=[self.timeframe, "60", "240"], 
                orderbook_callback=self.handle_incoming_orderbook_tick, 
                screener_callback=self.handle_incoming_basket_screener_update, 
                kline_callback=self.handle_incoming_kline_update, 
                trade_callback=self.handle_incoming_trade, 
                engine_reference=self
            )
            self.stream_feed_instance = stream_feed  
            stream_task = asyncio.create_task(stream_feed.initialize_multiplexed_stream())
            
            def _on_stream_done(t):
                if not t.cancelled() and not self.stream_restart_event.is_set(): self.stream_restart_event.set()
                    
            stream_task.add_done_callback(_on_stream_done)
            await self.stream_restart_event.wait()
            stream_task.cancel()
            stream_feed.terminate_all_feeds()
            self.stream_restart_event.clear()
            self.last_socket_reconnect = time.time() 
            await asyncio.sleep(2)

    async def run_exchange_state_reconciliation_daemon(self):
        logger.info("🛡️ STATE RECONCILIATION DAEMON ONLINE: Polling exchange state every 15s.")
        while True:
            await asyncio.sleep(15)
            try:
                pos_response = await self.executor.safe_call(self.executor.client.get_positions, category="linear", settleCoin="USDT")
                if pos_response.get("retCode") != 0: continue
                
                active_symbols = [p["symbol"] for p in pos_response.get("result", {}).get("list", []) if float(p.get("size", 0.0)) > 0]
                
                async with self.portfolio_state_lock:
                    for symbol in list(self.active_positions_map.keys()):
                        if symbol not in active_symbols:
                            if not (active_daemon := self.daemon_tasks.get(symbol)) or active_daemon.done():
                                logger.warning(f"🧹 STATE RECONCILIATION: Purging phantom lock for {symbol}")
                                self.active_positions_map.pop(symbol, None)
                                self.risk_vault.update_position_ledger(symbol, 0.0)
            except Exception as e: logger.debug(f"State reconciliation failed: {e}", exc_info=True)

    async def _position_lifecycle_daemon(self, symbol: str, signal_id: str, direction: str, current_price: float, atr: float, risk_matrix: dict, target_leverage: int = 8, market_regime: str = "TRENDING", is_recovery: bool = False, realigned_tp: float = None, dynamic_rr_ratio: float = 2.0, realigned_sl: float = None):
        """
        V55.1 TITANIUM SHIELD EXIT DAEMON:
        Enforces a 3-Minute Trade Immunity Window to eliminate 30-second tick-choking.
        Requires 5-tick sustained POFE persistence before ejecting.
        """
        exec_details = {"leverage": target_leverage, "execution_mode": "RECOVERY" if is_recovery else ("GHOST" if self.test_mode else "LIVE")}
        daemon_start_time = time.time()
        is_buy = direction == "BUY"
        
        if self.test_mode:
            await asyncio.sleep(60)
            self.log_to_wal_sync("settlement", [signal_id, 0.0, 0.0, "PAPER_TIMEOUT", exec_details])
            async with self.portfolio_state_lock: self.active_positions_map.pop(symbol, None)
            return

        try:
            order_filled, actual_entry, actual_qty_filled = False, current_price, risk_matrix.get("size", 1.0)
            for _ in range(5):  
                await asyncio.sleep(3)
                try:
                    pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                    pos_data = pos_res.get("result", {}).get("list", [])
                    if pos_data and float(pos_data[0].get("size", 0.0)) > 0:
                        order_filled, actual_entry, actual_qty_filled = True, float(pos_data[0].get("avgPrice", current_price)), float(pos_data[0].get("size", actual_qty_filled))
                        break
                except Exception as e: logger.debug(f"[X-RAY] Position fill check failed for {symbol}: {e}", exc_info=True); continue

            if not order_filled:
                try: await self.executor.safe_call(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
                except Exception as e: logger.debug(f"[X-RAY] Cancel all orders failed for {symbol}: {e}", exc_info=True)
                self.risk_vault.update_position_ledger(symbol, -risk_matrix['allocated_value_usdt'])
                async with self.portfolio_state_lock: self.active_positions_map.pop(symbol, None)
                return

            tick_dec = Decimal(str(self.tick_sizes.get(symbol, 0.0001)))
            def align_price(p: float) -> str: return str(Decimal(str(p)).quantize(tick_dec, rounding=ROUND_HALF_UP))

            actual_sl_distance = abs(actual_entry - realigned_sl) if realigned_sl else max(atr * self.live_params.get("sl_atr_mult", 2.5), actual_entry * 0.025)
            current_sl = realigned_sl if realigned_sl else (actual_entry - actual_sl_distance if is_buy else actual_entry + actual_sl_distance)
            current_tp = realigned_tp if realigned_tp else (actual_entry + (actual_sl_distance * dynamic_rr_ratio) if is_buy else actual_entry - (actual_sl_distance * dynamic_rr_ratio))
            
            if is_buy:
                current_tp = max(current_tp, current_price * 1.001)
                current_sl = min(current_sl, current_price * 0.999)
                if current_tp <= current_sl: current_tp = current_sl * 1.01
            else:
                current_tp = min(current_tp, current_price * 0.999)
                current_sl = max(current_sl, current_price * 1.001)
                if current_tp >= current_sl: current_tp = current_sl * 0.99
                
            try: await self.executor.safe_call(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, takeProfit=align_price(current_tp), stopLoss=align_price(current_sl))
            except Exception as e: logger.debug(f"[X-RAY] Initial TP/SL set failed for {symbol}: {e}", exc_info=True)

            stat_engine = self.stat_engines.get(symbol)
            feature_engine = self.feature_engines.get(symbol)
            
            highest_since_entry = actual_entry if is_buy else None
            lowest_since_entry = actual_entry if not is_buy else None
            
            r_t1 = round(max(1.0, dynamic_rr_ratio * 0.6), 2)
            r_t2 = round(max(1.5, dynamic_rr_ratio * 1.0), 2)
            r_t3 = round(max(2.0, dynamic_rr_ratio * 1.5), 2)
            scaled_levels = {r_t1: False, r_t2: False, r_t3: False}
            
            regime = market_regime 
            max_favorable_price, initial_risk = actual_entry, actual_sl_distance
            last_api_update_time, api_check_counter = time.time(), 0
            
            pofe_consecutive_ticks = 0 # Persistence counter to prevent single-tick ejections
            last_sent_sl_str = align_price(current_sl)
            last_sent_tp_str = align_price(current_tp)

            while True: 
                safe_c_price = current_price
                if stat_engine and stat_engine.true_micro_price > 0: safe_c_price = stat_engine.true_micro_price
                sl_proximity = abs(safe_c_price - current_sl) / (safe_c_price + 1e-9)
                loop_sleep = 0.2 if sl_proximity < 0.005 else 1.0
                await asyncio.sleep(loop_sleep) 
                
                now = time.time()
                api_check_counter += 1
                time_in_mins = (now - daemon_start_time) / 60.0
                
                requires_sl_update = False
                requires_tp_update = False
                
                if api_check_counter % (60 if loop_sleep == 1.0 else 300) == 0:
                    regime = feature_engine.detect_market_regime() if feature_engine else regime
                
                if api_check_counter >= (15 if loop_sleep == 1.0 else 75):
                    api_check_counter = 0
                    try:
                        pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                        pos_list = pos_res.get("result", {}).get("list", [])
                        if (not pos_list) or float(pos_list[0].get("size", 0.0)) == 0.0: break 
                    except Exception as e: logger.debug(f"[X-RAY] Daemon API health check failed for {symbol}: {e}", exc_info=True)

                if safe_c_price != current_price:
                    if is_buy and safe_c_price > max_favorable_price: 
                        max_favorable_price = safe_c_price
                        if safe_c_price > highest_since_entry: highest_since_entry = safe_c_price
                    elif not is_buy and safe_c_price < max_favorable_price: 
                        max_favorable_price = safe_c_price
                        if safe_c_price < lowest_since_entry: lowest_since_entry = safe_c_price
                    
                r_multiple = (max_favorable_price - actual_entry) / (initial_risk + 1e-9) if is_buy else (actual_entry - max_favorable_price) / (initial_risk + 1e-9)
                current_r = (safe_c_price - actual_entry) / (initial_risk + 1e-9) if is_buy else (actual_entry - safe_c_price) / (initial_risk + 1e-9)

                hawkes_z = getattr(stat_engine, 'hawkes_z', getattr(stat_engine, 'vpin_z', 0.0))
                vpin_z = getattr(stat_engine, 'vpin_z', hawkes_z)
                cvd_z = getattr(stat_engine, 'ofi_fast_z', 0.0) 
                
                ob = self.orderbook_snapshots.get(symbol, {})
                b_vol, a_vol = float(ob.get("bid_size", 0.0)), float(ob.get("ask_size", 0.0))
                imbalance = (b_vol - a_vol) / (b_vol + a_vol + 1e-9)
                
                # 🌌 1. VERIFIED POFE EJECTION (Only active AFTER 3.0 minute immunity window + 5-tick persistence)
                if time_in_mins >= 3.0:
                    if (is_buy and imbalance < -0.85 and cvd_z < -2.0) or (not is_buy and imbalance > 0.85 and cvd_z > 2.0):
                        pofe_consecutive_ticks += 1
                        if pofe_consecutive_ticks >= 5: # Must persist for 5 consecutive loops
                            try:
                                logger.critical(f"🛑 VERIFIED POFE EJECTION // {symbol} Sustained momentum shift verified (CVD z={cvd_z:.2f}, Imb={imbalance:.2f}). Escaping via Limit-IOC.")
                                escape_price = (safe_c_price * 0.999) if is_buy else (safe_c_price * 1.001)
                                await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side="Sell" if is_buy else "Buy", orderType="Limit", price=align_price(escape_price), qty=str(actual_qty_filled), timeInForce="IOC", reduceOnly=True)
                                await asyncio.sleep(1.0)
                                continue 
                            except Exception as e: logger.error(f"[X-RAY] POFE Limit-Escape failed for {symbol}: {e}", exc_info=True)
                    else:
                        pofe_consecutive_ticks = 0 # Reset counter if orderbook recovers

                # 🌌 2. VOLATILITY-AWARE VOLUME DEATH
                inst_var = getattr(stat_engine, 'inst_variance', 0.01)
                is_coiling = inst_var < 0.0001 
                if not is_coiling and current_r < -0.35 and hawkes_z < -1.5 and time_in_mins > 15.0:
                    try:
                        logger.warning(f"📉 VOLUME DEATH EJECTION // {symbol} Trade bleeding with dropping volatility. Escaping via Limit-IOC at {time_in_mins:.1f}m.")
                        escape_price = (safe_c_price * 0.999) if is_buy else (safe_c_price * 1.001)
                        await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side="Sell" if is_buy else "Buy", orderType="Limit", price=align_price(escape_price), qty=str(actual_qty_filled), timeInForce="IOC", reduceOnly=True)
                        await asyncio.sleep(1.0)
                        continue
                    except Exception as e: logger.error(f"[X-RAY] Volume death escape failed for {symbol}: {e}", exc_info=True)

                # 🌌 3. PARABOLIC ACCELERATION EJECTION
                if r_multiple >= 1.5 and (hawkes_z > 3.0 and abs(cvd_z) > 2.5):
                    try:
                        logger.critical(f"🚀 PARABOLIC EJECTION // {symbol} True Liquidation cascade detected (CVD z={cvd_z:.2f}). Exiting into strength via Limit-IOC at {r_multiple:.1f}R.")
                        escape_price = (safe_c_price * 0.9995) if is_buy else (safe_c_price * 1.0005)
                        await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side="Sell" if is_buy else "Buy", orderType="Limit", price=align_price(escape_price), qty=str(actual_qty_filled), timeInForce="IOC", reduceOnly=True)
                        await asyncio.sleep(1.0)
                        continue 
                    except Exception as e: logger.error(f"[X-RAY] Parabolic Escape failed for {symbol}: {e}", exc_info=True)

                # 🌌 4. SUB-1R HIGH-WATER MARK TRAILING (Shifted to +0.8R & 3-Min Immunity)
                if time_in_mins >= 3.0 and r_multiple >= 0.8 and current_sl == (realigned_sl if realigned_sl else (actual_entry - initial_risk if is_buy else actual_entry + initial_risk)):
                    sub_1r_sl = (max_favorable_price - (initial_risk * 0.5)) if is_buy else (max_favorable_price + (initial_risk * 0.5))
                    if (is_buy and sub_1r_sl > current_sl) or (not is_buy and sub_1r_sl < current_sl):
                        current_sl = sub_1r_sl
                        requires_sl_update = True
                        logger.info(f"[X-RAY] 🛡️ SUB-1R RATCHET // {symbol} Excursion hit +{r_multiple:.2f}R. Tightened risk floor to {align_price(current_sl)}.")

                if not self.test_mode and r_multiple >= 1.0:
                    try:
                        current_pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                        p_list = current_pos_res.get("result", {}).get("list", [])
                        if p_list and float(p_list[0].get("size", 0.0)) > 0:
                            current_qty = float(p_list[0]["size"])
                            limits = self.sor.instrument_cache.get(symbol, {"min_qty": 0.001, "qty_step": 0.001})
                            
                            for target_r, flag in scaled_levels.items():
                                if r_multiple >= target_r and not flag:
                                    portion = 0.25 if target_r == r_t1 else (0.35 if target_r == r_t2 else 0.40)
                                    qty_close = current_qty * portion
                                    aligned_qty_close = math.floor(qty_close / limits["qty_step"]) * limits["qty_step"]
                                    
                                    if aligned_qty_close >= limits["min_qty"] and (aligned_qty_close * safe_c_price) >= 6.0:
                                        logger.critical(f"[X-RAY] 💰 HARMONIC SCALE-OUT // {symbol} reached {target_r}R. Scaling out {aligned_qty_close} units ({portion*100}%).")
                                        await self.executor.safe_call(
                                            self.executor.client.place_order, 
                                            category="linear", symbol=symbol, 
                                            side="Sell" if is_buy else "Buy", 
                                            orderType="Limit", 
                                            price=align_price(safe_c_price),
                                            qty=str(aligned_qty_close), 
                                            timeInForce="IOC", reduceOnly=True
                                        )
                                        scaled_levels[target_r] = True
                                        break
                                    else:
                                        if not flag: scaled_levels[target_r] = True
                                        break
                    except Exception as e: logger.error(f"[X-RAY] Scale-out execution fault for {symbol}: {e}", exc_info=True)

                live_atr_raw = feature_engine.get_computed_atr() if feature_engine and hasattr(feature_engine, 'get_computed_atr') else 0.0
                live_atr = live_atr_raw if live_atr_raw > 0 else (safe_c_price * 0.005)
                
                base_mult = 2.5 if regime in ["TRENDING", "VOLATILE"] else 1.8
                min_mult = 0.4 
                
                compression_k = 2.5 
                compression_shift = 2.0 
                x = compression_k * (r_multiple - compression_shift)
                if x > 700: x = 700
                elif x < -700: x = -700
                sigmoid_factor = min_mult + (base_mult - min_mult) / (1.0 + math.exp(x))

                vol_ratio = live_atr / max(safe_c_price, 1e-9)
                dynamic_grace_period = max(15.0, min(60.0, 1.0 / (vol_ratio * 100 + 1e-9)))

                if r_multiple < 0.5 and time_in_mins > dynamic_grace_period:
                    theta_decay = max(0.4, 1.0 - ((time_in_mins - dynamic_grace_period) * 0.010)) 
                else:
                    theta_decay = 1.0 

                tox_mod = 0.6 if (is_buy and imbalance < -0.5) or (not is_buy and imbalance > 0.5) else 1.0
                
                raw_trail_dist = max(live_atr * sigmoid_factor * theta_decay * tox_mod, safe_c_price * 0.004)
                
                if r_multiple >= 1.0:
                    raw_sl = (max_favorable_price - raw_trail_dist) if is_buy else (max_favorable_price + raw_trail_dist)
                    be_plus = (actual_entry + actual_entry * 0.002) if is_buy else (actual_entry - actual_entry * 0.002)
                    new_sl_val = max(raw_sl, be_plus) if is_buy else min(raw_sl, be_plus)
                    
                    if (is_buy and new_sl_val > current_sl) or (not is_buy and new_sl_val < current_sl):
                        current_sl = new_sl_val
                        requires_sl_update = True

                # 🌌 V55.1 ASYMMETRIC TP REPULSION
                momentum_stretch = max(0.0, hawkes_z * 0.6) if regime == "TRENDING" else 0.0
                if hawkes_z < -1.5 and r_multiple > 1.0:
                    momentum_stretch -= 0.5 
                
                target_rr = min(6.0, dynamic_rr_ratio + momentum_stretch + (max(0.0, r_multiple - 1.0) * 0.3)) 
                calc_tp = actual_entry + (initial_risk * target_rr) if is_buy else actual_entry - (initial_risk * target_rr)
                
                if (is_buy and calc_tp > current_tp) or (not is_buy and calc_tp < current_tp):
                    if abs(calc_tp - current_tp) / actual_entry > 0.0025:
                        current_tp = calc_tp
                        requires_tp_update = True

                if (requires_sl_update or requires_tp_update) and (now - last_api_update_time > (3.0 if loop_sleep == 1.0 else 1.0)):
                    spread = (ob.get("best_ask", safe_c_price) - ob.get("best_bid", safe_c_price)) / safe_c_price if ob.get("best_bid", 0) > 0 else 0.0005
                    min_distance = max(live_atr * 0.2, safe_c_price * 0.003, spread * 1.5 * safe_c_price) 
                    
                    if is_buy:
                        current_sl = min(current_sl, safe_c_price - min_distance)
                        current_tp = max(current_tp, safe_c_price + min_distance)
                        if current_tp <= current_sl: current_tp = current_sl * 1.01
                    else:
                        current_sl = max(current_sl, safe_c_price + min_distance)
                        current_tp = min(current_tp, safe_c_price - min_distance)
                        if current_tp >= current_sl: current_tp = current_sl * 0.99

                    new_sl_str = align_price(current_sl)
                    new_tp_str = align_price(current_tp)

                    if new_sl_str != last_sent_sl_str or new_tp_str != last_sent_tp_str:
                        try:
                            await self.executor.safe_call(
                                self.executor.client.set_trading_stop, 
                                category="linear", symbol=symbol, positionIdx=0, 
                                takeProfit=new_tp_str, stopLoss=new_sl_str
                            )
                            last_api_update_time = now
                            last_sent_sl_str = new_sl_str
                            last_sent_tp_str = new_tp_str
                            
                            if requires_tp_update: logger.info(f"[X-RAY] 🌌 ASYMMETRIC TP REPULSION // {symbol} Target shifted to {new_tp_str}.")
                            if requires_sl_update: logger.info(f"[X-RAY] 🛡️ TRAILING RATCHET // {symbol} SL locked at {new_sl_str}.")
                        except Exception as e: 
                            logger.debug(f"[X-RAY] Failed to amend trailing stop for {symbol}: {e}", exc_info=True)

            await asyncio.sleep(2.0) 
            try: 
                closed_data = await self.executor.safe_call(self.executor.client.get_closed_pnl, category="linear", symbol=symbol, limit=1)
                closed_list = closed_data.get("result", {}).get("list", [])
                if closed_list:
                    net_pnl = float(closed_list[0].get("closedPnl", 0.0))
                    real_outcome = "PROFIT" if net_pnl > 0 else "LOSS"
                    capital_risked = (actual_entry * float(closed_list[0].get("qty", 1))) / target_leverage
                    self.risk_vault.update_kelly_metrics(net_pnl > 0, net_pnl / (capital_risked + 1e-9))
                    
                    fees = float(closed_list[0].get("execFee", 0.0))
                    exit_price = float(closed_list[0].get("avgExitPrice", actual_entry))
                    
                    raw_pnl = (exit_price - actual_entry) * float(closed_list[0].get("qty", 1)) if direction == "BUY" else (actual_entry - exit_price) * float(closed_list[0].get("qty", 1))
                    slip_cost = raw_pnl - net_pnl - fees
                    # Bounded slippage bps calculation to prevent glitched 200,000+ bps reports in Telegram
                    slippage_bps = min(500.0, max(-500.0, (slip_cost / (capital_risked + 1e-9)) * 10000)) if capital_risked > 0 else 0.0
                    duration_mins = (time.time() - daemon_start_time) / 60.0
                    
                    exec_details["fees_usdt"] = fees
                    
                    if net_pnl < 0:
                        async with self.circuit_breaker_lock:
                            prev_loss = [t for t in self.tick_error_counts.get(symbol, []) if time.time() - t < 7200]
                            prev_loss.append(time.time())
                            self.tick_error_counts[symbol] = prev_loss
                            if len(prev_loss) >= 2:
                                dynamic_lockout = 1800 * (1.0 + (min(3.0, stat_engine.inst_variance * 5000.0) if stat_engine else 0.0) * 2.0)
                                self.circuit_breakers[symbol] = time.time() + dynamic_lockout
                                logger.warning(f"[X-RAY] ⏸️ VOLATILITY LOCKOUT: {symbol} paused for {dynamic_lockout/60:.1f} mins after 2 consecutive losses.")
                else: 
                    net_pnl, real_outcome, slippage_bps, fees, duration_mins = 0.0, "RECONCILED", 0.0, 0.0, 0.0
            except Exception as e: 
                logger.error(f"[X-RAY] Failed to fetch closed PnL for {symbol}: {e}", exc_info=True)
                net_pnl, real_outcome, slippage_bps, fees, duration_mins = 0.0, "RECONCILED", 0.0, 0.0, 0.0
            
            self.log_to_wal_sync("settlement", [signal_id, net_pnl, slippage_bps, real_outcome, exec_details])
            self.track_task(self._safe_telegram_dispatch(self.telegram.format_execution_receipt(symbol, net_pnl, slippage_bps, fees, duration_mins, net_pnl > 0)))

        except Exception as e:
            logger.error(f"[X-RAY] Position daemon critical fault for {symbol}: {e}", exc_info=True)
            try:
                pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                if float(pos_res.get("result", {}).get("list", [{}])[0].get("size", 0.0)) > 0:
                    escape_price = (current_price * 0.999) if direction == "BUY" else (current_price * 1.001)
                    await self.executor.safe_call(
                        self.executor.client.place_order, 
                        category="linear", symbol=symbol, 
                        side="Sell" if pos_res["result"]["list"][0]["side"] == "Buy" else "Buy", 
                        orderType="Limit", price=align_price(escape_price),
                        qty=str(float(pos_res["result"]["list"][0]["size"])), 
                        timeInForce="IOC", reduceOnly=True
                    )
            except Exception as e2: logger.error(f"[X-RAY] Emergency daemon flatten failed for {symbol}: {e2}", exc_info=True)
        finally:
            async with self.portfolio_state_lock: self.active_positions_map.pop(symbol, None)
            self.risk_vault.update_position_ledger(symbol, 0.0)

    async def graceful_shutdown(self):
        logger.critical("🛑 INITIATING EMERGENCY FLATTEN & SHUTDOWN...")
        symbols_to_cancel = []
        async with self.portfolio_state_lock: symbols_to_cancel = list(self.active_positions_map.keys())
            
        for symbol in symbols_to_cancel:
            try: await self.executor.safe_call(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
            except Exception as e: logger.error(f"[X-RAY] Shutdown cancel order failed for {symbol}: {e}", exc_info=True)
        
        for symbol in symbols_to_cancel:
            try:
                pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                if float(pos_res.get("result", {}).get("list", [{}])[0].get("size", 0.0)) > 0:
                    await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side="Sell" if pos_res["result"]["list"][0]["side"] == "Buy" else "Buy", orderType="Market", qty=str(float(pos_res["result"]["list"][0]["size"])), timeInForce="IOC", reduceOnly=True)
            except Exception as e: logger.error(f"[X-RAY] Shutdown flatten order failed for {symbol}: {e}", exc_info=True)
                
        logger.critical("🔍 VERIFYING ZERO EXPOSURE...")
        verified_flat = False  
        for attempt in range(10):
            try:
                pos_response = await self.executor.safe_call(self.executor.client.get_positions, category="linear", settleCoin="USDT")
                active_orphans = [p for p in pos_response.get("result", {}).get("list", []) if float(p.get("size", 0.0)) > 0]
                if not active_orphans:
                    logger.critical("✅ EXPOSURE VERIFIED AT ZERO. ALL POSITIONS FLATTENED.")
                    verified_flat = True
                    break
                else:
                    logger.error(f"⚠️ {len(active_orphans)} positions still open. Retrying flatten sequence ({attempt+1}/10).")
                    for p in active_orphans:
                        sym = p["symbol"]
                        try: await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=sym, side="Sell" if p["side"] == "Buy" else "Buy", orderType="Market", qty=str(float(p["size"])), timeInForce="IOC", reduceOnly=True)
                        except Exception as e: logger.error(f"[X-RAY] Retry flatten failed for {sym}: {e}", exc_info=True)
                    await asyncio.sleep(5)
            except Exception as e: await asyncio.sleep(5)
                
        if not verified_flat: logger.critical("💀 FATAL: COULD NOT VERIFY ZERO EXPOSURE AFTER MAXIMUM RETRIES.")
            
        try:
            async with aiosqlite.connect(self.wal_db_path) as db:
                async with db.execute("SELECT COUNT(*) FROM pending_wal") as cursor:
                    count = (await cursor.fetchone())[0]
            logger.info(f"⏳ WAL Engine offline. {count} items remaining in local disk buffer for next reboot.")
        except Exception: pass
            
        if hasattr(self, 'telegram'): await self.telegram.close()
        if hasattr(self.executor, "_api_thread_pool"): self.executor._api_thread_pool.shutdown(wait=False, cancel_futures=True)
        logger.critical("✅ MATRIX DISCONNECTED.")

    async def _safe_daemon_run(self, coro_func):
        consecutive_crashes = 0
        while True:
            try: 
                await coro_func()
                consecutive_crashes = 0  
            except asyncio.CancelledError: break
            except Exception as e:
                consecutive_crashes += 1
                sleep_time = min(300, 5 * (2 ** (consecutive_crashes - 1)))
                logger.error(f"Daemon {coro_func.__name__} crashed. Restarting in {sleep_time}s: {e}", exc_info=True)
                await asyncio.sleep(sleep_time)

    async def run_engine_forever(self):
        self.fsm.release_global_emergency_lock()
        
        try:
            logger.info("🔍 Verifying Bybit account position mode compatibility...")
            try: 
                await self.executor.safe_call(self.executor.client.switch_position_mode, category="linear", coin="USDT", mode=0)
                logger.info("✅ Bybit Unified Account confirmed in One-Way Mode (positionIdx=0).")
            except Exception as e: 
                if "not modified" in str(e).lower() or "110025" in str(e): logger.info("✅ Bybit Unified Account already in One-Way Mode.")
                else: logger.debug(f"[X-RAY] Position mode verification note: {e}")
        except Exception as e: logger.debug(f"[X-RAY] Position mode check block failed: {e}", exc_info=True)

        try: await self._fetch_exchange_tick_sizes()
        except Exception: pass
        try: await self.synchronize_exchange_state()
        except Exception: pass
            
        try:
            boot_bal = await self.executor.get_wallet_balance_usdt()
            self.global_state_cache["start_of_day_balance"] = boot_bal
            self.global_state_cache["wallet_baseline"] = max(boot_bal, 0.01)
            self.global_state_cache["last_updated"] = time.time()
            self.global_state_cache["current_day"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        except Exception: pass
        
        try:
            max_notional = await self._get_max_affordable_notional()
            tickers_res = await self.executor.safe_call(self.executor.client.get_tickers, category="linear")
            full_market = []
            min_turnover = AdaptiveSessionClock.get_turnover_threshold()
            
            if tickers_res.get("retCode") == 0:
                ticker_list = tickers_res.get("result", {}).get("list", [])
                for t in ticker_list:
                    symbol = t.get("symbol", "")
                    if not symbol.endswith("USDT"): continue
                        
                    turnover = float(t.get("turnover24h", 0.0) or 0.0)
                    bid = float(t.get("bid1Price", 0.0) or 0.0)
                    ask = float(t.get("ask1Price", 0.0) or 0.0)
                    
                    if bid <= 0 or ask <= 0 or ask <= bid: continue
                    if turnover >= min_turnover:
                        if (self.hardware_min_qty.get(symbol, 1.0) * bid) <= max_notional:
                            full_market.append((turnover, symbol))
                        
                full_market.sort(key=lambda x: x[0], reverse=True)
                full_market = [item[1] for item in full_market]
                
            if len(full_market) < 24: 
                logger.warning(f"[X-RAY] Only {len(full_market)} coins pass the initial boot affordable micro-cap filter.")
                
        except Exception as e:
            logger.error(f"[X-RAY] Initial boot universe fetch failed: {e}", exc_info=True)
            full_market = []

        banned_keywords = ["SOXL", "SPCX", "SKHY", "SNDK", "BANK", "MUUSDT", "BEAT", "MSTR", "ESPUSDT", "DEXE", "PUMP", "EUL", "XAU", "XAG", "BTCUSDT"]
        full_market = [s for s in full_market if not any(b in s for b in banned_keywords)]

        new_core_basket = []
        if boot_basket := full_market[:24]:
            self.asset_basket = boot_basket[:24]
            self.shadow_basket = [s for s in full_market if s not in self.asset_basket][:6]
            self._initialize_symbol_structures(self.asset_basket + self.shadow_basket)
        
        daemons = [
            self.run_db_wal_worker, self._batch_wal_flush_loop, self.run_dna_prewarmer, 
            self.stream_manager_loop, self.run_system_heartbeat, self.cleanup_stale_locks, 
            self.run_shadow_resolution_daemon, self._universe_refresher_loop, 
            self.auction_engine.run_global_capital_auction_worker, self.run_omni_swarm_director,            
            self.run_exchange_state_reconciliation_daemon,
            self.run_crowded_trade_oracle 
        ]
        await asyncio.gather(*[asyncio.create_task(self._safe_daemon_run(d)) for d in daemons], return_exceptions=True)

async def main():
    engine = DistributedQuantEngine()
    try: await engine.run_engine_forever()
    except asyncio.CancelledError: pass
    finally: await engine.graceful_shutdown()

if __name__ == "__main__":
    from keep_alive import keep_alive
    keep_alive()
    try: asyncio.run(main())
    except KeyboardInterrupt: sys.exit(0)