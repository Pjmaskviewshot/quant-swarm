"""
💎 V25.6 APEX QUANTUM PRIME: BARE-METAL CORE
------------------------------------------------------------------------
Micro-Scalping & In-Flight Guard Execution Engine.

Architectural Supremacy (V25.6 - Database Schema Alignment & Bug Fixes):
- Forensic Ledger Synchronization: Fixed a critical UUID desync bug that orphaned 
  database records from their live execution contexts, restoring PnL feedback loops.
- Centralized Exit Matrix Integration: Stripped out all redundant manual trailing 
  stop logic in the lifecycle daemon. The daemon now inherently trusts the 
  `AdvancedIntelligentExitMatrix` to handle Kinetic TP Compression and AT-SL trails natively.
- Valid UUID Generation: Replaced truncated 16-character hex digests with standard 
  36-character UUID strings (`uuid.uuid4()`) to perfectly satisfy Supabase's constraints.
- Hot-Swap Convergence Warm-Up Guard: Enforces a strict 50-tick minimum warm-up 
  window for newly hot-swapped assets before the evaluation gate allows live execution.
- Async Concurrency Hardening: Throttled the ThreadPoolExecutor to prevent GIL starvation, 
  applied atomic reservation locks to eradicate double-spend entry race conditions, and 
  added explicit GC task-tracking sweep protection.
- Safe Boot Re-Arm Guard: Prevents blindly lifting the FSM emergency lock on startup 
  if equity is compromised.
"""

import os
import sys
import time
import math
import asyncio
import logging
import hashlib
import uuid
import datetime
import numpy as np
import concurrent.futures
import multiprocessing
from collections import deque
from typing import Dict, List, Any, Callable
from dataclasses import dataclass
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

class EmergencyShutdown(Exception):
    """Custom exception to trigger safe, async-aware system shutdown."""
    pass

# Core & Feature Modules
from core.fsm import SystemStateMachine
from core.memory import MemoryBank
from core.quantum_entry import QuantumEntryMatrix  
from core.intelligent_exit import IntelligentExitEngine, ExecutionGovernorFSM, PositionExitState, ThesisVector
from features.adaptive_engine import AdaptiveFeatureEngine
from features.omni_scanner import GlobalOmniScanner   
from features.micro_models import ContinuousMicrostructureEngine

# Execution & Risk
from execution.sor import SmartOrderRouter
from portfolio.risk_vault import InstitutionalRiskVault
from execution.delta_neutral import DeltaNeutralYieldEngine 

# External Connectors
from ingestion.multi_feed import MarketStateMatrix
from services.bybit_v5 import BybitUnifiedExecutor
from services.telegram_ops import AsyncTelegramReporter
from services.tensor_oracle import CrossAssetTensorOracle
from services.sector_oracle import SectorEigenOracle

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(name)s] - [%(levelname)s] - [%(message)s]', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("QUANT_CORE.V25_APEX")


@dataclass
class MutationCommand:
    target_asset: str
    mutation_type: str
    payload: Dict[str, Any]
    callback: Callable = None


class GlobalStateActor:
    """
    🚀 V25.1 LOCK-FREE STATE ACTOR (LMAX Disruptor Pattern)
    """
    def __init__(self, core_engine):
        self.core = core_engine
        self.mutation_queue = asyncio.Queue(maxsize=10000)
        self._is_running = False

    async def start(self):
        self._is_running = True
        logger.info("🛡️ V25.6 LOCK-FREE STATE ACTOR ONLINE. (Disruptor Pattern Active)")
        while self._is_running:
            try:
                cmd: MutationCommand = await self.mutation_queue.get()
                self._apply_mutation(cmd)
                self.mutation_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[X-RAY] State Actor Fault: {e}", exc_info=True)

    def _apply_mutation(self, cmd: MutationCommand):
        try:
            if cmd.mutation_type == "REGISTER_POSITION":
                self.core.active_positions_map[cmd.target_asset] = cmd.payload["direction"]
                self.core.risk_vault.update_position_ledger(cmd.target_asset, cmd.payload["notional"])
                self.core.in_flight_symbols.pop(cmd.target_asset, None)
            
            elif cmd.mutation_type == "RESERVE_IN_FLIGHT":
                self.core.in_flight_symbols[cmd.target_asset] = time.time() + 60.0

            elif cmd.mutation_type == "RELEASE_IN_FLIGHT":
                self.core.in_flight_symbols.pop(cmd.target_asset, None)

            elif cmd.mutation_type == "LIQUIDATE_POSITION":
                self.core.active_positions_map.pop(cmd.target_asset, None)
                self.core.in_flight_symbols.pop(cmd.target_asset, None)
                self.core.risk_vault.update_position_ledger(cmd.target_asset, 0.0)
                self.core.exit_states.pop(cmd.target_asset, None)
                self.core.active_contexts.pop(cmd.target_asset, None)
                self.core.last_exit_direction[cmd.target_asset] = (cmd.payload.get("direction", "NONE"), time.time())

            elif cmd.mutation_type == "UPDATE_PROFIT_PEAK":
                state = self.core.exit_states.get(cmd.target_asset)
                if state:
                    state.profit_state.peak_pnl = cmd.payload["peak_pnl"]
                    state.profit_state.locked_pnl = cmd.payload["locked_pnl"]

            if cmd.callback: cmd.callback(True)
        except Exception as e:
            logger.error(f"[X-RAY] Failed to apply mutation {cmd.mutation_type} for {cmd.target_asset}: {e}")
            if cmd.callback: cmd.callback(False)

    def dispatch(self, asset: str, m_type: str, payload: Dict[str, Any]):
        try:
            self.mutation_queue.put_nowait(MutationCommand(asset, m_type, payload))
        except asyncio.QueueFull:
            logger.critical(f"FATAL: State Disruptor Queue Overflow on {asset}.")


async def safe_daemon_wrapper(coro_func, engine_ref):
    while not engine_ref.fsm.is_emergency_locked():
        try:
            await coro_func()
        except asyncio.CancelledError: break
        except EmergencyShutdown as e:
            logger.critical(f"[SHUTDOWN] System Kill Switch triggered inside {coro_func.__name__}: {e}")
            engine_ref.fsm.trigger_global_emergency_lock()
            break
        except Exception as e:
            logger.error(f"[DAEMON CRASH] {coro_func.__name__} faulted: {e}", exc_info=True)
            await asyncio.sleep(2.0)


class DistributedQuantEngine:
    def __init__(self):
        load_dotenv()
        self.test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
        
        if self.test_mode: logger.critical("⚠️ TEST MODE: Paper Trading Armed.")
        else: logger.critical("💎 LIVE MODE: V25.6 APEX BARE-METAL CORE ACTIVE.")
        
        self.asset_basket: List[str] = []
        self.timeframe = os.getenv("TRADING_TIMEFRAME", "15")
        self.shadow_basket: List[str] = []
        
        # Throttled thread pool to prevent GIL starvation
        safe_workers = min(4, multiprocessing.cpu_count() - 1) if multiprocessing.cpu_count() > 1 else 1
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=safe_workers, thread_name_prefix="V25_Matrix_Workers")
        
        self.db_semaphore = asyncio.Semaphore(5)
        self.execution_semaphore = asyncio.Semaphore(10)
        
        self.circuit_breakers: Dict[str, float] = {}
        self.circuit_breaker_lock = asyncio.Lock() 
        
        self.stream_restart_event = asyncio.Event()
        self.force_dna_refresh = asyncio.Event() 
        
        self.fsm = SystemStateMachine()
        
        try:
            self.memory = MemoryBank()
        except Exception as e:
            logger.error(f"[X-RAY] ⚠️ CLOUD DB OFFLINE: Supabase connection failed ({e}). Booting in Degraded Local-Only Mode.")
            self.memory = None 
            
        self.risk_vault = InstitutionalRiskVault(max_drawdown_pct=0.05, max_single_position_risk_pct=0.015)
        
        self.yield_engine = DeltaNeutralYieldEngine(self)  
        
        # Core Physics Engines
        self.stat_engines: Dict[str, ContinuousMicrostructureEngine] = {} 
        self.feature_engines: Dict[str, AdaptiveFeatureEngine] = {}
        self.entry_matrices: Dict[str, QuantumEntryMatrix] = {}
        
        # State Arrays
        self.screener_memory, self.screener_metrics, self.ram_dna_cache = {}, {}, {}
        self.tick_history: Dict[str, deque] = {}
        self.volatility_baseline: Dict[str, float] = {}
        self.orderbook_snapshots: Dict[str, dict] = {} 
        
        self.state_actor = GlobalStateActor(self)
        self.active_positions_map: Dict[str, str] = {}  
        self.in_flight_symbols: Dict[str, float] = {}
        self.last_exit_direction: Dict[str, tuple] = {} 

        self.symbol_locks, self.daemon_tasks, self.last_eval_time = {}, {}, {}
        self.last_amend_time: Dict[str, float] = {}
        self._active_tasks = set()
        
        self.global_state_cache = {"last_updated": 0.0}
        self.live_params = self._load_live_params()
        self.last_socket_reconnect = 0.0 
        
        self.active_contexts: Dict[str, dict] = {}
        self.exit_states: Dict[str, Any] = {}
        self.recent_pnl_history: deque = deque(maxlen=15)
        
        self.telegram = AsyncTelegramReporter(token=os.getenv("TELEGRAM_BOT_TOKEN"), chat_id=os.getenv("TELEGRAM_CHAT_ID"))
        self.telegram_queue = asyncio.Queue(maxsize=50)
        
        self.executor = BybitUnifiedExecutor(api_key=os.getenv("BYBIT_API_KEY"), api_secret=os.getenv("BYBIT_API_SECRET"), testnet=self.test_mode)
        
        self.sor = SmartOrderRouter(executor=self.executor, max_slippage_pct=0.0012, core_engine=self)
        self.omni_scanner = GlobalOmniScanner(self.executor)
        self.stream_feed_instance = None  

    def _on_task_done(self, task):
        self._active_tasks.discard(task)
        if not task.cancelled() and task.exception():
            logger.error(f"[X-RAY] ❌ BACKGROUND TASK CRASHED: {task.exception()}", exc_info=task.exception())

    def track_task(self, coro: Any):
        # Explicitly clear dead tasks before evaluating the limit
        self._active_tasks = {t for t in self._active_tasks if not t.done()}
        
        if len(self._active_tasks) > 500:
            logger.critical("[X-RAY] 🛑 FATAL: TASK LIMIT EXCEEDED (>500). Dropping background task to prevent memory overflow.")
            dummy = asyncio.Future()
            dummy.set_result(None)
            return dummy
            
        task = asyncio.create_task(coro)
        self._active_tasks.add(task)
        task.add_done_callback(self._on_task_done)
        return task

    def _load_live_params(self) -> dict:
        default_params = {"sl_atr_mult": 2.5, "rr_ratio": 2.0}
        import os, json
        try:
            if os.path.exists("params.json"):
                with open("params.json", "r") as f: return {**default_params, **json.load(f)}
        except Exception: pass
        return default_params

    async def _prune_dead_symbols(self):
        active_set = set(self.asset_basket + self.shadow_basket + list(self.active_positions_map.keys()) + list(self.in_flight_symbols.keys()))
        for key in list(self.stat_engines.keys()):
            if key not in active_set:
                self.stat_engines.pop(key, None)
                self.feature_engines.pop(key, None)
                self.entry_matrices.pop(key, None) 
                self.symbol_locks.pop(key, None)
                self.tick_history.pop(key, None)
                self.ram_dna_cache.pop(key, None)
                self.screener_memory.pop(key, None)
                self.screener_metrics.pop(key, None)
                self.volatility_baseline.pop(key, None)
                self.last_eval_time.pop(key, None)
                self.orderbook_snapshots.pop(key, None) 

    async def _save_sgd_state(self):
        state_snapshot = {}
        for sym, engine in self.stat_engines.items():
            if hasattr(engine, 'rls_trend'):
                state_snapshot[sym] = {
                    "weights_trending": engine.rls_trend.w.copy().tolist(), 
                    "weights_ranging": engine.rls_range.w.copy().tolist(),
                    "P_trending": engine.rls_trend.P.copy().tolist(), 
                    "P_ranging": engine.rls_range.P.copy().tolist()
                }
        def _write_file():
            import tempfile, json
            try:
                storage_path = os.getenv("PERSISTENT_STORAGE_PATH", ".")
                target_path = os.path.join(storage_path, "sgd_state.json")
                fd, path = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(target_path)) or ".")
                with os.fdopen(fd, 'w') as f: json.dump(state_snapshot, f)
                os.replace(path, target_path)
            except Exception as e: logger.debug(f"[X-RAY] Failed RLS disk serialization: {e}")
        
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.thread_pool, _write_file)

    def _initialize_symbol_structures(self, symbols: List[str]):
        for s in symbols:
            if s not in self.stat_engines: self.stat_engines[s] = ContinuousMicrostructureEngine(symbol=s)
            if s not in self.feature_engines: self.feature_engines[s] = AdaptiveFeatureEngine(memory_window_long=1800)
            if s not in self.entry_matrices: self.entry_matrices[s] = QuantumEntryMatrix(window_size=10)
            if s not in self.symbol_locks: self.symbol_locks[s] = asyncio.Lock()
            if s not in self.tick_history: self.tick_history[s] = deque(maxlen=2000)
            if s not in self.screener_memory: self.screener_memory[s] = {"prices": deque(maxlen=1440), "highs": deque(maxlen=150), "lows": deque(maxlen=150), "volumes": deque(maxlen=1440), "last_update_time": 0.0}
            if s not in self.screener_metrics: self.screener_metrics[s] = {"vol_mult": 1.0}
            if s not in self.volatility_baseline: self.volatility_baseline[s] = 0.0
            if s not in self.ram_dna_cache: self.ram_dna_cache[s] = {"is_armed": True, "win_rate": 0.50}
            if s not in self.last_eval_time: self.last_eval_time[s] = 0.0 
            if s not in self.orderbook_snapshots: self.orderbook_snapshots[s] = {"best_bid": 0.0, "best_ask": 0.0} 

    def _safe_telegram_dispatch_sync(self, message: str, is_html: bool = True, message_type: str = "SUCCESS"):
        if not os.getenv("TELEGRAM_BOT_TOKEN") or len(os.getenv("TELEGRAM_BOT_TOKEN", "")) < 5: return
        try:
            self.telegram_queue.put_nowait((message, is_html, message_type))
        except asyncio.QueueFull:
            logger.warning("[X-RAY] ⚠️ Telegram queue full. Dropping telemetry to preserve HFT event loop.")

    async def _safe_telegram_dispatch(self, message: str, is_html: bool = True, message_type: str = "SUCCESS"):
        self._safe_telegram_dispatch_sync(message, is_html, message_type)

    async def run_telegram_worker(self):
        logger.info("📡 TELEGRAM WORKER ONLINE: Native async telemetry active.")
        while True:
            try:
                message, is_html, msg_type = await self.telegram_queue.get()
                if not os.getenv("TELEGRAM_BOT_TOKEN"):
                    self.telegram_queue.task_done()
                    continue

                if is_html:
                    await self.telegram.send_html_report(message)
                else:
                    await self.telegram.log_message(message, msg_type)
                    
                self.telegram_queue.task_done()
                
            except asyncio.CancelledError: break
            except Exception as e:
                logger.debug(f"Telegram dispatch failed: {e}")
                self.telegram_queue.task_done()

    async def run_correlation_engine(self):
        logger.info("🧠 CORRELATION ENGINE ONLINE: 15s High-Frequency Covariance Tracking.")
        while True:
            await asyncio.sleep(15.0)
            if not self.fsm.can_execute_trades: continue
            try:
                price_histories = {}
                for sym, mem in self.screener_memory.items():
                    if len(mem.get("prices", [])) >= 60:
                        price_histories[sym] = list(mem["prices"])[-60:]
                
                if price_histories:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(self.thread_pool, self.risk_vault.update_correlation_matrix, price_histories)
            except Exception as e:
                logger.debug(f"[X-RAY] Fast Correlation Matrix update failed: {e}")

    async def synchronize_exchange_state(self):
        try:
            open_orders_res = await self.executor.safe_call("GET", "/v5/order/realtime", category="linear", settleCoin="USDT")
            open_orders = open_orders_res.get("result", {}).get("list", [])
            if open_orders:
                logger.warning(f"🧹 BOOT ORPHAN RECONCILIATION: Found {len(open_orders)} resting orders. Sweeping...")
                for o in open_orders:
                    await self.executor.safe_call("POST", "/v5/order/cancel", is_execution=True, category="linear", symbol=o["symbol"], orderId=o["orderId"])

            pos_response = await self.executor.safe_call("GET", "/v5/position/list", category="linear", settleCoin="USDT")
            active_orphans = [p for p in pos_response.get("result", {}).get("list", []) if float(p.get("size", 0.0)) > 0]
            if not active_orphans: return
            logger.critical(f"⚠️ RECOVERY ENGAGED: Found {len(active_orphans)} active trades left open.")
            
            for pos in active_orphans:
                symbol = pos["symbol"]
                self._initialize_symbol_structures([symbol]) 
                qty, entry_price = float(pos["size"]), float(pos["avgPrice"])
                direction = "BUY" if pos["side"].upper() == "BUY" else "SELL"
                atr = entry_price * 0.015 
                
                self.state_actor.dispatch(symbol, "REGISTER_POSITION", {"direction": direction, "notional": qty * entry_price})
                risk_matrix = {"allocated_value_usdt": qty * entry_price, "size": qty, "arrival_price": entry_price}
                
                sig_id = str(uuid.uuid4())
                
                self.daemon_tasks[symbol] = self.track_task(self._position_lifecycle_daemon(
                    symbol, sig_id, direction, entry_price, atr, risk_matrix, 2, "RANGING"
                ))
        except Exception as e: logger.error(f"[X-RAY] Failed synchronizing exchange state: {e}", exc_info=True)

    async def run_fast_state_invariant_reconciliation(self):
        logger.info("🛡️ 5000ms STATE INVARIANT RECONCILIATION DAEMON ONLINE.")
        while True:
            await asyncio.sleep(5.0)
            try:
                now = time.time()
                expired_flights = [sym for sym, exp in self.in_flight_symbols.items() if now > exp]
                for sym in expired_flights:
                    self.in_flight_symbols.pop(sym, None)
                    logger.warning(f"[X-RAY] 🧹 IN-FLIGHT TTL EXPIRED: Purged phantom lock for {sym}")

                pos_response = await self.executor.safe_call("GET", "/v5/position/list", category="linear", settleCoin="USDT")
                if pos_response.get("retCode") != 0: continue

                active_on_exchange = {p["symbol"]: p for p in pos_response.get("result", {}).get("list", []) if float(p.get("size", 0.0)) > 0}

                # 1. Prune desynchronized local locks
                for tracked_sym in list(self.active_positions_map.keys()):
                    if tracked_sym not in active_on_exchange and tracked_sym not in self.in_flight_symbols:
                        if not (daemon := self.daemon_tasks.get(tracked_sym)) or daemon.done():
                            logger.warning(f"[X-RAY] 🧹 INVARIANT ENFORCED: Purging desynchronized lock for {tracked_sym}")
                            self.state_actor.dispatch(tracked_sym, "LIQUIDATE_POSITION", {"direction": "NONE"})
                            
                # Bidirectional Orphan Adoption
                for ex_sym, pos_data in active_on_exchange.items():
                    if ex_sym not in self.active_positions_map and ex_sym not in self.in_flight_symbols:
                        logger.critical(f"[X-RAY] 🛡️ ORPHAN ADOPTED: Found untracked position for {ex_sym}. Adopting into matrix.")
                        qty = float(pos_data["size"])
                        entry_price = float(pos_data.get("avgPrice", pos_data.get("markPrice", 0.0)))
                        direction = "BUY" if pos_data["side"].upper() == "BUY" else "SELL"
                        atr = entry_price * 0.015 
                        
                        if ex_sym not in self.stat_engines:
                            self._initialize_symbol_structures([ex_sym])
                            
                        self.state_actor.dispatch(ex_sym, "REGISTER_POSITION", {"direction": direction, "notional": qty * entry_price})
                        risk_matrix = {"allocated_value_usdt": qty * entry_price, "size": qty, "arrival_price": entry_price}
                        
                        sig_id = str(uuid.uuid4())
                        
                        self.daemon_tasks[ex_sym] = self.track_task(self._position_lifecycle_daemon(
                            ex_sym, sig_id, direction, entry_price, atr, risk_matrix, 2, "RANGING"
                        ))

            except Exception as e: logger.debug(f"[X-RAY] Fast invariant sync tick bypassed: {e}")

    async def run_system_heartbeat(self):
        start_time = time.time()
        loop_counter = 0
        loop = asyncio.get_running_loop()
        
        while True:
            await asyncio.sleep(60) 
            loop_counter += 1
            uptime_hours = (time.time() - start_time) / 3600

            if loop_counter % 5 == 0:
                self.global_state_cache["last_updated"] = time.time()
                await self._save_sgd_state()
                
                try: current_vault_balance = await self.executor.get_wallet_balance_usdt()
                except Exception as e: 
                    logger.debug(f"[X-RAY] Heartbeat true equity fetch failed: {e}", exc_info=True)
                    continue

                if "wallet_baseline" not in self.global_state_cache: 
                    self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01)

                if "lifetime_initial_balance" not in self.global_state_cache:
                    self.global_state_cache["lifetime_initial_balance"] = max(current_vault_balance, 0.01)

                now_utc = datetime.datetime.now(datetime.timezone.utc)
                current_day = now_utc.strftime("%Y-%m-%d")
                if self.global_state_cache.get("current_day") != current_day:
                    self.global_state_cache["current_day"] = current_day
                    self.global_state_cache["start_of_day_balance"] = current_vault_balance

                today_start_iso = now_utc.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                
                try:
                    execution_stats = await self.memory.get_forensic_execution_summary(today_start_iso) if self.memory else {}
                except Exception as e: 
                    logger.debug(f"[X-RAY] Heartbeat DB forensic fetch failed: {e}", exc_info=True)
                    execution_stats = {} 

                execution_stats["rolling_pnl_array"] = list(self.recent_pnl_history) if self.recent_pnl_history else [0.0]

                actual_net_pnl = current_vault_balance - self.global_state_cache["lifetime_initial_balance"]
                baseline = self.global_state_cache["wallet_baseline"]
                
                if current_vault_balance > baseline:
                    self.global_state_cache["wallet_baseline"] = current_vault_balance
                    baseline = current_vault_balance
                    
                drawdown_pct = max(0.0, (baseline - current_vault_balance) / baseline)
                
                if drawdown_pct >= 0.05 and len(self.active_positions_map) > 0:
                    self.fsm.trigger_global_emergency_lock()
                    self._safe_telegram_dispatch_sync(f"🚨 <b>EMERGENCY DRAWDOWN BREAKER TRIPPED</b>\nDrawdown: {drawdown_pct:.2%}. Engine shutting down.", is_html=True)
                    raise EmergencyShutdown(f"Drawdown limit reached: {drawdown_pct:.2%}")
                
                filled_blocks = min(10, int(drawdown_pct * 100))
                dd_bar = "🟢" * (10 - filled_blocks) + "🔴" * filled_blocks
                self.global_state_cache.update({"drawdown_bar": dd_bar, "actual_net_pnl": actual_net_pnl, "current_vault_balance": current_vault_balance, "drawdown_pct": drawdown_pct})

            if loop_counter % 10 == 0:
                cv = self.global_state_cache.get("current_vault_balance", 0.0)
                actual = self.global_state_cache.get("actual_net_pnl", 0.0)
                dd = self.global_state_cache.get("drawdown_pct", 0.0)
                dd_bar = self.global_state_cache.get("drawdown_bar", "🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢")
                live_count = len(self.asset_basket)
                shadow_count = len(self.shadow_basket)

                report = self.telegram.format_mission_control_dashboard(uptime_hours, live_count, shadow_count, cv, actual, dd, dd_bar, execution_stats)
                self._safe_telegram_dispatch_sync(report, is_html=True)

    async def handle_incoming_orderbook_tick(self, rich_payload: Dict[str, Any]):
        symbol = rich_payload.get("symbol")
        if not symbol or (symbol not in self.asset_basket and symbol not in self.shadow_basket): return

        async with self.symbol_locks[symbol]:
            self.screener_memory[symbol]["last_update_time"] = time.time()
            self.orderbook_snapshots[symbol] = rich_payload 
            
            stat_engine = self.stat_engines.get(symbol)
            if stat_engine:
                stat_engine.update_orderbook_pressure(
                    rich_payload["best_bid"], rich_payload["bid_vol"],
                    rich_payload["best_ask"], rich_payload["ask_vol"]
                )

            now = time.time()
            if now - self.last_eval_time.get(symbol + "_eval_throttle", 0.0) < 0.1: return
            self.last_eval_time[symbol + "_eval_throttle"] = now

            self.track_task(self._eval_gate(symbol, rich_payload, stat_engine, now))

    async def handle_incoming_trade(self, trade_data: Dict[str, Any]):
        symbol = trade_data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return
        
        now = time.time()
        price = float(trade_data.get("price", 0.0))
        if price < 0.000001: return
        
        volume = float(trade_data.get("size", 0.0))
        is_buy = str(trade_data.get("side", "")).upper() == "BUY"
        exchange_timestamp = float(trade_data.get("timestamp", now * 1000)) / 1000.0
        
        async with self.symbol_locks[symbol]:
            self.tick_history[symbol].append((exchange_timestamp, price))
            
            stat_engine = self.stat_engines.get(symbol)
            if stat_engine:
                stat_engine.update_trades(price, exchange_timestamp, volume, is_buy)

    async def _eval_gate(self, symbol: str, ob_payload: dict, stat_engine: ContinuousMicrostructureEngine, now: float):
        async with self.circuit_breaker_lock:
            if self.circuit_breakers.get(symbol, 0.0) > now or self.circuit_breakers.get("GLOBAL_MAINTENANCE", 0.0) > now: return
            
        if self.fsm.is_emergency_locked() or self.fsm.is_asset_locked(symbol): return
        
        # Enforce minimum 50-tick warm-up on hot-swapped symbols
        if not stat_engine or len(stat_engine.tick_prices) < 50:
            return

        # Early fast check to skip heavy math if already active
        is_active_fast = symbol in self.active_positions_map or symbol in self.in_flight_symbols
        if is_active_fast:
            if now - self.last_eval_time.get(symbol + "_learning_throttle", 0.0) < 1.0:
                return
            self.last_eval_time[symbol + "_learning_throttle"] = now

        try:
            price = ob_payload["micro_price"]
            log_mlofi_z = ob_payload["log_mlofi_z"]

            cluster_returns = {}
            for s, engine in self.stat_engines.items():
                if len(engine.tick_prices) >= 60:
                    arr = np.array(engine.tick_prices)
                    cluster_returns[s] = np.diff(np.log(arr + 1e-9)).tolist()

            sector_impulse, _ = await SectorEigenOracle.compute_sector_impulse(symbol, cluster_returns)

            btc_ticks = list(self.tick_history.get("BTCUSDT", []))
            alt_ticks = list(self.tick_history.get(symbol, []))
            tensor_alpha = await CrossAssetTensorOracle.compute_lead_lag_signal(symbol, "BTCUSDT", btc_ticks, alt_ticks)

            feature_engine = self.feature_engines.get(symbol)
            atr = feature_engine.get_computed_atr() if feature_engine else (price * 0.005)
            
            sl_dist_pct = max((atr * self.live_params.get("sl_atr_mult", 2.5)) / (price + 1e-9), 0.015)
            dynamic_rr = feature_engine.get_dynamic_rr_ratio() if feature_engine else self.live_params.get("rr_ratio", 2.0)
            tp_dist_pct = sl_dist_pct * dynamic_rr

            state = stat_engine.extract_statistical_state(
                current_price=price, log_mlofi_z=log_mlofi_z, hawkes_z=stat_engine.rough_hawkes_z,
                sector_impulse=sector_impulse, sl_dist_pct=sl_dist_pct, tp_dist_pct=tp_dist_pct,
                exchange_timestamp=ob_payload["timestamp"]
            )
            
            # If already active, we just update context for the lifecycle daemon and exit
            if is_active_fast:
                if symbol in self.active_contexts:
                    self.active_contexts[symbol]["latest_state"] = state
                return

            prob_success = max(state["p_up"], state["p_down"])
            action = state["action_dir"]
            
            dyn_gate = state.get("dynamic_gate", 0.54)
            if prob_success < dyn_gate: return
            
            dna_stats = self.ram_dna_cache.get(symbol, {"is_armed": True, "win_rate": 0.50})
            if not dna_stats.get("is_armed", True): return

            fusion_engine = self.entry_matrices.get(symbol)
            if not fusion_engine: return
            
            btc_mlofi = self.stream_feed_instance.log_mlofi_z.get("BTCUSDT", 0.0) if self.stream_feed_instance else 0.0
            eth_mlofi = self.stream_feed_instance.log_mlofi_z.get("ETHUSDT", 0.0) if self.stream_feed_instance else 0.0
            
            fusion_verdict = fusion_engine.fuse_signal_probability(
                symbol, prob_success, action, ob_payload["bids"], ob_payload["asks"]
            )
            exec_weight = fusion_verdict["execution_weight"]

            if exec_weight < 0.5: return 

            current_bal = self.global_state_cache.get("current_vault_balance", 21.0)
            
            target_notional = self.sor.calculate_risk_adjusted_notional(
                prob_success, exec_weight, sl_dist_pct, tp_dist_pct, current_bal, stat_engine.inst_variance
            )

            is_safe, risk_reason = self.risk_vault.evaluate_portfolio_safety(current_bal, target_notional, symbol)
            if not is_safe: return
            
            # 🚀 V25.6 AUDIT FIX: Atomic reservation to prevent race-condition double entries
            async with self.circuit_breaker_lock:
                if symbol in self.active_positions_map or symbol in self.in_flight_symbols:
                    return
                # Optimistic local lock before dispatch across the event loop
                self.in_flight_symbols[symbol] = time.time() + 60.0

            self.state_actor.dispatch(symbol, "RESERVE_IN_FLIGHT", {})
            logger.critical(f"🚀 TENSOR ALPHA DETECTED // {symbol} {action} | Prob: {prob_success:.2%} | ExecWt: {exec_weight:.2f}x | Size: ${target_notional:.2f}")

            try:
                await self.executor.adjust_leverage(symbol, 2)
            except Exception as e:
                logger.warning(f"[X-RAY] ⚠️ Leverage adjustment bypassed for {symbol}: {e}")

            dominant_regime = state.get("dominant_regime", "TRENDING")

            # DIRECT-DRIVE ROUTING
            success, arrival_price, f_price = await self.sor.execute_alpha_signal(
                symbol=symbol, direction=action, prob_success=prob_success, exec_weight=exec_weight,
                current_mid_price=price, sl_price=(price * (1-sl_dist_pct) if action=="BUY" else price * (1+sl_dist_pct)),
                tp_price=(price * (1+tp_dist_pct) if action=="BUY" else price * (1-tp_dist_pct)),
                inst_var=stat_engine.inst_variance, depth_snapshot=ob_payload, regime=dominant_regime
            )

            if not success or f_price <= 0:
                self.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})
                return

            actual_qty_filled = target_notional / f_price
            
            # Valid UUID string for Supabase correctly unified
            sig_id = str(uuid.uuid4())

            if stat_engine and hasattr(stat_engine, 'pending_trade_outcomes'):
                stat_engine.pending_trade_outcomes[sig_id] = {
                    "action": action,
                    "features": state.get("raw_features", np.zeros(18)),
                    "p_up": state.get("p_up", 0.5),
                    "beliefs": [
                        state.get("markov_beliefs", {}).get("trend", 0.25), 
                        state.get("markov_beliefs", {}).get("range", 0.25), 
                        state.get("markov_beliefs", {}).get("disloc", 0.25), 
                        state.get("markov_beliefs", {}).get("cascade", 0.25)
                    ]
                }

            safe_features = {
                "symbol": symbol, "market_regime": dominant_regime, 
                "virtual_sl": state["virtual_sl"], "virtual_tp": state["virtual_tp"], 
                "log_mlofi_z": log_mlofi_z, "hawkes_z": stat_engine.rough_hawkes_z,
                "sector_impulse": sector_impulse, "bid_ask_spread": 0.001
            }

            if self.memory:
                await self.memory.commit_prediction(
                    sig_id,  # EXACT MATCH: Prevents orphaned PnL resolution
                    time.time(), price, action, prob_success, safe_features, False
                )

            ticket_msg = self.telegram.format_entry_ticket(
                symbol, action, f_price, actual_qty_filled, 0.0, 
                (target_notional / current_bal), dominant_regime, safe_features
            )
            self.track_task(self._safe_telegram_dispatch(ticket_msg, is_html=True))

            self.daemon_tasks[symbol] = self.track_task(self._position_lifecycle_daemon(
                symbol, sig_id, action, f_price, atr, 
                {"allocated_value_usdt": target_notional, "size": actual_qty_filled, "arrival_price": arrival_price}, 
                2, "TRENDING", realigned_tp=(price * (1+tp_dist_pct) if action=="BUY" else price * (1-tp_dist_pct)),
                dynamic_rr_ratio=dynamic_rr, realigned_sl=(price * (1-sl_dist_pct) if action=="BUY" else price * (1+sl_dist_pct))
            ))

        except Exception as e:
            logger.error(f"[X-RAY] Trade evaluation fault for {symbol}: {e}", exc_info=True)
            self.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})

    def log_to_wal_sync(self, action_type: str, args: list):
        if not self.memory: return
        
        def _sync_db_call():
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try:
                if action_type == "prediction":
                    new_loop.run_until_complete(self.memory.commit_prediction(*args))
                elif action_type == "settlement":
                    new_loop.run_until_complete(self.memory.log_live_execution_result(*args))
            except Exception as e:
                logger.error(f"[X-RAY] DB Offload Fault: {e}")
            finally:
                new_loop.close()

        loop = asyncio.get_running_loop()
        loop.run_in_executor(self.thread_pool, _sync_db_call)

    async def run_dna_prewarmer(self):
        logger.info("🔥 RAM PRE-WARMER ONLINE.")
        while True:
            try:
                await asyncio.wait_for(self.force_dna_refresh.wait(), timeout=300.0)
                self.force_dna_refresh.clear()
            except asyncio.TimeoutError: pass
            
            try:
                async def _safe_fetch(sym, dna):
                    try:
                        if not self.memory: return {"is_armed": True, "win_rate": 0.50}
                        async with self.db_semaphore:
                            res = await asyncio.wait_for(self.memory.compute_latent_dna_edge(dna, 30), timeout=2.0)
                            return res
                    except Exception: return {"is_armed": True, "win_rate": 0.50} 

                fetch_tasks = {}
                for sym in list(self.asset_basket):
                    engine = self.stat_engines.get(sym)
                    fetch_tasks[sym] = _safe_fetch(sym, {
                        "vol_mult": self.screener_metrics.get(sym, {}).get("vol_mult", 1.0), 
                        "log_mlofi_z": engine.clean_ofi_z if engine else 0.0, 
                        "spread_pct": 0.001, 
                        "symbol": sym
                    })

                if not fetch_tasks: continue
                results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
                
                for sym, result in zip(list(fetch_tasks.keys()), results):
                    if isinstance(result, Exception): self.ram_dna_cache[sym] = {"is_armed": True, "win_rate": 0.50}
                    else: self.ram_dna_cache[sym] = result
            except Exception as e: logger.error(f"[X-RAY] DNA Prewarmer error: {e}")

    async def run_shadow_resolution_daemon(self):
        logger.info("👻 GHOST FORENSICS ONLINE.")
        interval_mins = float(self.timeframe)
        while True:
            await asyncio.sleep(300) 
            try:
                active_syms = list(self.active_positions_map.keys())
                current_prices = {sym: {"prices": list(self.screener_memory[sym]["prices"]), "highs": list(self.screener_memory[sym].get("highs", [])), "lows": list(self.screener_memory[sym].get("highs", []))} for sym in self.asset_basket + self.shadow_basket if self.screener_memory.get(sym) and self.screener_memory[sym].get("prices") and sym not in active_syms}
                
                if current_prices:
                    async with self.db_semaphore:
                        try: 
                            if self.memory:
                                await asyncio.wait_for(
                                    self.memory.resolve_batch_historical_predictions(list(current_prices.keys()), current_prices, 60.0, interval_mins), 
                                    timeout=15.0
                                )
                        except Exception as e: logger.debug(f"[X-RAY] Shadow resolution timeout: {e}")
            except Exception as e: logger.error(f"[X-RAY] Shadow resolution error: {e}")

    async def run_omni_swarm_director(self):
        logger.info("🌪️ OMNI-SWARM DIRECTOR ONLINE.")
        banned_keywords = ["AAPL", "TSLA", "NVDA", "AMZN", "MSFT", "GOOG", "META", "SOXL", "SPCX", "SKHY", "SNDK", "BANK", "MUUSDT", "BEAT", "MSTR", "ESPUSDT", "DEXE", "PUMP", "EUL", "XAU", "XAG", "USDC", "CLUSDT", "SSPCUSDT"]
        while True:
            await asyncio.sleep(15) 
            try:
                protected_symbols = set(self.active_positions_map.keys()) | set(self.in_flight_symbols.keys())
                dead_sym, hot_sym = await self.omni_scanner.scan_and_rank_universe(self.asset_basket, protected_symbols=protected_symbols)
                
                if dead_sym and hot_sym and not any(b in hot_sym for b in banned_keywords):
                    tick_res = await self.executor.safe_call("GET", "/v5/market/tickers", category="linear", symbol=hot_sym)
                    if tick_res.get("retCode") == 0 and tick_res.get("result", {}).get("list"):
                        t_data = tick_res["result"]["list"][0]
                        bid, ask, turnover = float(t_data.get("bid1Price", 0.0) or 0.0), float(t_data.get("ask1Price", 0.0) or 0.0), float(t_data.get("turnover24h", 0.0) or 0.0)
                        spread_bps = ((ask - bid) / bid) * 10000.0 if bid > 0 else 999.0
                        
                        if bid > 0 and ask > bid and turnover >= 15_000_000.0 and spread_bps <= 4.0:
                            if dead_sym in self.asset_basket: self.asset_basket.remove(dead_sym)
                            if hot_sym not in self.asset_basket: self.asset_basket.append(hot_sym)
                            self._initialize_symbol_structures([hot_sym])
                            await self._prune_dead_symbols() 
                            if self.stream_feed_instance and hasattr(self.stream_feed_instance, 'hot_swap_socket_stream'):
                                await self.stream_feed_instance.hot_swap_socket_stream(dead_sym, hot_sym)
                            logger.critical(f"[X-RAY] 🚀 DYNAMIC SWAP // {hot_sym} injected into matrix (Replaced {dead_sym}).")
            except Exception as e: logger.error(f"[X-RAY] Omni-Swarm Director error: {e}")

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
            logger.info("🌌 V25.0 MATRIX REFRESH: Probing High-Velocity Universe...")
            await self.sor._fetch_exchange_limits("BTCUSDT") # Warmup
            
            dynamic_basket = await self.executor.get_top_volatile_assets(limit=35, min_turnover=15_000_000.0)
            if not dynamic_basket or len(dynamic_basket) < 15:
                dynamic_basket = await self.executor.get_top_volatile_assets(limit=35, min_turnover=5_000_000.0)

            banned = ["AAPL", "TSLA", "NVDA", "AMZN", "MSFT", "GOOG", "META"]
            dynamic_basket = [s for s in dynamic_basket if not any(b in s for b in banned)]

            self.asset_basket = dynamic_basket[:15]
            self.shadow_basket = dynamic_basket[15:25] if len(dynamic_basket) >= 25 else dynamic_basket[15:]
                
            await self._prune_dead_symbols() 
            self._initialize_symbol_structures(self.asset_basket + self.shadow_basket)
            self.force_dna_refresh.set() 
            logger.info(f"✅ V25.0 MATRIX REFRESHED: {len(self.asset_basket)} Live Slots | {len(self.shadow_basket)} Shadow Slots Active.")
        except Exception as e:
            logger.error(f"[X-RAY] Universe refresher error: {e}")

    async def _universe_refresher_loop(self):
        while True:
            await asyncio.sleep(900)
            await self.run_universe_refresher()

    async def stream_manager_loop(self):
        while True:
            stream_feed = MarketStateMatrix(
                basket=self.asset_basket + self.shadow_basket[:10],
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

    # ==============================================================================
    # 🚀 V25.6 CENTRALIZED LIFECYCLE DAEMON
    # ==============================================================================

    async def _state_verify_entry(self, ctx: dict) -> str:
        for _ in range(5):  
            await asyncio.sleep(3)
            try:
                pos_res = await self.executor.safe_call("GET", "/v5/position/list", category="linear", symbol=ctx["symbol"])
                pos_data = pos_res.get("result", {}).get("list", [])
                if pos_data and float(pos_data[0].get("size", 0.0)) > 0:
                    ctx["actual_entry"] = float(pos_data[0].get("avgPrice", ctx["current_price"]))
                    ctx["actual_qty_filled"] = float(pos_data[0].get("size", ctx["actual_qty_filled"]))
                    
                    if "historical_favorable_price" in ctx:
                        ctx["max_favorable_price"] = ctx["historical_favorable_price"]
                        ctx["highest_since_entry"] = ctx["historical_favorable_price"] if ctx["is_buy"] else ctx["actual_entry"]
                        ctx["lowest_since_entry"] = ctx["historical_favorable_price"] if not ctx["is_buy"] else ctx["actual_entry"]
                    else:
                        ctx["highest_since_entry"] = ctx["actual_entry"] if ctx["is_buy"] else None
                        ctx["lowest_since_entry"] = ctx["actual_entry"] if not ctx["is_buy"] else None
                        ctx["max_favorable_price"] = ctx["actual_entry"]
                    
                    return "INITIALIZE_STOPS"
            except Exception as e: 
                logger.debug(f"[X-RAY] Position fill check failed for {ctx['symbol']}: {e}")

        try: await self.executor.safe_call("POST", "/v5/order/cancel-all", is_execution=True, category="linear", symbol=ctx["symbol"])
        except Exception: pass
        return "ABORT"

    async def _state_settle_trade(self, ctx: dict):
        symbol, actual_entry, target_leverage = ctx["symbol"], ctx["actual_entry"], ctx["target_leverage"]
        
        for _ in range(6):
            pos_res = await self.executor.safe_call("GET", "/v5/position/list", category="linear", symbol=symbol)
            pos_list = pos_res.get("result", {}).get("list", [])
            if not pos_list or float(pos_list[0].get("size", 0.0)) <= 0:
                break
            else:
                remaining_qty = float(pos_list[0].get("size", 0.0))
                side = "Sell" if pos_list[0]["side"] == "Buy" else "Buy"
                
                try:
                    qty_step = ctx.get("qty_step", 0.1)
                    precision = max(0, abs(int(math.floor(math.log10(qty_step)))))
                    qty_str = f"{remaining_qty:.{precision}f}"
                except Exception:
                    qty_str = str(remaining_qty)
                    
                await self.executor.safe_call(
                    "POST", "/v5/order/create", is_execution=True,
                    category="linear", symbol=symbol, side=side,
                    orderType="Market", qty=qty_str, timeInForce="IOC", reduceOnly=True
                )
                await asyncio.sleep(1.0)

        net_pnl, real_outcome, slippage_bps, fees, exit_price = 0.0, "RECONCILED", 0.0, 0.0, actual_entry
        
        for _ in range(8):
            await asyncio.sleep(2.5)
            try: 
                closed_data = await self.executor.safe_call("GET", "/v5/position/closed-pnl", category="linear", symbol=symbol, limit=10)
                closed_list = closed_data.get("result", {}).get("list", [])
                
                valid_close = None
                for pnl_record in closed_list:
                    if (time.time() - float(pnl_record.get("updatedTime", 0)) / 1000.0) < 300.0:
                        valid_close = pnl_record
                        break
                
                if valid_close:
                    net_pnl = float(valid_close.get("closedPnl", 0.0))
                    real_outcome = "PROFIT" if net_pnl > 0 else "LOSS"
                    
                    fees = float(valid_close.get("execFee", 0.0))
                    exit_price = float(valid_close.get("avgExitPrice", actual_entry))
                    break
            except Exception as e: 
                logger.debug(f"[X-RAY] Closed PnL polling retry for {symbol}: {e}")
                
        arrival_price = ctx.get("arrival_price", actual_entry)
        trigger_price = ctx.get("exit_trigger_price", exit_price)

        if ctx["is_buy"]:
            entry_slip = ((actual_entry - arrival_price) / (arrival_price + 1e-9)) * 10000.0
            exit_slip = ((trigger_price - exit_price) / (trigger_price + 1e-9)) * 10000.0
        else:
            entry_slip = ((arrival_price - actual_entry) / (arrival_price + 1e-9)) * 10000.0
            exit_slip = ((exit_price - trigger_price) / (trigger_price + 1e-9)) * 10000.0
            
        slippage_bps = max(-500.0, min(500.0, entry_slip + exit_slip))
        
        ctx["exec_details"]["tca_entry_slippage_bps"] = entry_slip
        ctx["exec_details"]["tca_exit_slippage_bps"] = exit_slip
        ctx["exec_details"]["tca_total_slippage_bps"] = slippage_bps
        ctx["exec_details"]["fees_usdt"] = fees
        
        self.recent_pnl_history.append(net_pnl)
        
        current_cached = self.global_state_cache.get("current_vault_balance", 21.0)
        self.global_state_cache["current_vault_balance"] = current_cached + net_pnl
        
        duration_mins = (time.time() - ctx["daemon_start_time"]) / 60.0
        
        if self.memory:
            await self.memory.log_live_execution_result(ctx["signal_id"], net_pnl, slippage_bps, real_outcome, ctx["exec_details"])
            
        # Plumb Realized PnL Back to RLS Filters
        if ctx.get("stat_engine") and hasattr(ctx["stat_engine"], "resolve_trade_outcome"):
            ctx["stat_engine"].resolve_trade_outcome(ctx["signal_id"], net_pnl)
            
        self._safe_telegram_dispatch_sync(self.telegram.format_execution_receipt(symbol, net_pnl, slippage_bps, fees, duration_mins, net_pnl > 0), is_html=True)
        self.state_actor.dispatch(symbol, "LIQUIDATE_POSITION", {"direction": ctx["direction"]})

    async def _position_lifecycle_daemon(
        self, symbol: str, signal_id: str, direction: str, current_price: float, atr: float, 
        risk_matrix: dict, target_leverage: int = 2, market_regime: str = "TRENDING", 
        is_recovery: bool = False, realigned_tp: float = None, dynamic_rr_ratio: float = 2.0, 
        realigned_sl: float = None, historical_favorable_price: float = None
    ):
        ctx = {
            "symbol": symbol, "signal_id": signal_id, "direction": direction, "is_buy": direction == "BUY",
            "current_price": current_price, "atr": atr, "target_leverage": target_leverage,
            "arrival_price": risk_matrix.get("arrival_price", current_price),
            "actual_entry": current_price,  
            "actual_qty_filled": risk_matrix.get("size", 1.0),
            "regime": market_regime, "daemon_start_time": time.time(),
            "qty_step": risk_matrix.get("qty_step", 0.1), 
            "stat_engine": self.stat_engines.get(symbol),
            "last_ob": {},
            "latest_tick_price": current_price,
            "current_vault_balance": self.global_state_cache.get("current_vault_balance", 21.0),
            "drawdown_pct": self.global_state_cache.get("drawdown_pct", 0.0),
            "active_positions_count": len(self.active_positions_map),
            "payload_features": {},
            "exec_details": {}
        }

        async with self.execution_semaphore:
            self.state_actor.dispatch(symbol, "RESERVE_IN_FLIGHT", {})
            loop_state = await self._state_verify_entry(ctx)
            if loop_state == "ABORT":
                self.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})
                return
            
            self.state_actor.dispatch(symbol, "REGISTER_POSITION", {"direction": direction, "notional": ctx["actual_qty_filled"] * ctx["actual_entry"]})

        if symbol not in self.exit_states:
            self.exit_states[symbol] = PositionExitState(
                position_id=signal_id, entry_time=time.time(), entry_price=ctx["actual_entry"],
                exit_side="Sell" if direction == "BUY" else "Buy", entry_balance=ctx["current_vault_balance"], 
                actual_qty=ctx["actual_qty_filled"], base_qty=ctx["actual_qty_filled"]
            )

        state = self.exit_states[symbol]
        self.active_contexts[symbol] = ctx

        if self.test_mode:
            self.state_actor.dispatch(symbol, "LIQUIDATE_POSITION", {"direction": ctx["direction"]})
            return

        try:
            actual_sl_distance = max(ctx["atr"] * self.live_params.get("sl_atr_mult", 2.5), ctx["actual_entry"] * 0.020)
            safe_rr = min(2.0, dynamic_rr_ratio)
            
            initial_sl = ctx["actual_entry"] - actual_sl_distance if ctx["is_buy"] else ctx["actual_entry"] + actual_sl_distance
            initial_tp = ctx["actual_entry"] + (actual_sl_distance * safe_rr) if ctx["is_buy"] else ctx["actual_entry"] - (actual_sl_distance * safe_rr)
            
            await self.sor._amend_trailing_stop(symbol, initial_sl, initial_tp)

            current_active_sl = initial_sl
            current_active_tp = initial_tp

            loop_state = "ACTIVE_MONITORING"

            while loop_state == "ACTIVE_MONITORING":
                await asyncio.sleep(0.050)

                current_price = ctx["latest_tick_price"]
                time_since_eval = time.time() - ctx.get("last_eval_time", 0)
                if current_price == ctx.get("last_eval_price") and time_since_eval < 1.0:
                    continue
                    
                ctx["last_eval_price"] = current_price
                ctx["last_eval_time"] = time.time()

                ob = self.orderbook_snapshots.get(symbol, {})
                best_bid = ob.get("best_bid", current_price)
                best_ask = ob.get("best_ask", current_price)

                ctx["safe_c_price"] = best_bid if ctx["is_buy"] else best_ask
                if ctx["stat_engine"] and ctx["stat_engine"].true_micro_price > 0:
                    ctx["safe_c_price"] = ctx["stat_engine"].true_micro_price

                ctx["now"] = time.time()
                ctx["current_vault_balance"] = self.global_state_cache.get("current_vault_balance", ctx["current_vault_balance"])
                ctx["drawdown_pct"] = self.global_state_cache.get("drawdown_pct", 0.0)
                ctx["active_positions_count"] = len(self.active_positions_map)
                ctx["last_ob"] = ob
                ctx["initial_risk_dist"] = abs(ctx["actual_entry"] - initial_sl)
                ctx["current_sl"] = current_active_sl
                ctx["current_tp"] = current_active_tp

                # Delegating to AdvancedIntelligentExitMatrix
                decision = IntelligentExitEngine.evaluate(ctx, state)
                
                target_sl = decision.exchange_ts_price
                target_tp = decision.dynamic_tp_price

                if target_sl > 0 and target_tp > 0:
                    # Prevent rate limit spam: Only amend if SL moved by 15% ATR or TP compressed
                    if abs(target_sl - current_active_sl) > (ctx["atr"] * 0.15) or abs(target_tp - current_active_tp) > (ctx["atr"] * 0.05):
                        if await self.sor._amend_trailing_stop(symbol, target_sl, target_tp):
                            current_active_sl = target_sl
                            current_active_tp = target_tp
                            logger.info(f"[X-RAY] 🛡️ TRAILING SL/TP STEPPED // {symbol} SL: {current_active_sl:.4f} | TP: {current_active_tp:.4f}")

                if decision.action in ["EXIT", "CLOSE", "EMERGENCY"]:
                    logger.critical(f"[X-RAY] 🎯 POSITION TERMINATED // {symbol}: {decision.reason}")
                    ctx["exit_trigger_price"] = current_price
                    break

                await ExecutionGovernorFSM.manage_execution(decision, state, ctx, self.executor)

                if state.q_retained <= 0.01 and state.execution_state == "OBSERVE":
                    ctx["exit_trigger_price"] = current_price
                    break

            await self._state_settle_trade(ctx)

        except Exception as e:
            logger.error(f"[X-RAY] FSM Position daemon fault for {symbol}: {e}", exc_info=True)
        finally:
            self.state_actor.dispatch(symbol, "LIQUIDATE_POSITION", {"direction": ctx["direction"]})

    async def graceful_shutdown(self):
        logger.critical("🛑 INITIATING EMERGENCY FLATTEN & SHUTDOWN...")
        
        if hasattr(self, 'telegram'):
            try:
                await self.telegram.send_html_report("🚨 <b>CRITICAL: SYSTEM SHUTDOWN INITIATED</b>\nAll positions are being flattened and Matrix disconnected.")
            except Exception:
                pass
                
        symbols_to_cancel = list(self.active_positions_map.keys())
            
        for symbol in symbols_to_cancel:
            try: await self.executor.safe_call("POST", "/v5/order/cancel-all", is_execution=True, category="linear", symbol=symbol)
            except Exception as e: logger.error(f"[X-RAY] Cancel failed for {symbol}: {e}")
        
        for symbol in symbols_to_cancel:
            try:
                pos_res = await self.executor.safe_call("GET", "/v5/position/list", category="linear", symbol=symbol)
                pos_list = pos_res.get("result", {}).get("list", [])
                if pos_list and float(pos_list[0].get("size", 0.0)) > 0:
                    qty = float(pos_list[0]["size"])
                    side = "Sell" if pos_list[0]["side"] == "Buy" else "Buy"
                    current_p = float(pos_list[0].get("markPrice", pos_list[0].get("avgPrice", 0.0)))
                    await self._execute_emergency_escape(symbol, current_p, qty, side == "Sell")
            except Exception as e: logger.error(f"[X-RAY] Flatten failed for {symbol}: {e}")

        if hasattr(self, 'memory') and self.memory:
            await self.memory.flush_and_close()
            
        if hasattr(self, 'telegram'): await self.telegram.close()
        
        self.thread_pool.shutdown(wait=False)
        logger.critical("✅ MATRIX DISCONNECTED.")

    async def run_engine_forever(self):
        # 🚀 BOOT FIX: Fetch true balance and evaluate safety BEFORE releasing global lock
        try:
            boot_bal = await self._get_true_equity_usdt()
            self.global_state_cache["start_of_day_balance"] = boot_bal
            self.global_state_cache["wallet_baseline"] = max(boot_bal, 0.01)
            self.global_state_cache["lifetime_initial_balance"] = max(boot_bal, 0.01)
            self.global_state_cache["last_updated"] = time.time()
            self.global_state_cache["current_day"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
            
            # Evaluate vault safety on boot; if intraday loss or drawdown limits are hit, STAY locked!
            is_safe, reason = self.risk_vault.evaluate_portfolio_safety(boot_bal, 0.0, "")
            if not is_safe:
                self.fsm.trigger_global_emergency_lock()
                logger.critical(f"🚨 BOOT SAFETY LATCH ENGAGED: Vault rejected state on startup ({reason}). Swarm remains locked.")
            else:
                self.fsm.release_global_emergency_lock()
        except Exception as e:
            logger.warning(f"[X-RAY] Boot equity validation failed ({e}). Retaining conservative emergency lock.")
            self.fsm.trigger_global_emergency_lock()
        
        try: 
            await self.executor.connect_ws()
        except Exception as e:
            logger.error(f"[X-RAY] ⚠️ Failed to bind private WebSocket stream: {e}")
        
        try:
            try: 
                await self.executor.safe_call("POST", "/v5/position/switch-mode", is_execution=True, category="linear", coin="USDT", mode=0)
                logger.info("✅ Bybit Unified Account confirmed in One-Way Mode.")
            except Exception as e: 
                if "not modified" in str(e).lower() or "110025" in str(e): logger.info("✅ Bybit Unified Account already in One-Way Mode.")
        except Exception: pass

        if hasattr(self, 'memory') and self.memory:
            await self.memory.start()

        try: await self.sor._fetch_exchange_limits("BTCUSDT") # Initialize limits
        except Exception: pass
        try: await self.synchronize_exchange_state()
        except Exception: pass
        
        await self.run_universe_refresher()
        
        daemons = [
            self.state_actor.start,
            self.run_telegram_worker,
            self.run_dna_prewarmer, 
            self.stream_manager_loop, self.run_system_heartbeat, 
            self.run_shadow_resolution_daemon, self._universe_refresher_loop, 
            self.run_omni_swarm_director,            
            self.run_fast_state_invariant_reconciliation,
            self.yield_engine.run_yield_scanner_daemon,
            self.run_correlation_engine 
        ]
        
        tasks = [asyncio.create_task(safe_daemon_wrapper(d, self)) for d in daemons]
        
        while not self.fsm.is_emergency_locked():
            await asyncio.sleep(0.5)
            
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def main():
    engine = DistributedQuantEngine()
    try: 
        await engine.run_engine_forever()
    except EmergencyShutdown: 
        logger.critical("🛑 Engine halted via Emergency Breaker.")
    except asyncio.CancelledError: 
        pass
    finally: 
        await engine.graceful_shutdown()

if __name__ == "__main__":
    from keep_alive import keep_alive
    keep_alive()
    try: asyncio.run(main())
    except KeyboardInterrupt: sys.exit(0)