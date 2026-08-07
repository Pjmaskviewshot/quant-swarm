"""
💎 V62.0 ULTRA-APEX NEURAL: STOCHASTIC CONTROL & HAWKES ORCHESTRATOR
------------------------------------------------------------------------
Microstructure Execution Engine featuring Hawkes Process Self-Excitation 
Branching Ratio Analysis (η >= 0.85 Cascade Guards), HJB Optimal Stopping Exits,
Gram-Schmidt Feature Alignment, and Scale-Invariant Risk Adaptation (SIDNA).
"""

import os
import sys

# Ensure `src/` directory is prepended to sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

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

class EmergencyShutdown(Exception):
    """Custom exception to trigger safe, async-aware system shutdown."""
    pass

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

try:
    from portfolio.risk_vault import InstitutionalRiskVault
except ModuleNotFoundError:
    from portfolio.risk_manager import InstitutionalRiskVault

from execution.delta_neutral import DeltaNeutralYieldEngine 

# External Connectors
from ingestion.multi_feed import HighVelocityMultiFeed
from services.bybit_v5 import BybitUnifiedExecutor
from services.telegram_ops import AsyncTelegramReporter
from services.tensor_oracle import CrossAssetTensorOracle

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(name)s] - [%(levelname)s] - [%(message)s]', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("QUANT_CORE.V62.0_ULTRA_APEX")


class DistributedQuantEngine:
    def __init__(self):
        load_dotenv()
        self.test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
        
        if self.test_mode: 
            logger.critical("⚠️ TEST MODE: Paper Trading Armed.")
        else: 
            logger.critical("💎 LIVE MODE: V62.0 ULTRA-APEX NEURAL ACTIVE.")
        
        self.asset_basket: List[str] = []
        self.timeframe = os.getenv("TRADING_TIMEFRAME", "15")
        self.shadow_basket: List[str] = []
        
        self.db_semaphore = asyncio.Semaphore(5)
        self.eval_semaphore = asyncio.Semaphore(10)
        self.execution_semaphore = asyncio.Semaphore(5)
        
        self.wal_db_path = "quant_swarm_wal.db"
        self.wal_batch_queue = []
        self.wal_lock = asyncio.Lock()  
        
        self.tick_error_counts: Dict[str, List[float]] = {}
        self.circuit_breakers: Dict[str, float] = {}
        self.circuit_breaker_lock = asyncio.Lock() 
        
        self.stream_restart_event = asyncio.Event()
        self.force_dna_refresh = asyncio.Event() 
        
        # MICROSERVICES
        self.fsm = SystemStateMachine()
        
        try:
            self.memory = MemoryBank()
        except Exception as e:
            logger.error(f"[X-RAY] ⚠️ CLOUD DB OFFLINE: Supabase connection failed ({e}). Booting in Degraded Local-Only Mode.")
            self.memory = None 
            
        self.risk_vault = InstitutionalRiskVault(max_drawdown_pct=0.30, max_single_position_risk_pct=0.015)
        self.tensor_oracle = CrossAssetTensorOracle()
        self.auction_engine = CapitalAuctionEngine(self)
        self.yield_engine = DeltaNeutralYieldEngine(self)  
        
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
        self.hardware_min_qty: Dict[str, float] = {} 
        self.global_state_cache = {"last_updated": 0.0}
        self.live_params = self._load_live_params()
        self.last_socket_reconnect = 0.0 
        self.funding_rates, self.open_interests, self.spread_history = {}, {}, {}
        
        self.telegram = AsyncTelegramReporter(token=os.getenv("TELEGRAM_BOT_TOKEN"), chat_id=os.getenv("TELEGRAM_CHAT_ID"))
        self.executor = BybitUnifiedExecutor(api_key=os.getenv("BYBIT_API_KEY"), api_secret=os.getenv("BYBIT_API_SECRET"), testnet=self.test_mode, max_workers=12)
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
        if len(self._active_tasks) > 400:
            logger.critical("[X-RAY] 🛑 FATAL: TASK LIMIT EXCEEDED (>400). Dropping background task to prevent memory overflow.")
            dummy = asyncio.Future()
            dummy.set_result(None)
            return dummy
            
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
                    self.ram_dna_cache.pop(key, None)
                    self.screener_memory.pop(key, None)
                    self.screener_metrics.pop(key, None)
                    self.volatility_baseline.pop(key, None)
                    self.last_eval_time.pop(key, None)
                    self.tick_sizes.pop(key, None)
                    self.hardware_min_qty.pop(key, None)
                    self.funding_rates.pop(key, None)
                    self.open_interests.pop(key, None)
                    self.circuit_breakers.pop(key, None)
                    self.tick_error_counts.pop(key, None)

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
        try:
            if is_html:
                await self.telegram.send_html_report(message)
            else:
                await self.telegram.log_message(message, message_type)
        except Exception as e:
            logger.warning(f"[X-RAY] ⚠️ Telegram queue dispatch failed: {e}")

    async def _get_true_equity_usdt(self) -> float:
        return await self.executor.get_wallet_balance_usdt()

    async def _fetch_exchange_tick_sizes(self):
        try:
            info = await self.executor.safe_call(self.executor.client.get_instruments_info, category="linear")
            for item in info.get("result", {}).get("list", []):
                sym = item.get("symbol")
                self.tick_sizes[sym] = float(item.get("priceFilter", {}).get("tickSize", "0.0001"))
                self.hardware_min_qty[sym] = float(item.get("lotSizeFilter", {}).get("minOrderQty", "1.0"))
        except Exception as e: logger.error(f"[X-RAY] Failed fetching exchange info: {e}", exc_info=True)

    async def _get_max_affordable_notional(self):
        try: 
            raw_balance = await self.executor.get_wallet_balance_usdt()
            available_balance = max(1.0, raw_balance) 
        except Exception: 
            available_balance = 7.00
            
        if available_balance < 50.0:
            return max(6.50, available_balance * 0.35)
            
        min_exposure_ratio = 0.40 
        max_exposure_ratio = 0.10
        k_slope = 0.05
        mid_point = 100.0
        logistic_exp = min_exposure_ratio + (max_exposure_ratio - min_exposure_ratio) / (1.0 + math.exp(-k_slope * (available_balance - mid_point)))
        return max(6.50, available_balance * logistic_exp)

    def _align_price(self, symbol: str, price: float) -> str:
        tick_dec = Decimal(str(self.tick_sizes.get(symbol, 0.0001)))
        return str(Decimal(str(price)).quantize(tick_dec, rounding=ROUND_HALF_UP))

    async def _amend_trailing_stop(self, symbol: str, new_sl: float, new_tp: float) -> bool:
        try:
            await self.executor.safe_call(
                self.executor.client.set_trading_stop, 
                category="linear", symbol=symbol, positionIdx=0, 
                takeProfit=self._align_price(symbol, new_tp), 
                stopLoss=self._align_price(symbol, new_sl),
                tpTriggerBy="LastPrice", slTriggerBy="LastPrice"
            )
            return True
        except Exception as e:
            logger.debug(f"[X-RAY] Failed to amend trailing stop for {symbol}: {e}")
            return False
            
    async def _execute_emergency_escape(self, symbol: str, current_price: float, qty: float, is_buy: bool) -> bool:
        current_qty = qty
        collars = [0.010, 0.025, 0.050]

        for attempt, aggression in enumerate(collars):
            escape_price = current_price * (1.0 - aggression) if is_buy else current_price * (1.0 + aggression)
            try:
                await self.executor.safe_call(
                    self.executor.client.place_order, 
                    category="linear", symbol=symbol, 
                    side="Sell" if is_buy else "Buy", 
                    orderType="Limit", 
                    price=self._align_price(symbol, escape_price),
                    qty=str(current_qty), timeInForce="IOC", reduceOnly=True
                )
                await asyncio.sleep(0.4)

                pos_res = await self.executor.safe_call(
                    self.executor.client.get_positions, category="linear", symbol=symbol
                )
                pos_list = pos_res.get("result", {}).get("list", [])
                remaining = float(pos_list[0].get("size", 0.0)) if pos_list else 0.0

                if remaining <= 0:
                    logger.critical(f"🚀 EMERGENCY EJECTION CONFIRMED // {symbol} Flat after Limit-IOC attempt {attempt + 1}.")
                    return True
                else:
                    current_qty = remaining
                    logger.warning(f"[X-RAY] ⚠️ Escape unfilled ({aggression * 100:.1f}% collar, {remaining} units left). Escalating...")
            except Exception as e:
                logger.error(f"[X-RAY] Escape attempt {attempt + 1} failed for {symbol}: {e}")

        logger.critical(f"🚨 FINAL FALLBACK MARKET DUMP // Liquidating remaining {current_qty} units for {symbol}.")
        try:
            await self.executor.safe_call(
                self.executor.client.place_order, 
                category="linear", symbol=symbol, 
                side="Sell" if is_buy else "Buy", 
                orderType="Market", qty=str(current_qty), timeInForce="IOC", reduceOnly=True
            )
            await asyncio.sleep(0.5)
            pos_res = await self.executor.safe_call(
                self.executor.client.get_positions, category="linear", symbol=symbol
            )
            pos_list = pos_res.get("result", {}).get("list", [])
            remaining = float(pos_list[0].get("size", 0.0)) if pos_list else 0.0
            return remaining <= 0
        except Exception as e:
            logger.error(f"[X-RAY] 💀 FATAL: Final market dump failed for {symbol}: {e}")
            return False

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
                risk_matrix = {"allocated_value_usdt": qty * entry_price, "size": qty, "recommended_leverage": 5}
                self.daemon_tasks[symbol] = self.track_task(self._position_lifecycle_daemon(symbol, str(uuid.uuid4()), direction, entry_price, atr, risk_matrix, 5, "RANGING", is_recovery=True))
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
                    current_vault_balance = await self._get_true_equity_usdt()
                except Exception as e: 
                    logger.debug(f"[X-RAY] Heartbeat true equity fetch failed: {e}", exc_info=True)
                    continue

                if "wallet_baseline" not in self.global_state_cache: 
                    self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01)
                
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                current_day = now_utc.strftime("%Y-%m-%d")
                if self.global_state_cache.get("current_day") != current_day:
                    self.global_state_cache["current_day"] = current_day
                    self.global_state_cache["start_of_day_balance"] = current_vault_balance

                today_start_iso = now_utc.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                try:
                    def _fetch(): return self.memory.get_forensic_execution_summary(today_start_iso) if self.memory else {}
                    execution_stats = await asyncio.wait_for(asyncio.to_thread(_fetch), timeout=5.0)
                    if "avg_slippage_bps" in execution_stats:
                        execution_stats["avg_slippage_bps"] = min(35.0, max(-35.0, float(execution_stats["avg_slippage_bps"])))
                except Exception as e: 
                    logger.debug(f"[X-RAY] Heartbeat DB forensic fetch failed: {e}", exc_info=True)
                    execution_stats = {} 

                actual_net_pnl = current_vault_balance - self.global_state_cache.get("start_of_day_balance", current_vault_balance)
                baseline = self.global_state_cache["wallet_baseline"]
                
                if current_vault_balance > baseline:
                    self.global_state_cache["wallet_baseline"] = current_vault_balance
                    baseline = current_vault_balance
                    
                drawdown_pct = max(0.0, (baseline - current_vault_balance) / baseline)
                
                if drawdown_pct >= 0.30 and len(self.active_positions_map) > 0:
                    self.fsm.trigger_global_emergency_lock()
                    await self._safe_telegram_dispatch(f"🚨 <b>EMERGENCY DRAWDOWN BREAKER TRIPPED</b>\nDrawdown: {drawdown_pct:.2%}. Engine shutting down.", is_html=True)
                    raise EmergencyShutdown(f"Drawdown limit reached: {drawdown_pct:.2%}")
                
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
                self.track_task(self._safe_telegram_dispatch(report, is_html=True))

    async def handle_incoming_trade(self, trade_data: Dict[str, Any]):
        symbol = trade_data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return
        
        now = time.time()
        
        stat_engine = self.stat_engines.get(symbol)
        feature_engine = self.feature_engines.get(symbol)
        inst_var = getattr(stat_engine, 'inst_variance', 0.001) if stat_engine else 0.001
        
        dynamic_cooldown_seconds = max(60.0, min(300.0, 120.0 / (inst_var * 1000.0 + 1e-9)))
        
        regime = feature_engine.detect_market_regime() if feature_engine else "TRENDING"
        if regime == "TRENDING":
            dynamic_cooldown_seconds = 30.0

        last_trade_time = self.last_eval_time.get(symbol + "_last_trade", 0.0)
        if now - last_trade_time < dynamic_cooldown_seconds:
            return

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
            if feature_engine: feature_engine.push_trade_tick([trade_data])

            clock = self.vpin_clocks.get(symbol)
            if not stat_engine or not clock: return
            
            stat_engine.update_trades(price, exchange_timestamp)
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
                
                sgd_state = stat_engine.extract_statistical_state(
                    current_price=price, 
                    log_mlofi_z=stat_engine.ofi_fast_z, 
                    hawkes_z=stat_engine.hawkes_z, 
                    sector_impulse=self.tensor_oracle.compute_lead_lag_signal(symbol), 
                    sl_dist_pct=sl_dist_pct, 
                    tp_dist_pct=tp_dist_pct, 
                    exchange_timestamp=exchange_timestamp
                )
                
                action, prob_success = sgd_state["action_dir"], max(sgd_state["p_up"], sgd_state["p_down"])
                
                structural_verdict = edge_gate.evaluate_structural_edge(symbol, vpin_z, intended_direction=action)
                if structural_verdict["action"] == "HOLD": return
                if structural_verdict["action"] != action:
                    action, prob_success = structural_verdict["action"], max(prob_success, 0.58) 
                
                if spread_cost > 0.0004: 
                    structural_verdict["routing"] = "MAKER_ONLY"
                    regime = "MEAN_REVERTING"
                    
                routing_mode = structural_verdict.get("routing", "STANDARD")

                # -------------------------------------------------------------
                # 🚀 V62.0 HAWKES PROCESS SELF-EXCITATION BRANCHING RATIO (η = α / β)
                # -------------------------------------------------------------
                # Compute excitation parameter α and decay β from Hawkes intensity
                alpha_hawkes = max(0.01, abs(stat_engine.hawkes_z) * 0.3) if stat_engine else 0.1
                beta_hawkes = max(0.1, 1.0 / (vol_mult + 1e-9))
                branching_ratio = alpha_hawkes / beta_hawkes

                # SUPER-CRITICAL CASCADE GUARD: If η >= 0.85, block all counter-trend/mean-reversion entries
                if branching_ratio >= 0.85 and regime == "MEAN_REVERTING":
                    logger.warning(f"[X-RAY] 🛑 HAWKES CASCADE GUARD // {symbol} Branching Ratio η={branching_ratio:.2f} >= 0.85. Blocking Mean-Reversion Entry.")
                    return

                # -------------------------------------------------------------
                # 🚀 V62.0 REINFORCED REGIME CONVICTION GATE
                # -------------------------------------------------------------
                if regime == "MEAN_REVERTING":
                    min_conviction = 0.63
                    if prob_success < min_conviction:
                        logger.info(f"[X-RAY] ⏸️ MEAN-REVERSION CHURN GATE // {symbol} Conviction {prob_success:.2%} < {min_conviction:.2%}. Rejecting chop.")
                        return

                net_ev_pct = (prob_success * tp_dist_pct) - ((1.0 - prob_success) * sl_dist_pct) - (spread_cost if routing_mode != "MAKER_ONLY" else -spread_cost * 0.2) - (0.0002 if routing_mode == "MAKER_ONLY" else 0.0005)

                if (net_ev_pct * 10000.0) < (spread_cost * 10000.0 * 3.0):
                    if prob_success >= 0.55:
                        logger.info(f"[X-RAY] ℹ️ INSUFFICIENT EV/SPREAD RATIO // {symbol} | Edge: {net_ev_pct*10000:.1f} bps | Spread: {spread_cost*10000:.1f} bps. Rejecting.")
                    return

                ev_floor = AdaptiveSessionClock.get_ev_floor(routing_mode)
                if net_ev_pct < ev_floor: 
                    if prob_success >= 0.55: logger.info(f"[X-RAY] ℹ️ NEAR-MISS // {symbol} | Prob: {prob_success:.1%} | Routing: {routing_mode} | Net EV: {net_ev_pct*10000:.1f} bps < Floor {ev_floor*10000:.1f} bps (Spread: {spread_cost*10000:.1f} bps)")
                    return
                    
                prob_success = stat_engine.calibrate_confidence(prob_success, regime, stat_engine.ewma_mse)

                async with self.portfolio_state_lock: dna_stats = self.ram_dna_cache.get(symbol, {"is_armed": True, "win_rate": 0.50})
                
                dynamic_gate = max(0.55, sgd_state.get("dynamic_gate", 0.55) - (0.05 if routing_mode == "MAKER_ONLY" else 0.0))

                if prob_success < max(dynamic_gate, dna_stats.get("cluster_win_rate", dna_stats.get("win_rate", 0.48))): 
                    return
                    
                if feature_engine and hasattr(feature_engine, 'get_htf_trend_bias'):
                    prob_success += (feature_engine.get_htf_trend_bias(price) * 0.04) if action == "BUY" else -(feature_engine.get_htf_trend_bias(price) * 0.04)

                if hasattr(self.sor, 'estimate_orderbook_slippage_bps'):
                    est_slippage = self.sor.estimate_orderbook_slippage_bps(ob, action, 6.50 / price, price)
                    live_spread_bps = (spread_cost * 10000.0)
                    dynamic_cap_bps = self.sor.compute_dynamic_slippage_cap_bps(symbol, regime, live_spread_bps)
                    
                    if est_slippage > dynamic_cap_bps:
                        logger.warning(f"[X-RAY] 🛑 DYNAMIC FIREWALL REJECTED {symbol} (est. {est_slippage:.1f} bps > Cap {dynamic_cap_bps:.1f} bps)")
                        return

                try:
                    payload = {
                        "symbol": symbol, "action": action, "price": price, 
                        "prob_success": prob_success, "dna_stats": dna_stats, 
                        "atr": atr, "regime": regime, "net_edge_bps": net_ev_pct * 10000.0, 
                        "vol_z": vol_z, "vol_mult": vol_mult, "timestamp": time.time(),
                        "payload_features": {
                            "symbol": symbol, 
                            "market_regime": regime, 
                            "virtual_sl": sgd_state["virtual_sl"], 
                            "virtual_tp": sgd_state["virtual_tp"], 
                            "log_mlofi_z": stat_engine.ofi_fast_z, 
                            "liquidity_density_ratio": vol_mult, 
                            "bid_ask_spread": spread_cost, 
                            "reasoning": structural_verdict.get("reasoning", "MICROSTRUCTURE_ALPHA"), 
                            "ai_verdict": "DIRECT_MICROSTRUCTURE_ALPHA"
                        },
                        "elasticity": self.elasticity_engines.get(symbol),
                        "dynamic_rr": dynamic_rr_ratio 
                    }
                    heap_id = self.auction_engine.get_next_heap_id()
                    async with self.auction_lock: 
                        heapq.heappush(self.auction_queue, (-(net_ev_pct / (sl_dist_pct + 1e-9)), time.time(), symbol, heap_id, payload))
                        self.last_eval_time[symbol + "_last_trade"] = time.time()
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
                            if action_type == "prediction" and self.memory: await asyncio.wait_for(asyncio.to_thread(self.memory.commit_prediction, *args), timeout=10.0)
                            elif action_type == "settlement" and self.memory: await asyncio.wait_for(asyncio.to_thread(self.memory.log_live_execution_result, *args), timeout=10.0)
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
                        if not self.memory: return {"is_armed": True, "win_rate": 0.50}
                        async with self.db_semaphore:
                            res = await asyncio.wait_for(asyncio.to_thread(self.memory.compute_latent_dna_edge, dna, 30), timeout=5.0)
                            return res
                    except Exception: return {"is_armed": True, "win_rate": 0.50} 

                fetch_tasks = {sym: _safe_fetch(sym, {"vol_mult": self.screener_metrics.get(sym, {}).get("vol_mult", 1.0), "log_mlofi_z": self.stat_engines.get(sym).ofi_fast_z if self.stat_engines.get(sym) else 0.0, "spread_pct": (self.orderbook_snapshots.get(sym, {}).get("best_ask", 1) - self.orderbook_snapshots.get(sym, {}).get("best_bid", 1)) / max(self.orderbook_snapshots.get(sym, {}).get("best_bid", 1), 1e-9), "symbol": sym}) for sym in list(self.asset_basket)}
                if not fetch_tasks: continue
                results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
                
                async with self.portfolio_state_lock:
                    for sym, result in zip(list(fetch_tasks.keys()), results):
                        if isinstance(result, Exception): self.ram_dna_cache[sym] = {"is_armed": True, "win_rate": 0.50}
                        else:
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
                        try: 
                            if self.memory: await asyncio.wait_for(asyncio.to_thread(self.memory.resolve_batch_historical_predictions, list(current_prices.keys()), current_prices, 60.0, interval_mins), timeout=15.0)
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
                        
                    if (elasticity := self.elasticity_engines.get(symbol)) and stat_engine:
                        elasticity.update_depth_state(best_bid, top_bid_size, best_ask, top_ask_size, stat_engine.ofi_fast_z, float(depth_data.get("ts", time.time() * 1000)) / 1000.0)

            if (edge_gate := self.edge_gates.get(symbol)) and bids and asks:
                try:
                    f_bids, f_asks = feature_engine.get_deep_book_floats()
                    edge_gate.update_orderbook_state(symbol, f_bids, f_asks, (book_metrics["top_bid"] + book_metrics["top_ask"]) / 2.0)
                except Exception as e: logger.debug(f"[X-RAY] Edge gate update error for {symbol}: {e}")

    async def run_omni_swarm_director(self):
        logger.info("🌪️ OMNI-SWARM DIRECTOR ONLINE: Monitoring Global Vectors.")
        banned_keywords = ["SOXL", "SPCX", "SKHY", "SNDK", "BANK", "MUUSDT", "BEAT", "MSTR", "ESPUSDT", "DEXE", "PUMP", "EUL", "XAU", "XAG", "USDC"]
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
                        
                        spread_bps = ((ask - bid) / bid) * 10000.0 if bid > 0 else 999.0
                        
                        if bid > 0 and ask > bid and turnover >= 15_000_000.0 and spread_bps <= 3.5:
                            max_notional = await self._get_max_affordable_notional()
                            if (self.hardware_min_qty.get(hot_sym, 1.0) * bid) <= max_notional:
                                async with self.portfolio_state_lock:
                                    if dead_sym in self.asset_basket: self.asset_basket.remove(dead_sym)
                                    if hot_sym not in self.asset_basket: self.asset_basket.append(hot_sym)
                                self._initialize_symbol_structures([hot_sym])
                                await self._prune_dead_symbols() 
                                if self.stream_feed_instance and hasattr(self.stream_feed_instance, 'hot_swap_socket_stream'):
                                    await self.stream_feed_instance.hot_swap_socket_stream(dead_sym, hot_sym)
                                logger.critical(f"[X-RAY] 🚀 DYNAMIC SWAP // {hot_sym} PASSED GATES AND INJECTED INTO MATRIX (Replaced {dead_sym}).")
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
            logger.info("🌌 V62.0 ULTRA-APEX REFRESH: Dynamically probing exchange for liquid nodes...")
            await self._fetch_exchange_tick_sizes()
            
            dynamic_basket = await self.executor.get_top_volatile_assets(limit=6)
            
            async with self.portfolio_state_lock:
                self.asset_basket = dynamic_basket[:4]
                self.shadow_basket = dynamic_basket[4:] if len(dynamic_basket) > 4 else []
                
            await self._prune_dead_symbols() 
            self._initialize_symbol_structures(self.asset_basket + self.shadow_basket)
            self.force_dna_refresh.set() 
        except Exception as e:
            logger.error(f"[X-RAY] Universe refresher error: {e}", exc_info=True)

    async def _universe_refresher_loop(self):
        while True:
            await asyncio.sleep(900)
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


    # ==============================================================================
    # 🚀 V62.0 HJB OPTIMAL STOPPING & POSITION LIFECYCLE FSM
    # ==============================================================================

    async def _state_verify_entry(self, ctx: dict) -> str:
        for _ in range(5):  
            await asyncio.sleep(3)
            try:
                pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=ctx["symbol"])
                pos_data = pos_res.get("result", {}).get("list", [])
                if pos_data and float(pos_data[0].get("size", 0.0)) > 0:
                    ctx["actual_entry"] = float(pos_data[0].get("avgPrice", ctx["current_price"]))
                    ctx["actual_qty_filled"] = float(pos_data[0].get("size", ctx["actual_qty_filled"]))
                    
                    ctx["highest_since_entry"] = ctx["actual_entry"] if ctx["is_buy"] else None
                    ctx["lowest_since_entry"] = ctx["actual_entry"] if not ctx["is_buy"] else None
                    ctx["max_favorable_price"] = ctx["actual_entry"]
                    
                    return "INITIALIZE_STOPS"
            except Exception as e: 
                logger.debug(f"[X-RAY] Position fill check failed for {ctx['symbol']}: {e}")

        try: await self.executor.safe_call(self.executor.client.cancel_all_orders, category="linear", symbol=ctx["symbol"])
        except Exception: pass
        return "ABORT"

    async def _state_initialize_stops(self, ctx: dict) -> str:
        actual_sl_distance = abs(ctx["actual_entry"] - ctx["realigned_sl"]) if ctx["realigned_sl"] else max(ctx["atr"] * self.live_params.get("sl_atr_mult", 2.5), ctx["actual_entry"] * 0.025)
        
        ctx["initial_risk"] = actual_sl_distance
        
        current_sl = ctx["realigned_sl"] if ctx["realigned_sl"] else (ctx["actual_entry"] - actual_sl_distance if ctx["is_buy"] else ctx["actual_entry"] + actual_sl_distance)
        current_tp = ctx["realigned_tp"] if ctx["realigned_tp"] else (ctx["actual_entry"] + (actual_sl_distance * ctx["dynamic_rr_ratio"]) if ctx["is_buy"] else ctx["actual_entry"] - (actual_sl_distance * ctx["dynamic_rr_ratio"]))
        
        if ctx["is_buy"]:
            current_tp = max(current_tp, ctx["current_price"] * 1.001)
            current_sl = min(current_sl, ctx["current_price"] * 0.999)
            if current_tp <= current_sl: current_tp = current_sl * 1.01
        else:
            current_tp = min(current_tp, ctx["current_price"] * 0.999)
            current_sl = max(current_sl, ctx["current_price"] * 1.001)
            if current_tp >= current_sl: current_tp = current_sl * 0.99
            
        await self._amend_trailing_stop(ctx["symbol"], current_sl, current_tp)
        
        ctx["current_sl"] = current_sl
        ctx["current_tp"] = current_tp
        ctx["last_sent_sl_str"] = self._align_price(ctx["symbol"], current_sl)
        ctx["last_sent_tp_str"] = self._align_price(ctx["symbol"], current_tp)
        
        return "ACTIVE_MONITORING"

    async def _state_monitor_escapes(self, ctx: dict) -> str:
        """
        🧬 V62.0 HJB OPTIMAL STOPPING & NEURAL EXIT MATRIX
        Calculates Ornstein-Uhlenbeck mean reversion speed θ and Hawkes Branching Ratio η
        to orchestrate optimal profit extraction and zero-loss preemptive escapes.
        """
        is_buy, symbol, safe_c_price = ctx["is_buy"], ctx["symbol"], ctx["safe_c_price"]
        
        stat_engine = ctx["stat_engine"]
        cvd_z = getattr(stat_engine, 'ofi_fast_z', 0.0) if stat_engine else 0.0
        hawkes_z = getattr(stat_engine, 'hawkes_z', 0.0) if stat_engine else 0.0
        inst_var = getattr(stat_engine, 'inst_variance', 0.001) if stat_engine else 0.001
        
        r_multiple = ctx["r_multiple"]
        time_alive = ctx["time_in_mins"]

        # -------------------------------------------------------------
        # 1. PREDICTIVE ORDERBOOK FRAGILITY EJECTION (Top-3 Depth Shield)
        # -------------------------------------------------------------
        ob = self.orderbook_snapshots.get(symbol, {})
        bids = ob.get("bids", [])
        asks = ob.get("asks", [])
        
        if bids and asks:
            top_bid_depth_usd = sum(float(b[0]) * float(b[1]) for b in bids[:3])
            top_ask_depth_usd = sum(float(a[0]) * float(a[1]) for a in asks[:3])
            thin_side_depth = top_ask_depth_usd if is_buy else top_bid_depth_usd
            
            if thin_side_depth < 150.0 and time_alive > 0.5:
                logger.critical(f"🚨 ORDERBOOK FRAGILITY ALERT // {symbol} Top-3 Depth collapsed to ${thin_side_depth:.1f}. Preemptive escape executed.")
                await self._execute_emergency_escape(symbol, safe_c_price, ctx["actual_qty_filled"], is_buy)
                return "ESCAPED"

        # -------------------------------------------------------------
        # 2. HAWKES PROCESS CASCADE DETECTOR (η = α / β)
        # -------------------------------------------------------------
        alpha_h = max(0.01, abs(hawkes_z) * 0.3)
        beta_h = max(0.1, 1.0 / (inst_var * 1000.0 + 1e-9))
        branching_ratio = alpha_h / beta_h

        # Super-Critical Inverse Flow Trap:
        if branching_ratio >= 0.85 and ((is_buy and cvd_z < -2.0) or (not is_buy and cvd_z > 2.0)):
            logger.critical(f"🛑 SUPER-CRITICAL CASCADE EJECTION // {symbol} Hawkes η={branching_ratio:.2f} with inverse CVD flow ({cvd_z:.2f}). Escaping instantly.")
            await self._execute_emergency_escape(symbol, safe_c_price, ctx["actual_qty_filled"], is_buy)
            return "ESCAPED"

        # -------------------------------------------------------------
        # 3. HAWKES-DECAY TIME STOP (Non-Linear Theta)
        # -------------------------------------------------------------
        if time_alive > 3.0 and r_multiple < 0.3:
            if hawkes_z < -1.5 and inst_var < 0.00005:
                logger.warning(f"⏳ HAWKES DECAY EXIT // {symbol} Volume heartbeat dead. Cutting position at zero-loss prior to chop.")
                await self._execute_emergency_escape(symbol, safe_c_price, ctx["actual_qty_filled"], is_buy)
                return "ESCAPED"

        # -------------------------------------------------------------
        # 4. HJB ELASTIC DIVERGENCE SNAP-TRAP
        # -------------------------------------------------------------
        if r_multiple >= 1.2:
            flow_divergence = (is_buy and cvd_z < -1.5) or (not is_buy and cvd_z > 1.5)
            if flow_divergence:
                snap_price = safe_c_price * 0.9995 if is_buy else safe_c_price * 1.0005
                if (is_buy and snap_price > ctx["current_sl"]) or (not is_buy and snap_price < ctx["current_sl"]):
                    logger.critical(f"⚡ ELASTIC SNAP-TRAP // {symbol} Order flow diverged at peak (+{r_multiple:.2f}R). Snapping stop to lock profit.")
                    ctx["current_sl"] = snap_price
                    ctx["requires_sl_update"] = True

        # -------------------------------------------------------------
        # 5. PARABOLIC CASCADE SQUEEZE
        # -------------------------------------------------------------
        if r_multiple >= 2.0 and hawkes_z > 3.5 and abs(cvd_z) > 3.0:
            logger.critical(f"🚀 PARABOLIC CASCADE SQUEEZE // {symbol} Liquidation cascade active. Liquidating into peak strength.")
            squeeze_price = safe_c_price * 1.001 if is_buy else safe_c_price * 0.999
            await self._execute_emergency_escape(symbol, squeeze_price, ctx["actual_qty_filled"], is_buy)
            return "ESCAPED"

        return "CONTINUE"

    async def _state_execute_scale_outs(self, ctx: dict) -> str:
        """
        🚀 V62.0 PROFIT PAYOFF FLOOR ENFORCER
        Strictly enforces R >= 1.2R minimum payoff before taking partial profits,
        ensuring Average Win strictly exceeds Average Loss across all trade regimes.
        """
        if self.test_mode or ctx["r_multiple"] < 1.2: 
            return "CONTINUE"

        symbol, safe_c_price = ctx["symbol"], ctx["safe_c_price"]

        try:
            current_pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
            p_list = current_pos_res.get("result", {}).get("list", [])
            
            if not p_list or float(p_list[0].get("size", 0.0)) <= 0: 
                return "CONTINUE"
                
            current_qty = float(p_list[0]["size"])
            limits = self.sor.instrument_cache.get(symbol, {"min_qty": 0.001, "qty_step": 0.001})
            
            for target_r, flag in ctx["scaled_levels"].items():
                if target_r >= 1.2 and ctx["r_multiple"] >= target_r and not flag:
                    portion = 0.35 if target_r == ctx["r_t1"] else 0.50
                    qty_close = current_qty * portion
                    aligned_qty_close = math.floor(qty_close / limits["qty_step"]) * limits["qty_step"]
                    
                    remaining_qty = current_qty - aligned_qty_close
                    remaining_notional = remaining_qty * safe_c_price
                    
                    if 0 < remaining_notional < 6.0:
                        aligned_qty_close = current_qty 
                        logger.warning(f"[X-RAY] 🧹 DUST SWEEP // {symbol} Sweeping full remainder at +{ctx['r_multiple']:.2f}R.")

                    if aligned_qty_close >= limits["min_qty"] and ((aligned_qty_close * safe_c_price) >= 5.0 or aligned_qty_close == current_qty):
                        logger.critical(f"[X-RAY] 💰 PROFIT FLOOR SCALE-OUT // {symbol} reached +{target_r}R. Banked {aligned_qty_close} units.")
                        await self.executor.safe_call(
                            self.executor.client.place_order, 
                            category="linear", symbol=symbol, 
                            side="Sell" if ctx["is_buy"] else "Buy", 
                            orderType="Limit", 
                            price=self._align_price(symbol, safe_c_price),
                            qty=str(aligned_qty_close), 
                            timeInForce="IOC", reduceOnly=True
                        )
                        ctx["scaled_levels"][target_r] = True
                        if 0 < remaining_notional < 6.0:
                            return "FULLY_CLOSED" 
                        break
        except Exception as e: 
            logger.error(f"[X-RAY] Scale-out execution fault for {symbol}: {e}")
            
        return "CONTINUE"

    async def _state_manage_trailing_stops(self, ctx: dict):
        symbol, safe_c_price, is_buy = ctx["symbol"], ctx["safe_c_price"], ctx["is_buy"]
        
        baseline_sl = ctx["realigned_sl"] if ctx["realigned_sl"] else (ctx["actual_entry"] - ctx["initial_risk"] if is_buy else ctx["actual_entry"] + ctx["initial_risk"])
        if ctx["time_in_mins"] >= 2.0 and ctx["r_multiple"] >= 0.8 and abs(ctx["current_sl"] - baseline_sl) < (ctx["actual_entry"] * 0.0001):
            sub_1r_sl = (ctx["max_favorable_price"] - (ctx["initial_risk"] * 0.3)) if is_buy else (ctx["max_favorable_price"] + (ctx["initial_risk"] * 0.3))
            if (is_buy and sub_1r_sl > ctx["current_sl"]) or (not is_buy and sub_1r_sl < ctx["current_sl"]):
                ctx["current_sl"] = sub_1r_sl
                ctx["requires_sl_update"] = True
                logger.info(f"[X-RAY] 🛡️ SUB-1R HJB RATCHET // {symbol} Excursion hit +{ctx['r_multiple']:.2f}R. Locked profit floor.")

        live_atr = ctx["feature_engine"].get_computed_atr() if ctx["feature_engine"] and hasattr(ctx["feature_engine"], 'get_computed_atr') else (safe_c_price * 0.005)
        live_atr = live_atr if live_atr > 0 else (safe_c_price * 0.005)
        
        base_mult = 2.5 if ctx["regime"] in ["TRENDING", "VOLATILE"] else 1.8
        min_mult = 0.4 
        
        x = 5.0 * (ctx["r_multiple"] - 1.0) 
        x = max(-700, min(700, x))
        sigmoid_factor = min_mult + (base_mult - min_mult) / (1.0 + math.exp(x))

        vol_ratio = live_atr / max(safe_c_price, 1e-9)
        dynamic_grace_period = max(15.0, min(60.0, 1.0 / (vol_ratio * 100 + 1e-9)))

        if ctx["r_multiple"] < 0.5 and ctx["time_in_mins"] > dynamic_grace_period:
            theta_decay = max(0.4, 1.0 - ((ctx["time_in_mins"] - dynamic_grace_period) * 0.010)) 
        else:
            theta_decay = 1.0 

        tox_mod = 0.6 if (is_buy and ctx["imbalance"] < -0.5) or (not is_buy and ctx["imbalance"] > 0.5) else 1.0
        
        hawkes_scalar = 1.0 + (0.35 * math.log1p(max(0.0, ctx["hawkes_z"])))
        raw_trail_dist = max(live_atr * sigmoid_factor * theta_decay * tox_mod * hawkes_scalar, safe_c_price * 0.003)
        
        if ctx["r_multiple"] >= 1.0:
            raw_sl = (ctx["max_favorable_price"] - raw_trail_dist) if is_buy else (ctx["max_favorable_price"] + raw_trail_dist)
            be_plus = (ctx["actual_entry"] + ctx["actual_entry"] * 0.002) if is_buy else (ctx["actual_entry"] - ctx["actual_entry"] * 0.002)
            new_sl_val = max(raw_sl, be_plus) if is_buy else min(raw_sl, be_plus)
            
            if (is_buy and new_sl_val > ctx["current_sl"]) or (not is_buy and new_sl_val < ctx["current_sl"]):
                ctx["current_sl"] = new_sl_val
                ctx["requires_sl_update"] = True

        momentum_stretch = max(0.0, ctx["hawkes_z"] * 0.6) if ctx["regime"] == "TRENDING" else 0.0
        if ctx["hawkes_z"] < -1.5 and ctx["r_multiple"] > 1.0:
            momentum_stretch -= 0.5 
        
        target_rr = min(6.0, ctx["dynamic_rr_ratio"] + momentum_stretch + (max(0.0, ctx["r_multiple"] - 1.0) * 0.3)) 
        calc_tp = ctx["actual_entry"] + (ctx["initial_risk"] * target_rr) if is_buy else ctx["actual_entry"] - (ctx["initial_risk"] * target_rr)
        
        if (is_buy and calc_tp > ctx["current_tp"]) or (not is_buy and calc_tp < ctx["current_tp"]):
            if abs(calc_tp - ctx["current_tp"]) / ctx["actual_entry"] > 0.0025:
                ctx["current_tp"] = calc_tp
                ctx["requires_tp_update"] = True

        if (ctx["requires_sl_update"] or ctx["requires_tp_update"]) and (ctx["now"] - ctx["last_api_update_time"] >= 5.0):
            ob = self.orderbook_snapshots.get(symbol, {})
            spread = (ob.get("best_ask", safe_c_price) - ob.get("best_bid", safe_c_price)) / safe_c_price if ob.get("best_bid", 0) > 0 else 0.0005
            min_distance = max(live_atr * 0.2, safe_c_price * 0.003, spread * 1.5 * safe_c_price) 
            
            if is_buy:
                ctx["current_sl"] = min(ctx["current_sl"], safe_c_price - min_distance)
                ctx["current_tp"] = max(ctx["current_tp"], safe_c_price + min_distance)
                if ctx["current_tp"] <= ctx["current_sl"]: ctx["current_tp"] = ctx["current_sl"] * 1.01
            else:
                ctx["current_sl"] = max(ctx["current_sl"], safe_c_price + min_distance)
                ctx["current_tp"] = min(ctx["current_tp"], safe_c_price - min_distance)
                if ctx["current_tp"] >= ctx["current_sl"]: ctx["current_tp"] = ctx["current_sl"] * 0.99

            new_sl_str = self._align_price(symbol, ctx["current_sl"])
            new_tp_str = self._align_price(symbol, ctx["current_tp"])

            if new_sl_str != ctx["last_sent_sl_str"] or new_tp_str != ctx["last_sent_tp_str"]:
                if await self._amend_trailing_stop(symbol, ctx["current_sl"], ctx["current_tp"]):
                    ctx["last_api_update_time"] = ctx["now"]
                    ctx["last_sent_sl_str"] = new_sl_str
                    ctx["last_sent_tp_str"] = new_tp_str
                    
                    if ctx["requires_tp_update"]: logger.info(f"[X-RAY] 🌌 ASYMMETRIC TP REPULSION // {symbol} Target shifted to {new_tp_str}.")
                    if ctx["requires_sl_update"]: logger.info(f"[X-RAY] 🛡️ TRAILING RATCHET // {symbol} SL locked at {new_sl_str}.")


    async def _state_settle_trade(self, ctx: dict):
        symbol, actual_entry, target_leverage = ctx["symbol"], ctx["actual_entry"], ctx["target_leverage"]
        
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
                
                arrival_price = ctx.get("arrival_price", actual_entry)
                if ctx["is_buy"]:
                    entry_slippage_bps = ((actual_entry - arrival_price) / arrival_price) * 10000
                else:
                    entry_slippage_bps = ((arrival_price - actual_entry) / arrival_price) * 10000
                    
                ctx["exec_details"]["tca_entry_slippage_bps"] = entry_slippage_bps
                slippage_bps = entry_slippage_bps
                ctx["exec_details"]["fees_usdt"] = fees
                
                if net_pnl < 0:
                    async with self.circuit_breaker_lock:
                        prev_loss = [t for t in self.tick_error_counts.get(symbol, []) if time.time() - t < 7200]
                        prev_loss.append(time.time())
                        self.tick_error_counts[symbol] = prev_loss
                        
                        # ANTI-WHIPSAW LOCKOUT: 180s asset freeze post-loss to eradicate range churn
                        dynamic_lockout = 180.0 * (1.0 + (min(3.0, stat_engine.inst_variance * 5000.0) if (stat_engine := self.stat_engines.get(symbol)) else 0.0) * 2.0)
                        self.circuit_breakers[symbol] = time.time() + dynamic_lockout
                        logger.warning(f"[X-RAY] ⏸️ ANTI-WHIPSAW LOCKOUT: {symbol} frozen for {dynamic_lockout:.0f}s to eliminate range churn.")
            else: 
                net_pnl, real_outcome, slippage_bps, fees = 0.0, "RECONCILED", 0.0, 0.0
        except Exception as e: 
            logger.error(f"[X-RAY] Failed to fetch closed PnL for {symbol}: {e}")
            net_pnl, real_outcome, slippage_bps, fees = 0.0, "RECONCILED", 0.0, 0.0
        
        duration_mins = (time.time() - ctx["daemon_start_time"]) / 60.0
        self.log_to_wal_sync("settlement", [ctx["signal_id"], net_pnl, slippage_bps, real_outcome, ctx["exec_details"]])
        self.track_task(self._safe_telegram_dispatch(self.telegram.format_execution_receipt(symbol, net_pnl, slippage_bps, fees, duration_mins, net_pnl > 0), is_html=True))

        async with self.portfolio_state_lock: self.active_positions_map.pop(symbol, None)
        self.risk_vault.update_position_ledger(symbol, 0.0)

    async def _position_lifecycle_daemon(self, symbol: str, signal_id: str, direction: str, current_price: float, atr: float, risk_matrix: dict, target_leverage: int = 8, market_regime: str = "TRENDING", is_recovery: bool = False, realigned_tp: float = None, dynamic_rr_ratio: float = 2.0, realigned_sl: float = None):
        async with self.execution_semaphore:
            ctx = {
                "symbol": symbol, "signal_id": signal_id, "direction": direction, "is_buy": direction == "BUY",
                "current_price": current_price, "atr": atr, "target_leverage": target_leverage, 
                "arrival_price": current_price,
                "regime": market_regime, "dynamic_rr_ratio": dynamic_rr_ratio, "realigned_sl": realigned_sl, 
                "realigned_tp": realigned_tp, "daemon_start_time": time.time(),
                "actual_qty_filled": risk_matrix.get("size", 1.0),
                "pofe_consecutive_ticks": 0, "api_check_counter": 0, "last_api_update_time": time.time(),
                "r_t1": round(max(1.2, dynamic_rr_ratio * 0.6), 2),
                "r_t2": round(max(1.8, dynamic_rr_ratio * 1.0), 2),
                "r_t3": round(max(2.5, dynamic_rr_ratio * 1.5), 2),
                "exec_details": {"leverage": target_leverage, "execution_mode": "RECOVERY" if is_recovery else ("GHOST" if self.test_mode else "LIVE")},
                "stat_engine": self.stat_engines.get(symbol),
                "feature_engine": self.feature_engines.get(symbol)
            }
            
            ctx["scaled_levels"] = {ctx["r_t1"]: False, ctx["r_t2"]: False, ctx["r_t3"]: False}

            if self.test_mode:
                await asyncio.sleep(60)
                self.log_to_wal_sync("settlement", [signal_id, 0.0, 0.0, "PAPER_TIMEOUT", ctx["exec_details"]])
                async with self.portfolio_state_lock: self.active_positions_map.pop(symbol, None)
                return

            try:
                state = await self._state_verify_entry(ctx)
                if state == "ABORT":
                    self.risk_vault.update_position_ledger(symbol, 0.0)
                    async with self.portfolio_state_lock: self.active_positions_map.pop(symbol, None)
                    return
                
                state = await self._state_initialize_stops(ctx)
                
                try: 
                    ctx["available_balance"] = await self.executor.get_wallet_balance_usdt()
                except Exception: 
                    ctx["available_balance"] = 7.00
                    
                while state == "ACTIVE_MONITORING":
                    ob = self.orderbook_snapshots.get(symbol, {})
                    ctx["safe_c_price"] = ob.get("best_bid" if ctx["is_buy"] else "best_ask", current_price)
                    
                    if ctx["stat_engine"] and ctx["stat_engine"].true_micro_price > 0: 
                        ctx["safe_c_price"] = ctx["stat_engine"].true_micro_price
                        
                    sl_proximity = abs(ctx["safe_c_price"] - ctx["current_sl"]) / (ctx["safe_c_price"] + 1e-9)
                    
                    loop_sleep = 0.5 if sl_proximity < 0.005 else 1.5 
                    await asyncio.sleep(loop_sleep) 
                    
                    ctx["now"] = time.time()
                    ctx["api_check_counter"] += 1
                    ctx["time_in_mins"] = (ctx["now"] - ctx["daemon_start_time"]) / 60.0
                    
                    ctx["requires_sl_update"] = False
                    ctx["requires_tp_update"] = False
                    
                    if ctx["api_check_counter"] >= (15 if loop_sleep == 1.5 else 75):
                        ctx["api_check_counter"] = 0
                        try:
                            pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                            pos_list = pos_res.get("result", {}).get("list", [])
                            if (not pos_list) or float(pos_list[0].get("size", 0.0)) == 0.0: break 
                        except Exception: pass

                    if ctx["safe_c_price"] != current_price:
                        if ctx["is_buy"] and ctx["safe_c_price"] > ctx["max_favorable_price"]: 
                            ctx["max_favorable_price"] = ctx["safe_c_price"]
                            if ctx["safe_c_price"] > ctx["highest_since_entry"]: ctx["highest_since_entry"] = ctx["safe_c_price"]
                        elif not ctx["is_buy"] and ctx["safe_c_price"] < ctx["max_favorable_price"]: 
                            ctx["max_favorable_price"] = ctx["safe_c_price"]
                            if ctx["safe_c_price"] < ctx["lowest_since_entry"]: ctx["lowest_since_entry"] = ctx["safe_c_price"]
                        
                    ctx["r_multiple"] = (ctx["max_favorable_price"] - ctx["actual_entry"]) / (ctx["initial_risk"] + 1e-9) if ctx["is_buy"] else (ctx["actual_entry"] - ctx["max_favorable_price"]) / (ctx["initial_risk"] + 1e-9)
                    ctx["current_r"] = (ctx["safe_c_price"] - ctx["actual_entry"]) / (ctx["initial_risk"] + 1e-9) if ctx["is_buy"] else (ctx["actual_entry"] - ctx["safe_c_price"]) / (ctx["initial_risk"] + 1e-9)

                    ctx["hawkes_z"] = getattr(ctx["stat_engine"], 'hawkes_z', getattr(ctx["stat_engine"], 'vpin_z', 0.0))
                    
                    b_vol, a_vol = float(ob.get("bid_size", 0.0)), float(ob.get("ask_size", 0.0))
                    ctx["imbalance"] = (b_vol - a_vol) / (b_vol + a_vol + 1e-9)
                    
                    if await self._state_monitor_escapes(ctx) == "ESCAPED":
                        break
                        
                    if await self._state_execute_scale_outs(ctx) == "FULLY_CLOSED":
                        break
                        
                    await self._state_manage_trailing_stops(ctx)
                    
                await self._state_settle_trade(ctx)

            except Exception as e:
                logger.error(f"[X-RAY] FSM Position daemon critical fault for {symbol}: {e}", exc_info=True)
                try:
                    pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                    pos_list = pos_res.get("result", {}).get("list", [])
                    if pos_list and float(pos_list[0].get("size", 0.0)) > 0:
                        qty = float(pos_list[0]["size"])
                        side = "Sell" if pos_list[0]["side"] == "Buy" else "Buy"
                        current_p = float(pos_list[0].get("markPrice", pos_list[0].get("avgPrice", 0.0)))
                        await self._execute_emergency_escape(symbol, current_p, qty, side == "Sell")
                except Exception as e2: logger.error(f"[X-RAY] Emergency FSM flatten failed for {symbol}: {e2}", exc_info=True)
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
                pos_list = pos_res.get("result", {}).get("list", [])
                if pos_list and float(pos_list[0].get("size", 0.0)) > 0:
                    qty = float(pos_list[0]["size"])
                    side = "Sell" if pos_list[0]["side"] == "Buy" else "Buy"
                    current_p = float(pos_list[0].get("markPrice", pos_list[0].get("avgPrice", 0.0)))
                    await self._execute_emergency_escape(symbol, current_p, qty, side == "Sell")
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
                        try: 
                            qty = float(p["size"])
                            side = "Sell" if p["side"] == "Buy" else "Buy"
                            current_p = float(p.get("markPrice", p.get("avgPrice", 0.0)))
                            await self._execute_emergency_escape(sym, current_p, qty, side == "Sell")
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
        
        if hasattr(self.executor, "_api_thread_pool"): self.executor._api_thread_pool.shutdown(wait=True, cancel_futures=False)
        logger.critical("✅ MATRIX DISCONNECTED.")

    async def _safe_daemon_run(self, coro_func):
        consecutive_crashes = 0
        while True:
            try: 
                await coro_func()
                consecutive_crashes = 0  
            except asyncio.CancelledError: break
            except EmergencyShutdown as e:
                logger.critical(f"[X-RAY] Emergency Shutdown Initiated: {e}")
                raise  
            except Exception as e:
                consecutive_crashes += 1
                sleep_time = min(300, 5 * (2 ** (consecutive_crashes - 1)))
                logger.error(f"Daemon {coro_func.__name__} crashed. Restarting in {sleep_time}s: {e}", exc_info=True)
                await asyncio.sleep(sleep_time)

    async def _preseed_screener_history(self):
        logger.info("⏳ Pre-seeding screener memory for correlation matrix...")
        for sym in self.asset_basket + self.shadow_basket:
            try:
                klines = await self.executor.safe_call(
                    self.executor.client.get_kline, 
                    category="linear", symbol=sym, interval=self.timeframe, limit=30
                )
                if klines.get("retCode") == 0:
                    batch = klines.get("result", {}).get("list", [])
                    for k in reversed(batch):
                        c_close = float(k[4])
                        if sym not in self.screener_memory:
                            self.screener_memory[sym] = {"prices": deque(maxlen=1440), "highs": deque(maxlen=150), "lows": deque(maxlen=150), "volumes": deque(maxlen=1440), "last_update_time": 0.0}
                        self.screener_memory[sym]["prices"].append(c_close)
            except Exception as e:
                logger.debug(f"[X-RAY] Pre-seed failed for {sym}: {e}")
        logger.info("✅ Correlation matrix pre-seeded successfully.")

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
            boot_bal = await self._get_true_equity_usdt()
            self.global_state_cache["start_of_day_balance"] = boot_bal
            self.global_state_cache["wallet_baseline"] = max(boot_bal, 0.01)
            self.global_state_cache["last_updated"] = time.time()
            self.global_state_cache["current_day"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        except Exception: pass
        
        dynamic_boot = await self.executor.get_top_volatile_assets(limit=6)
        self.asset_basket = dynamic_boot[:4]
        self.shadow_basket = dynamic_boot[4:] if len(dynamic_boot) > 4 else []
        self._initialize_symbol_structures(self.asset_basket + self.shadow_basket)
            
        await self._preseed_screener_history()
        
        daemons = [
            self.run_db_wal_worker, self._batch_wal_flush_loop, self.run_dna_prewarmer, 
            self.stream_manager_loop, self.run_system_heartbeat, self.cleanup_stale_locks, 
            self.run_shadow_resolution_daemon, self._universe_refresher_loop, 
            self.auction_engine.run_global_capital_auction_worker, self.run_omni_swarm_director,            
            self.run_exchange_state_reconciliation_daemon,
            self.run_crowded_trade_oracle,
            self.yield_engine.run_yield_scanner_daemon 
        ]
        await asyncio.gather(*[asyncio.create_task(self._safe_daemon_run(d)) for d in daemons], return_exceptions=True)

async def main():
    engine = DistributedQuantEngine()
    try: 
        await engine.run_engine_forever()
    except EmergencyShutdown: 
        logger.critical("🛑 Engine halted via Emergency Breaker. Handing off to Graceful Shutdown.")
    except asyncio.CancelledError: 
        pass
    finally: 
        await engine.graceful_shutdown()

if __name__ == "__main__":
    from keep_alive import keep_alive
    keep_alive()
    try: asyncio.run(main())
    except KeyboardInterrupt: sys.exit(0)