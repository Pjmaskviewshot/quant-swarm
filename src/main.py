"""
💎 V13.0 APEX OMEGA: EXPECTED VALUE ENGINE (PRODUCTION)
------------------------------------------------------------------------
Abolishes heuristic gates. Fuses macro flows and orderbook convexity 
directly into the base probability model. Trades purely on unified EV.

Upgraded with APEX OMEGA V13: Capital-Preservation Stochastic Control, 
Mahalanobis Distribution Shift Detection, Portfolio Commander, 
Profit Defender, and Stateful Execution Governance.
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
from core.quantum_entry import QuantumEntryMatrix  
from core.intelligent_exit import IntelligentExitEngine, ExecutionGovernorFSM, PositionExitState, ThesisVector
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
logger = logging.getLogger("QUANT_CORE.V13_APEX")


class DistributedQuantEngine:
    def __init__(self):
        load_dotenv()
        self.test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
        
        if self.test_mode: 
            logger.critical("⚠️ TEST MODE: Paper Trading Armed.")
        else: 
            logger.critical("💎 LIVE MODE: V13.0 APEX OMEGA MATRIX ACTIVE.")
        
        self.asset_basket: List[str] = []
        self.timeframe = os.getenv("TRADING_TIMEFRAME", "15")
        self.shadow_basket: List[str] = []
        
        self.db_semaphore = asyncio.Semaphore(5)
        self.eval_semaphore = asyncio.Semaphore(15)
        self.execution_semaphore = asyncio.Semaphore(10)
        
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
        self.entry_matrices: Dict[str, QuantumEntryMatrix] = {}
        self.elasticity_engines: Dict[str, MicroElasticityEngine] = {} 
        
        self.screener_memory, self.screener_metrics, self.orderbook_snapshots, self.ram_dna_cache = {}, {}, {}, {}
        self.volatility_baseline: Dict[str, float] = {}
        
        # STRICT ASYNC MUTATION LOCKS
        self.portfolio_state_lock = asyncio.Lock()
        
        self.active_positions_map: Dict[str, str] = {}  
        self.symbol_locks, self.eval_semaphores, self.daemon_tasks, self.last_eval_time = {}, {}, {}, {}
        self._active_tasks = set()
        
        self.auction_queue: List[tuple] = []  
        self.auction_queue_symbols = set()
        self.auction_lock = asyncio.Lock()
        
        self.tick_sizes: Dict[str, float] = {}
        self.hardware_min_qty: Dict[str, float] = {} 
        self.global_state_cache = {"last_updated": 0.0}
        self.live_params = self._load_live_params()
        self.last_socket_reconnect = 0.0 
        self.funding_rates, self.open_interests, self.spread_history = {}, {}, {}
        
        self.active_contexts: Dict[str, dict] = {}
        
        # APEX PERSISTENT STATE STORE
        self.exit_states: Dict[str, Any] = {}
        
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
                    self.entry_matrices.pop(key, None) 
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
            if s not in self.entry_matrices: self.entry_matrices[s] = QuantumEntryMatrix(window_size=1000)
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
        """Pulls Total Equity (Wallet Balance + Floating PnL) to match the Bybit App."""
        try:
            res = await self.executor.safe_call(self.executor.client.get_wallet_balance, accountType="UNIFIED")
            account_list = res.get("result", {}).get("list", [])
            if account_list and "totalEquity" in account_list[0]:
                return float(account_list[0]["totalEquity"])
            return await self.executor.get_wallet_balance_usdt()
        except Exception:
            return 11.40

    async def _fetch_exchange_tick_sizes(self):
        try:
            info = await self.executor.safe_call(self.executor.client.get_instruments_info, category="linear")
            for item in info.get("result", {}).get("list", []):
                sym = item.get("symbol")
                self.tick_sizes[sym] = float(item.get("priceFilter", {}).get("tickSize", "0.0001"))
                self.hardware_min_qty[sym] = float(item.get("lotSizeFilter", {}).get("minOrderQty", "1.0"))
        except Exception as e: logger.error(f"[X-RAY] Failed fetching exchange info: {e}", exc_info=True)

    def _get_max_affordable_notional(self, sl_dist_pct: float = 0.025) -> float:
        try:
            raw_balance = self.global_state_cache.get("current_vault_balance", 12.90)
            equity = max(1.0, float(raw_balance))
        except Exception:
            equity = 12.00

        max_risk_pct = 0.015
        max_risk_dollars = equity * max_risk_pct

        sl_pct = max(0.01, sl_dist_pct)
        allowed_notional = max_risk_dollars / sl_pct
        bybit_min_notional = 6.50

        if allowed_notional < bybit_min_notional:
            if equity < 50.0:
                return bybit_min_notional
            return allowed_notional

        return min(allowed_notional, equity * 1.5)

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

    async def _execute_verified_position_close(self, symbol: str, is_buy: bool, current_qty: float) -> bool:
        side = "Sell" if is_buy else "Buy"
        logger.critical(f"[X-RAY] 🚀 INITIATING VERIFIED CLOSE // {symbol} | Closing {current_qty} units.")
        
        res = await self.executor.safe_call(
            self.executor.client.place_order,
            category="linear", symbol=symbol,
            side=side, orderType="Market",
            qty=str(current_qty), timeInForce="IOC", reduceOnly=True
        )
        
        if res.get("retCode") != 0:
            logger.error(f"[X-RAY] ❌ Close order rejected by Bybit: {res.get('retMsg')}")
            return False

        for attempt in range(8):
            await asyncio.sleep(0.4)
            pos_res = await self.executor.safe_call(
                self.executor.client.get_positions,
                category="linear", symbol=symbol
            )
            pos_list = pos_res.get("result", {}).get("list", [])
            remaining = float(pos_list[0].get("size", 0.0)) if pos_list else 0.0
            
            if remaining <= 0.0:
                logger.critical(f"[X-RAY] ✅ POSITION CLOSED & VERIFIED // {symbol} is Flat (0.0000 units).")
                return True
            else:
                logger.warning(f"[X-RAY] ⏳ Awaiting fill confirmation ({attempt+1}/8)... Remaining: {remaining}")

        return False

    async def _execute_emergency_escape(self, symbol: str, current_price: float, qty: float, is_buy: bool) -> bool:
        """Fallback escape if verified close fails."""
        return await self._execute_verified_position_close(symbol, is_buy, qty)

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
                
                historical_max_favorable = entry_price
                try:
                    create_time_ms = int(pos.get("createdTime", time.time() * 1000))
                    klines = await self.executor.safe_call(self.executor.client.get_kline, category="linear", symbol=symbol, interval="15", limit=50)
                    if klines.get("retCode") == 0:
                        for k in klines.get("result", {}).get("list", []):
                            k_time = int(k[0])
                            if k_time >= create_time_ms - 900000:
                                k_high, k_low = float(k[2]), float(k[3])
                                if direction == "BUY" and k_high > historical_max_favorable: historical_max_favorable = k_high
                                elif direction == "SELL" and k_low < historical_max_favorable: historical_max_favorable = k_low
                except Exception as e: logger.debug(f"Amnesia recovery fetch failed for {symbol}: {e}")
                
                async with self.portfolio_state_lock: self.active_positions_map[symbol] = direction
                risk_matrix = {"allocated_value_usdt": qty * entry_price, "size": qty, "recommended_leverage": 5}
                self.daemon_tasks[symbol] = self.track_task(self._position_lifecycle_daemon(
                    symbol, str(uuid.uuid4()), direction, entry_price, atr, risk_matrix, 5, "RANGING", 
                    is_recovery=True, historical_favorable_price=historical_max_favorable
                ))
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

    async def run_fast_state_invariant_reconciliation(self):
        logger.info("🛡️ 500ms STATE INVARIANT RECONCILIATION DAEMON ONLINE.")
        while True:
            await asyncio.sleep(0.5)
            try:
                pos_response = await self.executor.safe_call(
                    self.executor.client.get_positions, category="linear", settleCoin="USDT"
                )
                if pos_response.get("retCode") != 0:
                    continue

                active_on_exchange = {
                    p["symbol"]: float(p.get("size", 0.0))
                    for p in pos_response.get("result", {}).get("list", [])
                    if float(p.get("size", 0.0)) > 0
                }

                async with self.portfolio_state_lock:
                    for tracked_sym in list(self.active_positions_map.keys()):
                        if tracked_sym not in active_on_exchange:
                            if not (daemon := self.daemon_tasks.get(tracked_sym)) or daemon.done():
                                logger.warning(f"[X-RAY] 🧹 INVARIANT ENFORCED: Purging desynchronized lock for {tracked_sym}")
                                self.active_positions_map.pop(tracked_sym, None)
                                self.risk_vault.update_position_ledger(tracked_sym, 0.0)

            except Exception as e:
                logger.debug(f"[X-RAY] Fast invariant sync tick bypassed: {e}")

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
                
                report = report.replace("V1.0 APEX", "V13.0 APEX OMEGA").replace("V12.0 APEX", "V13.0 APEX OMEGA").replace("V6.0 APEX", "V13.0 APEX OMEGA")
                self.track_task(self._safe_telegram_dispatch(report, is_html=True))

    async def handle_incoming_orderbook_tick(self, depth_data: Dict[str, Any]):
        symbol = depth_data.get("s")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return

        bids, asks = depth_data.get("b", []), depth_data.get("a", [])
        is_snapshot = depth_data.get("type") == "snapshot"
        
        async with self.symbol_locks[symbol]:
            feature_engine = self.feature_engines.get(symbol)
            if feature_engine:
                feature_engine.push_orderbook_tick(bids, asks, is_snapshot=is_snapshot)
                book_metrics = feature_engine.get_book_depth_metrics()
                if book_metrics and "top_bid" in book_metrics and "top_ask" in book_metrics:
                    best_bid, best_ask = book_metrics["top_bid"], book_metrics["top_ask"]
                    top_bid_size = float(bids[0][1]) if bids else (book_metrics.get("bid_depth_10", 10.0) / 10.0)
                    top_ask_size = float(asks[0][1]) if asks else (book_metrics.get("ask_depth_10", 10.0) / 10.0)
                    
                    if best_bid > 0 and best_ask > best_bid:
                        self.orderbook_snapshots[symbol] = {
                            "best_bid": best_bid, "bid_size": top_bid_size, 
                            "best_ask": best_ask, "ask_size": top_ask_size, 
                            "bids": bids, "asks": asks,
                            "bid_depth_10": book_metrics.get("bid_depth_10", 1.0),
                            "ask_depth_10": book_metrics.get("ask_depth_10", 1.0)
                        }
                        
                        stat_engine = self.stat_engines.get(symbol)
                        now, spread_val = time.time(), (best_ask - best_bid) / (best_bid + 1e-9)
                        
                        if (spread_hist := self.spread_history.get(symbol)) is not None:
                            spread_hist.append(spread_val)
                            async with self.circuit_breaker_lock:
                                med_spread = np.median(spread_hist) if len(spread_hist) >= 10 else spread_val
                                
                                if self.circuit_breakers.get(symbol, 0.0) > now and spread_val <= med_spread * 1.5:
                                    self.circuit_breakers[symbol] = 0.0
                                
                                spread_mult_cap = 6.0 if symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"] else 8.0
                                min_spread_floor = 0.0030 if symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"] else 0.0080
                                
                                if spread_val > med_spread * spread_mult_cap and spread_val > min_spread_floor:
                                    self.circuit_breakers[symbol] = now + 5.0
                        
                        if stat_engine: 
                            stat_engine.update_orderbook_pressure(best_bid, top_bid_size, best_ask, top_ask_size)
                            
                        if (elasticity := self.elasticity_engines.get(symbol)) and stat_engine:
                            elasticity.update_depth_state(
                                best_bid, top_bid_size, best_ask, top_ask_size, 
                                stat_engine.ofi_fast_z, float(depth_data.get("ts", time.time() * 1000)) / 1000.0
                            )

                if (edge_gate := self.edge_gates.get(symbol)) and bids and asks:
                    try:
                        f_bids, f_asks = feature_engine.get_deep_book_floats()
                        edge_gate.update_orderbook_state(symbol, f_bids, f_asks, (book_metrics["top_bid"] + book_metrics["top_ask"]) / 2.0)
                    except Exception as e: logger.debug(f"[X-RAY] Edge gate update error for {symbol}: {e}")

    async def handle_incoming_trade(self, trade_data: Dict[str, Any]):
        symbol = trade_data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return
        
        now = time.time()
        price = float(trade_data.get("price", 0.0))
        if price < 0.000001: return
        
        volume, is_buy = float(trade_data.get("size", 0.0)), (str(trade_data.get("side", "")).upper() == "BUY")
        exchange_timestamp = float(trade_data.get("timestamp", now * 1000)) / 1000.0
        
        async with self.symbol_locks[symbol]:
            stat_engine = self.stat_engines.get(symbol)
            feature_engine = self.feature_engines.get(symbol)
            clock = self.vpin_clocks.get(symbol)

            # 1. 🚀 SYNCHRONOUS DATA INGESTION: Always update statistical arrays inline to prevent data loss
            self.tensor_oracle.ingest_tick(symbol, price, exchange_timestamp) 
            if edge_gate := self.edge_gates.get(symbol): edge_gate.update_trade_flow(volume, is_buy)
            if feature_engine: feature_engine.push_trade_tick([trade_data])
            
            manifests = []
            if stat_engine and clock:
                stat_engine.update_trades(price, exchange_timestamp)
                self.risk_vault.push_microstructure_variance(stat_engine.inst_variance)
                manifests = clock.process_tick(price, volume, not is_buy)
                
            if symbol in self.active_contexts:
                ctx = self.active_contexts[symbol]
                ctx["latest_tick_price"] = price
                return

            inst_var = getattr(stat_engine, 'inst_variance', 0.001) if stat_engine else 0.001
            dynamic_cooldown_seconds = max(5.0, min(30.0, 15.0 / (inst_var * 1000.0 + 1e-9))) 
            regime = feature_engine.detect_market_regime() if feature_engine else "TRENDING"
            if regime == "TRENDING":
                dynamic_cooldown_seconds = 2.0  

            last_trade_time = self.last_eval_time.get(symbol + "_last_trade", 0.0)
            if now - last_trade_time < dynamic_cooldown_seconds:
                return

        # 2. 🚀 TASK LIMIT PROTECTOR: Prevent Event Loop Overload
        # Synchronously throttle the creation of heavy async evaluation tasks to max 5 per second.
        if now - self.last_eval_time.get(symbol + "_eval_throttle", 0.0) < 0.2:
            return  
        self.last_eval_time[symbol + "_eval_throttle"] = now

        async def _eval_gate(m_snapshot):
            async with self.circuit_breaker_lock:
                if self.circuit_breakers.get(symbol, 0.0) > now or self.circuit_breakers.get("GLOBAL_MAINTENANCE", 0.0) > now: return
                
            if not self.fsm.can_execute_trades or (time.time() - self.last_socket_reconnect < 5.0): return

            try:
                if not stat_engine or not clock: return
                
                valid_manifests = [m for m in m_snapshot if m.get("valid")]
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
            
                ob = self.orderbook_snapshots.get(symbol)
                if not ob or "bid_size" not in ob: return
                spread_cost = abs(ob["best_ask"] - ob["best_bid"]) / (price + 1e-9) if price > 0 else 0.001
                vol_mult = self.screener_metrics.get(symbol, {}).get("vol_mult", 1.0)
                
                if spread_cost > 0.0050: 
                    if symbol not in ["BTCUSDT", "ETHUSDT", "SOLUSDT"] and spread_cost <= 0.0100:
                        pass 
                    else:
                        logger.warning(f"🛑 HARD VETO // {symbol} Spread too toxic ({spread_cost*10000:.1f} bps).")
                        return
                
                if abs(vpin_z) > 3.5: 
                    logger.warning(f"🛑 HARD VETO // {symbol} Extreme toxic flow detected (VPIN Z: {vpin_z:.2f}).")
                    return
                
                async with self.eval_semaphores[symbol]:
                    raw_atr = feature_engine.get_computed_atr() if feature_engine and hasattr(feature_engine, 'get_computed_atr') else 0.0
                    atr = raw_atr if raw_atr > 0 else price * 0.005
                    
                    sl_atr_mult = max(2.0, self.live_params.get("sl_atr_mult", 2.5))
                    sl_dist_pct = max((atr * sl_atr_mult) / (price + 1e-9), 0.020)
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
                    
                    fusion_engine = self.entry_matrices.get(symbol)
                    exec_weight = 1.0
                    if fusion_engine:
                        btc_eng = self.stat_engines.get("BTCUSDT")
                        eth_eng = self.stat_engines.get("ETHUSDT")
                        
                        btc_ofi = getattr(btc_eng, 'ofi_fast_z', 0.0) if btc_eng else 0.0
                        eth_ofi = getattr(eth_eng, 'ofi_fast_z', 0.0) if eth_eng else 0.0
                        
                        fusion_engine.update_macro_flows(
                            asset_ofi_z=stat_engine.ofi_fast_z,
                            btc_ofi_z=btc_ofi,
                            eth_ofi_z=eth_ofi
                        )
                        fusion_engine.update_mlofi_state(stat_engine.ofi_fast_z)

                        bids_raw = ob.get("bids", [])
                        asks_raw = ob.get("asks", [])
                        
                        fusion_verdict = fusion_engine.fuse_signal_probability(
                            symbol=symbol,
                            raw_prob=prob_success,
                            intended_action=action,
                            bids=bids_raw,
                            asks=asks_raw
                        )
                        
                        prob_success = fusion_verdict.get("fused_prob", prob_success)
                        exec_weight = fusion_verdict.get("execution_weight", 1.0)
                    else:
                        return 

                    if prob_success < 0.515:
                        return

                    structural_verdict = edge_gate.evaluate_structural_edge(symbol, vpin_z, intended_direction=action)
                    action = structural_verdict.get("action", action)
                    edge_weight = structural_verdict.get("edge_weight", 1.0)
                    
                    if edge_weight == 0.0:
                        return 
                        
                    prob_success = min(0.95, prob_success * edge_weight)

                    routing_mode = structural_verdict.get("routing", "STANDARD")
                    prob_success = stat_engine.calibrate_confidence(prob_success, regime, stat_engine.ewma_mse)

                    async with self.portfolio_state_lock: 
                        dna_stats = self.ram_dna_cache.get(symbol, {"is_armed": True, "win_rate": 0.50})

                    if action == "BUY":
                        bbo_depth_qty = float(ob.get("ask_size", 1.0))
                    else:
                        bbo_depth_qty = float(ob.get("bid_size", 1.0))
                    
                    max_qty_by_depth = bbo_depth_qty * 0.50

                    max_notional = self._get_max_affordable_notional(sl_dist_pct=sl_dist_pct)
                    calculated_qty = (max_notional * exec_weight) / price
                    
                    try: 
                        raw_balance = self.global_state_cache.get("current_vault_balance", 12.90)
                        available_balance = max(1.0, float(raw_balance)) 
                    except Exception: 
                        available_balance = 12.90

                    max_permitted_notional = available_balance * 0.95
                    if (calculated_qty * price) > max_permitted_notional:
                        calculated_qty = max_permitted_notional / price

                    min_qty = self.hardware_min_qty.get(symbol, 1.0)
                    if calculated_qty < min_qty:
                        return
                        
                    if calculated_qty > max_qty_by_depth:
                        calculated_qty = max_qty_by_depth
                    
                    actual_notional = calculated_qty * price
                    actual_risk_dollars = actual_notional * sl_dist_pct
                    actual_risk_pct = (actual_risk_dollars / (available_balance + 1e-9)) * 100.0

                    if available_balance < 50.0 and actual_risk_pct > 3.0:
                        target_risk_dollars = available_balance * 0.025  
                        compressed_sl_pct = target_risk_dollars / (actual_notional + 1e-9)
                        compressed_sl_pct = max(0.010, compressed_sl_pct)  
                        
                        sl_atr_mult = max(2.0, self.live_params.get("sl_atr_mult", 2.5))
                        atr = (compressed_sl_pct * price) / sl_atr_mult
                        
                        tp_dist_pct = compressed_sl_pct * dynamic_rr_ratio
                        net_ev_pct = (prob_success * tp_dist_pct) - ((1.0 - prob_success) * compressed_sl_pct) - (spread_cost * 0.5)

                    elif actual_risk_pct > 2.0 and available_balance >= 50.0:
                        calculated_qty = (available_balance * 0.02) / (price * sl_dist_pct + 1e-9)
                        net_ev_pct = (prob_success * tp_dist_pct) - ((1.0 - prob_success) * sl_dist_pct) - (spread_cost * 0.5)
                    else:
                        net_ev_pct = (prob_success * tp_dist_pct) - ((1.0 - prob_success) * sl_dist_pct) - (spread_cost * 0.5)

                    try:
                        payload = {
                            "symbol": symbol, "action": action, "price": price, 
                            "prob_success": prob_success, "dna_stats": dna_stats, 
                            "atr": atr, "regime": regime, "net_edge_bps": net_ev_pct * 10000.0, 
                            "vol_z": vol_z, "vol_mult": vol_mult, "timestamp": time.time(),
                            "target_leverage": 10.0,
                            "actual_notional": actual_notional,
                            "payload_features": {
                                "symbol": symbol, 
                                "market_regime": regime, 
                                "virtual_sl": sgd_state["virtual_sl"], 
                                "virtual_tp": sgd_state["virtual_tp"], 
                                "log_mlofi_z": stat_engine.ofi_fast_z, 
                                "liquidity_density_ratio": vol_mult, 
                                "bid_ask_spread": spread_cost, 
                                "reasoning": structural_verdict.get("reasoning", "ALPHA_FUSION_EV"), 
                                "ai_verdict": "V13.0_APEX_OMEGA"
                            },
                            "elasticity": self.elasticity_engines.get(symbol),
                            "dynamic_rr": dynamic_rr_ratio 
                        }
                        heap_id = self.auction_engine.get_next_heap_id()
                        async with self.auction_lock: 
                            if symbol not in self.auction_queue_symbols:
                                heapq.heappush(self.auction_queue, (-(net_ev_pct / (sl_dist_pct + 1e-9)), time.time(), symbol, heap_id, payload))
                                self.auction_queue_symbols.add(symbol)
                                self.last_eval_time[symbol + "_last_trade"] = time.time()
                    except Exception as ex_payload: logger.error(f"[X-RAY] Failed to build auction payload for {symbol}: {ex_payload}", exc_info=True)
                    
            except Exception as e: logger.error(f"[X-RAY] Trade processing fault for {symbol}: {e}", exc_info=True)

        self.track_task(_eval_gate(manifests))

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
                            res = await asyncio.wait_for(asyncio.to_thread(self.memory.compute_latent_dna_edge, dna, 30), timeout=2.0)
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
                active_syms = []
                async with self.portfolio_state_lock:
                    active_syms = list(self.active_positions_map.keys())

                current_prices = {sym: {"prices": list(self.screener_memory[sym]["prices"]), "highs": list(self.screener_memory[sym].get("highs", [])), "lows": list(self.screener_memory[sym].get("lows", []))} for sym in self.asset_basket + self.shadow_basket if self.screener_memory.get(sym) and self.screener_memory[sym].get("prices") and sym not in active_syms}
                
                if current_prices:
                    async with self.db_semaphore:
                        try: 
                            if self.memory: await asyncio.wait_for(asyncio.to_thread(self.memory.resolve_batch_historical_predictions, list(current_prices.keys()), current_prices, 60.0, interval_mins), timeout=15.0)
                        except Exception as e: logger.debug(f"[X-RAY] Shadow resolution batch timeout: {e}", exc_info=True)
            except Exception as e: logger.error(f"[X-RAY] Shadow resolution daemon error: {e}", exc_info=True)

    async def run_omni_swarm_director(self):
        logger.info("🌪️ OMNI-SWARM DIRECTOR ONLINE: Monitoring Global Vectors.")
        banned_keywords = ["SOXL", "SPCX", "SKHY", "SNDK", "BANK", "MUUSDT", "BEAT", "MSTR", "ESPUSDT", "DEXE", "PUMP", "EUL", "XAU", "XAG", "USDC", "CLUSDT", "SSPCUSDT"]
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
                        
                        if bid > 0 and ask > bid and turnover >= 15_000_000.0 and spread_bps <= 4.0:
                            max_notional = self._get_max_affordable_notional()
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
            logger.info("🌌 V13.0 HYPER-SWARM REFRESH: Probing High-Velocity Matrix...")
            await self._fetch_exchange_tick_sizes()
            
            dynamic_basket = await self.executor.get_top_volatile_assets(limit=35, min_turnover=15_000_000.0)
            
            if not dynamic_basket or len(dynamic_basket) < 15:
                dynamic_basket = await self.executor.get_top_volatile_assets(limit=35, min_turnover=5_000_000.0)

            async with self.portfolio_state_lock:
                self.asset_basket = dynamic_basket[:15]
                self.shadow_basket = dynamic_basket[15:25] if len(dynamic_basket) >= 25 else dynamic_basket[15:]
                
            await self._prune_dead_symbols() 
            self._initialize_symbol_structures(self.asset_basket + self.shadow_basket)
            self.force_dna_refresh.set() 
            logger.info(f"✅ V13.0 MATRIX REFRESHED: {len(self.asset_basket)} Live Slots | {len(self.shadow_basket)} Shadow Slots Active.")
        except Exception as e:
            logger.error(f"[X-RAY] Universe refresher error: {e}", exc_info=True)

    async def _universe_refresher_loop(self):
        while True:
            await asyncio.sleep(900)
            await self.run_universe_refresher()

    async def stream_manager_loop(self):
        while True:
            stream_feed = HighVelocityMultiFeed(
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
    # 🚀 V13.0 EVENT-DRIVEN LIFECYCLE DAEMON
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

        try: await self.executor.safe_call(self.executor.client.cancel_all_orders, category="linear", symbol=ctx["symbol"])
        except Exception: pass
        return "ABORT"

    async def _state_settle_trade(self, ctx: dict):
        symbol, actual_entry, target_leverage = ctx["symbol"], ctx["actual_entry"], ctx["target_leverage"]
        
        # 🚀 MANDATORY INVARIANT: Verify position is truly 0.0 on exchange before settling
        for _ in range(6):
            pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
            pos_list = pos_res.get("result", {}).get("list", [])
            if not pos_list or float(pos_list[0].get("size", 0.0)) <= 0:
                break
            else:
                remaining_qty = float(pos_list[0].get("size", 0.0))
                side = "Sell" if pos_list[0]["side"] == "Buy" else "Buy"
                logger.warning(f"[X-RAY] ⚠️ Position {symbol} still has {remaining_qty} open. Force-liquidating to complete settlement.")
                await self.executor.safe_call(
                    self.executor.client.place_order,
                    category="linear", symbol=symbol, side=side,
                    orderType="Market", qty=str(remaining_qty), timeInForce="IOC", reduceOnly=True
                )
                await asyncio.sleep(1.0)

        net_pnl, real_outcome, slippage_bps, fees, exit_price = 0.0, "RECONCILED", 0.0, 0.0, actual_entry
        
        for _ in range(8):
            await asyncio.sleep(2.5)
            try: 
                closed_data = await self.executor.safe_call(self.executor.client.get_closed_pnl, category="linear", symbol=symbol, limit=10)
                closed_list = closed_data.get("result", {}).get("list", [])
                
                valid_close = None
                for pnl_record in closed_list:
                    if (time.time() - float(pnl_record.get("updatedTime", 0)) / 1000.0) < 300.0:
                        valid_close = pnl_record
                        break
                
                if valid_close:
                    net_pnl = float(valid_close.get("closedPnl", 0.0))
                    real_outcome = "PROFIT" if net_pnl > 0 else "LOSS"
                    capital_risked = (actual_entry * float(valid_close.get("qty", 1))) / target_leverage
                    self.risk_vault.update_kelly_metrics(net_pnl > 0, net_pnl / (capital_risked + 1e-9))
                    
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
        
        duration_mins = (time.time() - ctx["daemon_start_time"]) / 60.0
        self.log_to_wal_sync("settlement", [ctx["signal_id"], net_pnl, slippage_bps, real_outcome, ctx["exec_details"]])
        self.track_task(self._safe_telegram_dispatch(self.telegram.format_execution_receipt(symbol, net_pnl, slippage_bps, fees, duration_mins, net_pnl > 0), is_html=True))

        async with self.portfolio_state_lock: 
            self.active_positions_map.pop(symbol, None)
        self.risk_vault.update_position_ledger(symbol, 0.0)

    async def _position_lifecycle_daemon(self, symbol: str, signal_id: str, direction: str, current_price: float, atr: float, risk_matrix: dict, target_leverage: int = 8, market_regime: str = "TRENDING", is_recovery: bool = False, realigned_tp: float = None, dynamic_rr_ratio: float = 2.0, realigned_sl: float = None, historical_favorable_price: float = None):

        from core.intelligent_exit import IntelligentExitEngine, ExecutionGovernorFSM, PositionExitState, ThesisVector

        async with self.execution_semaphore:
            ctx = {
                "symbol": symbol, "signal_id": signal_id, "direction": direction, "is_buy": direction == "BUY",
                "current_price": current_price, "atr": atr, "target_leverage": target_leverage,
                "arrival_price": risk_matrix.get("arrival_price", current_price),
                "regime": market_regime, "daemon_start_time": time.time(),
                "actual_qty_filled": risk_matrix.get("size", 1.0),
                "stat_engine": self.stat_engines.get(symbol),
                "last_ob": {},
                "latest_tick_price": current_price,
                "current_vault_balance": self.global_state_cache.get("current_vault_balance", 12.0),
                "drawdown_pct": self.global_state_cache.get("drawdown_pct", 0.0),
                "active_positions_count": len(self.active_positions_map)
            }

            # 1. 🛡️ WAIT FOR PHYSICAL ENTRY CONFIRMATION FIRST
            loop_state = await self._state_verify_entry(ctx)
            if loop_state == "ABORT":
                self.risk_vault.update_position_ledger(symbol, 0.0)
                async with self.portfolio_state_lock: self.active_positions_map.pop(symbol, None)
                self.exit_states.pop(symbol, None)
                return

            async with self.portfolio_state_lock:
                self.active_positions_map[symbol] = direction

            # 2. 🚀 INITIALIZE PERSISTENT APEX OMEGA STATE WITH VERIFIED DATA
            if symbol not in self.exit_states:
                stat = self.stat_engines.get(symbol)
                ob = self.orderbook_snapshots.get(symbol, {})
                bid = max(ob.get("bid_size", 1.0), 1e-9)
                ask = max(ob.get("ask_size", 1.0), 1e-9)
                depth_ratio = bid / ask if direction == "BUY" else ask / bid

                entry_thesis = ThesisVector(features=np.array([
                    getattr(stat, "ofi_fast_z", 0.0) if stat else 0.0,
                    getattr(stat, "hawkes_z", 0.0) if stat else 0.0,
                    0.0,
                    depth_ratio,
                    getattr(stat, "inst_variance", 1e-6) if stat else 1e-6
                ]))

                self.exit_states[symbol] = PositionExitState(
                    position_id=signal_id,
                    entry_time=time.time(),
                    entry_price=ctx["actual_entry"],
                    exit_side="Sell" if direction == "BUY" else "Buy",
                    entry_balance=ctx["current_vault_balance"], # Used for Profit Lock
                    base_qty=ctx["actual_qty_filled"],          # Crucial for exchange sync
                    entry_thesis=entry_thesis,
                    thesis_inv_cov=np.eye(5)  # Identity matrix until empirical calibration
                )

            state = self.exit_states[symbol]

            # LOCK PROTECTED CONTEXT INJECTION
            async with self.portfolio_state_lock:
                self.active_contexts[symbol] = ctx

            if self.test_mode:
                async with self.portfolio_state_lock:
                    self.active_contexts.pop(symbol, None)
                    self.active_positions_map.pop(symbol, None)
                self.exit_states.pop(symbol, None)
                return

            try:
                # 3. SETUP DISASTER PROTECTIVE STOP (Deep Failsafe)
                actual_sl_distance = max(ctx["atr"] * self.live_params.get("sl_atr_mult", 2.5), ctx["actual_entry"] * 0.025)
                fail_sl = ctx["actual_entry"] - actual_sl_distance if ctx["is_buy"] else ctx["actual_entry"] + actual_sl_distance
                fail_tp = ctx["actual_entry"] + (actual_sl_distance * dynamic_rr_ratio * 3.0) if ctx["is_buy"] else ctx["actual_entry"] - (actual_sl_distance * dynamic_rr_ratio * 3.0)
                await self._amend_trailing_stop(symbol, fail_sl, fail_tp)

                loop_state = "ACTIVE_MONITORING"

                while loop_state == "ACTIVE_MONITORING":
                    await asyncio.sleep(1.0)

                    current_price = ctx["latest_tick_price"]

                    ob = self.orderbook_snapshots.get(symbol, {})
                    best_bid = ob.get("best_bid", current_price)
                    best_ask = ob.get("best_ask", current_price)

                    ctx["safe_c_price"] = best_bid if ctx["is_buy"] else best_ask
                    if ctx["stat_engine"] and ctx["stat_engine"].true_micro_price > 0:
                        ctx["safe_c_price"] = ctx["stat_engine"].true_micro_price

                    ctx["now"] = time.time()

                    # Live Portfolio Injection for the Commander
                    ctx["current_vault_balance"] = self.global_state_cache.get("current_vault_balance", ctx["current_vault_balance"])
                    ctx["drawdown_pct"] = self.global_state_cache.get("drawdown_pct", 0.0)
                    ctx["active_positions_count"] = len(self.active_positions_map)
                    ctx["last_ob"] = ob

                    # Update High Watermark (MFE)
                    if ctx["safe_c_price"] != current_price:
                        if ctx["is_buy"] and ctx["safe_c_price"] > ctx.get("max_favorable_price", 0.0):
                            ctx["max_favorable_price"] = ctx["safe_c_price"]
                        elif not ctx["is_buy"] and ctx["safe_c_price"] < ctx.get("max_favorable_price", 999999.0):
                            ctx["max_favorable_price"] = ctx["safe_c_price"]

                    # =================================================================
                    # ⚔️ APEX OMEGA V13 MASTER EVALUATION
                    # =================================================================
                    decision = IntelligentExitEngine.evaluate(ctx, state)

                    # 🛡️ DYNAMIC PROFIT LOCK RECONCILIATION
                    if decision.exchange_ts_price and decision.exchange_ts_price > 0:
                        if ctx["is_buy"]:
                            if fail_sl < decision.exchange_ts_price < current_price:
                                fail_sl = decision.exchange_ts_price
                                await self._amend_trailing_stop(symbol, fail_sl, fail_tp)
                        else:
                            if fail_sl > decision.exchange_ts_price > current_price:
                                fail_sl = decision.exchange_ts_price
                                await self._amend_trailing_stop(symbol, fail_sl, fail_tp)

                    # Execution routing
                    await ExecutionGovernorFSM.manage_execution(decision, state, ctx, self.executor)

                    # 🛑 TRUE EXIT CONDITION
                    if state.q_retained <= 0.01 and state.execution_state == "OBSERVE":
                        ctx["exit_trigger_price"] = current_price
                        break

                await self._state_settle_trade(ctx)

            except Exception as e:
                logger.error(f"[X-RAY] FSM Position daemon critical fault for {symbol}: {e}", exc_info=True)
                async with self.portfolio_state_lock: self.active_positions_map.pop(symbol, None)
                self.risk_vault.update_position_ledger(symbol, 0.0)
            finally:
                async with self.portfolio_state_lock:
                    self.active_contexts.pop(symbol, None)
                self.exit_states.pop(symbol, None)

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
        
        await self.run_universe_refresher()
        
        daemons = [
            self.run_db_wal_worker, self._batch_wal_flush_loop, self.run_dna_prewarmer, 
            self.stream_manager_loop, self.run_system_heartbeat, self.cleanup_stale_locks, 
            self.run_shadow_resolution_daemon, self._universe_refresher_loop, 
            self.auction_engine.run_global_capital_auction_worker, self.run_omni_swarm_director,            
            self.run_exchange_state_reconciliation_daemon,
            self.run_crowded_trade_oracle,
            self.run_fast_state_invariant_reconciliation
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