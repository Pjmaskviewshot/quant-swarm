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
from features.adaptive_engine import AdaptiveFeatureEngine
from features.vpin_clock import VolumeSynchronizedClock
from portfolio.risk_manager import InstitutionalRiskVault
from execution.sor import SmartOrderRouter

# External Connectors
from ingestion.multi_feed import HighVelocityMultiFeed
from services.bybit_v5 import BybitUnifiedExecutor
from services.telegram_ops import AsyncTelegramReporter
from services.adversarial_ai import AdversarialDebateMatrix
from services.data_feed import AsynchronousDataFeed
from services.tensor_oracle import CrossAssetTensorOracle

logging.getLogger("httpx").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(name)s] - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("QUANT_CORE.DISTRIBUTED_MAIN")

class ContinuousMicrostructureEngine:
    def __init__(self, memory_depth=500):
        self.prev_bid = 0.0
        self.prev_bid_size = 0.0
        self.prev_ask = 0.0
        self.prev_ask_size = 0.0
        
        self.ofi_fast_ewma = 0.0
        self.ofi_fast_ewmvar = 1.0
        self.ofi_fast_z = 0.0
        
        self.ofi_slow_ewma = 0.0
        self.ofi_slow_ewmvar = 1.0
        self.ofi_slow_z = 0.0
        
        self.micro_price_skew = 0.0
        
        self.last_trade_time = 0.0
        self.hawkes_pressure_state = 0.0
        self.hawkes_decay = 2.0  
        
        self.hawkes_ewma = 0.0
        self.hawkes_ewmvar = 1.0
        self.hawkes_z = 0.0
        
        self.hawkes_velocity = 0.0
        self.hawkes_acceleration = 0.0
        self.hawkes_z_prev = 0.0
        self.hawkes_v_prev = 0.0
        
        self.prices = deque(maxlen=memory_depth)
        self.log_returns = deque(maxlen=memory_depth)
        self.vol_ewma = 0.001
        self.inst_variance = 1e-6
        
        self.hurst = 0.5
        self.last_hurst_time = 0.0
        self.last_price_time = 0.0  
        self.shannon_entropy = 1.0
        
        self.alpha_fast = 0.15
        self.alpha_slow = 0.02
        
        self.weights = np.array([0.15, 0.15, 0.10, 0.10, 0.15, 0.10, 0.10, 0.05, 0.10]) 
        self.rms_decay = 0.90
        self.eg2 = np.zeros(9) + 1e-6
        self.learning_rate = 0.005   
        self.l1_lambda = 0.0001     
        self.l2_lambda = 0.0005     
        
        self.prediction_buffer = deque(maxlen=50000)
        self.sgd_updates = 0  
        
        self.validation_buffer = deque(maxlen=100)
        self.rolling_mse = 0.0

    def update_orderbook_pressure(self, best_bid: float, bid_vol: float, best_ask: float, ask_vol: float):
        delta_W = 0.0
        if best_bid > self.prev_bid: delta_W += bid_vol
        elif best_bid == self.prev_bid: delta_W += (bid_vol - self.prev_bid_size)
        else: delta_W -= self.prev_bid_size
            
        if best_ask < self.prev_ask: delta_W -= ask_vol
        elif best_ask == self.prev_ask: delta_W -= (ask_vol - self.prev_ask_size)
        else: delta_W += self.prev_ask_size
            
        self.prev_bid, self.prev_bid_size = best_bid, bid_vol
        self.prev_ask, self.prev_ask_size = best_ask, ask_vol
        
        self.ofi_fast_ewma = (1 - self.alpha_fast) * self.ofi_fast_ewma + self.alpha_fast * delta_W
        self.ofi_fast_ewmvar = (1 - self.alpha_fast) * self.ofi_fast_ewmvar + self.alpha_fast * (delta_W - self.ofi_fast_ewma)**2
        self.ofi_fast_z = (delta_W - self.ofi_fast_ewma) / (math.sqrt(self.ofi_fast_ewmvar) + 1e-9)
        
        self.ofi_slow_ewma = (1 - self.alpha_slow) * self.ofi_slow_ewma + self.alpha_slow * delta_W
        self.ofi_slow_ewmvar = (1 - self.alpha_slow) * self.ofi_slow_ewmvar + self.alpha_slow * (delta_W - self.ofi_slow_ewma)**2
        self.ofi_slow_z = (delta_W - self.ofi_slow_ewma) / (math.sqrt(self.ofi_slow_ewmvar) + 1e-9)
        
        current_mid = (best_bid + best_ask) / 2.0
        micro_price = (best_bid * ask_vol + best_ask * bid_vol) / (bid_vol + ask_vol + 1e-9)
        if current_mid > 0:
            self.micro_price_skew = ((micro_price - current_mid) / (current_mid + 1e-9)) * 10000.0 

    def update_trades(self, price: float, volume: float, is_buy: bool, current_time: float):
        if current_time - self.last_price_time >= 60.0:
            self.prices.append(price)
            if len(self.prices) > 2:
                ret = math.log(self.prices[-1] / (self.prices[-2] + 1e-9))
                if not math.isnan(ret) and not math.isinf(ret):
                    self.log_returns.append(ret)
                    self.vol_ewma = (1 - 0.01) * self.vol_ewma + 0.01 * abs(ret)
            if len(self.log_returns) > 10:
                self.inst_variance = np.var(list(self.log_returns)[-10:]) + 1e-9
            self.last_price_time = current_time

        volume_signed = volume if is_buy else -volume
        if self.last_trade_time > 0:
            dt = current_time - self.last_trade_time
            self.hawkes_pressure_state = self.hawkes_pressure_state * math.exp(-self.hawkes_decay * dt) + volume_signed
        else:
            self.hawkes_pressure_state = volume_signed
            
        self.last_trade_time = current_time
        
        self.hawkes_ewma = (1 - self.alpha_fast) * self.hawkes_ewma + self.alpha_fast * self.hawkes_pressure_state
        self.hawkes_ewmvar = (1 - self.alpha_slow) * self.hawkes_ewmvar + self.alpha_slow * (self.hawkes_pressure_state - self.hawkes_ewma)**2
        self.hawkes_z = (self.hawkes_pressure_state - self.hawkes_ewma) / (math.sqrt(self.hawkes_ewmvar) + 1e-9)

        self.hawkes_velocity = self.hawkes_z - self.hawkes_z_prev
        self.hawkes_acceleration = self.hawkes_velocity - self.hawkes_v_prev
        
        self.hawkes_z_prev = self.hawkes_z
        self.hawkes_v_prev = self.hawkes_velocity

        if len(self.prediction_buffer) > 0:
            while self.prediction_buffer and current_time - self.prediction_buffer[0][0] >= 300.0:
                old_time, old_price, features_array, old_pred_prob, virt_sl, virt_tp, action_dir = self.prediction_buffer.popleft()
                
                if price != old_price and old_price > 0:
                    y_true = 0.5 
                    
                    if action_dir == "BUY":
                        if price >= virt_tp: y_true = 1.0
                        elif price <= virt_sl: y_true = 0.0
                    else: 
                        if price <= virt_tp: y_true = 1.0
                        elif price >= virt_sl: y_true = 0.0
                    
                    if y_true == 0.5:
                        if action_dir == "BUY": y_true = 1.0 if price > old_price else 0.0
                        else: y_true = 1.0 if price < old_price else 0.0

                    error = old_pred_prob - y_true
                    
                    self.validation_buffer.append(error ** 2)
                    if len(self.validation_buffer) == 100:
                        self.rolling_mse = np.mean(self.validation_buffer)
                        if self.rolling_mse > 0.30:
                            logger.warning("📉 SGD DIVERGENCE: Resetting Elastic Net burn-in weights.")
                            self.weights = np.array([0.15, 0.15, 0.10, 0.10, 0.15, 0.10, 0.10, 0.05, 0.10])
                            self.eg2 = np.zeros(9) + 1e-6
                            self.sgd_updates = 0
                            self.validation_buffer.clear()
                            continue

                    grad = error * features_array
                    self.eg2 = (self.rms_decay * self.eg2) + ((1.0 - self.rms_decay) * (grad ** 2))
                    adjusted_lr = self.learning_rate / (np.sqrt(self.eg2) + 1e-8)
                    
                    l1_penalty = self.l1_lambda * np.sign(self.weights)
                    l2_penalty = self.l2_lambda * self.weights
                    
                    self.weights -= adjusted_lr * (grad + l1_penalty + l2_penalty)
                    self.sgd_updates += 1

    def extract_statistical_state(self, current_price: float, vpin_z: float, tensor_alpha: float, virtual_sl: float, virtual_tp: float) -> dict:
        current_time = time.time()
        
        if len(self.log_returns) > 30 and (current_time - self.last_hurst_time > 5.0):
            rets = np.array(self.log_returns)
            var_1 = np.var(rets)
            if var_1 > 1e-12:
                hurst_estimates = []
                for k in [2, 4, 8]:
                    if len(rets) >= k:
                        rets_k = np.convolve(rets, np.ones(k), 'valid')
                        vr = np.var(rets_k) / (k * var_1)
                        h_est = 0.5 + 0.5 * math.log(vr + 1e-9) / math.log(k)
                        hurst_estimates.append(max(0.1, min(0.9, h_est)))
                self.hurst = np.mean(hurst_estimates) if hurst_estimates else 0.5
            self.last_hurst_time = current_time

        ofi_delta_z = self.ofi_fast_z - self.ofi_slow_z
        liquidation_divergence = (self.hawkes_acceleration / 3.0) * (self.micro_price_skew / 10.0) * -1.0

        base_features = np.array([
            self.ofi_fast_z / 3.0,       
            ofi_delta_z / 6.0,           
            self.hawkes_z / 3.0,         
            self.micro_price_skew / 10.0,
            vpin_z / 4.0         
        ])
        
        cross_momentum = (self.ofi_fast_z / 3.0) * (self.hawkes_z / 3.0)            
        cross_skew_abs = (self.micro_price_skew / 10.0) * (ofi_delta_z / 6.0)       
        
        features = np.concatenate([base_features, [cross_momentum, cross_skew_abs, liquidation_divergence, tensor_alpha]])
        features = np.clip(features, -1.0, 1.0)
        
        feature_magnitudes = np.abs(features)
        attention_temperature = 0.35  
        exp_f = np.exp(feature_magnitudes / attention_temperature)
        attention_weights = exp_f / (np.sum(exp_f) + 1e-9)
        attended_features = features * attention_weights * len(features)

        if self.sgd_updates < 1000:
            active_weights = np.array([0.15, 0.15, 0.10, 0.10, 0.15, 0.10, 0.10, 0.05, 0.10])
        else:
            active_weights = self.weights

        logit = max(-5.0, min(5.0, np.dot(active_weights, attended_features)))
        T = 1.5
        p_up = 1.0 / (1.0 + math.exp(-logit / T))
        p_down = 1.0 - p_up
        
        action_dir = "BUY" if p_up > p_down else "SELL"
        prob_success = max(p_up, p_down)
        
        self.prediction_buffer.append((time.time(), current_price, attended_features, prob_success, virtual_sl, virtual_tp, action_dir))
        
        return {"p_up": p_up, "p_down": p_down, "entropy": self.shannon_entropy}

    def spread_adjusted_edge(self, current_price: float, action: str, spread_pct: float, expected_move_pct: float) -> float:
        base_edge_pct = expected_move_pct * abs(self.hawkes_velocity * 0.10) 
        net_edge_pct = base_edge_pct - spread_pct
        return net_edge_pct * 10000.0


class DistributedQuantEngine:
    def __init__(self):
        load_dotenv()
        self.test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
        
        if self.test_mode: logger.critical("⚠️ TEST MODE: Paper Trading Armed. No live executions will occur.")
        else: logger.critical("🟢 LIVE MODE: Capital Deployment Armed.")
        
        self.asset_basket: List[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        self.timeframe = os.getenv("TRADING_TIMEFRAME", "15")
        self.shadow_basket: List[str] = []
        
        self.db_semaphore = asyncio.Semaphore(5)
        self.wal_db_path = "quant_swarm_wal.db"
        
        # 🚀 V29.5 FIX: SQLite WAL Batching list
        self.wal_batch_queue = []
        
        self.tick_error_counts: Dict[str, List[float]] = {}
        self.circuit_breakers: Dict[str, float] = {}
        self.global_emergency_lock = False
        
        self.stream_restart_event = asyncio.Event()
        self.force_dna_refresh = asyncio.Event() 
        
        self.fsm = SystemStateMachine()
        self.memory = MemoryBank()
        self.risk_vault = InstitutionalRiskVault(max_drawdown_pct=0.25, max_single_position_risk_pct=0.15)
        self.ai_matrix = AdversarialDebateMatrix()
        self.data_feed = AsynchronousDataFeed(finnhub_key=os.getenv("FINNHUB_API_KEY", ""))
        self.tensor_oracle = CrossAssetTensorOracle()
        
        self.stat_engines: Dict[str, ContinuousMicrostructureEngine] = {} 
        self.vpin_clocks: Dict[str, VolumeSynchronizedClock] = {}
        self.feature_engines: Dict[str, AdaptiveFeatureEngine] = {}
        self.edge_gates: Dict[str, MicrostructureEdgeGate] = {}
        
        self.screener_memory: Dict[str, Dict[str, Any]] = {}
        self.screener_metrics: Dict[str, Dict[str, float]] = {}
        self.orderbook_snapshots: Dict[str, dict] = {}
        self.ram_dna_cache: Dict[str, dict] = {}
        
        self.volatility_baseline: Dict[str, float] = {}
        self.active_positions_lock: Dict[str, str] = {}  
        
        self.symbol_locks: Dict[str, asyncio.Lock] = {}
        # 🚀 V29.5 FIX: Per-Symbol Eval Semaphores preventing starvation
        self.eval_semaphores: Dict[str, asyncio.Semaphore] = {}
        
        self.daemon_tasks: Dict[str, asyncio.Task] = {}
        self.last_eval_time: Dict[str, float] = {}
        self._active_tasks = set()
        self._log_throttle_cache: Dict[str, float] = {}
        
        self.global_btc_ofi_z = 0.0
        self.tick_sizes: Dict[str, float] = {}
        self.global_state_cache = {"last_updated": 0.0}
        
        self.live_params = self._load_live_params()
        
        self._initialize_symbol_structures(self.asset_basket)
        self._load_sgd_state()

        self.telegram = AsyncTelegramReporter(token=os.getenv("TELEGRAM_BOT_TOKEN"), chat_id=os.getenv("TELEGRAM_CHAT_ID"))
        
        self.executor = BybitUnifiedExecutor(
            api_key=os.getenv("BYBIT_API_KEY"),
            api_secret=os.getenv("BYBIT_API_SECRET"),
            testnet=os.getenv("BYBIT_TESTNET", "false").lower() == "true",
            max_workers=8
        )
        self.sor = SmartOrderRouter(executor=self.executor, max_slippage_pct=0.005)

    def _get_vpin_bucket_size(self, symbol: str) -> float:
        if "BTC" in symbol: return 1_000_000.0
        if "ETH" in symbol: return 500_000.0
        if "SOL" in symbol: return 250_000.0
        return 100_000.0

    def _on_task_done(self, task):
        self._active_tasks.discard(task)
        if not task.cancelled() and task.exception():
            logger.error(f"❌ BACKGROUND TASK CRASHED: {task.exception()}", exc_info=task.exception())

    def track_task(self, coro: Any):
        task = asyncio.create_task(coro)
        self._active_tasks.add(task)
        task.add_done_callback(self._on_task_done)
        return task

    def _load_live_params(self) -> dict:
        default_params = {"prob_threshold": 0.55, "rr_ratio": 2.0, "sl_atr_mult": 1.5}
        try:
            if os.path.exists("params.json"):
                with open("params.json", "r") as f:
                    data = json.load(f)
                    logger.info(f"⚙️ PARAMETER BRIDGE: Loaded optimized configurations: {data}")
                    return {**default_params, **data}
        except Exception as e:
            logger.warning(f"⚠️ Could not load params.json, using defaults: {e}")
        return default_params

    def _save_sgd_state_sync(self):
        state = {}
        for sym, engine in self.stat_engines.items():
            state[sym] = {
                "weights": engine.weights.tolist(),
                "eg2": engine.eg2.tolist(),
                "sgd_updates": engine.sgd_updates
            }
        try:
            target_path = "sgd_state.json"
            fd, path = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(target_path)) or ".")
            with os.fdopen(fd, 'w') as f:
                json.dump(state, f)
            os.replace(path, target_path)
        except Exception as e:
            logger.debug(f"Failed to persist SGD state atomically: {e}")

    async def _save_sgd_state(self):
        await asyncio.to_thread(self._save_sgd_state_sync)

    def _load_sgd_state(self):
        try:
            if not os.path.exists("sgd_state.json"): return
            with open("sgd_state.json", "r") as f:
                state = json.load(f)
            for sym, data in state.items():
                if sym in self.stat_engines:
                    self.stat_engines[sym].weights = np.array(data["weights"])
                    self.stat_engines[sym].eg2 = np.array(data["eg2"])
                    self.stat_engines[sym].sgd_updates = data["sgd_updates"]
            logger.info("🧠 PRE-TRAINED MEMORY LOADED: Successfully recovered SGD state from disk.")
        except Exception as e:
            logger.debug(f"Could not load SGD state: {e}")

    def _initialize_symbol_structures(self, symbols: List[str]):
        for s in symbols:
            if s not in self.stat_engines: self.stat_engines[s] = ContinuousMicrostructureEngine()
            if s not in self.vpin_clocks: self.vpin_clocks[s] = VolumeSynchronizedClock(bucket_volume=self._get_vpin_bucket_size(s))
            if s not in self.feature_engines: self.feature_engines[s] = AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600)
            if s not in self.edge_gates: self.edge_gates[s] = MicrostructureEdgeGate(window_size=100)
            if s not in self.symbol_locks: self.symbol_locks[s] = asyncio.Lock()
            if s not in self.eval_semaphores: self.eval_semaphores[s] = asyncio.Semaphore(1)
            
            if s not in self.screener_memory:
                self.screener_memory[s] = {"prices": deque(maxlen=150), "highs": deque(maxlen=150), "lows": deque(maxlen=150), "volumes": deque(maxlen=150), "atr_history": deque(maxlen=100), "last_update_time": 0.0}
            if s not in self.screener_metrics: self.screener_metrics[s] = {"vol_mult": 1.0, "smoothed_price": 0.0}
            if s not in self.volatility_baseline: self.volatility_baseline[s] = 0.0
            if s not in self.ram_dna_cache: self.ram_dna_cache[s] = {"is_armed": True, "win_rate": 0.50}
            if s not in self.last_eval_time: self.last_eval_time[s] = 0.0 
            if s not in self.orderbook_snapshots: self.orderbook_snapshots[s] = {"best_bid": 0.0, "best_ask": 0.0}

    async def _safe_telegram_dispatch(self, message: str, is_html: bool = True, message_type: str = "SUCCESS"):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if is_html: await self.telegram.send_html_report(message)
                else: await self.telegram.log_message(message, message_type)
                return
            except Exception as e:
                logger.debug(f"Telegram dispatch failed: {e}")
                await asyncio.sleep(2 ** attempt)

    async def _fetch_exchange_tick_sizes(self):
        try:
            info = await self.executor.safe_call(self.executor.client.get_instruments_info, category="linear")
            for item in info.get("result", {}).get("list", []):
                self.tick_sizes[item.get("symbol")] = float(item.get("priceFilter", {}).get("tickSize", "0.0001"))
        except Exception as e:
            logger.error(f"Failed to fetch tick sizes: {e}", exc_info=True)

    async def synchronize_exchange_state(self):
        try:
            logger.info("📡 SYNCING EXCHANGE STATE: Scanning for orphaned live positions...")
            pos_response = await self.executor.safe_call(self.executor.client.get_positions, category="linear", settleCoin="USDT")
            active_orphans = [p for p in pos_response.get("result", {}).get("list", []) if float(p.get("size", 0.0)) > 0]
            
            if not active_orphans: return
            logger.critical(f"⚠️ RECOVERY ENGAGED: Found {len(active_orphans)} active trades left open.")
            
            for pos in active_orphans:
                symbol = pos["symbol"]
                self._initialize_symbol_structures([symbol]) 
                
                qty = float(pos["size"])
                entry_price = float(pos["avgPrice"])
                direction = "BUY" if pos["side"].upper() == "BUY" else "SELL"
                
                atr = entry_price * 0.015 
                
                self.active_positions_lock[symbol] = direction
                risk_matrix = {"allocated_value_usdt": qty * entry_price, "size": qty, "recommended_leverage": 8}
                
                daemon_task = self.track_task(self._position_lifecycle_daemon(
                    symbol, f"RECOVERY-{str(uuid.uuid4())[:8]}", direction, entry_price, atr, 
                    risk_matrix, 3, "RANGING"  
                ))
                self.daemon_tasks[symbol] = daemon_task
                
        except Exception as e:
            logger.error(f"Sync exchange state failed: {e}", exc_info=True)

    async def cleanup_stale_locks(self):
        while True:
            await asyncio.sleep(300) 
            try:
                for symbol in list(self.active_positions_lock.keys()):
                    active_daemon = self.daemon_tasks.get(symbol)
                    if active_daemon and not active_daemon.done():
                        continue 

                    if not hasattr(self.risk_vault, 'active_positions') or symbol not in self.risk_vault.active_positions:
                        pos_response = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                        if pos_response.get("retCode") == 0:
                            if not any(float(p.get("size", 0.0)) > 0 for p in pos_response.get("result", {}).get("list", [])):
                                self.active_positions_lock.pop(symbol, None)
            except Exception as e:
                logger.error(f"Stale lock cleanup error: {e}", exc_info=True)

    def log_to_wal_sync(self, action_type: str, args: list):
        self.wal_batch_queue.append((str(uuid.uuid4()), action_type, json.dumps(args), time.time()))

    async def _batch_wal_flush_loop(self):
        while True:
            await asyncio.sleep(5.0)
            if not self.wal_batch_queue: continue
            
            batch_to_process = self.wal_batch_queue[:]
            self.wal_batch_queue.clear()
            
            try:
                async with aiosqlite.connect(self.wal_db_path) as db:
                    await db.executemany(
                        "INSERT INTO pending_wal (id, action_type, payload, created_at) VALUES (?, ?, ?, ?)",
                        batch_to_process
                    )
                    await db.commit()
            except Exception as e:
                logger.error(f"Failed to write batch to local SQLite WAL: {e}")

    async def run_db_wal_worker(self):
        logger.info("🖲️ SQLITE WAL ENGINE ONLINE: Durable disk-backed execution logging.")
        try:
            async with aiosqlite.connect(self.wal_db_path) as db:
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS pending_wal (
                        id TEXT PRIMARY KEY,
                        action_type TEXT,
                        payload TEXT,
                        created_at REAL
                    )
                """)
                await db.commit()
        except Exception as e:
            logger.critical(f"FATAL: SQLite WAL Initialization Failed. Logging will drop! {e}")
            return

        while True:
            try:
                async with aiosqlite.connect(self.wal_db_path) as db:
                    await db.execute("""
                        DELETE FROM pending_wal WHERE id IN (
                            SELECT id FROM pending_wal ORDER BY created_at ASC LIMIT -1 OFFSET 50000
                        )
                    """)
                    await db.commit()
            
                async with aiosqlite.connect(self.wal_db_path) as db:
                    async with db.execute("SELECT id, action_type, payload FROM pending_wal ORDER BY created_at ASC LIMIT 5") as cursor:
                        rows = await cursor.fetchall()
                        
                for row in rows:
                    item_id, action_type, payload_str = row
                    args = json.loads(payload_str)
                    
                    try:
                        async with self.db_semaphore:
                            if action_type == "prediction":
                                await asyncio.wait_for(asyncio.to_thread(self.memory.commit_prediction, *args), timeout=10.0)
                            elif action_type == "settlement":
                                await asyncio.wait_for(asyncio.to_thread(self.memory.log_live_execution_result, *args), timeout=10.0)
                                
                        async with aiosqlite.connect(self.wal_db_path) as db:
                            await db.execute("DELETE FROM pending_wal WHERE id = ?", (item_id,))
                            await db.commit()
                            
                    except asyncio.TimeoutError:
                        logger.warning(f"WAL Sync Supabase Timeout. Keeping log buffered locally.")
                        break 
                    except Exception as e:
                        logger.warning(f"WAL Sync Failed. Retrying later: {e}")
                        
            except Exception:
                pass
                
            await asyncio.sleep(2.0)

    async def run_dna_prewarmer(self):
        logger.info("🔥 RAM PRE-WARMER ONLINE: Actively pre-fetching database edge logic.")
        while True:
            try:
                await asyncio.wait_for(self.force_dna_refresh.wait(), timeout=300.0)
                self.force_dna_refresh.clear()
            except asyncio.TimeoutError: pass
                
            try:
                async def _safe_fetch(sym, dna):
                    async with self.db_semaphore:
                        return await asyncio.wait_for(
                            asyncio.to_thread(self.memory.compute_latent_dna_edge, dna, 30),
                            timeout=10.0
                        )

                fetch_tasks = {}
                for symbol in list(self.asset_basket):
                    metrics = self.screener_metrics.get(symbol, {})
                    current_dna = {"vol_mult": metrics.get("vol_mult", 1.0), "z_obi": 0.0, "spread_pct": 0.001}
                    fetch_tasks[symbol] = _safe_fetch(symbol, current_dna)
                
                if not fetch_tasks: continue

                symbols = list(fetch_tasks.keys())
                results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
                
                for sym, result in zip(symbols, results):
                    if isinstance(result, Exception):
                        self.ram_dna_cache[sym] = self.ram_dna_cache.get(sym, {"is_armed": True, "win_rate": 0.50})
                    else:
                        self.ram_dna_cache[sym] = result
            except Exception as e:
                logger.error(f"DNA Pre-Warmer iteration failed: {e}", exc_info=True)

    async def run_shadow_resolution_daemon(self):
        logger.info("👻 GHOST FORENSICS ONLINE: Vectorized resolution engine activated.")
        
        interval_mins = 15.0
        try:
            interval_mins = float(self.timeframe)
        except Exception:
            mapping = {"D": 1440.0, "W": 10080.0, "M": 43200.0}
            interval_mins = mapping.get(str(self.timeframe).upper(), 15.0)

        while True:
            await asyncio.sleep(300) 
            try:
                current_prices = {}
                for sym in self.asset_basket + self.shadow_basket:
                    if self.screener_memory.get(sym) and self.screener_memory[sym].get("prices"):
                        current_prices[sym] = {
                            "prices": list(self.screener_memory[sym]["prices"]),
                            "highs": list(self.screener_memory[sym].get("highs", [])),
                            "lows": list(self.screener_memory[sym].get("lows", []))
                        }
                
                if current_prices:
                    async with self.db_semaphore:
                        await asyncio.wait_for(
                            asyncio.to_thread(
                                self.memory.resolve_batch_historical_predictions,
                                list(current_prices.keys()),
                                current_prices,
                                60.0,
                                interval_mins
                            ),
                            timeout=15.0
                        )
            except Exception as e:
                logger.error(f"Shadow resolution daemon failed: {e}", exc_info=True)

    async def run_ai_macro_evaluator(self):
        logger.info("🧠 AI MACRO LOOP ONLINE: Background narrative & structural evaluation started.")
        while True:
            await asyncio.sleep(60) 
            try:
                news_context = "Market data feed relying on local orderbook."
                try:
                    btc_snap = await asyncio.wait_for(self.data_feed.fetch_market_snapshot("BTCUSDT", "15"), timeout=5.0)
                    if btc_snap: news_context = btc_snap.get("news_context", news_context)
                except Exception:
                    pass

                for symbol in list(self.asset_basket):
                    if self.global_emergency_lock: continue

                    ob = self.orderbook_snapshots.get(symbol)
                    if not ob or ob["best_bid"] == 0.0: continue
                    current_price = (ob["best_bid"] + ob["best_ask"]) / 2.0

                    clock = self.vpin_clocks.get(symbol)
                    if not clock: continue
                    
                    vpin_hist = list(clock.vpin_history)
                    vpin_score = vpin_hist[-1] if vpin_hist else 0.0
                    vpin_z = 0.0
                    if len(vpin_hist) >= 20:
                        mean, std = np.mean(vpin_hist), np.std(vpin_hist) + 1e-9
                        vpin_z = (vpin_score - mean) / std
                        
                    dir_hist = list(clock.directional_imbalances)
                    directional_bias = np.mean(dir_hist) / (clock.bucket_volume + 1e-9) if dir_hist else 0.0
                    
                    vpin_data = {
                        "vpin_score": vpin_score,
                        "vpin_z_score": vpin_z,
                        "directional_bias": directional_bias,
                        "suggested_direction": "BUY" if directional_bias > 0 else "SELL",
                        "current_price": current_price,
                        "is_absorption_anomaly": False, 
                        "avg_trade_size": clock.bucket_volume / max(1, clock.current_bucket_ticks)
                    }

                    dna_stats = self.ram_dna_cache.get(symbol, {})
                    verdict = await self.ai_matrix.execute_debate_cycle(symbol, vpin_data, dna_stats, news_context)
                    
                    if verdict.get("schema_valid") and verdict.get("action") in ["BUY", "SELL", "HOLD"]:
                        raw_mult = 1.0 + (verdict.get("confidence", 0.0) * 0.20) if verdict.get("action") != "HOLD" else 1.0
                        mult = min(1.20, max(1.0, raw_mult))
                        self.fsm.update_ai_macro_state(symbol, verdict.get("action"), mult)
                        
                    await asyncio.sleep(2) 
                    
            except Exception as e:
                logger.error(f"AI Macro Evaluator Loop Error: {e}", exc_info=True)
                
            await asyncio.sleep(600)

    async def handle_incoming_orderbook_tick(self, depth_data: Dict[str, Any]):
        symbol = depth_data.get("s")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return

        bids, asks = depth_data.get("b", []), depth_data.get("a", [])
        if bids and asks:
            try:
                best_bid, bid_size = float(bids[0][0]), float(bids[0][1])
                best_ask, ask_size = float(asks[0][0]), float(asks[0][1])
                
                self.orderbook_snapshots[symbol] = {
                    "best_bid": best_bid, "bid_size": bid_size, 
                    "best_ask": best_ask, "ask_size": ask_size,
                    "bids": bids, "asks": asks
                }
                
                stat_engine = self.stat_engines.get(symbol)
                if stat_engine: 
                    stat_engine.update_orderbook_pressure(best_bid, bid_size, best_ask, ask_size)
                    if symbol == "BTCUSDT":
                        self.global_btc_ofi_z = stat_engine.ofi_fast_z
                        
            except Exception as e:
                logger.error(f"Orderbook L1 tick processing error for {symbol}: {e}", exc_info=True)

        is_snapshot = depth_data.get("type") == "snapshot"
        feature_engine = self.feature_engines.get(symbol)
        if feature_engine:
            feature_engine.push_orderbook_tick(bids, asks, is_snapshot=is_snapshot)
            
            edge_gate = self.edge_gates.get(symbol)
            if edge_gate and bids and asks:
                try:
                    f_bids, f_asks = feature_engine.get_deep_book_floats()
                    mid_price = (float(bids[0][0]) + float(asks[0][0])) / 2.0
                    edge_gate.update_orderbook_state(f_bids, f_asks, mid_price)
                except Exception as e:
                    logger.error(f"Deep-Book MLOFI calculation error for {symbol}: {e}", exc_info=True)

    async def handle_incoming_trade(self, trade_data: Dict[str, Any]):
        symbol = trade_data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return
        
        now = time.time()
        if self.circuit_breakers.get(symbol, 0.0) > now: return
        if self.global_emergency_lock: return

        try:
            price = float(trade_data.get("price", 0.0))
            volume = float(trade_data.get("size", 0.0))
            is_buy = (str(trade_data.get("side", "")).upper() == "BUY")
            exchange_timestamp = float(trade_data.get("timestamp", now * 1000)) / 1000.0
            
            # Phase 1: Lock-Free Mathematical State Updates
            self.tensor_oracle.ingest_tick(symbol, price, exchange_timestamp) 
            
            edge_gate = self.edge_gates.get(symbol)
            if edge_gate: edge_gate.update_trade_volume(volume)

            feature_engine = self.feature_engines.get(symbol)
            if feature_engine and hasattr(feature_engine, 'push_trade_tick'):
                feature_engine.push_trade_tick([trade_data])

            stat_engine = self.stat_engines.get(symbol)
            clock = self.vpin_clocks.get(symbol)
            if not stat_engine or not clock: return
            
            stat_engine.update_trades(price, volume, is_buy, exchange_timestamp)
            
            manifests = clock.process_tick(price, volume, not is_buy)
            valid_manifests = [m for m in manifests if m.get("valid")]
            
            if valid_manifests: vpin_z = float(valid_manifests[-1].get("vpin_z_score", 0.0))
            elif clock.vpin_history:
                hist = np.array(list(clock.vpin_history)[-200:])
                if len(hist) >= 50 and np.std(hist) > 0:
                    vpin_z = float((clock.vpin_history[-1] - np.mean(hist)) / (np.std(hist) + 1e-9))
                else:
                    vpin_z = 0.0
            else:
                vpin_z = 0.0
        
            throttle_time = 0.2 if abs(vpin_z) > 1.5 else 1.0
            if now - self.last_eval_time.get(symbol, 0.0) < throttle_time: return
            
            ob = self.orderbook_snapshots.get(symbol)
            if not ob or "bid_size" not in ob: return
            spread_cost = abs(ob["best_ask"] - ob["best_bid"]) / (price + 1e-9) if price > 0 else 0.001
            
            # Phase 2: Per-Symbol Semaphore Evaluation (Prevents BTC from starving Alts)
            async with self.eval_semaphores[symbol]:
                structural_verdict = edge_gate.evaluate_structural_edge(symbol, vpin_z)
                if structural_verdict["action"] == "HOLD": return
                
                raw_atr = feature_engine.get_computed_atr() if feature_engine and hasattr(feature_engine, 'get_computed_atr') else 0.0
                atr = raw_atr if raw_atr > 0 else price * 0.005
                sl_atr_mult = self.live_params.get("sl_atr_mult", 1.5)
                rr_ratio = self.live_params.get("rr_ratio", 2.0)
                
                # 🚀 V29.5 FIX: SL Floor explicitly set to 0.5% (Matches math comments)
                sl_dist_pct = max((atr * sl_atr_mult) / (price + 1e-9), 0.005)
                tp_dist_pct = sl_dist_pct * rr_ratio
                
                virtual_sl = price - (sl_dist_pct * price) if structural_verdict["action"] == "BUY" else price + (sl_dist_pct * price)
                virtual_tp = price + (tp_dist_pct * price) if structural_verdict["action"] == "BUY" else price - (tp_dist_pct * price)
                
                tensor_alpha = self.tensor_oracle.compute_lead_lag_signal(symbol)
                
                # Phase 3: Minimal Scope Atomic Guard (Prevents Double Entry)
                async with self.symbol_locks[symbol]:
                    if symbol in self.active_positions_lock: return
                    
                    if len(self.active_positions_lock) >= 5:
                        logger.debug(f"Rejecting {symbol} - Maximum Global Capacity Reached (5 open nodes)")
                        return

                    sgd_state = stat_engine.extract_statistical_state(price, vpin_z, tensor_alpha, virtual_sl, virtual_tp)
                    
                    p_up, p_down = sgd_state["p_up"], sgd_state["p_down"]
                    regime = feature_engine.detect_market_regime() if feature_engine else "TRENDING"
                    
                    prob_success = max(p_up, p_down)
                    action = "BUY" if p_up > p_down else "SELL"
                    
                    macro_state = self.fsm.get_ai_macro_state(symbol)
                    confidence_multiplier = macro_state.get("confidence_multiplier", 1.0)
                    prob_success = min(0.99, prob_success * confidence_multiplier)
                    
                    is_shadow_asset = symbol in self.shadow_basket
                    dna_stats = self.ram_dna_cache.get(symbol, {"is_armed": True, "win_rate": 0.50})
                    
                    if stat_engine.sgd_updates < 1000:
                        dna_stats["is_armed"] = False
                    
                    if is_shadow_asset or not dna_stats.get("is_armed", False):
                        if prob_success > 0.65: 
                            self.log_to_wal_sync("prediction", [str(uuid.uuid4()), now, price, action, prob_success, {"symbol": symbol, "market_regime": regime, "virtual_sl": virtual_sl, "virtual_tp": virtual_tp}, True])
                        return 
                    
                    min_threshold = max(self.live_params.get("prob_threshold", 0.55), dna_stats.get("win_rate", 0.60))
                    if prob_success < min_threshold: return 
                    
                    ev_pct = (prob_success * tp_dist_pct) - ((1.0 - prob_success) * sl_dist_pct)
                    net_edge_bps = stat_engine.spread_adjusted_edge(price, action, spread_cost, ev_pct)
                    if net_edge_bps <= 0.0: return 
                    
                    self.last_eval_time[symbol] = now
                    self.active_positions_lock[symbol] = action
                
                # Phase 4: Lock-Free Execution Dispatch
                logger.critical(f"🔬 INSTITUTIONAL TRIGGER // {symbol} [{regime}] | {action} | Fused-Prob: {prob_success:.2%} | Net Edge: {net_edge_bps:.2f} bps")
                self.track_task(self.execute_statistical_signal(symbol, action, price, prob_success, dna_stats, atr, regime, net_edge_bps, stat_engine.hawkes_z, 1.0))
                
        except Exception as e:
            self.tick_error_counts[symbol] = [t for t in self.tick_error_counts.get(symbol, []) if now - t < 60]
            self.tick_error_counts[symbol].append(now)
            if len(self.tick_error_counts[symbol]) > 5:
                self.circuit_breakers[symbol] = now + 300 
                logger.error(f"🛑 CIRCUIT BREAKER TRIGGERED for {symbol} due to 5+ execution errors. Paused for 5 minutes.")

    async def execute_statistical_signal(self, symbol: str, direction: str, current_price: float, confidence: float, dna_stats: dict, atr: float, regime: str, edge_bps: float, vol_z: float, vol_mult: float):
        try:
            signal_id = str(uuid.uuid4())
            sl_atr_mult, rr_ratio = self.live_params.get("sl_atr_mult", 1.5), self.live_params.get("rr_ratio", 2.0)
            
            sl_distance = max(atr * sl_atr_mult, current_price * 0.005)
            tp_distance = sl_distance * rr_ratio 
            
            tick_dec = Decimal(str(self.tick_sizes.get(symbol, 0.0001)))
            def align_price(p: float) -> str: return str(Decimal(str(p)).quantize(tick_dec, rounding=ROUND_HALF_UP))
            
            raw_sl = current_price - sl_distance if direction == "BUY" else current_price + sl_distance
            raw_tp = current_price + tp_distance if direction == "BUY" else current_price - tp_distance
            initial_sl_price, target_tp_price = float(align_price(raw_sl)), float(align_price(raw_tp))

            try: balance = await self.executor.get_wallet_balance_usdt()
            except Exception: return
                
            start_bal = self.global_state_cache.get("start_of_day_balance", balance)
            if start_bal > 0 and balance < (start_bal * 0.90): return
            
            correlation_penalty = 1.0
            if hasattr(self.risk_vault, "correlation_groups"):
                for group, assets in self.risk_vault.correlation_groups.items():
                    if symbol in assets:
                        active_correlated = sum(1 for a, a_dir in self.active_positions_lock.items() if a in assets and a != symbol and a_dir == direction)
                        if active_correlated > 0: correlation_penalty = max(0.25, 1.0 - (active_correlated * 0.25))
            
            edge = confidence - 0.50
            if edge <= 0.02: return
                
            base_risk = 0.01
            risk_multiplier = edge / 0.10  
            account_scaling = 0.75 if balance < 100.0 else 1.0
            fractional_risk = max(0.005, min(0.025, base_risk * risk_multiplier * account_scaling * correlation_penalty))

            if balance < 1.0: return

            dollar_risk = balance * fractional_risk
            position_size = max(dollar_risk / sl_distance, 6.00 / (current_price + 1e-9))
            notional = position_size * current_price

            if not self.risk_vault.evaluate_portfolio_safety(balance, notional, symbol): return

            target_leverage = self.risk_vault.calculate_dynamic_leverage(notional, balance, base_leverage=3, hard_cap=5, sl_distance_pct=(sl_distance / current_price))
            
            if self.test_mode:
                execution_success = True
            else:
                try:
                    await self.executor.safe_call(self.executor.adjust_leverage, symbol, target_leverage)
                    await asyncio.sleep(0.2) 
                except Exception: return

                feature_engine = self.feature_engines.get(symbol)
                current_depth = feature_engine.get_orderbook_snapshot() if feature_engine and hasattr(feature_engine, 'get_orderbook_snapshot') else {"bids": [[current_price, 1]], "asks": [[current_price, 1]]}

                if regime == "TRENDING": execution_success = await self.sor.execute_iceberg_block(symbol=symbol, direction=direction, total_qty=position_size, current_mid_price=current_price, stop_loss=initial_sl_price, take_profit=target_tp_price, depth_snapshot=current_depth, vol_z=vol_z, vol_mult=vol_mult, feature_engine=feature_engine)
                else: execution_success = await self.sor.execute_mean_reversion_bracket(symbol=symbol, direction=direction, total_qty=position_size, current_mid_price=current_price, stop_loss=initial_sl_price, take_profit=target_tp_price, depth_snapshot=current_depth, vol_z=vol_z, vol_mult=vol_mult, feature_engine=feature_engine)
            
            if not execution_success: return 
                
            if not self.test_mode:
                self.log_to_wal_sync("prediction", [signal_id, time.time(), current_price, direction, confidence, {"symbol": symbol, "market_regime": regime, "virtual_sl": initial_sl_price, "virtual_tp": target_tp_price}, False])
                
            self.risk_vault.update_position_ledger(symbol, notional)
            self.daemon_tasks[symbol] = self.track_task(self._position_lifecycle_daemon(symbol, signal_id, direction, current_price, atr, {"allocated_value_usdt": notional, "size": position_size}, target_leverage, regime))
            
        except Exception: self.active_positions_lock.pop(symbol, None)

    async def handle_incoming_kline_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return
        self._initialize_symbol_structures([symbol]) 
            
        interval = data["interval"]
        candle = data["candle_data"]
        c_open, c_high, c_low, c_close, c_vol = map(float, [candle.get("open", 0), candle.get("high", 0), candle.get("low", 0), candle.get("close", 0), candle.get("volume", 0)])

        async with self.symbol_locks[symbol]:
            feature_engine = self.feature_engines.get(symbol)
            if feature_engine:
                feature_engine.update_multi_timeframe_candle(timeframe=interval, open_p=c_open, high_p=c_high, low_p=c_low, close_p=c_close, volume=c_vol)
                if symbol in self.screener_memory:
                    self.screener_memory[symbol].setdefault("highs", deque(maxlen=150)).append(c_high)
                    self.screener_memory[symbol].setdefault("lows", deque(maxlen=150)).append(c_low)
                    self.screener_memory[symbol].setdefault("prices", deque(maxlen=150)).append(c_close)
                    self.screener_memory[symbol]["last_update_time"] = time.time()

    async def handle_incoming_basket_screener_update(self, data: Dict[str, Any]): pass

    async def run_universe_refresher(self):
        try:
            await self._fetch_exchange_tick_sizes()
            full_market = await self.executor.get_top_volatile_assets(limit=100, min_turnover=10_000_000)
            if len(full_market) < 25: full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
        except Exception: full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
            
        if "BTCUSDT" in full_market: full_market.remove("BTCUSDT")
        
        new_core_basket = ["BTCUSDT"] + [s for s in self.active_positions_lock.keys() if s != "BTCUSDT"]
        for sym in full_market:
            if sym not in new_core_basket and len(new_core_basket) < 25: new_core_basket.append(sym)
                
        self.asset_basket = new_core_basket
        self.shadow_basket = [s for s in full_market if s not in self.asset_basket]
        if len(self.shadow_basket) < 10: self.shadow_basket.extend([s for s in ["XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT"] if s not in self.shadow_basket])
        
        new_vpin_clocks, new_stat, new_dna_cache, new_last_eval, new_orderbooks, new_feature_engines, new_edge_gates, new_symbol_locks, new_eval_semaphores = {}, {}, {}, {}, {}, {}, {}, {}, {}
        
        for s in self.asset_basket + self.shadow_basket:
            new_vpin_clocks[s] = self.vpin_clocks.get(s, VolumeSynchronizedClock(bucket_volume=self._get_vpin_bucket_size(s)))
            new_stat[s] = self.stat_engines.get(s, ContinuousMicrostructureEngine())
            new_dna_cache[s] = self.ram_dna_cache.get(s, {})
            new_last_eval[s] = self.last_eval_time.get(s, 0.0)
            new_orderbooks[s] = self.orderbook_snapshots.get(s, {"best_bid": 0.0, "best_ask": 0.0})
            new_feature_engines[s] = self.feature_engines.get(s, AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600))
            new_edge_gates[s] = self.edge_gates.get(s, MicrostructureEdgeGate(window_size=100))
            new_symbol_locks[s] = self.symbol_locks.get(s, asyncio.Lock())
            new_eval_semaphores[s] = self.eval_semaphores.get(s, asyncio.Semaphore(1))
            
        self.vpin_clocks, self.stat_engines, self.ram_dna_cache, self.last_eval_time, self.orderbook_snapshots, self.feature_engines, self.edge_gates, self.symbol_locks, self.eval_semaphores = new_vpin_clocks, new_stat, new_dna_cache, new_last_eval, new_orderbooks, new_feature_engines, new_edge_gates, new_symbol_locks, new_eval_semaphores

        try:
            historical_data = {sym: list(self.screener_memory[sym]["prices"]) for sym in self.asset_basket if self.screener_memory.get(sym) and len(self.screener_memory[sym].get("prices", [])) > 30}
            if len(historical_data) >= 2: self.risk_vault.update_correlation_matrix(historical_data)
        except Exception: pass

        self.stream_restart_event.set()
        self.force_dna_refresh.set() 

    async def _universe_refresher_loop(self):
        while True:
            await asyncio.sleep(14400)
            await self.run_universe_refresher()

    async def stream_manager_loop(self):
        while True:
            stream_feed = HighVelocityMultiFeed(basket=self.asset_basket + self.shadow_basket[:10], intervals=["1", "5", "15"], orderbook_callback=self.handle_incoming_orderbook_tick, screener_callback=self.handle_incoming_basket_screener_update, kline_callback=self.handle_incoming_kline_update, trade_callback=self.handle_incoming_trade, engine_reference=self)
            stream_task = asyncio.create_task(stream_feed.initialize_multiplexed_stream())
            def _on_stream_done(t):
                if not t.cancelled() and not self.stream_restart_event.is_set(): self.stream_restart_event.set()
            stream_task.add_done_callback(_on_stream_done)
            await self.stream_restart_event.wait()
            stream_task.cancel()
            stream_feed.terminate_all_feeds()
            self.stream_restart_event.clear()
            await asyncio.sleep(2)

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
                try: current_vault_balance = await self.executor.get_wallet_balance_usdt()
                except Exception: continue

                if "wallet_baseline" not in self.global_state_cache: self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01)
                
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                current_day = now_utc.strftime("%Y-%m-%d")
                if self.global_state_cache.get("current_day") != current_day:
                    self.global_state_cache["current_day"] = current_day
                    self.global_state_cache["start_of_day_balance"] = current_vault_balance

                today_start_iso = now_utc.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                try:
                    def _fetch(): return self.memory.supabase.table("quantitative_ledger").select("market_regime, net_pnl, symbol, predicted_direction, actual_outcome").eq("resolved", True).eq("is_shadow", False).gte("timestamp", today_start_iso).execute()
                    async with self.db_semaphore: data = (await asyncio.to_thread(_fetch)).data or []
                except Exception: data = []

                actual_net_pnl = current_vault_balance - self.global_state_cache.get("start_of_day_balance", current_vault_balance)
                baseline = self.global_state_cache["wallet_baseline"]
                if current_vault_balance > baseline:
                    self.global_state_cache["wallet_baseline"] = current_vault_balance
                    baseline = current_vault_balance
                    
                drawdown_pct = max(0.0, (baseline - current_vault_balance) / baseline)
                
                if drawdown_pct >= 0.25:
                    await self._safe_telegram_dispatch(f"🚨 <b>EMERGENCY DRAWDOWN BREAKER TRIPPED</b>\nDrawdown: {drawdown_pct:.2%}. Engine shutting down.", is_html=True)
                    
                    # 🚀 V29.5 FIX: Replaced crashed db_wal_queue reference with proper SQLite query
                    try:
                        async with aiosqlite.connect(self.wal_db_path) as db:
                            async with db.execute("SELECT COUNT(*) FROM pending_wal") as cursor:
                                remaining = (await cursor.fetchone())[0]
                                if remaining > 0:
                                    logger.warning(f"⏳ {remaining} items in SQLite WAL buffer. Force shutting down.")
                    except Exception: pass
                    
                    await self.graceful_shutdown()
                    sys.exit(1)
                
                filled_blocks = min(10, int(drawdown_pct * 10))
                self.global_state_cache.update({"drawdown_bar": "🟢" * (10 - filled_blocks) + "🔴" * filled_blocks, "actual_net_pnl": actual_net_pnl, "current_vault_balance": current_vault_balance, "drawdown_pct": drawdown_pct, "daily_data": data})

            if loop_counter % 10 == 0:
                cv, actual, dd, dd_bar, data = self.global_state_cache.get("current_vault_balance", 0.0), self.global_state_cache.get("actual_net_pnl", 0.0), self.global_state_cache.get("drawdown_pct", 0.0), self.global_state_cache.get("drawdown_bar", "🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢"), self.global_state_cache.get("daily_data", [])
                try:
                    regime_stats = {}
                    for row in data:
                        regime, pnl = row.get("market_regime", "UNKNOWN"), float(row.get("net_pnl", 0.0))
                        if regime not in regime_stats: regime_stats[regime] = {"count": 0, "pnl": 0.0}
                        regime_stats[regime]["count"] += 1
                        regime_stats[regime]["pnl"] += pnl
                    regime_text = "".join([f"• {'🕸️' if r == 'RANGING' else '🚀'} <b>{r}:</b> <code>{s['count']} trades</code> | <code>{s['pnl']:+.4f} (Live PnL)</code>\n" for r, s in regime_stats.items()]) or "• <i>No resolved live metrics recorded today yet.</i>\n"
                    recent_trades = "".join([f"{'✅' if t.get('actual_outcome') == 'WIN' else '🔴'} {t.get('symbol')} | {t.get('predicted_direction')} | PnL: {float(t.get('net_pnl', 0)):+.4f}\n" for t in sorted(data, key=lambda x: x.get('timestamp', ''))[-5:]]) or "• <i>Waiting for first live execution cycle...</i>\n"
                except Exception: regime_text, recent_trades = "• ⚠️ <i>Supabase ledger context error.</i>\n", "• <i>Unavailable</i>\n"

                report = (
                    f"💎 <b>𝗣██𝗔𝗦𝗞 𝗘𝗠𝗣𝗜𝗥𝗘 | 𝗤𝗨𝗔𝗡𝗧 𝗦𝗪𝗔𝗥𝗠 (V29.5 PROD APEX)</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏱️ <b>𝗨𝗽𝘁𝗶𝗺𝗲:</b> <code>{uptime_hours:.2f} Hours</code> | 🛰️ <b>𝗡𝗼𝗱𝗲𝘀:</b> <code>{len(self.asset_basket)} Live</code>\n\n"
                    f"⚙️ <b>𝗘𝗡𝗚𝗜𝗡𝗘 𝗦𝗧𝗔𝗧𝗨𝗦: 𝗣𝗿𝗼𝗱𝘂𝗰𝘁𝗶𝗼𝗻 𝗚𝗿𝗮𝗱𝗲 𝗜𝗻𝗳𝗿𝗮𝘀𝘁𝗿𝘂𝗰𝘁𝘂𝗿𝗲</b>\n"
                    f"• Signal Engine:   <code>Softmax Transformer Head + Hawkes Jacobian</code>\n"
                    f"• Market Lead:     <code>BTC Cross-Asset Lead-Lag Tensor Oracle</code>\n"
                    f"• AI Macro Loop:   <code>Adversarial Debate Matrix (Active)</code>\n"
                    f"• Execution Guard: <code>Strict Atomic Routing + WAL Batching</code>\n"
                    f"• Risk Engine:     <code>Fractional Sizing (Max 5x Lev)</code>\n\n"
                    f"💵 <b>𝗙𝗜𝗡𝗔𝗡𝗖𝗜𝗔🇱 𝗩𝗔𝗨🇱𝗧 𝗣𝗥𝗢𝗙𝗜🇱𝗘</b>\n"
                    f"• Total Liquidity: <code>{cv:.4f} USDT</code>\n"
                    f"• Session Return:  <code>{actual:+.4f} USDT</code>\n"
                    f"• Peak Drawdown:   <code>{dd:.2%}</code>\n"
                    f"• Risk Buffer:     <code>[{dd_bar}]</code>\n\n"
                    f"🔬 <b>𝗗𝗔𝗜🇱𝗬 𝗥𝗘𝗚𝗜𝗠𝗘 𝗣𝗥𝗢𝗙𝗜🇱𝗘:</b>\n{regime_text}\n"
                    f"🏁 <b>𝗥𝗘𝗖𝗘𝗡𝗧 𝗦𝗧𝗔𝗧𝗘 𝗠𝗔𝗧𝗨𝗥𝗜𝗧𝗜𝗘𝗦</b>\n{recent_trades}"
                )
                self.track_task(self._safe_telegram_dispatch(report, is_html=True))

    async def _position_lifecycle_daemon(self, symbol: str, signal_id: str, direction: str, current_price: float, atr: float, risk_matrix: dict, target_leverage: int = 8, market_regime: str = "TRENDING"):
        exec_details = {"leverage": target_leverage, "execution_mode": "RECOVERY" if "RECOVERY" in signal_id else ("GHOST" if self.test_mode else "LIVE")}
        daemon_start_time = time.time()
        max_lifetime_seconds = 14400 

        if self.test_mode:
            await asyncio.sleep(60)
            self.active_positions_lock.pop(symbol, None)
            return

        try:
            order_filled = False
            actual_entry = current_price
            for _ in range(5):  
                await asyncio.sleep(3)
                try:
                    pos_response = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                    positions = pos_response.get("result", {}).get("list", [])
                    if positions and float(positions[0].get("size", 0.0)) > 0:
                        order_filled = True
                        actual_entry = float(positions[0].get("avgPrice", current_price))
                        break
                except Exception: continue

            if not order_filled:
                try: await self.executor.safe_call(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
                except Exception: pass
                self.risk_vault.update_position_ledger(symbol, -risk_matrix['allocated_value_usdt'])
                self.active_positions_lock.pop(symbol, None)
                return

            tick_dec = Decimal(str(self.tick_sizes.get(symbol, 0.0001)))
            actual_sl_distance = max(atr * self.live_params.get("sl_atr_mult", 1.5), actual_entry * 0.005)
            actual_tp_distance = actual_sl_distance * self.live_params.get("rr_ratio", 2.0)
            
            realigned_sl = actual_entry - actual_sl_distance if direction == "BUY" else actual_entry + actual_sl_distance
            realigned_tp = actual_entry + actual_tp_distance if direction == "BUY" else actual_entry - actual_tp_distance
            def align_price(p: float) -> str: return str(Decimal(str(p)).quantize(tick_dec, rounding=ROUND_HALF_UP))
            realigned_sl_str, realigned_tp_str = align_price(realigned_sl), align_price(realigned_tp)
            
            stops_verified = False
            for attempt in range(3):
                try:
                    await self.executor.safe_call(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, takeProfit=realigned_tp_str, stopLoss=realigned_sl_str)
                    pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                    if float(pos_res.get("result", {}).get("list", [{}])[0].get("stopLoss", 0.0)) > 0:
                        stops_verified = True
                        break
                except Exception: await asyncio.sleep(2)
                
            if not stops_verified:
                try:
                    pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                    pos_list = pos_res.get("result", {}).get("list", [])
                    if pos_list and float(pos_list[0].get("size", 0.0)) > 0:
                        await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side="Sell" if pos_list[0].get("side") == "Buy" else "Buy", orderType="Market", qty=str(float(pos_list[0].get("size"))), timeInForce="IOC", reduceOnly=True)
                except Exception: pass
                self.active_positions_lock.pop(symbol, None)
                return

            act_raw = actual_entry + (atr * 0.8) if direction == "BUY" else actual_entry - (atr * 0.8)
            try: await self.executor.safe_call(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, takeProfit=realigned_tp_str, stopLoss=realigned_sl_str, trailingStop=align_price(atr * 1.5), activePrice=align_price(act_raw))
            except Exception: pass

            while True: 
                await asyncio.sleep(20)
                if time.time() - daemon_start_time > max_lifetime_seconds:
                    try:
                        pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                        if float(pos_res.get("result", {}).get("list", [{}])[0].get("size", 0.0)) > 0:
                            await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side="Sell" if pos_res["result"]["list"][0]["side"] == "Buy" else "Buy", orderType="Market", qty=str(float(pos_res["result"]["list"][0]["size"])), timeInForce="IOC", reduceOnly=True)
                    except Exception: pass
                    self.active_positions_lock.pop(symbol, None)
                    self.risk_vault.update_position_ledger(symbol, 0.0)
                    break

                try:
                    pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                    position_gone = (not pos_res.get("result", {}).get("list", [])) or float(pos_res["result"]["list"][0].get("size", 0.0)) == 0.0
                except Exception: position_gone = False

                try:
                    settlement = await self.executor.check_recent_settlement(symbol, 300) 
                    if settlement.get("closed"):
                        self.log_to_wal_sync("settlement", [signal_id, float(settlement.get('pnl', 0.0)), actual_entry - current_price if direction == "BUY" else current_price - actual_entry, settlement['outcome'], exec_details])
                        self.risk_vault.update_position_ledger(symbol, 0.0)
                        break
                except Exception: pass

                if position_gone:
                    try: net_pnl = float((await self.executor.safe_call(self.executor.client.get_closed_pnl, category="linear", symbol=symbol, limit=5)).get("result", {}).get("list", [])[0].get("closedPnl", 0.0))
                    except Exception: net_pnl = 0.0
                    self.log_to_wal_sync("settlement", [signal_id, net_pnl, 0.0, "RECONCILED", exec_details])
                    self.risk_vault.update_position_ledger(symbol, 0.0)
                    break

        except Exception:
            try:
                pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                if float(pos_res.get("result", {}).get("list", [{}])[0].get("size", 0.0)) > 0:
                    await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side="Sell" if pos_res["result"]["list"][0]["side"] == "Buy" else "Buy", orderType="Market", qty=str(float(pos_res["result"]["list"][0]["size"])), timeInForce="IOC", reduceOnly=True)
            except Exception: pass
        finally:
            self.active_positions_lock.pop(symbol, None)
            self.risk_vault.update_position_ledger(symbol, 0.0)

    async def graceful_shutdown(self):
        for symbol in list(self.active_positions_lock.keys()):
            try:
                await self.executor.safe_call(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
                pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                if float(pos_res.get("result", {}).get("list", [{}])[0].get("size", 0.0)) > 0:
                    await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side="Sell" if pos_res["result"]["list"][0]["side"] == "Buy" else "Buy", orderType="Market", qty=str(float(pos_res["result"]["list"][0]["size"])), timeInForce="IOC", reduceOnly=True)
            except Exception: pass
            
        try:
            async with aiosqlite.connect(self.wal_db_path) as db:
                async with db.execute("SELECT COUNT(*) FROM pending_wal") as cursor:
                    count = (await cursor.fetchone())[0]
            logger.info(f"⏳ WAL Engine offline. {count} items remaining in local disk buffer for next reboot.")
        except Exception: pass
            
        if hasattr(self, 'telegram'): await self.telegram.close()

    async def _safe_daemon_run(self, coro_func):
        while True:
            try: await coro_func()
            except asyncio.CancelledError: break
            except Exception: await asyncio.sleep(5)

    async def run_engine_forever(self):
        self.global_emergency_lock = False
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
        
        try: full_market = await self.executor.get_top_volatile_assets(limit=100, min_turnover=10_000_000)
        except Exception: full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
            
        if boot_basket := full_market[:25]:
            if "BTCUSDT" in boot_basket: boot_basket.remove("BTCUSDT")
            self.asset_basket = ["BTCUSDT"] + boot_basket[:24]
            self._initialize_symbol_structures(self.asset_basket)
        
        daemons = [
            self.run_db_wal_worker, self._batch_wal_flush_loop, self.run_dna_prewarmer, 
            self.stream_manager_loop, self.run_system_heartbeat, self.cleanup_stale_locks, 
            self.run_shadow_resolution_daemon, self.run_ai_macro_evaluator, self._universe_refresher_loop
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