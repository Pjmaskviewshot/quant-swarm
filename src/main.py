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

# External Connectors & AI
from ingestion.multi_feed import HighVelocityMultiFeed
from services.bybit_v5 import BybitUnifiedExecutor
from services.telegram_ops import AsyncTelegramReporter
from services.adversarial_ai import AdversarialDebateMatrix
from services.data_feed import AsynchronousDataFeed

# Clean up HTTP logs
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
        
        self.trade_timestamps = deque()
        self.hawkes_decay = 2.0  
        self.hawkes_ewma = 0.0
        self.hawkes_ewmvar = 1.0
        self.hawkes_z = 0.0
        
        self.prices = deque(maxlen=memory_depth)
        self.log_returns = deque(maxlen=memory_depth)
        self.vol_ewma = 0.001
        self.inst_variance = 1e-6
        self.hurst = 0.5
        self.shannon_entropy = 1.0
        
        self.alpha_fast = 0.15
        self.alpha_slow = 0.02
        
        self.weights = np.array([0.20, 0.15, 0.15, 0.10, 0.15, 0.10, 0.10, 0.05]) 
        self.rms_decay = 0.90
        self.eg2 = np.zeros(8) + 1e-6
        
        self.learning_rate = 0.005   
        self.l1_lambda = 0.0001     
        self.l2_lambda = 0.0005     
        
        self.prediction_buffer = deque(maxlen=1000)
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
        self.prices.append(price)
        if len(self.prices) > 2:
            ret = math.log(self.prices[-1] / (self.prices[-2] + 1e-9))
            if not math.isnan(ret) and not math.isinf(ret):
                self.log_returns.append(ret)
                self.vol_ewma = (1 - 0.01) * self.vol_ewma + 0.01 * abs(ret)
                
        if len(self.log_returns) > 10:
            self.inst_variance = np.var(list(self.log_returns)[-10:]) + 1e-9

        self.trade_timestamps.append((current_time, volume if is_buy else -volume))
        while self.trade_timestamps and current_time - self.trade_timestamps[0][0] >= 60:
            self.trade_timestamps.popleft()
            
        hawkes_pressure = 0.0
        for t_time, t_vol in self.trade_timestamps:
            hawkes_pressure += t_vol * math.exp(-self.hawkes_decay * (current_time - t_time))
            
        self.hawkes_ewma = (1 - self.alpha_fast) * self.hawkes_ewma + self.alpha_fast * hawkes_pressure
        self.hawkes_ewmvar = (1 - self.alpha_slow) * self.hawkes_ewmvar + self.alpha_slow * (hawkes_pressure - self.hawkes_ewma)**2
        self.hawkes_z = (hawkes_pressure - self.hawkes_ewma) / (math.sqrt(self.hawkes_ewmvar) + 1e-9)

        if len(self.prediction_buffer) > 0:
            while self.prediction_buffer and current_time - self.prediction_buffer[0][0] >= 300.0:
                old_time, old_price, features_array, old_pred_prob = self.prediction_buffer.popleft()
                
                if price != old_price and old_price > 0:
                    y_true = 1.0 if price > old_price else 0.0
                    error = old_pred_prob - y_true
                    
                    self.validation_buffer.append(error ** 2)
                    if len(self.validation_buffer) == 100:
                        self.rolling_mse = np.mean(self.validation_buffer)
                        if self.rolling_mse > 0.30:
                            logger.warning("📉 SGD DIVERGENCE: Resetting Elastic Net burn-in weights.")
                            self.weights = np.array([0.20, 0.15, 0.15, 0.10, 0.15, 0.10, 0.10, 0.05])
                            self.eg2 = np.zeros(8) + 1e-6
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

    def extract_statistical_state(self, vpin_z: float, btc_lead_ofi_z: float = 0.0) -> dict:
        if len(self.log_returns) > 30:
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

        ofi_delta_z = self.ofi_fast_z - self.ofi_slow_z
        
        base_features = np.array([
            self.ofi_fast_z / 3.0,       
            ofi_delta_z / 6.0,           
            self.hawkes_z / 3.0,         
            self.micro_price_skew / 10.0,
            btc_lead_ofi_z / 3.0         
        ])
        
        cross_momentum = (self.ofi_fast_z / 3.0) * (self.hawkes_z / 3.0)            
        cross_btc_sync = (btc_lead_ofi_z / 3.0) * (self.ofi_fast_z / 3.0)           
        cross_skew_abs = (self.micro_price_skew / 10.0) * (ofi_delta_z / 6.0)       
        
        features = np.concatenate([base_features, [cross_momentum, cross_btc_sync, cross_skew_abs]])
        features = np.clip(features, -1.0, 1.0)
        
        if self.sgd_updates < 1000:
            active_weights = np.array([0.20, 0.15, 0.15, 0.10, 0.15, 0.10, 0.10, 0.05])
        else:
            active_weights = self.weights

        logit = max(-5.0, min(5.0, np.dot(active_weights, features)))
        
        T = 1.5
        p_up = 1.0 / (1.0 + math.exp(-logit / T))
        p_down = 1.0 - p_up
        
        if self.prices:
            self.prediction_buffer.append((time.time(), self.prices[-1], features, p_up))
            
        trend_weight = max(0.0, min(1.0, (self.hurst - 0.45) / 0.10))
        regime = "TRENDING" if trend_weight > 0.5 else "RANGING"
        
        return {
            "p_up": p_up, "p_down": p_down, 
            "entropy": self.shannon_entropy, "regime": regime
        }

    def spread_adjusted_edge(self, current_price: float, action: str, spread_pct: float, expected_move_pct: float) -> float:
        base_edge_pct = expected_move_pct * abs(self.hawkes_z * 0.10) 
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
        
        self.db_wal_queue = asyncio.Queue(maxsize=10000)
        self.db_semaphore = asyncio.Semaphore(5)
        self.eval_semaphore = asyncio.Semaphore(10)
        
        self.tick_error_counts: Dict[str, List[float]] = {}
        self.circuit_breakers: Dict[str, float] = {}
        
        self.stream_restart_event = asyncio.Event()
        self.force_dna_refresh = asyncio.Event() 
        
        self.fsm = SystemStateMachine()
        self.memory = MemoryBank()
        self.risk_vault = InstitutionalRiskVault(max_drawdown_pct=0.25, max_single_position_risk_pct=0.15)
        self.ai_matrix = AdversarialDebateMatrix()
        self.data_feed = AsynchronousDataFeed(finnhub_key=os.getenv("FINNHUB_API_KEY", ""))
        
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
            if s not in self.vpin_clocks: self.vpin_clocks[s] = VolumeSynchronizedClock(bucket_volume=250_000.0)
            if s not in self.feature_engines: self.feature_engines[s] = AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600)
            if s not in self.edge_gates: self.edge_gates[s] = MicrostructureEdgeGate(window_size=100)
            
            if s not in self.screener_memory:
                self.screener_memory[s] = {"prices": deque(maxlen=150), "highs": deque(maxlen=150), "lows": deque(maxlen=150), "volumes": deque(maxlen=150), "atr_history": deque(maxlen=100), "last_update_time": 0.0}
            if s not in self.screener_metrics: self.screener_metrics[s] = {"vol_mult": 1.0, "smoothed_price": 0.0}
            if s not in self.volatility_baseline: self.volatility_baseline[s] = 0.0
            if s not in self.ram_dna_cache: self.ram_dna_cache[s] = {"is_armed": True, "win_rate": 0.50}
            if s not in self.last_eval_time: self.last_eval_time[s] = 0.0 
            if s not in self.orderbook_snapshots: self.orderbook_snapshots[s] = {"best_bid": 0.0, "best_ask": 0.0}

    def _throttled_log(self, level: str, message: str, category: str = None, throttle_seconds: int = 30):
        current_time = time.time()
        key = category or hash(message)
        last_logged = self._log_throttle_cache.get(key, 0.0)
        
        if current_time - last_logged > throttle_seconds:
            self._log_throttle_cache[key] = current_time
            if level == "WARNING": logger.warning(message)
            elif level == "INFO": logger.info(message)
            elif level == "CRITICAL": logger.critical(message)

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
                    risk_matrix, 8, "RANGING"
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

    async def run_db_wal_worker(self):
        logger.info("🖲️ WAL ENGINE ONLINE: Durable asynchronous execution logging activated.")
        while True:
            payload = await self.db_wal_queue.get()
            action_type = payload.get("type")
            args = payload.get("args", [])
            
            try:
                async with self.db_semaphore:
                    if action_type == "prediction":
                        await asyncio.wait_for(asyncio.to_thread(self.memory.commit_prediction, *args), timeout=10.0)
                    elif action_type == "settlement":
                        await asyncio.wait_for(asyncio.to_thread(self.memory.log_live_execution_result, *args), timeout=10.0)
                self.db_wal_queue.task_done()
            except Exception:
                logger.warning("WAL Commit Delayed. Re-queuing payload for retry...")
                await asyncio.sleep(5.0)
                try:
                    self.db_wal_queue.put_nowait(payload)
                except asyncio.QueueFull:
                    logger.error("WAL Queue is full. Discarding oldest payload to protect RAM.")

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
        
        # 🚀 V27.7 FIX: Parse interval string safely
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
                for symbol in list(self.asset_basket):
                    if self.fsm.global_emergency_lock: continue

                    snapshot = None
                    try:
                        snapshot = await asyncio.wait_for(self.data_feed.fetch_market_snapshot(symbol, "15"), timeout=8.0)
                    except asyncio.TimeoutError:
                        logger.debug(f"REST API Timeout for {symbol}. Constructing synthetic snapshot from RAM.")
                    except Exception:
                        pass
                        
                    if not snapshot:
                        ob = self.orderbook_snapshots.get(symbol)
                        if not ob or ob["best_bid"] == 0.0: continue
                        snapshot = {
                            "current_price": (ob["best_bid"] + ob["best_ask"]) / 2.0,
                            "news_context": "Market data feed timeout. System relying strictly on real-time orderbook microstructure and VPIN."
                        }

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
                        "current_price": snapshot["current_price"],
                        "is_absorption_anomaly": False, 
                        "avg_trade_size": clock.bucket_volume / max(1, clock.current_bucket_ticks)
                    }

                    dna_stats = self.ram_dna_cache.get(symbol, {})
                    verdict = await self.ai_matrix.execute_debate_cycle(symbol, vpin_data, dna_stats, snapshot["news_context"])
                    
                    if verdict.get("schema_valid") and verdict.get("action") in ["BUY", "SELL", "HOLD"]:
                        raw_mult = 1.0 + (verdict.get("confidence", 0.0) * 0.20) if verdict.get("action") != "HOLD" else 1.0
                        mult = min(1.20, max(1.0, raw_mult))
                        self.fsm.update_ai_macro_state(symbol, verdict.get("action"), mult)
                        
                    await asyncio.sleep(5) 
                    
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
        if self.fsm.global_emergency_lock: return

        try:
            price = float(trade_data.get("price", 0.0))
            volume = float(trade_data.get("size", 0.0))
            is_buy = (str(trade_data.get("side", "")).upper() == "BUY")
            timestamp = float(trade_data.get("timestamp", now * 1000)) / 1000.0
            
            edge_gate = self.edge_gates.get(symbol)
            if edge_gate:
                edge_gate.update_trade_volume(volume)

            feature_engine = self.feature_engines.get(symbol)
            if feature_engine and hasattr(feature_engine, 'push_trade_tick'):
                feature_engine.push_trade_tick([trade_data])

            stat_engine = self.stat_engines.get(symbol)
            clock = self.vpin_clocks.get(symbol)
            if not stat_engine or not clock: return
            
            stat_engine.update_trades(price, volume, is_buy, timestamp)
            
            manifests = clock.process_tick(price, volume, not is_buy)
            valid_manifests = [m for m in manifests if m.get("valid")]
            
            if valid_manifests:
                vpin_z = float(valid_manifests[-1].get("vpin_z_score", 0.0))
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
            
            async with self.eval_semaphore:
                if not edge_gate: return
                
                verdict = edge_gate.evaluate_structural_edge(symbol, vpin_z)
                if verdict["action"] == "HOLD": return
                
                action = verdict["action"]
                prob_success = verdict["confidence"]
                
                macro_state = self.fsm.get_ai_macro_state(symbol)
                confidence_multiplier = macro_state.get("confidence_multiplier", 1.0)
                prob_success = min(0.99, prob_success * confidence_multiplier)
                
                regime = feature_engine.detect_market_regime() if feature_engine else "TRENDING"
                
                raw_atr = feature_engine.get_computed_atr() if feature_engine and hasattr(feature_engine, 'get_computed_atr') else 0.0
                atr = raw_atr if raw_atr > 0 else price * 0.005
                
                sl_atr_mult = self.live_params.get("sl_atr_mult", 1.5)
                rr_ratio = self.live_params.get("rr_ratio", 2.0)
                
                sl_dist_pct = max((atr * sl_atr_mult) / (price + 1e-9), 0.01)
                tp_dist_pct = sl_dist_pct * rr_ratio
                ev_pct = (prob_success * tp_dist_pct) - ((1.0 - prob_success) * sl_dist_pct)
                
                virtual_sl = price - (sl_dist_pct * price) if action == "BUY" else price + (sl_dist_pct * price)
                virtual_tp = price + (tp_dist_pct * price) if action == "BUY" else price - (tp_dist_pct * price)
                
                is_shadow_asset = symbol in self.shadow_basket
                if is_shadow_asset:
                    if prob_success > 0.65: 
                        try:
                            self.db_wal_queue.put_nowait({
                                "type": "prediction", 
                                "args": [str(uuid.uuid4()), now, price, action, prob_success, {"symbol": symbol, "market_regime": regime, "virtual_sl": virtual_sl, "virtual_tp": virtual_tp}, True]
                            })
                        except asyncio.QueueFull: pass
                    return 
                
                if symbol in self.active_positions_lock: return
                
                min_threshold = max(self.live_params.get("prob_threshold", 0.55), self.ram_dna_cache.get(symbol, {}).get("win_rate", 0.60))
                if prob_success < min_threshold: return 
                
                net_edge_bps = stat_engine.spread_adjusted_edge(price, action, spread_cost, ev_pct)
                if net_edge_bps <= 0.0: return 
                
                logger.critical(
                    f"🔬 INSTITUTIONAL TRIGGER // {symbol} [{regime}] "
                    f"| {action} | Fused-Prob: {prob_success:.2%} | EV: {ev_pct:.4f} | Net Edge: {net_edge_bps:.2f} bps"
                )
                
                self.last_eval_time[symbol] = now
                self.active_positions_lock[symbol] = action
                
                self.track_task(self.execute_statistical_signal(symbol, action, price, prob_success, regime, net_edge_bps, atr, stat_engine.hawkes_z, vpin_z))
                
        except Exception as e:
            self.tick_error_counts[symbol] = [t for t in self.tick_error_counts.get(symbol, []) if now - t < 60]
            self.tick_error_counts[symbol].append(now)
            if len(self.tick_error_counts[symbol]) > 5:
                self.circuit_breakers[symbol] = now + 300 
                logger.error(f"🛑 CIRCUIT BREAKER TRIGGERED for {symbol} due to 5+ execution errors. Paused for 5 minutes.")
            logger.error(f"Handle incoming trade exception for {symbol}: {e}", exc_info=True)

    async def execute_statistical_signal(self, symbol: str, action: str, price: float, confidence: float, regime: str, edge_bps: float, atr: float, vol_z: float, vol_mult: float):
        try:
            dna_stats = self.ram_dna_cache.get(symbol, {"is_armed": True, "win_rate": 0.50})
            
            success = await self.run_signal_lifecycle(
                symbol=symbol, 
                direction=action, 
                current_price=price, 
                confidence=confidence, 
                dna_stats=dna_stats, 
                atr=atr,
                regime=regime,
                edge_bps=edge_bps,
                vol_z=vol_z,
                vol_mult=vol_mult
            )
            if not success:
                self.active_positions_lock.pop(symbol, None)
        except Exception as e:
            logger.error(f"❌ EXECUTION ROUTING FAILURE for {symbol}: {e}", exc_info=True)
            self.active_positions_lock.pop(symbol, None)

    async def handle_incoming_kline_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return
        self._initialize_symbol_structures([symbol]) 
            
        interval = data["interval"]
        candle = data["candle_data"]
        c_open, c_high, c_low, c_close, c_vol = map(float, [candle.get("open", 0), candle.get("high", 0), candle.get("low", 0), candle.get("close", 0), candle.get("volume", 0)])

        feature_engine = self.feature_engines.get(symbol)
        if feature_engine:
            feature_engine.update_multi_timeframe_candle(timeframe=interval, open_p=c_open, high_p=c_high, low_p=c_low, close_p=c_close, volume=c_vol)
            
            if symbol in self.screener_memory:
                self.screener_memory[symbol].setdefault("highs", deque(maxlen=150)).append(c_high)
                self.screener_memory[symbol].setdefault("lows", deque(maxlen=150)).append(c_low)
                self.screener_memory[symbol].setdefault("prices", deque(maxlen=150)).append(c_close)
                self.screener_memory[symbol]["last_update_time"] = time.time()

    async def handle_incoming_basket_screener_update(self, data: Dict[str, Any]):
        pass

    async def run_universe_refresher(self):
        logger.info("🌍 FAST SATELLITE ROTATION INITIATED. Querying Bybit...")
        try:
            await self._fetch_exchange_tick_sizes()
            full_market = await self.executor.get_top_volatile_assets(limit=100, min_turnover=10_000_000)
            if len(full_market) < 25: full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
        except Exception as e:
            logger.error(f"Failed to fetch market data via REST: {e}")
            full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
            
        if "BTCUSDT" in full_market: full_market.remove("BTCUSDT")
        
        new_core_basket = ["BTCUSDT"] + [s for s in self.active_positions_lock.keys() if s != "BTCUSDT"]
        for sym in full_market:
            if sym not in new_core_basket and len(new_core_basket) < 25: new_core_basket.append(sym)
                
        self.asset_basket = new_core_basket
        self.shadow_basket = [s for s in full_market if s not in self.asset_basket]
        if len(self.shadow_basket) < 10:
            self.shadow_basket.extend([s for s in ["XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT"] if s not in self.shadow_basket])
        
        new_vpin_clocks = {}
        new_stat = {}
        new_dna_cache = {}
        new_last_eval = {}
        new_orderbooks = {}
        new_feature_engines = {}
        new_edge_gates = {}
        
        all_symbols = self.asset_basket + self.shadow_basket
        
        for s in all_symbols:
            new_vpin_clocks[s] = self.vpin_clocks.get(s, VolumeSynchronizedClock(bucket_volume=250_000.0))
            if s in self.stat_engines:
                new_stat[s] = self.stat_engines[s]
            else:
                new_stat[s] = ContinuousMicrostructureEngine()
                
            new_dna_cache[s] = self.ram_dna_cache.get(s, {})
            new_last_eval[s] = self.last_eval_time.get(s, 0.0)
            new_orderbooks[s] = self.orderbook_snapshots.get(s, {"best_bid": 0.0, "best_ask": 0.0})
            new_feature_engines[s] = self.feature_engines.get(s, AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600))
            new_edge_gates[s] = self.edge_gates.get(s, MicrostructureEdgeGate(window_size=100))
            
        self.vpin_clocks = new_vpin_clocks
        self.stat_engines = new_stat
        self.ram_dna_cache = new_dna_cache
        self.last_eval_time = new_last_eval
        self.orderbook_snapshots = new_orderbooks
        self.feature_engines = new_feature_engines
        self.edge_gates = new_edge_gates

        try:
            historical_data = {}
            for sym in self.asset_basket:
                if self.screener_memory.get(sym) and len(self.screener_memory[sym].get("prices", [])) > 30:
                    historical_data[sym] = list(self.screener_memory[sym]["prices"])
            if len(historical_data) >= 2:
                self.risk_vault.update_correlation_matrix(historical_data)
                logger.info("🛡️ Dynamic Cross-Asset Correlation Matrix Updated.")
        except Exception as e:
            logger.error(f"Correlation Matrix update skipped: {e}", exc_info=True)

        logger.info("🌌 QUANT UNIVERSE MATRIX RE-CALIBRATED.")
        self.stream_restart_event.set()
        self.force_dna_refresh.set() 

    async def _universe_refresher_loop(self):
        while True:
            await asyncio.sleep(14400)
            await self.run_universe_refresher()

    async def stream_manager_loop(self):
        while True:
            stream_feed = HighVelocityMultiFeed(
                basket=self.asset_basket + self.shadow_basket[:10], intervals=["1", "5", "15"],
                orderbook_callback=self.handle_incoming_orderbook_tick, screener_callback=self.handle_incoming_basket_screener_update,
                kline_callback=self.handle_incoming_kline_update, trade_callback=self.handle_incoming_trade, engine_reference=self  
            )
            stream_task = asyncio.create_task(stream_feed.initialize_multiplexed_stream())
            
            def _on_stream_done(t):
                if not t.cancelled() and not self.stream_restart_event.is_set():
                    logger.error("🚨 Stream task exited prematurely. Forcing restart.")
                    self.stream_restart_event.set()
                    
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

            logger.info(f"💓 SWARM HEARTBEAT: Matrix is active. Uptime: {uptime_hours:.2f} hours.")

            if loop_counter % 5 == 0:
                self.global_state_cache["last_updated"] = time.time()
                await self._save_sgd_state()
                
                try:
                    current_vault_balance = await self.executor.get_wallet_balance_usdt()
                except Exception as e:
                    logger.error(f"Failed to fetch balance during heartbeat: {e}", exc_info=True)
                    continue

                if "wallet_baseline" not in self.global_state_cache: self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01)
                
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                current_day = now_utc.strftime("%Y-%m-%d")
                if self.global_state_cache.get("current_day") != current_day:
                    self.global_state_cache["current_day"] = current_day
                    self.global_state_cache["start_of_day_balance"] = current_vault_balance
                    logger.info(f"🌅 MIDNIGHT UTC RESET: New start of day balance = {current_vault_balance:.4f} USDT")

                today_start_iso = now_utc.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                
                try:
                    def _fetch(): return self.memory.supabase.table("quantitative_ledger").select("market_regime, net_pnl, symbol, predicted_direction, actual_outcome").eq("resolved", True).eq("is_shadow", False).gte("timestamp", today_start_iso).execute()
                    async with self.db_semaphore:
                        data = (await asyncio.to_thread(_fetch)).data or []
                except Exception:
                    data = []

                actual_net_pnl = current_vault_balance - self.global_state_cache.get("start_of_day_balance", current_vault_balance)
                baseline = self.global_state_cache["wallet_baseline"]
                
                if current_vault_balance > baseline:
                    self.global_state_cache["wallet_baseline"] = current_vault_balance
                    baseline = current_vault_balance
                    
                drawdown_pct = max(0.0, (baseline - current_vault_balance) / baseline)
                
                # 🚀 V27.7 FIX: Graceful queue drain before emergency shutdown
                if drawdown_pct >= 0.25:
                    logger.critical(f"🚨 FATAL: PORTFOLIO DRAWDOWN EXCEEDED 25% ({drawdown_pct:.2%}). INITIATING EMERGENCY SHUTDOWN.")
                    self.track_task(self._safe_telegram_dispatch(f"🚨 <b>EMERGENCY DRAWDOWN BREAKER TRIPPED</b>\nDrawdown: {drawdown_pct:.2%}. Engine shutting down.", is_html=True))
                    await self.graceful_shutdown()
                    
                    try:
                        await asyncio.wait_for(self.db_wal_queue.join(), timeout=3.0)
                    except asyncio.TimeoutError:
                        logger.warning("WAL Queue flush timed out during shutdown.")
                    os._exit(1)
                
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
                    f"💎 <b>𝗣██𝗔𝗦𝗞 𝗘𝗠𝗣𝗜𝗥𝗘 | 𝗤𝗨𝗔𝗡𝗧 𝗦𝗪𝗔𝗥𝗠 (V27.7 SIGNAL APEX)</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏱️ <b>𝗨𝗽𝘁𝗶𝗺𝗲:</b> <code>{uptime_hours:.2f} Hours</code> | 🛰️ <b>𝗡𝗼𝗱𝗲𝘀:</b> <code>{len(self.asset_basket)} Live</code>\n\n"
                    f"⚙️ <b>𝗘𝗡𝗚𝗜𝗡𝗘 𝗦𝗧𝗔𝗧𝗨𝗦: 𝗣𝗿𝗼𝗱𝘂𝗰𝘁𝗶𝗼𝗻 𝗚𝗿𝗮𝗱𝗲 𝗜𝗻𝗳𝗿𝗮𝘀𝘁𝗿𝘂𝗰𝘁𝘂𝗿𝗲</b>\n"
                    f"• Signal Engine:   <code>Deep-Book MLOFI + Amihud Liquidity</code>\n"
                    f"• Market Lead:     <code>BTC Cross-Sectional Flow Matrix</code>\n"
                    f"• Risk Engine:     <code>Fixed-Frac Target (Capped Kelly)</code>\n\n"
                    f"💵 <b>𝗙𝗜𝗡𝗔𝗡𝗖𝗜𝗔🇱 𝗩𝗔𝗨🇱𝗧 𝗣𝗥𝗢𝗙𝗜🇱𝗘</b>\n"
                    f"• Total Liquidity: <code>{cv:.4f} USDT</code>\n"
                    f"• Session Return:  <code>{actual:+.4f} USDT</code>\n"
                    f"• Peak Drawdown:   <code>{dd:.2%}</code>\n"
                    f"• Risk Buffer:     <code>[{dd_bar}]</code>\n\n"
                    f"🔬 <b>𝗗𝗔𝗜🇱𝗬 𝗥𝗘𝗚𝗜𝗠𝗘 𝗣𝗥𝗢𝗙𝗜🇱𝗘:</b>\n{regime_text}\n"
                    f"🏁 <b>𝗥𝗘𝗖𝗘𝗡𝗧 𝗦𝗘𝗦𝗦𝗜𝗢𝗡 𝗠𝗔𝗧𝗨𝗥𝗜𝗧𝗜𝗘𝗦</b>\n{recent_trades}"
                )
                self.track_task(self._safe_telegram_dispatch(report, is_html=True))

    async def run_signal_lifecycle(self, symbol: str, direction: str, current_price: float, confidence: float, dna_stats: dict, atr: float, regime: str, edge_bps: float, vol_z: float, vol_mult: float) -> bool:
        try:
            signal_id = str(uuid.uuid4())
            is_armed = dna_stats.get("is_armed", False)

            sl_atr_mult = self.live_params.get("sl_atr_mult", 1.5)
            rr_ratio = self.live_params.get("rr_ratio", 2.0)

            sl_distance = max(atr * sl_atr_mult, current_price * 0.01)
            tp_distance = sl_distance * rr_ratio 
            
            tick_dec = Decimal(str(self.tick_sizes.get(symbol, 0.0001)))
            def align_price(p: float) -> str:
                return str(Decimal(str(p)).quantize(tick_dec, rounding=ROUND_HALF_UP))
            
            raw_sl = current_price - sl_distance if direction == "BUY" else current_price + sl_distance
            raw_tp = current_price + tp_distance if direction == "BUY" else current_price - tp_distance
            
            initial_sl_price = float(align_price(raw_sl))
            target_tp_price = float(align_price(raw_tp))

            try:
                balance = await self.executor.get_wallet_balance_usdt()
            except Exception as e:
                logger.error(f"Failed to fetch balance for execution sizing: {e}", exc_info=True)
                return False
                
            start_bal = self.global_state_cache.get("start_of_day_balance", balance)
            if start_bal > 0 and balance < (start_bal * 0.90):
                logger.critical(f"🛑 DAILY LOSS LIMIT TRIGGERED. Balance ({balance:.2f}) dropped below 90% of start-of-day ({start_bal:.2f}). Trading halted.")
                return False
            
            correlation_penalty = 1.0
            if hasattr(self.risk_vault, "correlation_groups"):
                for group, assets in self.risk_vault.correlation_groups.items():
                    if symbol in assets:
                        active_correlated = sum(1 for a, a_dir in self.active_positions_lock.items() if a in assets and a != symbol and a_dir == direction)
                        if active_correlated > 0:
                            correlation_penalty = max(0.25, 1.0 - (active_correlated * 0.25))
            
            reward_risk = tp_distance / sl_distance
            
            b = reward_risk
            p = confidence
            true_kelly = p - ((1.0 - p) / b) if b > 0 else 0.0
            
            if true_kelly <= 0.0:
                logger.debug(f"Rejecting {symbol} {direction} - Negative Kelly Edge: {true_kelly:.4f}")
                return False
            
            account_scaling = 0.75 if balance < 100.0 else 1.0
            quarter_kelly = max(0.005, min(0.025, true_kelly * 0.25 * account_scaling * correlation_penalty))

            if not is_armed:
                try:
                    self.db_wal_queue.put_nowait({
                        "type": "prediction", 
                        "args": [signal_id, time.time(), current_price, direction, confidence, {"symbol": symbol, "market_regime": regime, "virtual_sl": initial_sl_price, "virtual_tp": target_tp_price}, True]
                    })
                except asyncio.QueueFull: pass
                return False

            if balance < 1.0:
                return False

            dollar_risk = balance * quarter_kelly
            position_size = max(dollar_risk / sl_distance, 6.00 / (current_price + 1e-9))
            notional = position_size * current_price

            if not self.risk_vault.evaluate_portfolio_safety(balance, notional, symbol):
                return False

            target_leverage = self.risk_vault.calculate_dynamic_leverage(notional, balance, base_leverage=5, hard_cap=10, sl_distance_pct=(sl_distance / current_price))
            
            if self.test_mode:
                logger.critical(f"📜 PAPER TRADE EXECUTED: {symbol} {direction} {position_size} @ {current_price}")
                execution_success = True
            else:
                try:
                    await self.executor.safe_call(self.executor.adjust_leverage, symbol, target_leverage)
                    await asyncio.sleep(0.2) 
                except Exception as e:
                    if "110043" in str(e):
                        pass
                    else:
                        logger.error(f"Leverage adjustment failed: {e}. Aborting trade.", exc_info=True)
                        return False

                feature_engine = self.feature_engines.get(symbol)
                if feature_engine and hasattr(feature_engine, 'get_orderbook_snapshot'):
                    current_depth = feature_engine.get_orderbook_snapshot()
                else:
                    cached_depth = self.orderbook_snapshots.get(symbol, {"best_bid": current_price, "best_ask": current_price})
                    current_depth = {"bids": [[cached_depth["best_bid"], 1]], "asks": [[cached_depth["best_ask"], 1]]}

                if regime == "TRENDING": execution_success = await self.sor.execute_iceberg_block(symbol=symbol, direction=direction, total_qty=position_size, current_mid_price=current_price, stop_loss=initial_sl_price, take_profit=target_tp_price, depth_snapshot=current_depth, vol_z=vol_z, vol_mult=vol_mult, feature_engine=feature_engine)
                else: execution_success = await self.sor.execute_mean_reversion_bracket(symbol=symbol, direction=direction, total_qty=position_size, current_mid_price=current_price, stop_loss=initial_sl_price, take_profit=target_tp_price, depth_snapshot=current_depth, vol_z=vol_z, vol_mult=vol_mult, feature_engine=feature_engine)
            
            if not execution_success:
                return False 
                
            if not self.test_mode:
                try:
                    self.db_wal_queue.put_nowait({
                        "type": "prediction", 
                        "args": [signal_id, time.time(), current_price, direction, confidence, {"symbol": symbol, "market_regime": regime, "virtual_sl": initial_sl_price, "virtual_tp": target_tp_price}, False]
                    })
                except asyncio.QueueFull: pass
                
            self.risk_vault.update_position_ledger(symbol, notional)
            
            self.track_task(self._safe_telegram_dispatch(f"🧬 *HFT EXECUTION FIRE*\n• Node: {symbol} | {direction}\n• Fixed-Frac Risk: {quarter_kelly:.2%}\n• Leverage Applied: {target_leverage}x\n• Notional Value: ${notional:.2f} USDT", is_html=False, message_type="SUCCESS"))
            
            daemon_task = self.track_task(self._position_lifecycle_daemon(symbol, signal_id, direction, current_price, atr, {"allocated_value_usdt": notional, "size": position_size}, target_leverage, regime))
            self.daemon_tasks[symbol] = daemon_task
            
            return True

        except Exception as e:
            logger.error(f"Distributed swarm execution routing failed for {symbol}: {e}", exc_info=True)
            return False

    async def _position_lifecycle_daemon(self, symbol: str, signal_id: str, direction: str, current_price: float, atr: float, risk_matrix: dict, target_leverage: int = 8, market_regime: str = "TRENDING"):
        logger.info(f"👻 APEX MONITOR ARMED // Native Exchange Hand-off for {symbol}")
        exec_details = {"leverage": target_leverage, "execution_mode": "RECOVERY" if "RECOVERY" in signal_id else ("GHOST" if self.test_mode else "LIVE")}
        
        daemon_start_time = time.time()
        max_lifetime_seconds = 14400 

        if self.test_mode:
            await asyncio.sleep(60)
            logger.critical(f"📜 PAPER TRADE CLOSED: {symbol}")
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
                except Exception as e: 
                    logger.error(f"Position confirmation fault: {e}", exc_info=True)
                    continue

            if not order_filled:
                logger.critical(f"🔓 PORTFOLIO UNLOCKED // SOR failed to fill {symbol} within 15s. Canceling.")
                try: 
                    await self.executor.safe_call(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
                except Exception: pass
                
                self.risk_vault.update_position_ledger(symbol, -risk_matrix['allocated_value_usdt'])
                self.active_positions_lock.pop(symbol, None)
                return

            tick_dec = Decimal(str(self.tick_sizes.get(symbol, 0.0001)))
            
            # 🚀 V27.7 FIX: Use live optimized params for Daemon bracketing
            sl_atr_mult = self.live_params.get("sl_atr_mult", 1.5)
            rr_ratio = self.live_params.get("rr_ratio", 2.0)
            actual_sl_distance = max(atr * sl_atr_mult, actual_entry * 0.01)
            actual_tp_distance = actual_sl_distance * rr_ratio
            
            realigned_sl = actual_entry - actual_sl_distance if direction == "BUY" else actual_entry + actual_sl_distance
            realigned_tp = actual_entry + actual_tp_distance if direction == "BUY" else actual_entry - actual_tp_distance
            
            def align_price(p: float) -> str:
                return str(Decimal(str(p)).quantize(tick_dec, rounding=ROUND_HALF_UP))
                
            realigned_sl_str = align_price(realigned_sl)
            realigned_tp_str = align_price(realigned_tp)
            
            stops_verified = False
            for attempt in range(3):
                try:
                    await self.executor.safe_call(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, takeProfit=realigned_tp_str, stopLoss=realigned_sl_str)
                    pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                    
                    pos_verify = pos_res.get("result", {}).get("list", [{}])[0]
                    if float(pos_verify.get("stopLoss", 0.0)) > 0 and float(pos_verify.get("takeProfit", 0.0)) > 0:
                        stops_verified = True
                        break
                except Exception as e: 
                    logger.error(f"Failed to set/verify hard stops (Attempt {attempt+1}): {e}", exc_info=True)
                await asyncio.sleep(2)
                
            if not stops_verified:
                logger.error(f"🚨 CRITICAL: Failed to verify SL/TP for {symbol} after 3 attempts. FLATTENING POSITION.")
                try:
                    pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                    pos_list = pos_res.get("result", {}).get("list", [])
                    if pos_list and float(pos_list[0].get("size", 0.0)) > 0:
                        actual_qty = str(float(pos_list[0].get("size")))
                        side = pos_list[0].get("side")
                        flatten_side = "Sell" if side == "Buy" else "Buy"
                        await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side=flatten_side, orderType="Market", qty=actual_qty, timeInForce="IOC", reduceOnly=True)
                except Exception as e:
                    logger.error(f"Failed to flatten unverified position: {e}", exc_info=True)
                    
                self.active_positions_lock.pop(symbol, None)
                return

            act_raw = actual_entry + (atr * 0.8) if direction == "BUY" else actual_entry - (atr * 0.8)
            activation_price = align_price(act_raw)
            trailing_distance_str = align_price(atr * 1.5)

            try:
                await self.executor.safe_call(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, takeProfit=realigned_tp_str, stopLoss=realigned_sl_str, trailingStop=trailing_distance_str, activePrice=activation_price)
                logger.info(f"🛡️ NATIVE TRAIL ARMED // {symbol} Trailing Stop handed to exchange (Act: {activation_price}, Dist: {trailing_distance_str})")
            except Exception as e: 
                logger.error(f"Failed to arm trailing stop: {e}", exc_info=True)

            consecutive_errors = 0
            while True: 
                await asyncio.sleep(20)
                
                if time.time() - daemon_start_time > max_lifetime_seconds:
                    logger.critical(f"⏳ DAEMON TIMEOUT // {symbol} position exceeded 4-hour limit. Force reconciling.")
                    try:
                        pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                        pos_list = pos_res.get("result", {}).get("list", [])
                        if pos_list and float(pos_list[0].get("size", 0.0)) > 0:
                            actual_qty = str(float(pos_list[0].get("size")))
                            side = pos_list[0].get("side")
                            flatten_side = "Sell" if side == "Buy" else "Buy"
                            await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side=flatten_side, orderType="Market", qty=actual_qty, timeInForce="IOC", reduceOnly=True)
                            logger.critical(f"✅ TIMEOUT FLATTEN SUCCESSFUL for {symbol}.")
                    except Exception as e:
                        logger.error(f"Timeout flatten failed for {symbol}: {e}")
                    
                    self.active_positions_lock.pop(symbol, None)
                    self.risk_vault.update_position_ledger(symbol, 0.0)
                    break

                try:
                    pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                    pos_list = pos_res.get("result", {}).get("list", [])
                    position_gone = (not pos_list) or float(pos_list[0].get("size", 0.0)) == 0.0
                    consecutive_errors = 0  
                except Exception as pos_gone_err:
                    consecutive_errors += 1
                    logger.error(f"Position polling error ({consecutive_errors}/10): {pos_gone_err}")
                    if consecutive_errors >= 10:
                        raise RuntimeError("10 consecutive API failures during daemon polling. Exchange likely offline.")
                    position_gone = False

                try:
                    settlement = await self.executor.check_recent_settlement(symbol, 300) 
                    if settlement.get("closed"):
                        net_pnl = float(settlement.get('pnl', 0.0))
                        self.track_task(self._safe_telegram_dispatch(f"🔔 <b>EXCHANGE EXECUTION TERMINATION</b>\n━━━━━━━━━━━━━━━━━━━━━━\n📈 <b>Asset Node:</b> <code>{symbol}</code>\n📊 <b>Outcome:</b> {'🟢 PROFIT' if net_pnl > 0 else '🔴 LOSS'}\n💰 <b>Net Return:</b> <code>{net_pnl:.4f} USDT</code>\n━━━━━━━━━━━━━━━━━━━━━━", is_html=True))
                        try:
                            self.db_wal_queue.put_nowait({
                                "type": "settlement", 
                                "args": [signal_id, net_pnl, actual_entry - current_price if direction == "BUY" else current_price - actual_entry, settlement['outcome'], exec_details]
                            })
                        except asyncio.QueueFull: pass
                        self.risk_vault.update_position_ledger(symbol, 0.0)
                        break
                except Exception as e:
                    logger.debug(f"Settlement check error: {e}")

                if position_gone:
                    logger.warning(f"🧾 RECONCILIATION // {symbol} closed. Pulling final PnL snapshot.")
                    try: 
                        pnl_res = await self.executor.safe_call(self.executor.client.get_closed_pnl, category="linear", symbol=symbol, limit=5)
                        net_pnl = float(pnl_res.get("result", {}).get("list", [])[0].get("closedPnl", 0.0))
                    except Exception: net_pnl = 0.0
                    
                    try:
                        self.db_wal_queue.put_nowait({
                            "type": "settlement", 
                            "args": [signal_id, net_pnl, 0.0, "RECONCILED", exec_details]
                        })
                    except asyncio.QueueFull: pass
                    
                    self.risk_vault.update_position_ledger(symbol, 0.0)
                    break

        except Exception as daemon_error:
            logger.error(f"☠️ FATAL DAEMON CRASH on {symbol}: {daemon_error}", exc_info=True)
            logger.critical(f"🚑 EMERGENCY INTERVENTION // Attempting to flatten {symbol} position to protect capital.")
            try:
                pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                pos_list = pos_res.get("result", {}).get("list", [])
                if pos_list and float(pos_list[0].get("size", 0.0)) > 0:
                    actual_qty = str(float(pos_list[0].get("size")))
                    side = pos_list[0].get("side")
                    flatten_side = "Sell" if side == "Buy" else "Buy"
                    
                    await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side=flatten_side, orderType="Market", qty=actual_qty, timeInForce="IOC", reduceOnly=True)
                    logger.critical(f"✅ EMERGENCY FLATTEN SUCCESSFUL for {symbol}. Closed {actual_qty} units.")
            except Exception as flatten_e: 
                logger.error(f"❌ EMERGENCY FLATTEN FAILED for {symbol}: {flatten_e}", exc_info=True)
                
        finally:
            self.active_positions_lock.pop(symbol, None)
            self.risk_vault.update_position_ledger(symbol, 0.0)

    async def graceful_shutdown(self):
        logger.critical("🛑 INITIATING EMERGENCY FLATTEN & SHUTDOWN...")
        for symbol in list(self.active_positions_lock.keys()):
            try:
                await self.executor.safe_call(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
                
                pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                pos_list = pos_res.get("result", {}).get("list", [])
                if pos_list and float(pos_list[0].get("size", 0.0)) > 0:
                    qty_str = str(float(pos_list[0].get("size")))
                    side = pos_list[0].get("side")
                    flatten_side = "Sell" if side == "Buy" else "Buy"
                    
                    await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side=flatten_side, orderType="Market", qty=qty_str, timeInForce="IOC", reduceOnly=True)
                    logger.critical(f"✅ EMERGENCY FLATTEN EXECUTED for {symbol}")
            except Exception as e:
                logger.error(f"Shutdown flatten failed for {symbol}: {e}", exc_info=True)
        
        if hasattr(self, 'telegram'):
            await self.telegram.close()
            
        logger.critical("✅ MATRIX DISCONNECTED.")

    async def _safe_daemon_run(self, coro_func):
        while True:
            try:
                await coro_func()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.critical(f"🔥 FATAL DAEMON CRASH: {coro_func.__name__} | Exception: {e}. Restarting in 5s...", exc_info=True)
                await asyncio.sleep(5)

    async def run_engine_forever(self):
        logger.critical("LAUNCHING DECENTRALIZED QUANT SWARM DAEMON DEPLOYMENTS...")
        
        try: await self._fetch_exchange_tick_sizes()
        except Exception as e: logger.error(f"Boot sequence tick fetch error: {e}", exc_info=True)
            
        try: await self.synchronize_exchange_state()
        except Exception as e: logger.error(f"Boot sequence sync error: {e}", exc_info=True)
        
        try:
            boot_bal = await self.executor.get_wallet_balance_usdt()
            self.global_state_cache["start_of_day_balance"] = boot_bal
            self.global_state_cache["wallet_baseline"] = max(boot_bal, 0.01)
            self.global_state_cache["last_updated"] = time.time()
            self.global_state_cache["current_day"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
            logger.info(f"💵 INITIAL VAULT BASELINE LOCKED: {boot_bal:.4f} USDT")
        except Exception as boot_bal_err:
            logger.error(f"Boot balance initialization failed: {boot_bal_err}")
        
        try: 
            full_market = await self.executor.get_top_volatile_assets(limit=100, min_turnover=10_000_000)
            if len(full_market) < 25: full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
        except Exception: 
            full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
            
        if boot_basket := full_market[:25]:
            if "BTCUSDT" in boot_basket: boot_basket.remove("BTCUSDT")
            self.asset_basket = ["BTCUSDT"] + boot_basket[:24]
            self._initialize_symbol_structures(self.asset_basket)
        
        daemons = [
            self.run_db_wal_worker,
            self.run_dna_prewarmer, 
            self.stream_manager_loop,
            self.run_system_heartbeat,
            self.cleanup_stale_locks,
            self.run_shadow_resolution_daemon,
            self.run_ai_macro_evaluator,
            self._universe_refresher_loop
        ]
        
        tasks = [asyncio.create_task(self._safe_daemon_run(d)) for d in daemons]
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    from keep_alive import keep_alive
    keep_alive()
    engine = DistributedQuantEngine()
    try:
        asyncio.run(engine.run_engine_forever())
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt received. Graceful shutdown sequence initiated.")
        asyncio.run(engine.graceful_shutdown())
        sys.exit(0)