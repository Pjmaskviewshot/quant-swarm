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
import random
import heapq
import numpy as np
import aiosqlite  
from collections import deque
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

# Core & Feature Modules
from core.fsm import SystemStateMachine
from core.memory import MemoryBank
from core.edge_gate import MicrostructureEdgeGate
from features.adaptive_engine import AdaptiveFeatureEngine
from features.vpin_clock import VolumeSynchronizedClock
from features.omni_scanner import GlobalOmniScanner  # 🚀 V33.0 IMPORT
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
logger = logging.getLogger("QUANT_CORE.V33_OMNI_SWARM")


class ClusterWarmStartRLS:
    """
    🌌 V32.0 CLUSTER-WARM-STARTED RLS ENGINE
    Eliminates cold-start divergence and zero-knowledge learning delays by 
    assigning calibrated covariance priors based on asset volatility profiles.
    """
    @staticmethod
    def get_cluster_priors(symbol: str):
        if any(m in symbol for m in ["BTC", "ETH", "SOL"]):
            w_trend = np.array([0.22, 0.18, 0.15, 0.08, 0.12, 0.10, 0.05, 0.05, 0.05])
            w_range = np.array([0.08, 0.15, 0.05, 0.22, 0.18, 0.05, 0.12, 0.08, 0.07])
            p_scale = 1.0
        elif any(m in symbol for m in ["AVAX", "LINK", "XRP", "ADA", "DOT", "NEAR"]):
            w_trend = np.array([0.20, 0.16, 0.14, 0.10, 0.10, 0.10, 0.08, 0.06, 0.06])
            w_range = np.array([0.09, 0.14, 0.06, 0.20, 0.16, 0.06, 0.11, 0.09, 0.09])
            p_scale = 2.0
        else:
            w_trend = np.array([0.15, 0.12, 0.10, 0.15, 0.08, 0.10, 0.10, 0.10, 0.10])
            w_range = np.array([0.10, 0.10, 0.08, 0.18, 0.14, 0.08, 0.12, 0.10, 0.10])
            p_scale = 0.5 

        return w_trend, w_range, np.eye(9) * p_scale


class ContinuousMicrostructureEngine:
    def __init__(self, symbol: str = "GENERIC", memory_depth=500):
        self.symbol = symbol
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
        self.hawkes_ewma = 0.0
        self.hawkes_ewmvar = 1.0
        self.hawkes_z = 0.0
        
        self.hawkes_velocity = 0.0
        self.hawkes_acceleration = 0.0
        self.hawkes_z_prev = 0.0
        self.hawkes_v_prev = 0.0
        
        self.prices = deque(maxlen=memory_depth)
        self.log_returns = deque(maxlen=memory_depth)
        self.inst_variance = 1e-6
        self.vol_ewma = 0.0 
        
        self.hurst = 0.5
        self.kaufman_er = 0.5
        self.last_hurst_time = 0.0
        self.last_price_time = 0.0  
        self.shannon_entropy = 1.0
        
        w_t, w_r, P_init = ClusterWarmStartRLS.get_cluster_priors(symbol)
        self.weights_trending = w_t
        self.weights_ranging  = w_r
        self.P_trending = P_init.copy()
        self.P_ranging = P_init.copy()
        self.forgetting_factor = 0.998
        
        self.prediction_buffer = deque(maxlen=50000)
        self.historical_probs = deque(maxlen=2000) 
        self.rls_updates = 100 
        
        self.validation_buffer = deque(maxlen=100)
        self.rolling_mse = 0.0

    def get_dynamic_decays(self):
        vol_scalar = min(1.0, max(0.0, self.inst_variance * 5000.0))
        alpha_fast = np.clip(0.05 + (vol_scalar * 0.25) + (self.kaufman_er * 0.05), 0.05, 0.35)
        alpha_slow = alpha_fast / 5.0
        hawkes_decay = np.clip(1.0 + (vol_scalar * 4.0), 1.0, 5.0)
        return alpha_fast, alpha_slow, hawkes_decay

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
        
        alpha_fast, alpha_slow, _ = self.get_dynamic_decays()
        
        self.ofi_fast_ewma = (1 - alpha_fast) * self.ofi_fast_ewma + alpha_fast * delta_W
        self.ofi_fast_ewmvar = (1 - alpha_fast) * self.ofi_fast_ewmvar + alpha_fast * (delta_W - self.ofi_fast_ewma)**2
        self.ofi_fast_z = (delta_W - self.ofi_fast_ewma) / (math.sqrt(self.ofi_fast_ewmvar) + 1e-9)
        
        self.ofi_slow_ewma = (1 - alpha_slow) * self.ofi_slow_ewma + alpha_slow * delta_W
        self.ofi_slow_ewmvar = (1 - alpha_slow) * self.ofi_slow_ewmvar + alpha_slow * (delta_W - self.ofi_slow_ewma)**2
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

        alpha_fast, alpha_slow, hawkes_decay = self.get_dynamic_decays()

        volume_signed = volume if is_buy else -volume
        if self.last_trade_time > 0:
            dt = current_time - self.last_trade_time
            self.hawkes_pressure_state = self.hawkes_pressure_state * math.exp(-hawkes_decay * dt) + volume_signed
        else:
            self.hawkes_pressure_state = volume_signed
            
        self.last_trade_time = current_time
        
        self.hawkes_ewma = (1 - alpha_fast) * self.hawkes_ewma + alpha_fast * self.hawkes_pressure_state
        self.hawkes_ewmvar = (1 - alpha_slow) * self.hawkes_ewmvar + alpha_slow * (self.hawkes_pressure_state - self.hawkes_ewma)**2
        self.hawkes_z = (self.hawkes_pressure_state - self.hawkes_ewma) / (math.sqrt(self.hawkes_ewmvar) + 1e-9)

        self.hawkes_velocity = self.hawkes_z - self.hawkes_z_prev
        self.hawkes_acceleration = self.hawkes_velocity - self.hawkes_v_prev
        
        self.hawkes_z_prev = self.hawkes_z
        self.hawkes_v_prev = self.hawkes_velocity

        if len(self.prediction_buffer) > 0:
            while self.prediction_buffer and current_time - self.prediction_buffer[0][0] >= 300.0:
                old_time, old_price, features_array, old_pred_prob, virt_sl, virt_tp, action_dir, r_blend = self.prediction_buffer.popleft()
                
                if price != old_price and old_price > 0:
                    y_true = 0.5 
                    if action_dir == "BUY":
                        if price >= virt_tp: y_true = 1.0
                        elif price <= virt_sl: y_true = 0.0
                    else: 
                        if price <= virt_tp: y_true = 1.0
                        elif price >= virt_sl: y_true = 0.0
                    
                    if y_true == 0.5:
                        y_true = 1.0 if ((price > old_price) == (action_dir == "BUY")) else 0.0

                    error = y_true - old_pred_prob 
                    
                    self.validation_buffer.append(error ** 2)
                    if len(self.validation_buffer) == 100:
                        self.rolling_mse = np.mean(self.validation_buffer)
                        if self.rolling_mse > 0.35 or np.trace(self.P_trending) > 5000.0:
                            # 🚀 V33.1 FIX: Soft Damping instead of hard trace reset
                            logger.warning(f"📉 Applying Levenberg-Marquardt Trace Damping for {self.symbol}.")
                            trace_t = np.trace(self.P_trending)
                            trace_r = np.trace(self.P_ranging)
                            if trace_t > 5000:
                                self.P_trending = self.P_trending / (trace_t / 1000.0) + np.eye(9) * 0.1
                            if trace_r > 5000:
                                self.P_ranging = self.P_ranging / (trace_r / 1000.0) + np.eye(9) * 0.1
                            self.validation_buffer.clear()
                            continue

                    x = features_array.reshape(-1, 1)
                    pi = old_pred_prob
                    var_pi = max(1e-4, pi * (1.0 - pi))  
                    
                    if r_blend > 0.1:
                        lam_t = self.forgetting_factor
                        P_x_t = self.P_trending @ x
                        den_t = lam_t + var_pi * float(x.T @ P_x_t)
                        K_t = P_x_t / den_t
                        update_t = (K_t.flatten() * error * r_blend)
                        self.weights_trending = self.weights_trending + update_t
                        self.P_trending = (self.P_trending - var_pi * (K_t @ (x.T @ self.P_trending))) / lam_t
                        self.P_trending += np.eye(9) * 1e-4

                    r_range = 1.0 - r_blend
                    if r_range > 0.1:
                        lam_r = self.forgetting_factor
                        P_x_r = self.P_ranging @ x
                        den_r = lam_r + var_pi * float(x.T @ P_x_r)
                        K_r = P_x_r / den_r
                        update_r = (K_r.flatten() * error * r_range)
                        self.weights_ranging = self.weights_ranging + update_r
                        self.P_ranging = (self.P_ranging - var_pi * (K_r @ (x.T @ self.P_ranging))) / lam_r
                        self.P_ranging += np.eye(9) * 1e-4
                    
                    self.rls_updates += 1

    def extract_statistical_state(self, current_price: float, vpin_z: float, tensor_alpha: float, virtual_sl: float, virtual_tp: float) -> dict:
        current_time = time.time()
        
        if len(self.prices) >= 20:
            prices_arr = np.array(list(self.prices)[-20:])
            directional_change = abs(prices_arr[-1] - prices_arr[0])
            path_volatility = np.sum(np.abs(np.diff(prices_arr))) + 1e-9
            self.kaufman_er = float(directional_change / path_volatility)
        else:
            self.kaufman_er = 0.5

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
        
        attention_temp = max(0.15, min(0.48, 0.18 + 0.30 * (1.0 - self.kaufman_er)))
        feature_magnitudes = np.abs(features)
        exp_f = np.exp(feature_magnitudes / attention_temp)
        attention_weights = exp_f / (np.sum(exp_f) + 1e-9)
        attended_features = features * attention_weights * len(features)

        r_blend = 1.0 / (1.0 + math.exp(-12.0 * (self.kaufman_er - 0.35)))

        active_w_trend = self.weights_trending
        active_w_range = self.weights_ranging

        logit_trend = np.dot(active_w_trend, attended_features)
        logit_range = np.dot(active_w_range, attended_features)
        logit_fused = (r_blend * logit_trend) + ((1.0 - r_blend) * logit_range)
        
        logit = max(-5.0, min(5.0, logit_fused))
        p_up = 1.0 / (1.0 + math.exp(-logit))  
        p_down = 1.0 - p_up
        
        action_dir = "BUY" if p_up > p_down else "SELL"
        prob_success = max(p_up, p_down)
        
        self.historical_probs.append(prob_success)
        
        if len(self.historical_probs) > 100:
            mean_prob = np.mean(self.historical_probs)
            std_prob = np.std(self.historical_probs) + 1e-9
            dynamic_gate = mean_prob + (1.25 * std_prob)  
        else:
            dynamic_gate = 0.58
        
        self.prediction_buffer.append((time.time(), current_price, attended_features, prob_success, virtual_sl, virtual_tp, action_dir, r_blend))
        
        return {"p_up": p_up, "p_down": p_down, "entropy": self.shannon_entropy, "r_blend": r_blend, "dynamic_gate": dynamic_gate}

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
        self.eval_semaphore = asyncio.Semaphore(10)
        self.wal_db_path = "quant_swarm_wal.db"
        
        self.wal_batch_queue = []
        self.wal_lock = asyncio.Lock()  
        
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
        self.eval_semaphores: Dict[str, asyncio.Semaphore] = {}
        
        self.daemon_tasks: Dict[str, asyncio.Task] = {}
        self.last_eval_time: Dict[str, float] = {}
        self._active_tasks = set()
        
        self.auction_queue: List[tuple] = []  
        self.auction_lock = asyncio.Lock()
        
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
        
        # 🚀 V33.0 OMNI-SWARM SCANNER
        self.omni_scanner = GlobalOmniScanner(self.executor)
        self.stream_feed_instance = None  # Reference held for hot-swapping

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
        default_params = {"sl_atr_mult": 1.5, "rr_ratio": 2.0}
        try:
            if os.path.exists("params.json"):
                with open("params.json", "r") as f:
                    data = json.load(f)
                    return {**default_params, **data}
        except Exception: 
            pass
        return default_params

    def _save_sgd_state_sync(self):
        state = {}
        for sym, engine in self.stat_engines.items():
            state[sym] = {
                "weights_trending": engine.weights_trending.tolist(),
                "weights_ranging": engine.weights_ranging.tolist(),
                "P_trending": engine.P_trending.tolist(),
                "P_ranging": engine.P_ranging.tolist(),
                "rls_updates": engine.rls_updates
            }
        try:
            target_path = "sgd_state.json"
            fd, path = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(target_path)) or ".")
            with os.fdopen(fd, 'w') as f: json.dump(state, f)
            os.replace(path, target_path)
        except Exception: pass

    async def _save_sgd_state(self):
        await asyncio.to_thread(self._save_sgd_state_sync)

    def _load_sgd_state(self):
        try:
            if not os.path.exists("sgd_state.json"): return
            with open("sgd_state.json", "r") as f:
                state = json.load(f)
            for sym, data in state.items():
                if sym in self.stat_engines:
                    if "weights_trending" in data:
                        self.stat_engines[sym].weights_trending = np.array(data["weights_trending"])
                        self.stat_engines[sym].weights_ranging = np.array(data["weights_ranging"])
                    if "P_trending" in data:
                        self.stat_engines[sym].P_trending = np.array(data["P_trending"])
                        self.stat_engines[sym].P_ranging = np.array(data["P_ranging"])
                    
                    if "rls_updates" in data: self.stat_engines[sym].rls_updates = data["rls_updates"]
            logger.info("🧠 KALMAN RLS MEMORY LOADED: Recovered Covariance Tensors from disk.")
        except Exception: pass

    def _initialize_symbol_structures(self, symbols: List[str]):
        for s in symbols:
            if s not in self.stat_engines: self.stat_engines[s] = ContinuousMicrostructureEngine(symbol=s)
            if s not in self.vpin_clocks: self.vpin_clocks[s] = VolumeSynchronizedClock(bucket_volume=self._get_vpin_bucket_size(s))
            if s not in self.feature_engines: self.feature_engines[s] = AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600)
            if s not in self.edge_gates: self.edge_gates[s] = MicrostructureEdgeGate(window_size=100)
            if s not in self.symbol_locks: self.symbol_locks[s] = asyncio.Lock()
            if s not in self.eval_semaphores: self.eval_semaphores[s] = asyncio.Semaphore(1)
            
            if s not in self.screener_memory: self.screener_memory[s] = {"prices": deque(maxlen=150), "highs": deque(maxlen=150), "lows": deque(maxlen=150), "volumes": deque(maxlen=150), "atr_history": deque(maxlen=100), "last_update_time": 0.0}
            if s not in self.screener_metrics: self.screener_metrics[s] = {"vol_mult": 1.0, "smoothed_price": 0.0}
            if s not in self.volatility_baseline: self.volatility_baseline[s] = 0.0
            if s not in self.ram_dna_cache: self.ram_dna_cache[s] = {"is_armed": True, "win_rate": 0.50}
            if s not in self.last_eval_time: self.last_eval_time[s] = 0.0 
            if s not in self.orderbook_snapshots: self.orderbook_snapshots[s] = {"best_bid": 0.0, "best_ask": 0.0}

    async def _safe_telegram_dispatch(self, message: str, is_html: bool = True, message_type: str = "SUCCESS"):
        for attempt in range(3):
            try:
                if is_html: await self.telegram.send_html_report(message)
                else: await self.telegram.log_message(message, message_type)
                return
            except Exception: await asyncio.sleep(2 ** attempt)

    async def _fetch_exchange_tick_sizes(self):
        try:
            info = await self.executor.safe_call(self.executor.client.get_instruments_info, category="linear")
            for item in info.get("result", {}).get("list", []):
                self.tick_sizes[item.get("symbol")] = float(item.get("priceFilter", {}).get("tickSize", "0.0001"))
        except Exception as e:
            logger.error(f"Failed fetching tick sizes: {e}", exc_info=True)

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
                
                self.active_positions_lock[symbol] = direction
                risk_matrix = {"allocated_value_usdt": qty * entry_price, "size": qty, "recommended_leverage": 8}
                
                daemon_task = self.track_task(self._position_lifecycle_daemon(symbol, f"RECOVERY-{str(uuid.uuid4())[:8]}", direction, entry_price, atr, risk_matrix, 3, "RANGING"))
                self.daemon_tasks[symbol] = daemon_task
        except Exception as e:
            logger.error(f"Failed synchronizing exchange state: {e}", exc_info=True)

    async def cleanup_stale_locks(self):
        while True:
            await asyncio.sleep(300) 
            try:
                for symbol in list(self.active_positions_lock.keys()):
                    active_daemon = self.daemon_tasks.get(symbol)
                    if active_daemon and not active_daemon.done(): continue 

                    if not hasattr(self.risk_vault, 'active_positions') or symbol not in self.risk_vault.active_positions:
                        pos_response = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                        if pos_response.get("retCode") == 0:
                            if not any(float(p.get("size", 0.0)) > 0 for p in pos_response.get("result", {}).get("list", [])):
                                self.active_positions_lock.pop(symbol, None)
            except Exception as e:
                logger.error(f"Failed stale lock cleanup: {e}", exc_info=True)

    async def log_to_wal_async(self, action_type: str, args: list):
        async with self.wal_lock:
            self.wal_batch_queue.append((str(uuid.uuid4()), action_type, json.dumps(args), time.time()))

    def log_to_wal_sync(self, action_type: str, args: list):
        self.track_task(self.log_to_wal_async(action_type, args))

    async def _batch_wal_flush_loop(self):
        while True:
            await asyncio.sleep(5.0)
            
            async with self.wal_lock:
                if not self.wal_batch_queue: continue
                batch_to_process = self.wal_batch_queue[:]
                self.wal_batch_queue.clear()
            
            try:
                async with aiosqlite.connect(self.wal_db_path) as db:
                    await db.executemany("INSERT INTO pending_wal (id, action_type, payload, created_at) VALUES (?, ?, ?, ?)", batch_to_process)
                    await db.commit()
            except Exception as e:
                logger.error(f"Failed SQLite WAL write. Re-queuing {len(batch_to_process)} events: {e}")
                async with self.wal_lock:
                    self.wal_batch_queue = batch_to_process + self.wal_batch_queue

    async def run_db_wal_worker(self):
        logger.info("🖲️ SQLITE WAL ENGINE ONLINE: Durable disk-backed execution logging.")
        try:
            async with aiosqlite.connect(self.wal_db_path) as db:
                await db.execute("""CREATE TABLE IF NOT EXISTS pending_wal (id TEXT PRIMARY KEY, action_type TEXT, payload TEXT, created_at REAL)""")
                await db.commit()
        except Exception as e:
            logger.critical(f"FATAL: SQLite WAL Initialization Failed. Logging will drop! {e}")
            return

        while True:
            try:
                async with aiosqlite.connect(self.wal_db_path) as db:
                    await db.execute("DELETE FROM pending_wal WHERE id IN (SELECT id FROM pending_wal ORDER BY created_at ASC LIMIT -1 OFFSET 50000)")
                    await db.commit()
            
                async with aiosqlite.connect(self.wal_db_path) as db:
                    async with db.execute("SELECT id, action_type, payload FROM pending_wal ORDER BY created_at ASC LIMIT 5") as cursor:
                        rows = await cursor.fetchall()
                        
                for row in rows:
                    item_id, action_type, payload_str = row
                    args = json.loads(payload_str)
                    
                    try:
                        async with self.db_semaphore:
                            if action_type == "prediction": await asyncio.wait_for(asyncio.to_thread(self.memory.commit_prediction, *args), timeout=10.0)
                            elif action_type == "settlement": await asyncio.wait_for(asyncio.to_thread(self.memory.log_live_execution_result, *args), timeout=10.0)
                                
                        async with aiosqlite.connect(self.wal_db_path) as db:
                            await db.execute("DELETE FROM pending_wal WHERE id = ?", (item_id,))
                            await db.commit()
                            
                    except asyncio.TimeoutError: break 
                    except Exception as e:
                        logger.error(f"WAL worker row processing error: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"WAL worker loop error: {e}", exc_info=True)
            await asyncio.sleep(2.0)

    async def run_dna_prewarmer(self):
        logger.info("🔥 RAM PRE-WARMER ONLINE: Pre-fetching database edge logic.")
        while True:
            try:
                await asyncio.wait_for(self.force_dna_refresh.wait(), timeout=300.0)
                self.force_dna_refresh.clear()
            except asyncio.TimeoutError: pass
                
            try:
                async def _safe_fetch(sym, dna):
                    async with self.db_semaphore:
                        return await asyncio.wait_for(asyncio.to_thread(self.memory.compute_latent_dna_edge, dna, 30), timeout=10.0)

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
                logger.error(f"DNA Prewarmer error: {e}", exc_info=True)

    async def run_shadow_resolution_daemon(self):
        logger.info("👻 GHOST FORENSICS ONLINE: Vectorized resolution engine activated.")
        interval_mins = 15.0
        try: interval_mins = float(self.timeframe)
        except Exception: pass

        while True:
            await asyncio.sleep(300) 
            try:
                current_prices = {}
                for sym in self.asset_basket + self.shadow_basket:
                    if self.screener_memory.get(sym) and self.screener_memory[sym].get("prices"):
                        current_prices[sym] = {"prices": list(self.screener_memory[sym]["prices"]), "highs": list(self.screener_memory[sym].get("highs", [])), "lows": list(self.screener_memory[sym].get("lows", []))}
                
                if current_prices:
                    async with self.db_semaphore:
                        await asyncio.wait_for(asyncio.to_thread(self.memory.resolve_batch_historical_predictions, list(current_prices.keys()), current_prices, 60.0, interval_mins), timeout=15.0)
            except Exception as e:
                logger.error(f"Shadow resolution daemon error: {e}", exc_info=True)

    async def run_ai_macro_evaluator(self):
        logger.info("🧠 AI MACRO LOOP ONLINE: Background narrative evaluation on 1-Hour schedule.")
        while True:
            await asyncio.sleep(3600) 
            try:
                news_context = "Market data feed relying on local orderbook."
                try:
                    btc_snap = await asyncio.wait_for(self.data_feed.fetch_market_snapshot("BTCUSDT", "60"), timeout=5.0)
                    if btc_snap: news_context = btc_snap.get("news_context", news_context)
                except Exception: pass

                for symbol in list(self.asset_basket):
                    if self.global_emergency_lock: continue

                    ob = self.orderbook_snapshots.get(symbol)
                    if not ob or ob["best_bid"] == 0.0: continue
                    current_price = (ob["best_bid"] + ob["best_ask"]) / 2.0

                    clock = self.vpin_clocks.get(symbol)
                    if not clock: continue
                    
                    vpin_hist = list(clock.vpin_history)
                    vpin_score = vpin_hist[-1] if vpin_hist else 0.0
                    vpin_z = float((vpin_score - np.mean(vpin_hist)) / (np.std(vpin_hist) + 1e-9)) if len(vpin_hist) >= 20 else 0.0
                        
                    dir_hist = list(clock.directional_imbalances)
                    directional_bias = np.mean(dir_hist) / (clock.bucket_volume + 1e-9) if dir_hist else 0.0
                    
                    vpin_data = {"vpin_score": vpin_score, "vpin_z_score": vpin_z, "directional_bias": directional_bias, "suggested_direction": "BUY" if directional_bias > 0 else "SELL", "current_price": current_price, "is_absorption_anomaly": False, "avg_trade_size": clock.bucket_volume / max(1, clock.current_bucket_ticks)}

                    dna_stats = self.ram_dna_cache.get(symbol, {})
                    verdict = await self.ai_matrix.execute_debate_cycle(symbol, vpin_data, dna_stats, news_context)
                    
                    if verdict.get("schema_valid") and verdict.get("action") in ["BUY", "SELL", "HOLD"]:
                        raw_mult = 1.0 + (verdict.get("confidence", 0.0) * 0.20) if verdict.get("action") != "HOLD" else 1.0
                        self.fsm.update_ai_macro_state(symbol, verdict.get("action"), min(1.20, max(1.0, raw_mult)))
                        
                    await asyncio.sleep(2) 
                    
            except Exception as e:
                logger.error(f"AI Macro Evaluator error: {e}", exc_info=True)

    async def handle_incoming_orderbook_tick(self, depth_data: Dict[str, Any]):
        symbol = depth_data.get("s")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return

        bids, asks = depth_data.get("b", []), depth_data.get("a", [])
        if bids and asks:
            try:
                best_bid, bid_size = float(bids[0][0]), float(bids[0][1])
                best_ask, ask_size = float(asks[0][0]), float(asks[0][1])
                self.orderbook_snapshots[symbol] = {"best_bid": best_bid, "bid_size": bid_size, "best_ask": best_ask, "ask_size": ask_size, "bids": bids, "asks": asks}
                
                stat_engine = self.stat_engines.get(symbol)
                if stat_engine: 
                    stat_engine.update_orderbook_pressure(best_bid, bid_size, best_ask, ask_size)
                    if symbol == "BTCUSDT": self.global_btc_ofi_z = stat_engine.ofi_fast_z
            except Exception as e:
                logger.debug(f"Orderbook pressure update error for {symbol}: {e}")

        is_snapshot = depth_data.get("type") == "snapshot"
        feature_engine = self.feature_engines.get(symbol)
        if feature_engine:
            feature_engine.push_orderbook_tick(bids, asks, is_snapshot=is_snapshot)
            edge_gate = self.edge_gates.get(symbol)
            if edge_gate and bids and asks:
                try:
                    f_bids, f_asks = feature_engine.get_deep_book_floats()
                    mid_price = (float(bids[0][0]) + float(asks[0][0])) / 2.0
                    edge_gate.update_orderbook_state(symbol, f_bids, f_asks, mid_price)
                except Exception as e:
                    logger.debug(f"Edge gate update error for {symbol}: {e}")

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
                vpin_z = float((clock.vpin_history[-1] - np.mean(hist)) / (np.std(hist) + 1e-9)) if len(hist) >= 50 and np.std(hist) > 0 else 0.0
            else: vpin_z = 0.0
        
            throttle_time = 0.2 if abs(vpin_z) > 1.5 else 1.0
            if now - self.last_eval_time.get(symbol, 0.0) < throttle_time: return
            
            ob = self.orderbook_snapshots.get(symbol)
            if not ob or "bid_size" not in ob: return
            spread_cost = abs(ob["best_ask"] - ob["best_bid"]) / (price + 1e-9) if price > 0 else 0.001
            
            # Phase 2: Per-Symbol Semaphore Evaluation
            async with self.eval_semaphores[symbol]:
                structural_verdict = edge_gate.evaluate_structural_edge(symbol, vpin_z)
                if structural_verdict["action"] == "HOLD": return
                
                raw_atr = feature_engine.get_computed_atr() if feature_engine and hasattr(feature_engine, 'get_computed_atr') else 0.0
                atr = raw_atr if raw_atr > 0 else price * 0.005
                sl_atr_mult = self.live_params.get("sl_atr_mult", 1.5)
                rr_ratio = self.live_params.get("rr_ratio", 2.0)
                
                sl_dist_pct = max((atr * sl_atr_mult) / (price + 1e-9), 0.005)
                tp_dist_pct = sl_dist_pct * rr_ratio
                
                virtual_sl = price - (sl_dist_pct * price) if structural_verdict["action"] == "BUY" else price + (sl_dist_pct * price)
                virtual_tp = price + (tp_dist_pct * price) if structural_verdict["action"] == "BUY" else price - (tp_dist_pct * price)
                tensor_alpha = self.tensor_oracle.compute_lead_lag_signal(symbol)
                
                # Phase 3: Minimal Scope Atomic Guard
                async with self.symbol_locks[symbol]:
                    if symbol in self.active_positions_lock: return

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
                    
                    if is_shadow_asset or not dna_stats.get("is_armed", False):
                        if prob_success > 0.65: 
                            self.log_to_wal_sync("prediction", [str(uuid.uuid4()), now, price, action, prob_success, {"symbol": symbol, "market_regime": regime, "virtual_sl": virtual_sl, "virtual_tp": virtual_tp}, True])
                        return 
                    
                    # 🚀 V32.0 FRICTION-ADJUSTED EXPECTED VALUE (EV) CALCULATION
                    taker_fee_pct = 0.0011  # Bybit 0.055% taker fee round-trip
                    net_ev_pct = (prob_success * tp_dist_pct) - ((1.0 - prob_success) * sl_dist_pct) - spread_cost - taker_fee_pct
                    
                    # Compute Risk-Adjusted Sharpe Proxy for Auction Ranking
                    net_sharpe = net_ev_pct / (sl_dist_pct + 1e-9)
                    
                    # Require positive Net EV after fees and spread
                    if net_ev_pct <= 0.0005:  # Must yield at least 5 bps net edge
                        return
                        
                    dynamic_gate = sgd_state.get("dynamic_gate", 0.58)
                    min_threshold = max(dynamic_gate, dna_stats.get("win_rate", 0.50))
                    if prob_success < min_threshold: return
                    
                    # 🚀 V32.0 GLOBAL AUCTION PUSH
                    payload = {
                        "symbol": symbol, "action": action, "price": price, 
                        "prob_success": prob_success, "dna_stats": dna_stats, 
                        "atr": atr, "regime": regime, "net_edge_bps": net_ev_pct * 10000.0, 
                        "vol_z": stat_engine.hawkes_z, "vol_mult": 1.0, "timestamp": now
                    }
                    
                    async with self.auction_lock:
                        heapq.heappush(self.auction_queue, (-net_sharpe, symbol, payload))
                        
                    self.last_eval_time[symbol] = now
                
        except Exception as e:
            logger.error(f"Trade processing fault for {symbol}: {e}", exc_info=True)
            self.tick_error_counts[symbol] = [t for t in self.tick_error_counts.get(symbol, []) if now - t < 60]
            self.tick_error_counts[symbol].append(now)
            if len(self.tick_error_counts[symbol]) > 5:
                self.circuit_breakers[symbol] = now + 300 
                logger.error(f"🛑 CIRCUIT BREAKER TRIGGERED for {symbol}. Paused for 5 minutes.")

    async def run_global_capital_auction_worker(self):
        """
        🚀 V32.0 GLOBAL CAPITAL AUCTION WORKER (PATCHED)
        Runs continuously in the background. Batches candidate trade signals every 500ms,
        ranks them by Friction-Adjusted Sharpe Ratio, and deploys available capital slots 
        only to the single highest-conviction asset in the universe. Re-queues valid runners-up.
        """
        logger.info("🏛️ GLOBAL CAPITAL AUCTION ENGINE ONLINE: Processing Priority Matrix.")
        while True:
            await asyncio.sleep(0.5)  # 500ms auction window
            
            async with self.auction_lock:
                if not self.auction_queue:
                    continue
                
                candidates = []
                while self.auction_queue:
                    candidates.append(heapq.heappop(self.auction_queue))
            
            if not candidates: continue
            
            top_neg_sharpe, top_symbol, top_payload = candidates[0]
            top_sharpe = -top_neg_sharpe
            
            # 🚀 V33.1 FIX: Put viable runners-up back in the queue
            async with self.auction_lock:
                for i in range(1, len(candidates)):
                    # Only re-queue if it's less than 5 seconds old
                    if time.time() - candidates[i][2]["timestamp"] < 5.0:
                        heapq.heappush(self.auction_queue, candidates[i])

            if len(self.active_positions_lock) >= 5:
                continue
                
            async with self.symbol_locks[top_symbol]:
                if top_symbol in self.active_positions_lock:
                    continue
                    
                self.active_positions_lock[top_symbol] = top_payload["action"]
                
                logger.critical(
                    f"🏛️ AUCTION WINNER // {top_symbol} [{top_payload['regime']}] | "
                    f"{top_payload['action']} | Net Sharpe: {top_sharpe:.2f} | "
                    f"Prob: {top_payload['prob_success']:.2%} | Net Edge: {top_payload['net_edge_bps']:.1f} bps"
                )
                
                self.track_task(self.execute_statistical_signal(
                    top_payload["symbol"], top_payload["action"], top_payload["price"], 
                    top_payload["prob_success"], top_payload["dna_stats"], top_payload["atr"], 
                    top_payload["regime"], top_payload["net_edge_bps"], top_payload["vol_z"], top_payload["vol_mult"]
                ))

    async def run_omni_swarm_director(self):
        """
        🚀 V33.0 OMNI-SWARM DIRECTOR
        Monitors 250+ Bybit Perpetuals for Idiosyncratic Alpha.
        Hot-swaps stagnant assets for explosive assets in real-time.
        """
        logger.info("🌪️ OMNI-SWARM DIRECTOR ONLINE: Monitoring 250+ Global Vectors.")
        while True:
            await asyncio.sleep(15)  # Scan entire market every 15 seconds
            
            try:
                dead_sym, hot_sym = await self.omni_scanner.scan_and_rank_universe(self.asset_basket)
                
                if dead_sym and hot_sym:
                    # 1. Update Asset Array Safely
                    if dead_sym in self.asset_basket:
                        self.asset_basket.remove(dead_sym)
                    if hot_sym not in self.asset_basket:
                        self.asset_basket.append(hot_sym)
                    
                    # 2. Initialize Internal Memory Structures for the new coin instantly
                    self._initialize_symbol_structures([hot_sym])
                    
                    # 3. Fire WebSocket command to the ingestion stream dynamically 
                    if self.stream_feed_instance and hasattr(self.stream_feed_instance, 'hot_swap_socket_stream'):
                        await self.stream_feed_instance.hot_swap_socket_stream(dead_sym, hot_sym)
                        
                    logger.critical(f"🚀 {hot_sym} FULLY ARMED AND INJECTED INTO QUANT MATRIX.")
            except Exception as e:
                logger.error(f"Omni-Swarm Director iteration failed: {e}")

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
            except Exception as e: 
                logger.error(f"Failed to fetch balance for {symbol} entry: {e}", exc_info=True)
                self.active_positions_lock.pop(symbol, None)
                return
                
            start_bal = self.global_state_cache.get("start_of_day_balance", balance)
            
            if start_bal > 0 and balance < (start_bal * 0.95):
                logger.critical(f"🛑 DAILY LOSS LIMIT TRIGGERED. Balance ({balance:.2f}) dropped below 95% of start ({start_bal:.2f}).")
                self.global_emergency_lock = True
                self.active_positions_lock.pop(symbol, None)
                return
            
            correlation_penalty = 1.0
            if hasattr(self.risk_vault, "correlation_groups"):
                for group, assets in self.risk_vault.correlation_groups.items():
                    if symbol in assets:
                        active_correlated = sum(1 for a, a_dir in self.active_positions_lock.items() if a in assets and a != symbol and a_dir == direction)
                        if active_correlated > 0: correlation_penalty = max(0.25, 1.0 - (active_correlated * 0.25))
            
            edge = confidence - 0.50
            if edge <= 0.02: 
                self.active_positions_lock.pop(symbol, None)
                return
                
            base_risk = 0.01
            risk_multiplier = edge / 0.10  
            account_scaling = 0.75 if balance < 100.0 else 1.0
            fractional_risk = max(0.005, min(0.025, base_risk * risk_multiplier * account_scaling * correlation_penalty))

            if balance < 1.0: 
                self.active_positions_lock.pop(symbol, None)
                return

            dollar_risk = balance * fractional_risk
            position_size = max(dollar_risk / sl_distance, 6.00 / (current_price + 1e-9))
            notional = position_size * current_price

            if not self.risk_vault.evaluate_portfolio_safety(balance, notional, symbol): 
                self.active_positions_lock.pop(symbol, None)
                return

            target_leverage = self.risk_vault.calculate_dynamic_leverage(notional, balance, sl_distance_pct=(sl_distance / current_price))
            
            if self.test_mode:
                execution_success = random.random() < 0.85
                if execution_success:
                    base_slip = 2.0
                    tox_multiplier = max(1.0, abs(vol_z))
                    slippage_bps = random.lognormvariate(math.log(base_slip * tox_multiplier), 0.6)
                    
                    penalty_factor = (1 + slippage_bps / 10000) if direction == "BUY" else (1 - slippage_bps / 10000)
                    current_price *= penalty_factor
                    logger.critical(f"📜 PAPER TRADE EXECUTED: {symbol} {direction} {position_size} @ {current_price:.4f} (Slip: {slippage_bps:.1f} bps)")
            else:
                try:
                    await self.executor.safe_call(self.executor.adjust_leverage, symbol, target_leverage)
                    await asyncio.sleep(0.2) 
                except Exception as e: 
                    logger.error(f"Leverage adjustment failed for {symbol}: {e}", exc_info=True)
                    self.active_positions_lock.pop(symbol, None)
                    return

                feature_engine = self.feature_engines.get(symbol)
                current_depth = feature_engine.get_orderbook_snapshot() if feature_engine and hasattr(feature_engine, 'get_orderbook_snapshot') else {"bids": [[current_price, 1]], "asks": [[current_price, 1]]}

                if regime == "TRENDING": execution_success = await self.sor.execute_iceberg_block(symbol=symbol, direction=direction, total_qty=position_size, current_mid_price=current_price, stop_loss=initial_sl_price, take_profit=target_tp_price, depth_snapshot=current_depth, vol_z=vol_z, vol_mult=vol_mult, feature_engine=feature_engine)
                else: execution_success = await self.sor.execute_mean_reversion_bracket(symbol=symbol, direction=direction, total_qty=position_size, current_mid_price=current_price, stop_loss=initial_sl_price, take_profit=target_tp_price, depth_snapshot=current_depth, vol_z=vol_z, vol_mult=vol_mult, feature_engine=feature_engine)
            
            if not execution_success: 
                self.active_positions_lock.pop(symbol, None)
                return 
                
            if not self.test_mode:
                self.log_to_wal_sync("prediction", [signal_id, time.time(), current_price, direction, confidence, {"symbol": symbol, "market_regime": regime, "virtual_sl": initial_sl_price, "virtual_tp": target_tp_price}, False])
                
                # 🚀 REAL-TIME TELEGRAM ENTRY ALERT
                entry_msg = (
                    f"⚡ <b>ENTRY ALERT // {symbol}</b>\n"
                    f"• Action: <b>{direction}</b>\n"
                    f"• Price: <code>{current_price:.5f}</code>\n"
                    f"• Size: <code>{position_size:.2f}</code>\n"
                    f"• Edge: <code>{edge_bps:.1f} bps</code>"
                )
                self.track_task(self._safe_telegram_dispatch(entry_msg, is_html=True))
                
            self.risk_vault.update_position_ledger(symbol, notional)
            self.daemon_tasks[symbol] = self.track_task(self._position_lifecycle_daemon(symbol, signal_id, direction, current_price, atr, {"allocated_value_usdt": notional, "size": position_size}, target_leverage, regime))
            
        except Exception as e:
            logger.error(f"Critical failure in execute_statistical_signal for {symbol}: {e}", exc_info=True)
            self.active_positions_lock.pop(symbol, None)

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
        except Exception as e:
            logger.error(f"Universe refresher failed fetching assets: {e}", exc_info=True)
            full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
            
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
            new_stat[s] = self.stat_engines.get(s, ContinuousMicrostructureEngine(symbol=s))
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
        except Exception as e:
            logger.error(f"Correlation matrix update failed: {e}", exc_info=True)

        self.stream_restart_event.set()
        self.force_dna_refresh.set() 

    async def _universe_refresher_loop(self):
        while True:
            await asyncio.sleep(14400)
            await self.run_universe_refresher()

    async def stream_manager_loop(self):
        while True:
            stream_feed = HighVelocityMultiFeed(basket=self.asset_basket + self.shadow_basket[:10], intervals=["1", "5", "15"], orderbook_callback=self.handle_incoming_orderbook_tick, screener_callback=self.handle_incoming_basket_screener_update, kline_callback=self.handle_incoming_kline_update, trade_callback=self.handle_incoming_trade, engine_reference=self)
            self.stream_feed_instance = stream_feed  # 🚀 Expose to Omni-Swarm for Hot-Swapping
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
                except Exception as e:
                    logger.debug(f"Heartbeat balance fetch failed: {e}")
                    continue

                if "wallet_baseline" not in self.global_state_cache: self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01)
                
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                current_day = now_utc.strftime("%Y-%m-%d")
                if self.global_state_cache.get("current_day") != current_day:
                    self.global_state_cache["current_day"] = current_day
                    self.global_state_cache["start_of_day_balance"] = current_vault_balance
                    self.global_emergency_lock = False

                today_start_iso = now_utc.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                try:
                    def _fetch(): return self.memory.supabase.table("quantitative_ledger").select("market_regime, net_pnl, symbol, predicted_direction, actual_outcome").eq("resolved", True).eq("is_shadow", False).gte("timestamp", today_start_iso).execute()
                    async with self.db_semaphore: data = (await asyncio.to_thread(_fetch)).data or []
                except Exception as e:
                    logger.debug(f"Heartbeat ledger fetch failed: {e}")
                    data = []

                actual_net_pnl = current_vault_balance - self.global_state_cache.get("start_of_day_balance", current_vault_balance)
                baseline = self.global_state_cache["wallet_baseline"]
                if current_vault_balance > baseline:
                    self.global_state_cache["wallet_baseline"] = current_vault_balance
                    baseline = current_vault_balance
                    
                drawdown_pct = max(0.0, (baseline - current_vault_balance) / baseline)
                
                if drawdown_pct >= 0.25:
                    await self._safe_telegram_dispatch(f"🚨 <b>EMERGENCY DRAWDOWN BREAKER TRIPPED</b>\nDrawdown: {drawdown_pct:.2%}. Engine shutting down.", is_html=True)
                    try:
                        async with aiosqlite.connect(self.wal_db_path) as db:
                            async with db.execute("SELECT COUNT(*) FROM pending_wal") as cursor:
                                remaining = (await cursor.fetchone())[0]
                                if remaining > 0: logger.warning(f"⏳ {remaining} items in SQLite WAL buffer. Force shutting down.")
                    except Exception as e:
                        logger.error(f"Drawdown WAL check failed: {e}")
                    
                    await self.graceful_shutdown()
                    sys.exit(0)
                
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
                except Exception as e:
                    logger.error(f"Heartbeat regime stat calculation failed: {e}")
                    regime_text, recent_trades = "• ⚠️ <i>Supabase ledger context error.</i>\n", "• <i>Unavailable</i>\n"

                report = (
                    f"💎 <b>𝗣██𝗔𝗦𝗞 𝗘𝗠𝗣𝗜𝗥𝗘 | 𝗤𝗨𝗔𝗡𝗧 𝗦𝗪𝗔𝗥𝗠 (V33.0 OMNI-SWARM)</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏱️ <b>𝗨𝗽𝘁𝗶𝗺𝗲:</b> <code>{uptime_hours:.2f} Hours</code> | 🛰️ <b>𝗡𝗼𝗱𝗲𝘀:</b> <code>{len(self.asset_basket)} Live</code>\n\n"
                    f"⚙️ <b>𝗘𝗡𝗚𝗜𝗡𝗘 𝗦𝗧𝗔𝗧𝗨𝗦: 𝖦𝗅𝗈𝖻𝖺𝗅 𝖧𝗈𝗍-𝖲𝗐𝖺𝗉 𝖠𝗋𝖼𝗁𝗂𝗍𝖾𝖼𝗍𝗎𝗋𝖾</b>\n"
                    f"• Signal Engine:   <code>Cluster Warm-Started RLS + Friction Penalty</code>\n"
                    f"• Omni Scanner:    <code>Beta-Stripped Alpha Identification (250+ Assets)</code>\n"
                    f"• Execution Guard: <code>Infinite Loop Drawdown Breaker Verification</code>\n"
                    f"• Risk Engine:     <code>5% Daily Limit | Strict Max 5x Leverage</code>\n\n"
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
                except Exception as e:
                    logger.debug(f"Position fetch failed for {symbol}: {e}")
                    continue

            if not order_filled:
                try: await self.executor.safe_call(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
                except Exception as e: logger.debug(f"Cancel orders failed for {symbol}: {e}")
                
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
                except Exception as e:
                    logger.debug(f"Set trading stop failed for {symbol}: {e}")
                    await asyncio.sleep(2)
                
            if not stops_verified:
                try:
                    pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                    pos_list = pos_res.get("result", {}).get("list", [])
                    if pos_list and float(pos_list[0].get("size", 0.0)) > 0:
                        await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side="Sell" if pos_list[0].get("side") == "Buy" else "Buy", orderType="Market", qty=str(float(pos_list[0].get("size"))), timeInForce="IOC", reduceOnly=True)
                except Exception as e:
                    logger.debug(f"Panic flatten failed for {symbol}: {e}")
                self.active_positions_lock.pop(symbol, None)
                return

            act_raw = actual_entry + (atr * 0.8) if direction == "BUY" else actual_entry - (atr * 0.8)
            try: await self.executor.safe_call(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, takeProfit=realigned_tp_str, stopLoss=realigned_sl_str, trailingStop=align_price(atr * 1.5), activePrice=align_price(act_raw))
            except Exception as e:
                logger.debug(f"Trailing stop set failed for {symbol}: {e}")

            while True: 
                await asyncio.sleep(20)
                if time.time() - daemon_start_time > max_lifetime_seconds:
                    try:
                        pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                        if float(pos_res.get("result", {}).get("list", [{}])[0].get("size", 0.0)) > 0:
                            await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side="Sell" if pos_res["result"]["list"][0]["side"] == "Buy" else "Buy", orderType="Market", qty=str(float(pos_res["result"]["list"][0]["size"])), timeInForce="IOC", reduceOnly=True)
                    except Exception as e:
                        logger.error(f"Timeout panic flatten failed for {symbol}: {e}")
                        
                    self.active_positions_lock.pop(symbol, None)
                    self.risk_vault.update_position_ledger(symbol, 0.0)
                    break

                try:
                    pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                    position_gone = (not pos_res.get("result", {}).get("list", [])) or float(pos_res["result"]["list"][0].get("size", 0.0)) == 0.0
                except Exception as e:
                    logger.debug(f"Position check failed for {symbol}: {e}")
                    position_gone = False

                try:
                    settlement = await self.executor.check_recent_settlement(symbol, 300) 
                    if settlement.get("closed"):
                        pnl = float(settlement.get('pnl', 0.0))
                        self.log_to_wal_sync("settlement", [signal_id, pnl, actual_entry - current_price if direction == "BUY" else current_price - actual_entry, settlement['outcome'], exec_details])
                        self.risk_vault.update_position_ledger(symbol, 0.0)
                        
                        # 🚀 REAL-TIME TELEGRAM EXIT ALERT
                        exit_msg = (
                            f"🏁 <b>TRADE CLOSED // {symbol}</b>\n"
                            f"• Outcome: <b>{settlement['outcome']}</b>\n"
                            f"• Net PnL: <code>{pnl:+.4f} USDT</code>"
                        )
                        self.track_task(self._safe_telegram_dispatch(exit_msg, is_html=True))
                        break
                except Exception as e:
                    logger.debug(f"Settlement check failed for {symbol}: {e}")

                if position_gone:
                    try: net_pnl = float((await self.executor.safe_call(self.executor.client.get_closed_pnl, category="linear", symbol=symbol, limit=5)).get("result", {}).get("list", [])[0].get("closedPnl", 0.0))
                    except Exception as e:
                        logger.debug(f"Closed PnL fetch failed for {symbol}: {e}")
                        net_pnl = 0.0
                    
                    self.log_to_wal_sync("settlement", [signal_id, net_pnl, 0.0, "RECONCILED", exec_details])
                    self.risk_vault.update_position_ledger(symbol, 0.0)
                    
                    # 🚀 REAL-TIME TELEGRAM RECONCILIATION ALERT
                    exit_msg = (
                        f"🏁 <b>TRADE RECONCILED // {symbol}</b>\n"
                        f"• Net PnL: <code>{net_pnl:+.4f} USDT</code>"
                    )
                    self.track_task(self._safe_telegram_dispatch(exit_msg, is_html=True))
                    break

        except Exception as e:
            logger.error(f"Position daemon critical fault for {symbol}: {e}", exc_info=True)
            try:
                pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                if float(pos_res.get("result", {}).get("list", [{}])[0].get("size", 0.0)) > 0:
                    await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side="Sell" if pos_res["result"]["list"][0]["side"] == "Buy" else "Buy", orderType="Market", qty=str(float(pos_res["result"]["list"][0]["size"])), timeInForce="IOC", reduceOnly=True)
            except Exception as inner_e:
                logger.error(f"Panic flatten failed in daemon exception block for {symbol}: {inner_e}")
        finally:
            self.active_positions_lock.pop(symbol, None)
            self.risk_vault.update_position_ledger(symbol, 0.0)

    async def graceful_shutdown(self):
        logger.critical("🛑 INITIATING EMERGENCY FLATTEN & SHUTDOWN...")
        
        for symbol in list(self.active_positions_lock.keys()):
            try:
                await self.executor.safe_call(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
            except Exception as e:
                logger.error(f"Shutdown order cancellation failed for {symbol}: {e}")
        
        for symbol in list(self.active_positions_lock.keys()):
            try:
                pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                if float(pos_res.get("result", {}).get("list", [{}])[0].get("size", 0.0)) > 0:
                    current_side = pos_res["result"]["list"][0]["side"]
                    close_side = "Sell" if current_side == "Buy" else "Buy"
                    qty = str(float(pos_res["result"]["list"][0]["size"]))
                    await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side=close_side, orderType="Market", qty=qty, timeInForce="IOC", reduceOnly=True)
            except Exception as e:
                logger.error(f"Shutdown flatten order failed for {symbol}: {e}")
                
        logger.critical("🔍 VERIFYING ZERO EXPOSURE...")
        max_verify_attempts = 10
        for attempt in range(max_verify_attempts):
            try:
                pos_response = await self.executor.safe_call(self.executor.client.get_positions, category="linear", settleCoin="USDT")
                active_orphans = [p for p in pos_response.get("result", {}).get("list", []) if float(p.get("size", 0.0)) > 0]
                
                if not active_orphans:
                    logger.critical("✅ EXPOSURE VERIFIED AT ZERO. ALL POSITIONS FLATTENED.")
                    break
                else:
                    logger.error(f"⚠️ {len(active_orphans)} positions still open. Retrying flatten sequence (Attempt {attempt+1}/{max_verify_attempts}).")
                    for p in active_orphans:
                        sym = p["symbol"]
                        close_side = "Sell" if p["side"] == "Buy" else "Buy"
                        qty = str(float(p["size"]))
                        try:
                            await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=sym, side=close_side, orderType="Market", qty=qty, timeInForce="IOC", reduceOnly=True)
                        except Exception as e:
                            logger.error(f"Retry flatten failed for {sym}: {e}")
                    await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Exposure verification API call failed: {e}")
                await asyncio.sleep(5)
                
        if attempt == max_verify_attempts - 1:
            logger.critical("💀 FATAL: COULD NOT VERIFY ZERO EXPOSURE AFTER MAXIMUM RETRIES. MANUAL INTERVENTION REQUIRED.")
            if hasattr(self, 'telegram'): 
                await self.telegram.send_html_report("🚨 <b>FATAL SHUTDOWN ERROR</b>\nCould not flatten all positions. Manual intervention required immediately.")
            
        try:
            async with aiosqlite.connect(self.wal_db_path) as db:
                async with db.execute("SELECT COUNT(*) FROM pending_wal") as cursor:
                    count = (await cursor.fetchone())[0]
            logger.info(f"⏳ WAL Engine offline. {count} items remaining in local disk buffer for next reboot.")
        except Exception as e:
            logger.error(f"WAL shutdown check failed: {e}")
            
        if hasattr(self, 'telegram'): await self.telegram.close()
        
        if hasattr(self.executor, "executor"):
            logger.info("🧹 Sweeping Executor ThreadPool...")
            self.executor.executor.shutdown(wait=False, cancel_futures=True)
            
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
        self.global_emergency_lock = False
        try: await self._fetch_exchange_tick_sizes()
        except Exception as e: logger.warning(f"Engine boot tick size fetch failed: {e}")
        
        try: await self.synchronize_exchange_state()
        except Exception as e: logger.warning(f"Engine boot sync state failed: {e}")
            
        try:
            boot_bal = await self.executor.get_wallet_balance_usdt()
            self.global_state_cache["start_of_day_balance"] = boot_bal
            self.global_state_cache["wallet_baseline"] = max(boot_bal, 0.01)
            self.global_state_cache["last_updated"] = time.time()
            self.global_state_cache["current_day"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        except Exception as e: logger.warning(f"Engine boot balance fetch failed: {e}")
        
        try: full_market = await self.executor.get_top_volatile_assets(limit=100, min_turnover=10_000_000)
        except Exception as e:
            logger.error(f"Engine boot full market fetch failed: {e}")
            full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
            
        if "BTCUSDT" in full_market: full_market.remove("BTCUSDT")
        
        if boot_basket := full_market[:25]:
            self.asset_basket = ["BTCUSDT"] + boot_basket[:24]
            self._initialize_symbol_structures(self.asset_basket)
        
        daemons = [
            self.run_db_wal_worker, self._batch_wal_flush_loop, self.run_dna_prewarmer, 
            self.stream_manager_loop, self.run_system_heartbeat, self.cleanup_stale_locks, 
            self.run_shadow_resolution_daemon, self.run_ai_macro_evaluator, self._universe_refresher_loop,
            self.run_global_capital_auction_worker,  # 🚀 V32.0 AUCTION DAEMON
            self.run_omni_swarm_director             # 🚀 V33.0 OMNI-SWARM DAEMON
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