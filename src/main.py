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
from itertools import permutations
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

# Core & Feature Modules
from core.fsm import SystemStateMachine
from core.memory import MemoryBank
from core.edge_gate import MicrostructureEdgeGate
from core.micro_elasticity import MicroElasticityEngine 
from features.adaptive_engine import AdaptiveFeatureEngine
from features.vpin_clock import VolumeSynchronizedClock
from features.omni_scanner import GlobalOmniScanner  
from portfolio.risk_manager import InstitutionalRiskVault  
from execution.sor import SmartOrderRouter

# External Connectors
from ingestion.multi_feed import HighVelocityMultiFeed
from services.bybit_v5 import BybitUnifiedExecutor
from services.telegram_ops import AsyncTelegramReporter
from services.data_feed import AsynchronousDataFeed
from services.tensor_oracle import CrossAssetTensorOracle

logging.getLogger("httpx").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(name)s] - [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("QUANT_CORE.V41.17_PATCHED")


class ClusterWarmStartRLS:
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


def compute_permutation_entropy(series: list, order: int = 3, delay: int = 1) -> float:
    if len(series) < (order * delay): 
        return 1.0
    
    sub_vectors = []
    for i in range(len(series) - (order - 1) * delay):
        sub_vectors.append([series[i + j * delay] for j in range(order)])
        
    perm_counts = {perm: 0 for perm in permutations(range(order))}
    
    for vec in sub_vectors:
        rank = tuple(np.argsort(vec))
        perm_counts[rank] += 1
        
    total = len(sub_vectors)
    entropy = 0.0
    for count in perm_counts.values():
        if count > 0:
            p = count / total
            entropy -= p * math.log2(p)
            
    max_entropy = math.log2(math.factorial(order))
    return float(entropy / max_entropy)


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
        self.true_micro_price = 0.0
        
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
        self.entropy_history = deque(maxlen=200) 
        
        w_t, w_r, P_init = ClusterWarmStartRLS.get_cluster_priors(symbol)
        self.weights_trending = w_t
        self.weights_ranging  = w_r
        self.P_trending = P_init.copy()
        self.P_ranging = P_init.copy()
        
        self.prediction_buffer = deque(maxlen=50000)
        self.historical_probs = deque(maxlen=2000) 
        self.rls_updates = 100 
        self.ewma_mse = 0.25 

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
        self.true_micro_price = (best_bid * ask_vol + best_ask * bid_vol) / (bid_vol + ask_vol + 1e-9)
        if current_mid > 0:
            self.micro_price_skew = ((self.true_micro_price - current_mid) / (current_mid + 1e-9)) * 10000.0 

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
                    self.ewma_mse = (0.98 * self.ewma_mse) + (0.02 * (error ** 2))

                    x = features_array.reshape(-1, 1)
                    var_pi = max(1e-4, old_pred_prob * (1.0 - old_pred_prob))  
                    
                    dynamic_lambda = max(0.990, min(0.9995, 0.990 + (self.shannon_entropy * 0.0095)))
                    
                    if r_blend > 0.1:
                        P_x_t = self.P_trending @ x
                        den_t = dynamic_lambda + var_pi * float((x.T @ P_x_t)[0][0])
                        K_t = P_x_t / den_t
                        update_t = (K_t.flatten() * error * r_blend)
                        self.weights_trending = self.weights_trending + update_t
                        self.P_trending = (self.P_trending - var_pi * (K_t @ (x.T @ self.P_trending))) / dynamic_lambda
                        
                        trace_t = np.trace(self.P_trending)
                        if trace_t > 1000.0:
                            self.P_trending = (self.P_trending * (1000.0 / trace_t)) + (np.eye(9) * 0.01)
                        else:
                            self.P_trending += np.eye(9) * 1e-5

                    r_range = 1.0 - r_blend
                    if r_range > 0.1:
                        P_x_r = self.P_ranging @ x
                        den_r = dynamic_lambda + var_pi * float((x.T @ P_x_r)[0][0])
                        K_r = P_x_r / den_r
                        update_r = (K_r.flatten() * error * r_range)
                        self.weights_ranging = self.weights_ranging + update_r
                        self.P_ranging = (self.P_ranging - var_pi * (K_r @ (x.T @ self.P_ranging))) / dynamic_lambda
                        
                        trace_r = np.trace(self.P_ranging)
                        if trace_r > 1000.0:
                            self.P_ranging = (self.P_ranging * (1000.0 / trace_r)) + (np.eye(9) * 0.01)
                        else:
                            self.P_ranging += np.eye(9) * 1e-5
                    
                    self.P_trending = (self.P_trending + self.P_trending.T) / 2.0 + (np.eye(9) * 1e-6)
                    self.P_ranging = (self.P_ranging + self.P_ranging.T) / 2.0 + (np.eye(9) * 1e-6)
                    self.rls_updates += 1

    def calibrate_confidence(self, prob: float, regime: str, mse: float) -> float:
        floor = 0.50
        ceiling = 0.85

        if regime in ["TRENDING_BULL", "TRENDING_BEAR", "TRENDING"]:
            ceiling = min(0.92, ceiling + 0.07)
            floor = max(0.48, floor - 0.02)
        elif regime == "LIQUIDITY_VACUUM":
            ceiling = min(0.90, ceiling + 0.05)
            floor = max(0.52, floor + 0.03)
        else:
            ceiling = min(0.80, ceiling - 0.05)
            floor = max(0.52, floor + 0.03)

        mse_penalty = min(0.10, mse * 0.4)
        ceiling = max(floor, ceiling - mse_penalty)

        return max(floor, min(ceiling, prob))

    def extract_statistical_state(self, current_price: float, vpin_z: float, tensor_alpha: float, sl_dist_pct: float, tp_dist_pct: float, exchange_timestamp: float) -> dict:
        if len(self.prices) > 10:
            self.shannon_entropy = compute_permutation_entropy(list(self.prices)[-20:])
            self.entropy_history.append(self.shannon_entropy) 
        
        if len(self.prices) >= 20:
            prices_arr = np.array(list(self.prices)[-20:])
            directional_change = abs(prices_arr[-1] - prices_arr[0])
            path_volatility = np.sum(np.abs(np.diff(prices_arr))) + 1e-9
            self.kaufman_er = float(directional_change / path_volatility)
        else:
            self.kaufman_er = 0.5

        if len(self.log_returns) > 30 and (exchange_timestamp - self.last_hurst_time > 5.0):
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
            self.last_hurst_time = exchange_timestamp

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
            error_penalty = max(0.0, (self.ewma_mse - 0.25) * 2.0)
            dynamic_gate = mean_prob + (1.25 * std_prob) + error_penalty
        else:
            dynamic_gate = 0.52 
            
        dynamic_gate = max(0.52, dynamic_gate)
            
        if len(self.entropy_history) > 30:
            entropy_arr = np.array(self.entropy_history)
            entropy_mean = np.mean(entropy_arr)
            entropy_std = np.std(entropy_arr) + 1e-9
            entropy_z = (self.shannon_entropy - entropy_mean) / entropy_std
            
            if entropy_z > 2.0: 
                dynamic_gate = min(0.85, dynamic_gate + 0.05)
        
        virtual_sl = current_price - (sl_dist_pct * current_price) if action_dir == "BUY" else current_price + (sl_dist_pct * current_price)
        virtual_tp = current_price + (tp_dist_pct * current_price) if action_dir == "BUY" else current_price - (tp_dist_pct * current_price)

        self.prediction_buffer.append((exchange_timestamp, current_price, attended_features, prob_success, virtual_sl, virtual_tp, action_dir, r_blend))
        
        return {
            "p_up": p_up, "p_down": p_down, "action_dir": action_dir, 
            "entropy": self.shannon_entropy, "r_blend": r_blend, 
            "dynamic_gate": dynamic_gate, "virtual_sl": virtual_sl, "virtual_tp": virtual_tp
        }


class DistributedQuantEngine:
    def __init__(self):
        load_dotenv()
        self.test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
        
        if self.test_mode: logger.critical("⚠️ TEST MODE: Paper Trading Armed.")
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
        self.circuit_breaker_lock = asyncio.Lock() 
        
        self.stream_restart_event = asyncio.Event()
        self.force_dna_refresh = asyncio.Event() 
        
        self.fsm = SystemStateMachine()
        self.memory = MemoryBank()
        self.risk_vault = InstitutionalRiskVault(max_drawdown_pct=0.25, max_single_position_risk_pct=0.015)
        
        self.data_feed = AsynchronousDataFeed(finnhub_key=os.getenv("FINNHUB_API_KEY", ""))
        self.tensor_oracle = CrossAssetTensorOracle()
        
        self.stat_engines: Dict[str, ContinuousMicrostructureEngine] = {} 
        self.vpin_clocks: Dict[str, VolumeSynchronizedClock] = {}
        self.feature_engines: Dict[str, AdaptiveFeatureEngine] = {}
        self.edge_gates: Dict[str, MicrostructureEdgeGate] = {}
        self.elasticity_engines: Dict[str, MicroElasticityEngine] = {} 
        
        self.screener_memory: Dict[str, Dict[str, Any]] = {}
        self.screener_metrics: Dict[str, Dict[str, float]] = {}
        self.orderbook_snapshots: Dict[str, dict] = {}
        self.ram_dna_cache: Dict[str, dict] = {}
        
        self.volatility_baseline: Dict[str, float] = {}
        
        self.portfolio_state_lock = asyncio.Lock()
        self.active_positions_map: Dict[str, str] = {}  
        
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
        self.last_socket_reconnect = 0.0 
        
        self.funding_rates: Dict[str, float] = {}
        self.open_interests: Dict[str, float] = {}
        self.spread_history: Dict[str, deque] = {}
        
        self._initialize_symbol_structures(self.asset_basket)
        self._load_sgd_state()

        self.telegram = AsyncTelegramReporter(token=os.getenv("TELEGRAM_BOT_TOKEN"), chat_id=os.getenv("TELEGRAM_CHAT_ID"))
        
        self.executor = BybitUnifiedExecutor(
            api_key=os.getenv("BYBIT_API_KEY"),
            api_secret=os.getenv("BYBIT_API_SECRET"),
            testnet=os.getenv("BYBIT_TESTNET", "false").lower() == "true",
            max_workers=8
        )
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
                    self.screener_memory.pop(key, None)
                    self.screener_metrics.pop(key, None)
                    self.last_eval_time.pop(key, None)

    async def _save_sgd_state(self):
        # 🚀 V41.17 FIX: Thread-safe memory serialization
        state_snapshot = {}
        async with self.portfolio_state_lock:
            for sym, engine in self.stat_engines.items():
                state_snapshot[sym] = {
                    "weights_trending": engine.weights_trending.copy().tolist(),
                    "weights_ranging": engine.weights_ranging.copy().tolist(),
                    "P_trending": engine.P_trending.copy().tolist(),
                    "P_ranging": engine.P_ranging.copy().tolist(),
                    "rls_updates": engine.rls_updates
                }
                
        def _write_file():
            try:
                target_path = "sgd_state.json"
                fd, path = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(target_path)) or ".")
                with os.fdopen(fd, 'w') as f: 
                    json.dump(state_snapshot, f)
                os.replace(path, target_path)
            except Exception as e:
                logger.debug(f"Failed RLS disk serialization: {e}")

        await asyncio.to_thread(_write_file)

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
        except Exception as e: logger.debug(f"Failed to load RLS state: {e}", exc_info=True)

    def _initialize_symbol_structures(self, symbols: List[str]):
        for s in symbols:
            if s not in self.stat_engines: self.stat_engines[s] = ContinuousMicrostructureEngine(symbol=s)
            if s not in self.vpin_clocks: self.vpin_clocks[s] = VolumeSynchronizedClock(bucket_volume=self._get_vpin_bucket_size(s))
            if s not in self.feature_engines: self.feature_engines[s] = AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600)
            if s not in self.edge_gates: self.edge_gates[s] = MicrostructureEdgeGate(window_size=100)
            if s not in self.elasticity_engines: self.elasticity_engines[s] = MicroElasticityEngine() 
            if s not in self.symbol_locks: self.symbol_locks[s] = asyncio.Lock()
            if s not in self.eval_semaphores: self.eval_semaphores[s] = asyncio.Semaphore(1)
            
            if s not in self.spread_history: self.spread_history[s] = deque(maxlen=50)
            if s not in self.screener_memory: self.screener_memory[s] = {"prices": deque(maxlen=1440), "highs": deque(maxlen=150), "lows": deque(maxlen=150), "volumes": deque(maxlen=1440), "atr_history": deque(maxlen=100), "last_update_time": 0.0}
            if s not in self.screener_metrics: self.screener_metrics[s] = {"vol_mult": 1.0, "smoothed_price": 0.0}
            if s not in self.volatility_baseline: self.volatility_baseline[s] = 0.0
            if s not in self.ram_dna_cache: self.ram_dna_cache[s] = {"is_armed": True, "win_rate": 0.50}
            if s not in self.last_eval_time: self.last_eval_time[s] = 0.0 
            if s not in self.orderbook_snapshots: self.orderbook_snapshots[s] = {"best_bid": 0.0, "best_ask": 0.0}

    async def _safe_telegram_dispatch(self, message: str, is_html: bool = True, message_type: str = "SUCCESS"):
        if not os.getenv("TELEGRAM_BOT_TOKEN") or len(os.getenv("TELEGRAM_BOT_TOKEN", "")) < 5:
            return
            
        if hasattr(self, '_telegram_blocked_until') and time.time() < self._telegram_blocked_until:
            return
            
        for attempt in range(2):
            try:
                if is_html: await self.telegram.send_html_report(message)
                else: await self.telegram.log_message(message, message_type)
                return
            except Exception as e: 
                logger.debug(f"Telegram dispatch failed attempt {attempt}: {e}", exc_info=True)
                await asyncio.sleep(2 ** attempt)
        self._telegram_blocked_until = time.time() + 3600
        logger.warning("⚠️ Telegram unreachable. Disabling telemetry dispatcher temporarily for 1 hour.")

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
                
                async with self.portfolio_state_lock:
                    self.active_positions_map[symbol] = direction
                risk_matrix = {"allocated_value_usdt": qty * entry_price, "size": qty, "recommended_leverage": 8}
                
                recovery_uuid = str(uuid.uuid4())
                daemon_task = self.track_task(self._position_lifecycle_daemon(symbol, recovery_uuid, direction, entry_price, atr, risk_matrix, 3, "RANGING", is_recovery=True))
                self.daemon_tasks[symbol] = daemon_task
        except Exception as e:
            logger.error(f"Failed synchronizing exchange state: {e}", exc_info=True)

    async def cleanup_stale_locks(self):
        while True:
            await asyncio.sleep(300) 
            try:
                symbols_to_check = []
                async with self.portfolio_state_lock:
                    symbols_to_check = list(self.active_positions_map.keys())
                
                for symbol in symbols_to_check:
                    active_daemon = self.daemon_tasks.get(symbol)
                    if active_daemon and not active_daemon.done(): continue 

                    if not hasattr(self.risk_vault, 'active_positions') or symbol not in self.risk_vault.active_positions:
                        pos_response = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                        if pos_response.get("retCode") == 0:
                            if not any(float(p.get("size", 0.0)) > 0 for p in pos_response.get("result", {}).get("list", [])):
                                async with self.portfolio_state_lock:
                                    if symbol in self.active_positions_map:
                                        self.active_positions_map.pop(symbol, None)
            except Exception as e:
                logger.error(f"Failed stale lock cleanup: {e}", exc_info=True)

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
            except Exception as e:
                logger.debug(f"Crowded-Trade Oracle fault: {e}", exc_info=True)
            await asyncio.sleep(120) 

    async def handle_incoming_trade(self, trade_data: Dict[str, Any]):
        symbol = trade_data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return
        
        now = time.time()
        async with self.circuit_breaker_lock:
            if self.circuit_breakers.get(symbol, 0.0) > now or self.circuit_breakers.get("GLOBAL_MAINTENANCE", 0.0) > now: return
            
        if not self.fsm.can_execute_trades: return
        
        if time.time() - self.last_socket_reconnect < 30.0:
            return

        try:
            price = float(trade_data.get("price", 0.0))
            if price < 0.000001: return
            
            volume = float(trade_data.get("size", 0.0))
            is_buy = (str(trade_data.get("side", "")).upper() == "BUY")
            exchange_timestamp = float(trade_data.get("timestamp", now * 1000)) / 1000.0
            
            self.tensor_oracle.ingest_tick(symbol, price, exchange_timestamp) 
            
            edge_gate = self.edge_gates.get(symbol)
            if edge_gate: 
                edge_gate.update_trade_flow(volume, is_buy)

            feature_engine = self.feature_engines.get(symbol)
            if feature_engine and hasattr(feature_engine, 'push_trade_tick'):
                feature_engine.push_trade_tick([trade_data])

            stat_engine = self.stat_engines.get(symbol)
            clock = self.vpin_clocks.get(symbol)
            if not stat_engine or not clock: return
            
            stat_engine.update_trades(price, volume, is_buy, exchange_timestamp)
            self.risk_vault.push_microstructure_variance(stat_engine.inst_variance)
            
            manifests = clock.process_tick(price, volume, not is_buy)
            valid_manifests = [m for m in manifests if m.get("valid")]
            
            if valid_manifests: vpin_z = float(valid_manifests[-1].get("vpin_z_score", 0.0))
            elif clock.vpin_history:
                hist = np.array(list(clock.vpin_history)[-200:])
                vpin_z = float((clock.vpin_history[-1] - np.mean(hist)) / (np.std(hist) + 1e-9)) if len(hist) >= 50 and np.std(hist) > 0 else 0.0
            else: vpin_z = 0.0
        
            throttle_time = 0.2 if abs(vpin_z) > 1.5 else 1.0
            if now - self.last_eval_time.get(symbol, 0.0) < throttle_time: return
            self.last_eval_time[symbol] = now
            
            ob = self.orderbook_snapshots.get(symbol)
            if not ob or "bid_size" not in ob: return
            spread_cost = abs(ob["best_ask"] - ob["best_bid"]) / (price + 1e-9) if price > 0 else 0.001
            vol_mult = self.screener_metrics.get(symbol, {}).get("vol_mult", 1.0)
            
            async with self.eval_semaphores[symbol]:
                raw_atr = feature_engine.get_computed_atr() if feature_engine and hasattr(feature_engine, 'get_computed_atr') else 0.0
                atr = raw_atr if raw_atr > 0 else price * 0.005
                
                sl_atr_mult = self.live_params.get("sl_atr_mult", 1.5)
                dynamic_rr_ratio = feature_engine.get_dynamic_rr_ratio() if feature_engine and hasattr(feature_engine, 'get_dynamic_rr_ratio') else self.live_params.get("rr_ratio", 2.0)
                
                sl_dist_pct = max((atr * sl_atr_mult) / (price + 1e-9), 0.008)
                tp_dist_pct = sl_dist_pct * dynamic_rr_ratio
                
                tensor_alpha = self.tensor_oracle.compute_lead_lag_signal(symbol)
                
                sgd_state = stat_engine.extract_statistical_state(
                    price, vpin_z, tensor_alpha, sl_dist_pct, tp_dist_pct, exchange_timestamp
                )
                
                p_up, p_down = sgd_state["p_up"], sgd_state["p_down"]
                action = sgd_state["action_dir"]
                virtual_sl = sgd_state["virtual_sl"]
                virtual_tp = sgd_state["virtual_tp"]
                
                structural_verdict = edge_gate.evaluate_structural_edge(symbol, vpin_z, intended_direction=action)
                if structural_verdict["action"] == "HOLD": return
                
                if structural_verdict["action"] != action:
                    action = structural_verdict["action"]
                    prob_success = max(max(p_up, p_down), 0.62) 
                else:
                    prob_success = max(p_up, p_down)
                
                regime = feature_engine.detect_market_regime() if feature_engine else "TRENDING"
                
                if spread_cost > 0.0005: 
                    structural_verdict["routing"] = "MAKER_ONLY"
                    regime = "MEAN_REVERTING"
                    
                fee_pct = 0.0002 if structural_verdict.get("routing") == "MAKER_ONLY" else 0.0006
                net_ev_pct = (prob_success * tp_dist_pct) - ((1.0 - prob_success) * sl_dist_pct) - (spread_cost if structural_verdict.get("routing") != "MAKER_ONLY" else -spread_cost * 0.2) - fee_pct

                if net_ev_pct <= 0.0001: 
                    return
                    
                dynamic_gate = sgd_state.get("dynamic_gate", 0.52)
                if structural_verdict.get("routing") == "MAKER_ONLY":
                    dynamic_gate -= 0.08  

                async with self.portfolio_state_lock:
                    dna_stats = self.ram_dna_cache.get(symbol, {"is_armed": True, "win_rate": 0.50})
                    
                dna_win_rate = dna_stats.get("cluster_win_rate", dna_stats.get("win_rate", 0.50))
                min_threshold = max(dynamic_gate, dna_win_rate)
                
                if prob_success < min_threshold: return

                funding_rate = self.funding_rates.get(symbol, 0.0)
                if funding_rate > 0.00025: 
                    if action == "BUY": prob_success -= 0.06
                    elif action == "SELL": prob_success += 0.06
                elif funding_rate < -0.00025:
                    if action == "SELL": prob_success -= 0.06
                    elif action == "BUY": prob_success += 0.06
                    
                if feature_engine and hasattr(feature_engine, 'get_htf_trend_bias'):
                    htf_bias = feature_engine.get_htf_trend_bias(price)
                    if action == "BUY": prob_success += (htf_bias * 0.05)
                    elif action == "SELL": prob_success -= (htf_bias * 0.05)
                    
                prob_success = stat_engine.calibrate_confidence(prob_success, regime, stat_engine.ewma_mse)

                # 🚀 V41.17 FIX: NameError execution scope guard
                try:
                    vol_z = stat_engine.hawkes_z if stat_engine else 0.0
                    
                    payload_features = {
                        "symbol": symbol, "market_regime": regime,
                        "virtual_sl": virtual_sl, "virtual_tp": virtual_tp,
                        "adaptive_obi_z": stat_engine.ofi_fast_z, 
                        "liquidity_density_ratio": vol_mult, "bid_ask_spread": spread_cost,
                        "reasoning": structural_verdict.get("reasoning", "MICROSTRUCTURE_ALPHA"),
                        "ai_verdict": "DIRECT_MICROSTRUCTURE_ALPHA"
                    }
                    
                    elasticity = self.elasticity_engines.get(symbol)
                    
                    payload = {
                        "symbol": symbol, "action": action, "price": price, 
                        "prob_success": prob_success, "dna_stats": dna_stats, 
                        "atr": atr, "regime": regime, "net_edge_bps": net_ev_pct * 10000.0, 
                        "vol_z": vol_z, "vol_mult": vol_mult, "timestamp": time.time(),
                        "payload_features": payload_features,
                        "elasticity": elasticity,
                        "dynamic_rr": dynamic_rr_ratio 
                    }
                    
                    async with self.auction_lock:
                        net_sharpe_proxy = net_ev_pct / (sl_dist_pct + 1e-9)
                        heapq.heappush(self.auction_queue, (-net_sharpe_proxy, time.time(), symbol, payload))
                except Exception as ex_payload:
                    logger.error(f"Failed to build auction payload for {symbol}: {ex_payload}", exc_info=True)
                
        except Exception as e:
            logger.error(f"Trade processing fault for {symbol}: {e}", exc_info=True)

    async def log_to_wal_async(self, action_type: str, args: list):
        async with self.wal_lock:
            if len(self.wal_batch_queue) > 10000:
                self.wal_batch_queue.pop(0)
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
                logger.error(f"Failed SQLite WAL write: {e}", exc_info=True)
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
                    except asyncio.TimeoutError: 
                        logger.debug("WAL Supabase flush timeout, will retry.")
                        break 
                    except Exception as e: 
                        logger.error(f"WAL processing error for item {item_id}: {e}", exc_info=True)
            except Exception as e: 
                logger.error(f"WAL Worker Loop Error: {e}", exc_info=True)
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
                            if res and isinstance(res, dict):
                                res["is_armed"] = True
                            return res
                    except Exception as e:
                        logger.debug(f"DNA Fetch failed for {sym}: {e}", exc_info=True)
                        return {"is_armed": True, "win_rate": 0.50} 

                fetch_tasks = {}
                for symbol in list(self.asset_basket):
                    metrics = self.screener_metrics.get(symbol, {})
                    stat_engine = self.stat_engines.get(symbol)
                    z_obi = stat_engine.ofi_fast_z if stat_engine else 0.0
                    ob = self.orderbook_snapshots.get(symbol, {})
                    spread_pct = 0.001
                    if ob.get("best_bid", 0) > 0 and ob.get("best_ask", 0) > 0:
                        spread_pct = (ob["best_ask"] - ob["best_bid"]) / ob["best_bid"]

                    current_dna = {"vol_mult": metrics.get("vol_mult", 1.0), "z_obi": z_obi, "spread_pct": spread_pct, "symbol": symbol}
                    fetch_tasks[symbol] = _safe_fetch(symbol, current_dna)
                
                if not fetch_tasks: continue
                symbols = list(fetch_tasks.keys())
                results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
                
                async with self.portfolio_state_lock:
                    for sym, result in zip(symbols, results):
                        if isinstance(result, Exception): 
                            self.ram_dna_cache[sym] = {"is_armed": True, "win_rate": 0.50}
                        else: 
                            if isinstance(result, dict):
                                result["is_armed"] = True 
                            self.ram_dna_cache[sym] = result
            except Exception as e: logger.error(f"DNA Prewarmer error: {e}", exc_info=True)

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
                        try:
                            await asyncio.wait_for(asyncio.to_thread(self.memory.resolve_batch_historical_predictions, list(current_prices.keys()), current_prices, 60.0, interval_mins), timeout=15.0)
                        except Exception as e: logger.debug(f"Shadow resolution batch timeout: {e}", exc_info=True)
            except Exception as e: logger.error(f"Shadow resolution daemon error: {e}", exc_info=True)

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
                best_bid = book_metrics["top_bid"]
                best_ask = book_metrics["top_ask"]
                
                top_bid_size = float(bids[0][1]) if bids else (book_metrics.get("bid_depth_10", 10.0) / 10.0)
                top_ask_size = float(asks[0][1]) if asks else (book_metrics.get("ask_depth_10", 10.0) / 10.0)
                
                if best_bid > 0 and best_ask > best_bid:
                    self.orderbook_snapshots[symbol] = {
                        "best_bid": best_bid, "bid_size": top_bid_size, 
                        "best_ask": best_ask, "ask_size": top_ask_size, 
                        "bids": bids, "asks": asks
                    }
                    
                    stat_engine = self.stat_engines.get(symbol)
                    
                    now = time.time()
                    spread_val = (best_ask - best_bid) / (best_bid + 1e-9)
                    spread_hist = self.spread_history.get(symbol)
                    if spread_hist is not None:
                        spread_hist.append(spread_val)
                        
                    async with self.circuit_breaker_lock:
                        if len(spread_hist) >= 30 and self.circuit_breakers.get(symbol, 0.0) <= now:
                            med_spread = np.median(spread_hist)
                            multiplier = 4.0 if symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"] else 5.0
                                
                            if spread_val > med_spread * multiplier and spread_val > max(0.0010, med_spread * 1.5):
                                logger.warning(f"⚠️ LIQUIDITY FRACTURE // {symbol} Spread spiked to {spread_val*10000:.1f} bps. Tripping 60s Circuit Breaker.")
                                self.circuit_breakers[symbol] = now + 60.0
                    
                    if stat_engine: 
                        stat_engine.update_orderbook_pressure(best_bid, top_bid_size, best_ask, top_ask_size)
                        if symbol == "BTCUSDT": self.global_btc_ofi_z = stat_engine.ofi_fast_z
                        
                    elasticity = self.elasticity_engines.get(symbol)
                    if elasticity and stat_engine:
                        exchange_ts = float(depth_data.get("ts", time.time() * 1000)) / 1000.0
                        elasticity.update_depth_state(best_bid, top_bid_size, best_ask, top_ask_size, stat_engine.ofi_fast_z, exchange_ts)

            edge_gate = self.edge_gates.get(symbol)
            if edge_gate and bids and asks:
                try:
                    f_bids, f_asks = feature_engine.get_deep_book_floats()
                    mid_price = (book_metrics["top_bid"] + book_metrics["top_ask"]) / 2.0
                    edge_gate.update_orderbook_state(symbol, f_bids, f_asks, mid_price)
                except Exception as e: logger.debug(f"Edge gate update error for {symbol}: {e}")

    async def run_omni_swarm_director(self):
        logger.info("🌪️ OMNI-SWARM DIRECTOR ONLINE: Monitoring Global Vectors.")
        while True:
            await asyncio.sleep(15) 
            try:
                protected_symbols = set()
                async with self.portfolio_state_lock:
                    protected_symbols = set(self.active_positions_map.keys())
                dead_sym, hot_sym = await self.omni_scanner.scan_and_rank_universe(self.asset_basket, protected_symbols=protected_symbols)
                
                if dead_sym and hot_sym:
                    tick_res = await self.executor.safe_call(self.executor.client.get_tickers, category="linear", symbol=hot_sym)
                    if tick_res.get("retCode") == 0 and tick_res.get("result", {}).get("list"):
                        t_data = tick_res["result"]["list"][0]
                        bid = float(t_data.get("bid1Price", 0.0) or 0.0)
                        ask = float(t_data.get("ask1Price", 0.0) or 0.0)
                        turnover = float(t_data.get("turnover24h", 0.0) or 0.0)
                        
                        if bid > 0 and ask > bid:
                            spread_bps = ((ask - bid) / bid) * 10000.0
                            if turnover < 40_000_000.0 or spread_bps > 8.0:
                                continue 
                        else:
                            continue
                    else:
                        continue
                        
                    async with self.portfolio_state_lock:
                        if dead_sym in self.asset_basket: self.asset_basket.remove(dead_sym)
                        if hot_sym not in self.asset_basket: self.asset_basket.append(hot_sym)
                        
                    self._initialize_symbol_structures([hot_sym])
                    await self._prune_dead_symbols() 
                    if self.stream_feed_instance and hasattr(self.stream_feed_instance, 'hot_swap_socket_stream'):
                        await self.stream_feed_instance.hot_swap_socket_stream(dead_sym, hot_sym)
                    logger.critical(f"🚀 {hot_sym} PASSED DYNAMIC GATE AND INJECTED INTO QUANT MATRIX.")
            except Exception as e: logger.error(f"Omni-Swarm Director iteration failed: {e}", exc_info=True)

    async def handle_incoming_kline_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return
        self._initialize_symbol_structures([symbol]) 
        interval, candle = str(data["interval"]), data["candle_data"]
        c_open, c_high, c_low, c_close, c_vol = map(float, [candle.get("open", 0), candle.get("high", 0), candle.get("low", 0), candle.get("close", 0), candle.get("volume", 0)])

        async with self.symbol_locks[symbol]:
            feature_engine = self.feature_engines.get(symbol)
            if feature_engine:
                feature_engine.update_multi_timeframe_candle(timeframe=interval, open_p=c_open, high_p=c_high, low_p=c_low, close_p=c_close, volume=c_vol)
                if str(interval) == str(self.timeframe) and symbol in self.screener_memory:
                    self.screener_memory[symbol].setdefault("highs", deque(maxlen=150)).append(c_high)
                    self.screener_memory[symbol].setdefault("lows", deque(maxlen=150)).append(c_low)
                    self.screener_memory[symbol].setdefault("prices", deque(maxlen=1440)).append(c_close)
                    self.screener_memory[symbol]["last_update_time"] = time.time()

    async def handle_incoming_basket_screener_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return
        try:
            raw_data = data.get("raw_data", {})
            if "turnover24h" in raw_data:
                turnover = float(raw_data["turnover24h"])
                if symbol not in self.screener_metrics: self.screener_metrics[symbol] = {}
                baseline = self.volatility_baseline.get(symbol, turnover)
                if baseline > 0:
                    vol_mult = min(10.0, max(0.1, turnover / baseline))
                    self.screener_metrics[symbol]["vol_mult"] = vol_mult
                self.volatility_baseline[symbol] = (baseline * 0.99) + (turnover * 0.01)
        except Exception as e: logger.debug(f"Screener update parse failed for {symbol}: {e}")

    async def run_universe_refresher(self):
        try:
            await self._fetch_exchange_tick_sizes()
            tickers_res = await self.executor.safe_call(self.executor.client.get_tickers, category="linear")
            full_market = []
            
            if tickers_res.get("retCode") == 0:
                ticker_list = tickers_res.get("result", {}).get("list", [])
                
                for t in ticker_list:
                    symbol = t.get("symbol", "")
                    if not symbol.endswith("USDT"): 
                        continue
                        
                    turnover = float(t.get("turnover24h", 0.0) or 0.0)
                    bid = float(t.get("bid1Price", 0.0) or 0.0)
                    ask = float(t.get("ask1Price", 0.0) or 0.0)
                    
                    if bid <= 0 or ask <= 0 or ask <= bid:
                        continue
                        
                    if turnover >= 20_000_000.0:
                        full_market.append((turnover, symbol))
                        
                full_market.sort(key=lambda x: x[0], reverse=True)
                full_market = [item[1] for item in full_market]
                
            if len(full_market) < 12: 
                full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOGEUSDT"]
                
        except Exception as e:
            logger.error(f"Universe refresher failed fetching assets: {e}", exc_info=True)
            full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOGEUSDT"]

        if "BTCUSDT" in full_market: full_market.remove("BTCUSDT")
        new_core_basket = ["BTCUSDT"]
        
        async with self.portfolio_state_lock:
            for s in self.active_positions_map.keys():
                if s != "BTCUSDT": new_core_basket.append(s)
                
        for sym in full_market:
            if sym not in new_core_basket and len(new_core_basket) < 12: new_core_basket.append(sym)
                
        async with self.portfolio_state_lock:
            self.asset_basket = new_core_basket
            self.shadow_basket = [s for s in full_market if s not in self.asset_basket][:3]
            
        await self._prune_dead_symbols() 
        
        new_vpin_clocks, new_stat, new_dna_cache, new_last_eval, new_orderbooks, new_feature_engines, new_edge_gates, new_symbol_locks, new_eval_semaphores, new_el_engines, new_spread_history = {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
        for s in self.asset_basket + self.shadow_basket:
            new_vpin_clocks[s] = self.vpin_clocks.get(s, VolumeSynchronizedClock(bucket_volume=self._get_vpin_bucket_size(s)))
            new_stat[s] = self.stat_engines.get(s, ContinuousMicrostructureEngine(symbol=s))
            
            async with self.portfolio_state_lock:
                new_dna_cache[s] = {"is_armed": True, "win_rate": 0.50} 
                
            new_last_eval[s] = self.last_eval_time.get(s, 0.0)
            new_orderbooks[s] = self.orderbook_snapshots.get(s, {"best_bid": 0.0, "best_ask": 0.0})
            new_feature_engines[s] = self.feature_engines.get(s, AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600))
            new_edge_gates[s] = self.edge_gates.get(s, MicrostructureEdgeGate(window_size=100))
            new_el_engines[s] = self.elasticity_engines.get(s, MicroElasticityEngine())
            new_symbol_locks[s] = self.symbol_locks.get(s, asyncio.Lock())
            new_eval_semaphores[s] = self.eval_semaphores.get(s, asyncio.Semaphore(1))
            new_spread_history[s] = self.spread_history.get(s, deque(maxlen=50))
            
        self.vpin_clocks, self.stat_engines, self.last_eval_time, self.orderbook_snapshots, self.feature_engines, self.edge_gates, self.symbol_locks, self.eval_semaphores, self.elasticity_engines, self.spread_history = new_vpin_clocks, new_stat, new_last_eval, new_orderbooks, new_feature_engines, new_edge_gates, new_symbol_locks, new_eval_semaphores, new_el_engines, new_spread_history
        
        async with self.portfolio_state_lock:
            self.ram_dna_cache = new_dna_cache

        try:
            historical_data = {sym: list(self.screener_memory[sym]["prices"]) for sym in self.asset_basket if self.screener_memory.get(sym) and len(self.screener_memory[sym].get("prices", [])) > 30}
            if len(historical_data) >= 2: self.risk_vault.update_correlation_matrix(historical_data)
        except Exception as e: logger.error(f"Correlation matrix update failed: {e}", exc_info=True)

        self.stream_restart_event.set()
        self.force_dna_refresh.set() 

    async def stream_manager_loop(self):
        while True:
            stream_feed = HighVelocityMultiFeed(
                basket=self.asset_basket + self.shadow_basket[:2], 
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
                if not t.cancelled() and not self.stream_restart_event.is_set(): 
                    self.stream_restart_event.set()
                    
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
                
                actual_positions = pos_response.get("result", {}).get("list", [])
                active_symbols = [p["symbol"] for p in actual_positions if float(p.get("size", 0.0)) > 0]
                
                async with self.portfolio_state_lock:
                    symbols_to_check = list(self.active_positions_map.keys())
                    for symbol in symbols_to_check:
                        if symbol not in active_symbols:
                            active_daemon = self.daemon_tasks.get(symbol)
                            if not active_daemon or active_daemon.done():
                                logger.warning(f"🧹 STATE RECONCILIATION: Purging phantom lock for {symbol}")
                                self.active_positions_map.pop(symbol, None)
                                self.risk_vault.update_position_ledger(symbol, 0.0)
            except Exception as e: logger.debug(f"State reconciliation failed: {e}", exc_info=True)

    async def run_global_capital_auction_worker(self):
        logger.info("🏛️ GLOBAL CAPITAL AUCTION ENGINE ONLINE: Processing Priority Matrix.")
        while True:
            await asyncio.sleep(0.5) 
            
            async with self.portfolio_state_lock:
                if len(self.active_positions_map) >= 5:
                    continue

            best_candidate = None
            async with self.auction_lock:
                if not self.auction_queue:
                    continue
                    
                if len(self.auction_queue) > 1000:
                    self.auction_queue = heapq.nsmallest(500, self.auction_queue) 
                    heapq.heapify(self.auction_queue)
                    
                valid_candidates = []
                now = time.time()
                while self.auction_queue:
                    item = heapq.heappop(self.auction_queue)
                    _, _, sym, payload = item
                    if now - payload["timestamp"] < 2.0:
                        valid_candidates.append(item)
                        
                if not valid_candidates: continue
                    
                best_candidate = valid_candidates[0]
                for i in range(1, len(valid_candidates)):
                    heapq.heappush(self.auction_queue, valid_candidates[i])

            if not best_candidate: continue
            
            top_neg_sharpe, _, top_symbol, top_payload = best_candidate
            top_sharpe = -top_neg_sharpe

            async with self.portfolio_state_lock:
                if top_symbol in self.active_positions_map or len(self.active_positions_map) >= 5:
                    continue
                    
                current_ob = self.orderbook_snapshots.get(top_symbol)
                if current_ob and current_ob.get("best_bid", 0) > 0:
                    live_mid = (current_ob["best_bid"] + current_ob["best_ask"]) / 2.0
                    drift_pct = abs(live_mid - top_payload["price"]) / top_payload["price"]
                    if drift_pct > 0.0015: 
                        logger.warning(f"⏳ AUCTION DISCARD // {top_symbol} Signal drifted {drift_pct*10000:.1f} bps in queue. Aborting execution.")
                        continue

                self.active_positions_map[top_symbol] = top_payload["action"]
            
            logger.critical(
                f"🏛️ AUCTION WINNER // {top_symbol} [{top_payload['regime']}] | "
                f"{top_payload['action']} | Net Sharpe: {top_sharpe:.2f} | "
                f"Prob: {top_payload['prob_success']:.2%} | Net Edge: {top_payload['net_edge_bps']:.1f} bps"
            )
            
            self.track_task(self.execute_statistical_signal(
                top_payload["symbol"], top_payload["action"], top_payload["price"], 
                top_payload["prob_success"], top_payload["dna_stats"], top_payload["atr"], 
                top_payload["regime"], top_payload["net_edge_bps"], top_payload["vol_z"], top_payload["vol_mult"],
                top_payload.get("payload_features"), elasticity=top_payload.get("elasticity"),
                dynamic_rr_ratio=top_payload.get("dynamic_rr", self.live_params.get("rr_ratio", 2.0))
            ))

    async def execute_statistical_signal(self, symbol: str, direction: str, current_price: float, confidence: float, dna_stats: dict, atr: float, regime: str, edge_bps: float, vol_z: float, vol_mult: float, payload_features: dict = None, elasticity: Any = None, dynamic_rr_ratio: float = 2.0):
        try:
            if symbol in self.daemon_tasks and not self.daemon_tasks[symbol].done():
                logger.warning(f"⚠️ Lifecycle daemon active for {symbol}. Aborting duplicate.")
                async with self.portfolio_state_lock: 
                    if symbol in self.active_positions_map: self.active_positions_map.pop(symbol, None)
                return

            signal_id = str(uuid.uuid4())
            sl_atr_mult = self.live_params.get("sl_atr_mult", 1.5)
            
            sl_distance = max(atr * sl_atr_mult, current_price * 0.008)
            tp_distance = sl_distance * dynamic_rr_ratio 
            
            tick_dec = Decimal(str(self.tick_sizes.get(symbol, 0.0001)))
            def align_price(p: float) -> str: return str(Decimal(str(p)).quantize(tick_dec, rounding=ROUND_HALF_UP))
            
            if direction == "BUY":
                raw_sl = min(current_price - (tp_distance * 0.1), current_price - sl_distance)
                raw_tp = current_price + tp_distance
            else:
                raw_sl = max(current_price + (tp_distance * 0.1), current_price + sl_distance)
                raw_tp = current_price - tp_distance
                
            initial_sl_price, target_tp_price = float(align_price(raw_sl)), float(align_price(raw_tp))

            try: 
                # 🚀 V41.17 FIX: Bybit V5 Unified Account Parameter Fix
                wallet_info = await self.executor.safe_call(self.executor.client.get_wallet_balance, accountType="UNIFIED", coin="USDT")
                coin_list = wallet_info.get("result", {}).get("list", [])
                available_balance = float(coin_list[0].get("availableBalance", 0.0)) if coin_list else 0.0
            except Exception as e: 
                logger.debug(f"Wallet fetch failed before execution: {e}", exc_info=True)
                available_balance = 0.0

            if available_balance < 5.0: 
                logger.warning(f"⚠️ MARGIN EXHAUSTED // Skipping {symbol}: Available margin ({available_balance:.2f} USDT) too low.")
                async with self.portfolio_state_lock: 
                    if symbol in self.active_positions_map: self.active_positions_map.pop(symbol, None)
                return
                
            if available_balance < 50.0:
                target_position_size = 6.00 / (current_price + 1e-9)
                fractional_risk = 0.05
            else:
                fractional_risk = self.risk_vault.calculate_optimal_fraction(confidence, net_edge_bps=edge_bps)
                dollar_risk = available_balance * fractional_risk
                target_position_size = max(dollar_risk / sl_distance, 6.00 / (current_price + 1e-9))
                
            target_notional = target_position_size * current_price

            if available_balance >= 50.0 and not self.risk_vault.evaluate_portfolio_safety(available_balance, target_notional, symbol): 
                async with self.portfolio_state_lock: 
                    if symbol in self.active_positions_map: self.active_positions_map.pop(symbol, None)
                return

            target_leverage = 5 if available_balance < 50.0 else self.risk_vault.calculate_dynamic_leverage(target_notional, available_balance, sl_distance_pct=(sl_distance / current_price))
            
            if self.test_mode:
                execution_success = random.random() < 0.85
                actual_filled_notional = target_notional
            else:
                try:
                    await self.executor.adjust_leverage(symbol, target_leverage)
                    await asyncio.sleep(0.2) 
                except Exception as e: 
                    logger.debug(f"Leverage adjust note for {symbol}: {e}")

                feature_engine = self.feature_engines.get(symbol)
                current_depth = feature_engine.get_orderbook_snapshot() if feature_engine and hasattr(feature_engine, 'get_orderbook_snapshot') else {"bids": [[current_price, 1]], "asks": [[current_price, 1]]}

                try:
                    if regime in ["TRENDING_BULL", "TRENDING_BEAR", "TRENDING"]:
                        res = await self.sor.execute_iceberg_block(symbol=symbol, direction=direction, total_qty=target_position_size, current_mid_price=current_price, stop_loss=initial_sl_price, take_profit=target_tp_price, depth_snapshot=current_depth, vol_z=vol_z, vol_mult=vol_mult, feature_engine=feature_engine)
                    else:
                        res = await self.sor.execute_mean_reversion_bracket(symbol=symbol, direction=direction, total_qty=target_position_size, current_mid_price=current_price, stop_loss=initial_sl_price, take_profit=target_tp_price, depth_snapshot=current_depth, vol_z=vol_z, vol_mult=vol_mult, feature_engine=feature_engine, elasticity=elasticity)
                    
                    execution_success = res[0] if isinstance(res, tuple) else bool(res)
                except Exception as ex:
                    err_str = str(ex)
                    if any(code in err_str for code in ["10004", "10016", "10002", "500"]):
                        logger.critical(f"🚨 BYBIT SYSTEM MAINTENANCE DETECTED ({err_str}). Tripping 180s System Pause.")
                        async with self.circuit_breaker_lock:
                            self.circuit_breakers["GLOBAL_MAINTENANCE"] = time.time() + 180.0
                    elif "110007" in err_str or "not enough" in err_str or "10001" in err_str:
                        logger.warning(f"⚠️ EXCHANGE REJECTION // Skipping {symbol}: {err_str}")
                    else:
                        logger.error(f"Execution error for {symbol}: {err_str}", exc_info=True)
                    async with self.portfolio_state_lock: 
                        if symbol in self.active_positions_map: self.active_positions_map.pop(symbol, None)
                    return

            if not execution_success: 
                async with self.portfolio_state_lock: 
                    if symbol in self.active_positions_map: self.active_positions_map.pop(symbol, None)
                return 
                
            if not self.test_mode:
                try:
                    pos_response = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                    pos_data = pos_response.get("result", {}).get("list", [])
                    actual_qty_filled = float(pos_data[0].get("size", 0.0)) if pos_data else 0.0
                    actual_filled_notional = actual_qty_filled * current_price
                    if actual_filled_notional <= 0:
                        async with self.portfolio_state_lock: 
                            if symbol in self.active_positions_map: self.active_positions_map.pop(symbol, None)
                        return
                except Exception as e:
                    logger.warning(f"Position verification failed after execute for {symbol}: {e}", exc_info=True)
                    actual_qty_filled = target_position_size
                    actual_filled_notional = target_notional
                    
                safe_features = payload_features if payload_features else {"symbol": symbol, "market_regime": regime, "virtual_sl": initial_sl_price, "virtual_tp": target_tp_price}
                self.log_to_wal_sync("prediction", [signal_id, time.time(), current_price, direction, confidence, safe_features, False])
                
                ticket_msg = self.telegram.format_entry_ticket(
                    symbol, direction, current_price, actual_qty_filled, 
                    edge_bps, fractional_risk, regime, safe_features
                )
                self.track_task(self._safe_telegram_dispatch(ticket_msg, is_html=True))
                
            self.risk_vault.update_position_ledger(symbol, actual_filled_notional)
            self.daemon_tasks[symbol] = self.track_task(self._position_lifecycle_daemon(symbol, signal_id, direction, current_price, atr, {"allocated_value_usdt": actual_filled_notional, "size": actual_qty_filled if not self.test_mode else target_position_size}, target_leverage, regime, realigned_tp=target_tp_price, dynamic_rr_ratio=dynamic_rr_ratio))
            
        except Exception as e:
            logger.error(f"Critical failure in execute_statistical_signal for {symbol}: {e}", exc_info=True)
            async with self.portfolio_state_lock: 
                if symbol in self.active_positions_map: self.active_positions_map.pop(symbol, None)

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
                    logger.debug(f"Heartbeat balance fetch failed: {e}", exc_info=True)
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
                except Exception as e: logger.debug(f"Heartbeat DB forensic fetch failed: {e}", exc_info=True); execution_stats = {} 

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
                
                live_count = 0
                async with self.portfolio_state_lock:
                    live_count = sum(1 for s in self.asset_basket if self.ram_dna_cache.get(s, {}).get("is_armed", True))
                shadow_count = len(self.asset_basket) + len(self.shadow_basket) - live_count

                report = self.telegram.format_mission_control_dashboard(
                    uptime_hours, live_count, shadow_count, cv, actual, dd, dd_bar, execution_stats
                )
                self.track_task(self._safe_telegram_dispatch(report))

    async def _position_lifecycle_daemon(self, symbol: str, signal_id: str, direction: str, current_price: float, atr: float, risk_matrix: dict, target_leverage: int = 8, market_regime: str = "TRENDING", is_recovery: bool = False, realigned_tp: float = None, dynamic_rr_ratio: float = 2.0):
        exec_details = {"leverage": target_leverage, "execution_mode": "RECOVERY" if is_recovery else ("GHOST" if self.test_mode else "LIVE")}
        daemon_start_time = time.time()
        
        if self.test_mode:
            await asyncio.sleep(60)
            self.log_to_wal_sync("settlement", [signal_id, 0.0, 0.0, "PAPER_TIMEOUT", exec_details])
            async with self.portfolio_state_lock: 
                if symbol in self.active_positions_map: self.active_positions_map.pop(symbol, None)
            return

        try:
            order_filled, actual_entry, initial_qty = False, current_price, risk_matrix.get("size", 1.0)
            for _ in range(5):  
                await asyncio.sleep(3)
                try:
                    pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                    pos_data = pos_res.get("result", {}).get("list", [])
                    if pos_data and float(pos_data[0].get("size", 0.0)) > 0:
                        order_filled = True
                        actual_entry = float(pos_data[0].get("avgPrice", current_price))
                        initial_qty = float(pos_data[0].get("size", initial_qty))
                        break
                except Exception as e: logger.debug(f"Position fill check failed for {symbol}: {e}", exc_info=True); continue

            if not order_filled:
                try: await self.executor.safe_call(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
                except Exception as e: logger.debug(f"Cancel all orders failed for {symbol}: {e}", exc_info=True)
                self.risk_vault.update_position_ledger(symbol, -risk_matrix['allocated_value_usdt'])
                async with self.portfolio_state_lock: 
                    if symbol in self.active_positions_map: self.active_positions_map.pop(symbol, None)
                return

            tick_dec = Decimal(str(self.tick_sizes.get(symbol, 0.0001)))
            def align_price(p: float) -> str: return str(Decimal(str(p)).quantize(tick_dec, rounding=ROUND_HALF_UP))

            actual_sl_distance = max(atr * self.live_params.get("sl_atr_mult", 1.5), actual_entry * 0.008)
            
            realigned_sl = actual_entry - actual_sl_distance if direction == "BUY" else actual_entry + actual_sl_distance
            current_tp = realigned_tp if realigned_tp else (actual_entry + (actual_sl_distance * dynamic_rr_ratio) if direction == "BUY" else actual_entry - (actual_sl_distance * dynamic_rr_ratio))
            
            try: await self.executor.safe_call(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, takeProfit=align_price(current_tp), stopLoss=align_price(realigned_sl))
            except Exception as e: logger.debug(f"Initial TP/SL set failed for {symbol}: {e}", exc_info=True)

            stat_engine = self.stat_engines.get(symbol)
            elasticity_engine = self.elasticity_engines.get(symbol)
            max_favorable_price = actual_entry
            current_sl = realigned_sl
            initial_risk = actual_sl_distance
            
            locked_breakeven = False
            scaled_out_50_pct = False
            last_api_update_time = time.time()
            api_check_counter = 0

            while True: 
                await asyncio.sleep(1.0) 
                now = time.time()
                api_check_counter += 1
                
                if api_check_counter >= 15:
                    api_check_counter = 0
                    try:
                        pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                        pos_list = pos_res.get("result", {}).get("list", [])
                        if (not pos_list) or float(pos_list[0].get("size", 0.0)) == 0.0:
                            break 
                        
                        unrealized_pnl = float(pos_list[0].get("unrealisedPnl", 0.0))
                        elapsed_hours = (now - daemon_start_time) / 3600.0
                        if elapsed_hours > 4.0 and unrealized_pnl < 0:
                            logger.info(f"⏳ TIME DECAY EJECTION // Flattening stale position {symbol}.")
                            await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side="Sell" if pos_list[0]["side"] == "Buy" else "Buy", orderType="Market", qty=str(float(pos_list[0]["size"])), timeInForce="IOC", reduceOnly=True)
                            break
                    except Exception as e: logger.debug(f"Daemon API health check failed for {symbol}: {e}", exc_info=True)

                if stat_engine and stat_engine.true_micro_price > 0:
                    c_price = stat_engine.true_micro_price
                    if direction == "BUY" and c_price > max_favorable_price: max_favorable_price = c_price
                    elif direction == "SELL" and c_price < max_favorable_price: max_favorable_price = c_price
                    
                    profit_distance = abs(max_favorable_price - actual_entry)
                    r_multiple = profit_distance / (initial_risk + 1e-9)

                    if r_multiple >= 1.0 and not scaled_out_50_pct and not self.test_mode:
                        try:
                            current_pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                            p_list = current_pos_res.get("result", {}).get("list", [])
                            if p_list and float(p_list[0].get("size", 0.0)) > 0:
                                curr_size = float(p_list[0]["size"])
                                half_qty = curr_size * 0.5
                                limits = self.sor.instrument_cache.get(symbol, {"min_qty": 0.001, "qty_step": 0.001})
                                min_qty = limits["min_qty"]
                                qty_step = limits["qty_step"]
                                aligned_half_qty = math.floor(half_qty / qty_step) * qty_step
                                
                                if aligned_half_qty >= min_qty and (aligned_half_qty * c_price) >= 5.0:
                                    close_side = "Sell" if direction == "BUY" else "Buy"
                                    logger.critical(f"💰 PARTIAL TAKE-PROFIT // {symbol} reached 1R. Scaling out {aligned_half_qty} units.")
                                    await self.executor.safe_call(
                                        self.executor.client.place_order, category="linear", symbol=symbol,
                                        side=close_side, orderType="Market", qty=str(aligned_half_qty), timeInForce="IOC", reduceOnly=True
                                    )
                                    scaled_out_50_pct = True
                                else:
                                    logger.info(f"ℹ️ {symbol} reached 1R, but position is at exchange minimum. Skipping scale-out; trailing full position.")
                                    scaled_out_50_pct = True 
                        except Exception as e:
                            logger.error(f"Scale-out execution fault for {symbol}: {e}", exc_info=True)

                    if r_multiple >= 0.5 and not locked_breakeven:
                        if r_multiple >= 1.0:
                            breakeven_offset = actual_entry * 0.0006  # 🚀 V41.17 FIX: Calibrated 6 bps breakeven offset
                            current_sl = actual_entry + breakeven_offset if direction == "BUY" else actual_entry - breakeven_offset
                            locked_breakeven = True
                            logger.info(f"🛡️ RISK ELIMINATED // {symbol} reached 1R. Stop-Loss ratcheted to Break-Even (+6 bps).")
                        else:
                            half_risk_sl = actual_entry - (initial_risk * 0.5) if direction == "BUY" else actual_entry + (initial_risk * 0.5)
                            if (direction == "BUY" and half_risk_sl > current_sl) or (direction == "SELL" and half_risk_sl < current_sl):
                                current_sl = half_risk_sl

                    vol_scalar = min(1.0, stat_engine.inst_variance * 5000.0)
                    elasticity = elasticity_engine.orderbook_elasticity if elasticity_engine and hasattr(elasticity_engine, 'orderbook_elasticity') else 1.0
                    
                    base_mult = max(0.4, 2.0 - (r_multiple * 0.4))
                    vol_adj = 1.0 + (vol_scalar * 0.5)
                    el_adj = max(0.8, min(1.5, 1.0 / (elasticity + 0.2)))
                    
                    dynamic_trail_dist = initial_risk * base_mult * vol_adj * el_adj

                    ob = self.orderbook_snapshots.get(symbol, {})
                    b_vol, a_vol = float(ob.get("bid_size", 0.0)), float(ob.get("ask_size", 0.0))
                    tox_mult = 1.0
                    if b_vol > 0 and a_vol > 0:
                        imbalance = (b_vol - a_vol) / (b_vol + a_vol)
                        if direction == "BUY" and imbalance < -0.5: tox_mult = max(0.4, 1.0 + imbalance)
                        elif direction == "SELL" and imbalance > 0.5: tox_mult = max(0.4, 1.0 - imbalance)

                    dynamic_trail_dist *= tox_mult
                    calculated_sl = max_favorable_price - dynamic_trail_dist if direction == "BUY" else max_favorable_price + dynamic_trail_dist

                    requires_tp_update = False
                    if r_multiple > 1.2:
                        tp_expansion_factor = min(4.0, r_multiple + (vol_scalar * 2.0))
                        dynamic_tp_dist = initial_risk * tp_expansion_factor
                        
                        calc_tp = actual_entry + dynamic_tp_dist if direction == "BUY" else actual_entry - dynamic_tp_dist
                        
                        if direction == "BUY" and calc_tp > current_tp:
                            if (calc_tp - current_tp) / current_price > 0.002:
                                current_tp = calc_tp
                                requires_tp_update = True
                        elif direction == "SELL" and calc_tp < current_tp:
                            if (current_tp - calc_tp) / current_price > 0.002:
                                current_tp = calc_tp
                                requires_tp_update = True

                    requires_sl_update = False
                    if direction == "BUY" and calculated_sl > current_sl:
                        if (calculated_sl - current_sl) / current_price > 0.0010:
                            current_sl = calculated_sl
                            requires_sl_update = True
                    elif direction == "SELL" and calculated_sl < current_sl:
                        if (current_sl - calculated_sl) / current_price > 0.0010:
                            current_sl = calculated_sl
                            requires_sl_update = True

                    if (requires_sl_update or requires_tp_update) and (now - last_api_update_time > 3.0):
                        try:
                            await self.executor.safe_call(
                                self.executor.client.set_trading_stop, 
                                category="linear", symbol=symbol, positionIdx=0, 
                                takeProfit=align_price(current_tp), stopLoss=align_price(current_sl)
                            )
                            last_api_update_time = now
                            if requires_tp_update:
                                logger.info(f"🌌 TREND EXPANSION // {symbol} momentum accelerating. Take-Profit pushed out to {align_price(current_tp)}.")
                        except Exception as e: logger.debug(f"Failed to amend trailing stop for {symbol}: {e}", exc_info=True)

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
                    slippage_bps = (slip_cost / capital_risked) * 10000 if capital_risked > 0 else 0.0
                    duration_mins = (time.time() - daemon_start_time) / 60.0
                    
                    exec_details["fees_usdt"] = fees
                    
                    if net_pnl < 0:
                        async with self.circuit_breaker_lock:
                            prev_loss = self.tick_error_counts.get(symbol, [])
                            prev_loss = [t for t in prev_loss if time.time() - t < 7200]
                            prev_loss.append(time.time())
                            self.tick_error_counts[symbol] = prev_loss
                            if len(prev_loss) >= 2:
                                base_lockout = 1800  
                                vol_scalar_lockout = min(3.0, stat_engine.inst_variance * 5000.0) if stat_engine else 0.0
                                dynamic_lockout = base_lockout * (1.0 + vol_scalar_lockout * 2.0)
                                self.circuit_breakers[symbol] = time.time() + dynamic_lockout
                                logger.warning(f"⏸️ VOLATILITY LOCKOUT: {symbol} paused for {dynamic_lockout/60:.1f} mins after 2 consecutive losses.")
                else: 
                    net_pnl, real_outcome, slippage_bps, fees, duration_mins = 0.0, "RECONCILED", 0.0, 0.0, 0.0
            except Exception as e: 
                logger.error(f"Failed to fetch closed PnL for {symbol}: {e}", exc_info=True)
                net_pnl, real_outcome, slippage_bps, fees, duration_mins = 0.0, "RECONCILED", 0.0, 0.0, 0.0
            
            self.log_to_wal_sync("settlement", [signal_id, net_pnl, slippage_bps, real_outcome, exec_details])
            receipt_msg = self.telegram.format_execution_receipt(symbol, net_pnl, slippage_bps, fees, duration_mins, net_pnl > 0)
            self.track_task(self._safe_telegram_dispatch(receipt_msg))

        except Exception as e:
            logger.error(f"Position daemon critical fault for {symbol}: {e}", exc_info=True)
            try:
                pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                if float(pos_res.get("result", {}).get("list", [{}])[0].get("size", 0.0)) > 0:
                    await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side="Sell" if pos_res["result"]["list"][0]["side"] == "Buy" else "Buy", orderType="Market", qty=str(float(pos_res["result"]["list"][0]["size"])), timeInForce="IOC", reduceOnly=True)
            except Exception as e2: logger.error(f"Emergency daemon flatten failed for {symbol}: {e2}", exc_info=True)
        finally:
            async with self.portfolio_state_lock: 
                if symbol in self.active_positions_map: self.active_positions_map.pop(symbol, None)
            self.risk_vault.update_position_ledger(symbol, 0.0)

    async def graceful_shutdown(self):
        logger.critical("🛑 INITIATING EMERGENCY FLATTEN & SHUTDOWN...")
        symbols_to_cancel = []
        async with self.portfolio_state_lock:
            symbols_to_cancel = list(self.active_positions_map.keys())
            
        for symbol in symbols_to_cancel:
            try: await self.executor.safe_call(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
            except Exception as e: logger.error(f"Shutdown cancel order failed for {symbol}: {e}", exc_info=True)
        
        for symbol in symbols_to_cancel:
            try:
                pos_res = await self.executor.safe_call(self.executor.client.get_positions, category="linear", symbol=symbol)
                if float(pos_res.get("result", {}).get("list", [{}])[0].get("size", 0.0)) > 0:
                    current_side = pos_res["result"]["list"][0]["side"]
                    close_side = "Sell" if current_side == "Buy" else "Buy"
                    qty = str(float(pos_res["result"]["list"][0]["size"]))
                    await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=symbol, side=close_side, orderType="Market", qty=qty, timeInForce="IOC", reduceOnly=True)
            except Exception as e: logger.error(f"Shutdown flatten order failed for {symbol}: {e}", exc_info=True)
                
        logger.critical("🔍 VERIFYING ZERO EXPOSURE...")
        max_verify_attempts = 10
        verified_flat = False  
        for attempt in range(max_verify_attempts):
            try:
                pos_response = await self.executor.safe_call(self.executor.client.get_positions, category="linear", settleCoin="USDT")
                active_orphans = [p for p in pos_response.get("result", {}).get("list", []) if float(p.get("size", 0.0)) > 0]
                if not active_orphans:
                    logger.critical("✅ EXPOSURE VERIFIED AT ZERO. ALL POSITIONS FLATTENED.")
                    verified_flat = True
                    break
                else:
                    logger.error(f"⚠️ {len(active_orphans)} positions still open. Retrying flatten sequence ({attempt+1}/{max_verify_attempts}).")
                    for p in active_orphans:
                        sym = p["symbol"]
                        close_side = "Sell" if p["side"] == "Buy" else "Buy"
                        qty = str(float(p["size"]))
                        try: await self.executor.safe_call(self.executor.client.place_order, category="linear", symbol=sym, side=close_side, orderType="Market", qty=qty, timeInForce="IOC", reduceOnly=True)
                        except Exception as e: logger.error(f"Retry flatten failed for {sym}: {e}", exc_info=True)
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
                if "not modified" in str(e).lower() or "110025" in str(e):
                    logger.info("✅ Bybit Unified Account already in One-Way Mode.")
                else:
                    logger.debug(f"Position mode verification note: {e}")
        except Exception as e: logger.debug(f"Position mode check block failed: {e}", exc_info=True)

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
            tickers_res = await self.executor.safe_call(self.executor.client.get_tickers, category="linear")
            full_market = []
            
            if tickers_res.get("retCode") == 0:
                ticker_list = tickers_res.get("result", {}).get("list", [])
                for t in ticker_list:
                    symbol = t.get("symbol", "")
                    if not symbol.endswith("USDT"): 
                        continue
                        
                    turnover = float(t.get("turnover24h", 0.0) or 0.0)
                    bid = float(t.get("bid1Price", 0.0) or 0.0)
                    ask = float(t.get("ask1Price", 0.0) or 0.0)
                    
                    if bid <= 0 or ask <= 0 or ask <= bid:
                        continue
                        
                    if turnover >= 20_000_000.0:
                        full_market.append((turnover, symbol))
                        
                full_market.sort(key=lambda x: x[0], reverse=True)
                full_market = [item[1] for item in full_market]
                
            if len(full_market) < 12: 
                full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOGEUSDT"]
                
        except Exception as e:
            logger.error(f"Initial boot universe fetch failed: {e}", exc_info=True)
            full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOGEUSDT"]

        if "BTCUSDT" in full_market: full_market.remove("BTCUSDT")
        
        if boot_basket := full_market[:12]:
            self.asset_basket = ["BTCUSDT"] + boot_basket[:11]
            self._initialize_symbol_structures(self.asset_basket)
        
        daemons = [
            self.run_db_wal_worker, self._batch_wal_flush_loop, self.run_dna_prewarmer, 
            self.stream_manager_loop, self.run_system_heartbeat, self.cleanup_stale_locks, 
            self.run_shadow_resolution_daemon, self._universe_refresher_loop, 
            self.run_global_capital_auction_worker, self.run_omni_swarm_director,            
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