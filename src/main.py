import os
import sys
import time
import math
import asyncio
import logging
import uuid
import weakref
import traceback
import random
import datetime
import numpy as np
from collections import deque
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Any
from dotenv import load_dotenv

# Core & Feature Modules
from core.memory import MemoryBank
from features.adaptive_engine import AdaptiveFeatureEngine
from portfolio.risk_manager import InstitutionalRiskVault
from execution.sor import SmartOrderRouter

# External Service Connectors
from services.ai_router import ResilientAIRouter
from services.data_feed import AsynchronousDataFeed
from ingestion.multi_feed import HighVelocityMultiFeed
from services.bybit_v5 import BybitUnifiedExecutor
from services.telegram_ops import AsyncTelegramReporter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(name)s] - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("QUANT_CORE.DISTRIBUTED_MAIN")


class FastMathEngine:
    def __init__(self):
        self.k_est = 0.0
        self.k_err = 1.0
        self.q = 0.01  
        self.r = 0.1   
        self.trade_timestamps = []
        self.decay_factor = 0.5  

    def kalman_update(self, measurement: float) -> float:
        if self.k_est == 0.0:
            self.k_est = measurement
            return measurement
            
        p_pred = self.k_err + self.q
        kalman_gain = p_pred / (p_pred + self.r)
        self.k_est = self.k_est + kalman_gain * (measurement - self.k_est)
        self.k_err = (1.0 - kalman_gain) * p_pred
        return self.k_est

    def hawkes_cluster_score(self, current_time: float, volume: float) -> float:
        self.trade_timestamps.append((current_time, volume))
        self.trade_timestamps = [t for t in self.trade_timestamps if current_time - t[0] < 60]
        
        excitement = 0.0
        for t_time, t_vol in self.trade_timestamps:
            time_diff = max(0.0, current_time - t_time)
            excitement += t_vol * math.exp(-self.decay_factor * time_diff)
            
        return excitement


class AdaptiveLiquidityEntropySurface:
    def __init__(self, feature_dim: int = 7, lattice_size: int = 5, learning_rate: float = 0.1):
        self.feature_dim = feature_dim
        self.lattice_size = lattice_size
        self.lr = learning_rate
        
        self.lattice = np.random.randn(lattice_size, lattice_size, feature_dim) * 0.1 + 1.0
        
        self.node_priors: Dict[tuple, List[float]] = {
            (i, j): [2.0, 2.0] for i in range(lattice_size) for j in range(lattice_size)
        }
        self.node_activations = np.zeros((lattice_size, lattice_size))
        self.last_winners: Dict[str, tuple] = {}
        
    def _find_best_matching_unit(self, vector: np.ndarray) -> tuple:
        diff = self.lattice - vector.reshape(1, 1, -1)
        distances = np.sum(diff ** 2, axis=2)
        return np.unravel_index(np.argmin(distances), distances.shape)
    
    def _update_lattice(self, winner: tuple, vector: np.ndarray, outcome: float = None):
        i, j = winner
        local_lr = self.lr / (1.0 + 0.01 * self.node_activations[i, j])
        self.lattice[i, j] += local_lr * (vector - self.lattice[i, j])
        self.node_activations[i, j] += 1
        
        if outcome is not None:
            if outcome > 0:
                self.node_priors[winner][0] += 1.0  
            else:
                self.node_priors[winner][1] += 1.0  
    
    def evaluate_fluid_edge(self, symbol: str, features: Dict[str, float], market_regime: str) -> Dict[str, Any]:
        vector = np.array([
            max(0.0, features.get("vol_mult", 1.0)),
            max(0.0, features.get("spread_state", 1.0)),
            max(0.0, features.get("volatility_state", 1.0)),
            max(0.0, features.get("tfis", 0.0)),
            max(0.0, features.get("obr", 1.0)),
            max(0.0, features.get("cmv", 1.0)),
            max(0.0, features.get("temporal_anomaly", 1.0)),
        ])
        
        winner = self._find_best_matching_unit(vector)
        self.last_winners[symbol] = winner
        
        alpha, beta = self.node_priors[winner]
        expected_win_rate = alpha / (alpha + beta)
        
        vol_mult = features.get("vol_mult", 1.0)
        
        if market_regime == "TRENDING":
            midpoint = 0.75
            k_steepness = 4.0
            volume_score = 1.0 / (1.0 + math.exp(-k_steepness * (vol_mult - midpoint)))
        else:
            midpoint = 0.40
            k_steepness = -3.5
            volume_score = 1.0 / (1.0 + math.exp(-k_steepness * (vol_mult - midpoint)))
            
        fluid_gating_coefficient = min(1.0, max(0.1, volume_score * (expected_win_rate / 0.50)))
        
        return {
            "node_id": f"NODE_{winner[0]}_{winner[1]}",
            "bayesian_win_rate": round(expected_win_rate, 4),
            "gating_coefficient": round(fluid_gating_coefficient, 4),
            "action": "PROCEED" if fluid_gating_coefficient >= 0.35 else "STAND_DOWN"
        }
        
    def feedback(self, symbol: str, outcome: float):
        winner = self.last_winners.get(symbol)
        if winner:
            dummy_vector = self.lattice[winner]
            self._update_lattice(winner, dummy_vector, outcome)


class DistributedQuantEngine:
    def __init__(self):
        load_dotenv()
        
        self.test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
        
        if self.test_mode:
            logger.critical("⚠️ SYSTEM INITIALIZED IN TEST MODE (GHOST TRADING SIMULATION ACTIVE) ⚠️")
        else:
            logger.critical("🟢 SYSTEM INITIALIZED IN LIVE PRODUCTION MODE. CAPITAL DEPLOYMENT ARMED.")
        
        self.asset_basket: List[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        self.timeframe = os.getenv("TRADING_TIMEFRAME", "15")
        
        self.shadow_basket: List[str] = []
        self.shadow_cooldown: Dict[str, float] = {}
        
        self.stream_restart_event = asyncio.Event()
        
        self.memory = MemoryBank()
        self.risk_vault = InstitutionalRiskVault(max_drawdown_pct=0.25, max_single_position_risk_pct=0.15)
        
        self.feature_engines: Dict[str, AdaptiveFeatureEngine] = {
            s: AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600) for s in self.asset_basket
        }
        
        self.math_engines: Dict[str, FastMathEngine] = {s: FastMathEngine() for s in self.asset_basket}
        self.ales = AdaptiveLiquidityEntropySurface(feature_dim=7, lattice_size=5)
        
        self.macro_regimes: Dict[str, str] = {s: "HOLD" for s in self.asset_basket}
        self.macro_confidences: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.current_atrs: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.last_execution_timestamps: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        
        self.volatility_baseline: Dict[str, float] = {}
        self.volatility_window = 100
        
        self.screener_memory: Dict[str, Dict[str, Any]] = {
            s: {
                "prices": deque(maxlen=150), 
                "highs": deque(maxlen=150), 
                "lows": deque(maxlen=150), 
                "macro_prices": deque(maxlen=48), 
                "volumes": deque(maxlen=150), 
                "atr_history": deque(maxlen=self.volatility_window),
                "last_update_time": 0.0
            } for s in self.asset_basket
        }
        
        self.screener_metrics: Dict[str, Dict[str, float]] = {
            s: {"vol_mult": 1.0, "vol_z": 0.0, "smoothed_price": 0.0, "hawkes_score": 0.0} for s in self.asset_basket
        }
        
        self.pending_macro_payloads: Dict[str, dict] = {}
        self.active_workers: Dict[str, asyncio.Task] = {}
        self.active_positions_lock = set()
        
        self._daemon_registry = weakref.WeakSet()
        self._log_throttle_cache: Dict[str, float] = {}
        
        self.tick_sizes: Dict[str, float] = {}
        self.global_macro_news_cache: str = "No significant macro shifts detected."
        self.last_news_fetch: float = 0.0

        self.global_state_cache = {"last_updated": 0.0}
        self.node_metrics_cache: Dict[str, Dict[str, Any]] = {}

        nv_keys = [os.getenv("NVIDIA_API_KEY_1"), os.getenv("NVIDIA_API_KEY_2")]
        self.ai_router = ResilientAIRouter(nv_keys=nv_keys, deepseek_key=os.getenv("DEEPSEEK_API_KEY"))
        self.macro_data_feed = AsynchronousDataFeed(finnhub_key=os.getenv("FINNHUB_API_KEY"))
        self.telegram = AsyncTelegramReporter(token=os.getenv("TELEGRAM_BOT_TOKEN"), chat_id=os.getenv("TELEGRAM_CHAT_ID"))
        
        self.executor = BybitUnifiedExecutor(
            api_key=os.getenv("BYBIT_API_KEY"),
            api_secret=os.getenv("BYBIT_API_SECRET"),
            testnet=os.getenv("BYBIT_TESTNET", "false").lower() == "true"
        )
        self.sor = SmartOrderRouter(executor=self.executor, max_slippage_pct=0.005)

    def _throttled_log(self, level: str, message: str, category: str = None, throttle_seconds: int = 30):
        current_time = time.time()
        key = category or hash(message)
        last_logged = self._log_throttle_cache.get(key, 0.0)
        
        if current_time - last_logged > throttle_seconds:
            self._log_throttle_cache[key] = current_time
            if level == "WARNING":
                logger.warning(message)
            elif level == "INFO":
                logger.info(message)
            elif level == "CRITICAL":
                logger.critical(message)

    async def _fetch_exchange_tick_sizes(self):
        try:
            logger.info("📡 Fetching global tick size matrix from Bybit matching engine...")
            info = await asyncio.to_thread(self.executor.client.get_instruments_info, category="linear")
            data = info.get("result", {}).get("list", [])
            for item in data:
                sym = item.get("symbol")
                tick_str = item.get("priceFilter", {}).get("tickSize", "0.0001")
                self.tick_sizes[sym] = float(tick_str)
            logger.info(f"✅ Master Tick Size Matrix loaded for {len(self.tick_sizes)} derivatives.")
        except Exception as e:
            logger.error(f"Failed to fetch global tick sizes: {e}")

    async def synchronize_exchange_state(self):
        try:
            logger.info("📡 SYNCING EXCHANGE STATE: Scanning for orphaned live positions...")
            pos_response = await asyncio.to_thread(self.executor.client.get_positions, category="linear", settleCoin="USDT")
            positions = pos_response.get("result", {}).get("list", [])
            active_orphans = [p for p in positions if float(p.get("size", 0.0)) > 0]
            
            if not active_orphans:
                logger.info("✅ EXCHANGE STATE CLEAN: No orphaned positions detected.")
                return

            logger.critical(f"⚠️ RECOVERY ENGAGED: Found {len(active_orphans)} active trades left open during container blackout.")
            
            for pos in active_orphans:
                symbol = pos["symbol"]
                qty = float(pos["size"])
                entry_price = float(pos["avgPrice"])
                side = pos["side"]
                direction = "BUY" if side.upper() == "BUY" else "SELL"
                current_sl = float(pos.get("stopLoss", 0.0))
                
                if current_sl == 0.0:
                    current_sl = entry_price * 0.95 if direction == "BUY" else entry_price * 1.05

                logger.critical(f"⚕️ RESURRECTING DAEMON FOR {symbol} | Dir: {direction} | Qty: {qty} | Entry: {entry_price}")
                
                self.active_positions_lock.add(symbol)
                
                atr = entry_price * 0.0125
                risk_matrix = {"allocated_value_usdt": qty * entry_price, "size": qty, "recommended_leverage": 8}
                feature_engine = self.feature_engines.get(symbol)
                signal_id = f"RECOVERY-{str(uuid.uuid4())[:8]}" 
                
                target_tp = entry_price * 1.05 if direction == "BUY" else entry_price * 0.95
                
                daemon_task = asyncio.create_task(self._position_lifecycle_daemon(
                    symbol, signal_id, direction, entry_price, current_sl, target_tp, atr, risk_matrix, feature_engine, 8, "RANGING"
                ))
                self._daemon_registry.add(daemon_task)
                daemon_task.add_done_callback(
                    lambda t: logger.error(f"☠️ DAEMON CRASH: {t.exception()}") if t.exception() else None
                )
                
        except Exception as e:
            logger.error(f"❌ Failed to synchronize exchange state on boot: {e}")

    async def cleanup_stale_locks(self):
        while True:
            await asyncio.sleep(300) 
            try:
                for symbol in list(self.active_positions_lock):
                    if not hasattr(self.risk_vault, 'active_positions') or symbol not in self.risk_vault.active_positions:
                        pos_response = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                        positions = pos_response.get("result", {}).get("list", [])
                        is_active = any(float(p.get("size", 0.0)) > 0 for p in positions)
                        
                        if not is_active:
                            self.active_positions_lock.discard(symbol)
                            logger.warning(f"🔄 STALE LOCK CLEANUP: Freeing {symbol} from dead thread lock.")
            except Exception as e:
                logger.error(f"Cleanup thread failed: {e}")

    def calculate_adaptive_regime_parameters(self, market_regime: str, metrics: dict, confidence: float = 0.0) -> dict:
        vol_mult = float(metrics.get("vol_mult", 1.0))
        dynamic_z_threshold = 2.5 - (confidence * 1.0)
        liquidity_buffer = 1.0 / math.sqrt(max(0.10, vol_mult))
        
        optimized = {
            "cooldown_period": 3600.0, 
            "z_score_threshold": dynamic_z_threshold, 
            "position_scaling": 1.0,
            "sl_multiplier": 2.0 + (liquidity_buffer * 0.5), 
            "tp_multiplier": max(1.0, 2.0 - (liquidity_buffer * 0.2)),
            "execution_verdict": True
        }

        if market_regime == "TRENDING":
            optimized["cooldown_period"] = 1800.0  
            optimized["sl_multiplier"] = 1.2 + (liquidity_buffer * 0.3)
            optimized["tp_multiplier"] = 3.0 + (vol_mult * 0.5)      
            if vol_mult >= 3.0:
                optimized["tp_multiplier"] += 1.5 
            
        return optimized

    async def _update_global_news_cache(self):
        current_time = time.time()
        if current_time - self.last_news_fetch > 300: 
            try:
                context = await asyncio.wait_for(
                    self.macro_data_feed.fetch_market_snapshot("BTCUSDT", self.timeframe),
                    timeout=8.0
                )
                if context and "news_context" in context:
                    self.global_macro_news_cache = context["news_context"]
                self.last_news_fetch = current_time
            except Exception as e:
                pass 

    async def run_macro_commander(self):
        logger.info("🧠 MACRO COMMANDER ONLINE. Waiting for workers to gather data...")
        while True:
            await asyncio.sleep(300) 
            await self._update_global_news_cache()
            
            if not self.pending_macro_payloads:
                continue
                
            batch_payload = dict(self.pending_macro_payloads)
            try:
                is_market_active = False
                for sym, data in batch_payload.items():
                    if abs(data.get("volatility_z_score", 0.0)) >= 1.5:
                        is_market_active = True
                        break

                if not is_market_active:
                    logger.info("💤 COMMANDER: Matrix is flat (|Z| < 1.5). API bypassed to save execution costs.")
                    for symbol in batch_payload.keys():
                        self.macro_regimes[symbol] = "HOLD"
                        self.macro_confidences[symbol] = 0.35 
                    continue 

                for sym in batch_payload:
                    batch_payload[sym].pop("macro_news_stream", None)
                    batch_payload[sym].pop("global_macro_news", None)

                final_ai_payload = {
                    "GLOBAL_MACRO_NEWS": self.global_macro_news_cache,
                    "ASSET_MATRIX": batch_payload
                }

                try:
                    logger.info(f"🚨 COMMANDER: Structural Anomaly Detected. Routing Batch ({len(batch_payload)} assets) to Cascade Circuit Breaker...")
                    
                    verdict_matrix = await asyncio.wait_for(
                        self.ai_router.extract_market_verdict(final_ai_payload),
                        timeout=60.0
                    )
                    
                    if isinstance(verdict_matrix, dict):
                        target_dict = verdict_matrix.get("ASSET_MATRIX", verdict_matrix)
                        
                        if isinstance(target_dict, dict):
                            for symbol, data in target_dict.items():
                                if symbol in self.asset_basket and isinstance(data, dict):
                                    self.macro_regimes[symbol] = data.get("direction", "HOLD")
                                    self.macro_confidences[symbol] = data.get("confidence", 0.35)
                                    logger.info(f"🔄 COMMANDER SYNCED // Target: {symbol} | Bias: {self.macro_regimes[symbol]} | Conf: {self.macro_confidences[symbol]:.2f}")
                                    
                except asyncio.TimeoutError:
                    logger.error("⏳ COMMANDER TIMEOUT on Single Batch.")
                except Exception as e:
                    logger.error(f"⚠️ COMMANDER ERROR on Single Batch: {e}")
                    
            except Exception as e:
                logger.error(f"⚠️ COMMANDER FATAL ERROR: Failed to process payload: {e}")

    async def run_macro_regime_loop(self):
        logger.critical("🐺 IMMORTAL WATCHDOG ONLINE. Deploying Swarm Data Gatherers...")
        
        for symbol in self.asset_basket:
            task = asyncio.create_task(self._asset_data_gatherer_lifecycle(symbol))
            self.active_workers[symbol] = task
            await asyncio.sleep(0.5) 
            
        logger.info(f"Successfully deployed {len(self.active_workers)} independent asset workers.")
        
        while True:
            await asyncio.sleep(60)
            for symbol in list(self.asset_basket):
                task = self.active_workers.get(symbol)
                if task is None or task.done():
                    if task and not task.done():
                        task.cancel()
                    logger.critical(f"⚕️ WATCHDOG RESURRECTING {symbol} NODE...")
                    new_task = asyncio.create_task(self._asset_data_gatherer_lifecycle(symbol))
                    self.active_workers[symbol] = new_task

    async def _asset_data_gatherer_lifecycle(self, symbol: str):
        import random
        await asyncio.sleep(random.uniform(0, 15))
        
        while True:
            try:
                history = self.screener_memory.get(symbol, {}).get("prices", [])
                
                if len(history) < 15:
                    await asyncio.sleep(15.0)
                    continue
                
                current_price = history[-1]
                
                feature_engine = self.feature_engines.get(symbol)
                self.current_atrs[symbol] = current_price * 0.0125
                metrics = self.screener_metrics.get(symbol, {"vol_mult": 1.0, "vol_z": 0.0})
                
                # 🚀 V5 REGIME-ISOLATED SYNC
                regime = feature_engine.detect_market_regime() if feature_engine else "RANGING"
                node_stats = self.node_metrics_cache.get(symbol, {}).get(regime, {})
                bayesian_acc = node_stats.get("bayesian_edge", 0.50)
                
                current_tfi = feature_engine.tfi_history[-1] if feature_engine and len(feature_engine.tfi_history) > 0 else 0.0
                current_obi = feature_engine.obi_history[-1] if feature_engine and len(feature_engine.obi_history) > 0 else 0.0

                self.pending_macro_payloads[symbol] = {
                    "price": current_price,
                    "atr_volatility": self.current_atrs[symbol],
                    "volume_multiplier": round(metrics.get("vol_mult", 1.0), 2),
                    "volatility_z_score": round(metrics.get("vol_z", 0.0), 2),
                    "trade_flow_imbalance": round(current_tfi, 4),
                    "obi_z_score": round(current_obi, 4),
                    "node_bayesian_edge": f"{bayesian_acc:.2%}" 
                }
            except Exception as e:
                pass 
                
            await asyncio.sleep(60.0) 

    async def handle_incoming_orderbook_tick(self, depth_data: Dict[str, Any]):
        symbol = depth_data.get("s")
        if symbol not in self.asset_basket:
            return

        bids = depth_data.get("b", [])
        asks = depth_data.get("a", [])
        is_snapshot = depth_data.get("type") == "snapshot"
        features = self.feature_engines[symbol].push_orderbook_tick(bids, asks, is_snapshot=is_snapshot)
        
        if not features.get("valid"):
            return

        z_obi = features["adaptive_obi_z"]
        mid_price = features["mid_price"]
        market_regime = features.get("market_regime", "RANGING")
        real_spread = features.get("bid_ask_spread", mid_price * 0.0005)

        metrics = self.screener_metrics.get(symbol, {"vol_mult": 1.0, "vol_z": 0.0})
        
        live_confidence = self.macro_confidences.get(symbol, 0.35) 
        
        optimization = self.calculate_adaptive_regime_parameters(market_regime, metrics, live_confidence)

        if not optimization["execution_verdict"]:
            return

        current_time = time.time()
        if (current_time - self.last_execution_timestamps.get(symbol, 0)) < optimization["cooldown_period"]:
            return

        effective_z_threshold = optimization["z_score_threshold"]
        vol_mult = metrics.get("vol_mult", 1.0)
        regime = self.macro_regimes.get(symbol, "HOLD")
        
        fe = self.feature_engines[symbol]
        current_obi = fe.obi_history[-1] if len(fe.obi_history) > 0 else 0.0
        prev_obi = fe.obi_history[-2] if len(fe.obi_history) > 1 else current_obi
        current_tfi = fe.tfi_history[-1] if len(fe.tfi_history) > 0 else 0.0

        adaptive_mieg_long = (z_obi <= -effective_z_threshold) and (current_obi > prev_obi) and (current_tfi > 0.15)
        adaptive_mieg_short = (z_obi >= effective_z_threshold) and (current_obi < prev_obi) and (current_tfi < -0.15)

        history = self.screener_memory.get(symbol, {}).get("prices", [])
        
        if len(history) < 100:
            return 
            
        prices_array = np.array(list(history)[-100:])
        median_price = np.median(prices_array)
        mad = np.median(np.abs(prices_array - median_price))
        mad_scaled = mad * 1.4826 + 1e-6 
        
        price_z_score = (mid_price - median_price) / mad_scaled

        raw_atr = fe.get_computed_atr() if hasattr(fe, 'get_computed_atr') else 0.0
        atr = raw_atr if raw_atr > 0 else mid_price * 0.0125
        baseline_atr = max(self.volatility_baseline.get(symbol, atr), mid_price * 0.001)

        spread_pct = real_spread / mid_price
        hawkes_score = metrics.get("hawkes_score", 0.0)
        valid_hawkes = [m.get("hawkes_score", 0.0) for m in self.screener_metrics.values() if "hawkes_score" in m]
        avg_hawkes = np.mean(valid_hawkes) if valid_hawkes else 0.1
        
        ales_features = {
            "vol_mult": vol_mult,
            "spread_state": spread_pct / 0.0005,
            "volatility_state": atr / baseline_atr,
            "tfis": abs(hawkes_score / (avg_hawkes + 1e-6)),
            "obr": 0.85,  
            "cmv": 1.0,
            "temporal_anomaly": 1.0,
        }

        fluid_manifest = self.ales.evaluate_fluid_edge(symbol, ales_features, market_regime)
        
        if fluid_manifest["action"] == "STAND_DOWN":
            self._throttled_log("INFO", f"💀 DME GATING STAND DOWN // {symbol} | Gating Weight: {fluid_manifest['gating_coefficient']:.2f}", f"DME_GATE_{symbol}", 60)
            return

        hawkes_ratio = hawkes_score / (avg_hawkes + 1e-6)
        raw_vol_z = metrics.get("vol_z", 0.0)
        vol_z_abs = abs(raw_vol_z)
        
        kinetic_alpha = vol_mult * hawkes_ratio
        
        total_friction = max(real_spread, mid_price * 0.0001) + (real_spread * (1.0 / math.sqrt(max(0.10, vol_mult))))
        
        dynamic_max_spread = 0.0015 * (1.0 + math.log1p(vol_z_abs))
        
        if spread_pct > dynamic_max_spread:
            return 
        
        if (mid_price * 0.02) < total_friction:
            return
        
        trade_direction = None
        has_pure_edge = False
        is_golden_setup = False

        if market_regime == "TRENDING":
            if vol_mult >= 1.2 and vol_z_abs >= 1.5:
                if z_obi <= -effective_z_threshold and adaptive_mieg_long and price_z_score <= -0.5:
                    has_pure_edge = True
                    trade_direction = "BUY"
                elif z_obi >= effective_z_threshold and adaptive_mieg_short and price_z_score >= 0.5:
                    has_pure_edge = True
                    trade_direction = "SELL"
                    
            price_vector = np.clip(price_z_score / 2.0, -1.0, 1.0) 
            obi_vector = np.clip(z_obi / max(1.0, effective_z_threshold), -1.0, 1.0)
            kinetic_vector = np.clip(math.log1p(vol_mult) * max(0.0, hawkes_ratio - 1.0), 0.0, 1.5)
            
            trend_force = 0.0
            if price_vector > 0 and obi_vector > 0:
                trend_force = ((price_vector + obi_vector) / 2.0) * (1.0 + kinetic_vector)
            elif price_vector < 0 and obi_vector < 0:
                trend_force = ((price_vector + obi_vector) / 2.0) * (1.0 + kinetic_vector)
                
            dynamic_activation_barrier = 1.25 
            if kinetic_alpha >= 3.0:
                dynamic_activation_barrier = 0.85
            elif kinetic_alpha >= 2.0:
                dynamic_activation_barrier = 1.05
            
            if trend_force >= dynamic_activation_barrier and not has_pure_edge:
                has_pure_edge = True
                trade_direction = "BUY"
                logger.critical(f"🚀 [KINETIC HUNTER] {symbol} Breakout struck! (Force: {trend_force:.2f} | Alpha: {kinetic_alpha:.2f})")
            elif trend_force <= -dynamic_activation_barrier and not has_pure_edge:
                has_pure_edge = True
                trade_direction = "SELL"
                logger.critical(f"🚀 [KINETIC HUNTER] {symbol} Breakdown struck! (Force: {trend_force:.2f} | Alpha: {kinetic_alpha:.2f})")

        elif market_regime == "RANGING":
            is_exhausted = vol_mult < 1.0 
            is_extreme_deviation = abs(price_z_score) >= 2.0 
            
            if is_extreme_deviation and is_exhausted:
                if price_z_score <= -2.0 and z_obi <= -effective_z_threshold: 
                    has_pure_edge = True
                    trade_direction = "BUY"
                    logger.critical(f"🕸️ [LIQUIDITY TRAP] {symbol} Exhausted Dip (Z: {price_z_score:.2f}). Reverting to Mean.")
                elif price_z_score >= 2.0 and z_obi >= effective_z_threshold: 
                    has_pure_edge = True
                    trade_direction = "SELL"
                    logger.critical(f"🕸️ [LIQUIDITY TRAP] {symbol} Exhausted Pump (Z: {price_z_score:.2f}). Reverting to Mean.")

        if not has_pure_edge:
            return

        # 🚀 V5 DYNAMIC ARMING
        node_stats = self.node_metrics_cache.get(symbol, {}).get(market_regime, {})
        is_armed = node_stats.get("is_armed", False)
        bayesian_edge = node_stats.get("bayesian_edge", 0.50)

        if not is_armed:
            if self.test_mode:
                is_golden_setup = True  
            else:
                logger.info(f"👻 NODE LOCKED // {symbol} edge valid, but Regime Edge is {bayesian_edge:.2%} (< 55%). Routing to SHADOW.")

        if is_armed and abs(price_z_score) >= 2.5 and vol_mult >= 1.5:
            is_golden_setup = True
            self._throttled_log("INFO", f"✨ GOLDEN SETUP DETECTED // {symbol} mathematical edge overrides AI bias.", f"GOLDEN_{symbol}", 60)

        if regime == "HOLD" and not is_golden_setup:
            return 
            
        if trade_direction == "BUY" and regime == "SELL" and not is_golden_setup:
            return 
            
        if trade_direction == "SELL" and regime == "BUY" and not is_golden_setup:
            return 

        self.last_execution_timestamps[symbol] = current_time
        mode_label = "🔥 LIVE" if (is_armed and not self.test_mode) else "👻 GHOST"
        logger.critical(f"{mode_label} PURE EDGE DETECTED // Node: {symbol} | Regime: {market_regime} | Z: {price_z_score:.2f} | Gating: {fluid_manifest['gating_coefficient']:.2f}")
        
        lifecycle_task = asyncio.create_task(self.run_signal_lifecycle(
            symbol, trade_direction, mid_price, optimization, real_spread, vol_z_abs, fluid_manifest["gating_coefficient"], is_golden_setup
        ))
        self._daemon_registry.add(lifecycle_task)
        lifecycle_task.add_done_callback(
            lambda t: logger.error(f"Lifecycle crash: {t.exception()}") if t.exception() else None
        )

    async def handle_incoming_basket_screener_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if not symbol or symbol not in self.asset_basket:
            return
            
        if symbol not in self.screener_memory:
            self.screener_memory[symbol] = {
                "prices": deque(maxlen=150), "highs": deque(maxlen=150), "lows": deque(maxlen=150), 
                "macro_prices": deque(maxlen=48), "volumes": deque(maxlen=150), 
                "atr_history": deque(maxlen=self.volatility_window), "last_update_time": 0.0
            }
            
        self.screener_memory[symbol]["last_update_time"] = time.time()

    async def handle_incoming_kline_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if symbol not in self.asset_basket:
            return
            
        interval = data["interval"]
        candle = data["candle_data"]
        
        c_open = float(candle.get("open", 0))
        c_high = float(candle.get("high", 0))
        c_low = float(candle.get("low", 0))
        c_close = float(candle.get("close", 0))
        c_vol = float(candle.get("volume", 0))

        if not candle.get("confirm", False):
            return

        self.feature_engines[symbol].update_multi_timeframe_candle(
            timeframe=interval, open_p=c_open, high_p=c_high, low_p=c_low, close_p=c_close, volume=c_vol
        )
        
        if symbol not in self.screener_memory:
            self.screener_memory[symbol] = {
                "prices": deque(maxlen=150), "highs": deque(maxlen=150), "lows": deque(maxlen=150), 
                "macro_prices": deque(maxlen=48), "volumes": deque(maxlen=150), 
                "atr_history": deque(maxlen=self.volatility_window), "last_update_time": 0.0
            }
            
        history = self.screener_memory[symbol]

        if str(interval) == "15":
             if "macro_prices" not in history:
                 history["macro_prices"] = deque(maxlen=48)
             history["macro_prices"].append(c_close)
                 
        if str(interval) == "1":
            if "volumes" not in history: history["volumes"] = deque(maxlen=150)
            if "prices" not in history: history["prices"] = deque(maxlen=150)
            if "highs" not in history: history["highs"] = deque(maxlen=150)
            if "lows" not in history: history["lows"] = deque(maxlen=150)
            
            history["volumes"].append(c_vol)
            history["prices"].append(c_close)
            history["highs"].append(c_high)
            history["lows"].append(c_low)
            
            current_raw_atr = self.feature_engines[symbol].get_computed_atr() if hasattr(self.feature_engines[symbol], 'get_computed_atr') else (c_high - c_low)
            if current_raw_atr > 0:
                if "atr_history" not in history:
                    history["atr_history"] = deque(maxlen=self.volatility_window)
                history["atr_history"].append(current_raw_atr)
                    
                if len(history["atr_history"]) >= 20:
                    self.volatility_baseline[symbol] = np.mean(list(history["atr_history"]))
                
            if len(history["volumes"]) >= 15:
                vol_array = np.array(list(history["volumes"]))
                price_array = np.array(list(history["prices"]))
                
                weights = np.exp(np.linspace(-1., 0., len(vol_array[:-1])))
                weights /= weights.sum()
                ewm_vol = np.sum(vol_array[:-1] * weights)
                ewm_vol = max(ewm_vol, 1.0) 
                vol_mult = c_vol / ewm_vol
                
                returns = np.diff(np.log(price_array))
                if len(returns) > 0:
                    ret_weights = np.exp(np.linspace(-1., 0., len(returns)))
                    ret_weights /= ret_weights.sum()
                    ewm_mean_ret = np.sum(returns * ret_weights)
                    
                    variance = np.sum(ret_weights * (returns - ewm_mean_ret)**2)
                    ewm_std_ret = np.sqrt(variance) + 1e-6
                    vel_z = (returns[-1] - ewm_mean_ret) / ewm_std_ret
                else:
                    vel_z = 0.0
                    
                macro_hist = list(history.get("macro_prices", []))
                if len(macro_hist) >= 10:
                    macro_mean = np.mean(macro_hist)
                    macro_std = np.std(macro_hist) + 1e-6
                    macro_z = (c_close - macro_mean) / macro_std
                else:
                    macro_z = (c_close - np.mean(price_array)) / (np.std(price_array) + 1e-6)
                    
                vol_z = (vel_z * 0.4) + (macro_z * 0.6) 
                
                current_time = time.time()
                math_engine = self.math_engines.setdefault(symbol, FastMathEngine())
                smoothed_price = math_engine.kalman_update(c_close)
                hawkes_score = math_engine.hawkes_cluster_score(current_time, c_vol)
                
                self.screener_metrics[symbol] = {
                    "vol_mult": float(vol_mult),
                    "vol_z": float(vol_z),
                    "smoothed_price": smoothed_price,
                    "hawkes_score": hawkes_score
                }

    async def run_universe_refresher(self):
        while True:
            await asyncio.sleep(14400) 
            logger.info("🌍 FAST SATELLITE ROTATION INITIATED. Querying Bybit...")
            
            try:
                await self._fetch_exchange_tick_sizes()
                full_market = await self.executor.get_top_volatile_assets(limit=100, min_turnover=10_000_000)
                
                if len(full_market) < 25:
                    full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT", "NEARUSDT", "APTUSDT", "INJUSDT", "OPUSDT", "FILUSDT", "ARBUSDT", "STXUSDT", "RNDRUSDT", "MNTUSDT", "MKRUSDT", "SEIUSDT", "SUIUSDT", "ORDIUSDT"]
            except Exception as e:
                logger.error(f"Failed to fetch market data via REST: {e}")
                full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT", "NEARUSDT", "APTUSDT", "INJUSDT", "OPUSDT", "FILUSDT", "ARBUSDT", "STXUSDT", "RNDRUSDT", "MNTUSDT", "MKRUSDT", "SEIUSDT", "SUIUSDT", "ORDIUSDT"]
                
            if "BTCUSDT" in full_market:
                full_market.remove("BTCUSDT")
                
            new_core_basket = ["BTCUSDT"]
            for locked_sym in self.active_positions_lock:
                if locked_sym not in new_core_basket:
                    new_core_basket.append(locked_sym)

            for sym in full_market:
                if sym not in new_core_basket and len(new_core_basket) < 25:
                    new_core_basket.append(sym)
                    
            self.asset_basket = new_core_basket
            self.shadow_basket = [s for s in full_market if s not in self.asset_basket]
            
            if len(self.shadow_basket) < 10:
                fallback_shadow = ["XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT"]
                self.shadow_basket.extend([s for s in fallback_shadow if s not in self.shadow_basket])
            
            new_feature_engines = {}
            new_math_engines = {}
            new_screener_memory = {}
            new_macro_regimes = {}
            new_macro_confidences = {}
            new_current_atrs = {}
            new_last_execs = {}
            new_screener_metrics = {}
            new_volatility_baseline = {}
            
            for s in self.asset_basket:
                new_feature_engines[s] = self.feature_engines.get(s, AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600))
                new_math_engines[s] = self.math_engines.get(s, FastMathEngine())
                
                cached_history = self.screener_memory.get(s)
                if not cached_history or len(cached_history.get("prices", [])) < 100:
                    try:
                        klines = await asyncio.to_thread(
                            self.executor.client.get_kline,
                            category="linear", symbol=s, interval="1", limit=150
                        )
                        data = klines.get("result", {}).get("list", [])
                        if data:
                            closes = [float(k[4]) for k in data][::-1]
                            highs = [float(k[2]) for k in data][::-1]
                            lows = [float(k[3]) for k in data][::-1]
                            volumes = [float(k[5]) for k in data][::-1]
                            new_screener_memory[s] = {
                                "prices": deque(closes, maxlen=150), 
                                "highs": deque(highs, maxlen=150), 
                                "lows": deque(lows, maxlen=150), 
                                "macro_prices": deque(closes, maxlen=48), 
                                "volumes": deque(volumes, maxlen=150), 
                                "atr_history": deque(maxlen=self.volatility_window),
                                "last_update_time": time.time()
                            }
                        else:
                            new_screener_memory[s] = {
                                "prices": deque(maxlen=150), "highs": deque(maxlen=150), "lows": deque(maxlen=150), 
                                "macro_prices": deque(maxlen=48), "volumes": deque(maxlen=150), 
                                "atr_history": deque(maxlen=self.volatility_window), "last_update_time": 0.0
                            }
                        await asyncio.sleep(0.5) 
                    except Exception as kline_err:
                        logger.error(f"Error fetching klines during refresh for {s}: {kline_err}")
                        new_screener_memory[s] = {
                            "prices": deque(maxlen=150), "highs": deque(maxlen=150), "lows": deque(maxlen=150), 
                            "macro_prices": deque(maxlen=48), "volumes": deque(maxlen=150), 
                            "atr_history": deque(maxlen=self.volatility_window), "last_update_time": 0.0
                        }
                else:
                    new_screener_memory[s] = cached_history

                new_macro_regimes[s] = self.macro_regimes.get(s, "HOLD")
                new_macro_confidences[s] = self.macro_confidences.get(s, 0.35)
                new_current_atrs[s] = self.current_atrs.get(s, 0.0)
                new_last_execs[s] = self.last_execution_timestamps.get(s, 0.0)
                new_screener_metrics[s] = self.screener_metrics.get(s, {"vol_mult": 1.0, "vol_z": 0.0, "smoothed_price": 0.0, "hawkes_score": 0.0})
                new_volatility_baseline[s] = self.volatility_baseline.get(s, 0.0)
                
            self.feature_engines = new_feature_engines
            self.math_engines = new_math_engines
            self.screener_memory = new_screener_memory
            self.macro_regimes = new_macro_regimes
            self.macro_confidences = new_macro_confidences
            self.current_atrs = new_current_atrs
            self.last_execution_timestamps = new_last_execs
            self.screener_metrics = new_screener_metrics
            self.volatility_baseline = new_volatility_baseline
            
            logger.info(f"🌌 QUANT UNIVERSE MATRIX RE-CALIBRATED.")
            
            for old_symbol in list(self.active_workers.keys()):
                if old_symbol not in self.asset_basket:
                    task = self.active_workers[old_symbol]
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=2.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass
                    del self.active_workers[old_symbol]
                    self.pending_macro_payloads.pop(old_symbol, None)
                    self.ales.last_winners.pop(old_symbol, None)
                    
            for new_symbol in self.asset_basket:
                if new_symbol not in self.active_workers:
                    self.active_workers[new_symbol] = asyncio.create_task(self._asset_data_gatherer_lifecycle(new_symbol))
            
            self.stream_restart_event.set()

    async def run_shadow_swarm_scanner(self):
        logger.info("🦇 SHADOW SWARM: Activated. Simulating background validations to feed the FSM...")
        while True:
            await asyncio.sleep(300) 
            if not self.shadow_basket:
                continue
                
            try:
                tickers = await asyncio.to_thread(self.executor.client.get_tickers, category="linear")
                t_list = tickers.get("result", {}).get("list", [])
                price_map = {t["symbol"]: float(t["lastPrice"]) for t in t_list if t["symbol"] in self.shadow_basket}
                
                # We can safely shadow test heavily because V5 completely purges SHADOW_SIM from live grading
                for sym in self.shadow_basket:
                    if sym not in price_map: 
                        continue
                        
                    if random.random() < 0.15: 
                        price = price_map[sym]
                        direction = random.choice(["BUY", "SELL"])
                        features = {
                            "symbol": sym, "virtual_sl": price * 0.98, "virtual_tp": price * 1.04, 
                            "market_regime": "SHADOW_SIM", "adaptive_obi_z": 0.0, "liquidity_density_ratio": 1.0
                        }
                        await asyncio.to_thread(self.memory.commit_prediction, str(uuid.uuid4()), time.time(), price, direction, 0.50, features, is_shadow=True)
            except Exception as e:
                logger.error(f"Shadow swarm encountered an API error: {e}")

    async def stream_manager_loop(self):
        while True:
            stream_feed = HighVelocityMultiFeed(
                basket=self.asset_basket,
                intervals=["1", "5", "15"],
                orderbook_callback=self.handle_incoming_orderbook_tick,
                screener_callback=self.handle_incoming_basket_screener_update,
                kline_callback=self.handle_incoming_kline_update,
                engine_reference=self  
            )
            
            stream_task = asyncio.create_task(stream_feed.initialize_multiplexed_stream())
            await self.stream_restart_event.wait()
            stream_task.cancel()
            self.stream_restart_event.clear()
            logger.info("♻️ Structural data multiplexers systematically torn down to process hot-universe mutation.")
            await asyncio.sleep(2)

    async def run_system_heartbeat(self):
        start_time = time.time()
        loop_counter = 0
        
        while True:
            await asyncio.sleep(60) 
            loop_counter += 1
            uptime_hours = (time.time() - start_time) / 3600
            
            if loop_counter % 5 == 0:
                try:
                    logger.info("⚡ Executing high-frequency database resolution sweep...")
                    age_cutoff_time = time.time() - 1800 
                    valid_assets = [sym for sym in self.asset_basket if self.screener_memory.get(sym, {}).get("prices")]
                    
                    if valid_assets:
                        current_prices = {sym: self.screener_memory[sym] for sym in valid_assets}
                        for symbol_key, _timestamp in self.shadow_cooldown.items():
                            if symbol_key not in current_prices:
                                current_prices[symbol_key] = self.screener_memory.get(symbol_key, {
                                    "prices": deque(maxlen=150), "highs": deque(maxlen=150), "lows": deque(maxlen=150)
                                })
                        
                        await asyncio.to_thread(
                            self.memory.resolve_batch_historical_predictions,
                            assets=list(current_prices.keys()),
                            current_prices=current_prices,
                            age_cutoff=age_cutoff_time
                        )
                except Exception as e:
                    logger.error(f"❌ Failed to execute batched prediction validation: {str(e)}")

            logger.info(f"💓 SWARM HEARTBEAT: Matrix is active. Uptime: {uptime_hours:.2f} hours.")

            if loop_counter % 5 == 0:
                # 🚀 V5 UPGRADE: Fetch Per-Node Regime-Isolated Metrics Dict
                self.node_metrics_cache = await asyncio.to_thread(
                    self.memory.compute_decentralized_node_accuracy,
                    window_size=150, core_basket=self.asset_basket
                )
                self.global_state_cache["last_updated"] = time.time()
                
                # 🚀 INSTANT LIQUIDITY SYNC
                current_vault_balance = await self.executor.get_wallet_balance_usdt()

                try:
                    price_histories = {s: list(m.get("prices", [])) for s, m in self.screener_memory.items()}
                    self.risk_vault.update_correlation_matrix(price_histories)
                except Exception as corr_err:
                    logger.debug(f"Correlation matrix refresh skipped: {corr_err}")
                
                if "wallet_baseline" not in self.global_state_cache:
                    self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01)
                
                if "start_of_day_balance" not in self.global_state_cache:
                    self.global_state_cache["start_of_day_balance"] = current_vault_balance
                    
                # 🛡️ DELTA-NEUTRAL DEPOSIT DETECTOR
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                today_start_iso = now_utc.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                
                def _fetch_daily_rows():
                    return self.memory.supabase.table("quantitative_ledger")\
                        .select("market_regime, net_pnl, symbol, predicted_direction, actual_outcome")\
                        .eq("resolved", True)\
                        .eq("is_shadow", False)\
                        .gte("timestamp", today_start_iso)\
                        .execute()

                response = await asyncio.to_thread(_fetch_daily_rows)
                data = response.data if response else []
                
                db_daily_pnl = sum(float(row.get("net_pnl", 0.0)) for row in data)
                expected_balance = self.global_state_cache["start_of_day_balance"] + db_daily_pnl
                discrepancy = current_vault_balance - expected_balance
                
                if discrepancy > 5.0:
                    logger.critical(f"💰 CAPITAL ADDITION DETECTED (+${discrepancy:.2f}). Absorbing fresh liquidity.")
                    self.global_state_cache["start_of_day_balance"] += discrepancy
                    self.global_state_cache["wallet_baseline"] += discrepancy
                elif discrepancy < -5.0:
                    logger.critical(f"💸 MANUAL WITHDRAWAL DETECTED (-${abs(discrepancy):.2f}). Auto-Calibrating Risk Vault.")
                    self.global_state_cache["start_of_day_balance"] += discrepancy
                    self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01)
                    if hasattr(self.risk_vault, 'peak_balance'):
                        self.risk_vault.peak_balance = self.global_state_cache["wallet_baseline"]

                # Actual net PnL is now completely bulletproof against deposits
                actual_net_pnl = current_vault_balance - self.global_state_cache["start_of_day_balance"]
                baseline = self.global_state_cache["wallet_baseline"]
                
                if current_vault_balance > baseline:
                    self.global_state_cache["wallet_baseline"] = current_vault_balance
                    baseline = current_vault_balance
                    
                drawdown_pct = max(0.0, (baseline - current_vault_balance) / baseline)
                bar_length = 10
                filled_blocks = min(bar_length, int(drawdown_pct * bar_length))
                
                self.global_state_cache["drawdown_bar"] = "🟢" * (bar_length - filled_blocks) + "🔴" * filled_blocks
                self.global_state_cache["actual_net_pnl"] = actual_net_pnl
                self.global_state_cache["current_vault_balance"] = current_vault_balance
                self.global_state_cache["drawdown_pct"] = drawdown_pct
                self.global_state_cache["daily_data"] = data

            # 🚀 HIGH-FREQUENCY TELEMETRY (Every 10 mins instead of 30)
            if loop_counter % 10 == 0:
                current_vault_balance = self.global_state_cache.get("current_vault_balance", 0.0)
                actual_net_pnl = self.global_state_cache.get("actual_net_pnl", 0.0)
                drawdown_pct = self.global_state_cache.get("drawdown_pct", 0.0)
                drawdown_bar = self.global_state_cache.get("drawdown_bar", "🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢")
                data = self.global_state_cache.get("daily_data", [])

                try:
                    regime_stats = {}
                    for row in data:
                        regime = row.get("market_regime", "UNKNOWN")
                        pnl = float(row.get("net_pnl", 0.0))
                        if regime not in regime_stats:
                            regime_stats[regime] = {"count": 0, "pnl": 0.0}
                        regime_stats[regime]["count"] += 1
                        regime_stats[regime]["pnl"] += pnl
                        
                    regime_breakdown_text = ""
                    for regime, stats in regime_stats.items():
                        icon = "🕸️" if regime == "RANGING" else "🚀"
                        regime_breakdown_text += f"• {icon} <b>{regime}:</b> <code>{stats['count']} trades</code> | <code>{stats['pnl']:+.4f} (Live PnL)</code>\n"
                        
                    if not regime_breakdown_text:
                        regime_breakdown_text = "• <i>No resolved live metrics recorded today yet.</i>\n"
                        
                    recent_trades_text = ""
                    if data:
                        sorted_data = sorted(data, key=lambda x: x.get('timestamp', ''))[-5:]  
                        for t in sorted_data:
                            pnl_val = float(t.get('net_pnl', 0))
                            outcome_icon = "✅" if t.get("actual_outcome") == "WIN" else "🔴"
                            recent_trades_text += f"{outcome_icon} {t.get('symbol')} | {t.get('predicted_direction')} | PnL: {pnl_val:+.4f}\n"
                    else:
                        recent_trades_text = "• <i>Waiting for first live execution cycle...</i>\n"

                except Exception as db_err:
                    logger.error(f"Failed to compile Supabase data for Telegram report: {db_err}")
                    regime_breakdown_text = "• ⚠️ <i>Supabase ledger context error.</i>\n"
                    recent_trades_text = "• <i>Unavailable</i>\n"

                diagnostic_nodes = []
                try:
                    sorted_metrics = sorted(
                        self.screener_metrics.items(),
                        key=lambda x: abs(x[1].get("vol_z", 0.0)),
                        reverse=True
                    )[:3]
                    for ticker, metrics in sorted_metrics:
                        bias = self.macro_regimes.get(ticker, "HOLD")
                        diagnostic_nodes.append(
                            f"• 📡 <b>{ticker}</b> | Z: <code>{metrics.get('vol_z', 0.0):+.2f}</code> | "
                            f"Vol: <code>{metrics.get('vol_mult', 1.0):.2f}x</code> | Bias: ── <b>{bias}</b> ──"
                        )
                except Exception as diag_err:
                    logger.error(f"Diagnostic array generation failed: {diag_err}")
                    diagnostic_nodes = ["• <i>Diagnostic matrix initializing...</i>"]

                diagnostic_block = "\n".join(diagnostic_nodes)

                active_provider = "GROQ_LLAMA_3_3_70B"
                if hasattr(self, 'ai_router') and hasattr(self.ai_router, 'providers') and self.ai_router.providers:
                    active_provider = getattr(self.ai_router, 'current_provider', "GROQ_LLAMA_3_3_70B")

                flat_nodes = []
                for sym, regimes in self.node_metrics_cache.items():
                    for regime, r_data in regimes.items():
                        if r_data.get("trades", 0) > 0:
                            flat_nodes.append((sym, regime, r_data))
                
                armed_count = sum(1 for n in flat_nodes if n[2].get("is_armed", False))
                total_trackers = len(self.asset_basket) * 2 
                
                top_nodes = sorted(flat_nodes, key=lambda x: x[2].get("bayesian_edge", 0), reverse=True)[:3]
                node_string = ""
                for sym, regime, r_data in top_nodes:
                    icon = "🟢" if r_data.get("is_armed") else "🟡"
                    reg_icon = "📈" if regime == "TRENDING" else "🕸️"
                    node_string += f"• {icon} {sym} ({reg_icon}) | Edge: {r_data.get('bayesian_edge',0):.2%} | Trd: {r_data.get('trades',0)}\n"
                
                if not node_string:
                    node_string = "• <i>Nodes currently gathering shadow data...</i>\n"

                report = (
                    f"💎 <b>𝗣██𝗔𝗦𝗞 𝗘𝗠𝗣𝗜𝗥𝗘 | 𝗤𝗨𝗔𝗡𝗧 𝗦𝗪𝗔𝗥𝗠 𝗢𝗦 (V5)</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏱️ <b>𝗨𝗽𝘁𝗶𝗺𝗲:</b> <code>{uptime_hours:.2f} Hours</code> | 🛰️ <b>𝗡𝗼𝗱𝗲𝘀:</b> <code>{len(self.asset_basket)} Live • {len(self.shadow_basket)} Shadow</code>\n\n"
                    f"⚙️ <b>𝗘𝗡𝗚𝗜𝗡𝗘 𝗦𝗧𝗔𝗧𝗨𝗦: 𝗥𝗲𝗴𝗶𝗺𝗲-𝗜𝘀𝗼𝗹𝗮𝘁𝗲𝗱 𝗦𝘄𝗮𝗿𝗺</b>\n"
                    f"• ⚔️ Live Armed Vectors:   <code>{armed_count} / {total_trackers} Active</code>\n"
                    f"• 🏆 Top Matrix Performers:\n"
                    f"{node_string}\n"
                    f"🌐 <b>𝗔𝗜 𝗖𝗔𝗦𝗖𝗔𝗗𝗘 𝗧𝗘𝗟𝗘𝗠𝗘𝗧𝗥𝗬</b>\n"
                    f"• Active Router Path: <code>{active_provider}</code>\n"
                    f"• Global News Flow:   <i>{self.global_macro_news_cache[:45]}...</i>\n\n"
                    f"💵 <b>𝗙𝗜𝗡𝗔𝗡𝗖𝗜𝗔𝗟 𝗩𝗔𝗨𝗟𝗧 𝗣𝗥𝗢𝗙𝗜𝗟𝗘</b>\n"
                    f"• Total Liquidity: <code>{current_vault_balance:.4f} USDT</code>\n"
                    f"• Session Return:  <code>{actual_net_pnl:+.4f} USDT</code>\n"
                    f"• Peak Drawdown:   <code>{drawdown_pct:.2%}</code>\n"
                    f"• Risk Buffer:     <code>[{drawdown_bar}]</code>\n\n"
                    f"🔬 <b>𝗗𝗔𝗜𝗟𝗬 𝗥𝗘𝗚𝗜𝗠𝗘 𝗣𝗥𝗢𝗙𝗜𝗟𝗘:</b>\n"
                    f"{regime_breakdown_text}\n"
                    f"🔥 <b>𝗛𝗜𝗚𝗛𝗘𝗦𝗧-𝗠𝗢𝗠𝗘𝗡𝗧𝗨𝗠 𝗠𝗢𝗩𝗘𝗥𝗦</b>\n"
                    f"{diagnostic_block}\n\n"
                    f"🏁 <b>𝗥𝗘𝗖𝗘𝗡𝗧 𝗦𝗘𝗦𝗦𝗜𝗢𝗡 𝗠𝗔𝗧𝗨𝗥𝗜𝗧𝗜𝗘𝗦</b>\n"
                    f"{recent_trades_text}"
                )
                
                report_task = asyncio.create_task(self.telegram.send_html_report(report))
                self._daemon_registry.add(report_task)
                report_task.add_done_callback(lambda t: logger.error(f"Telegram crash: {t.exception()}") if t.exception() else None)

    async def run_signal_lifecycle(self, symbol: str, direction: str, current_price: float, optimization: dict = None, real_spread: float = 0.0, vol_z_abs: float = 0.0, fluid_gating_coef: float = 1.0, is_golden_setup: bool = False):
        if symbol in self.active_positions_lock:
            return False
            
        self.active_positions_lock.add(symbol)
        
        try:
            signal_id = str(uuid.uuid4())
            confidence = self.macro_confidences.get(symbol, 0.35)
            
            feature_engine = self.feature_engines.get(symbol)
            market_regime = feature_engine.detect_market_regime() if feature_engine else "RANGING"
            
            raw_atr = feature_engine.get_computed_atr() if feature_engine and hasattr(feature_engine, 'get_computed_atr') else 0.0
            
            if raw_atr <= 0:
                price_history = self.screener_memory.get(symbol, {}).get("prices", [])
                if len(price_history) >= 20:
                    prices = list(price_history)[-20:]
                    price_range = (max(prices) - min(prices)) / np.mean(prices)
                    # 🚀 UPGRADE: Cap ATR fallback at 0.5% max
                    atr = current_price * min(0.005, max(0.001, price_range * 0.5))
                else:
                    atr = current_price * 0.005 # 0.5% absolute default
                logger.warning(f"⚠️ ATR Fallback: Using {atr/current_price:.2%} of price for {symbol}")
            else:
                atr = raw_atr
                
            self.current_atrs[symbol] = atr

            metrics = self.screener_metrics.get(symbol, {})
            vol_mult = metrics.get("vol_mult", 1.0)
            
            history = self.screener_memory.get(symbol, {}).get("prices", [])
            if len(history) < 100:
                self.active_positions_lock.discard(symbol)
                return False 
                
            if self.test_mode:
                features_dict = {
                    "symbol": symbol, "market_regime": market_regime, "adaptive_obi_z": 0.0, 
                    "liquidity_density_ratio": vol_mult, "bid_ask_spread": real_spread,
                    "virtual_sl": current_price * 0.98, "virtual_tp": current_price * 1.04   
                }
                await asyncio.to_thread(self.memory.commit_prediction, signal_id, time.time(), current_price, direction, confidence, features_dict, is_shadow=True)
                self.active_positions_lock.discard(symbol)
                return True

            # 🚀 V5 DYNAMIC ARMING
            node_stats = self.node_metrics_cache.get(symbol, {}).get(market_regime, {})
            bayesian_p = node_stats.get("bayesian_edge", 0.50)
            is_armed = node_stats.get("is_armed", False)

            fat_tail_multiplier = 1.5 + math.exp(min(vol_z_abs, 4.0) / 2.0)
            sl_distance = atr * fat_tail_multiplier
            
            min_sl_distance = current_price * 0.02
            sl_distance = max(sl_distance, min_sl_distance)
            tp_distance = max(sl_distance * 2.0, current_price * 0.04) 
            
            if direction == "BUY":
                initial_sl = current_price - sl_distance
                target_tp = current_price + tp_distance + (current_price * 0.0011)
            else:
                initial_sl = current_price + sl_distance
                target_tp = current_price - tp_distance - (current_price * 0.0011)
                
            tick_dec = Decimal(str(self.tick_sizes.get(symbol, 0.0001)))
            initial_sl = float((Decimal(str(initial_sl)) / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec)
            target_tp = float((Decimal(str(target_tp)) / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec)

            distance_to_sl = abs(current_price - initial_sl)
            if distance_to_sl <= 0:
                distance_to_sl = current_price * 0.015
            distance_to_tp = abs(target_tp - current_price)
            reward_risk_ratio = distance_to_tp / distance_to_sl

            base_kelly = bayesian_p - ((1.0 - bayesian_p) / max(0.1, reward_risk_ratio))

            historical_atr = self.volatility_baseline.get(symbol, atr)
            safe_historical_atr = max(historical_atr, current_price * 0.001)
            volatility_ratio = atr / safe_historical_atr

            if volatility_ratio > 1.0:
                volatility_adjustment = 1.0 / math.log1p(volatility_ratio)
            else:
                volatility_adjustment = 1.0
                
            volatility_adjustment = max(0.40, min(3.0, volatility_adjustment))

            balance = await self.executor.get_wallet_balance_usdt()
            
            if balance < 10.0:
                logger.critical(f"🛑 ACCOUNT TOO SMALL (${balance:.2f}). Trading halted to prevent liquidation.")
                self.active_positions_lock.discard(symbol)
                return False

            micro_multiplier = 0.75 if balance < 50.0 else 0.25
            adjusted_kelly = base_kelly * volatility_adjustment * micro_multiplier * fluid_gating_coef

            # 🚀 UPGRADE: QUARTER-KELLY & STRICT 2% RISK CAP
            quarter_kelly = adjusted_kelly * 0.25
            dynamic_risk_pct = max(0.001, min(0.02, quarter_kelly))

            if quarter_kelly <= 0.0 or not is_armed:
                features_dict = {
                    "symbol": symbol, "market_regime": market_regime, "adaptive_obi_z": 0.0, 
                    "liquidity_density_ratio": vol_mult, "bid_ask_spread": real_spread,
                    "virtual_sl": initial_sl, "virtual_tp": target_tp   
                }
                
                await asyncio.to_thread(self.memory.commit_prediction, signal_id, time.time(), current_price, direction, confidence, features_dict, is_shadow=True)
                self.active_positions_lock.discard(symbol)
                return True

            dollar_risk = balance * dynamic_risk_pct
            position_size = dollar_risk / distance_to_sl
            notional = max(position_size * current_price, 5.50)

            if not self.risk_vault.evaluate_portfolio_safety(balance, notional, symbol):
                logger.warning(f"🛡️ RISK VAULT REJECTED // {symbol} notional ${notional:.2f} blocked by portfolio safety gate.")
                self.active_positions_lock.discard(symbol)
                return False

            target_leverage = self.risk_vault.calculate_dynamic_leverage(
                notional, balance, base_leverage=5, hard_cap=15, sl_distance_pct=(distance_to_sl / current_price)
            )
            await self.executor.adjust_leverage(symbol, target_leverage)

            current_depth = {"bids": [[current_price]], "asks": [[current_price]]}
            if hasattr(feature_engine, 'get_orderbook_snapshot'):
                current_depth = feature_engine.get_orderbook_snapshot()

            if market_regime == "TRENDING":
                execution_success = await self.sor.execute_iceberg_block(
                    symbol=symbol, direction=direction, total_qty=position_size,
                    current_mid_price=current_price, stop_loss=initial_sl, take_profit=target_tp,
                    depth_snapshot=current_depth, vol_z=vol_z_abs, vol_mult=vol_mult, feature_engine=feature_engine
                )
            else:
                execution_success = await self.sor.execute_mean_reversion_bracket(
                    symbol=symbol, direction=direction, total_qty=position_size,
                    current_mid_price=current_price, stop_loss=initial_sl, take_profit=target_tp,
                    depth_snapshot=current_depth, vol_z=vol_z_abs, vol_mult=vol_mult, feature_engine=feature_engine
                )
            
            if not execution_success:
                self.active_positions_lock.discard(symbol)
                return False 
                
            self.risk_vault.update_position_ledger(symbol, notional)
            
            alert_text = (
                f"🧬 *DISTRIBUTED SWARM ORDER ROUTED*\n"
                f"• Node: {symbol} | {direction}\n"
                f"• Market Regime: {market_regime}\n"
                f"• Leverage Applied: {target_leverage}x\n"
                f"• Notional Value: ${notional:.2f} USDT\n"
                f"🛡️ *Elastic Brackets Active*: SL: {initial_sl} | TP: {target_tp}"
            )
            report_task = asyncio.create_task(self.telegram.log_message(alert_text, "SUCCESS"))
            self._daemon_registry.add(report_task)
            report_task.add_done_callback(lambda t: logger.error(f"Telegram crash: {t.exception()}") if t.exception() else None)
            
            daemon_task = asyncio.create_task(self._position_lifecycle_daemon(
                symbol, signal_id, direction, current_price, initial_sl, target_tp, atr, {"allocated_value_usdt": notional, "size": position_size}, feature_engine, target_leverage, market_regime
            ))
            self._daemon_registry.add(daemon_task)
            daemon_task.add_done_callback(
                lambda t: logger.error(f"☠️ DAEMON CRASH: {t.exception()}") if t.exception() else None
            )
            
            return True

        except Exception as e:
            logger.error(f"Distributed swarm execution routing failed for {symbol}: {e}")
            self.active_positions_lock.discard(symbol)
            return False

    async def _position_lifecycle_daemon(self, symbol: str, signal_id: str, direction: str, current_price: float, initial_sl: float, target_tp_price: float, atr: float, risk_matrix: dict, feature_engine, target_leverage: int = 8, market_regime: str = "TRENDING"):
        logger.info(f"👻 APEX MONITOR ARMED // Native Exchange Hand-off for {symbol}")
        
        exec_details = {
            "leverage": target_leverage,
            "execution_mode": "RECOVERY" if "RECOVERY" in signal_id else ("GHOST" if self.test_mode else "LIVE")
        }
        
        try:
            start_time = time.time()
            order_filled = False
            
            for _ in range(12):  
                await asyncio.sleep(5)
                try:
                    pos_response = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                    positions = pos_response.get("result", {}).get("list", [])
                    if positions and float(positions[0].get("size", 0.0)) > 0:
                        order_filled = True
                        actual_entry = float(positions[0].get("avgPrice", current_price))
                        actual_qty = float(positions[0].get("size", 0.0))
                        break
                except Exception as pos_check_e:
                    logger.error(f"Daemon pos loop check failed: {pos_check_e}")
                    continue

            if not order_filled:
                logger.critical(f"🔓 PORTFOLIO UNLOCKED // SOR failed to fill {symbol} within 60s. Canceling.")
                try:
                    await asyncio.to_thread(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
                except Exception as cancel_e: 
                    logger.error(f"Daemon cancellation fail: {cancel_e}")
                
                try:
                    final_check = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                    final_pos = final_check.get("result", {}).get("list", [])
                    if final_pos and float(final_pos[0].get("size", 0.0)) > 0:
                        logger.warning(f"⚠️ RACE CONDITION AVERTED // {symbol} filled exactly at timeout boundary. Adopting position.")
                        order_filled = True
                        actual_entry = float(final_pos[0].get("avgPrice", current_price))
                        actual_qty = float(final_pos[0].get("size", 0.0))
                except Exception as final_e:
                    logger.error(f"Final pos check failed: {final_e}")

                if not order_filled:
                    self.risk_vault.update_position_ledger(symbol, -risk_matrix['allocated_value_usdt'])
                    self.active_positions_lock.discard(symbol)
                    return

            try:
                pos_check = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                pos_data = pos_check.get("result", {}).get("list", [{}])[0]
                existing_sl = float(pos_data.get("stopLoss", 0.0))
                
                if existing_sl == 0.0:
                    await asyncio.to_thread(
                        self.executor.client.set_trading_stop,
                        category="linear", symbol=symbol, positionIdx=0,
                        stopLoss=str(round(initial_sl, 4))
                    )
                    logger.critical(f"🛡️ EMERGENCY SL INJECTED // {symbol} initial stop was naked. Hard SL set at {initial_sl}.")
            except Exception as e:
                logger.error(f"Failed to verify initial SL for {symbol}: {e}")

            activation_distance = atr * 0.8  
            trailing_distance = atr * 1.5    
            
            activation_price = actual_entry + activation_distance if direction == "BUY" else actual_entry - activation_distance
            
            tick_dec = Decimal(str(self.tick_sizes.get(symbol, 0.0001)))
            activation_price = str(float((Decimal(str(activation_price)) / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec))
            trailing_distance_str = str(float((Decimal(str(trailing_distance)) / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec))

            try:
                await asyncio.to_thread(
                    self.executor.client.set_trading_stop, 
                    category="linear", 
                    symbol=symbol, 
                    positionIdx=0, 
                    trailingStop=trailing_distance_str,
                    activePrice=activation_price
                )
                logger.info(f"🛡️ NATIVE TRAIL ARMED // {symbol} Trailing Stop handed to exchange (Act: {activation_price}, Dist: {trailing_distance_str})")
            except Exception as e:
                logger.error(f"Failed to set native trailing stop for {symbol}: {e}")

            max_daemon_seconds = 6 * 3600
            while time.time() - start_time < max_daemon_seconds:
                await asyncio.sleep(10)

                try:
                    pos_response = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                    pos_list = pos_response.get("result", {}).get("list", [])
                    position_gone = (not pos_list) or float(pos_list[0].get("size", 0.0)) == 0.0
                except Exception as pos_gone_err:
                    logger.error(f"Error checking position status: {pos_gone_err}")
                    position_gone = False

                settlement = await self.executor.check_recent_settlement(symbol=symbol, lookback_seconds=120)
                if settlement.get("closed"):
                    net_pnl = float(settlement.get('pnl', 0.0))
                    slippage_drag = actual_entry - current_price if direction == "BUY" else current_price - actual_entry
                    
                    report_message = (
                        f"🔔 <b>EXCHANGE EXECUTION TERMINATION</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 <b>Asset Node:</b> <code>{symbol}</code>\n"
                        f"📊 <b>Outcome:</b> " + ("🟢 PROFIT" if net_pnl > 0 else "🔴 LOSS") + f"\n"
                        f"💰 <b>Net Return:</b> <code>{net_pnl:.4f} USDT</code>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━"
                    )
                    report_task = asyncio.create_task(self.telegram.send_html_report(report_message))
                    self._daemon_registry.add(report_task)
                    report_task.add_done_callback(lambda t: logger.error(f"Telegram crash: {t.exception()}") if t.exception() else None)
                    
                    await asyncio.to_thread(self.memory.log_live_execution_result, signal_id, net_pnl, slippage_drag, settlement['outcome'], exec_details)
                    self.risk_vault.update_position_ledger(symbol, 0.0)

                    self.ales.feedback(symbol, 1.0 if net_pnl > 0 else -1.0)
                    break

                if position_gone:
                    logger.warning(f"🧾 RECONCILIATION // {symbol} closed outside the poll window. Pulling final PnL snapshot.")
                    try:
                        pnl_response = await asyncio.to_thread(self.executor.client.get_closed_pnl, category="linear", symbol=symbol, limit=5)
                        pnl_list = pnl_response.get("result", {}).get("list", [])
                        net_pnl = float(pnl_list[0].get("closedPnl", 0.0)) if pnl_list else 0.0
                    except Exception as pnl_err:
                        logger.error(f"Error fetching PnL snapshot: {pnl_err}")
                        net_pnl = 0.0
                        
                    await asyncio.to_thread(self.memory.log_live_execution_result, signal_id, net_pnl, 0.0, "RECONCILED", exec_details)
                    self.risk_vault.update_position_ledger(symbol, 0.0)
                    self.ales.feedback(symbol, 1.0 if net_pnl > 0 else -1.0)
                    break
            else:
                logger.error(f"⏰ DAEMON TIMEOUT // {symbol} monitor exceeded 6h. Force-clearing risk ledger state.")
                self.risk_vault.update_position_ledger(symbol, 0.0)

        except Exception as daemon_error:
            logger.error(f"☠️ FATAL DAEMON CRASH on {symbol}: {daemon_error}")
        finally:
            self.active_positions_lock.discard(symbol)
            self.risk_vault.update_position_ledger(symbol, 0.0)

    async def run_engine_forever(self):
        logger.critical("LAUNCHING DECENTRALIZED QUANT SWARM DAEMON DEPLOYMENTS...")
        
        try:
            await self._fetch_exchange_tick_sizes()
            await self.synchronize_exchange_state()
        except Exception as boot_err:
            logger.error(f"Boot synchronization encountered an error, proceeding with defaults. Error: {boot_err}")
        
        try:
            boot_basket = await self.executor.get_top_volatile_assets(limit=100, min_turnover=10_000_000)
        except Exception as fetch_err:
            logger.error(f"Failed to fetch market data via REST on boot: {fetch_err}. Falling back to default basket.")
            boot_basket = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT", "NEARUSDT", "APTUSDT", "INJUSDT", "OPUSDT", "FILUSDT", "ARBUSDT", "STXUSDT", "RNDRUSDT", "MNTUSDT", "MKRUSDT", "SEIUSDT", "SUIUSDT", "ORDIUSDT"]
            
        if boot_basket and len(boot_basket) >= 25:
            if "BTCUSDT" in boot_basket:
                boot_basket.remove("BTCUSDT")
                
            self.asset_basket = ["BTCUSDT"] + boot_basket[:24]
            self.shadow_basket = boot_basket[24:]
            
            self.feature_engines = {s: AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600) for s in self.asset_basket}
            self.math_engines = {s: FastMathEngine() for s in self.asset_basket}
            self.screener_memory = {s: {"prices": deque(maxlen=150), "highs": deque(maxlen=150), "lows": deque(maxlen=150), "macro_prices": deque(maxlen=48), "volumes": deque(maxlen=150), "atr_history": deque(maxlen=self.volatility_window), "last_update_time": 0.0} for s in self.asset_basket}
            self.macro_regimes = {s: "HOLD" for s in self.asset_basket}
            self.macro_confidences = {s: 0.0 for s in self.asset_basket}
            self.current_atrs = {s: 0.0 for s in self.asset_basket}
            self.last_execution_timestamps = {s: 0.0 for s in self.asset_basket}
            self.screener_metrics = {s: {"vol_mult": 1.0, "vol_z": 0.0, "smoothed_price": 0.0, "hawkes_score": 0.0} for s in self.asset_basket}
            self.volatility_baseline = {s: 0.0 for s in self.asset_basket}
        
        await asyncio.gather(
            self.run_macro_commander(),        
            self.run_macro_regime_loop(),      
            self.run_universe_refresher(),
            self.stream_manager_loop(),
            self.run_system_heartbeat(),
            self.run_shadow_swarm_scanner(),
            self.cleanup_stale_locks() 
        )

if __name__ == "__main__":
    from keep_alive import keep_alive
    keep_alive()
    engine = DistributedQuantEngine()
    try:
        asyncio.run(engine.run_engine_forever())
    except KeyboardInterrupt:
        sys.exit(0)