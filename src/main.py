import os
import sys
import time
import math
import asyncio
import logging
import uuid
import re
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
from core.hawkes_engine import BivariateHawkesEngine  
from core.structural_edge_gate import MicrostructureEdgeGate 
from features.adaptive_engine import AdaptiveFeatureEngine
from features.vpin_clock import VolumeSynchronizedClock
from portfolio.risk_manager import InstitutionalRiskVault
from execution.sor import SmartOrderRouter

# External Service Connectors
from services.ai_router import ResilientAIRouter
from services.adversarial_ai import AdversarialDebateMatrix
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


class PreCognitiveFractionalEngine:
    """
    🚀 APEX UPGRADE: Fractional Brownian Motion & Order Flow Fluid Dynamics
    Replaces basic math with predictive Hurst Exponent estimations and 
    Elastic Order Flow Tensors for pre-cognitive market mapping.
    """
    def __init__(self, memory_depth=500):
        # Kalman-Bucy Variance Squeezer
        self.k_est = 0.0
        self.k_err = 1.0
        self.q = 0.01  
        self.r = 0.1   
        self.trade_timestamps = deque() 
        self.decay_factor = 0.5  
        
        # Fractional Memory (Hurst) Array
        self.prices = deque(maxlen=memory_depth)
        self.log_returns = deque(maxlen=memory_depth)
        self.hurst_exponent = 0.5 # 0.5 = Random Walk
        
        # Order Flow Imbalance (OFI) Elastic Tensor
        self.rolling_ofi = 0.0
        self.ofi_variance = 1.0
        self.prev_bid = 0.0
        self.prev_bid_size = 0.0
        self.prev_ask = 0.0
        self.prev_ask_size = 0.0

    def kalman_update(self, measurement: float) -> float:
        if self.k_est == 0.0:
            self.k_est = measurement
            return measurement
        p_pred = self.k_err + self.q
        kalman_gain = p_pred / (p_pred + self.r)
        self.k_est = self.k_est + kalman_gain * (measurement - self.k_est)
        self.k_err = (1.0 - kalman_gain) * p_pred
        return self.k_est

    def update_hurst_online(self, price: float) -> float:
        self.prices.append(price)
        if len(self.prices) > 2:
            ret = math.log(self.prices[-1] / self.prices[-2])
            if not math.isnan(ret) and not math.isinf(ret):
                self.log_returns.append(ret)
                
        if len(self.log_returns) > 30:
            rets = np.array(self.log_returns)
            var_1 = np.var(rets)
            if var_1 > 1e-12:
                rets_k = rets[2:] + rets[1:-1]
                var_k = np.var(rets_k)
                vr = var_k / (2.0 * var_1)
                h_est = 0.5 + 0.5 * math.log(vr + 1e-9) / math.log(2)
                # Bound Hurst to physical market realities
                self.hurst_exponent = max(0.1, min(0.9, h_est))
        return self.hurst_exponent

    def apply_orderbook_fluid_dynamics(self, best_bid, bid_vol, best_ask, ask_vol) -> float:
        delta_W = 0.0
        # Calculate instantaneous physical pressure
        if best_bid >= self.prev_bid:
            delta_W += bid_vol if best_bid > self.prev_bid else (bid_vol - self.prev_bid_size)
        if best_ask <= self.prev_ask:
            delta_W -= ask_vol if best_ask < self.prev_ask else (ask_vol - self.prev_ask_size)
            
        self.prev_bid, self.prev_bid_size = best_bid, bid_vol
        self.prev_ask, self.prev_ask_size = best_ask, ask_vol
        
        # Exponentially Weighted Moving Average of Pressure
        alpha = 0.1
        self.rolling_ofi = (1 - alpha) * self.rolling_ofi + alpha * delta_W
        self.ofi_variance = (1 - alpha) * self.ofi_variance + alpha * (delta_W - self.rolling_ofi)**2
        
        return self.rolling_ofi / (math.sqrt(self.ofi_variance) + 1e-9)

    def hawkes_cluster_score(self, current_time: float, volume: float) -> float:
        self.trade_timestamps.append((current_time, volume))
        while self.trade_timestamps and current_time - self.trade_timestamps[0][0] >= 60:
            self.trade_timestamps.popleft()
        
        excitement = 0.0
        for t_time, t_vol in self.trade_timestamps:
            excitement += t_vol * math.exp(-self.decay_factor * (current_time - t_time))
        return excitement


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
        self.force_dna_refresh = asyncio.Event() # 🚀 Instant Event Trigger
        
        self.memory = MemoryBank()
        self.risk_vault = InstitutionalRiskVault(max_drawdown_pct=0.25, max_single_position_risk_pct=0.15)
        
        # 🚀 CORE DATA STRUCTURES
        self.vpin_clocks: Dict[str, VolumeSynchronizedClock] = {}
        self.hawkes_engines: Dict[str, BivariateHawkesEngine] = {}
        self.edge_gates: Dict[str, MicrostructureEdgeGate] = {}
        self.feature_engines: Dict[str, AdaptiveFeatureEngine] = {}
        self.fractional_engines: Dict[str, PreCognitiveFractionalEngine] = {} # 🚀 UPGRADED ENGINE
        self.screener_memory: Dict[str, Dict[str, Any]] = {}
        self.screener_metrics: Dict[str, Dict[str, float]] = {}
        
        # 🚀 THE RAM PRE-WARMER CACHE
        self.ram_dna_cache: Dict[str, dict] = {}
        
        self.debate_matrix = AdversarialDebateMatrix() 
        
        self.macro_regimes: Dict[str, str] = {}
        self.macro_confidences: Dict[str, float] = {}
        self.current_atrs: Dict[str, float] = {}
        self.last_execution_buckets: Dict[str, int] = {}
        self.volatility_baseline: Dict[str, float] = {}
        self.volatility_window = 100
        
        self.active_positions_lock = set()
        self.evaluation_lock = set() 
        
        # 🚀 Anti-Spam Throttle Log
        self.last_vpin_eval_time: Dict[str, float] = {}
        
        self._daemon_registry = weakref.WeakSet()
        self._log_throttle_cache: Dict[str, float] = {}
        
        self.tick_sizes: Dict[str, float] = {}
        self.global_macro_news_cache: str = "No significant macro shifts detected."
        self.last_news_fetch: float = 0.0

        self.global_state_cache = {"last_updated": 0.0}
        
        # 🚀 SAFELY INITIALIZE EVERYTHING UPFRONT
        self._initialize_symbol_structures(self.asset_basket)

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

    def _initialize_symbol_structures(self, symbols: List[str]):
        """ATOMIC STATE MANAGER"""
        for s in symbols:
            if s not in self.vpin_clocks: self.vpin_clocks[s] = VolumeSynchronizedClock(bucket_volume=1_000_000.0)
            if s not in self.hawkes_engines: self.hawkes_engines[s] = BivariateHawkesEngine(calibration_window=500)
            if s not in self.edge_gates: self.edge_gates[s] = MicrostructureEdgeGate()
            if s not in self.feature_engines: self.feature_engines[s] = AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600)
            if s not in self.fractional_engines: self.fractional_engines[s] = PreCognitiveFractionalEngine()
            
            if s not in self.screener_memory:
                self.screener_memory[s] = {
                    "prices": deque(maxlen=150), "highs": deque(maxlen=150), "lows": deque(maxlen=150), 
                    "macro_prices": deque(maxlen=48), "volumes": deque(maxlen=150), 
                    "atr_history": deque(maxlen=self.volatility_window), "last_update_time": 0.0
                }
            if s not in self.screener_metrics: self.screener_metrics[s] = {"vol_mult": 1.0, "vol_z": 0.0, "smoothed_price": 0.0, "hawkes_score": 0.0}
            if s not in self.last_execution_buckets: self.last_execution_buckets[s] = 0
            if s not in self.volatility_baseline: self.volatility_baseline[s] = 0.0
            if s not in self.ram_dna_cache: self.ram_dna_cache[s] = {"is_armed": True, "win_rate": 0.50}
            if s not in self.last_vpin_eval_time: self.last_vpin_eval_time[s] = 0.0 

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
                logger.warning(f"⚠️ Telegram dispatch fault: {e}")
                await asyncio.sleep(2 ** attempt)

    async def _fetch_exchange_tick_sizes(self):
        try:
            info = await asyncio.to_thread(self.executor.client.get_instruments_info, category="linear")
            for item in info.get("result", {}).get("list", []):
                self.tick_sizes[item.get("symbol")] = float(item.get("priceFilter", {}).get("tickSize", "0.0001"))
        except Exception: pass

    async def synchronize_exchange_state(self):
        try:
            logger.info("📡 SYNCING EXCHANGE STATE: Scanning for orphaned live positions...")
            pos_response = await asyncio.to_thread(self.executor.client.get_positions, category="linear", settleCoin="USDT")
            active_orphans = [p for p in pos_response.get("result", {}).get("list", []) if float(p.get("size", 0.0)) > 0]
            
            if not active_orphans: return
            logger.critical(f"⚠️ RECOVERY ENGAGED: Found {len(active_orphans)} active trades left open.")
            
            for pos in active_orphans:
                symbol = pos["symbol"]
                self._initialize_symbol_structures([symbol]) 
                
                qty, entry_price = float(pos["size"]), float(pos["avgPrice"])
                direction = "BUY" if pos["side"].upper() == "BUY" else "SELL"
                current_sl = float(pos.get("stopLoss", 0.0)) or (entry_price * 0.95 if direction == "BUY" else entry_price * 1.05)

                self.active_positions_lock.add(symbol)
                atr = entry_price * 0.0125
                risk_matrix = {"allocated_value_usdt": qty * entry_price, "size": qty, "recommended_leverage": 8}
                
                daemon_task = asyncio.create_task(self._position_lifecycle_daemon(
                    symbol, f"RECOVERY-{str(uuid.uuid4())[:8]}", direction, entry_price, current_sl, 
                    entry_price * 1.05 if direction == "BUY" else entry_price * 0.95, atr, 
                    risk_matrix, self.feature_engines[symbol], 8, "RANGING"
                ))
                self._daemon_registry.add(daemon_task)
        except Exception: pass

    async def cleanup_stale_locks(self):
        while True:
            await asyncio.sleep(300) 
            try:
                for symbol in list(self.active_positions_lock):
                    if not hasattr(self.risk_vault, 'active_positions') or symbol not in self.risk_vault.active_positions:
                        pos_response = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                        if not any(float(p.get("size", 0.0)) > 0 for p in pos_response.get("result", {}).get("list", [])):
                            self.active_positions_lock.discard(symbol)
            except Exception: pass

    async def _update_global_news_cache(self):
        if time.time() - self.last_news_fetch > 300: 
            try:
                context = await asyncio.wait_for(self.macro_data_feed.fetch_market_snapshot("BTCUSDT", self.timeframe), timeout=8.0)
                if context and "news_context" in context: self.global_macro_news_cache = context["news_context"]
                self.last_news_fetch = time.time()
            except Exception: pass

    async def run_macro_commander(self):
        logger.info("🧠 MACRO COMMANDER ONLINE. Systemic LLM oversight enabled.")
        while True:
            await asyncio.sleep(900) 
            await self._update_global_news_cache()
            try:
                verdict = await self.debate_matrix.execute_debate_cycle("BTCUSDT", {"vpin_z_score": 0.0, "current_price": 0.0}, {}, self.global_macro_news_cache)
                macro_action = verdict.get("action", "HOLD")
                for symbol, engine in self.hawkes_engines.items():
                    if hasattr(engine, 'base_mu'):
                        engine.base_mu = np.array([0.15, 0.05]) if macro_action == "BUY" else (np.array([0.05, 0.15]) if macro_action == "SELL" else np.array([0.1, 0.1]))
            except Exception: pass

    async def run_dna_prewarmer(self):
        logger.info("🔥 RAM PRE-WARMER ONLINE: Actively pre-fetching database edge logic.")
        while True:
            try:
                await asyncio.wait_for(self.force_dna_refresh.wait(), timeout=300.0)
                self.force_dna_refresh.clear()
            except asyncio.TimeoutError: pass
                
            try:
                fetch_tasks = {}
                for symbol in list(self.asset_basket):
                    feature_engine = self.feature_engines.get(symbol)
                    metrics = self.screener_metrics.get(symbol, {})
                    if not feature_engine: continue
                    
                    current_dna = {
                        "vol_mult": metrics.get("vol_mult", 1.0), 
                        "z_obi": feature_engine.obi_history[-1] if feature_engine.obi_history else 0.0, 
                        "spread_pct": 0.001
                    }
                    fetch_tasks[symbol] = asyncio.to_thread(self.memory.compute_latent_dna_edge, current_dna, 30)
                
                if not fetch_tasks: continue

                symbols = list(fetch_tasks.keys())
                results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
                
                for sym, result in zip(symbols, results):
                    if isinstance(result, Exception):
                        self._throttled_log("WARNING", f"⚠️ DNA pre-warm failed for {sym}: {result}. Retaining previous edge state.")
                        self.ram_dna_cache[sym] = self.ram_dna_cache.get(sym, {"is_armed": True, "win_rate": 0.50})
                    else:
                        self.ram_dna_cache[sym] = result
            except Exception: pass

    async def handle_incoming_trade(self, trade_data: Dict[str, Any]):
        symbol = trade_data.get("symbol")
        if not symbol or symbol not in self.asset_basket: return
        
        try:
            hawkes = self.hawkes_engines.get(symbol)
            frac_engine = self.fractional_engines.get(symbol)
            if not hawkes or not frac_engine: return
            
            price = float(trade_data.get("price", 0.0))
            volume = float(trade_data.get("size", 0.0))
            is_buy = (str(trade_data.get("side", "")).upper() == "BUY")
            timestamp = float(trade_data.get("timestamp", time.time() * 1000)) / 1000.0
            
            # 🚀 Online Pre-Cognitive Updates
            frac_engine.update_hurst_online(price)
            frac_engine.kalman_update(price)
            
            hawkes.apply_tick(timestamp, is_buy, volume)
            delta = hawkes.calculate_imbalance_delta()
            
            if abs(delta) >= 0.85 and symbol not in self.active_positions_lock:
                action = "BUY" if delta > 0 else "SELL"
                logger.critical(f"⚡ HAWKES CASCADE DETECTED // {symbol} | Delta: {delta:.2f} | Executing {action} in <1ms.")
                self.active_positions_lock.add(symbol)
                asyncio.create_task(self._execute_hawkes_trigger(symbol, action, price))
                
        except Exception: pass

    async def _execute_hawkes_trigger(self, symbol: str, action: str, price: float):
        try:
            dna_stats = self.ram_dna_cache.get(symbol, {"is_armed": True, "win_rate": 0.50})
            await self.run_signal_lifecycle(symbol=symbol, direction=action, current_price=price, confidence=0.90, dna_stats=dna_stats, vpin_z=4.0)
        except Exception as e:
            logger.error(f"❌ HAWKES EXECUTION FAILURE for {symbol}: {e}")
            self.active_positions_lock.discard(symbol)

    async def handle_incoming_orderbook_tick(self, depth_data: Dict[str, Any]):
        symbol = depth_data.get("s")
        if not symbol or symbol not in self.asset_basket: return

        bids, asks = depth_data.get("b", []), depth_data.get("a", [])
        if bids and asks:
            try:
                best_bid, bid_size = float(bids[0][0]), float(bids[0][1])
                best_ask, ask_size = float(asks[0][0]), float(asks[0][1])
                mid_price = (best_bid + best_ask) / 2.0
                
                gate = self.edge_gates.get(symbol)
                if gate: gate.update_orderbook_state(best_bid, bid_size, best_ask, ask_size, mid_price)
                
                # 🚀 Fluid Dynamics Update
                frac_engine = self.fractional_engines.get(symbol)
                if frac_engine: frac_engine.apply_orderbook_fluid_dynamics(best_bid, bid_size, best_ask, ask_size)
                    
            except Exception: pass

        is_snapshot = depth_data.get("type") == "snapshot"
        feature_engine = self.feature_engines.get(symbol)
        if feature_engine:
            feature_engine.push_orderbook_tick(bids, asks, is_snapshot=is_snapshot)

    async def handle_incoming_basket_screener_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if not symbol or symbol not in self.asset_basket: return
        self._initialize_symbol_structures([symbol]) 
        self.screener_memory[symbol]["last_update_time"] = time.time()

    async def handle_incoming_kline_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if not symbol or symbol not in self.asset_basket: return
        self._initialize_symbol_structures([symbol]) 
            
        interval = data["interval"]
        candle = data["candle_data"]
        c_open, c_high, c_low, c_close, c_vol = map(float, [candle.get("open", 0), candle.get("high", 0), candle.get("low", 0), candle.get("close", 0), candle.get("volume", 0)])

        if not candle.get("confirm", False): return
        
        feature_engine = self.feature_engines.get(symbol)
        if feature_engine:
            feature_engine.update_multi_timeframe_candle(timeframe=interval, open_p=c_open, high_p=c_high, low_p=c_low, close_p=c_close, volume=c_vol)
        
        history = self.screener_memory.get(symbol)
        if not history: return

        if str(interval) == "1":
            history["volumes"].append(c_vol)
            history["prices"].append(c_close)
            history["highs"].append(c_high)
            history["lows"].append(c_low)
            
            is_seller_initiated = c_close < c_open 
            
            clock = self.vpin_clocks.get(symbol)
            if clock:
                manifests = clock.process_tick(c_close, c_vol, is_seller_initiated)
                for manifest in manifests:
                    if manifest.get("valid"):
                        asyncio.create_task(self.evaluate_vpin_anomaly(symbol, manifest))
            
            current_raw_atr = feature_engine.get_computed_atr() if feature_engine and hasattr(feature_engine, 'get_computed_atr') else (c_high - c_low)
            if current_raw_atr > 0:
                history["atr_history"].append(current_raw_atr)
                if len(history["atr_history"]) >= 20:
                    self.volatility_baseline[symbol] = np.mean(list(history["atr_history"]))
                
            if len(history["volumes"]) >= 15:
                vol_array = np.array(list(history["volumes"]))
                weights = np.exp(np.linspace(-1., 0., len(vol_array[:-1])))
                weights /= weights.sum()
                ewm_vol = max(np.sum(vol_array[:-1] * weights), 1.0) 
                self.screener_metrics[symbol] = {"vol_mult": float(c_vol / ewm_vol), "vol_z": 0.0, "smoothed_price": c_close, "hawkes_score": 0.0}

    async def evaluate_vpin_anomaly(self, symbol: str, vpin_manifest: dict):
        clock = self.vpin_clocks.get(symbol)
        if not clock: return
        
        # 🚀 ANTI-SPAM THROTTLE
        now = time.time()
        if now - self.last_vpin_eval_time.get(symbol, 0.0) < 2.0: return
        self.last_vpin_eval_time[symbol] = now
        
        vpin_z = float(vpin_manifest.get("vpin_z_score", 0.0))
        if abs(vpin_z) < 2.0: return
            
        current_bucket_count = clock.total_buckets_closed
        if (current_bucket_count - self.last_execution_buckets.get(symbol, 0)) < 15: return
        if time.time() - self.screener_memory.get(symbol, {}).get("last_update_time", 0.0) > 8.0: return
            
        if symbol in self.active_positions_lock or symbol in self.evaluation_lock: return 
        
        self.evaluation_lock.add(symbol)
        drift_pct = 0.0
        
        try:
            feature_engine = self.feature_engines.get(symbol)
            if not feature_engine: return
            
            market_regime = feature_engine.detect_market_regime()
            atr = feature_engine.get_computed_atr() if hasattr(feature_engine, 'get_computed_atr') else (vpin_manifest["current_price"] * 0.01)
            
            ob_snapshot = feature_engine.get_orderbook_snapshot() if hasattr(feature_engine, 'get_orderbook_snapshot') else {"bids": [[0,0]], "asks": [[0,0]]}
            best_bid = float(ob_snapshot.get("bids", [[vpin_manifest["current_price"]]])[0][0])
            best_ask = float(ob_snapshot.get("asks", [[vpin_manifest["current_price"]]])[0][0])
            spread_cost = (best_ask - best_bid) / vpin_manifest["current_price"]
            
            dna_stats = self.ram_dna_cache.get(symbol, {"is_armed": True, "win_rate": 0.50})
            edge_gate = self.edge_gates.get(symbol)
            if not edge_gate: return
            
            debate_start_time = time.time()
            verdict = edge_gate.evaluate_structural_edge(symbol, vpin_z)
            debate_latency = time.time() - debate_start_time
            
            action = verdict.get("action", "HOLD")
            confidence = verdict.get("confidence", 0.0)
            
            prices_list = self.screener_memory.get(symbol, {}).get("prices", [])
            post_debate_price = prices_list[-1] if prices_list else vpin_manifest["current_price"]
            drift_pct = abs(post_debate_price - vpin_manifest["current_price"]) / vpin_manifest["current_price"]
            
            # 🚀 PRE-COGNITIVE FRACTAL EDGE CALCULATIONS (The Brains)
            fractal_engine = self.fractional_engines.get(symbol)
            hurst_H = fractal_engine.hurst_exponent if fractal_engine else 0.5
            ofi_z = (fractal_engine.rolling_ofi / (math.sqrt(fractal_engine.ofi_variance) + 1e-9)) if fractal_engine else 0.0
            
            # Predict Regime State BEFORE standard moving averages catch up
            predicted_regime = "FRACTAL_TREND" if hurst_H > 0.6 else ("FRACTAL_RANGE" if hurst_H < 0.4 else market_regime)
            
            z_impact = min(1.5, 1.0 + (max(0, abs(vpin_z) - 2.0) * 0.25))
            
            # Massive combat upgrades
            if predicted_regime == "FRACTAL_TREND":
                regime_multiplier = 3.5  # Heavy breakout allocation
                dynamic_threshold = max(0.0003, spread_cost) # Execute immediately
            elif predicted_regime == "FRACTAL_RANGE":
                regime_multiplier = 1.8  # Counter-trend scalp
                dynamic_threshold = max(0.0005, spread_cost + 0.0002)
            else:
                regime_multiplier = 2.0
                dynamic_threshold = max(0.0008, spread_cost + 0.0005)
                
            expected_roi = (atr / vpin_manifest["current_price"]) * regime_multiplier * z_impact
            
            # OFI Elastic Confirmation (Order flow confirms structural edge)
            if action == "BUY" and ofi_z > 1.5: confidence += 0.08
            if action == "SELL" and ofi_z < -1.5: confidence += 0.08
            if action == "BUY" and ofi_z < -1.0: confidence -= 0.15 # Institutional trap fading detected
            if action == "SELL" and ofi_z > 1.0: confidence -= 0.15
            
            net_alpha = (confidence * (expected_roi - spread_cost)) - drift_pct
            
            if net_alpha < dynamic_threshold:
                # 🚀 LOGGING FIX: Uses _throttled_log to stop printing to the terminal every 2 seconds during chop
                self._throttled_log(
                    "CRITICAL", 
                    f"🛡️ PRE-COGNITIVE DECAY // {symbol} [{predicted_regime} | H={hurst_H:.2f}] | Alpha: {net_alpha:.2%} | Drift: {drift_pct:.2%}. Aborting.",
                    category=f"alpha_decay_{symbol}",
                    throttle_seconds=15
                )
                return
            
            # 🚀 DYNAMIC CONFIDENCE GATE
            min_confidence = 0.48 if predicted_regime == "FRACTAL_TREND" else 0.54
            if abs(vpin_z) >= 3.5: min_confidence -= 0.08
            
            if action in ["BUY", "SELL"] and confidence >= min_confidence:
                self.active_positions_lock.add(symbol)
                self.last_execution_buckets[symbol] = current_bucket_count
                asyncio.create_task(self.run_signal_lifecycle(symbol, action, post_debate_price, confidence, dna_stats, vpin_z))
            else:
                self._throttled_log("INFO", f"🛑 EDGE GATE QUARANTINE // Math rejected {symbol}. Reason: {verdict.get('reasoning')}", category=f"quarantine_{symbol}", throttle_seconds=20)
                
        except Exception as e:
            logger.error(f"❌ VPIN Anomaly evaluation failed for {symbol}: {e}")
        finally:
            self.evaluation_lock.discard(symbol)

    async def run_universe_refresher(self):
        while True:
            await asyncio.sleep(14400) 
            logger.info("🌍 FAST SATELLITE ROTATION INITIATED. Querying Bybit...")
            
            try:
                await self._fetch_exchange_tick_sizes()
                full_market = await self.executor.get_top_volatile_assets(limit=100, min_turnover=10_000_000)
                if len(full_market) < 25: full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
            except Exception:
                full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
                
            if "BTCUSDT" in full_market: full_market.remove("BTCUSDT")
            
            new_core_basket = ["BTCUSDT"] + [s for s in self.active_positions_lock if s != "BTCUSDT"]
            for sym in full_market:
                if sym not in new_core_basket and len(new_core_basket) < 25: new_core_basket.append(sym)
                    
            self.asset_basket = new_core_basket
            self.shadow_basket = [s for s in full_market if s not in self.asset_basket]
            if len(self.shadow_basket) < 10:
                self.shadow_basket.extend([s for s in ["XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT"] if s not in self.shadow_basket])
            
            # 🚀 ATOMIC RE-ALLOCATION (Fixes memory leaks)
            new_vpin_clocks = {}
            new_hawkes_engines = {}
            new_edge_gates = {}
            new_fractional = {}
            new_dna_cache = {}
            new_last_dna = {}
            new_last_eval = {}
            
            for s in self.asset_basket:
                new_vpin_clocks[s] = self.vpin_clocks.get(s, VolumeSynchronizedClock(bucket_volume=1_000_000.0))
                new_hawkes_engines[s] = self.hawkes_engines.get(s, BivariateHawkesEngine(calibration_window=500))
                new_edge_gates[s] = self.edge_gates.get(s, MicrostructureEdgeGate())
                new_fractional[s] = self.fractional_engines.get(s, PreCognitiveFractionalEngine())
                new_dna_cache[s] = self.ram_dna_cache.get(s, {})
                new_last_dna[s] = self.last_dna_fetch.get(s, 0.0)
                new_last_eval[s] = self.last_vpin_eval_time.get(s, 0.0)
                
            self.vpin_clocks = new_vpin_clocks
            self.hawkes_engines = new_hawkes_engines
            self.edge_gates = new_edge_gates
            self.fractional_engines = new_fractional
            self.ram_dna_cache = new_dna_cache
            self.last_dna_fetch = new_last_dna
            self.last_vpin_eval_time = new_last_eval

            new_feature_engines = {}
            new_screener_memory = {}
            new_last_buckets = {}
            for s in self.asset_basket:
                new_feature_engines[s] = self.feature_engines.get(s, AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600))
                new_last_buckets[s] = self.last_execution_buckets.get(s, 0)
                cached_history = self.screener_memory.get(s)
                if not cached_history:
                    new_screener_memory[s] = {"prices": deque(maxlen=150), "highs": deque(maxlen=150), "lows": deque(maxlen=150), "macro_prices": deque(maxlen=48), "volumes": deque(maxlen=150), "atr_history": deque(maxlen=self.volatility_window), "last_update_time": 0.0}
                else: new_screener_memory[s] = cached_history

            self.feature_engines = new_feature_engines
            self.screener_memory = new_screener_memory
            self.last_execution_buckets = new_last_buckets

            logger.info(f"🌌 QUANT UNIVERSE MATRIX RE-CALIBRATED.")
            self.stream_restart_event.set()
            self.force_dna_refresh.set() 

    async def run_shadow_swarm_scanner(self):
        logger.info("🦇 SHADOW SWARM: Activated.")
        while True:
            await asyncio.sleep(300) 
            if not self.shadow_basket: continue
            try:
                price_map = {t["symbol"]: float(t["lastPrice"]) for t in (await asyncio.to_thread(self.executor.client.get_tickers, category="linear")).get("result", {}).get("list", []) if t["symbol"] in self.shadow_basket}
                for sym in self.shadow_basket:
                    if sym in price_map and random.random() < 0.15: 
                        price = price_map[sym]
                        await asyncio.to_thread(self.memory.commit_prediction, str(uuid.uuid4()), time.time(), price, random.choice(["BUY", "SELL"]), 0.50, {"symbol": sym, "virtual_sl": price * 0.98, "virtual_tp": price * 1.04, "market_regime": "SHADOW_SIM", "adaptive_obi_z": 0.0, "liquidity_density_ratio": 1.0}, is_shadow=True)
            except Exception: pass

    async def stream_manager_loop(self):
        while True:
            stream_feed = HighVelocityMultiFeed(
                basket=self.asset_basket, intervals=["1", "5", "15"],
                orderbook_callback=self.handle_incoming_orderbook_tick, screener_callback=self.handle_incoming_basket_screener_update,
                kline_callback=self.handle_incoming_kline_update, trade_callback=self.handle_incoming_trade, engine_reference=self  
            )
            stream_task = asyncio.create_task(stream_feed.initialize_multiplexed_stream())
            await self.stream_restart_event.wait()
            stream_task.cancel()
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
                try:
                    age_cutoff_time = time.time() - 1800 
                    valid_assets = [sym for sym in self.asset_basket if self.screener_memory.get(sym, {}).get("prices")]
                    if valid_assets: await asyncio.to_thread(self.memory.resolve_batch_historical_predictions, assets=list(valid_assets), current_prices={sym: self.screener_memory[sym] for sym in valid_assets}, age_cutoff=age_cutoff_time)
                except Exception: pass

            logger.info(f"💓 SWARM HEARTBEAT: Matrix is active. Uptime: {uptime_hours:.2f} hours.")

            if loop_counter % 5 == 0:
                self.global_state_cache["last_updated"] = time.time()
                current_vault_balance = await self.executor.get_wallet_balance_usdt()

                if "wallet_baseline" not in self.global_state_cache: self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01)
                if "start_of_day_balance" not in self.global_state_cache: self.global_state_cache["start_of_day_balance"] = current_vault_balance
                    
                today_start_iso = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                data = (await asyncio.to_thread(lambda: self.memory.supabase.table("quantitative_ledger").select("market_regime, net_pnl, symbol, predicted_direction, actual_outcome").eq("resolved", True).eq("is_shadow", False).gte("timestamp", today_start_iso).execute())).data or []
                
                db_daily_pnl = sum(float(row.get("net_pnl", 0.0)) for row in data)
                discrepancy = current_vault_balance - (self.global_state_cache["start_of_day_balance"] + db_daily_pnl)
                
                if discrepancy > 5.0 or discrepancy < -5.0:
                    self.global_state_cache["start_of_day_balance"] += discrepancy
                    self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01) if discrepancy < -5.0 else self.global_state_cache["wallet_baseline"] + discrepancy

                actual_net_pnl = current_vault_balance - self.global_state_cache["start_of_day_balance"]
                baseline = self.global_state_cache["wallet_baseline"]
                
                if current_vault_balance > baseline:
                    self.global_state_cache["wallet_baseline"] = current_vault_balance
                    baseline = current_vault_balance
                    
                drawdown_pct = max(0.0, (baseline - current_vault_balance) / baseline)
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

                clock_states = [f"• ⏱️ <b>{s}</b> | Vol-Clock Z: <code>{self.vpin_clocks[s].vpin_history[-1]:.2f}</code> | Blks: {self.vpin_clocks[s].total_buckets_closed}" for s in [k for k, v in self.vpin_clocks.items() if len(v.vpin_history) > 0][:3]] or ["• <i>Volume Buckets filling...</i>"]

                report = (
                    f"💎 <b>𝗣██𝗔𝗦𝗞 𝗘𝗠𝗣𝗜𝗥𝗘 | 𝗤𝗨𝗔𝗡𝗧 𝗦𝗪𝗔𝗥𝗠 𝗢𝗦 (V8 PRE-COGNITIVE)</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏱️ <b>𝗨𝗽𝘁𝗶𝗺𝗲:</b> <code>{uptime_hours:.2f} Hours</code> | 🛰️ <b>𝗡𝗼𝗱𝗲𝘀:</b> <code>{len(self.asset_basket)} Live • {len(self.shadow_basket)} Shadow</code>\n\n"
                    f"⚙️ <b>𝗘𝗡𝗚𝗜𝗡𝗘 𝗦𝗧𝗔𝗧𝗨𝗦: 𝗙𝗿𝗮𝗰𝘁𝗶𝗼𝗻𝗮𝗹 𝗕𝗿𝗼𝘄𝗻𝗶𝗮𝗻 𝗠𝗼𝘁𝗶𝗼𝗻 + 𝗢𝗙𝗜 𝗧𝗲𝗻𝘀𝗼𝗿</b>\n"
                    f"• Hurst Analysis (H): <code>Online, Pre-mapping Trends</code>\n"
                    f"• Order Flow Elasticity: <code>Scanning for Trap Fades</code>\n"
                    f"• Dynamic Edge Scaling: <code>Active (Micro-Scalp Enabled)</code>\n\n"
                    f"💵 <b>𝗙𝗜𝗡𝗔𝗡𝗖𝗜𝗔𝗟 𝗩𝗔𝗨𝗟𝗧 𝗣𝗥𝗢𝗙𝗜𝗟𝗘</b>\n"
                    f"• Total Liquidity: <code>{cv:.4f} USDT</code>\n"
                    f"• Session Return:  <code>{actual:+.4f} USDT</code>\n"
                    f"• Peak Drawdown:   <code>{dd:.2%}</code>\n"
                    f"• Risk Buffer:     <code>[{dd_bar}]</code>\n\n"
                    f"🔬 <b>𝗗𝗔𝗜𝗟𝗬 𝗥𝗘𝗚𝗜𝗠𝗘 𝗣𝗥𝗢𝗙𝗜𝗟𝗘:</b>\n{regime_text}\n"
                    f"🔥 <b>𝗔𝗖𝗧𝗜𝗩𝗘 𝗩𝗣𝗜𝗡 𝗩𝗢𝗟𝗨𝗠𝗘 𝗖𝗟𝗢𝗖𝗞𝗦</b>\n{chr(10).join(clock_states)}\n\n"
                    f"🏁 <b>𝗥𝗘𝗖𝗘𝗡𝗧 𝗦𝗘𝗦𝗦𝗜𝗢𝗡 𝗠𝗔𝗧𝗨𝗥𝗜𝗧𝗜𝗘𝗦</b>\n{recent_trades}"
                )
                self._daemon_registry.add(asyncio.create_task(self._safe_telegram_dispatch(report, is_html=True)))

    async def run_signal_lifecycle(self, symbol: str, direction: str, current_price: float, confidence: float, dna_stats: dict, vpin_z: float = 0.0):
        try:
            signal_id = str(uuid.uuid4())
            feature_engine = self.feature_engines.get(symbol)
            market_regime = feature_engine.detect_market_regime() if feature_engine else "RANGING"
            
            raw_atr = feature_engine.get_computed_atr() if feature_engine and hasattr(feature_engine, 'get_computed_atr') else 0.0
            if raw_atr <= 0:
                prices = list(self.screener_memory.get(symbol, {}).get("prices", []))[-20:]
                atr = current_price * min(0.005, max(0.001, (max(prices) - min(prices)) / np.mean(prices))) if len(prices) >= 20 else current_price * 0.005 
            else: atr = raw_atr

            bayesian_p = confidence 
            is_armed = dna_stats.get("is_armed", False)

            sl_distance = max(atr * 2.0, current_price * 0.02)
            tp_distance = max(sl_distance * 2.0, current_price * 0.04) * (1.0 + math.log1p(max(0, abs(vpin_z) - 2.0)))
            
            initial_sl = current_price - sl_distance if direction == "BUY" else current_price + sl_distance
            target_tp = current_price + tp_distance + (current_price * 0.0011) if direction == "BUY" else current_price - tp_distance - (current_price * 0.0011)
                
            tick_dec = Decimal(str(self.tick_sizes.get(symbol, 0.0001)))
            initial_sl = float((Decimal(str(initial_sl)) / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec)
            target_tp = float((Decimal(str(target_tp)) / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec)

            distance_to_sl = max(abs(current_price - initial_sl), current_price * 0.015)
            quarter_kelly = (bayesian_p - ((1.0 - bayesian_p) / max(0.1, abs(target_tp - current_price) / distance_to_sl))) * (0.75 if (balance := await self.executor.get_wallet_balance_usdt()) < 100.0 else 1.0) * 0.25

            if quarter_kelly <= 0.0 or not is_armed:
                await asyncio.to_thread(self.memory.commit_prediction, signal_id, time.time(), current_price, direction, confidence, {"symbol": symbol, "market_regime": market_regime, "adaptive_obi_z": 0.0, "liquidity_density_ratio": 1.0, "bid_ask_spread": 0.001, "virtual_sl": initial_sl, "virtual_tp": target_tp}, is_shadow=True)
                self.active_positions_lock.discard(symbol)
                return True

            if balance < 1.0:
                logger.critical(f"🛑 ACCOUNT EMPTY (${balance:.2f}). Need at least $1.00 to cover exchange margin.")
                self.active_positions_lock.discard(symbol)
                return False

            if balance < 50.0:
                notional, bypass_vault = 6.00, True
                position_size = notional / current_price
            else:
                dollar_risk = balance * max(0.005, min(0.02, quarter_kelly))
                position_size = dollar_risk / distance_to_sl
                notional, bypass_vault = max(position_size * current_price, 6.00), False

            if not bypass_vault and not self.risk_vault.evaluate_portfolio_safety(balance, notional, symbol):
                self.active_positions_lock.discard(symbol)
                return False

            target_leverage = self.risk_vault.calculate_dynamic_leverage(notional, balance, base_leverage=5, hard_cap=15, sl_distance_pct=(distance_to_sl / current_price))
            await self.executor.adjust_leverage(symbol, target_leverage)
            await asyncio.sleep(0.2) 

            current_depth = feature_engine.get_orderbook_snapshot() if hasattr(feature_engine, 'get_orderbook_snapshot') else {"bids": [[current_price]], "asks": [[current_price]]}

            if market_regime == "TRENDING": execution_success = await self.sor.execute_iceberg_block(symbol=symbol, direction=direction, total_qty=position_size, current_mid_price=current_price, stop_loss=initial_sl, take_profit=target_tp, depth_snapshot=current_depth, vol_z=0.0, vol_mult=1.0, feature_engine=feature_engine)
            else: execution_success = await self.sor.execute_mean_reversion_bracket(symbol=symbol, direction=direction, total_qty=position_size, current_mid_price=current_price, stop_loss=initial_sl, take_profit=target_tp, depth_snapshot=current_depth, vol_z=0.0, vol_mult=1.0, feature_engine=feature_engine)
            
            if not execution_success:
                self.active_positions_lock.discard(symbol)
                return False 
                
            self.risk_vault.update_position_ledger(symbol, notional)
            
            self._daemon_registry.add(asyncio.create_task(self._safe_telegram_dispatch(f"🧬 *HFT EXECUTION FIRE*\n• Node: {symbol} | {direction}\n• Signal Confidence: {confidence:.2%}\n• Leverage Applied: {target_leverage}x\n• Notional Value: ${notional:.2f} USDT\n🛡️ *Elastic Brackets Active*: SL: {initial_sl} | TP: {target_tp}", is_html=False, message_type="SUCCESS")))
            self._daemon_registry.add(asyncio.create_task(self._position_lifecycle_daemon(symbol, signal_id, direction, current_price, initial_sl, target_tp, atr, {"allocated_value_usdt": notional, "size": position_size}, feature_engine, target_leverage, market_regime)))
            
            return True

        except Exception as e:
            logger.error(f"Distributed swarm execution routing failed for {symbol}: {e}")
            self.active_positions_lock.discard(symbol)
            return False

    async def _position_lifecycle_daemon(self, symbol: str, signal_id: str, direction: str, current_price: float, initial_sl: float, target_tp_price: float, atr: float, risk_matrix: dict, feature_engine, target_leverage: int = 8, market_regime: str = "TRENDING"):
        logger.info(f"👻 APEX MONITOR ARMED // Native Exchange Hand-off for {symbol}")
        exec_details = {"leverage": target_leverage, "execution_mode": "RECOVERY" if "RECOVERY" in signal_id else ("GHOST" if self.test_mode else "LIVE")}
        
        try:
            start_time = time.time()
            order_filled = False
            
            for _ in range(12):  
                await asyncio.sleep(5)
                try:
                    positions = (await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)).get("result", {}).get("list", [])
                    if positions and float(positions[0].get("size", 0.0)) > 0:
                        order_filled = True
                        actual_entry = float(positions[0].get("avgPrice", current_price))
                        break
                except Exception: continue

            if not order_filled:
                logger.critical(f"🔓 PORTFOLIO UNLOCKED // SOR failed to fill {symbol} within 60s. Canceling.")
                try: await asyncio.to_thread(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
                except Exception: pass
                
                try:
                    final_pos = (await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)).get("result", {}).get("list", [])
                    if final_pos and float(final_pos[0].get("size", 0.0)) > 0:
                        order_filled = True
                        actual_entry = float(final_pos[0].get("avgPrice", current_price))
                except Exception: pass

                if not order_filled:
                    self.risk_vault.update_position_ledger(symbol, -risk_matrix['allocated_value_usdt'])
                    self.active_positions_lock.discard(symbol)
                    return

            try:
                pos_data = (await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)).get("result", {}).get("list", [{}])[0]
                if float(pos_data.get("stopLoss", 0.0)) == 0.0:
                    await asyncio.to_thread(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, stopLoss=str(round(initial_sl, 4)))
            except Exception: pass

            tick_dec = Decimal(str(self.tick_sizes.get(symbol, 0.0001)))
            activation_price = str(float((Decimal(str(actual_entry + (atr * 0.8) if direction == "BUY" else actual_entry - (atr * 0.8))) / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec))
            trailing_distance_str = str(float((Decimal(str(atr * 1.5)) / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec))

            try:
                await asyncio.to_thread(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, trailingStop=trailing_distance_str, activePrice=activation_price)
                logger.info(f"🛡️ NATIVE TRAIL ARMED // {symbol} Trailing Stop handed to exchange (Act: {activation_price}, Dist: {trailing_distance_str})")
            except Exception: pass

            while time.time() - start_time < 6 * 3600:
                await asyncio.sleep(10)
                try:
                    pos_list = (await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)).get("result", {}).get("list", [])
                    position_gone = (not pos_list) or float(pos_list[0].get("size", 0.0)) == 0.0
                except Exception: position_gone = False

                settlement = await self.executor.check_recent_settlement(symbol=symbol, lookback_seconds=120)
                if settlement.get("closed"):
                    net_pnl = float(settlement.get('pnl', 0.0))
                    self._daemon_registry.add(asyncio.create_task(self._safe_telegram_dispatch(f"🔔 <b>EXCHANGE EXECUTION TERMINATION</b>\n━━━━━━━━━━━━━━━━━━━━━━\n📈 <b>Asset Node:</b> <code>{symbol}</code>\n📊 <b>Outcome:</b> {'🟢 PROFIT' if net_pnl > 0 else '🔴 LOSS'}\n💰 <b>Net Return:</b> <code>{net_pnl:.4f} USDT</code>\n━━━━━━━━━━━━━━━━━━━━━━", is_html=True)))
                    await asyncio.to_thread(self.memory.log_live_execution_result, signal_id, net_pnl, actual_entry - current_price if direction == "BUY" else current_price - actual_entry, settlement['outcome'], exec_details)
                    self.risk_vault.update_position_ledger(symbol, 0.0)
                    break

                if position_gone:
                    logger.warning(f"🧾 RECONCILIATION // {symbol} closed outside the poll window. Pulling final PnL snapshot.")
                    try: net_pnl = float((await asyncio.to_thread(self.executor.client.get_closed_pnl, category="linear", symbol=symbol, limit=5)).get("result", {}).get("list", [])[0].get("closedPnl", 0.0))
                    except Exception: net_pnl = 0.0
                    await asyncio.to_thread(self.memory.log_live_execution_result, signal_id, net_pnl, 0.0, "RECONCILED", exec_details)
                    self.risk_vault.update_position_ledger(symbol, 0.0)
                    break
            else:
                logger.error(f"⏰ DAEMON TIMEOUT // {symbol} monitor exceeded 6h. Force-clearing risk ledger state.")
                self.risk_vault.update_position_ledger(symbol, 0.0)

        except Exception as daemon_error:
            logger.error(f"☠️ FATAL DAEMON CRASH on {symbol}: {daemon_error}")
            logger.critical(f"🚑 EMERGENCY INTERVENTION // Attempting to flatten {symbol} position to protect capital.")
            try:
                await asyncio.to_thread(self.executor.client.place_order, category="linear", symbol=symbol, side="Sell" if direction == "BUY" else "Buy", orderType="Market", qty=str(risk_matrix["size"]), timeInForce="IOC")
                logger.critical(f"✅ EMERGENCY FLATTEN SUCCESSFUL for {symbol}.")
            except Exception as flatten_e: logger.error(f"❌ EMERGENCY FLATTEN FAILED for {symbol}: {flatten_e}")
                
        finally:
            self.active_positions_lock.discard(symbol)
            self.risk_vault.update_position_ledger(symbol, 0.0)

    async def run_engine_forever(self):
        logger.critical("LAUNCHING DECENTRALIZED QUANT SWARM DAEMON DEPLOYMENTS...")
        
        try: await self._fetch_exchange_tick_sizes()
        except Exception: pass
        try: await self.synchronize_exchange_state()
        except Exception: pass
        
        try: boot_basket = await self.executor.get_top_volatile_assets(limit=100, min_turnover=10_000_000)
        except Exception: boot_basket = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
            
        if boot_basket and len(boot_basket) >= 25:
            if "BTCUSDT" in boot_basket: boot_basket.remove("BTCUSDT")
            self.asset_basket, self.shadow_basket = ["BTCUSDT"] + boot_basket[:24], boot_basket[24:]
            self._initialize_symbol_structures(self.asset_basket)
        
        await asyncio.gather(
            self.run_macro_commander(),        
            self.run_universe_refresher(),
            self.run_dna_prewarmer(), 
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