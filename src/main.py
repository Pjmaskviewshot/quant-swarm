import os
import sys
import time
import math
import asyncio
import logging
import uuid
import traceback
import random
import datetime
import numpy as np
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Any
from dotenv import load_dotenv

# Core & Feature Modules
from core.memory import MemoryBank
from core.fsm import SystemStateMachine, TradingState
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
    """1D Kalman Filter & Hawkes Process Approximation for Async Event Loops"""
    def __init__(self):
        # 1D Kalman Filter State
        self.k_est = 0.0
        self.k_err = 1.0
        self.q = 0.01  # Process noise
        self.r = 0.1   # Measurement noise
        
        # Hawkes Excitement State
        self.trade_timestamps = []
        self.decay_factor = 0.5  # Controls how fast old volume "forgets"

    def kalman_update(self, measurement: float) -> float:
        """A lightning-fast 1D Kalman filter to find the true price trend through noise."""
        if self.k_est == 0.0:
            self.k_est = measurement
            return measurement
            
        # Prediction Update
        p_pred = self.k_err + self.q
        # Measurement Update
        kalman_gain = p_pred / (p_pred + self.r)
        self.k_est = self.k_est + kalman_gain * (measurement - self.k_est)
        self.k_err = (1.0 - kalman_gain) * p_pred
        return self.k_est

    def hawkes_cluster_score(self, current_time: float, volume: float) -> float:
        """Approximates a Hawkes process by calculating time-decayed volume clustering."""
        self.trade_timestamps.append((current_time, volume))
        
        # Keep only last 60 seconds of data to prevent memory leaks
        self.trade_timestamps = [t for t in self.trade_timestamps if current_time - t[0] < 60]
        
        excitement = 0.0
        for t_time, t_vol in self.trade_timestamps:
            time_diff = current_time - t_time
            excitement += t_vol * math.exp(-self.decay_factor * time_diff)
            
        return excitement


class DistributedQuantEngine:
    def __init__(self):
        load_dotenv()
        
        # 🚀 SYSTEM CONFIGURATION
        self.test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
        self.historical_win_rate = float(os.getenv("HISTORICAL_WIN_RATE", "0.58"))
        self.historical_win_loss_ratio = float(os.getenv("HISTORICAL_WIN_LOSS_RATIO", "1.65"))
        self.min_horizon_floor = int(os.getenv("MIN_HORIZON_FLOOR", "30"))
        
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
        self.fsm = SystemStateMachine(accuracy_threshold=0.60, warmup_epochs=self.min_horizon_floor)
        self.risk_vault = InstitutionalRiskVault(max_drawdown_pct=0.25, max_single_position_risk_pct=0.15)
        
        self.feature_engines: Dict[str, AdaptiveFeatureEngine] = {
            s: AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600) for s in self.asset_basket
        }
        
        # 🚀 INIT FAST MATH ENGINES
        self.math_engines: Dict[str, FastMathEngine] = {s: FastMathEngine() for s in self.asset_basket}
        
        self.macro_regimes: Dict[str, str] = {s: "HOLD" for s in self.asset_basket}
        self.macro_confidences: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.current_atrs: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.last_execution_timestamps: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.screener_memory: Dict[str, Dict[str, Any]] = {
            s: {"prices": [], "macro_prices": [], "volumes": [], "last_update_time": 0.0} for s in self.asset_basket
        }
        
        self.screener_metrics: Dict[str, Dict[str, float]] = {
            s: {"vol_mult": 1.0, "vol_z": 0.0, "smoothed_price": 0.0, "hawkes_score": 0.0} for s in self.asset_basket
        }
        
        self.pending_macro_payloads: Dict[str, dict] = {}
        self.active_workers: Dict[str, asyncio.Task] = {}
        self.active_positions_lock = set()
        self.tick_sizes: Dict[str, float] = {}
        
        self.global_macro_news_cache: str = "No significant macro shifts detected."
        self.last_news_fetch: float = 0.0

        self.global_state_cache = {
            "rolling_accuracy": 0.50,
            "total_resolved": 0,
            "dynamic_window": self.min_horizon_floor,
            "last_updated": 0.0
        }

        nv_keys = [os.getenv("NVIDIA_API_KEY_1"), os.getenv("NVIDIA_API_KEY_2")]
        self.ai_router = ResilientAIRouter(nv_keys=nv_keys, deepseek_key=os.getenv("DEEPSEEK_API_KEY"))
        self.macro_data_feed = AsynchronousDataFeed(finnhub_key=os.getenv("FINNHUB_API_KEY"))
        self.telegram = AsyncTelegramReporter(token=os.getenv("TELEGRAM_BOT_TOKEN"), chat_id=os.getenv("TELEGRAM_CHAT_ID"))
        
        self.executor = BybitUnifiedExecutor(api_key=os.getenv("BYBIT_API_KEY"), api_secret=os.getenv("BYBIT_API_SECRET"), testnet=False)
        self.sor = SmartOrderRouter(executor=self.executor, max_slippage_pct=0.005)

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
            pos_response = await asyncio.to_thread(
                self.executor.client.get_positions,
                category="linear",
                settleCoin="USDT"
            )
            
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
                
                asyncio.create_task(self._position_lifecycle_daemon(
                    symbol, signal_id, direction, entry_price, current_sl, target_tp, atr, risk_matrix, feature_engine, 8, "RANGING"
                ))
                
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

    def compute_dynamic_memory_window(self, vol_mult: float) -> int:
        return self.min_horizon_floor

    def calculate_adaptive_regime_parameters(self, market_regime: str, metrics: dict, confidence: float = 0.0) -> dict:
        vol_mult = float(metrics.get("vol_mult", 1.0))
        dynamic_z_threshold = 2.5 - (confidence * 1.0)
        liquidity_buffer = 1.0 / math.sqrt(max(0.10, vol_mult))
        
        optimized = {
            "cooldown_period": 600.0,
            "z_score_threshold": dynamic_z_threshold, 
            "position_scaling": 1.0,
            "sl_multiplier": 2.0 + (liquidity_buffer * 0.5), 
            "tp_multiplier": max(1.0, 2.0 - (liquidity_buffer * 0.2)),
            "execution_verdict": True
        }

        if market_regime == "TRENDING":
            optimized["cooldown_period"] = 300.0  
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
                logger.debug(f"Silent background fail on news fetch: {e}")

    async def run_macro_commander(self):
        logger.info("🧠 MACRO COMMANDER ONLINE. Waiting for workers to gather data...")
        while True:
            await asyncio.sleep(60) 
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
                        self.macro_confidences[symbol] = 0.0
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
                                    self.macro_confidences[symbol] = data.get("confidence", 0.0)
                                    logger.info(f"🔄 COMMANDER SYNCED // Target: {symbol} | Bias: {self.macro_regimes[symbol]} | Conf: {self.macro_confidences[symbol]:.2%}")
                                    
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
                    if task and task.done() and task.exception():
                        logger.error(f"☠️ WATCHDOG FATAL ALERT: {symbol} worker died from unhandled exception.")
                    else:
                        logger.error(f"☠️ WATCHDOG ALERT: {symbol} worker thread vanished or stalled silently.")
                        
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
                
                if len(history) < 60:
                    klines = await asyncio.to_thread(
                        self.executor.client.get_kline,
                        category="linear", symbol=symbol, interval="15", limit=60
                    )
                    data = klines.get("result", {}).get("list", [])
                    if data:
                        closes = [float(k[4]) for k in data][::-1]
                        volumes = [float(k[5]) for k in data][::-1]
                        self.screener_memory[symbol] = {
                            "prices": closes, 
                            "volumes": volumes,
                            "macro_prices": closes.copy(),
                            "last_update_time": time.time()
                        }
                        history = closes
                
                current_price = history[-1] if history else 0.0
                
                if current_price == 0.0:
                    klines = await asyncio.to_thread(
                        self.executor.client.get_kline,
                        category="linear", symbol=symbol, interval="15", limit=1
                    )
                    current_price = float(klines.get("result", {}).get("list", [[0.0, 0.0, 0.0, 0.0, 0.0]])[0][4])

                feature_engine = self.feature_engines.get(symbol)
                self.current_atrs[symbol] = current_price * 0.0125
                metrics = self.screener_metrics.get(symbol, {"vol_mult": 1.0, "vol_z": 0.0})
                rolling_acc = self.global_state_cache.get("rolling_accuracy", 0.50)
                
                self.pending_macro_payloads[symbol] = {
                    "price": current_price,
                    "atr_volatility": self.current_atrs[symbol],
                    "volume_multiplier": round(metrics.get("vol_mult", 1.0), 2),
                    "volatility_z_score": round(metrics.get("vol_z", 0.0), 2),
                    "rolling_system_accuracy": f"{rolling_acc:.2%}" 
                }
            except Exception as e:
                logger.error(f"⚠️ DATA WORKER ERROR: Exception on {symbol} loop: {e}")
                
            await asyncio.sleep(random.uniform(45.0, 75.0))

    async def handle_incoming_orderbook_tick(self, depth_data: Dict[str, Any]):
        symbol = depth_data.get("s")
        if symbol not in self.asset_basket:
            return

        bids = depth_data.get("b", [])
        asks = depth_data.get("a", [])
        features = self.feature_engines[symbol].push_orderbook_tick(bids, asks)
        
        if not features.get("valid"):
            return

        z_obi = features["adaptive_obi_z"]
        mid_price = features["mid_price"]
        market_regime = features.get("market_regime", "RANGING")
        real_spread = features.get("bid_ask_spread", mid_price * 0.0005)

        metrics = self.screener_metrics.get(symbol, {"vol_mult": 1.0, "vol_z": 0.0})
        
        live_confidence = self.macro_confidences.get(symbol, 0.0)
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
        if len(history) < 20:
            return 
            
        prices_array = np.array(history)
        local_mean = np.mean(prices_array)
        local_std = np.std(prices_array) if np.std(prices_array) > 0 else 1e-6
        price_z_score = (mid_price - local_mean) / local_std
        
        raw_vol_z = metrics.get("vol_z", 0.0)
        vol_z_abs = abs(raw_vol_z)
        
        spread_pct = real_spread / mid_price
        
        dynamic_max_spread = 0.0015 * (1.0 + math.log1p(vol_z_abs))
        
        if spread_pct > dynamic_max_spread:
            return 
            
        trade_direction = None
        has_pure_edge = False
        is_golden_setup = False
        
        # 🚀 APEX UPGRADE 2: Dynamic Time-Warping Trend Matrix
        hawkes_score = metrics.get("hawkes_score", 0.0)
        valid_hawkes = [m.get("hawkes_score", 0.0) for m in self.screener_metrics.values() if "hawkes_score" in m]
        avg_hawkes = np.mean(valid_hawkes) if valid_hawkes else 0.1
        hawkes_ratio = hawkes_score / avg_hawkes if avg_hawkes > 0 else 0.0

        if market_regime == "TRENDING":
            # 1. Existing Logic: Catching the Pullback
            if vol_mult >= 1.2 and vol_z_abs >= 1.5:
                if z_obi <= -effective_z_threshold and adaptive_mieg_long and price_z_score <= -0.5:
                    has_pure_edge = True
                    trade_direction = "BUY"
                elif z_obi >= effective_z_threshold and adaptive_mieg_short and price_z_score >= 0.5:
                    has_pure_edge = True
                    trade_direction = "SELL"
                    
            # 2. Advanced Trend Matrix: Vector Alignment
            price_vector = np.clip(price_z_score / 2.0, -1.0, 1.0) 
            obi_vector = np.clip(z_obi / max(1.0, effective_z_threshold), -1.0, 1.0)
            kinetic_vector = np.clip(math.log1p(vol_mult) * max(0.0, hawkes_ratio - 1.0), 0.0, 1.5)
            
            trend_force = 0.0
            if price_vector > 0 and obi_vector > 0:
                trend_force = ((price_vector + obi_vector) / 2.0) * (1.0 + kinetic_vector)
            elif price_vector < 0 and obi_vector < 0:
                trend_force = ((price_vector + obi_vector) / 2.0) * (1.0 + kinetic_vector)
                
            activation_barrier = 1.25 
            
            if trend_force >= activation_barrier and not has_pure_edge:
                has_pure_edge = True
                trade_direction = "BUY"
                logger.critical(f"🚀 [VECTOR ALIGNMENT] {symbol} Upward breakout confirmed (Force: {trend_force:.2f}).")
            elif trend_force <= -activation_barrier and not has_pure_edge:
                has_pure_edge = True
                trade_direction = "SELL"
                logger.critical(f"🚀 [VECTOR ALIGNMENT] {symbol} Downward breakdown confirmed (Force: {trend_force:.2f}).")

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

        is_active = self.fsm.current_state in [TradingState.ACTIVE_TRADING, TradingState.ACTIVE_MEAN_REVERSION]
        
        if not has_pure_edge:
            return

        if not self.test_mode and is_active:
            if trade_direction == "BUY" and regime == "SELL" and not is_golden_setup:
                return 
            if trade_direction == "SELL" and regime == "BUY" and not is_golden_setup:
                return 

        if symbol != "BTCUSDT" and trade_direction:
            btc_history = self.screener_memory.get("BTCUSDT", {}).get("prices", [])
            
            if len(btc_history) >= 30:
                btc_array = np.array(btc_history)
                btc_mean = np.mean(btc_array)
                btc_std = np.std(btc_array) + 1e-6
                btc_z_score = (btc_array[-1] - btc_mean) / btc_std
                btc_pct_change = (btc_array[-1] - btc_mean) / btc_mean
                
                macro_bias = self.macro_regimes.get("BTCUSDT", "HOLD")
                btc_metrics = self.screener_metrics.get("BTCUSDT", {})
                btc_vol_z = btc_metrics.get("vol_z", 0.0)
                
                # 🚀 Fix: Ensure a real percentage drop accompanies the Z-Score to prevent 60-min myopia
                is_systemic_bull_squeeze = (btc_z_score >= 2.0 and btc_pct_change > 0.015) or btc_vol_z >= 2.5
                is_systemic_bear_squeeze = (btc_z_score <= -2.0 and btc_pct_change < -0.015) or btc_vol_z <= -2.5
                
                if trade_direction == "SELL" and is_systemic_bull_squeeze:
                    logger.critical(f"🛡️ MACRO SHIELD // BTC systemic short-squeeze detected (Z: {btc_z_score:.2f}). {symbol} short blocked.")
                    return 
                    
                if trade_direction == "BUY" and is_systemic_bear_squeeze:
                    logger.critical(f"🛡️ MACRO SHIELD // BTC systemic flash-crash detected (Z: {btc_z_score:.2f}). {symbol} dip-buy blocked.")
                    return
                    
                alt_history = self.screener_memory.get(symbol, {}).get("prices", [])
                if len(alt_history) >= 30:
                    alt_array = np.array(alt_history[-30:])
                    btc_slice = btc_array[-30:]
                    
                    btc_returns = np.diff(np.log(btc_slice))
                    alt_returns = np.diff(np.log(alt_array))
                    
                    if np.std(btc_returns) > 0 and np.std(alt_returns) > 0:
                        correlation = np.corrcoef(btc_returns, alt_returns)[0, 1]
                        if not math.isnan(correlation) and correlation > 0.45:
                            if trade_direction == "BUY" and btc_z_score <= -1.0:
                                return
                            if trade_direction == "SELL" and btc_z_score >= 1.0:
                                return

        self.last_execution_timestamps[symbol] = current_time
        mode_label = "🔥 LIVE" if is_active else "👻 GHOST"
        logger.critical(f"{mode_label} PURE EDGE DETECTED // Node: {symbol} | Regime: {market_regime} | Z: {price_z_score:.2f}")
        
        asyncio.create_task(self.run_signal_lifecycle(
            symbol, trade_direction, mid_price, optimization, real_spread, vol_z_abs, is_golden_setup
        ))

    async def handle_incoming_basket_screener_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if not symbol or symbol not in self.asset_basket:
            return
            
        if symbol not in self.screener_memory:
            self.screener_memory[symbol] = {"prices": [], "macro_prices": [], "volumes": [], "last_update_time": 0.0}
            
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
        
        self.feature_engines[symbol].update_multi_timeframe_candle(
            timeframe=interval,
            open_p=c_open,
            high_p=c_high,
            low_p=c_low,
            close_p=c_close,
            volume=c_vol
        )
        
        if symbol not in self.screener_memory:
            self.screener_memory[symbol] = {"prices": [], "macro_prices": [], "volumes": [], "last_update_time": 0.0}
            
        history = self.screener_memory[symbol]

        # 🚀 APEX UPGRADE: Continuous Exponential Moving Volatility (CEMV)
        if str(interval) == "15":
             if "macro_prices" not in history:
                 history["macro_prices"] = []
             
             macro_hist = history["macro_prices"]
             macro_hist.append(c_close)
             if len(macro_hist) > 48: 
                 macro_hist.pop(0)
                 
        if str(interval) == "1":
            history["volumes"].append(c_vol)
            history["prices"].append(c_close)
            
            if len(history["volumes"]) > 60:
                history["volumes"].pop(0)
                history["prices"].pop(0)
                
            if len(history["volumes"]) >= 15:
                vol_array = np.array(history["volumes"])
                price_array = np.array(history["prices"])
                
                # 1. Advanced Volume Multiplier (Exponential Decay)
                weights = np.exp(np.linspace(-1., 0., len(vol_array[:-1])))
                weights /= weights.sum()
                ewm_vol = np.sum(vol_array[:-1] * weights)
                ewm_vol = max(ewm_vol, 1.0)
                vol_mult = c_vol / ewm_vol
                
                # 2. Continuous Volatility Z-Score (CEMV)
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
                    
                # 3. Macro Baseline Detachment (True Flash Crash Detector)
                macro_hist = history.get("macro_prices", [])
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
            await asyncio.sleep(1800) 
            logger.info("🌍 FAST SATELLITE ROTATION INITIATED. Querying Bybit...")
            
            await self._fetch_exchange_tick_sizes()
            full_market = await self.executor.get_top_volatile_assets(limit=100, min_turnover=10_000_000)
            
            if len(full_market) < 25:
                continue
                
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
            
            for s in self.asset_basket:
                new_feature_engines[s] = self.feature_engines.get(s, AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600))
                new_math_engines[s] = self.math_engines.get(s, FastMathEngine())
                new_screener_memory[s] = self.screener_memory.get(s, {"prices": [], "macro_prices": [], "volumes": [], "last_update_time": 0.0})
                new_macro_regimes[s] = self.macro_regimes.get(s, "HOLD")
                new_macro_confidences[s] = self.macro_confidences.get(s, 0.0)
                new_current_atrs[s] = self.current_atrs.get(s, 0.0)
                new_last_execs[s] = self.last_execution_timestamps.get(s, 0.0)
                new_screener_metrics[s] = self.screener_metrics.get(s, {"vol_mult": 1.0, "vol_z": 0.0, "smoothed_price": 0.0, "hawkes_score": 0.0})
                
            self.feature_engines = new_feature_engines
            self.math_engines = new_math_engines
            self.screener_memory = new_screener_memory
            self.macro_regimes = new_macro_regimes
            self.macro_confidences = new_macro_confidences
            self.current_atrs = new_current_atrs
            self.last_execution_timestamps = new_last_execs
            self.screener_metrics = new_screener_metrics
            
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
                    
            for new_symbol in self.asset_basket:
                if new_symbol not in self.active_workers:
                    self.active_workers[new_symbol] = asyncio.create_task(self._asset_data_gatherer_lifecycle(new_symbol))
            
            self.stream_restart_event.set()

    def _detect_shadow_regime(self, closes: np.ndarray) -> str:
        if len(closes) < 30:
            return "RANGING"
        directional_change = abs(closes[-1] - closes[0])
        absolute_changes = np.sum(np.abs(np.diff(closes)))
        er = directional_change / absolute_changes if absolute_changes > 0 else 0.0
        
        sma = np.mean(closes)
        std_dev = np.std(closes)
        bb_width = (4 * std_dev) / sma if sma > 0 else 0.0
        
        if er >= 0.35 and bb_width >= 0.004:
            return "TRENDING"
        return "RANGING"

    async def run_shadow_swarm_scanner(self):
        logger.critical("🦇 SHADOW SWARM ONLINE. Hunting for pure data across extended universe...")
        while True:
            await asyncio.sleep(120) 
            if not self.shadow_basket:
                continue
                
            BATCH_SIZE = 10
            for i in range(0, len(self.shadow_basket), BATCH_SIZE):
                batch = self.shadow_basket[i:i+BATCH_SIZE]
                for symbol in batch:
                    try:
                        if symbol in self.shadow_cooldown and time.time() - self.shadow_cooldown[symbol] < 300:
                            continue
                            
                        klines = await asyncio.to_thread(
                            self.executor.client.get_kline,
                            category="linear", symbol=symbol, interval="15", limit=60
                        )
                        
                        data = klines.get("result", {}).get("list", [])
                        if len(data) < 30:
                            continue

                        closes = np.array([float(k[4]) for k in data])[::-1]
                        volumes = np.array([float(k[5]) for k in data])[::-1]
                        
                        current_price = closes[-1]
                        if current_price <= 0.01:
                            continue
                        
                        current_vol = volumes[-1]
                        avg_vol = np.mean(volumes[:-1]) if len(volumes) > 1 else 1.0
                        vol_mult = current_vol / avg_vol if avg_vol > 0 else 1.0
                        
                        returns = np.diff(np.log(closes))
                        mean_return = np.mean(returns) if len(returns) > 0 else 0.0
                        std_return = np.std(returns) if len(returns) > 0 else 1e-6
                        vol_z = abs((returns[-1] - mean_return) / (std_return + 1e-6))
                        
                        if (vol_z >= 2.2 and vol_mult >= 0.8) or (vol_mult >= 1.5 and vol_z >= 1.4):
                            direction = "BUY" if returns[-1] < 0 else "SELL"
                            market_regime = self._detect_shadow_regime(closes)
                            
                            features_dict = {
                                "symbol": symbol,
                                "market_regime": market_regime,
                                "adaptive_obi_z": vol_z,
                                "liquidity_density_ratio": vol_mult,
                                "bid_ask_spread": 0.0
                            }
                            
                            self.memory.commit_prediction(
                                str(uuid.uuid4()),
                                time.time(),
                                current_price,
                                direction,
                                0.0,
                                features_dict,
                                is_shadow=True
                            )
                            self.shadow_cooldown[symbol] = time.time()
                    except Exception as e:
                        logger.error(f"Shadow scanner error for {symbol}: {e}")
                    await asyncio.sleep(1.5) 
                await asyncio.sleep(2) 

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
                        current_prices = {sym: self.screener_memory[sym]["prices"] for sym in valid_assets}
                        for s, data in self.shadow_cooldown.items():
                            if s not in current_prices:
                                current_prices[s] = self.screener_memory.get(s, {}).get("prices", [0.0])
                        
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
                avg_dynamic_window = self.min_horizon_floor
                accuracy, pool_size = self.memory.compute_rolling_accuracy(
                    window_size=avg_dynamic_window, core_basket=self.asset_basket
                )
                
                self.global_state_cache["rolling_accuracy"] = accuracy
                self.global_state_cache["total_resolved"] = pool_size
                self.global_state_cache["dynamic_window"] = avg_dynamic_window
                self.global_state_cache["last_updated"] = time.time()
                self.fsm.warmup_epochs = avg_dynamic_window
                
                if accuracy < 0.60:
                    if self.fsm.current_state != TradingState.EMERGENCY_LOCK:
                        self.fsm.current_state = TradingState.CALIBRATING
                        logger.critical(f"📉 EDGE DECAY OVERRIDE: Accuracy {accuracy:.2%} is below 60%. Engine locked to CALIBRATING.")
                elif self.fsm.current_state == TradingState.EMERGENCY_LOCK and accuracy >= 0.70:
                    self.fsm.current_state = TradingState.CALIBRATING
                    logger.info("🔓 EMERGENCY LOCK LIFTED. System recovering via Calibrating state.")
                else:
                    self.fsm.process_state_transition(accuracy, pool_size, "RANGING") 

            if loop_counter % 30 == 0:
                state = self.fsm.current_state.value
                current_vault_balance = await self.executor.get_wallet_balance_usdt()
                
                acc = self.global_state_cache.get("rolling_accuracy", 0.0)
                dyn_win = self.global_state_cache.get("dynamic_window", self.min_horizon_floor)
                pool = self.global_state_cache.get("total_resolved", 0)
                
                if "wallet_baseline" not in self.global_state_cache:
                    self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01)
                
                baseline = self.global_state_cache["wallet_baseline"]
                
                if current_vault_balance > baseline:
                    self.global_state_cache["wallet_baseline"] = current_vault_balance
                    baseline = current_vault_balance
                elif current_vault_balance < (baseline * 0.75): 
                    logger.critical(f"💸 MANUAL WITHDRAWAL DETECTED. Auto-Calibrating FSM Baseline.")
                    self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01)
                    baseline = self.global_state_cache["wallet_baseline"]
                    if hasattr(self.risk_vault, 'peak_balance'):
                        self.risk_vault.peak_balance = baseline
                    
                drawdown_pct = max(0.0, (baseline - current_vault_balance) / baseline)
                
                bar_length = 10
                filled_blocks = min(bar_length, int(drawdown_pct * bar_length))
                drawdown_bar = "🟢" * (bar_length - filled_blocks) + "🔴" * filled_blocks

                try:
                    now_utc = datetime.datetime.now(datetime.timezone.utc)
                    today_start_iso = now_utc.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                    
                    response = self.memory.supabase.table("quantitative_ledger")\
                        .select("market_regime, net_pnl, symbol, predicted_direction, actual_outcome")\
                        .eq("resolved", True)\
                        .eq("is_shadow", False)\
                        .gte("timestamp", today_start_iso)\
                        .order("timestamp", desc=False)\
                        .execute()
                    
                    data = response.data if response else []
                    net_pnl = sum(float(row.get("net_pnl", 0.0)) for row in data)
                    
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
                        regime_breakdown_text += f"• {icon} <b>{regime}:</b> <code>{stats['count']} trades</code> | <code>{stats['pnl']:+.4f} USDT</code>\n"
                        
                    if not regime_breakdown_text:
                        regime_breakdown_text = "• <i>No resolved metrics recorded today yet.</i>\n"
                        
                    recent_trades_text = ""
                    if data:
                        sorted_data = data[-5:]  
                        for t in sorted_data:
                            pnl_val = float(t.get('net_pnl', 0))
                            if pnl_val == 0.0:
                                outcome_icon = "👻"
                            else:
                                outcome_icon = "✅" if t.get("actual_outcome") == "WIN" else "🔴"
                            
                            recent_trades_text += f"{outcome_icon} {t.get('symbol')} | {t.get('predicted_direction')} | PnL: {pnl_val:+.4f}\n"
                    else:
                        recent_trades_text = "• <i>Waiting for first maturity cycle...</i>\n"

                except Exception as db_err:
                    logger.error(f"Failed to compile Supabase data for Telegram report: {db_err}")
                    net_pnl = 0.0
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
                            f"Vol: <code>{metrics.get('vol_mult', 1.0):.2f}x</code> | Bias: <code>{bias}</code>"
                        )
                except Exception:
                    diagnostic_nodes = ["• <i>Diagnostic matrix initializing...</i>"]

                diagnostic_block = "\n".join(diagnostic_nodes)

                report = (
                    f"📊 <b>PJMASK EMPIRE ADVANCED QUANT PULSE</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏱ <b>Engine Run Uptime:</b> <code>{uptime_hours:.2f} Hours</code>\n"
                    f"🎛 <b>FSM State Gear:</b> <code>{state}</code>\n"
                    f"🎯 <b>Rolling Edge Accuracy:</b> <code>{acc:.2%}</code>\n"
                    f"📏 <b>Active Memory Horizon:</b> <code>{dyn_win} Trades Required</code>\n"
                    f"🏊‍♂️ <b>Database Validation Pool:</b> <code>{pool} Resolved</code>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"💳 <b>Net Wallet Liquidity:</b> <code>{current_vault_balance:.4f} USDT</code>\n"
                    f"📈 <b>24H Calendar Net Return:</b> <code>{net_pnl:+.4f} USDT</code>\n"
                    f"📉 <b>Drawdown Profile Status:</b> <code>{drawdown_pct:.2%}</code>\n"
                    f"🎚 <b>Risk Horizon Bar:</b>\n<code>[{drawdown_bar}]</code>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔬 <b>DAILY REGIME PROFILE:</b>\n"
                    f"{regime_breakdown_text}"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔥 <b>LIVE HIGHEST-MOMENTUM MOVERS:</b>\n"
                    f"{diagnostic_block}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🏁 <b>RECENT SESSION RESOLUTIONS:</b>\n"
                    f"{recent_trades_text}"
                    f"📡 <b>Nodes:</b> <code>{len(self.asset_basket)} Live</code> | <code>{len(self.shadow_basket)} Shadow</code>\n"
                    f"🧠 <b>AI Core:</b> <code>Hardened NV-KIMI/DeepSeek/Groq Cascade</code>"
                )
                asyncio.create_task(self.telegram.send_html_report(report))

    def calculate_initial_bracket(self, entry_price: float, atr: float, side: str, vol_z: float = 0.0, confidence: float = 0.0, tick_size: float = 0.0001):
        vol_z_abs = abs(vol_z)
        dynamic_sl_mult = 1.2 + (math.log1p(vol_z_abs) * 0.4) 
        
        dynamic_rr_ratio = 2.0 + (confidence * 1.5)
        dynamic_tp_mult = dynamic_sl_mult * dynamic_rr_ratio
        
        fee_offset = entry_price * (0.00055 * 2)
        
        if side.upper() == "BUY":
            initial_sl = entry_price - (dynamic_sl_mult * atr)
            target_tp = entry_price + (dynamic_tp_mult * atr) + fee_offset
        else:  
            initial_sl = entry_price + (dynamic_sl_mult * atr)
            target_tp = entry_price - (dynamic_tp_mult * atr) - fee_offset
            
        tick_dec = Decimal(str(tick_size))
        return float((Decimal(str(initial_sl)) / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec), \
               float((Decimal(str(target_tp)) / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec)

    async def run_signal_lifecycle(self, symbol: str, direction: str, current_price: float, optimization: dict = None, real_spread: float = 0.0, vol_z_abs: float = 0.0, is_golden_setup: bool = False):
        if symbol in self.active_positions_lock:
            logger.warning(f"🔒 Guard execution bypass [{symbol}]: Signal ignored to prevent position stacking collision.")
            return False
            
        self.active_positions_lock.add(symbol)
        
        try:
            signal_id = str(uuid.uuid4())
            confidence = self.macro_confidences.get(symbol, 0.0)
            
            feature_engine = self.feature_engines.get(symbol)
            market_regime = feature_engine.detect_market_regime() if feature_engine else "RANGING"
            
            raw_atr = feature_engine.get_computed_atr() if feature_engine and hasattr(feature_engine, 'get_computed_atr') else current_price * 0.015
            atr = max(raw_atr, current_price * 0.0125) 
            self.current_atrs[symbol] = atr

            metrics = self.screener_metrics.get(symbol, {})
            vol_mult = metrics.get("vol_mult", 1.0)
            
            spread_pct = real_spread / current_price if current_price > 0 else 0.0
            
            cached_balance = self.global_state_cache.get("wallet_baseline", 10.0)
            estimated_trade_value = cached_balance * 0.15 
            capital_scale = math.tanh(estimated_trade_value / 1000.0)
            
            conviction_bonus = math.expm1(max(0.0, confidence)) 
            
            base_floor = 0.05 + (0.15 * capital_scale)
            spread_weight = (50.0 * (1.0 + capital_scale)) - (conviction_bonus * 5.0)
            
            dynamic_vol_floor = max(0.02, base_floor + (spread_pct * max(10.0, spread_weight)))
            
            if vol_mult < dynamic_vol_floor:
                logger.info(f"⚖️ DYNAMIC LIQUIDITY WALL // Node: {symbol} | Vol: {vol_mult:.2f}x | Req Floor: {dynamic_vol_floor:.2f}x. Trade skipped.")
                self.active_positions_lock.discard(symbol)
                return False

            expected_profit = (current_price * 0.02) 
            safe_spread = max(real_spread, current_price * 0.0001) 
            slippage_penalty = safe_spread * (1.0 / math.sqrt(max(0.10, vol_mult)))
            total_friction = safe_spread + slippage_penalty
            
            if expected_profit < total_friction:
                logger.info(f"⚖️ FRICTION WALL // Node: {symbol} | Target too small to survive exchange fees & slippage. Signal shredded.")
                self.active_positions_lock.discard(symbol)
                return False
            
            rolling_acc = self.global_state_cache["rolling_accuracy"]
            
            is_whitelisted_state = self.fsm.current_state in [TradingState.ACTIVE_TRADING, TradingState.ACTIVE_MEAN_REVERSION]
            has_institutional_edge = rolling_acc >= 0.60
            
            if not self.test_mode and (not is_whitelisted_state or not has_institutional_edge):
                logger.critical(f"👻 [SHIELD ACTIVE] FSM State: {self.fsm.current_state.value} | Accuracy: {rolling_acc:.2%}")
                logger.critical(f"👻 [SHIELD ACTIVE] Routing to Ghost Simulation -> Node: {symbol}")
                
                tick_size = self.tick_sizes.get(symbol, 0.0001) 
                initial_sl, target_tp = self.calculate_initial_bracket(current_price, atr, direction, vol_z_abs, confidence, tick_size)
                features_dict = {
                    "symbol": symbol, "market_regime": market_regime, "adaptive_obi_z": 0.0, 
                    "liquidity_density_ratio": vol_mult, "bid_ask_spread": real_spread,
                    "virtual_sl": initial_sl, "virtual_tp": target_tp    
                }
                self.memory.commit_prediction(signal_id, time.time(), current_price, direction, confidence, features_dict, is_shadow=False)
                
                self.active_positions_lock.discard(symbol)
                return True
            
            if self.test_mode:
                self.active_positions_lock.discard(symbol)
                return True 

            balance = await self.executor.get_wallet_balance_usdt()
            
            history = self.screener_memory.get(symbol, {}).get("prices", [])
            prices_array = np.array(history)
            local_mean = np.mean(prices_array) if len(prices_array) > 0 else current_price
            local_std = np.std(prices_array) if len(prices_array) > 0 and np.std(prices_array) > 0 else 1e-6
            price_z_score = (current_price - local_mean) / local_std

            if market_regime == "TRENDING":
                if direction == "SELL" and price_z_score > 0.5: 
                    logger.critical(f"🛡️ REGIME-LOCK // Trending market. Short blocked on {symbol}.")
                    self.active_positions_lock.discard(symbol)
                    return False
                if direction == "BUY" and price_z_score < -0.5: 
                    logger.critical(f"🛡️ REGIME-LOCK // Trending market. Long blocked on {symbol}.")
                    self.active_positions_lock.discard(symbol)
                    return False
                    
            smoothed_price = metrics.get("smoothed_price", current_price)
            kalman_diff_pct = (current_price - smoothed_price) / current_price if current_price > 0 else 0.0

            if direction == "BUY" and kalman_diff_pct < -0.001:
                logger.warning(f"🛡️ KALMAN SHIELD // {symbol} raw price spiked, but Kalman trend is down. Fakeout avoided.")
                self.active_positions_lock.discard(symbol)
                return False
            if direction == "SELL" and kalman_diff_pct > 0.001:
                logger.warning(f"🛡️ KALMAN SHIELD // {symbol} raw price dropped, but Kalman trend is up. Fakeout avoided.")
                self.active_positions_lock.discard(symbol)
                return False

            hawkes_score = metrics.get("hawkes_score", 0.0)
            valid_hawkes = [m.get("hawkes_score", 0.0) for m in self.screener_metrics.values() if "hawkes_score" in m]
            avg_hawkes = np.mean(valid_hawkes) if valid_hawkes else 0.0
            
            if market_regime == "TRENDING" and hawkes_score < (avg_hawkes * 1.2):
                logger.warning(f"🛡️ HAWKES SHIELD // {symbol} lacks institutional volume clustering. Momentum unverified.")
                self.active_positions_lock.discard(symbol)
                return False
            
            sigmoid_risk_pct = 0.01 + (0.03 / (1.0 + math.exp(0.04 * (balance - 40.0))))
            dollar_risk = balance * sigmoid_risk_pct

            tick_size = self.tick_sizes.get(symbol, 0.0001) 
            initial_sl, target_tp = self.calculate_initial_bracket(
                current_price, atr, direction, vol_z_abs, confidence, tick_size
            )
            
            features_dict = {
                "symbol": symbol, "market_regime": market_regime, "adaptive_obi_z": 0.0, 
                "liquidity_density_ratio": vol_mult, "bid_ask_spread": real_spread,
                "virtual_sl": initial_sl, "virtual_tp": target_tp    
            }
            self.memory.commit_prediction(signal_id, time.time(), current_price, direction, confidence, features_dict, is_shadow=False)

            active_live_trades = len(self.active_positions_lock)
            max_allowed_trades = 2 if rolling_acc < 0.70 else 5
            
            if not self.test_mode and active_live_trades >= max_allowed_trades:
                logger.warning(f"🛑 CONCURRENCY CAPPED // {active_live_trades} active trades. Pausing {symbol} to prevent swarm over-exposure.")
                self.active_positions_lock.discard(symbol)
                return False

            sl_distance = abs(current_price - initial_sl)
            if sl_distance <= 0: sl_distance = current_price * 0.015
            
            position_size = (dollar_risk / sl_distance)
            notional = position_size * current_price
            
            if notional < 5.50:
                position_size = 5.50 / current_price
                notional = 5.50
                
            if (notional * (sl_distance / current_price)) > (balance * 0.12):
                logger.warning(f"⚖️ FATAL RISK WALL // {symbol} forces toxic exposure on micro-balance. Blocked.")
                self.active_positions_lock.discard(symbol)
                return False
            
            max_allowed_leverage = 20 if vol_z_abs < 1.5 else (10 if vol_z_abs < 2.5 else 5)
            target_leverage = int(min(max(1, math.ceil(notional / (balance * 0.12))), max_allowed_leverage))
            
            risk_matrix = {
                "allocated_value_usdt": notional,
                "size": position_size,
                "recommended_leverage": target_leverage
            }

            logger.info(f"📐 [APEX PROFILER] Node: {symbol} | Sigmoid Risk: {sigmoid_risk_pct:.2%} (${dollar_risk:.2f}) | Notional: ${notional:.2f} | Lev: {target_leverage}x")

            leverage_success = await self.executor.adjust_leverage(symbol, target_leverage)
            if not leverage_success:
                self.active_positions_lock.discard(symbol)
                return False

            current_depth = {"bids": [[current_price]], "asks": [[current_price]]}
            if hasattr(feature_engine, 'get_orderbook_snapshot'):
                current_depth = feature_engine.get_orderbook_snapshot()

            if market_regime == "TRENDING":
                execution_success = await self.sor.execute_iceberg_block(
                    symbol=symbol, direction=direction, total_qty=risk_matrix["size"],
                    current_mid_price=current_price, stop_loss=initial_sl, take_profit=target_tp,
                    depth_snapshot=current_depth, vol_z=vol_z_abs, vol_mult=vol_mult, feature_engine=feature_engine
                )
            else:
                execution_success = await self.sor.execute_mean_reversion_bracket(
                    symbol=symbol, direction=direction, total_qty=risk_matrix["size"],
                    current_mid_price=current_price, stop_loss=initial_sl, take_profit=target_tp,
                    depth_snapshot=current_depth, vol_z=vol_z_abs, vol_mult=vol_mult, feature_engine=feature_engine
                )
            
            if not execution_success:
                self.active_positions_lock.discard(symbol)
                return False 
                
            self.risk_vault.update_position_ledger(symbol, risk_matrix['allocated_value_usdt'])
            
            alert_text = (
                f"🧬 *DISTRIBUTED SWARM ORDER ROUTED*\n"
                f"• Node: {symbol} | {direction}\n"
                f"• Market Regime: {market_regime}\n"
                f"• Leverage Applied: {target_leverage}x\n"
                f"• Notional Value: ${risk_matrix['allocated_value_usdt']:.2f} USDT\n"
                f"🛡️ *Brackets Active*: SL: {initial_sl} | TP (Fee Offset): {target_tp}"
            )
            asyncio.create_task(self.telegram.log_message(alert_text, "SUCCESS"))
            
            asyncio.create_task(self._position_lifecycle_daemon(
                symbol, signal_id, direction, current_price, initial_sl, target_tp, atr, risk_matrix, feature_engine, target_leverage, market_regime
            ))
            return True

        except Exception as e:
            logger.error(f"Distributed swarm execution routing failed for {symbol}: {e}")
            self.active_positions_lock.discard(symbol)
            return False

    async def _position_lifecycle_daemon(self, symbol: str, signal_id: str, direction: str, current_price: float, initial_sl: float, target_tp_price: float, atr: float, risk_matrix: dict, feature_engine, target_leverage: int = 8, market_regime: str = "TRENDING"):
        logger.info(f"👻 APEX MONITOR ARMED // CAVATE Daemon injected for node {symbol}")
        
        try:
            polling_interval = 2.5  
            start_time = time.time()
            order_filled = False
            position_reconciled = False
            tp_ghosting_active = False 
            
            current_hard_stop = initial_sl
            peak_observed_price = current_price
            minimum_api_step = self.tick_sizes.get(symbol, 0.0001) * 5.0

            while True:
                await asyncio.sleep(polling_interval)
                actual_filled_qty = 0.0
                actual_entry_price = current_price

                try:
                    pos_response = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                    positions = pos_response.get("result", {}).get("list", [])
                    has_active_position = False
                    
                    if positions:
                        for p in positions:
                            size_float = float(p.get("size", 0.0))
                            if size_float > 0:
                                has_active_position = True
                                order_filled = True 
                                actual_filled_qty = size_float
                                actual_entry_price = float(p.get("avgPrice", current_price))
                                break
                except Exception:
                    has_active_position = False

                if has_active_position and not position_reconciled:
                    actual_notional = actual_filled_qty * actual_entry_price
                    allocated_notional = risk_matrix['allocated_value_usdt']
                    
                    if actual_notional < (allocated_notional * 0.95):
                        freed_margin = allocated_notional - actual_notional
                        self.risk_vault.update_position_ledger(symbol, -freed_margin)
                        risk_matrix['allocated_value_usdt'] = actual_notional
                        logger.info(f"⚖️ LEDGER RECONCILED // {symbol} partial fill detected. Freed ${freed_margin:.2f} of phantom margin.")
                    
                    position_reconciled = True

                if not order_filled and (time.time() - start_time) > 180:
                    try:
                        open_orders = await asyncio.to_thread(self.executor.client.get_open_orders, category="linear", symbol=symbol)
                        for order in open_orders.get("result", {}).get("list", []):
                            if order.get("side").upper() == direction:
                                await asyncio.to_thread(
                                    self.executor.client.cancel_order, category="linear", symbol=symbol, orderId=order.get("orderId")
                                )
                    except Exception:
                        pass
                    
                    self.risk_vault.update_position_ledger(symbol, -risk_matrix['allocated_value_usdt'])
                    logger.critical(f"🔓 PORTFOLIO UNLOCKED // Stale exposure dissolved for {symbol}.")
                    break

                if order_filled and not has_active_position:
                    settlement = await self.executor.check_recent_settlement(symbol=symbol, lookback_seconds=30)
                    if settlement.get("closed"):
                        net_pnl = float(settlement.get('pnl', 0.0))
                        entry_px = float(settlement.get('entry', actual_entry_price))
                        slippage_drag = entry_px - current_price if direction == "BUY" else current_price - entry_px
                        
                        report_message = (
                            f"🔔 <b>EXCHANGE EXECUTION TERMINATION ALERT</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"📈 <b>Asset Node:</b> <code>{symbol}</code>\n"
                            f"📊 <b>Outcome Verdict:</b> " + ("🟢 PROFIT" if net_pnl > 0 else "🔴 LOSS") + f"\n"
                            f"💰 <b>Net Session Return:</b> <code>{net_pnl:.4f} USDT</code>\n"
                            f"⚡ <b>Slippage Footprint:</b> <code>{slippage_drag:.4f} Price Units</code>\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━"
                        )
                        asyncio.create_task(self.telegram.send_html_report(report_message))
                        self.memory.log_live_execution_result(signal_id, net_pnl, slippage_drag, settlement['outcome'])
                        self.risk_vault.update_position_ledger(symbol, -risk_matrix['allocated_value_usdt'])
                        break
                
                if has_active_position:
                    trade_duration = time.time() - start_time
                    live_mid = feature_engine.get_latest_mid() if feature_engine and hasattr(feature_engine, 'get_latest_mid') else None
                    
                    if live_mid:
                        profit_distance = (live_mid - actual_entry_price) if direction == "BUY" else (actual_entry_price - live_mid)
                        
                        live_metrics = self.screener_metrics.get(symbol, {})
                        live_vol_z = abs(live_metrics.get("vol_z", 0.0))
                        vol_mult = live_metrics.get("vol_mult", 1.0)
                        
                        vol_dampener = 1.0 + (math.log1p(live_vol_z) * 0.2)
                        liquidity_penalty = 1.0 / math.sqrt(max(0.1, vol_mult))
                        
                        profit_atr_multiple = max(0.0, profit_distance / atr)
                        
                        base_leash_mult = 2.5 if market_regime == "TRENDING" else 1.8
                        max_decay = 1.75 if market_regime == "TRENDING" else 1.05
                        acceleration_k = 0.5
                        
                        dynamic_leash_mult = base_leash_mult - (max_decay * (1.0 - math.exp(-acceleration_k * profit_atr_multiple)))
                        
                        if not tp_ghosting_active and target_tp_price > 0:
                            distance_to_tp = abs(target_tp_price - live_mid)
                            if distance_to_tp < (atr * 0.5) and live_vol_z > 2.0:
                                try:
                                    await asyncio.to_thread(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, takeProfit="0")
                                    tp_ghosting_active = True
                                    logger.critical(f"🚀 [MOMENTUM HARVESTING] {symbol} TP Ghosting engaged! Hard TP canceled. Parabolic Trailing initiated.")
                                except Exception as e:
                                    logger.debug(f"TP Ghosting override failed: {e}")
                        
                        if tp_ghosting_active:
                            dynamic_leash_mult = 0.35 
                        
                        active_leash = atr * dynamic_leash_mult * vol_dampener * liquidity_penalty
                        
                        if trade_duration > 2700:
                            time_decay_factor = math.exp(-max(0, trade_duration - 2700) / 3600)
                            active_leash = active_leash * time_decay_factor
                        
                        fee_offset = actual_entry_price * 0.0012

                        if direction == "BUY":
                            if live_mid > peak_observed_price: peak_observed_price = live_mid
                                
                            if profit_atr_multiple > 0.8:
                                lock_distance = min(0.6 * atr, (profit_atr_multiple - 0.8) * atr * 0.5)
                                minimum_safe_stop = actual_entry_price + fee_offset + lock_distance
                            else:
                                minimum_safe_stop = 0.0

                            target_stop = max(peak_observed_price - active_leash, minimum_safe_stop)
                            
                            if target_stop > (current_hard_stop + minimum_api_step) and target_stop < live_mid:
                                try:
                                    response = await asyncio.to_thread(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, stopLoss=str(round(target_stop, 4)))
                                    if isinstance(response, dict) and response.get("retCode") == 0:
                                        current_hard_stop = target_stop
                                except Exception:
                                    pass
                                        
                        elif direction == "SELL":
                            if live_mid < peak_observed_price: peak_observed_price = live_mid
                                
                            if profit_atr_multiple > 0.8:
                                lock_distance = min(0.6 * atr, (profit_atr_multiple - 0.8) * atr * 0.5)
                                minimum_safe_stop = actual_entry_price - fee_offset - lock_distance
                            else:
                                minimum_safe_stop = float('inf')

                            target_stop = min(peak_observed_price + active_leash, minimum_safe_stop)
                            
                            if target_stop < (current_hard_stop - minimum_api_step) and target_stop > live_mid:
                                try:
                                    response = await asyncio.to_thread(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, stopLoss=str(round(target_stop, 4)))
                                    if isinstance(response, dict) and response.get("retCode") == 0:
                                        current_hard_stop = target_stop
                                except Exception:
                                    pass

        except Exception as daemon_error:
            logger.error(f"☠️ FATAL DAEMON CRASH on {symbol}: {daemon_error}")
        finally:
            self.active_positions_lock.discard(symbol)

    async def run_engine_forever(self):
        logger.critical("LAUNCHING DISTRIBUTED QUANT SWARM DAEMON DEPLOYMENTS...")
        await self._fetch_exchange_tick_sizes()
        await self.synchronize_exchange_state()
        
        boot_basket = await self.executor.get_top_volatile_assets(limit=100, min_turnover=10_000_000)
        
        if boot_basket and len(boot_basket) >= 25:
            if "BTCUSDT" in boot_basket:
                boot_basket.remove("BTCUSDT")
                
            self.asset_basket = ["BTCUSDT"] + boot_basket[:24]
            self.shadow_basket = boot_basket[24:]
            
            self.feature_engines = {s: AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600) for s in self.asset_basket}
            self.math_engines = {s: FastMathEngine() for s in self.asset_basket}
            self.screener_memory = {s: {"prices": [], "macro_prices": [], "volumes": [], "last_update_time": 0.0} for s in self.asset_basket}
            self.macro_regimes = {s: "HOLD" for s in self.asset_basket}
            self.macro_confidences = {s: 0.0 for s in self.asset_basket}
            self.current_atrs = {s: 0.0 for s in self.asset_basket}
            self.last_execution_timestamps = {s: 0.0 for s in self.asset_basket}
            self.screener_metrics = {s: {"vol_mult": 1.0, "vol_z": 0.0, "smoothed_price": 0.0, "hawkes_score": 0.0} for s in self.asset_basket}
        
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