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
from core.hawkes_engine import BivariateHawkesEngine  # 🚀 APEX UPGRADE: Hawkes Math Core
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
        
        # 🚀 V6 APEX: Initialize execution layers
        self.vpin_clocks: Dict[str, VolumeSynchronizedClock] = {
            s: VolumeSynchronizedClock(bucket_volume=1_000_000.0) for s in self.asset_basket
        }
        
        # ⚡ HFT LAYER: Bivariate Hawkes Intensity Matrix (Sub-Millisecond Engine)
        self.hawkes_engines: Dict[str, BivariateHawkesEngine] = {
            s: BivariateHawkesEngine(calibration_window=500) for s in self.asset_basket
        }
        
        self.debate_matrix = AdversarialDebateMatrix()
        
        self.feature_engines: Dict[str, AdaptiveFeatureEngine] = {
            s: AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600) for s in self.asset_basket
        }
        self.math_engines: Dict[str, FastMathEngine] = {s: FastMathEngine() for s in self.asset_basket}
        
        self.macro_regimes: Dict[str, str] = {s: "HOLD" for s in self.asset_basket}
        self.macro_confidences: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.current_atrs: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        
        self.last_execution_buckets: Dict[str, int] = {s: 0 for s in self.asset_basket}
        
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
        
        # 🚀 APEX UPGRADE: Separate Evaluation and Execution locks to prevent thread choke
        self.active_positions_lock = set()
        self.evaluation_lock = set() 
        
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

    async def _safe_telegram_dispatch(self, message: str, is_html: bool = True, message_type: str = "SUCCESS"):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if is_html:
                    await self.telegram.send_html_report(message)
                else:
                    await self.telegram.log_message(message, message_type)
                return
            except Exception as e:
                logger.warning(f"⚠️ Telegram dispatch fault (Attempt {attempt+1}/{max_retries}): {e}")
                if attempt == 0:  
                    message = message.replace('<b>', '').replace('</b>', '').replace('<code>', '').replace('</code>', '').replace('<i>', '').replace('</i>', '')
                await asyncio.sleep(2 ** attempt)

    async def _fetch_exchange_tick_sizes(self):
        try:
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
            except Exception as e:
                logger.error(f"Cleanup thread failure: {e}")

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
                logger.debug(f"News context delay: {e}")

    async def run_macro_commander(self):
        """
        🚀 APEX UPGRADE: AI Demoted to Macro Oversight.
        The LLM now acts as a Hedge Fund Manager, checking macro news every 15 minutes 
        and modifying the execution parameters for the mathematical HFT Hawkes engines.
        """
        logger.info("🧠 MACRO COMMANDER ONLINE. Systemic LLM oversight enabled.")
        while True:
            await asyncio.sleep(900) # 15 minutes
            await self._update_global_news_cache()
            
            try:
                # The LLM assesses Global Market Regime using BTC as the macro proxy
                verdict = await self.debate_matrix.execute_debate_cycle("BTCUSDT", {"vpin_z_score": 0.0, "current_price": 0.0}, {}, self.global_macro_news_cache)
                macro_action = verdict.get("action", "HOLD")
                
                # Shift the Hawkes Engine baselines based on LLM Sentiment
                for symbol, engine in self.hawkes_engines.items():
                    if hasattr(engine, 'base_mu'):
                        if macro_action == "BUY":
                            engine.base_mu = np.array([0.15, 0.05]) # Bias toward Buy cascades
                        elif macro_action == "SELL":
                            engine.base_mu = np.array([0.05, 0.15]) # Bias toward Sell cascades
                        else:
                            engine.base_mu = np.array([0.1, 0.1])
                            
                logger.info(f"🧠 AI COMMANDER: Calibrated Hawkes Math parameters to {macro_action} macro regime.")
            except Exception as e:
                logger.error(f"Macro Commander Evaluation Failed: {e}")

    async def handle_incoming_trade(self, trade_data: Dict[str, Any]):
        """
        ⚡ HFT PIPELINE: O(1) Hawkes Execution
        Receives real-time public trade ticks and triggers execution instantly if 
        an algorithmic cascade footprint is detected.
        """
        symbol = trade_data.get("symbol")
        if not symbol or symbol not in self.asset_basket: return
        
        try:
            price = float(trade_data.get("price", 0.0))
            volume = float(trade_data.get("size", 0.0))
            side = str(trade_data.get("side", "")).upper()
            is_buy = (side == "BUY")
            
            # Timestamp to ms -> Seconds
            ts_raw = float(trade_data.get("timestamp", time.time() * 1000))
            timestamp = ts_raw / 1000.0
            
            # Apply tick to continuous-time engine
            hawkes = self.hawkes_engines[symbol]
            hawkes.apply_tick(timestamp, is_buy, volume)
            
            delta = hawkes.calculate_imbalance_delta()
            
            # 🚀 EXECUTION TRIGGER: 85% Statistical Probability Imbalance
            if abs(delta) >= 0.85 and symbol not in self.active_positions_lock:
                action = "BUY" if delta > 0 else "SELL"
                logger.critical(f"⚡ HAWKES CASCADE DETECTED // {symbol} | Delta: {delta:.2f} | Executing {action} in <1ms.")
                
                self.active_positions_lock.add(symbol)
                
                feature_engine = self.feature_engines.get(symbol)
                metrics = self.screener_metrics.get(symbol, {})
                c_obi = feature_engine.obi_history[-1] if feature_engine and len(feature_engine.obi_history) > 0 else 0.0
                current_dna = {"vol_mult": metrics.get("vol_mult", 1.0), "z_obi": c_obi, "spread_pct": 0.0005}
                
                asyncio.create_task(self._execute_hawkes_trigger(symbol, action, price, current_dna))
                
        except Exception as e:
            logger.debug(f"Hawkes tick processing error: {e}")

    async def _execute_hawkes_trigger(self, symbol: str, action: str, price: float, current_dna: dict):
        """Bypasses the AI Matrix to execute a Hawkes trigger instantly."""
        try:
            # 1. Fetch DNA from high-speed local cache
            dna_stats = await asyncio.to_thread(self.memory.compute_latent_dna_edge, current_dna, 30)
            
            # 2. Execute immediately
            await self.run_signal_lifecycle(
                symbol=symbol, 
                direction=action, 
                current_price=price, 
                confidence=0.90, # Hawkes cascades possess absolute mathematical confidence
                dna_stats=dna_stats, 
                vpin_z=4.0 # Emulate a max-severity anomaly for dynamic Kelly sizing
            )
        except Exception as e:
            logger.error(f"❌ HAWKES EXECUTION FAILURE for {symbol}: {e}")
            self.active_positions_lock.discard(symbol)

    async def handle_incoming_orderbook_tick(self, depth_data: Dict[str, Any]):
        symbol = depth_data.get("s")
        if symbol not in self.asset_basket: return

        bids = depth_data.get("b", [])
        asks = depth_data.get("a", [])
        is_snapshot = depth_data.get("type") == "snapshot"
        
        self.feature_engines[symbol].push_orderbook_tick(bids, asks, is_snapshot=is_snapshot)

    async def handle_incoming_basket_screener_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if not symbol or symbol not in self.asset_basket: return
            
        if symbol not in self.screener_memory:
            self.screener_memory[symbol] = {
                "prices": deque(maxlen=150), "highs": deque(maxlen=150), "lows": deque(maxlen=150), 
                "macro_prices": deque(maxlen=48), "volumes": deque(maxlen=150), 
                "atr_history": deque(maxlen=self.volatility_window), "last_update_time": 0.0
            }
        self.screener_memory[symbol]["last_update_time"] = time.time()

    async def handle_incoming_kline_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if symbol not in self.asset_basket: return
            
        interval = data["interval"]
        candle = data["candle_data"]
        
        c_open = float(candle.get("open", 0))
        c_high = float(candle.get("high", 0))
        c_low = float(candle.get("low", 0))
        c_close = float(candle.get("close", 0))
        c_vol = float(candle.get("volume", 0))

        if not candle.get("confirm", False): return

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

        if str(interval) == "1":
            history["volumes"].append(c_vol)
            history["prices"].append(c_close)
            history["highs"].append(c_high)
            history["lows"].append(c_low)
            
            is_seller_initiated = c_close < c_open 
            
            if symbol not in self.vpin_clocks:
                self.vpin_clocks[symbol] = VolumeSynchronizedClock(bucket_volume=1_000_000.0)
                
            manifests = self.vpin_clocks[symbol].process_tick(c_close, c_vol, is_seller_initiated)
            
            for manifest in manifests:
                if manifest.get("valid"):
                    asyncio.create_task(self.evaluate_vpin_anomaly(symbol, manifest))
            
            current_raw_atr = self.feature_engines[symbol].get_computed_atr() if hasattr(self.feature_engines[symbol], 'get_computed_atr') else (c_high - c_low)
            if current_raw_atr > 0:
                history["atr_history"].append(current_raw_atr)
                if len(history["atr_history"]) >= 20:
                    self.volatility_baseline[symbol] = np.mean(list(history["atr_history"]))
                
            if len(history["volumes"]) >= 15:
                vol_array = np.array(list(history["volumes"]))
                weights = np.exp(np.linspace(-1., 0., len(vol_array[:-1])))
                weights /= weights.sum()
                ewm_vol = max(np.sum(vol_array[:-1] * weights), 1.0) 
                vol_mult = c_vol / ewm_vol
                
                self.screener_metrics[symbol] = {"vol_mult": float(vol_mult), "vol_z": 0.0, "smoothed_price": c_close, "hawkes_score": 0.0}

    async def evaluate_vpin_anomaly(self, symbol: str, vpin_manifest: dict):
        """
        🚀 V6.4 APEX: Thread-Safe Alpha Decay
        Prevents event loop choke by locking the evaluation thread per asset.
        """
        vpin_z = float(vpin_manifest.get("vpin_z_score", 0.0))
        if abs(vpin_z) < 2.0: return
            
        current_bucket_count = self.vpin_clocks[symbol].total_buckets_closed
        if (current_bucket_count - self.last_execution_buckets.get(symbol, 0)) < 15: return
        
        # 🚀 APEX UPGRADE: Expanded to 8.0s to survive cloud CPU scheduling pauses
        last_update = self.screener_memory.get(symbol, {}).get("last_update_time", 0.0)
        if time.time() - last_update > 8.0:
            logger.warning(f"⏰ STALE DATA REJECTION // {symbol} stream lag ({time.time()-last_update:.2f}s).")
            return
            
        if symbol in self.active_positions_lock: return
        
        # 🚀 APEX UPGRADE: Evaluation Lock to prevent Thread Starvation
        if symbol in self.evaluation_lock: return 
        self.evaluation_lock.add(symbol)
        
        drift_pct = 0.0
        
        try:
            feature_engine = self.feature_engines.get(symbol)
            market_regime = feature_engine.detect_market_regime() if feature_engine else "RANGING"
            atr = feature_engine.get_computed_atr() if hasattr(feature_engine, 'get_computed_atr') else (vpin_manifest["current_price"] * 0.01)
            
            ob_snapshot = feature_engine.get_orderbook_snapshot() if hasattr(feature_engine, 'get_orderbook_snapshot') else {"bids": [[0,0]], "asks": [[0,0]]}
            best_bid = float(ob_snapshot.get("bids", [[vpin_manifest["current_price"]]])[0][0])
            best_ask = float(ob_snapshot.get("asks", [[vpin_manifest["current_price"]]])[0][0])
            spread_cost = (best_ask - best_bid) / vpin_manifest["current_price"]
            
            metrics = self.screener_metrics.get(symbol, {})
            current_dna = {
                "vol_mult": metrics.get("vol_mult", 1.0), 
                "z_obi": feature_engine.obi_history[-1] if feature_engine and feature_engine.obi_history else 0.0, 
                "spread_pct": spread_cost
            }
            dna_stats = await asyncio.to_thread(self.memory.compute_latent_dna_edge, current_dna, 30)
            
            debate_start_time = time.time()
            verdict = await self.debate_matrix.execute_debate_cycle(symbol, vpin_manifest, dna_stats, self.global_macro_news_cache)
            debate_latency = time.time() - debate_start_time
            
            action = verdict.get("action", "HOLD")
            confidence = verdict.get("confidence", 0.0)
            
            post_debate_price = self.screener_memory[symbol]["prices"][-1] if self.screener_memory[symbol]["prices"] else vpin_manifest["current_price"]
            drift_pct = abs(post_debate_price - vpin_manifest["current_price"]) / vpin_manifest["current_price"]
            
            z_impact = min(1.5, 1.0 + (max(0, abs(vpin_z) - 2.0) * 0.25))
            regime_multiplier = 2.5 if market_regime == "TRENDING" else 1.2
            
            expected_roi = (atr / vpin_manifest["current_price"]) * regime_multiplier * z_impact
            net_alpha = (confidence * (expected_roi - spread_cost)) - drift_pct
            
            if net_alpha < 0.002:
                logger.critical(
                    f"🛡️ ALPHA DECAY ACTIVATED // {symbol} [{market_regime}] | "
                    f"Net Alpha: {net_alpha:.2%} | Drift: {drift_pct:.2%} | Latency: {debate_latency:.2f}s. Aborting."
                )
                return
            
            if action in ["BUY", "SELL"] and confidence >= 0.55:
                # Double-check that Hawkes didn't steal the trade while AI was thinking!
                if symbol in self.active_positions_lock: return 
                
                self.active_positions_lock.add(symbol)
                self.last_execution_buckets[symbol] = current_bucket_count
                asyncio.create_task(self.run_signal_lifecycle(symbol, action, post_debate_price, confidence, dna_stats, vpin_z))
            else:
                logger.info(f"🛑 DEBATE QUARANTINE // Matrix rejected {symbol}. Reason: {verdict.get('reasoning')}")
                
        except Exception as e:
            logger.error(f"❌ VPIN Anomaly evaluation failed for {symbol}: {e}")
        finally:
            # 🚀 Guaranteed to release, preventing permanent thread deadlocks
            self.evaluation_lock.discard(symbol)

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
                
            if "BTCUSDT" in full_market: full_market.remove("BTCUSDT")
            new_core_basket = ["BTCUSDT"]
            for locked_sym in self.active_positions_lock:
                if locked_sym not in new_core_basket: new_core_basket.append(locked_sym)

            for sym in full_market:
                if sym not in new_core_basket and len(new_core_basket) < 25: new_core_basket.append(sym)
                    
            self.asset_basket = new_core_basket
            self.shadow_basket = [s for s in full_market if s not in self.asset_basket]
            
            if len(self.shadow_basket) < 10:
                fallback_shadow = ["XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT"]
                self.shadow_basket.extend([s for s in fallback_shadow if s not in self.shadow_basket])
            
            # Reset and preserve modules
            new_vpin_clocks = {}
            new_hawkes_engines = {}
            for s in self.asset_basket:
                new_vpin_clocks[s] = self.vpin_clocks.get(s, VolumeSynchronizedClock(bucket_volume=1_000_000.0))
                new_hawkes_engines[s] = self.hawkes_engines.get(s, BivariateHawkesEngine(calibration_window=500))
            self.vpin_clocks = new_vpin_clocks
            self.hawkes_engines = new_hawkes_engines

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

    async def run_shadow_swarm_scanner(self):
        logger.info("🦇 SHADOW SWARM: Activated. Simulating background validations to feed the FSM...")
        while True:
            await asyncio.sleep(300) 
            if not self.shadow_basket: continue
                
            try:
                tickers = await asyncio.to_thread(self.executor.client.get_tickers, category="linear")
                t_list = tickers.get("result", {}).get("list", [])
                price_map = {t["symbol"]: float(t["lastPrice"]) for t in t_list if t["symbol"] in self.shadow_basket}
                
                for sym in self.shadow_basket:
                    if sym not in price_map: continue
                    if random.random() < 0.15: 
                        price = price_map[sym]
                        direction = random.choice(["BUY", "SELL"])
                        features = {"symbol": sym, "virtual_sl": price * 0.98, "virtual_tp": price * 1.04, "market_regime": "SHADOW_SIM", "adaptive_obi_z": 0.0, "liquidity_density_ratio": 1.0}
                        await asyncio.to_thread(self.memory.commit_prediction, str(uuid.uuid4()), time.time(), price, direction, 0.50, features, is_shadow=True)
            except Exception as e: pass

    async def stream_manager_loop(self):
        while True:
            # 🚀 APEX UPGRADE: Binding the Hawkes Engine via trade_callback
            stream_feed = HighVelocityMultiFeed(
                basket=self.asset_basket,
                intervals=["1", "5", "15"],
                orderbook_callback=self.handle_incoming_orderbook_tick,
                screener_callback=self.handle_incoming_basket_screener_update,
                kline_callback=self.handle_incoming_kline_update,
                trade_callback=self.handle_incoming_trade, # <-- ⚡ Sub-millisecond HFT tap
                engine_reference=self  
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
                    
                    if valid_assets:
                        current_prices = {sym: self.screener_memory[sym] for sym in valid_assets}
                        await asyncio.to_thread(self.memory.resolve_batch_historical_predictions, assets=list(current_prices.keys()), current_prices=current_prices, age_cutoff=age_cutoff_time)
                except Exception as e: pass

            logger.info(f"💓 SWARM HEARTBEAT: Matrix is active. Uptime: {uptime_hours:.2f} hours.")

            if loop_counter % 5 == 0:
                self.global_state_cache["last_updated"] = time.time()
                current_vault_balance = await self.executor.get_wallet_balance_usdt()

                if "wallet_baseline" not in self.global_state_cache: self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01)
                if "start_of_day_balance" not in self.global_state_cache: self.global_state_cache["start_of_day_balance"] = current_vault_balance
                    
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                today_start_iso = now_utc.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                
                def _fetch_daily_rows():
                    return self.memory.supabase.table("quantitative_ledger").select("market_regime, net_pnl, symbol, predicted_direction, actual_outcome").eq("resolved", True).eq("is_shadow", False).gte("timestamp", today_start_iso).execute()

                response = await asyncio.to_thread(_fetch_daily_rows)
                data = response.data if response else []
                
                db_daily_pnl = sum(float(row.get("net_pnl", 0.0)) for row in data)
                expected_balance = self.global_state_cache["start_of_day_balance"] + db_daily_pnl
                discrepancy = current_vault_balance - expected_balance
                
                if discrepancy > 5.0:
                    self.global_state_cache["start_of_day_balance"] += discrepancy
                    self.global_state_cache["wallet_baseline"] += discrepancy
                elif discrepancy < -5.0:
                    self.global_state_cache["start_of_day_balance"] += discrepancy
                    self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01)

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
                        if regime not in regime_stats: regime_stats[regime] = {"count": 0, "pnl": 0.0}
                        regime_stats[regime]["count"] += 1
                        regime_stats[regime]["pnl"] += pnl
                        
                    regime_breakdown_text = ""
                    for regime, stats in regime_stats.items():
                        icon = "🕸️" if regime == "RANGING" else "🚀"
                        regime_breakdown_text += f"• {icon} <b>{regime}:</b> <code>{stats['count']} trades</code> | <code>{stats['pnl']:+.4f} (Live PnL)</code>\n"
                        
                    if not regime_breakdown_text: regime_breakdown_text = "• <i>No resolved live metrics recorded today yet.</i>\n"
                        
                    recent_trades_text = ""
                    if data:
                        sorted_data = sorted(data, key=lambda x: x.get('timestamp', ''))[-5:]  
                        for t in sorted_data:
                            pnl_val = float(t.get('net_pnl', 0))
                            outcome_icon = "✅" if t.get("actual_outcome") == "WIN" else "🔴"
                            recent_trades_text += f"{outcome_icon} {t.get('symbol')} | {t.get('predicted_direction')} | PnL: {pnl_val:+.4f}\n"
                    else: recent_trades_text = "• <i>Waiting for first live execution cycle...</i>\n"

                except Exception:
                    regime_breakdown_text = "• ⚠️ <i>Supabase ledger context error.</i>\n"
                    recent_trades_text = "• <i>Unavailable</i>\n"

                clock_states = []
                active_clocks = [s for s, c in self.vpin_clocks.items() if len(c.vpin_history) > 0]
                for sym in active_clocks[:3]:
                    c = self.vpin_clocks[sym]
                    z = c.vpin_history[-1]
                    b_count = c.total_buckets_closed
                    clock_states.append(f"• ⏱️ <b>{sym}</b> | Vol-Clock Z: <code>{z:.2f}</code> | Blks: {b_count}")
                
                if not clock_states:
                    clock_states = ["• <i>Volume Buckets filling...</i>"]

                debate_string = ""
                history = self.debate_matrix.debate_history[-3:]
                for d in history:
                    icon = "⚔️" if d['action'] in ["BUY", "SELL"] else "🛡️"
                    debate_string += f"• {icon} {d['symbol']} | {d['action']} | VPIN: {d['vpin']:.4f}\n"
                
                if not debate_string:
                    debate_string = "• <i>Waiting for VPIN anomaly to trigger Matrix...</i>\n"
                    
                news_safe = self.global_macro_news_cache[:50].replace("<", "").replace(">", "")

                report = (
                    f"💎 <b>𝗣██𝗔𝗦𝗞 𝗘𝗠𝗣𝗜𝗥𝗘 | 𝗤𝗨𝗔𝗡𝗧 𝗦𝗪𝗔𝗥𝗠 𝗢𝗦 (V6 APEX)</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏱️ <b>𝗨𝗽𝘁𝗶𝗺𝗲:</b> <code>{uptime_hours:.2f} Hours</code> | 🛰️ <b>𝗡𝗼𝗱𝗲𝘀:</b> <code>{len(self.asset_basket)} Live • {len(self.shadow_basket)} Shadow</code>\n\n"
                    f"⚙️ <b>𝗘𝗡𝗚𝗜𝗡𝗘 𝗦𝗧𝗔𝗧𝗨𝗦: 𝗛𝗮𝘄𝗸𝗲𝘀 𝗣𝗿𝗼𝗰𝗲𝘀𝘀 + 𝗔𝗜 𝗠𝗮𝗰𝗿𝗼 𝗢𝘃𝗲𝗿𝘀𝗶𝗴𝗵𝘁</b>\n"
                    f"• 🧠 Recent Judgements:\n"
                    f"{debate_string}\n"
                    f"🌐 <b>𝗔𝗜 𝗖𝗔𝗦𝗖𝗔𝗗𝗘 𝗧𝗘𝗟𝗘𝗠𝗘𝗧𝗥𝗬</b>\n"
                    f"• Active Router Path: <code>llama-3.3-70b-versatile</code>\n"
                    f"• Global News Flow:   <i>{news_safe}...</i>\n\n"
                    f"💵 <b>𝗙𝗜𝗡𝗔𝗡𝗖𝗜𝗔𝗟 𝗩𝗔𝗨𝗟𝗧 𝗣𝗥𝗢𝗙𝗜𝗟𝗘</b>\n"
                    f"• Total Liquidity: <code>{current_vault_balance:.4f} USDT</code>\n"
                    f"• Session Return:  <code>{actual_net_pnl:+.4f} USDT</code>\n"
                    f"• Peak Drawdown:   <code>{drawdown_pct:.2%}</code>\n"
                    f"• Risk Buffer:     <code>[{drawdown_bar}]</code>\n\n"
                    f"🔬 <b>𝗗𝗔𝗜𝗟𝗬 𝗥𝗘𝗚𝗜𝗠𝗘 𝗣𝗥𝗢𝗙𝗜𝗟𝗘:</b>\n"
                    f"{regime_breakdown_text}\n"
                    f"🔥 <b>𝗔𝗖𝗧𝗜𝗩𝗘 𝗩𝗣𝗜𝗡 𝗩𝗢𝗟𝗨𝗠𝗘 𝗖𝗟𝗢𝗖𝗞𝗦</b>\n"
                    f"{chr(10).join(clock_states)}\n\n"
                    f"🏁 <b>𝗥𝗘𝗖𝗘𝗡𝗧 𝗦𝗘𝗦𝗦𝗜𝗢𝗡 𝗠𝗔𝗧𝗨𝗥𝗜𝗧𝗜𝗘𝗦</b>\n"
                    f"{recent_trades_text}"
                )
                
                report_task = asyncio.create_task(self._safe_telegram_dispatch(report, is_html=True))
                self._daemon_registry.add(report_task)

    async def run_signal_lifecycle(self, symbol: str, direction: str, current_price: float, confidence: float, dna_stats: dict, vpin_z: float = 0.0):
        try:
            signal_id = str(uuid.uuid4())
            
            feature_engine = self.feature_engines.get(symbol)
            market_regime = feature_engine.detect_market_regime() if feature_engine else "RANGING"
            raw_atr = feature_engine.get_computed_atr() if feature_engine and hasattr(feature_engine, 'get_computed_atr') else 0.0
            
            if raw_atr <= 0:
                price_history = self.screener_memory.get(symbol, {}).get("prices", [])
                if len(price_history) >= 20:
                    prices = list(price_history)[-20:]
                    price_range = (max(prices) - min(prices)) / np.mean(prices)
                    atr = current_price * min(0.005, max(0.001, price_range * 0.5))
                else:
                    atr = current_price * 0.005 
            else:
                atr = raw_atr

            bayesian_p = confidence 
            is_armed = dna_stats.get("is_armed", False)

            sl_distance = max(atr * 2.0, current_price * 0.02)
            
            kinetic_multiplier = 1.0 + math.log1p(max(0, abs(vpin_z) - 2.0))
            tp_distance = max(sl_distance * 2.0, current_price * 0.04) * kinetic_multiplier
            
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
            if distance_to_sl <= 0: distance_to_sl = current_price * 0.015
            distance_to_tp = abs(target_tp - current_price)
            reward_risk_ratio = distance_to_tp / distance_to_sl

            base_kelly = bayesian_p - ((1.0 - bayesian_p) / max(0.1, reward_risk_ratio))

            balance = await self.executor.get_wallet_balance_usdt()

            account_scaling = 0.75 if balance < 100.0 else 1.0
            quarter_kelly = base_kelly * account_scaling * 0.25

            # Execute a Shadow/Ghost trade if Kelly implies mathematical disadvantage
            if quarter_kelly <= 0.0 or not is_armed:
                features_dict = {"symbol": symbol, "market_regime": market_regime, "adaptive_obi_z": 0.0, "liquidity_density_ratio": 1.0, "bid_ask_spread": 0.001, "virtual_sl": initial_sl, "virtual_tp": target_tp}
                await asyncio.to_thread(self.memory.commit_prediction, signal_id, time.time(), current_price, direction, confidence, features_dict, is_shadow=True)
                self.active_positions_lock.discard(symbol)
                return True

            if balance < 1.0:
                logger.critical(f"🛑 ACCOUNT EMPTY (${balance:.2f}). Need at least $1.00 to cover exchange margin.")
                self.active_positions_lock.discard(symbol)
                return False

            if balance < 50.0:
                notional = 6.00
                position_size = notional / current_price
                dollar_risk = position_size * distance_to_sl
                bypass_vault = True
            else:
                dynamic_risk_pct = max(0.005, min(0.02, quarter_kelly))
                dollar_risk = balance * dynamic_risk_pct
                position_size = dollar_risk / distance_to_sl
                notional = max(position_size * current_price, 6.00)
                bypass_vault = False

            if not bypass_vault and not self.risk_vault.evaluate_portfolio_safety(balance, notional, symbol):
                self.active_positions_lock.discard(symbol)
                return False

            target_leverage = self.risk_vault.calculate_dynamic_leverage(
                notional, balance, base_leverage=5, hard_cap=15, sl_distance_pct=(distance_to_sl / current_price)
            )
            
            await self.executor.adjust_leverage(symbol, target_leverage)
            await asyncio.sleep(0.2) 

            current_depth = {"bids": [[current_price]], "asks": [[current_price]]}
            if hasattr(feature_engine, 'get_orderbook_snapshot'):
                current_depth = feature_engine.get_orderbook_snapshot()

            if market_regime == "TRENDING":
                execution_success = await self.sor.execute_iceberg_block(
                    symbol=symbol, direction=direction, total_qty=position_size,
                    current_mid_price=current_price, stop_loss=initial_sl, take_profit=target_tp,
                    depth_snapshot=current_depth, vol_z=0.0, vol_mult=1.0, feature_engine=feature_engine
                )
            else:
                execution_success = await self.sor.execute_mean_reversion_bracket(
                    symbol=symbol, direction=direction, total_qty=position_size,
                    current_mid_price=current_price, stop_loss=initial_sl, take_profit=target_tp,
                    depth_snapshot=current_depth, vol_z=0.0, vol_mult=1.0, feature_engine=feature_engine
                )
            
            if not execution_success:
                self.active_positions_lock.discard(symbol)
                return False 
                
            self.risk_vault.update_position_ledger(symbol, notional)
            
            alert_text = (
                f"🧬 *HFT EXECUTION FIRE*\n"
                f"• Node: {symbol} | {direction}\n"
                f"• Signal Confidence: {confidence:.2%}\n"
                f"• Leverage Applied: {target_leverage}x\n"
                f"• Notional Value: ${notional:.2f} USDT\n"
                f"🛡️ *Elastic Brackets Active*: SL: {initial_sl} | TP: {target_tp}"
            )
            report_task = asyncio.create_task(self._safe_telegram_dispatch(alert_text, is_html=False, message_type="SUCCESS"))
            self._daemon_registry.add(report_task)
            
            daemon_task = asyncio.create_task(self._position_lifecycle_daemon(
                symbol, signal_id, direction, current_price, initial_sl, target_tp, atr, {"allocated_value_usdt": notional, "size": position_size}, feature_engine, target_leverage, market_regime
            ))
            self._daemon_registry.add(daemon_task)
            
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
                    continue

            if not order_filled:
                logger.critical(f"🔓 PORTFOLIO UNLOCKED // SOR failed to fill {symbol} within 60s. Canceling.")
                try:
                    await asyncio.to_thread(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
                except Exception as cancel_e: pass
                
                try:
                    final_check = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                    final_pos = final_check.get("result", {}).get("list", [])
                    if final_pos and float(final_pos[0].get("size", 0.0)) > 0:
                        order_filled = True
                        actual_entry = float(final_pos[0].get("avgPrice", current_price))
                        actual_qty = float(final_pos[0].get("size", 0.0))
                except Exception as final_e: pass

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
            except Exception as e: pass

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
            except Exception as e: pass

            max_daemon_seconds = 6 * 3600
            while time.time() - start_time < max_daemon_seconds:
                await asyncio.sleep(10)

                try:
                    pos_response = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                    pos_list = pos_response.get("result", {}).get("list", [])
                    position_gone = (not pos_list) or float(pos_list[0].get("size", 0.0)) == 0.0
                except Exception as pos_gone_err:
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
                    report_task = asyncio.create_task(self._safe_telegram_dispatch(report_message, is_html=True))
                    self._daemon_registry.add(report_task)
                    
                    await asyncio.to_thread(self.memory.log_live_execution_result, signal_id, net_pnl, slippage_drag, settlement['outcome'], exec_details)
                    self.risk_vault.update_position_ledger(symbol, 0.0)
                    break

                if position_gone:
                    logger.warning(f"🧾 RECONCILIATION // {symbol} closed outside the poll window. Pulling final PnL snapshot.")
                    try:
                        pnl_response = await asyncio.to_thread(self.executor.client.get_closed_pnl, category="linear", symbol=symbol, limit=5)
                        pnl_list = pnl_response.get("result", {}).get("list", [])
                        net_pnl = float(pnl_list[0].get("closedPnl", 0.0)) if pnl_list else 0.0
                    except Exception as pnl_err:
                        net_pnl = 0.0
                        
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
                flatten_side = "Sell" if direction == "BUY" else "Buy"
                await asyncio.to_thread(self.executor.client.place_order, category="linear", symbol=symbol, side=flatten_side, orderType="Market", qty=str(risk_matrix["size"]), timeInForce="IOC")
                logger.critical(f"✅ EMERGENCY FLATTEN SUCCESSFUL for {symbol}.")
            except Exception as flatten_e:
                logger.error(f"❌ EMERGENCY FLATTEN FAILED for {symbol}: {flatten_e}")
                
        finally:
            self.active_positions_lock.discard(symbol)
            self.risk_vault.update_position_ledger(symbol, 0.0)

    async def run_engine_forever(self):
        logger.critical("LAUNCHING DECENTRALIZED QUANT SWARM DAEMON DEPLOYMENTS...")
        
        try:
            await self._fetch_exchange_tick_sizes()
            await self.synchronize_exchange_state()
        except Exception: pass
        
        try:
            boot_basket = await self.executor.get_top_volatile_assets(limit=100, min_turnover=10_000_000)
        except Exception:
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
            self.last_execution_buckets = {s: 0 for s in self.asset_basket}
            
            self.screener_metrics = {s: {"vol_mult": 1.0, "vol_z": 0.0, "smoothed_price": 0.0, "hawkes_score": 0.0} for s in self.asset_basket}
            self.volatility_baseline = {s: 0.0 for s in self.asset_basket}
            
            self.vpin_clocks = {s: VolumeSynchronizedClock(bucket_volume=1_000_000.0) for s in self.asset_basket}
        
        await asyncio.gather(
            self.run_macro_commander(),        
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