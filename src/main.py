import os
import sys
import time
import asyncio
import logging
import uuid
import traceback
import random
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

class DistributedQuantEngine:
    def __init__(self):
        load_dotenv()
        
        # ====================================================================
        # 🟢 LIVE PRODUCTION MODE ACTIVE
        # ====================================================================
        self.test_mode = False 
        
        if self.test_mode:
            logger.critical("⚠️ SYSTEM INITIALIZED IN TEST MODE (GHOST TRADING SIMULATION ACTIVE) ⚠️")
        else:
            logger.critical("🟢 SYSTEM INITIALIZED IN LIVE PRODUCTION MODE. CAPITAL DEPLOYMENT ARMED.")
        
        self.asset_basket: List[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        self.timeframe = os.getenv("TRADING_TIMEFRAME", "15")
        
        # 🚀 THE SHADOW SWARM: Massive surface area to feed the FSM
        self.shadow_basket: List[str] = []
        self.shadow_cooldown: Dict[str, float] = {}
        self.shadow_resolution_tracker: Dict[str, Dict[str, Any]] = {}
        
        self.stream_restart_event = asyncio.Event()
        
        self.memory = MemoryBank()
        
        # 🚀 UPGRADE 2: Enforce a strict professional horizon limit base
        self.min_horizon_floor = 150
        self.fsm = SystemStateMachine(accuracy_threshold=0.60, warmup_epochs=self.min_horizon_floor)
        
        # 🛡️ Adjusted limits to 25% max drawdown and 15% risk per position
        self.risk_vault = InstitutionalRiskVault(max_drawdown_pct=0.25, max_single_position_risk_pct=0.15)
        
        self.feature_engines: Dict[str, AdaptiveFeatureEngine] = {s: AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600) for s in self.asset_basket}
        self.macro_regimes: Dict[str, str] = {s: "HOLD" for s in self.asset_basket}
        self.macro_confidences: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.current_atrs: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.last_execution_timestamps: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.screener_memory: Dict[str, Dict[str, List[float]]] = {s: {"prices": [], "volumes": []} for s in self.asset_basket}
        
        # 📊 LIVE METRICS STORAGE: Holds real context to feed the AI Router
        self.screener_metrics: Dict[str, Dict[str, float]] = {s: {"vol_mult": 1.0, "vol_z": 0.0} for s in self.asset_basket}
        
        # 📦 BATCHING QUEUE: Holds data from the workers until the Commander is ready
        self.pending_macro_payloads: Dict[str, dict] = {}
        
        self.active_workers: Dict[str, asyncio.Task] = {}
        
        # 🛡️ POSITION LOCK DEPLOYMENT MATRIX
        self.active_positions_lock = set()
        
        # 🚀 TICK SIZE MATRIX
        self.tick_sizes: Dict[str, float] = {}
        
        # Fallback global cooldown (Overridden dynamically per asset in execution loop)
        self.execution_cooldown_period = 10.0 if self.test_mode else 900.0  
        
        self.historical_win_rate = 0.58
        self.historical_win_loss_ratio = 1.65

        # 🚀 THE SINGLETON CACHE: Stops event loop strangulation. 
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
        """
        🚀 INSTITUTIONAL UPGRADE: Fetches the absolute source of truth for 
        price precision directly from Bybit's matching engine.
        """
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

    # ====================================================================
    # 🧠 UPGRADE 1: THE ADAPTIVE MEMORY HORIZON (Exponential Scaling)
    # ====================================================================
    def compute_dynamic_memory_window(self, vol_mult: float) -> int:
        """
        Calculates how many trades the FSM needs to prove edge.
        Uses quadratic compression to aggressively shrink the window during
        parabolic volume events, but respects the new minimum horizon floor.
        """
        normalized = min(2.0, vol_mult) / 2.0
        compressed = 1.0 - (normalized * normalized * 0.5)
        target_window = int(250 * max(0.4, compressed))
        return max(self.min_horizon_floor, min(300, target_window))

    # ====================================================================
    # 🧠 THE TRUE INSTITUTIONAL REGIME ATTENUATOR
    # ====================================================================
    def calculate_adaptive_regime_parameters(self, market_regime: str, metrics: dict) -> dict:
        """
        Organically scales execution barriers and cooldowns based strictly on volume.
        Maintains absolute mathematical rigor to ensure training data is valid.
        """
        vol_mult = float(metrics.get("vol_mult", 1.0))

        optimized = {
            "cooldown_period": 300.0,
            "z_score_threshold": 2.2, # Baseline target hardened to suppress chop
            "position_scaling": 1.0,
            "sl_multiplier": 1.5,
            "tp_multiplier": 2.0,
            "execution_verdict": True
        }

        if market_regime == "RANGING":
            optimized["z_score_threshold"] = 2.4 # Strict micro-structure filtering for ranging markets
            optimized["cooldown_period"] = 900.0 # 🚀 Slow down: 15-minute cooldown prevents fee churn
            optimized["sl_multiplier"] = 1.2     # 🚀 Tighten the leash: Cut losses instantly
            optimized["tp_multiplier"] = 1.6     # 🚀 Positive R:R: You make more when you win
            if vol_mult < 0.5: # 🛡️ Lowered floor to 0.5x to prevent blocking the Velocity Gate
                optimized["execution_verdict"] = False
        elif market_regime == "TRENDING":
            optimized["z_score_threshold"] = 1.9 
            optimized["cooldown_period"] = 120.0  
            optimized["tp_multiplier"] = 2.5     
            
        return optimized

    # ==========================================
    # THREAD 1A: THE MACRO COMMANDER (BATCHER)
    # ==========================================
    async def run_macro_commander(self):
        logger.info("🧠 MACRO COMMANDER ONLINE. Waiting for workers to gather data...")
        while True:
            await asyncio.sleep(60) 
            if not self.pending_macro_payloads:
                continue
                
            batch_payload = dict(self.pending_macro_payloads)
            try:
                global_news = "No significant macro shifts detected."
                if len(batch_payload) > 0:
                    first_sym = list(batch_payload.keys())[0]
                    if "macro_news_stream" in batch_payload[first_sym]:
                        global_news = batch_payload[first_sym]["macro_news_stream"]

                compressed_matrix = ""
                for sym, data in batch_payload.items():
                    compressed_matrix += f"[{sym}] P:{data['price']:.4f} ATR:{data['atr_volatility']:.4f} V_Mult:{data['volume_multiplier']}x Z_Vol:{data['volatility_z_score']} ACC:{data['rolling_system_accuracy']} | "
                    
                llm_payload = {
                    "GLOBAL_MACRO_CONTEXT": global_news,
                    "QUANTITATIVE_ASSET_MATRIX": compressed_matrix
                }
                
                try:
                    logger.info(f"🚀 Routing SINGLE Macro-Batch ({len(batch_payload)} assets) to DeepSeek API...")
                    verdict_matrix = await asyncio.wait_for(
                        self.ai_router.extract_market_verdict(llm_payload),
                        timeout=30.0 
                    )
                    
                    if isinstance(verdict_matrix, dict):
                        for symbol, data in verdict_matrix.items():
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

    # ==========================================
    # THREAD 1B: THE IMMORTAL WATCHDOG
    # ==========================================
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
                        exc = task.exception()
                        logger.error(f"☠️ WATCHDOG FATAL ALERT: {symbol} worker died from unhandled exception:")
                        logger.error(f"\n{''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}")
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
                context = await asyncio.wait_for(
                    self.macro_data_feed.fetch_market_snapshot(symbol, self.timeframe),
                    timeout=8.0
                )
                if not context:
                    await asyncio.sleep(30)
                    continue
                
                current_price = context["current_price"]
                feature_engine = self.feature_engines.get(symbol)
                self.current_atrs[symbol] = current_price * 0.0045
                
                metrics = self.screener_metrics.get(symbol, {"vol_mult": 1.0, "vol_z": 0.0})
                
                # 🚀 SHIELD CACHE FIX: No database queries are permitted here. Reads instantly from memory.
                rolling_acc = self.global_state_cache.get("rolling_accuracy", 0.50)
                
                self.pending_macro_payloads[symbol] = {
                    "asset": symbol,
                    "price": current_price,
                    "atr_volatility": self.current_atrs[symbol],
                    "volume_multiplier": round(metrics.get("vol_mult", 1.0), 2),
                    "volatility_z_score": round(metrics.get("vol_z", 0.0), 2),
                    "macro_news_stream": context["news_context"],
                    "rolling_system_accuracy": f"{rolling_acc:.2%}" 
                }
            except asyncio.TimeoutError:
                logger.error(f"⏳ DATA WORKER TIMEOUT: API hung on {symbol}. Loop will retry.")
            except Exception as e:
                logger.error(f"⚠️ DATA WORKER ERROR: Exception on {symbol} loop: {e}")
                
            await asyncio.sleep(random.uniform(45.0, 75.0))

    # ==========================================
    # THREAD 2: FAST MICROSTRUCTURE PIPELINE
    # ==========================================
    async def handle_incoming_orderbook_tick(self, depth_data: Dict[str, Any]):
        symbol = depth_data.get("s")
        if symbol not in self.asset_basket: return

        bids = depth_data.get("b", [])
        asks = depth_data.get("a", [])
        features = self.feature_engines[symbol].push_orderbook_tick(bids, asks)
        if not features.get("valid"): return

        z_obi = features["adaptive_obi_z"]
        mid_price = features["mid_price"]
        market_regime = features.get("market_regime", "RANGING")
        
        # 🚀 EXTRACT REAL EMA SPREAD FROM FEATURE ENGINE
        real_spread = features.get("bid_ask_spread", mid_price * 0.0005)

        metrics = self.screener_metrics.get(symbol, {"vol_mult": 1.0, "vol_z": 0.0})
        optimization = self.calculate_adaptive_regime_parameters(market_regime, metrics)

        if not optimization["execution_verdict"]: return

        current_time = time.time()
        if (current_time - self.last_execution_timestamps.get(symbol, 0)) < optimization["cooldown_period"]:
            return

        effective_z_threshold = optimization["z_score_threshold"]
        vol_mult = metrics.get("vol_mult", 1.0)

        # 🚀 GATE 1: THE VELOCITY STRIKE (Leading Indicator)
        gate_1_velocity = (abs(z_obi) >= effective_z_threshold) and (vol_mult >= 0.8)

        # 🚀 GATE 2: THE ACCUMULATION GRIND (Lagging Indicator)
        gate_2_accumulation = (vol_mult >= 1.5) and (abs(z_obi) >= 1.4)

        has_pure_edge = gate_1_velocity or gate_2_accumulation

        trade_direction = None
        is_active = self.fsm.current_state in [TradingState.ACTIVE_TRADING, TradingState.ACTIVE_MEAN_REVERSION]

        if self.test_mode or not is_active:
            if has_pure_edge:  
                trade_direction = "BUY" if z_obi > 0 else "SELL"
        else:
            # 🚀 ASYMMETRIC VETO MODEL
            regime = self.macro_regimes.get(symbol, "HOLD")
            if has_pure_edge:
                if z_obi > 0 and regime != "SELL":  
                    trade_direction = "BUY"
                elif z_obi < 0 and regime != "BUY":
                    trade_direction = "SELL" 

        if trade_direction:
            self.last_execution_timestamps[symbol] = current_time
            mode_label = "🔥 LIVE" if is_active else "👻 GHOST"
            logger.critical(f"{mode_label} PURE EDGE DETECTED // Node: {symbol} | Regime: {market_regime} | Z-OBI: {z_obi:.2f} | Dynamic Target: {effective_z_threshold:.2f}")
            
            # 🚀 PASS REAL SPREAD TO SIGNAL LIFECYCLE
            asyncio.create_task(self.run_signal_lifecycle(symbol, trade_direction, mid_price, optimization, real_spread))

    # ==========================================
    # THREAD 3: LIGHTWEIGHT BASKET TRACKER
    # ==========================================
    async def handle_incoming_basket_screener_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if not symbol or symbol not in self.asset_basket: return

        price_str = data.get("lastPrice")
        volume_str = data.get("turnover24h")
        if price_str is None or volume_str is None: return

        current_price = float(price_str)
        current_volume = float(volume_str)

        if symbol not in self.screener_memory:
            self.screener_memory[symbol] = {"prices": [], "volumes": []}

        history = self.screener_memory[symbol]
        history["prices"].append(current_price)
        history["volumes"].append(current_volume)

        if len(history["prices"]) > 60:
            history["prices"].pop(0)
            history["volumes"].pop(0)

        if len(history["prices"]) < 15: return

        prices_array = np.array(history["prices"])
        volumes_array = np.array(history["volumes"])

        mean_volume = np.mean(volumes_array[:-1]) if len(volumes_array) > 1 else 1.0
        volume_multiplier = current_volume / mean_volume if mean_volume > 0 else 1.0

        returns = np.diff(np.log(prices_array))
        mean_return = np.mean(returns) if len(returns) > 0 else 0.0
        std_return = np.std(returns) if len(returns) > 0 else 1e-6
        current_return = returns[-1] if len(returns) > 0 else 0.0
        volatility_z = abs((current_return - mean_return) / std_return) if std_return > 0 else 0.0

        self.screener_metrics[symbol] = {
            "vol_mult": float(volume_multiplier),
            "vol_z": float(volatility_z)
        }

    # ==========================================
    # THREAD 4: KLINE AGGREGATION PIPELINE
    # ==========================================
    async def handle_incoming_kline_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if symbol not in self.asset_basket: return
        interval = data["interval"]
        candle = data["candle_data"]
        self.feature_engines[symbol].update_multi_timeframe_candle(
            timeframe=interval,
            open_p=float(candle.get("open", 0)),
            high_p=float(candle.get("high", 0)),
            low_p=float(candle.get("low", 0)),
            close_p=float(candle.get("close", 0)),
            volume=float(candle.get("volume", 0))
        )

    # ==========================================
    # THREAD 5: THE GLOBAL SATELLITE RADAR
    # ==========================================
    async def run_universe_refresher(self):
        while True:
            await asyncio.sleep(14400) 
            logger.info("🌍 GLOBAL SATELLITE SCAN INITIATED. Querying Bybit endpoints for volatility targets...")
            
            # 🚀 Update the global tick size matrix dynamically
            await self._fetch_exchange_tick_sizes()
            
            full_market = await self.executor.get_top_volatile_assets(limit=100, min_turnover=10_000_000)
            
            if len(full_market) < 15:
                logger.warning("Dynamic satellite scan returned insufficient asset velocity metrics. Maintaining current tracking universe.")
                continue
                
            self.asset_basket = full_market[:15]
            self.shadow_basket = full_market[15:]
            
            # 🚀 FALLBACK PROTECTION: Ensure shadow basket never runs dry
            if len(self.shadow_basket) < 10:
                fallback_shadow = ["XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT"]
                self.shadow_basket.extend([s for s in fallback_shadow if s not in self.shadow_basket])
                logger.warning(f"⚠️ Appended fallback shadow basket to ensure data volume.")
            
            new_feature_engines, new_screener_memory, new_macro_regimes = {}, {}, {}
            new_macro_confidences, new_current_atrs, new_last_execs, new_screener_metrics = {}, {}, {}, {}
            
            for s in self.asset_basket:
                new_feature_engines[s] = self.feature_engines.get(s, AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600))
                new_screener_memory[s] = self.screener_memory.get(s, {"prices": [], "volumes": []})
                new_macro_regimes[s] = self.macro_regimes.get(s, "HOLD")
                new_macro_confidences[s] = self.macro_confidences.get(s, 0.0)
                new_current_atrs[s] = self.current_atrs.get(s, 0.0)
                new_last_execs[s] = self.last_execution_timestamps.get(s, 0.0)
                new_screener_metrics[s] = self.screener_metrics.get(s, {"vol_mult": 1.0, "vol_z": 0.0})
                
            self.feature_engines = new_feature_engines
            self.screener_memory = new_screener_memory
            self.macro_regimes = new_macro_regimes
            self.macro_confidences = new_macro_confidences
            self.current_atrs = new_current_atrs
            self.last_execution_timestamps = new_last_execs
            self.screener_metrics = new_screener_metrics
            
            logger.info(f"🌌 QUANT UNIVERSE MATRIX RE-CALIBRATED. Operational Hunt Targets: {', '.join(self.asset_basket)}")
            logger.info(f"🦇 SHADOW SWARM RE-CALIBRATED. {len(self.shadow_basket)} background assets active.")
            
            for old_symbol in list(self.active_workers.keys()):
                if old_symbol not in self.asset_basket:
                    self.active_workers[old_symbol].cancel()
                    del self.active_workers[old_symbol]
                    self.pending_macro_payloads.pop(old_symbol, None)
                    
            for new_symbol in self.asset_basket:
                if new_symbol not in self.active_workers:
                    task = asyncio.create_task(self._asset_data_gatherer_lifecycle(new_symbol))
                    self.active_workers[new_symbol] = task
            
            self.stream_restart_event.set()

    # ==========================================
    # 👻 NEW THREAD: THE SHADOW SWARM SCANNER
    # ==========================================
    def _detect_shadow_regime(self, closes: np.ndarray) -> str:
        """Lightweight regime detection for shadow assets to maintain data purity"""
        if len(closes) < 30:
            return "RANGING"
        directional_change = abs(closes[-1] - closes[0])
        absolute_changes = np.sum(np.abs(np.diff(closes)))
        er = directional_change / absolute_changes if absolute_changes > 0 else 0.0
        
        sma = np.mean(closes)
        std_dev = np.std(closes)
        bb_width = (4 * std_dev) / sma if sma > 0 else 0.0
        
        return "TRENDING" if er >= 0.35 and bb_width >= 0.004 else "RANGING"

    async def run_shadow_swarm_scanner(self):
        """
        🦇 SHADOW SWARM: Scans 85+ assets for pure 2.0+ Z-Score setups
        Feeds the FSM with uncompromised, institutional-grade data
        """
        logger.critical("🦇 SHADOW SWARM ONLINE. Hunting for pure data across extended universe...")
        
        while True:
            await asyncio.sleep(120) # 2 minute scan cycle
            
            if not self.shadow_basket:
                continue
                
            # Process in batches to respect rate limits
            BATCH_SIZE = 10
            for i in range(0, len(self.shadow_basket), BATCH_SIZE):
                batch = self.shadow_basket[i:i+BATCH_SIZE]
                
                for symbol in batch:
                    try:
                        # Cooldown check
                        if symbol in self.shadow_cooldown:
                            if time.time() - self.shadow_cooldown[symbol] < 300:
                                continue
                                
                        # Fetch 15m candles
                        klines = await asyncio.to_thread(
                            self.executor.client.get_kline,
                            category="linear", symbol=symbol, interval="15", limit=60
                        )
                        
                        data = klines.get("result", {}).get("list", [])
                        if len(data) < 30: continue

                        closes = np.array([float(k[4]) for k in data])[::-1]
                        volumes = np.array([float(k[5]) for k in data])[::-1]
                        
                        current_price = closes[-1]
                        if current_price <= 0.01: continue
                        
                        current_vol = volumes[-1]
                        avg_vol = np.mean(volumes[:-1]) if len(volumes) > 1 else 1.0
                        vol_mult = current_vol / avg_vol if avg_vol > 0 else 1.0
                        
                        # Volatility Z-Score
                        returns = np.diff(np.log(closes))
                        mean_return = np.mean(returns) if len(returns) > 0 else 0.0
                        std_return = np.std(returns) if len(returns) > 0 else 1e-6
                        vol_z = abs((returns[-1] - mean_return) / (std_return + 1e-6))
                        
                        # 🚀 DUAL-GATE ALPHA TRIGGER INTEGRATION
                        gate_1_shadow = (vol_z >= 2.2) and (vol_mult >= 0.8)
                        gate_2_shadow = (vol_mult >= 1.5) and (vol_z >= 1.4)
                        
                        if gate_1_shadow or gate_2_shadow:
                            direction = "BUY" if returns[-1] < 0 else "SELL"
                            logger.critical(f"🦇 [SHADOW HIT] {symbol} | Vol: {vol_mult:.2f}x | Z: {vol_z:.2f}")
                            
                            # Detect regime for accuracy tracking
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
                                features_dict
                            )
                            
                            # Update trackers
                            self.shadow_cooldown[symbol] = time.time()
                            
                    except Exception as e:
                        logger.debug(f"Shadow scan failed for {symbol}: {e}")
                    
                    await asyncio.sleep(0.5) # 🚀 UPGRADED: Throttled to completely bypass Bybit 10006 Rate Limits
                await asyncio.sleep(1) # Batch pause

    # ==========================================
    # THREAD 6: HIGH-VELOCITY NETWORK CONNECTOR
    # ==========================================
    async def stream_manager_loop(self):
        while True:
            intervals_matrix = ["1", "5", "15"]
            stream_feed = HighVelocityMultiFeed(
                basket=self.asset_basket,
                intervals=intervals_matrix,
                orderbook_callback=self.handle_incoming_orderbook_tick,
                screener_callback=self.handle_incoming_basket_screener_update,
                kline_callback=self.handle_incoming_kline_update
            )
            
            stream_task = asyncio.create_task(stream_feed.initialize_multiplexed_stream())
            await self.stream_restart_event.wait()
            stream_task.cancel()
            self.stream_restart_event.clear()
            logger.info("♻️ Structural data multiplexers systematically torn down to process hot-universe mutation.")
            await asyncio.sleep(2)

    # ==========================================
    # THREAD 7: SYSTEM HEARTBEAT & DIAGNOSTICS (THE GLOBAL COMMANDER)
    # ==========================================
    async def run_system_heartbeat(self):
        start_time = time.time()
        loop_counter = 0
        
        while True:
            await asyncio.sleep(60) 
            loop_counter += 1
            uptime_hours = (time.time() - start_time) / 3600
            
            # 🚀 1. BATCHED DB RESOLUTION: Reduces DB network strangulation from 15 requests to 1
            if loop_counter % 60 == 0:
                try:
                    logger.info("⚡ Executing unified database resolution sweep across asset array...")
                    age_cutoff_time = time.time() - 3600 # 1-Hour Decay Fix applied globally
                    
                    # Consolidate all assets that have active price streams
                    valid_assets = [sym for sym in self.asset_basket if self.screener_memory.get(sym, {}).get("prices")]
                    
                    if valid_assets:
                        # Construct mapped dict for current prices to avoid internal lookup delays
                        current_prices = {sym: self.screener_memory[sym]["prices"][-1] for sym in valid_assets}
                        
                        # Cross reference shadow matrix space as well to capture full coverage
                        for s, data in self.shadow_cooldown.items():
                            if s not in current_prices: 
                                current_prices[s] = self.screener_memory.get(s, {}).get("prices", [0.0])[-1]
                        
                        # Direct, singular call to the memory unit using batched execution
                        await asyncio.to_thread(
                            self.memory.resolve_batch_historical_predictions,
                            assets=list(current_prices.keys()),
                            current_prices=current_prices,
                            age_cutoff=age_cutoff_time
                        )
                except Exception as e:
                    logger.error(f"❌ Failed to execute batched prediction validation: {str(e)}")

            logger.info(f"💓 SWARM HEARTBEAT: Matrix is active. Uptime: {uptime_hours:.2f} hours. AI Queue: {len(self.pending_macro_payloads)} assets ready.")

            if loop_counter % 60 == 0:
                avg_vol_mult = np.mean([m.get("vol_mult", 1.0) for m in self.screener_metrics.values()]) if self.screener_metrics else 1.0
                avg_dynamic_window = self.compute_dynamic_memory_window(avg_vol_mult)
                
                # 🚀 SINGLETON CACHE ASSIGNMENT
                accuracy, pool_size = self.memory.compute_rolling_accuracy(window_size=avg_dynamic_window)
                
                self.global_state_cache["rolling_accuracy"] = accuracy
                self.global_state_cache["total_resolved"] = pool_size
                self.global_state_cache["dynamic_window"] = avg_dynamic_window
                self.global_state_cache["last_updated"] = time.time()
                
                self.fsm.warmup_epochs = avg_dynamic_window
                
                # 🛡️ FSM REGRESSION GATE (OVERRIDE ACTIVATED)
                if accuracy < 0.60:
                    self.fsm.current_state = TradingState.CALIBRATING
                    logger.critical(f"📉 EDGE DECAY OVERRIDE: Accuracy {accuracy:.2%} is below 60% floor. Engine locked to CALIBRATING.")
                else:
                    self.fsm.process_state_transition(accuracy, pool_size, "RANGING") 

                state = self.fsm.current_state.value
                current_vault_balance = await self.executor.get_wallet_balance_usdt()
                
                initial_baseline = 7.80
                drawdown_pct = max(0.0, (initial_baseline - current_vault_balance) / initial_baseline)
                
                bar_length = 10
                filled_blocks = min(bar_length, int(drawdown_pct * bar_length))
                drawdown_bar = "🟢" * (bar_length - filled_blocks) + "🔴" * filled_blocks

                try:
                    response = self.memory.supabase.table("quantitative_ledger")\
                        .select("market_regime, net_pnl")\
                        .eq("resolved", True)\
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
                        regime_breakdown_text = "• <i>No resolved metrics recorded in this epoch yet.</i>\n"
                except Exception as db_err:
                    logger.error(f"Failed to compile Supabase data for Telegram report: {db_err}")
                    net_pnl = 0.0
                    regime_breakdown_text = "• ⚠️ <i>Supabase ledger context temporarily loading...</i>\n"

                report = (
                    f"📊 <b>PJMASK EMPIRE ADVANCED QUANT PULSE</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏱ <b>Engine Run Uptime:</b> <code>{uptime_hours:.2f} Hours</code>\n"
                    f"🎛 <b>FSM State Gear:</b> <code>{state}</code>\n"
                    f"🎯 <b>Rolling Edge Accuracy:</b> <code>{accuracy:.2%}</code>\n"
                    f"📏 <b>Active Memory Horizon:</b> <code>{avg_dynamic_window} Trades Required</code>\n"
                    f"🏊‍♂️ <b>Database Validation Pool:</b> <code>{pool_size} Resolved</code>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"💳 <b>Net Wallet Liquidity:</b> <code>{current_vault_balance:.4f} USDT</code>\n"
                    f"📈 <b>Net Realized Return:</b> <code>{net_pnl:+.4f} USDT</code>\n"
                    f"📉 <b>Drawdown Profile Status:</b> <code>{drawdown_pct:.2%}</code>\n"
                    f"🎚 <b>Risk Horizon Bar:</b>\n<code>[{drawdown_bar}]</code>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔬 <b>FORENSIC REGIME PROFILE:</b>\n"
                    f"{regime_breakdown_text}"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📡 <b>Operational Swarm Nodes:</b> <code>{len(self.asset_basket)} Live | {len(self.shadow_basket)} Shadow</code>\n"
                    f"🧠 <b>AI Inference Framework:</b> <code>DeepSeek V4 (Native Cloud)</code>"
                )
                asyncio.create_task(self.telegram.send_html_report(report))

    # ==========================================
    # CORE ORCHESTRATOR: END-TO-END TRADE LIFECYCLE
    # ==========================================
    # 🚀 Update the parameters to include tick_size
    def calculate_initial_bracket(self, entry_price: float, atr: float, side: str, leverage: int, vol_z: float = 0.0, optimization: dict = None, tick_size: float = 0.0001):
        if optimization is None:
            optimization = {"sl_multiplier": 1.5, "tp_multiplier": 2.0}
            
        fee_drag_factor = 0.00055 * 2 * leverage
        fee_buffer = entry_price * fee_drag_factor
        
        tp_multiplier = optimization["tp_multiplier"]
        sl_multiplier = optimization["sl_multiplier"]
        
        if vol_z >= 3.0:
            tp_multiplier = max(tp_multiplier, 4.5)
            logger.info(f"📈 EXTREME VOLATILITY DETECTED (Z: {vol_z:.2f}). Stretching TP to {tp_multiplier}x ATR.")
        elif vol_z >= 1.5:
            tp_multiplier = max(tp_multiplier, 3.0)
            logger.info(f"📊 ELEVATED VOLATILITY DETECTED (Z: {vol_z:.2f}). Stretching TP to {tp_multiplier}x ATR.")
        
        if side.upper() == "BUY":
            initial_sl = entry_price - (sl_multiplier * atr)
            target_tp = entry_price + (tp_multiplier * atr) + fee_buffer
        else:  
            initial_sl = entry_price + (sl_multiplier * atr)
            target_tp = entry_price - (tp_multiplier * atr) - fee_buffer
            
        # ====================================================================
        # 🚀 INSTITUTIONAL FIX: Exact Tick Size Quantization
        # Maps the mathematical bracket perfectly to the exchange's required grid
        # ====================================================================
        tick_dec = Decimal(str(tick_size))
        sl_dec = Decimal(str(initial_sl))
        tp_dec = Decimal(str(target_tp))
        
        # Divide by tick step, round to nearest integer, multiply by tick step
        snapped_sl = (sl_dec / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec
        snapped_tp = (tp_dec / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec
        
        # Final formatting cleanup to ensure Bybit's API parser accepts it natively
        final_sl = float(snapped_sl.quantize(tick_dec))
        final_tp = float(snapped_tp.quantize(tick_dec))
        
        return final_sl, final_tp

    # 🚀 THE MASTER FIX: Add real_spread to the function signature
    async def run_signal_lifecycle(self, symbol: str, direction: str, current_price: float, optimization: dict = None, real_spread: float = 0.0):
        if optimization is None:
            optimization = {"position_scaling": 1.0, "sl_multiplier": 1.5, "tp_multiplier": 2.0}
            
        if symbol in self.active_positions_lock: 
            logger.warning(f"🔒 Guard execution bypass [{symbol}]: Signal ignored to prevent position stacking collision.")
            return False
            
        self.active_positions_lock.add(symbol)
        
        try:
            signal_id = str(uuid.uuid4())
            confidence = self.macro_confidences.get(symbol, 0.0)
            
            feature_engine = self.feature_engines.get(symbol)
            market_regime = feature_engine.detect_market_regime() if feature_engine else "RANGING"
            atr = feature_engine.get_computed_atr() if feature_engine and hasattr(feature_engine, 'get_computed_atr') else current_price * 0.015
            self.current_atrs[symbol] = atr

            metrics = self.screener_metrics.get(symbol, {})
            vol_mult = metrics.get("vol_mult", 1.0)
            
            features_dict = {
                "symbol": symbol,
                "market_regime": market_regime,
                "adaptive_obi_z": 0.0, 
                "liquidity_density_ratio": vol_mult,
                "bid_ask_spread": real_spread # 🚀 Save the real spread to the database
            }
            
            self.memory.commit_prediction(signal_id, time.time(), current_price, direction, confidence, features_dict)
            
            rolling_acc = self.global_state_cache["rolling_accuracy"]
            dynamic_window = self.global_state_cache["dynamic_window"]

            is_whitelisted_state = self.fsm.current_state in [TradingState.ACTIVE_TRADING, TradingState.ACTIVE_MEAN_REVERSION]
            has_institutional_edge = rolling_acc >= 0.60
            
            if not self.test_mode and (not is_whitelisted_state or not has_institutional_edge):
                logger.critical(
                    f"👻 [SHIELD ACTIVE] Routing to Ghost Simulation -> Node: {symbol} | "
                    f"Accuracy: {rolling_acc:.2%} (Floor: 60%) | Target Dynamic Memory: {dynamic_window} trades"
                )
                self.active_positions_lock.discard(symbol)
                return True
            
            if self.test_mode:
                logger.critical(f"🧪 [SIMULATION SUCCESS] Ghost trade committed -> Node: {symbol} | ID: {signal_id[:8]} | Dir: {direction}")
                self.active_positions_lock.discard(symbol)
                return True 

            balance = await self.executor.get_wallet_balance_usdt()
            risk_matrix = self.risk_vault.compute_variance_adjusted_kelly(
                account_balance=balance, win_rate=self.historical_win_rate,
                win_loss_ratio=self.historical_win_loss_ratio, asset_volatility_atr=atr,
                current_price=current_price, ai_confidence=confidence, market_regime=market_regime
            )
            
            if not risk_matrix["approved"] or risk_matrix["size"] <= 0.0:
                logger.warning(f"Execution canceled for {symbol}. Kelly criteria failed.")
                self.active_positions_lock.discard(symbol)
                return False

            scaled_allocation = risk_matrix["allocated_value_usdt"] * optimization["position_scaling"]
            final_allocation = max(self.risk_vault.exchange_min_notional, scaled_allocation)
            risk_matrix["allocated_value_usdt"] = final_allocation
            risk_matrix["size"] = final_allocation / current_price

            if not self.risk_vault.evaluate_portfolio_safety(balance, risk_matrix['allocated_value_usdt'], symbol):
                logger.warning(f"Global portfolio safety boundary breached for {symbol}.")
                self.active_positions_lock.discard(symbol)
                return False

            logger.critical(f"🎯 RISK CLEARANCE GRANTED // Node: {symbol} | Scaled Notional Size: {risk_matrix['allocated_value_usdt']:.2f} USDT.")
            
            target_leverage = risk_matrix.get("recommended_leverage", 1)
            leverage_success = await self.executor.adjust_leverage(symbol, target_leverage)
            
            if not leverage_success:
                logger.error(f"Execution aborted. Failed to set required leverage.")
                self.active_positions_lock.discard(symbol)
                return False
            
            vol_z = metrics.get("vol_z", 0.0)
            
            # 🚀 Fetch the exact legal tick step for this specific coin
            tick_size = self.tick_sizes.get(symbol, 0.0001) 
            
            initial_sl, target_tp = self.calculate_initial_bracket(
                current_price, atr, direction, target_leverage, vol_z, optimization, tick_size
            )
            
            # =========================================================================
            # 🚀 INSTITUTIONAL UPGRADE: Dynamic Total Cost of Execution (TCE) Model
            # =========================================================================
            if vol_mult < 0.3: 
                logger.warning(f"❌ TRADE SKIPPED // {symbol} volume critically low ({vol_mult:.2f}x). Halting to prevent liquidity trap.")
                self.active_positions_lock.discard(symbol)
                return False

            # Bybit 0.055% maker/taker fee is already buffered inside target_tp
            expected_profit = target_tp - current_price if direction == "BUY" else current_price - target_tp
            
            # Ensure real_spread has a safe minimum boundary (1 basis point) to prevent divide-by-zero math errors
            safe_spread = max(real_spread, current_price * 0.0001) 
            
            # 🚀 The Master Formula: Slippage penalty expands geometrically when volume dries up
            # If volume is 2.0x, penalty is half the spread. If volume is 0.5x, penalty is double the spread.
            slippage_penalty = safe_spread * (1.0 / max(0.4, vol_mult))
            total_friction = safe_spread + slippage_penalty
            
            if expected_profit < total_friction:
                logger.warning(f"❌ TRADE SKIPPED // {symbol} Expected profit ({expected_profit:.4f}) cannot clear dynamic structural friction ({total_friction:.4f}). Spread: {safe_spread/current_price:.3%} | Vol: {vol_mult:.2f}x")
                self.active_positions_lock.discard(symbol)
                return False
            # =========================================================================

            current_depth = {"bids": [[current_price]], "asks": [[current_price]]}
            if hasattr(feature_engine, 'get_orderbook_snapshot'):
                current_depth = feature_engine.get_orderbook_snapshot()

            if market_regime == "TRENDING":
                execution_success = await self.sor.execute_iceberg_block(
                    symbol=symbol, direction=direction, total_qty=risk_matrix["size"],
                    current_mid_price=current_price, stop_loss=initial_sl, take_profit=target_tp,
                    depth_snapshot=current_depth
                )
            else:
                execution_success = await self.sor.execute_mean_reversion_bracket(
                    symbol=symbol, direction=direction, total_qty=risk_matrix["size"],
                    current_mid_price=current_price, stop_loss=initial_sl, take_profit=target_tp,
                    depth_snapshot=current_depth
                )
            
            if not execution_success:
                logger.error(f"❌ SIGNAL EXECUTION ABORTED // Capital safely retained for {symbol}.")
                self.active_positions_lock.discard(symbol)
                return False 
                
            self.risk_vault.update_position_ledger(symbol, risk_matrix['allocated_value_usdt'])
            logger.critical(f"🔥 POSITION CONFIRMED LIVE // Monitoring execution loops armed for {symbol}.")
            
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
                symbol, signal_id, direction, current_price, initial_sl, atr, risk_matrix, feature_engine
            ))
            
            return True

        except Exception as e:
            logger.error(f"Distributed swarm execution routing failed for {symbol}: {e}")
            self.active_positions_lock.discard(symbol)
            return False

    async def _position_lifecycle_daemon(self, symbol: str, signal_id: str, direction: str, current_price: float, initial_sl: float, atr: float, risk_matrix: dict, feature_engine):
        logger.info(f"👻 EXCH MONITOR ARMED // Daemon injected for node {symbol}")
        polling_interval = 4  
        start_time = time.time()
        order_filled = False
        
        current_hard_stop = initial_sl
        peak_observed_price = current_price
        activation_threshold = atr * 1.0  
        minimum_api_step = atr * 0.4      

        while True:
            await asyncio.sleep(polling_interval)
            try:
                pos_response = await asyncio.to_thread(
                    self.executor.client.get_positions,
                    category="linear", symbol=symbol
                )
                positions = pos_response.get("result", {}).get("list", [])
                has_active_position = False
                if positions:
                    size = float(positions[0].get("size", 0.0))
                    if size > 0:
                        has_active_position = True
                        order_filled = True 
            except Exception as e:
                logger.error(f"Failed to verify live position matrix status for {symbol}: {e}")
                has_active_position = False

            if not order_filled and (time.time() - start_time) > 180:
                logger.warning(f"⏳ LIMIT TIMEOUT // {symbol} order remained untriggered for 3 minutes. Purging...")
                try:
                    await asyncio.to_thread(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
                except Exception as e:
                    logger.error(f"Order cleanup pipeline failed for {symbol}: {e}")
                
                self.risk_vault.update_position_ledger(symbol, -risk_matrix['allocated_value_usdt'])
                self.active_positions_lock.discard(symbol)
                logger.critical(f"🔓 PORTFOLIO UNLOCKED // Stale exposure dissolved for {symbol}. Hunting lines re-armed.")
                break

            if order_filled and not has_active_position:
                settlement = await self.executor.check_recent_settlement(symbol=symbol, lookback_seconds=30)
                if settlement.get("closed"):
                    logger.critical(f"🏁 POSITION TERMINATION DETECTED // Symbol: {symbol}")
                    net_pnl = float(settlement.get('pnl', 0.0))
                    entry_px = float(settlement.get('entry', current_price))
                    exit_px = float(settlement.get('exit', current_price))
                    slippage_drag = entry_px - current_price if direction == "BUY" else current_price - entry_px
                    
                    report_message = (
                        f"🔔 <b>EXCHANGE EXECUTION TERMINATION ALERT</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 <b>Asset Node:</b> <code>{symbol}</code>\n"
                        f"📊 <b>Outcome Verdict:</b> " + ("🟢 PROFIT" if net_pnl > 0 else "🔴 LOSS") + f"\n"
                        f"💰 <b>Net Session Return:</b> <code>{net_pnl:.4f} USDT</code>\n"
                        f"⚡ <b>Slippage Footprint:</b> <code>{slippage_drag:.4f} Price Units</code>\n"
                        f"⚙️ <b>Execution Trailing Method:</b> <code>Kinetic Asymmetric Lock</code>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━"
                    )
                    asyncio.create_task(self.telegram.send_html_report(report_message))
                    self.memory.log_live_execution_result(signal_id, net_pnl, slippage_drag, settlement['outcome'])
                    self.risk_vault.update_position_ledger(symbol, -risk_matrix['allocated_value_usdt'])
                    self.active_positions_lock.discard(symbol)
                    break
            
            if has_active_position:
                live_mid = feature_engine.get_latest_mid() if feature_engine and hasattr(feature_engine, 'get_latest_mid') else None
                if live_mid:
                    profit_distance = (live_mid - current_price) if direction == "BUY" else (current_price - live_mid)
                    if profit_distance >= (atr * 2.5):
                        active_leash = atr * 0.5   
                    elif profit_distance >= (atr * 1.5):
                        active_leash = atr * 1.0   
                    else:
                        active_leash = atr * 2.0   

                    if direction == "BUY":
                        if live_mid > peak_observed_price: peak_observed_price = live_mid
                        if peak_observed_price >= (current_price + activation_threshold):
                            target_stop = peak_observed_price - active_leash
                            if target_stop > (current_hard_stop + minimum_api_step) and target_stop < live_mid:
                                amend_success = await asyncio.to_thread(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, stopLoss=str(round(target_stop, 4)))
                                if amend_success:
                                    current_hard_stop = target_stop
                                    logger.info(f"📈 KINETIC STOP ADVANCED for {symbol} // New Stop: {round(target_stop, 4)} | Active Leash: {round(active_leash, 4)}")
                    elif direction == "SELL":
                        if live_mid < peak_observed_price: peak_observed_price = live_mid
                        if peak_observed_price <= (current_price - activation_threshold):
                            target_stop = peak_observed_price + active_leash
                            if target_stop < (current_hard_stop - minimum_api_step) and target_stop > live_mid:
                                amend_success = await asyncio.to_thread(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, stopLoss=str(round(target_stop, 4)))
                                if amend_success:
                                    current_hard_stop = target_stop
                                    logger.info(f"📉 KINETIC STOP ADVANCED for {symbol} // New Stop: {round(target_stop, 4)} | Active Leash: {round(active_leash, 4)}")

    # ==========================================
    # ORCHESTRATION BOOTSTRAPPER
    # ==========================================
    async def run_engine_forever(self):
        logger.critical("LAUNCHING DISTRIBUTED QUANT SWARM DAEMON DEPLOYMENTS...")
        logger.info("🌍 Booting up Global Satellite Radar to execute asset tracking optimization matrix...")
        
        # 🚀 Fetch the global tick size matrix before launching workers
        await self._fetch_exchange_tick_sizes()
        
        boot_basket = await self.executor.get_top_volatile_assets(limit=100, min_turnover=10_000_000)
        if boot_basket and len(boot_basket) >= 15:
            self.asset_basket = boot_basket[:15]
            self.shadow_basket = boot_basket[15:]
            self.feature_engines = {s: AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600) for s in self.asset_basket}
            self.screener_memory = {s: {"prices": [], "volumes": []} for s in self.asset_basket}
            self.macro_regimes = {s: "HOLD" for s in self.asset_basket}
            self.macro_confidences = {s: 0.0 for s in self.asset_basket}
            self.current_atrs = {s: 0.0 for s in self.asset_basket}
            self.last_execution_timestamps = {s: 0.0 for s in self.asset_basket}
            self.screener_metrics = {s: {"vol_mult": 1.0, "vol_z": 0.0} for s in self.asset_basket}
            logger.info(f"🧬 Boot initialization successful. Matrix structured using {len(self.asset_basket)} core nodes and {len(self.shadow_basket)} shadow nodes.")
        else:
            logger.warning("Initial satellite boot lookup underperformed. Deploying default infrastructure fallback configurations.")
        
        await self.telegram.log_message(
            f"🚀 *DYNAMIC SATELLITE SWARM ENGINE ONLINE*\nMapping Processing Execution Completed.\nCore Hunting Matrix Scope:\n`{', '.join(self.asset_basket)}`", 
            "SUCCESS"
        )
        
        await asyncio.gather(
            self.run_macro_commander(),        
            self.run_macro_regime_loop(),      
            self.run_universe_refresher(),
            self.stream_manager_loop(),
            self.run_system_heartbeat(),
            self.run_shadow_swarm_scanner() 
        )

if __name__ == "__main__":
    from keep_alive import keep_alive
    keep_alive()
    engine = DistributedQuantEngine()
    try:
        asyncio.run(engine.run_engine_forever())
    except KeyboardInterrupt:
        logger.critical("Gracefully tearing down network connections. Session closed.")
        sys.exit(0)