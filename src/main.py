import os
import sys
import time
import asyncio
import logging
import uuid
import traceback
import random
import numpy as np
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
        
        self.stream_restart_event = asyncio.Event()
        
        self.memory = MemoryBank()
        self.fsm = SystemStateMachine(accuracy_threshold=0.65, warmup_epochs=10)
        
        # 🛡️ UPGRADE: Adjusted limits to 25% max drawdown and 15% risk per position
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
        
        # Fallback global cooldown (Overridden dynamically per asset in execution loop)
        self.execution_cooldown_period = 10.0 if self.test_mode else 900.0  
        
        self.historical_win_rate = 0.58
        self.historical_win_loss_ratio = 1.65

        nv_keys = [os.getenv("NVIDIA_API_KEY_1"), os.getenv("NVIDIA_API_KEY_2")]
        self.ai_router = ResilientAIRouter(nv_keys=nv_keys, deepseek_key=os.getenv("DEEPSEEK_API_KEY"))
        self.macro_data_feed = AsynchronousDataFeed(finnhub_key=os.getenv("FINNHUB_API_KEY"))
        self.telegram = AsyncTelegramReporter(token=os.getenv("TELEGRAM_BOT_TOKEN"), chat_id=os.getenv("TELEGRAM_CHAT_ID"))
        
        self.executor = BybitUnifiedExecutor(api_key=os.getenv("BYBIT_API_KEY"), api_secret=os.getenv("BYBIT_API_SECRET"), testnet=False)
        
        # 🛡️ UPGRADE: Relaxed spread guard to 0.5% for volatile altcoins
        self.sor = SmartOrderRouter(executor=self.executor, max_slippage_pct=0.005)

    # ====================================================================
    # 🧠 UPGRADE 1: THE ADAPTIVE MEMORY HORIZON (Exponential Scaling)
    # ====================================================================
    def compute_dynamic_memory_window(self, vol_mult: float) -> int:
        """
        Calculates how many trades the FSM needs to prove edge.
        Uses quadratic compression to aggressively shrink the window to 10
        during parabolic volume events, instantly unlocking live capital.
        """
        # Sigmoid-like response to volume
        normalized = min(2.0, vol_mult) / 2.0
        compressed = 1.0 - (normalized * normalized * 0.8)  # Quadratic compression
        target_window = int(50 * max(0.2, compressed))
        return max(10, min(50, target_window))

    # ====================================================================
    # 🧠 THE TRUE INSTITUTIONAL REGIME ATTENUATOR
    # ====================================================================
    def calculate_adaptive_regime_parameters(self, market_regime: str, metrics: dict) -> dict:
        """
        Organically scales execution barriers and cooldowns based strictly on volume.
        Maintains absolute mathematical rigor to ensure training data is valid.
        """
        vol_mult = float(metrics.get("vol_mult", 1.0))

        # The Institutional Baseline (Used when market volume is exactly average)
        optimized = {
            "cooldown_period": 300.0,      # 5 minute baseline
            "z_score_threshold": 2.0,      # 95th percentile baseline
            "position_scaling": 1.0,
            "sl_multiplier": 1.5,
            "tp_multiplier": 2.0,
            "execution_verdict": True
        }

        # 🌊 FLUID MARKET BREATHING (The Math Upgrade)
        if market_regime == "RANGING":
            compression_penalty = max(0.0, 1.5 - vol_mult)
            
            # The Z-score and Cooldown scale continuously with the market's pulse.
            optimized["z_score_threshold"] = 2.0 + (compression_penalty * 1.5)
            optimized["cooldown_period"] = 300.0 + (compression_penalty * 600.0)
            
            # Chop Optimization: Wider stops to survive wicks, tighter profits to capture mean reversion
            optimized["sl_multiplier"] = 2.0 
            optimized["tp_multiplier"] = 1.5 
            
            # Total Liquidity Blackout
            if vol_mult < 0.6:
                optimized["execution_verdict"] = False
            
        elif market_regime == "TRENDING":
            # In a trend, strike fast and let profits ride
            optimized["z_score_threshold"] = 2.0
            optimized["cooldown_period"] = 120.0  
            optimized["tp_multiplier"] = 3.0     
            
        return optimized

    # ==========================================
    # THREAD 1A: THE MACRO COMMANDER (BATCHER)
    # ==========================================
    async def run_macro_commander(self):
        """Batches payload data into a SINGLE query to native DeepSeek API."""
        logger.info("🧠 MACRO COMMANDER ONLINE. Waiting for workers to gather data...")
        
        while True:
            await asyncio.sleep(60) 
            
            if not self.pending_macro_payloads:
                continue
                
            batch_payload = dict(self.pending_macro_payloads)
            
            try:
                # 1. Fetch a single global narrative context
                global_news = "No significant macro shifts detected."
                if len(batch_payload) > 0:
                    first_sym = list(batch_payload.keys())[0]
                    if "macro_news_stream" in batch_payload[first_sym]:
                        global_news = batch_payload[first_sym]["macro_news_stream"]

                # 2. Compress the ENTIRE matrix into ONE payload with context indicators
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
        """Monitors the data gatherers and resurrects them if they die."""
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
                        traceback_str = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
                        logger.error(f"\n{traceback_str}")
                    else:
                        logger.error(f"☠️ WATCHDOG ALERT: {symbol} worker thread vanished or stalled silently.")
                        
                    if task and not task.done():
                        task.cancel()
                        
                    logger.critical(f"⚕️ WATCHDOG RESURRECTING {symbol} NODE...")
                    new_task = asyncio.create_task(self._asset_data_gatherer_lifecycle(symbol))
                    self.active_workers[symbol] = new_task

    async def _asset_data_gatherer_lifecycle(self, symbol: str):
        """Workers fetch data, resolve predictions, and track indicator metrics."""
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
                
                # RESOLVE OLD PREDICTIONS BEFORE FSM CALCULATION
                age_cutoff = time.time() - 300  # 5 minutes old
                resolved_count = self.memory.resolve_historical_predictions(
                    current_price=current_price,
                    age_cutoff=age_cutoff
                )
                
                if resolved_count > 0:
                    logger.info(f"✅ Resolved {resolved_count} historical ghost trades for {symbol}.")

                feature_engine = self.feature_engines.get(symbol)
                market_regime = feature_engine.detect_market_regime() if feature_engine else "RANGING"

                # Feature Engine calculation
                self.current_atrs[symbol] = current_price * 0.0045
                
                # Safely pull live context features from screener memory
                metrics = self.screener_metrics.get(symbol, {"vol_mult": 1.0, "vol_z": 0.0})
                
                # 🚀 UPGRADE: Compute rolling accuracy using the Dynamic Horizon 
                dynamic_window = self.compute_dynamic_memory_window(metrics.get("vol_mult", 1.0))
                rolling_acc, total_resolved = self.memory.compute_rolling_accuracy(window_size=dynamic_window)
                self.fsm.process_state_transition(rolling_acc, total_resolved, market_regime)
                
                logger.info(f"📊 FSM STATUS: {self.fsm.current_state.value} | Accuracy: {rolling_acc:.2%} | Window Target: {dynamic_window}")

                # Drop data into the BATCH QUEUE
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
                
            cooldown = random.uniform(45.0, 75.0)
            await asyncio.sleep(cooldown)

    # ==========================================
    # THREAD 2: FAST MICROSTRUCTURE PIPELINE
    # ==========================================
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

        # 🚀 INTEGRATION: Fetch metrics early to feed the adaptive attenuator
        metrics = self.screener_metrics.get(symbol, {"vol_mult": 1.0, "vol_z": 0.0})
        
        # 🚀 INTEGRATION: Calculate dynamic parameters based on current regime
        optimization = self.calculate_adaptive_regime_parameters(market_regime, metrics)

        # 🛡️ Structural Circuit Breaker
        if not optimization["execution_verdict"]:
            return

        # 🚀 INTEGRATION: Dynamic Cooldown Evaluation
        current_time = time.time()
        if (current_time - self.last_execution_timestamps.get(symbol, 0)) < optimization["cooldown_period"]:
            return

        trade_direction = None
        is_active = self.fsm.current_state in [TradingState.ACTIVE_TRADING, TradingState.ACTIVE_MEAN_REVERSION]
        
        # 1. Institutional Volume Verification
        has_institutional_volume = metrics.get("vol_mult", 1.0) >= 1.5
        
        # 2. Dynamic Extreme Z-Score Threshold
        extreme_z_threshold = optimization["z_score_threshold"]

        if self.test_mode or not is_active:
            # Ghost trading thresholds (Calibration mode)
            if z_obi >= extreme_z_threshold and has_institutional_volume:  
                trade_direction = "BUY"
            elif z_obi <= -extreme_z_threshold and has_institutional_volume:
                trade_direction = "SELL"
        else:
            regime = self.macro_regimes.get(symbol, "HOLD")
            if market_regime == "TRENDING":
                if regime == "BUY" and z_obi >= extreme_z_threshold and has_institutional_volume:
                    trade_direction = "BUY"  
                elif regime == "SELL" and z_obi <= -extreme_z_threshold and has_institutional_volume:
                    trade_direction = "SELL" 
            else:
                if regime == "BUY" and z_obi >= extreme_z_threshold and has_institutional_volume:
                    trade_direction = "BUY"
                elif regime == "SELL" and z_obi <= -extreme_z_threshold and has_institutional_volume:
                    trade_direction = "SELL"

        if trade_direction:
            self.last_execution_timestamps[symbol] = current_time
            logger.critical(f"🔥 SWARM EDGE DETECTED // Node: {symbol} | Regime: {market_regime} | Z-OBI: {z_obi:.2f} | Dynamic Z-Target: {extreme_z_threshold:.2f}")
            
            # 🚀 INTEGRATION: Pass the optimization dictionary down into the lifecycle
            asyncio.create_task(self.run_signal_lifecycle(symbol, trade_direction, mid_price, optimization))

    # ==========================================
    # THREAD 3: LIGHTWEIGHT BASKET TRACKER
    # ==========================================
    async def handle_incoming_basket_screener_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if not symbol or symbol not in self.asset_basket:
            return

        price_str = data.get("lastPrice")
        volume_str = data.get("turnover24h")
        
        if price_str is None or volume_str is None:
            return

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

        if len(history["prices"]) < 15:
            return

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

        if (volume_multiplier >= 2.5 or volatility_z >= 2.5):
            logger.debug(f"📊 SWARM ALPHA ALERT // {symbol} exhibiting massive divergence. VolMult: {volume_multiplier:.2f}x | Z-Vol: {volatility_z:.2f}")

    # ==========================================
    # THREAD 4: KLINE AGGREGATION PIPELINE
    # ==========================================
    async def handle_incoming_kline_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if symbol not in self.asset_basket:
            return

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
            new_basket = await self.executor.get_top_volatile_assets(limit=15, min_turnover=50_000_000)
            
            if len(new_basket) < 5:
                logger.warning("Dynamic satellite scan returned insufficient asset velocity metrics. Maintaining current tracking universe.")
                continue
                
            self.asset_basket = new_basket
            
            new_feature_engines = {}
            new_screener_memory = {}
            new_macro_regimes = {}
            new_macro_confidences = {}
            new_current_atrs = {}
            new_last_execs = {}
            new_screener_metrics = {}
            
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
            
            for old_symbol in list(self.active_workers.keys()):
                if old_symbol not in self.asset_basket:
                    logger.info(f"♻️ Retiring old node: {old_symbol}")
                    self.active_workers[old_symbol].cancel()
                    del self.active_workers[old_symbol]
                    self.pending_macro_payloads.pop(old_symbol, None)
                    
            for new_symbol in self.asset_basket:
                if new_symbol not in self.active_workers:
                    logger.info(f"🌱 Spawning new node: {new_symbol}")
                    task = asyncio.create_task(self._asset_data_gatherer_lifecycle(new_symbol))
                    self.active_workers[new_symbol] = task
            
            self.stream_restart_event.set()

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
    # THREAD 7: SYSTEM HEARTBEAT & DIAGNOSTICS
    # ==========================================
    async def run_system_heartbeat(self):
        start_time = time.time()
        loop_counter = 0
        
        while True:
            await asyncio.sleep(60) 
            loop_counter += 1
            
            uptime_seconds = time.time() - start_time
            uptime_hours = uptime_seconds / 3600
            
            logger.info(f"💓 SWARM HEARTBEAT: Matrix is active. Uptime: {uptime_hours:.2f} hours. AI Queue: {len(self.pending_macro_payloads)} assets ready.")

            if loop_counter % 60 == 0:
                accuracy, pool_size = self.memory.compute_rolling_accuracy(window_size=30)
                state = self.fsm.current_state.value
                current_vault_balance = await self.executor.get_wallet_balance_usdt()
                
                initial_baseline = 7.80
                drawdown_pct = max(0.0, (initial_baseline - current_vault_balance) / initial_baseline)
                
                bar_length = 10
                filled_blocks = int(drawdown_pct * bar_length)
                filled_blocks = min(bar_length, filled_blocks)
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

                # 🚀 HEARTBEAT TRANSPARENCY: Display the Active Memory Horizon
                avg_vol_mult = np.mean([m.get("vol_mult", 1.0) for m in self.screener_metrics.values()]) if self.screener_metrics else 1.0
                avg_dynamic_window = self.compute_dynamic_memory_window(avg_vol_mult)

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
                    f"📡 <b>Operational Swarm Nodes:</b> <code>{len(self.asset_basket)} Active Threads</code>\n"
                    f"🧠 <b>AI Inference Framework:</b> <code>DeepSeek V4 (Native Cloud)</code>"
                )
                asyncio.create_task(self.telegram.send_html_report(report))

    # ==========================================
    # CORE ORCHESTRATOR: END-TO-END TRADE LIFECYCLE
    # ==========================================
    
    def calculate_initial_bracket(self, entry_price: float, atr: float, side: str, leverage: int, vol_z: float = 0.0, optimization: dict = None):
        """
        ⚡ PHASE 3 UPGRADE: DYNAMIC TARGET STRETCHING & FEE ACCUMULATION
        """
        if optimization is None:
            optimization = {"sl_multiplier": 1.5, "tp_multiplier": 2.0}
            
        fee_drag_factor = 0.00055 * 2 * leverage
        fee_buffer = entry_price * fee_drag_factor
        
        # 🚀 INTEGRATION: Apply Optimized Multipliers
        tp_multiplier = optimization["tp_multiplier"]
        sl_multiplier = optimization["sl_multiplier"]
        
        # Exponential stretch for high volume spikes
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
            
        return round(initial_sl, 4), round(target_tp, 4)

    async def run_signal_lifecycle(self, symbol: str, direction: str, current_price: float, optimization: dict = None):
        """
        Manages the complete lifecycle of a trading signal with concurrent overlap locks active.
        """
        if optimization is None:
            optimization = {"position_scaling": 1.0, "sl_multiplier": 1.5, "tp_multiplier": 2.0}
            
        if symbol in self.active_positions_lock:
            logger.info(f"🛑 OVERLAP PREVENTED // Already actively trading {symbol}. Signal safely bypassed.")
            return False
            
        logger.info(f"🚦 PROCESSING SIGNAL // Symbol: {symbol} | Direction: {direction}")
        
        try:
            self.active_positions_lock.add(symbol)
            signal_id = str(uuid.uuid4())
            confidence = self.macro_confidences.get(symbol, 0.0)
            
            feature_engine = self.feature_engines.get(symbol)
            market_regime = feature_engine.detect_market_regime() if feature_engine else "RANGING"

            if feature_engine and hasattr(feature_engine, 'get_computed_atr') and feature_engine.get_computed_atr():
                atr = feature_engine.get_computed_atr()
                logger.info(f"🎯 Scaled Volatility Engine Engaged // Real ATR for {symbol}: {atr:.4f}")
            else:
                atr = current_price * 0.015
            
            self.current_atrs[symbol] = atr

            metrics = self.screener_metrics.get(symbol, {})
            features_dict = {
                "symbol": symbol,
                "market_regime": market_regime,
                "adaptive_obi_z": 0.0, 
                "liquidity_density_ratio": metrics.get("vol_mult", 1.0),
                "bid_ask_spread": 0.0
            }
            self.memory.commit_prediction(signal_id, time.time(), current_price, direction, confidence, features_dict)
            
            # ====================================================================
            # 🛡️ THE IRON SHIELD + 🚀 DYNAMIC MEMORY HORIZON
            # ====================================================================
            dynamic_window = self.compute_dynamic_memory_window(metrics.get("vol_mult", 1.0))
            rolling_acc, total_resolved = self.memory.compute_rolling_accuracy(window_size=dynamic_window)

            # We ONLY deploy live capital if we have mathematically proven our edge.
            is_whitelisted_state = self.fsm.current_state in [TradingState.ACTIVE_TRADING, TradingState.ACTIVE_MEAN_REVERSION]
            has_institutional_edge = rolling_acc >= 0.65

            if not self.test_mode and (not is_whitelisted_state or not has_institutional_edge):
                logger.critical(
                    f"👻 [SHIELD ACTIVE] Routing to Ghost Simulation -> Node: {symbol} | "
                    f"Accuracy: {rolling_acc:.2%} (Floor: 65%) | Target Dynamic Memory: {dynamic_window} trades"
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
            initial_sl, target_tp = self.calculate_initial_bracket(current_price, atr, direction, target_leverage, vol_z, optimization)
            
            vol_mult = metrics.get("vol_mult", 1.0)
            if vol_mult < 0.6: 
                logger.warning(f"❌ TRADE SKIPPED // {symbol} volume dangerously low (vol_mult: {vol_mult:.2f}). Slippage risk too high.")
                self.active_positions_lock.discard(symbol)
                return False

            expected_profit = target_tp - current_price if direction == "BUY" else current_price - target_tp
            spread_cost = current_price * 0.006  
            
            if expected_profit < (spread_cost * 2):
                logger.warning(f"❌ TRADE SKIPPED // {symbol} profit margin too tight to clear exchange friction.")
                self.active_positions_lock.discard(symbol)
                return False

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
                    await asyncio.to_thread(
                        self.executor.client.cancel_all_orders,
                        category="linear", symbol=symbol
                    )
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
                    # ============================================================
                    # 🚀 UPGRADE 2: KINETIC TAKE-PROFIT SHIFTING (Greed Engine)
                    # ============================================================
                    profit_distance = (live_mid - current_price) if direction == "BUY" else (current_price - live_mid)
                    
                    if profit_distance >= (atr * 2.5):
                        # Tier 3: Extreme Profit Extraction
                        active_leash = atr * 0.5
                    elif profit_distance >= (atr * 1.5):
                        # Tier 2: Capital Preservation (Break-even Lock)
                        active_leash = atr * 1.0
                    else:
                        # Tier 1: Wide Breathing Room
                        active_leash = atr * 2.0

                    if direction == "BUY":
                        if live_mid > peak_observed_price:
                            peak_observed_price = live_mid
                        
                        if peak_observed_price >= (current_price + activation_threshold):
                            target_stop = peak_observed_price - active_leash
                            if target_stop > (current_hard_stop + minimum_api_step) and target_stop < live_mid:
                                amend_success = await asyncio.to_thread(
                                    self.executor.client.set_trading_stop,
                                    category="linear", symbol=symbol, positionIdx=0, stopLoss=str(round(target_stop, 4))
                                )
                                if amend_success:
                                    current_hard_stop = target_stop
                                    logger.info(f"📈 KINETIC STOP ADVANCED for {symbol} // New Stop: {round(target_stop, 4)} | Active Leash: {round(active_leash, 4)}")
                                    
                    elif direction == "SELL":
                        if live_mid < peak_observed_price:
                            peak_observed_price = live_mid
                        
                        if peak_observed_price <= (current_price - activation_threshold):
                            target_stop = peak_observed_price + active_leash
                            if target_stop < (current_hard_stop - minimum_api_step) and target_stop > live_mid:
                                amend_success = await asyncio.to_thread(
                                    self.executor.client.set_trading_stop,
                                    category="linear", symbol=symbol, positionIdx=0, stopLoss=str(round(target_stop, 4))
                                )
                                if amend_success:
                                    current_hard_stop = target_stop
                                    logger.info(f"📉 KINETIC STOP ADVANCED for {symbol} // New Stop: {round(target_stop, 4)} | Active Leash: {round(active_leash, 4)}")

    # ==========================================
    # ORCHESTRATION BOOTSTRAPPER
    # ==========================================
    async def run_engine_forever(self):
        logger.critical("LAUNCHING DISTRIBUTED QUANT SWARM DAEMON DEPLOYMENTS...")
        
        logger.info("🌍 Booting up Global Satellite Radar to execute asset tracking optimization matrix...")
        boot_basket = await self.executor.get_top_volatile_assets(limit=15, min_turnover=50_000_000)
        
        if boot_basket and len(boot_basket) >= 5:
            self.asset_basket = boot_basket
            self.feature_engines = {s: AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600) for s in self.asset_basket}
            self.screener_memory = {s: {"prices": [], "volumes": []} for s in self.asset_basket}
            self.macro_regimes = {s: "HOLD" for s in self.asset_basket}
            self.macro_confidences = {s: 0.0 for s in self.asset_basket}
            self.current_atrs = {s: 0.0 for s in self.asset_basket}
            self.last_execution_timestamps = {s: 0.0 for s in self.asset_basket}
            self.screener_metrics = {s: {"vol_mult": 1.0, "vol_z": 0.0} for s in self.asset_basket}
            
            logger.info(f"🧬 Boot initialization successful. Matrix structured using {len(self.asset_basket)} concurrent nodes.")
        else:
            logger.warning("Initial satellite boot lookup underperformed. Deploying default infrastructure fallback configurations.")
        
        await self.telegram.log_message(
            f"🚀 *DYNAMIC SATELLITE SWARM ENGINE ONLINE*\nMapping Processing Execution Completed.\nConcurrent Hunting Matrix Scope:\n`{', '.join(self.asset_basket)}`", 
            "SUCCESS"
        )
        
        await asyncio.gather(
            self.run_macro_commander(),        
            self.run_macro_regime_loop(),      
            self.run_universe_refresher(),
            self.stream_manager_loop(),
            self.run_system_heartbeat()        
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