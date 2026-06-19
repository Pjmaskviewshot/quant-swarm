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
from core.fsm import SystemStateMachine
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
        self.risk_vault = InstitutionalRiskVault(max_drawdown_pct=0.10, max_single_position_risk_pct=0.02)
        
        self.feature_engines: Dict[str, AdaptiveFeatureEngine] = {s: AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600) for s in self.asset_basket}
        self.macro_regimes: Dict[str, str] = {s: "HOLD" for s in self.asset_basket}
        self.macro_confidences: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.current_atrs: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.last_execution_timestamps: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.screener_memory: Dict[str, Dict[str, List[float]]] = {s: {"prices": [], "volumes": []} for s in self.asset_basket}
        
        # 📦 BATCHING QUEUE: Holds data from the workers until the Commander is ready
        self.pending_macro_payloads: Dict[str, dict] = {}
        
        self.active_workers: Dict[str, asyncio.Task] = {}
        
        self.execution_cooldown_period = 10.0 if self.test_mode else 60.0  
        self.historical_win_rate = 0.58
        self.historical_win_loss_ratio = 1.65

        nv_keys = [os.getenv("NVIDIA_API_KEY_1"), os.getenv("NVIDIA_API_KEY_2")]
        self.ai_router = ResilientAIRouter(nv_keys=nv_keys, deepseek_key=os.getenv("DEEPSEEK_API_KEY"))
        self.macro_data_feed = AsynchronousDataFeed(finnhub_key=os.getenv("FINNHUB_API_KEY"))
        self.telegram = AsyncTelegramReporter(token=os.getenv("TELEGRAM_BOT_TOKEN"), chat_id=os.getenv("TELEGRAM_CHAT_ID"))
        
        self.executor = BybitUnifiedExecutor(api_key=os.getenv("BYBIT_API_KEY"), api_secret=os.getenv("BYBIT_API_SECRET"), testnet=False)
        self.sor = SmartOrderRouter(executor=self.executor, max_slippage_pct=0.0012)

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

                # 2. Compress the ENTIRE matrix into ONE payload
                compressed_matrix = ""
                for sym, data in batch_payload.items():
                    compressed_matrix += f"[{sym}] P:{data['price']:.4f} ATR:{data['atr_volatility']:.4f} ACC:{data['rolling_system_accuracy']} | "
                    
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
        """Workers NO LONGER TALK TO THE AI. They fetch data, resolve predictions, and update the FSM queue."""
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
                
                # ✅ RESOLVE OLD PREDICTIONS BEFORE FSM CALCULATION
                age_cutoff = time.time() - 300  # 5 minutes old
                resolved_count = self.memory.resolve_historical_predictions(
                    current_price=current_price,
                    age_cutoff=age_cutoff
                )
                
                if resolved_count > 0:
                    logger.info(f"✅ Resolved {resolved_count} historical ghost trades for {symbol}.")

                self.current_atrs[symbol] = current_price * 0.0045
                
                # Compute rolling accuracy with actual resolved data
                rolling_acc, total_resolved = self.memory.compute_rolling_accuracy(window_size=50)
                self.fsm.process_state_transition(rolling_acc, total_resolved)
                
                logger.info(f"📊 FSM STATUS: {self.fsm.current_state.value} | Accuracy: {rolling_acc:.2%} | Resolved Pool: {total_resolved}")
                
                # Drop data into the BATCH QUEUE
                self.pending_macro_payloads[symbol] = {
                    "asset": symbol,
                    "price": current_price,
                    "atr_volatility": self.current_atrs[symbol],
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

        current_time = time.time()
        if (current_time - self.last_execution_timestamps.get(symbol, 0)) < self.execution_cooldown_period:
            return

        trade_direction = None
        
        if self.test_mode:
            if z_obi >= 0.2:
                trade_direction = "BUY"
            elif z_obi <= -0.2:
                trade_direction = "SELL"
        else:
            regime = self.macro_regimes.get(symbol, "HOLD")
            if regime == "BUY" and z_obi >= 2.0:
                trade_direction = "BUY"  
            elif regime == "SELL" and z_obi <= -2.0:
                trade_direction = "SELL" 

        if trade_direction:
            self.last_execution_timestamps[symbol] = current_time
            logger.critical(f"🔥 SWARM EDGE DETECTED // Node: {symbol} | Macro: {self.macro_regimes.get(symbol, 'HOLD')} | Z-OBI: {z_obi:.2f} | Mid: {mid_price}")
            asyncio.create_task(self._route_validated_execution_block(symbol, trade_direction, mid_price))

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
            
            for s in self.asset_basket:
                new_feature_engines[s] = self.feature_engines.get(s, AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600))
                new_screener_memory[s] = self.screener_memory.get(s, {"prices": [], "volumes": []})
                new_macro_regimes[s] = self.macro_regimes.get(s, "HOLD")
                new_macro_confidences[s] = self.macro_confidences.get(s, 0.0)
                new_current_atrs[s] = self.current_atrs.get(s, 0.0)
                new_last_execs[s] = self.last_execution_timestamps.get(s, 0.0)
                
            self.feature_engines = new_feature_engines
            self.screener_memory = new_screener_memory
            self.macro_regimes = new_macro_regimes
            self.macro_confidences = new_macro_confidences
            self.current_atrs = new_current_atrs
            self.last_execution_timestamps = new_last_execs
            
            logger.info(f"🌌 QUANT UNIVERSE MATRIX RE-CALIBRATED. Operational Hunt Targets: {', '.join(self.asset_basket)}")
            
            for old_symbol in list(self.active_workers.keys()):
                if old_symbol not in self.asset_basket:
                    logger.info(f"♻️ Retiring old node: {old_symbol}")
                    self.active_workers[old_symbol].cancel()
                    del self.active_workers[old_symbol]
                    self.pending_macro_payloads.pop(old_symbol, None) # Remove from batch queue
                    
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
        """Prints a periodic health check and sends an hourly financial report to Telegram."""
        start_time = time.time()
        loop_counter = 0
        
        while True:
            await asyncio.sleep(60) # Pulse every 1 minute
            loop_counter += 1
            
            uptime_seconds = time.time() - start_time
            uptime_hours = uptime_seconds / 3600
            
            # Standard local terminal logging
            logger.info(f"💓 SWARM HEARTBEAT: Matrix is active. Uptime: {uptime_hours:.2f} hours. AI Queue: {len(self.pending_macro_payloads)} assets ready.")

            # Hourly Telegram Telemetry Report (Triggers every 60 loops)
            if loop_counter % 60 == 0:
                # Query the latest telemetry from the memory bank
                accuracy, pool_size = self.memory.compute_rolling_accuracy(window_size=50)
                state = self.fsm.current_state.value
                
                # Construct the HTML-formatted push notification
                report = (
                    f"📊 <b>QUANT SWARM HOURLY REPORT</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏱ <b>Uptime:</b> <code>{uptime_hours:.2f} hrs</code>\n"
                    f"🎛 <b>FSM State:</b> <code>{state}</code>\n"
                    f"🎯 <b>Macro Accuracy:</b> <code>{accuracy:.2%}</code>\n"
                    f"🏊‍♂️ <b>Resolved Validation Pool:</b> <code>{pool_size}</code>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📡 <b>Active Nodes:</b> <code>{len(self.asset_basket)} Assets</code>\n"
                    f"🤖 <b>AI Router:</b> <code>DeepSeek V4 (Native)</code>"
                )
                
                # Dispatch asynchronously
                asyncio.create_task(self.telegram.send_html_report(report))

    # ==========================================
    # EXECUTION ROUTER (PORTFOLIO + SOR)
    # ==========================================
    
    def calculate_initial_bracket(self, entry_price: float, atr: float, side: str):
        """
        Calculates initial hard stop and phase 2 trigger boundaries.
        """
        if side.upper() == "BUY":
            initial_sl = entry_price - (1.5 * atr)
            activation_price = entry_price + (2.5 * atr)
        else:  # SELL
            initial_sl = entry_price + (1.5 * atr)
            activation_price = entry_price - (2.5 * atr)
            
        return round(initial_sl, 4), round(activation_price, 4)

    async def _route_validated_execution_block(self, symbol: str, direction: str, current_price: float):
        try:
            signal_id = str(uuid.uuid4())
            confidence = self.macro_confidences.get(symbol, 0.0)
            atr = self.current_atrs.get(symbol, current_price * 0.0045)
            
            self.memory.commit_prediction(signal_id, time.time(), current_price, direction, confidence)
            
            if not self.test_mode and not self.fsm.can_execute_trades:
                logger.info(f"👻 [CALIBRATION] Ghost trade saved to database -> Node: {symbol} | ID: {signal_id[:8]} | Dir: {direction}")
                return
            
            if self.test_mode:
                logger.critical(f"🧪 [SIMULATION SUCCESS] Ghost trade committed to database row -> Node: {symbol} | ID: {signal_id[:8]}... | Dir: {direction}")
                return 

            balance = await self.executor.get_wallet_balance_usdt()
            
            risk_matrix = self.risk_vault.compute_variance_adjusted_kelly(
                account_balance=balance,
                win_rate=self.historical_win_rate,
                win_loss_ratio=self.historical_win_loss_ratio,
                asset_volatility_atr=atr,
                current_price=current_price,
                ai_confidence=confidence 
            )
            
            if not risk_matrix["approved"] or risk_matrix["size"] <= 0.0:
                logger.warning(f"Execution canceled for {symbol}. Variance-adjusted Kelly criteria not met.")
                return

            if not self.risk_vault.evaluate_portfolio_safety(balance, risk_matrix['allocated_value_usdt'], symbol):
                logger.warning(f"Swarm global execution blocked. Portfolio cannot support additional exposure for {symbol}.")
                return

            logger.critical(f"🎯 RISK CLEARANCE GRANTED // Node: {symbol} | Notional Size: {risk_matrix['allocated_value_usdt']} USDT.")
            
            target_leverage = risk_matrix.get("recommended_leverage", 1)
            leverage_success = await self.executor.adjust_leverage(symbol, target_leverage)
            
            if not leverage_success:
                logger.error(f"Execution aborted. Failed to safely set required leverage ({target_leverage}x) on {symbol}.")
                return
            
            # --- DUAL-PHASED DYNAMIC BRACKET LOGIC INTEGRATION ---
            initial_sl, activation_price = self.calculate_initial_bracket(current_price, atr, direction)
            
            success = await self.sor.execute_iceberg_block(
                symbol=symbol,
                direction=direction,
                total_qty=risk_matrix["size"],
                current_mid_price=current_price,
                # Pass parameters securely down to your Bybit SOR to manage execution
                stop_loss=initial_sl,
                activation_price=activation_price 
            )
            
            if success:
                self.risk_vault.update_position_ledger(symbol, risk_matrix['allocated_value_usdt'])
                
                alert_text = (
                    f"🧬 *DISTRIBUTED SWARM ORDER ROUTED*\n"
                    f"• Execution Node: {symbol}\n"
                    f"• Action Basis: {direction}\n"
                    f"• AI Macro Confidence: {confidence:.2%}\n"
                    f"• Leverage Applied: {target_leverage}x\n"
                    f"• Total Notional Value: ${risk_matrix['allocated_value_usdt']} USDT\n"
                    f"🛡️ *Phase 1 Active*: Hard SL at {initial_sl} | Harpoon Trigger at {activation_price}"
                )
                asyncio.create_task(self.telegram.log_message(alert_text, "SUCCESS"))

        except Exception as e:
            logger.error(f"Distributed swarm execution routing failed for {symbol}: {e}")

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
            
            logger.info(f"🧬 Boot initialization successful. Matrix structured using {len(self.asset_basket)} concurrent nodes.")
        else:
            logger.warning("Initial satellite boot lookup underperformed. Deploying default infrastructure fallback configurations.")
        
        await self.telegram.log_message(
            f"🚀 *DYNAMIC SATELLITE SWARM ENGINE ONLINE*\nMapping Processing Execution Completed.\nConcurrent Hunting Matrix Scope:\n`{', '.join(self.asset_basket)}`", 
            "SUCCESS"
        )
        
        await asyncio.gather(
            self.run_macro_commander(),        # <-- The new AI Request Batcher
            self.run_macro_regime_loop(),      # <-- The Watchdog and Gatherers
            self.run_universe_refresher(),
            self.stream_manager_loop(),
            self.run_system_heartbeat()        # <-- Uptime Tracking
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