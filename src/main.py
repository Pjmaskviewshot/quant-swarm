import os
import sys
import time
import asyncio
import logging
import uuid
import traceback
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
        # test_mode = False -> FSM strict accuracy rules are enforced.
        # Live Bybit API routes are fully armed. Real capital is at risk.
        # ====================================================================
        self.test_mode = False 
        
        if self.test_mode:
            logger.critical("⚠️ SYSTEM INITIALIZED IN TEST MODE (GHOST TRADING SIMULATION ACTIVE) ⚠️")
        else:
            logger.critical("🟢 SYSTEM INITIALIZED IN LIVE PRODUCTION MODE. CAPITAL DEPLOYMENT ARMED.")
        
        # Operational parameters
        self.asset_basket: List[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        self.timeframe = os.getenv("TRADING_TIMEFRAME", "15")
        
        # Dynamic Stream Management Flags
        self.stream_restart_event = asyncio.Event()
        
        # 1. State Engines & Global Risk Controller
        self.memory = MemoryBank()
        self.fsm = SystemStateMachine(accuracy_threshold=0.65, warmup_epochs=10)
        self.risk_vault = InstitutionalRiskVault(max_drawdown_pct=0.10, max_single_position_risk_pct=0.02)
        
        # 2. Swarm Intelligence Matrices
        self.feature_engines: Dict[str, AdaptiveFeatureEngine] = {s: AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600) for s in self.asset_basket}
        self.macro_regimes: Dict[str, str] = {s: "HOLD" for s in self.asset_basket}
        self.macro_confidences: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.current_atrs: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.last_execution_timestamps: Dict[str, float] = {s: 0.0 for s in self.asset_basket}
        self.screener_memory: Dict[str, Dict[str, List[float]]] = {s: {"prices": [], "volumes": []} for s in self.asset_basket}
        
        # 🛡️ THE WATCHDOG REGISTRY
        self.active_workers: Dict[str, asyncio.Task] = {}
        
        self.execution_cooldown_period = 10.0 if self.test_mode else 60.0  
        self.historical_win_rate = 0.58
        self.historical_win_loss_ratio = 1.65

        # 3. External Service Interfaces
        nv_keys = [os.getenv("NVIDIA_API_KEY_1"), os.getenv("NVIDIA_API_KEY_2")]
        self.ai_router = ResilientAIRouter(nv_keys=nv_keys, deepseek_key=os.getenv("DEEPSEEK_API_KEY"))
        self.macro_data_feed = AsynchronousDataFeed(finnhub_key=os.getenv("FINNHUB_API_KEY"))
        self.telegram = AsyncTelegramReporter(token=os.getenv("TELEGRAM_BOT_TOKEN"), chat_id=os.getenv("TELEGRAM_CHAT_ID"))
        
        # 4. Execution & SOR Integration
        self.executor = BybitUnifiedExecutor(api_key=os.getenv("BYBIT_API_KEY"), api_secret=os.getenv("BYBIT_API_SECRET"), testnet=False)
        self.sor = SmartOrderRouter(executor=self.executor, max_slippage_pct=0.0012)

    # ==========================================
    # THREAD 1: THE IMMORTAL WATCHDOG
    # ==========================================
    async def run_macro_regime_loop(self):
        """Spawns and rigorously monitors independent asset workers. Resurrects dead threads instantly."""
        logger.critical("🐺 IMMORTAL WATCHDOG ONLINE. Deploying Swarm Matrix...")
        
        # 1. Initial Deployment
        for symbol in self.asset_basket:
            task = asyncio.create_task(self._asset_worker_lifecycle(symbol))
            self.active_workers[symbol] = task
            await asyncio.sleep(1.5) # Stagger boot sequence to avoid immediate rate limits
            
        logger.info(f"Successfully deployed {len(self.active_workers)} independent asset workers.")
        
        # 2. The Infinite Monitoring Loop
        while True:
            # Check the health of every worker every 60 seconds
            await asyncio.sleep(60)
            
            for symbol in list(self.asset_basket):
                task = self.active_workers.get(symbol)
                
                # If the task doesn't exist, is done, or threw an exception, it is DEAD.
                if task is None or task.done():
                    # Diagnostic Telemetry: Print exactly WHY it died
                    if task and task.done() and task.exception():
                        exc = task.exception()
                        logger.error(f"☠️ WATCHDOG FATAL ALERT: {symbol} worker died from unhandled exception:")
                        # Extracts the full Python traceback (line number and error type)
                        traceback_str = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
                        logger.error(f"\n{traceback_str}")
                    else:
                        logger.error(f"☠️ WATCHDOG ALERT: {symbol} worker thread vanished or stalled silently.")
                        
                    # Brutally cancel the dead task just in case it is zombied in memory
                    if task and not task.done():
                        task.cancel()
                        
                    # Resurrect the worker
                    logger.critical(f"⚕️ WATCHDOG RESURRECTING {symbol} NODE...")
                    new_task = asyncio.create_task(self._asset_worker_lifecycle(symbol))
                    self.active_workers[symbol] = new_task

    async def _asset_worker_lifecycle(self, symbol: str):
        """Isolated background worker managing the data sync and AI routing for a single asset."""
        while True:
            try:
                # 1. Enforce strict data fetch boundaries using the Guillotine
                context = await asyncio.wait_for(
                    self.macro_data_feed.fetch_market_snapshot(symbol, self.timeframe),
                    timeout=8.0
                )
                
                if not context:
                    await asyncio.sleep(30)
                    continue
                    
                # 2. Update memory matrices locally
                self.current_atrs[symbol] = context["current_price"] * 0.0045
                rolling_acc, total_resolved = self.memory.compute_rolling_accuracy(window_size=50)
                self.fsm.process_state_transition(rolling_acc, total_resolved)
                
                payload = {
                    "asset": symbol,
                    "price": context["current_price"],
                    "atr_volatility": self.current_atrs[symbol],
                    "macro_news_stream": context["news_context"],
                    "rolling_system_accuracy": f"{rolling_acc:.2%}"
                }
                
                # 3. Route to NVIDIA API with hard isolated timeout limits
                verdict = await asyncio.wait_for(
                    self.ai_router.extract_market_verdict(payload),
                    timeout=15.0
                )
                
                # 4. State updates committed safely to shared thread memory
                self.macro_regimes[symbol] = verdict.get("direction", "HOLD")
                self.macro_confidences[symbol] = verdict.get("confidence", 0.0)
                
                logger.info(f"🔄 SWARM NODE SYNCED // Target: {symbol} | Bias: {self.macro_regimes[symbol]} | Conf: {self.macro_confidences[symbol]:.2%}")
                
            except asyncio.TimeoutError:
                logger.error(f"⏳ WORKER TIMEOUT: API hung on {symbol}. Loop will retry.")
            except Exception as e:
                # This catches minor network blips. Fatal code errors will break the loop and be caught by the Watchdog.
                logger.error(f"⚠️ WORKER ERROR: Exception on {symbol} loop: {e}")
                
            # Each worker rests independently for 60 seconds before pulling fresh AI data.
            await asyncio.sleep(60)

    # ==========================================
    # THREAD 2: FAST MICROSTRUCTURE PIPELINE
    # ==========================================
    async def handle_incoming_orderbook_tick(self, depth_data: Dict[str, Any]):
        """Microsecond-Scale Order Book Evaluator processing all active nodes simultaneously."""
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
        """Monitors the lightweight public ticker pipeline to log internal alpha state."""
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
        """Updates multi-timeframe analytics arrays dynamically for the relevant Swarm Node."""
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
        """Autonomous 4-Hour Loop executing quantitative global analysis to map out fresh alpha vectors."""
        while True:
            await asyncio.sleep(14400) # Sleep interval mapping strictly to 4 hours
            
            logger.info("🌍 GLOBAL SATELLITE SCAN INITIATED. Querying Bybit endpoints for volatility targets...")
            new_basket = await self.executor.get_top_volatile_assets(limit=15, min_turnover=50_000_000)
            
            if len(new_basket) < 5:
                logger.warning("Dynamic satellite scan returned insufficient asset velocity metrics. Maintaining current tracking universe.")
                continue
                
            self.asset_basket = new_basket
            
            # Rebuild Swarm Matrices atomically to maintain safety without memory leaks
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
            
            # Instead of manually triggering the macro loop, we let the Immortal Watchdog
            # cleanly kill workers that are no longer in the basket and spawn new ones naturally.
            for old_symbol in list(self.active_workers.keys()):
                if old_symbol not in self.asset_basket:
                    logger.info(f"♻️ Retiring old node: {old_symbol}")
                    self.active_workers[old_symbol].cancel()
                    del self.active_workers[old_symbol]
                    
            for new_symbol in self.asset_basket:
                if new_symbol not in self.active_workers:
                    logger.info(f"🌱 Spawning new node: {new_symbol}")
                    task = asyncio.create_task(self._asset_worker_lifecycle(new_symbol))
                    self.active_workers[new_symbol] = task
            
            self.stream_restart_event.set()

    # ==========================================
    # THREAD 6: HIGH-VELOCITY NETWORK CONNECTOR
    # ==========================================
    async def stream_manager_loop(self):
        """Maintains low-latency network state boundaries and hot-swaps WebSocket configurations safely."""
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
    # EXECUTION ROUTER (PORTFOLIO + SOR)
    # ==========================================
    async def _route_validated_execution_block(self, symbol: str, direction: str, current_price: float):
        """The Central Nervous System for the Swarm. Validates global risk before executing any node."""
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
            
            success = await self.sor.execute_iceberg_block(
                symbol=symbol,
                direction=direction,
                total_qty=risk_matrix["size"],
                current_mid_price=current_price
            )
            
            if success:
                self.risk_vault.update_position_ledger(symbol, risk_matrix['allocated_value_usdt'])
                
                alert_text = (
                    f"🧬 *DISTRIBUTED SWARM ORDER ROUTED*\n"
                    f"• Execution Node: {symbol}\n"
                    f"• Action Basis: {direction}\n"
                    f"• AI Macro Confidence: {confidence:.2%}\n"
                    f"• Leverage Applied: {target_leverage}x\n"
                    f"• Total Notional Value: ${risk_matrix['allocated_value_usdt']} USDT"
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
            self.run_macro_regime_loop(),
            self.run_universe_refresher(),
            self.stream_manager_loop()
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