import os
import sys
import time
import asyncio
import logging
import uuid
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
        self.test_mode = False 
        # ====================================================================
        
        if self.test_mode:
            logger.critical("⚠️ SYSTEM INITIALIZED IN TEST MODE (GHOST TRADING SIMULATION ACTIVE) ⚠️")
        else:
            logger.critical("🟢 SYSTEM INITIALIZED IN LIVE PRODUCTION MODE. CAPITAL DEPLOYMENT ARMED.")
        
        # Operational parameters - Upgraded to handle a multi-asset basket matrix
        raw_basket = os.getenv("TRADING_BASKET", "DOGEUSDT,XRPUSDT,ADAUSDT,SOLUSDT")
        self.asset_basket = [symbol.strip() for symbol in raw_basket.split(",") if symbol.strip()]
        
        # This pointer targets the current high-alpha asset under execution
        self.active_target = self.asset_basket[0]
        self.timeframe = os.getenv("TRADING_TIMEFRAME", "15")
        self.macro_interval = int(os.getenv("MACRO_UPDATE_INTERVAL_SECONDS", "300"))
        
        # 1. State & Feature Engines
        self.memory = MemoryBank()
        self.fsm = SystemStateMachine(accuracy_threshold=0.65, warmup_epochs=10)
        self.feature_engine = AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600)
        self.risk_vault = InstitutionalRiskVault(max_drawdown_pct=0.10, max_single_position_risk_pct=0.02)
        
        # 2. External Service Interfaces
        nv_keys = [os.getenv("NVIDIA_API_KEY_1"), os.getenv("NVIDIA_API_KEY_2")]
        self.ai_router = ResilientAIRouter(nv_keys=nv_keys, deepseek_key=os.getenv("DEEPSEEK_API_KEY"))
        self.macro_data_feed = AsynchronousDataFeed(finnhub_key=os.getenv("FINNHUB_API_KEY"))
        self.telegram = AsyncTelegramReporter(token=os.getenv("TELEGRAM_BOT_TOKEN"), chat_id=os.getenv("TELEGRAM_CHAT_ID"))
        
        # 3. Execution & SOR Integration
        self.executor = BybitUnifiedExecutor(api_key=os.getenv("BYBIT_API_KEY"), api_secret=os.getenv("BYBIT_API_SECRET"), testnet=False)
        self.sor = SmartOrderRouter(executor=self.executor, max_slippage_pct=0.0012)
        
        # Thread Synchronized Runtime Variables
        self.macro_regime = "HOLD"  
        self.macro_confidence = 0.0
        self.current_atr = 0.0
        self.last_execution_timestamp = 0.0
        self.execution_cooldown_period = 10.0 if self.test_mode else 60.0  

        # Internal memory matrix for Thread 3 Screener
        self.screener_memory = {s: {"prices": [], "volumes": []} for s in self.asset_basket}

        # Baseline stats for Kelly formulation
        self.historical_win_rate = 0.58
        self.historical_win_loss_ratio = 1.65

    # ==========================================
    # THREAD 1: SLOW MACRO AI PIPELINE
    # ==========================================
    async def run_macro_regime_loop(self):
        """High-Density Deep Model Context Processing Task focusing on the Active Target."""
        while True:
            try:
                logger.info(f"Syncing macro perspective matrix for active target {self.active_target} with inference clusters...")
                context = await self.macro_data_feed.fetch_market_snapshot(self.active_target, self.timeframe)
                
                if context:
                    self.current_atr = context["current_price"] * 0.0045 
                    
                    rolling_acc, total_resolved = self.memory.compute_rolling_accuracy(window_size=50)
                    self.fsm.process_state_transition(rolling_acc, total_resolved)
                    
                    payload = {
                        "asset": self.active_target,
                        "price": context["current_price"],
                        "atr_volatility": self.current_atr,
                        "macro_news_stream": context["news_context"],
                        "rolling_system_accuracy": f"{rolling_acc:.2%}"
                    }
                    
                    verdict = await self.ai_router.extract_market_verdict(payload)
                    self.macro_regime = verdict.get("direction", "HOLD")
                    self.macro_confidence = verdict.get("confidence", 0.0)
                    
                    logger.info(f"🔄 MACRO SYSTEM SYNCED // Target: {self.active_target} | Bias: {self.macro_regime} | Conf: {self.macro_confidence:.2%}")
                    
            except Exception as e:
                logger.error(f"Exception encountered inside background intelligence worker thread: {e}")
                
            await asyncio.sleep(self.macro_interval)

    # ==========================================
    # THREAD 2: FAST MICROSTRUCTURE PIPELINE
    # ==========================================
    async def handle_incoming_orderbook_tick(self, depth_data: Dict[str, Any]):
        """Microsecond-Scale Order Book Evaluator with Asset Isolation Filtering."""
        symbol = depth_data.get("s")
        
        if symbol != self.active_target:
            return

        bids = depth_data.get("b", [])
        asks = depth_data.get("a", [])
        
        features = self.feature_engine.push_orderbook_tick(bids, asks)
        if not features.get("valid"):
            return

        z_obi = features["adaptive_obi_z"]
        mid_price = features["mid_price"]
        
        # Enforces FSM guardrails in production
        if not self.fsm.can_execute_trades and not self.test_mode:
            return

        current_time = time.time()
        if (current_time - self.last_execution_timestamp) < self.execution_cooldown_period:
            return

        trade_direction = None
        
        if self.test_mode:
            if z_obi >= 0.2:
                trade_direction = "BUY"
            elif z_obi <= -0.2:
                trade_direction = "SELL"
        else:
            # Strict Institutional Execution Configuration
            if self.macro_regime == "BUY" and z_obi >= 2.0:
                trade_direction = "BUY"  
            elif self.macro_regime == "SELL" and z_obi <= -2.0:
                trade_direction = "SELL" 

        if trade_direction:
            self.last_execution_timestamp = current_time
            logger.critical(f"🔥 EDGE DETECTED // Asset: {self.active_target} | Macro: {self.macro_regime} | Z-OBI: {z_obi:.2f} | Mid: {mid_price}")
            asyncio.create_task(self._route_validated_execution_block(trade_direction, mid_price))

    # ==========================================
    # THREAD 3: LIGHTWEIGHT BASKET TRACKER & HOT-SWAPPER
    # ==========================================
    async def handle_incoming_basket_screener_update(self, data: Dict[str, Any]):
        """Monitors the lightweight public ticker pipeline for our token array matrix."""
        symbol = data.get("symbol")
        if not symbol or symbol not in self.asset_basket:
            return

        price_str = data.get("lastPrice")
        volume_str = data.get("turnover24h")
        
        if price_str is None or volume_str is None:
            return

        current_price = float(price_str)
        current_volume = float(volume_str)

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

        if (volume_multiplier >= 2.5 or volatility_z >= 2.5) and symbol != self.active_target:
            old_target = self.active_target
            self.active_target = symbol
            
            self.feature_engine = AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600)
            
            logger.critical(f"📡 TARGET HOT-SWAP INITIATED // Detached: {old_target} -> Locked: {self.active_target}")
            
            alert_text = (
                f"📡 *SCREENER CORE TARGET HOT-SWAPPED*\n"
                f"• Standing Down: {old_target}\n"
                f"• Active Target Locked: {self.active_target}\n"
                f"• Volume Multiplier: {volume_multiplier:.2f}x\n"
                f"• Volatility Divergence: {volatility_z:.2f} σ"
            )
            asyncio.create_task(self.telegram.log_message(alert_text, "INFO"))

    # ==========================================
    # THREAD 4: KLINE AGGREGATION PIPELINE
    # ==========================================
    async def handle_incoming_kline_update(self, data: Dict[str, Any]):
        """Updates multi-timeframe analytics arrays dynamically for the Active Target."""
        symbol = data.get("symbol")
        if symbol != self.active_target:
            return

        interval = data["interval"]
        candle = data["candle_data"]
        
        self.feature_engine.update_multi_timeframe_candle(
            timeframe=interval,
            open_p=float(candle.get("open", 0)),
            high_p=float(candle.get("high", 0)),
            low_p=float(candle.get("low", 0)),
            close_p=float(candle.get("close", 0)),
            volume=float(candle.get("volume", 0))
        )

    # ==========================================
    # EXECUTION ROUTER (PORTFOLIO + SOR)
    # ==========================================
    async def _route_validated_execution_block(self, direction: str, current_price: float):
        try:
            signal_id = str(uuid.uuid4())
            
            self.memory.commit_prediction(signal_id, time.time(), current_price, direction, self.macro_confidence)
            
            if self.test_mode:
                logger.critical(f"🧪 [SIMULATION SUCCESS] Ghost trade committed to database row -> ID: {signal_id[:8]}... | Dir: {direction} | Price: {current_price}")
                return 

            balance = await self.executor.get_wallet_balance_usdt()
            if not self.risk_vault.evaluate_portfolio_safety(balance):
                logger.warning("Execution blocked by institutional portfolio draw-down circuits.")
                return

            # Note the addition of `ai_confidence=self.macro_confidence` below
            risk_matrix = self.risk_vault.compute_variance_adjusted_kelly(
                account_balance=balance,
                win_rate=self.historical_win_rate,
                win_loss_ratio=self.historical_win_loss_ratio,
                asset_volatility_atr=self.current_atr,
                current_price=current_price,
                ai_confidence=self.macro_confidence 
            )
            
            if not risk_matrix["approved"] or risk_matrix["size"] <= 0.0:
                logger.warning("Execution canceled. Variance-adjusted Kelly criteria not met.")
                return

            logger.critical(f"🎯 RISK CLEARANCE GRANTED // Asset: {self.active_target} | Notional Size: {risk_matrix['allocated_value_usdt']} USDT.")
            
            # --- DYNAMIC LEVERAGE APPLICATION ---
            target_leverage = risk_matrix.get("recommended_leverage", 1)
            leverage_success = await self.executor.adjust_leverage(self.active_target, target_leverage)
            
            if not leverage_success:
                logger.error(f"Execution aborted. Failed to safely set required leverage ({target_leverage}x) on Bybit.")
                return
            # ------------------------------------
            
            success = await self.sor.execute_iceberg_block(
                symbol=self.active_target,
                direction=direction,
                total_qty=risk_matrix["size"],
                current_mid_price=current_price
            )
            
            if success:
                alert_text = (
                    f"🧬 *DISTRIBUTED ORDER ROUTED SUCCESSFULLY*\n"
                    f"• Instrument Target: {self.active_target}\n"
                    f"• Action Basis: {direction}\n"
                    f"• AI Macro Confidence: {self.macro_confidence:.2%}\n"
                    f"• Leverage Applied: {target_leverage}x\n"
                    f"• Total Notional Value: ${risk_matrix['allocated_value_usdt']} USDT"
                )
                asyncio.create_task(self.telegram.log_message(alert_text, "SUCCESS"))

        except Exception as e:
            logger.error(f"Distributed execution routing failed: {e}")

    # ==========================================
    # ORCHESTRATION BOOTSTRAPPER
    # ==========================================
    async def run_engine_forever(self):
        logger.critical("LAUNCHING DISTRIBUTED QUANT SWARM DAEMON DEPLOYMENTS...")
        
        intervals_matrix = ["1", "5", "15"]
        
        stream_feed = HighVelocityMultiFeed(
            basket=self.asset_basket,
            intervals=intervals_matrix,
            orderbook_callback=self.handle_incoming_orderbook_tick,
            screener_callback=self.handle_incoming_basket_screener_update,
            kline_callback=self.handle_incoming_kline_update
        )
        
        await self.telegram.log_message(f"🚀 *DYNAMIC MULTI-ASSET SWARM ONLINE*\nMonitoring Matrix: {', '.join(self.asset_basket)}", "SUCCESS")
        
        await asyncio.gather(
            self.run_macro_regime_loop(),
            stream_feed.initialize_multiplexed_stream()
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