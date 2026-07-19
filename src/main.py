import os
import sys
import time
import math
import asyncio
import logging
import uuid
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

# External Connectors
from services.ai_router import ResilientAIRouter
from services.adversarial_ai import AdversarialDebateMatrix
from services.data_feed import AsynchronousDataFeed
from ingestion.multi_feed import HighVelocityMultiFeed
from services.bybit_v5 import BybitUnifiedExecutor
from services.telegram_ops import AsyncTelegramReporter

# Clean up HTTP logs to prevent terminal spam
logging.getLogger("httpx").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(name)s] - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("QUANT_CORE.DISTRIBUTED_MAIN")


class ContinuousMicrostructureEngine:
    """
    🔬 V19.1 GENESIS: PRODUCTION QUANTITATIVE NODE
    True 4-State OFI, Continuous Fuzzy Regime Transitions, Multi-Lag Hurst Convolution,
    and Asymmetric Expected Value (EV) projections.
    """
    def __init__(self, memory_depth=500):
        self.prev_bid = 0.0
        self.prev_bid_size = 0.0
        self.prev_ask = 0.0
        self.prev_ask_size = 0.0
        self.ofi_ewma = 0.0
        self.ofi_ewmvar = 1.0
        self.ofi_z = 0.0
        
        self.trade_timestamps = deque()
        self.hawkes_decay = 2.0  
        self.hawkes_ewma = 0.0
        self.hawkes_ewmvar = 1.0
        self.hawkes_z = 0.0
        
        self.prices = deque(maxlen=memory_depth)
        self.log_returns = deque(maxlen=memory_depth)
        self.vol_ewma = 0.001
        self.inst_variance = 1e-6
        self.hurst = 0.5
        self.shannon_entropy = 1.0
        
        self.alpha = 0.05
        self.gamma = 0.1  
        self.kappa = 1.5  

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
        
        self.ofi_ewma = (1 - self.alpha) * self.ofi_ewma + self.alpha * delta_W
        self.ofi_ewmvar = (1 - self.alpha) * self.ofi_ewmvar + self.alpha * (delta_W - self.ofi_ewma)**2
        self.ofi_z = (delta_W - self.ofi_ewma) / (math.sqrt(self.ofi_ewmvar) + 1e-9)

    def update_trades(self, price: float, volume: float, is_buy: bool, current_time: float):
        self.prices.append(price)
        if len(self.prices) > 2:
            ret = math.log(self.prices[-1] / self.prices[-2])
            if not math.isnan(ret) and not math.isinf(ret):
                self.log_returns.append(ret)
                self.vol_ewma = (1 - 0.01) * self.vol_ewma + 0.01 * abs(ret)
                
        if len(self.log_returns) > 10:
            self.inst_variance = np.var(list(self.log_returns)[-10:]) + 1e-9

        self.trade_timestamps.append((current_time, volume if is_buy else -volume))
        while self.trade_timestamps and current_time - self.trade_timestamps[0][0] >= 60:
            self.trade_timestamps.popleft()
            
        hawkes_pressure = 0.0
        for t_time, t_vol in self.trade_timestamps:
            hawkes_pressure += t_vol * math.exp(-self.hawkes_decay * (current_time - t_time))
            
        self.hawkes_ewma = (1 - self.alpha) * self.hawkes_ewma + self.alpha * hawkes_pressure
        self.hawkes_ewmvar = (1 - self.alpha) * self.hawkes_ewmvar + self.alpha * (hawkes_pressure - self.hawkes_ewma)**2
        self.hawkes_z = (hawkes_pressure - self.hawkes_ewma) / (math.sqrt(self.hawkes_ewmvar) + 1e-9)

    def extract_statistical_state(self, vpin_z: float) -> dict:
        if len(self.log_returns) > 30:
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
                
                hist, _ = np.histogram(rets, bins=10, density=False)
                total_obs = np.sum(hist)
                if total_obs > 0:
                    p = hist / total_obs
                    p = p[p > 0]
                    self.shannon_entropy = -np.sum(p * np.log2(p))

        trend_weight = max(0.0, min(1.0, (self.hurst - 0.45) / 0.10))
        fade_weight = 1.0 - trend_weight
        regime = "TRENDING" if trend_weight > 0.5 else "RANGING"
        
        logit = (self.ofi_z * 0.4) + (self.hawkes_z * 0.4) + (vpin_z * 0.2)
        logit = max(-10.0, min(10.0, logit))
        
        p_trend_up = 1.0 / (1.0 + math.exp(-logit))
        p_fade_up = 1.0 - p_trend_up
        
        p_up = (p_trend_up * trend_weight) + (p_fade_up * fade_weight)
        p_down = 1.0 - p_up
        
        return {
            "p_up": p_up, "p_down": p_down, 
            "entropy": self.shannon_entropy, "regime": regime
        }

    def solve_hjb_advantage(self, current_price: float, action: str, spread_pct: float) -> float:
        total_intensity = abs(self.hawkes_ewma) + 1e-9
        imbalance = self.hawkes_ewma / total_intensity
        
        dollar_variance = self.inst_variance * (current_price ** 2)
        reservation_price = current_price + (imbalance * dollar_variance * self.gamma)
        optimal_barrier = (self.gamma * dollar_variance) + (2 / self.kappa) * math.log(1 + self.kappa / self.gamma)
        
        if action == "BUY": return (reservation_price - current_price) / current_price - (spread_pct * optimal_barrier)
        else: return (current_price - reservation_price) / current_price - (spread_pct * optimal_barrier)


class DistributedQuantEngine:
    def __init__(self):
        load_dotenv()
        self.test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
        
        if self.test_mode: logger.critical("⚠️ TEST MODE: Paper Trading Armed. No live executions will occur.")
        else: logger.critical("🟢 LIVE MODE: Capital Deployment Armed.")
        
        self.asset_basket: List[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        self.timeframe = os.getenv("TRADING_TIMEFRAME", "15")
        self.shadow_basket: List[str] = []
        
        self.api_semaphore = asyncio.Semaphore(15)
        self.db_semaphore = asyncio.Semaphore(5)
        self.eval_semaphore = asyncio.Semaphore(10)
        
        self.tick_error_counts: Dict[str, List[float]] = {}
        self.circuit_breakers: Dict[str, float] = {}
        
        self.stream_restart_event = asyncio.Event()
        self.force_dna_refresh = asyncio.Event() 
        
        self.memory = MemoryBank()
        self.risk_vault = InstitutionalRiskVault(max_drawdown_pct=0.25, max_single_position_risk_pct=0.15)
        
        self.stat_engines: Dict[str, ContinuousMicrostructureEngine] = {} 
        self.vpin_clocks: Dict[str, VolumeSynchronizedClock] = {}
        self.edge_gates: Dict[str, MicrostructureEdgeGate] = {}
        self.feature_engines: Dict[str, AdaptiveFeatureEngine] = {}
        self.screener_memory: Dict[str, Dict[str, Any]] = {}
        self.screener_metrics: Dict[str, Dict[str, float]] = {}
        self.orderbook_snapshots: Dict[str, dict] = {}
        self.ram_dna_cache: Dict[str, dict] = {}
        self.debate_matrix = AdversarialDebateMatrix() 
        
        self.macro_regimes: Dict[str, str] = {}
        self.volatility_baseline: Dict[str, float] = {}
        self.active_positions_lock = set()
        
        self.last_eval_time: Dict[str, float] = {}
        self._active_tasks = set()
        self._log_throttle_cache: Dict[str, float] = {}
        
        self.tick_sizes: Dict[str, float] = {}
        self.global_macro_news_cache: str = "No significant macro shifts detected."
        self.last_news_fetch: float = 0.0
        self.global_state_cache = {"last_updated": 0.0}
        
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

    def track_task(self, coro: Any):
        task = asyncio.create_task(coro)
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)
        return task

    def _initialize_symbol_structures(self, symbols: List[str]):
        for s in symbols:
            if s not in self.stat_engines: self.stat_engines[s] = ContinuousMicrostructureEngine()
            if s not in self.vpin_clocks: self.vpin_clocks[s] = VolumeSynchronizedClock(bucket_volume=250_000.0)
            if s not in self.edge_gates: self.edge_gates[s] = MicrostructureEdgeGate()
            if s not in self.feature_engines: self.feature_engines[s] = AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600)
            
            if s not in self.screener_memory:
                self.screener_memory[s] = {"prices": deque(maxlen=150), "volumes": deque(maxlen=150), "atr_history": deque(maxlen=100), "last_update_time": 0.0}
            if s not in self.screener_metrics: self.screener_metrics[s] = {"vol_mult": 1.0, "smoothed_price": 0.0}
            if s not in self.volatility_baseline: self.volatility_baseline[s] = 0.0
            if s not in self.ram_dna_cache: self.ram_dna_cache[s] = {"is_armed": True, "win_rate": 0.50}
            if s not in self.last_eval_time: self.last_eval_time[s] = 0.0 
            if s not in self.orderbook_snapshots: self.orderbook_snapshots[s] = {"best_bid": 0.0, "best_ask": 0.0}

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
                logger.debug(f"Telegram dispatch failed: {e}")
                await asyncio.sleep(2 ** attempt)

    async def _fetch_exchange_tick_sizes(self):
        try:
            async with self.api_semaphore:
                info = await asyncio.to_thread(self.executor.client.get_instruments_info, category="linear")
                for item in info.get("result", {}).get("list", []):
                    self.tick_sizes[item.get("symbol")] = float(item.get("priceFilter", {}).get("tickSize", "0.0001"))
        except Exception as e:
            logger.error(f"Failed to fetch tick sizes: {e}", exc_info=True)

    async def synchronize_exchange_state(self):
        try:
            logger.info("📡 SYNCING EXCHANGE STATE: Scanning for orphaned live positions...")
            async with self.api_semaphore:
                pos_response = await asyncio.to_thread(self.executor.client.get_positions, category="linear", settleCoin="USDT")
            active_orphans = [p for p in pos_response.get("result", {}).get("list", []) if float(p.get("size", 0.0)) > 0]
            
            if not active_orphans: return
            logger.critical(f"⚠️ RECOVERY ENGAGED: Found {len(active_orphans)} active trades left open.")
            
            for pos in active_orphans:
                symbol = pos["symbol"]
                self._initialize_symbol_structures([symbol]) 
                
                qty = float(pos["size"])
                entry_price = float(pos["avgPrice"])
                direction = "BUY" if pos["side"].upper() == "BUY" else "SELL"
                
                atr = entry_price * 0.015 
                current_sl = float(pos.get("stopLoss", 0.0)) or (entry_price - (atr*1.5) if direction == "BUY" else entry_price + (atr*1.5))

                self.active_positions_lock.add(symbol)
                risk_matrix = {"allocated_value_usdt": qty * entry_price, "size": qty, "recommended_leverage": 8}
                
                self.track_task(self._position_lifecycle_daemon(
                    symbol, f"RECOVERY-{str(uuid.uuid4())[:8]}", direction, entry_price, atr, 
                    risk_matrix, 8, "RANGING", current_sl
                ))
        except Exception as e:
            logger.error(f"Sync exchange state failed: {e}", exc_info=True)

    async def cleanup_stale_locks(self):
        while True:
            await asyncio.sleep(300) 
            try:
                for symbol in list(self.active_positions_lock):
                    if not hasattr(self.risk_vault, 'active_positions') or symbol not in self.risk_vault.active_positions:
                        async with self.api_semaphore:
                            pos_response = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                        if pos_response.get("retCode") == 0:
                            if not any(float(p.get("size", 0.0)) > 0 for p in pos_response.get("result", {}).get("list", [])):
                                self.active_positions_lock.discard(symbol)
            except Exception as e:
                logger.error(f"Stale lock cleanup error: {e}", exc_info=True)

    async def _update_global_news_cache(self):
        if time.time() - self.last_news_fetch > 300: 
            try:
                context = await asyncio.wait_for(self.macro_data_feed.fetch_market_snapshot("BTCUSDT", self.timeframe), timeout=8.0)
                if context and "news_context" in context: self.global_macro_news_cache = context["news_context"]
                self.last_news_fetch = time.time()
            except Exception as e:
                logger.error(f"News fetch skipped: {e}")

    async def run_macro_commander(self):
        logger.info("🧠 MACRO COMMANDER ONLINE. Systemic LLM oversight enabled.")
        while True:
            await asyncio.sleep(900) 
            await self._update_global_news_cache()
            try:
                verdict = await self.debate_matrix.execute_debate_cycle("BTCUSDT", {"vpin_z_score": 0.0, "current_price": 0.0}, {}, self.global_macro_news_cache)
                self.macro_regimes["BTCUSDT"] = verdict.get("action", "HOLD")
            except Exception as e:
                logger.error(f"Macro Commander cycle failed: {e}", exc_info=True)

    async def run_dna_prewarmer(self):
        logger.info("🔥 RAM PRE-WARMER ONLINE: Actively pre-fetching database edge logic.")
        while True:
            try:
                await asyncio.wait_for(self.force_dna_refresh.wait(), timeout=300.0)
                self.force_dna_refresh.clear()
            except asyncio.TimeoutError: pass
                
            try:
                async def _safe_fetch(sym, dna):
                    async with self.db_semaphore:
                        return await asyncio.to_thread(self.memory.compute_latent_dna_edge, dna, 30)

                fetch_tasks = {}
                for symbol in list(self.asset_basket):
                    metrics = self.screener_metrics.get(symbol, {})
                    current_dna = {"vol_mult": metrics.get("vol_mult", 1.0), "z_obi": 0.0, "spread_pct": 0.001}
                    fetch_tasks[symbol] = _safe_fetch(symbol, current_dna)
                
                if not fetch_tasks: continue

                symbols = list(fetch_tasks.keys())
                results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
                
                for sym, result in zip(symbols, results):
                    if isinstance(result, Exception):
                        logger.error(f"DNA Cache fallback for {sym}: {result}")
                        self.ram_dna_cache[sym] = self.ram_dna_cache.get(sym, {"is_armed": True, "win_rate": 0.50})
                    else:
                        self.ram_dna_cache[sym] = result
            except Exception as e:
                logger.error(f"DNA Pre-Warmer iteration failed: {e}", exc_info=True)

    async def handle_incoming_orderbook_tick(self, depth_data: Dict[str, Any]):
        symbol = depth_data.get("s")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return

        bids, asks = depth_data.get("b", []), depth_data.get("a", [])
        if bids and asks:
            try:
                best_bid, bid_size = float(bids[0][0]), float(bids[0][1])
                best_ask, ask_size = float(asks[0][0]), float(asks[0][1])
                
                self.orderbook_snapshots[symbol] = {"best_bid": best_bid, "bid_size": bid_size, "best_ask": best_ask, "ask_size": ask_size}
                
                stat_engine = self.stat_engines.get(symbol)
                if stat_engine: stat_engine.update_orderbook_pressure(best_bid, bid_size, best_ask, ask_size)
                
                gate = self.edge_gates.get(symbol)
                if gate: gate.update_orderbook_state(best_bid, bid_size, best_ask, ask_size, (best_bid + best_ask) / 2.0)
            except Exception as e:
                logger.error(f"Orderbook tick processing error for {symbol}: {e}", exc_info=True)

        is_snapshot = depth_data.get("type") == "snapshot"
        feature_engine = self.feature_engines.get(symbol)
        if feature_engine:
            feature_engine.push_orderbook_tick(bids, asks, is_snapshot=is_snapshot)

    async def handle_incoming_trade(self, trade_data: Dict[str, Any]):
        symbol = trade_data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return
        
        now = time.time()
        if self.circuit_breakers.get(symbol, 0.0) > now: return

        try:
            stat_engine = self.stat_engines.get(symbol)
            clock = self.vpin_clocks.get(symbol)
            if not stat_engine or not clock: return
            
            price = float(trade_data.get("price", 0.0))
            volume = float(trade_data.get("size", 0.0))
            is_buy = (str(trade_data.get("side", "")).upper() == "BUY")
            timestamp = float(trade_data.get("timestamp", now * 1000)) / 1000.0
            
            stat_engine.update_trades(price, volume, is_buy, timestamp)
            
            manifests = clock.process_tick(price, volume, not is_buy)
            valid_manifests = [m for m in manifests if m.get("valid")]
            
            if valid_manifests:
                vpin_z = float(valid_manifests[-1].get("vpin_z_score", 0.0))
            elif clock.vpin_history:
                hist = np.array(list(clock.vpin_history)[-50:])
                if len(hist) >= 20 and np.std(hist) > 0:
                    vpin_z = float((clock.vpin_history[-1] - np.mean(hist)) / np.std(hist))
                else:
                    vpin_z = 0.0
            else:
                vpin_z = 0.0
            
            throttle_time = 0.2 if abs(vpin_z) > 1.5 else 1.0
            if now - self.last_eval_time.get(symbol, 0.0) < throttle_time: return
            
            ob = self.orderbook_snapshots.get(symbol)
            if not ob or "bid_size" not in ob: return
            spread_cost = abs(ob["best_ask"] - ob["best_bid"]) / price if price > 0 else 0.001
            
            async with self.eval_semaphore:
                state = stat_engine.extract_statistical_state(vpin_z)
                p_up, p_down = state["p_up"], state["p_down"]
                entropy, regime = state["entropy"], state["regime"]
                
                action = "BUY" if p_up > p_down else "SELL"
                prob_success = max(p_up, p_down)
                
                is_shadow_asset = symbol in self.shadow_basket
                if is_shadow_asset:
                    if prob_success > 0.65: 
                        async def _shadow_commit():
                            async with self.db_semaphore:
                                await asyncio.to_thread(self.memory.commit_prediction, str(uuid.uuid4()), now, price, action, prob_success, {"symbol": symbol, "market_regime": regime}, True)
                        self.track_task(_shadow_commit())
                    return 
                
                if symbol in self.active_positions_lock: return
                
                min_threshold = max(0.55, self.ram_dna_cache.get(symbol, {}).get("win_rate", 0.60))
                if prob_success < min_threshold: return 
                
                if entropy < 0.4:
                    prob_success = min(0.99, prob_success + 0.05)
                    self._throttled_log("INFO", f"🕵️ LOW ENTROPY // {symbol} Stealth footprint detected. Prob: {prob_success:.2%}", category=f"entropy_{symbol}", throttle_seconds=60)
                
                hjb_advantage = stat_engine.solve_hjb_advantage(price, action, spread_cost)
                if hjb_advantage <= 0.0: return 
                
                feature_engine = self.feature_engines.get(symbol)
                raw_atr = feature_engine.get_computed_atr() if feature_engine and hasattr(feature_engine, 'get_computed_atr') else 0.0
                atr = raw_atr if raw_atr > 0 else price * 0.005
                
                sl_dist_pct = max((atr * 1.5) / price, 0.01)
                tp_dist_pct = sl_dist_pct * 2.0
                
                ev = (prob_success * tp_dist_pct) - ((1.0 - prob_success) * sl_dist_pct) - spread_cost
                
                if ev < 0.0001: return 
                
                logger.critical(
                    f"🔬 INSTITUTIONAL TRIGGER // {symbol} [{regime}] "
                    f"| {action} | Baye-Prob: {prob_success:.2%} | EV: {ev:.4f} | HJB Adv: {hjb_advantage:.4f}"
                )
                
                self.last_eval_time[symbol] = now
                self.active_positions_lock.add(symbol)
                self.track_task(self.execute_statistical_signal(symbol, action, price, prob_success, regime, hjb_advantage, atr))
                
        except Exception as e:
            self.tick_error_counts[symbol] = [t for t in self.tick_error_counts.get(symbol, []) if now - t < 60]
            self.tick_error_counts[symbol].append(now)
            if len(self.tick_error_counts[symbol]) > 5:
                self.circuit_breakers[symbol] = now + 300 
                logger.error(f"🛑 CIRCUIT BREAKER TRIGGERED for {symbol} due to 5+ execution errors. Paused for 5 minutes.")
            logger.error(f"Handle incoming trade exception for {symbol}: {e}", exc_info=True)

    async def execute_statistical_signal(self, symbol: str, action: str, price: float, confidence: float, regime: str, hjb_adv: float, atr: float):
        try:
            dna_stats = self.ram_dna_cache.get(symbol, {"is_armed": True, "win_rate": 0.50})
            
            await self.run_signal_lifecycle(
                symbol=symbol, 
                direction=action, 
                current_price=price, 
                confidence=confidence, 
                dna_stats=dna_stats, 
                atr=atr,
                regime=regime,
                hjb_adv=hjb_adv
            )
        except Exception as e:
            logger.error(f"❌ EXECUTION ROUTING FAILURE for {symbol}: {e}", exc_info=True)
            self.active_positions_lock.discard(symbol)

    async def handle_incoming_kline_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket: return
        self._initialize_symbol_structures([symbol]) 
            
        interval = data["interval"]
        candle = data["candle_data"]
        c_open, c_high, c_low, c_close, c_vol = map(float, [candle.get("open", 0), candle.get("high", 0), candle.get("low", 0), candle.get("close", 0), candle.get("volume", 0)])

        feature_engine = self.feature_engines.get(symbol)
        if feature_engine:
            feature_engine.update_multi_timeframe_candle(timeframe=interval, open_p=c_open, high_p=c_high, low_p=c_low, close_p=c_close, volume=c_vol)

    async def handle_incoming_basket_screener_update(self, data: Dict[str, Any]):
        pass

    async def run_universe_refresher(self):
        while True:
            await asyncio.sleep(14400) 
            logger.info("🌍 FAST SATELLITE ROTATION INITIATED. Querying Bybit...")
            
            try:
                await self._fetch_exchange_tick_sizes()
                async with self.api_semaphore:
                    # 🚀 BUG FIX: We directly await custom wrapper functions
                    full_market = await self.executor.get_top_volatile_assets(100, 10_000_000)
                if len(full_market) < 25: full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
            except Exception as e:
                logger.error(f"Failed to fetch market data via REST: {e}")
                full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
                
            if "BTCUSDT" in full_market: full_market.remove("BTCUSDT")
            
            new_core_basket = ["BTCUSDT"] + [s for s in self.active_positions_lock if s != "BTCUSDT"]
            for sym in full_market:
                if sym not in new_core_basket and len(new_core_basket) < 25: new_core_basket.append(sym)
                    
            self.asset_basket = new_core_basket
            self.shadow_basket = [s for s in full_market if s not in self.asset_basket]
            if len(self.shadow_basket) < 10:
                self.shadow_basket.extend([s for s in ["XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT"] if s not in self.shadow_basket])
            
            new_vpin_clocks = {}
            new_edge_gates = {}
            new_stat = {}
            new_dna_cache = {}
            new_last_eval = {}
            new_orderbooks = {}
            new_feature_engines = {}
            
            all_symbols = self.asset_basket + self.shadow_basket
            
            for s in all_symbols:
                new_vpin_clocks[s] = self.vpin_clocks.get(s, VolumeSynchronizedClock(bucket_volume=250_000.0))
                new_edge_gates[s] = self.edge_gates.get(s, MicrostructureEdgeGate())
                new_stat[s] = self.stat_engines.get(s, ContinuousMicrostructureEngine())
                new_dna_cache[s] = self.ram_dna_cache.get(s, {})
                new_last_eval[s] = self.last_eval_time.get(s, 0.0)
                new_orderbooks[s] = self.orderbook_snapshots.get(s, {"best_bid": 0.0, "best_ask": 0.0})
                new_feature_engines[s] = self.feature_engines.get(s, AdaptiveFeatureEngine(memory_window_short=500, memory_window_long=3600))
                
            self.vpin_clocks = new_vpin_clocks
            self.edge_gates = new_edge_gates
            self.stat_engines = new_stat
            self.ram_dna_cache = new_dna_cache
            self.last_eval_time = new_last_eval
            self.orderbook_snapshots = new_orderbooks
            self.feature_engines = new_feature_engines

            try:
                historical_data = {}
                for sym in self.asset_basket:
                    if self.screener_memory.get(sym) and len(self.screener_memory[sym].get("prices", [])) > 30:
                        historical_data[sym] = list(self.screener_memory[sym]["prices"])
                if len(historical_data) >= 2:
                    self.risk_vault.update_correlation_matrix(historical_data)
                    logger.info("🛡️ Dynamic Cross-Asset Correlation Matrix Updated.")
            except Exception as e:
                logger.error(f"Correlation Matrix update skipped: {e}", exc_info=True)

            logger.info(f"🌌 QUANT UNIVERSE MATRIX RE-CALIBRATED.")
            self.stream_restart_event.set()
            self.force_dna_refresh.set() 

    async def stream_manager_loop(self):
        while True:
            stream_feed = HighVelocityMultiFeed(
                basket=self.asset_basket + self.shadow_basket[:10], intervals=["1", "5", "15"],
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

            logger.info(f"💓 SWARM HEARTBEAT: Matrix is active. Uptime: {uptime_hours:.2f} hours.")

            if loop_counter % 5 == 0:
                self.global_state_cache["last_updated"] = time.time()
                try:
                    async with self.api_semaphore:
                        # 🚀 BUG FIX: Directly await async wrapper
                        current_vault_balance = await self.executor.get_wallet_balance_usdt()
                except Exception as e:
                    logger.error(f"Failed to fetch balance during heartbeat: {e}", exc_info=True)
                    continue

                if "wallet_baseline" not in self.global_state_cache: self.global_state_cache["wallet_baseline"] = max(current_vault_balance, 0.01)
                if "start_of_day_balance" not in self.global_state_cache: self.global_state_cache["start_of_day_balance"] = current_vault_balance
                    
                today_start_iso = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                
                try:
                    def _fetch(): return self.memory.supabase.table("quantitative_ledger").select("market_regime, net_pnl, symbol, predicted_direction, actual_outcome").eq("resolved", True).eq("is_shadow", False).gte("timestamp", today_start_iso).execute()
                    async with self.db_semaphore:
                        data = (await asyncio.to_thread(_fetch)).data or []
                except Exception:
                    data = []
                
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

                report = (
                    f"💎 <b>𝗣██𝗔𝗦𝗞 𝗘𝗠𝗣𝗜𝗥𝗘 | 𝗤𝗨𝗔𝗡𝗧 𝗦𝗪𝗔𝗥𝗠 (V19.1: LIVE FIRE)</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏱️ <b>𝗨𝗽𝘁𝗶𝗺𝗲:</b> <code>{uptime_hours:.2f} Hours</code> | 🛰️ <b>𝗡𝗼𝗱𝗲𝘀:</b> <code>{len(self.asset_basket)} Live</code>\n\n"
                    f"⚙️ <b>𝗘𝗡𝗚𝗜𝗡𝗘 𝗦𝗧𝗔𝗧𝗨𝗦: 𝗣𝗿𝗼𝗱𝘂𝗰𝘁𝗶𝗼𝗻 𝗚𝗿𝗮𝗱𝗲 𝗜𝗻𝗳𝗿𝗮𝘀𝘁𝗿𝘂𝗰𝘁𝘂𝗿𝗲</b>\n"
                    f"• Orderflow Filter: <code>True 4-State Cont OFI</code>\n"
                    f"• Execution Guard: <code>Slippage-Adjusted Stops Realignment</code>\n"
                    f"• Risk Engine: <code>Correlated Penalty Scaling Active</code>\n\n"
                    f"💵 <b>𝗙𝗜𝗡𝗔𝗡𝗖𝗜𝗔𝗟 𝗩𝗔𝗨𝗟𝗧 𝗣𝗥𝗢𝗙𝗜𝗟𝗘</b>\n"
                    f"• Total Liquidity: <code>{cv:.4f} USDT</code>\n"
                    f"• Session Return:  <code>{actual:+.4f} USDT</code>\n"
                    f"• Peak Drawdown:   <code>{dd:.2%}</code>\n"
                    f"• Risk Buffer:     <code>[{dd_bar}]</code>\n\n"
                    f"🔬 <b>𝗗𝗔𝗜𝗟𝗬 𝗥𝗘𝗚𝗜𝗠𝗘 𝗣𝗥𝗢𝗙𝗜𝗟𝗘:</b>\n{regime_text}\n"
                    f"🏁 <b>𝗥𝗘𝗖𝗘𝗡𝗧 𝗦𝗘𝗦𝗦𝗜𝗢𝗡 𝗠𝗔𝗧𝗨𝗥𝗜𝗧𝗜𝗘𝗦</b>\n{recent_trades}"
                )
                self.track_task(self._safe_telegram_dispatch(report, is_html=True))

    async def run_signal_lifecycle(self, symbol: str, direction: str, current_price: float, confidence: float, dna_stats: dict, atr: float, regime: str, hjb_adv: float):
        try:
            signal_id = str(uuid.uuid4())
            is_armed = dna_stats.get("is_armed", False)

            sl_distance = max(atr * 1.5, current_price * 0.01)
            tp_distance = sl_distance * 2.0 

            try:
                async with self.api_semaphore:
                    # 🚀 BUG FIX: Directly await async wrapper
                    balance = await self.executor.get_wallet_balance_usdt()
            except Exception as e:
                logger.error(f"Failed to fetch balance for execution sizing: {e}", exc_info=True)
                self.active_positions_lock.discard(symbol)
                return False
            
            correlation_penalty = 1.0
            if hasattr(self.risk_vault, "correlation_groups"):
                for group, assets in self.risk_vault.correlation_groups.items():
                    if symbol in assets:
                        active_correlated = sum(1 for a in assets if a in self.risk_vault.active_positions)
                        if active_correlated > 0:
                            correlation_penalty = max(0.25, 1.0 - (active_correlated * 0.25))
            
            reward_risk = tp_distance / sl_distance
            base_kelly = confidence - ((1.0 - confidence) / max(0.1, reward_risk))
            account_scaling = 0.75 if balance < 100.0 else 1.0
            
            hjb_boost = 1.0 + min(1.0, hjb_adv * 10)
            quarter_kelly = max(0.005, min(0.025, base_kelly * 0.25 * account_scaling * hjb_boost * correlation_penalty))

            if quarter_kelly <= 0.0 or not is_armed:
                try:
                    async with self.db_semaphore:
                        await asyncio.to_thread(self.memory.commit_prediction, signal_id, time.time(), current_price, direction, confidence, {"symbol": symbol, "market_regime": regime}, True)
                except Exception as e:
                    logger.debug(f"Shadow commit failed: {e}")
                self.active_positions_lock.discard(symbol)
                return True

            if balance < 1.0:
                self.active_positions_lock.discard(symbol)
                return False

            dollar_risk = balance * quarter_kelly
            position_size = dollar_risk / sl_distance
            notional = max(position_size * current_price, 6.00) 

            if not self.risk_vault.evaluate_portfolio_safety(balance, notional, symbol):
                self.active_positions_lock.discard(symbol)
                return False

            target_leverage = self.risk_vault.calculate_dynamic_leverage(notional, balance, base_leverage=5, hard_cap=15, sl_distance_pct=(sl_distance / current_price))
            
            initial_sl_price = current_price - sl_distance if direction == "BUY" else current_price + sl_distance
            target_tp_price = current_price + tp_distance if direction == "BUY" else current_price - tp_distance
            
            if self.test_mode:
                logger.critical(f"📜 PAPER TRADE EXECUTED: {symbol} {direction} {position_size} @ {current_price}")
                execution_success = True
            else:
                try:
                    async with self.db_semaphore:
                        await asyncio.to_thread(self.memory.commit_prediction, signal_id, time.time(), current_price, direction, confidence, {"symbol": symbol, "market_regime": regime}, False) 
                except Exception as e:
                    logger.error(f"Failed to commit live signal to DB: {e}")

                try:
                    async with self.api_semaphore:
                        # 🚀 BUG FIX: Directly await async wrapper
                        await self.executor.adjust_leverage(symbol, target_leverage)
                        await asyncio.sleep(0.2) 
                except Exception as e:
                    logger.error(f"Leverage adjustment failed: {e}. Aborting trade.", exc_info=True)
                    self.active_positions_lock.discard(symbol)
                    return False

                feature_engine = self.feature_engines.get(symbol)
                if feature_engine and hasattr(feature_engine, 'get_orderbook_snapshot'):
                    current_depth = feature_engine.get_orderbook_snapshot()
                else:
                    cached_depth = self.orderbook_snapshots.get(symbol, {"best_bid": current_price, "best_ask": current_price})
                    current_depth = {"bids": [[cached_depth["best_bid"], 1]], "asks": [[cached_depth["best_ask"], 1]]}

                if regime == "TRENDING": execution_success = await self.sor.execute_iceberg_block(symbol=symbol, direction=direction, total_qty=position_size, current_mid_price=current_price, stop_loss=initial_sl_price, take_profit=target_tp_price, depth_snapshot=current_depth, vol_z=0.0, vol_mult=1.0, feature_engine=feature_engine)
                else: execution_success = await self.sor.execute_mean_reversion_bracket(symbol=symbol, direction=direction, total_qty=position_size, current_mid_price=current_price, stop_loss=initial_sl_price, take_profit=target_tp_price, depth_snapshot=current_depth, vol_z=0.0, vol_mult=1.0, feature_engine=feature_engine)
            
            if not execution_success:
                self.active_positions_lock.discard(symbol)
                return False 
                
            self.risk_vault.update_position_ledger(symbol, notional)
            
            self.track_task(self._safe_telegram_dispatch(f"🧬 *HFT EXECUTION FIRE*\n• Node: {symbol} | {direction}\n• Signal Probability: {confidence:.2%}\n• Leverage Applied: {target_leverage}x\n• Notional Value: ${notional:.2f} USDT", is_html=False, message_type="SUCCESS"))
            self.track_task(self._position_lifecycle_daemon(symbol, signal_id, direction, current_price, atr, {"allocated_value_usdt": notional, "size": position_size}, target_leverage, regime))
            
            return True

        except Exception as e:
            logger.error(f"Distributed swarm execution routing failed for {symbol}: {e}", exc_info=True)
            self.active_positions_lock.discard(symbol)
            return False

    async def _position_lifecycle_daemon(self, symbol: str, signal_id: str, direction: str, current_price: float, atr: float, risk_matrix: dict, target_leverage: int = 8, market_regime: str = "TRENDING"):
        logger.info(f"👻 APEX MONITOR ARMED // Native Exchange Hand-off for {symbol}")
        exec_details = {"leverage": target_leverage, "execution_mode": "RECOVERY" if "RECOVERY" in signal_id else ("GHOST" if self.test_mode else "LIVE")}
        
        if self.test_mode:
            await asyncio.sleep(60)
            logger.critical(f"📜 PAPER TRADE CLOSED: {symbol}")
            self.active_positions_lock.discard(symbol)
            return

        try:
            start_time = time.time()
            order_filled = False
            actual_entry = current_price
            
            for _ in range(5):  
                await asyncio.sleep(3)
                try:
                    async with self.api_semaphore:
                        pos_response = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                    positions = pos_response.get("result", {}).get("list", [])
                    if positions and float(positions[0].get("size", 0.0)) > 0:
                        order_filled = True
                        actual_entry = float(positions[0].get("avgPrice", current_price))
                        break
                except Exception as e: 
                    logger.error(f"Position confirmation fault: {e}", exc_info=True)
                    continue

            if not order_filled:
                logger.critical(f"🔓 PORTFOLIO UNLOCKED // SOR failed to fill {symbol} within 15s. Canceling.")
                try: 
                    async with self.api_semaphore:
                        await asyncio.to_thread(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
                except Exception: pass
                
                self.risk_vault.update_position_ledger(symbol, -risk_matrix['allocated_value_usdt'])
                self.active_positions_lock.discard(symbol)
                return

            tick_dec = Decimal(str(self.tick_sizes.get(symbol, 0.0001)))
            actual_sl_distance = max(atr * 1.5, actual_entry * 0.01)
            actual_tp_distance = actual_sl_distance * 2.0
            
            realigned_sl = actual_entry - actual_sl_distance if direction == "BUY" else actual_entry + actual_sl_distance
            realigned_sl_str = str(float((Decimal(str(realigned_sl)) / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec))
            
            realigned_tp = actual_entry + actual_tp_distance if direction == "BUY" else actual_entry - actual_tp_distance
            realigned_tp_str = str(float((Decimal(str(realigned_tp)) / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec))
            
            stops_verified = False
            for attempt in range(3):
                try:
                    async with self.api_semaphore:
                        await asyncio.to_thread(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, takeProfit=realigned_tp_str, stopLoss=realigned_sl_str)
                        pos_res = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                    
                    pos_verify = pos_res.get("result", {}).get("list", [{}])[0]
                    if float(pos_verify.get("stopLoss", 0.0)) > 0 and float(pos_verify.get("takeProfit", 0.0)) > 0:
                        stops_verified = True
                        break
                except Exception as e: 
                    logger.error(f"Failed to set/verify hard stops (Attempt {attempt+1}): {e}", exc_info=True)
                await asyncio.sleep(2)
                
            if not stops_verified:
                logger.error(f"🚨 CRITICAL: Failed to verify SL/TP for {symbol} after 3 attempts. FLATTENING POSITION.")
                flatten_side = "Sell" if direction == "BUY" else "Buy"
                async with self.api_semaphore:
                    await asyncio.to_thread(self.executor.client.place_order, category="linear", symbol=symbol, side=flatten_side, orderType="Market", qty=str(risk_matrix["size"]), timeInForce="IOC", reduceOnly=True)
                self.active_positions_lock.discard(symbol)
                return

            activation_price = str(float((Decimal(str(actual_entry + (atr * 0.8) if direction == "BUY" else actual_entry - (atr * 0.8))) / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec))
            trailing_distance_str = str(float((Decimal(str(atr * 1.5)) / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec))

            try:
                async with self.api_semaphore:
                    await asyncio.to_thread(self.executor.client.set_trading_stop, category="linear", symbol=symbol, positionIdx=0, takeProfit=realigned_tp_str, stopLoss=realigned_sl_str, trailingStop=trailing_distance_str, activePrice=activation_price)
                logger.info(f"🛡️ NATIVE TRAIL ARMED // {symbol} Trailing Stop handed to exchange (Act: {activation_price}, Dist: {trailing_distance_str})")
            except Exception as e: 
                logger.error(f"Failed to arm trailing stop: {e}", exc_info=True)

            while time.time() - start_time < 3600: 
                await asyncio.sleep(10)
                try:
                    async with self.api_semaphore:
                        pos_res = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                    pos_list = pos_res.get("result", {}).get("list", [])
                    position_gone = (not pos_list) or float(pos_list[0].get("size", 0.0)) == 0.0
                except Exception as pos_gone_err:
                    logger.error(f"Position polling error: {pos_gone_err}", exc_info=True)
                    position_gone = False

                try:
                    async with self.api_semaphore:
                        # 🚀 BUG FIX: Directly await async wrapper
                        settlement = await self.executor.check_recent_settlement(symbol, 300) 
                    if settlement.get("closed"):
                        net_pnl = float(settlement.get('pnl', 0.0))
                        self.track_task(self._safe_telegram_dispatch(f"🔔 <b>EXCHANGE EXECUTION TERMINATION</b>\n━━━━━━━━━━━━━━━━━━━━━━\n📈 <b>Asset Node:</b> <code>{symbol}</code>\n📊 <b>Outcome:</b> {'🟢 PROFIT' if net_pnl > 0 else '🔴 LOSS'}\n💰 <b>Net Return:</b> <code>{net_pnl:.4f} USDT</code>\n━━━━━━━━━━━━━━━━━━━━━━", is_html=True))
                        async with self.db_semaphore:
                            await asyncio.to_thread(self.memory.log_live_execution_result, signal_id, net_pnl, actual_entry - current_price if direction == "BUY" else current_price - actual_entry, settlement['outcome'], exec_details)
                        self.risk_vault.update_position_ledger(symbol, 0.0)
                        break
                except Exception as e:
                    logger.error(f"Settlement check error: {e}", exc_info=True)

                if position_gone:
                    logger.warning(f"🧾 RECONCILIATION // {symbol} closed outside the poll window. Pulling final PnL snapshot.")
                    try: 
                        async with self.api_semaphore:
                            pnl_res = await asyncio.to_thread(self.executor.client.get_closed_pnl, category="linear", symbol=symbol, limit=5)
                        net_pnl = float(pnl_res.get("result", {}).get("list", [])[0].get("closedPnl", 0.0))
                    except Exception: net_pnl = 0.0
                    
                    try: 
                        async with self.db_semaphore:
                            await asyncio.to_thread(self.memory.log_live_execution_result, signal_id, net_pnl, 0.0, "RECONCILED", exec_details)
                    except Exception: pass
                    
                    self.risk_vault.update_position_ledger(symbol, 0.0)
                    break
            else:
                logger.error(f"⏰ DAEMON TIMEOUT // {symbol} monitor exceeded 1h constraint. Forcing reconciliation sequence.")
                self.risk_vault.update_position_ledger(symbol, 0.0)

        except Exception as daemon_error:
            logger.error(f"☠️ FATAL DAEMON CRASH on {symbol}: {daemon_error}", exc_info=True)
            logger.critical(f"🚑 EMERGENCY INTERVENTION // Attempting to flatten {symbol} position to protect capital.")
            try:
                flatten_side = "Sell" if direction == "BUY" else "Buy"
                async with self.api_semaphore:
                    await asyncio.to_thread(self.executor.client.place_order, category="linear", symbol=symbol, side=flatten_side, orderType="Market", qty=str(risk_matrix["size"]), timeInForce="IOC", reduceOnly=True)
                logger.critical(f"✅ EMERGENCY FLATTEN SUCCESSFUL for {symbol}.")
            except Exception as flatten_e: logger.error(f"❌ EMERGENCY FLATTEN FAILED for {symbol}: {flatten_e}", exc_info=True)
                
        finally:
            self.active_positions_lock.discard(symbol)
            self.risk_vault.update_position_ledger(symbol, 0.0)

    async def graceful_shutdown(self):
        logger.critical("🛑 INITIATING EMERGENCY FLATTEN & SHUTDOWN...")
        for symbol in list(self.active_positions_lock):
            try:
                async with self.api_semaphore:
                    await asyncio.to_thread(self.executor.client.cancel_all_orders, category="linear", symbol=symbol)
                    pos_res = await asyncio.to_thread(self.executor.client.get_positions, category="linear", symbol=symbol)
                    
                pos_list = pos_res.get("result", {}).get("list", [])
                if pos_list and float(pos_list[0].get("size", 0.0)) > 0:
                    qty_str = str(float(pos_list[0].get("size")))
                    side = pos_list[0].get("side")
                    flatten_side = "Sell" if side == "Buy" else "Buy"
                    
                    async with self.api_semaphore:
                        await asyncio.to_thread(self.executor.client.place_order, category="linear", symbol=symbol, side=flatten_side, orderType="Market", qty=qty_str, timeInForce="IOC", reduceOnly=True)
                    logger.critical(f"✅ EMERGENCY FLATTEN EXECUTED for {symbol}")
            except Exception as e:
                logger.error(f"Shutdown flatten failed for {symbol}: {e}", exc_info=True)
        logger.critical("✅ MATRIX DISCONNECTED.")

    async def run_engine_forever(self):
        logger.critical("LAUNCHING DECENTRALIZED QUANT SWARM DAEMON DEPLOYMENTS...")
        
        try: await self._fetch_exchange_tick_sizes()
        except Exception as e: logger.error(f"Boot sequence tick fetch error: {e}", exc_info=True)
            
        try: await self.synchronize_exchange_state()
        except Exception as e: logger.error(f"Boot sequence sync error: {e}", exc_info=True)
        
        try: 
            async with self.api_semaphore:
                full_market = await self.executor.get_top_volatile_assets(100, 10_000_000)
            if len(full_market) < 25: full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
        except Exception: 
            full_market = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
            
        if boot_basket := full_market[:25]:
            if "BTCUSDT" in boot_basket: boot_basket.remove("BTCUSDT")
            self.asset_basket = ["BTCUSDT"] + boot_basket[:24]
            self._initialize_symbol_structures(self.asset_basket)
        
        try:
            await asyncio.gather(
                self.run_macro_commander(),
                self.run_dna_prewarmer(), 
                self.stream_manager_loop(),
                self.run_system_heartbeat(),
                self.cleanup_stale_locks() 
            )
        except Exception as global_err:
            logger.critical(f"FATAL SYSTEM ERROR: {global_err}. Initiating emergency flatten...", exc_info=True)
            await self.graceful_shutdown()

if __name__ == "__main__":
    from keep_alive import keep_alive
    keep_alive()
    engine = DistributedQuantEngine()
    try:
        asyncio.run(engine.run_engine_forever())
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt received. Graceful shutdown sequence initiated.")
        asyncio.run(engine.graceful_shutdown())
        sys.exit(0)