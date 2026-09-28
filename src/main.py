"""
V50.1 APEX TITAN: FAULT-TOLERANT BARE-METAL CORE ORCHESTRATOR (25D MANIFOLD)
---------------------------------------------------------------------------------
High-frequency multi-asset statistical micro-scalping & risk governance system.

Production Hardening & Quantitative Upgrades (V50.1 Engine Alignment):
- Resilient API Timeout Backoff: Wraps Bybit REST portfolio and position probes 
  with exponential backoff to gracefully absorb cloud network jitter (ServerTimeoutError).
- Dedicated 15s Cloud Mutex Heartbeat: Eradicates twin-leader collision window (<45s TTL).
- Pre-Flight Exchange Limit Resolution on Orphan Adoption: Guarantees exact lot size
  filters for recovered positions, preventing 0-qty exit rejects.
- True Wallet Equity Settle Recalibration: Re-queries live Bybit account equity on
  every trade settlement to eliminate synthetic balance drift.
- MarkPrice Real-Time Context Feed: Injects live mark price into CAMB lifecycle ctx
  to eliminate Bybit 34036/110043 stop clamping rejections.
- Zero-Leak Asset Memory Eviction: Completely deallocates stat engines, feature matrices,
  and throttle timestamps during dynamic symbol pruning.
- Adaptive Universe Scaling: Clamps active basket to 4 pairs on micro accounts (<$250).
"""

import os
import sys
import faulthandler

faulthandler.enable()

import math
import time
import asyncio
import logging
import uuid
import datetime
import json
import random
import numpy as np
import concurrent.futures
import multiprocessing
from collections import deque
from typing import Dict, List, Any, Callable, Set, Optional, Tuple
from dataclasses import dataclass
from decimal import Decimal
from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Early Boot: Bind Cloud Health Interface immediately
if os.getenv("PORT") or os.getenv("ENABLE_KEEP_ALIVE", "false").lower() == "true":
    try:
        from keep_alive import keep_alive
        keep_alive()
        logging.info("  TITAN HEALTH SERVER: Bound early to cloud PORT interface.")
    except Exception as e:
        logging.warning(f"[HEALTH] Early keep-alive initialization bypassed: {e}")


class EmergencyShutdown(Exception):
    """Custom exception to trigger safe, async-aware system shutdown."""
    pass


# Core & Feature Modules
from core.fsm import SystemStateMachine
from core.memory import MemoryBank
from core.quantum_entry import QuantumEntryMatrix  
from core.intelligent_exit import IntelligentExitEngine, ExecutionGovernorFSM, PositionExitState, ExitDecision, EXIT_CONFIG
from core.edge_gate import EdgeGate, EdgeGateConfig, horizon_edge_bps
from v12.runtime import V12Runtime, resolve_mode
from features.adaptive_engine import AdaptiveFeatureEngine
from features.omni_scanner import GlobalOmniScanner     
from features.micro_models import ContinuousMicrostructureEngine

# Execution & Risk
from runtime_config import get_config, TradingMode, is_banned_symbol
from market_data import is_tradeable as market_data_is_tradeable
from observability import METRICS, ReasonCode
from equity import EquitySnapshot, parse_wallet_response, estimate_live_equity, compute_drawdown
from execution.sor import SmartOrderRouter
from execution.paper_broker import PaperBroker
from portfolio.risk_vault import InstitutionalRiskVault
from execution.delta_neutral import DeltaNeutralYieldEngine 

# External Connectors
from ingestion.multi_feed import MarketStateMatrix
from services.bybit_v5 import BybitUnifiedExecutor
from services.telegram_ops import AsyncTelegramReporter
from services.sector_oracle import SectorEigenOracle

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.basicConfig(
    force=True,
    level=logging.INFO, 
    format='%(asctime)s - [%(name)s] - [%(levelname)s] - [%(message)s]', 
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("QUANT_CORE.TITAN_CORE")

# Capital Governance Floor: Defaults to $50.00 for micro-account survivability
MIN_REQUIRED_EQUITY = float(os.getenv("MIN_REQUIRED_EQUITY", "50.0"))


def _finite_float(value, default: float = 0.0) -> float:
    """Parse an exchange numeric field; missing, blank or non-finite -> default."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    return f if math.isfinite(f) else default

# Unified TradFi, Synthetic Commodity, and Settlement Asset Exclusion Matrix
BANNED_ASSET_KEYWORDS = [
    "AAPL", "TSLA", "NVDA", "AMZN", "MSFT", "GOOG", "META", "SOXL",
    "SPCX", "SKHY", "SNDK", "BANK", "MUUSDT", "BEAT", "MSTR", "ESPUSDT",
    "DEXE", "PUMP", "EUL", "XAU", "XAG", "USDC", "CLUSDT", "SSPCUSDT",
    "KO", "HANMI", "LRCX", "PURR", "MUU", "XIAOMI", "INTW", "CLANKER",
    "AAOI", "COIN", "PLTR", "ARM", "BABA", "NIO", "AMD", "WTIUSDT", "BRENTUSDT"
]


@dataclass
class MutationCommand:
    target_asset: str
    mutation_type: str
    payload: Dict[str, Any]
    callback: Optional[Callable] = None


class GlobalStateActor:
    """Centralizes position ledger, margin allocation, and in-flight mutations."""
    def __init__(self, core_engine):
        self.core = core_engine
        self.mutation_queue = asyncio.Queue(maxsize=10000)
        self._is_running = False

    async def start(self):
        self._is_running = True
        logger.info("  STATE ACTOR ONLINE: Synchronous queue loop active.")
        while self._is_running:
            try:
                cmd: MutationCommand = await self.mutation_queue.get()
                self._apply_mutation(cmd)
                self.mutation_queue.task_done()
            except asyncio.CancelledError:
                break
            except RuntimeError as e:
                if "event loop" in str(e).lower():
                    break
                logger.error(f"[X-RAY] State Actor Runtime Fault: {e}")
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"[X-RAY] State Actor Fault: {e}", exc_info=True)
                await asyncio.sleep(0.5)

    def _apply_mutation(self, cmd: MutationCommand):
        try:
            if cmd.mutation_type == "REGISTER_POSITION":
                self.core.active_positions_map[cmd.target_asset] = cmd.payload["direction"]
                self.core.risk_vault.update_position_ledger(cmd.target_asset, cmd.payload["notional"])
                self.core.in_flight_symbols.pop(cmd.target_asset, None)
                self.core.in_flight_notionals.pop(cmd.target_asset, None)

            elif cmd.mutation_type == "RESERVE_IN_FLIGHT":
                # AUDIT B10: TTL derived from the slowest execution route, not a
                # hardcoded 45s that a TWAP could outlive.
                ttl = self.core._execution_budget_seconds()
                self.core.in_flight_symbols[cmd.target_asset] = time.time() + ttl
                self.core.in_flight_notionals[cmd.target_asset] = float(cmd.payload.get("notional", 0.0))

            elif cmd.mutation_type == "RELEASE_IN_FLIGHT":
                self.core.in_flight_symbols.pop(cmd.target_asset, None)
                self.core.in_flight_notionals.pop(cmd.target_asset, None)

            elif cmd.mutation_type == "LIQUIDATE_POSITION":
                self.core.active_positions_map.pop(cmd.target_asset, None)
                self.core.in_flight_symbols.pop(cmd.target_asset, None)
                self.core.in_flight_notionals.pop(cmd.target_asset, None)
                self.core.risk_vault.update_position_ledger(cmd.target_asset, 0.0)
                self.core.exit_states.pop(cmd.target_asset, None)
                self.core.active_contexts.pop(cmd.target_asset, None)
                self.core.last_exit_direction[cmd.target_asset] = (
                    cmd.payload.get("direction", "NONE"),
                    time.time(),
                    cmd.payload.get("outcome", "NONE")
                )

            elif cmd.mutation_type == "UPDATE_PROFIT_PEAK":
                state = self.core.exit_states.get(cmd.target_asset)
                if state:
                    state.profit_state.peak_pnl = cmd.payload["peak_pnl"]
                    state.profit_state.locked_pnl = cmd.payload["locked_pnl"]

            if cmd.callback:
                cmd.callback(True)
        except Exception as e:
            logger.error(f"[X-RAY] Failed to apply mutation {cmd.mutation_type} for {cmd.target_asset}: {e}")
            if cmd.callback:
                cmd.callback(False)

    def dispatch(self, asset: str, m_type: str, payload: Dict[str, Any]):
        try:
            self.mutation_queue.put_nowait(MutationCommand(asset, m_type, payload))
        except asyncio.QueueFull:
            logger.critical(f"FATAL: State Queue Overflow on {asset}.")


async def safe_daemon_wrapper(coro_func, engine_ref):
    while not engine_ref.fsm.is_emergency_locked():
        try:
            await coro_func()
        except asyncio.CancelledError:
            break
        except EmergencyShutdown as e:
            logger.critical(f"[SHUTDOWN] Kill switch triggered inside {coro_func.__name__}: {e}")
            engine_ref.fsm.trigger_global_emergency_lock(reason=str(e))
            break
        except Exception as e:
            logger.error(f"[DAEMON CRASH] {coro_func.__name__} faulted: {e}", exc_info=True)
            engine_ref.fsm.record_module_error(coro_func.__name__)
            await asyncio.sleep(2.0)


class DistributedQuantEngine:
    def __init__(self):
        load_dotenv()
        # AUDIT B7: mode is resolved centrally and defaults to PAPER. LIVE is
        # reachable only via LIVE_TRADING=1 plus credentials; an ambiguous
        # configuration raises rather than resolving to something permissive.
        self.config = get_config()
        self.mode = self.config.mode
        # Retained for context payloads. PAPER is a full simulation via
        # PaperBroker, so no lifecycle step is skipped in any mode any more.
        self.test_mode = not self.config.mode.places_real_orders

        for line in self.config.banner().splitlines():
            logger.critical(line)

        self.asset_basket: List[str] = []
        self.timeframe = os.getenv("TRADING_TIMEFRAME", "15")
        # AUDIT B5/B6: the ATR/bar timeframe must actually be subscribed.
        self.bar_series_interval = os.getenv("BAR_SERIES_INTERVAL", "5")
        self.kline_intervals = sorted(
            {self.timeframe, self.bar_series_interval, "60", "240"},
            key=lambda x: int(x)
        )
        self.shadow_basket: List[str] = []

        safe_workers = min(4, multiprocessing.cpu_count() - 1) if multiprocessing.cpu_count() > 1 else 1
        self.math_pool = concurrent.futures.ThreadPoolExecutor(max_workers=safe_workers, thread_name_prefix="Titan_Math")
        self.io_pool = concurrent.futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix="Titan_IO")

        self.db_semaphore = asyncio.Semaphore(10)
        self.execution_semaphore = asyncio.Semaphore(8)

        self.circuit_breakers: Dict[str, float] = {}
        self.circuit_breaker_lock = asyncio.Lock()
        # AUDIT B9: guards check-then-reserve as one atomic step.
        self.reservation_lock = asyncio.Lock()

        self.stream_restart_event = asyncio.Event()
        self.force_dna_refresh = asyncio.Event()

        self.fsm = SystemStateMachine()

        try:
            self.memory = MemoryBank()
        except Exception as e:
            logger.error(f"[X-RAY] CLOUD DB OFFLINE: Supabase connection failed ({e}). Booting in Local Mode.")
            self.memory = None

        # APEX 2026-09: after-cost edge gate. Learns from this account's own
        # settled trades which conviction bands actually pay. Shadow by default.
        self.edge_gate = EdgeGate(EdgeGateConfig.from_env())
        self._entry_conviction: Dict[str, float] = {}

        # V12: multi-horizon forecasts -> expected value AFTER costs -> kill-switch
        # hierarchy -> trade journal and learning loop. See src/v12/runtime.py.
        # V12_MODE=enforce (default) | shadow | off.
        self.v12_mode = resolve_mode()
        self.v12: Optional[V12Runtime] = None
        if self.v12_mode != "off":
            try:
                self.v12 = V12Runtime(trading_mode=self.mode.value, mode=self.v12_mode)
                logger.critical(f"  V12 PIPELINE {self.v12_mode.upper()} // exit policy: "
                                f"{self.v12.exit_policy_source}")
            except Exception as e:
                logger.critical(
                    f"[V12] pipeline failed to start ({e}). "
                    + ("NEW ENTRIES BLOCKED -- enforce mode fails closed." if self.v12_mode == "enforce"
                       else "Shadow evaluation disabled."), exc_info=True)

        max_dd_pct = float(os.getenv("MAX_DRAWDOWN_PCT", "0.15"))
        single_risk_pct = float(os.getenv("MAX_SINGLE_POSITION_RISK_PCT", "0.025"))

        self.risk_vault = InstitutionalRiskVault(
            max_drawdown_pct=max_dd_pct, 
            max_single_position_risk_pct=single_risk_pct,
            exchange_min_notional=6.50,
            tail_gap_cushion_pct=0.0020
        )
        logger.info(
            f"  RISK VAULT INITIALIZED // Hard Stop: {max_dd_pct:.1%} | "
            f"Soft Freeze: {self.risk_vault.soft_freeze_drawdown_pct:.1%} | "
            f"Daily Limit: {self.risk_vault.daily_loss_limit_pct:.1%} | "
            f"Single Pos Risk Cap: {single_risk_pct:.1%}"
        )

        self.yield_engine = DeltaNeutralYieldEngine(self)

        self.stat_engines: Dict[str, ContinuousMicrostructureEngine] = {}
        self.feature_engines: Dict[str, AdaptiveFeatureEngine] = {}
        self.entry_matrices: Dict[str, QuantumEntryMatrix] = {}

        self.screener_memory, self.screener_metrics, self.ram_dna_cache = {}, {}, {}
        self.tick_history: Dict[str, deque] = {}
        self.volatility_baseline: Dict[str, float] = {}
        self.orderbook_snapshots: Dict[str, dict] = {}

        self.state_actor = GlobalStateActor(self)
        self.active_positions_map: Dict[str, str] = {}
        self.in_flight_symbols: Dict[str, float] = {}
        self.in_flight_notionals: Dict[str, float] = {}
        self.last_exit_direction: Dict[str, Tuple[str, float, str]] = {}

        self.symbol_locks, self.daemon_tasks, self.last_eval_time = {}, {}, {}
        self.last_amend_time: Dict[str, float] = {}
        self._active_tasks = set()
        self._last_overflow_log = 0.0

        self._evaluating_symbols: Set[str] = set()
        self._cluster_calc_lock = asyncio.Lock()
        self._last_cluster_time: float = 0.0
        self._cached_cluster_returns: Dict[str, List[float]] = {}

        self.global_state_cache = {"last_updated": 0.0}
        self.live_params = self._load_live_params()
        self.last_socket_reconnect = 0.0

        self.subscription_state: Set[str] = set()   # AUDIT B3
        self.equity_snapshot: Optional[EquitySnapshot] = None   # AUDIT B11
        self.active_contexts: Dict[str, dict] = {}
        self.exit_states: Dict[str, Any] = {}
        self.recent_pnl_history: deque = deque(maxlen=20)

        self.telegram = AsyncTelegramReporter(token=os.getenv("TELEGRAM_BOT_TOKEN"), chat_id=os.getenv("TELEGRAM_CHAT_ID"))
        self.telegram_queue = asyncio.Queue(maxsize=50)

        self._real_executor = BybitUnifiedExecutor(
            api_key=os.getenv("BYBIT_API_KEY"),
            api_secret=os.getenv("BYBIT_API_SECRET"),
            testnet=self.config.mode.uses_testnet_endpoints
        )

        # AUDIT B7: in PAPER the broker intercepts every state-changing endpoint
        # and simulates the account in memory, while public market data still
        # comes from the real exchange. No order can reach Bybit.
        self.paper_broker = None
        if self.config.mode is TradingMode.PAPER:
            self.paper_broker = PaperBroker(
                self._real_executor,
                starting_balance=float(os.getenv("PAPER_STARTING_BALANCE", "1000.0")),
            )
            self.executor = self.paper_broker
            logger.critical("  PAPER BROKER ENGAGED: no order will reach the exchange.")
        else:
            self.executor = self._real_executor
        if self.v12 is not None:
            # every REST call reports transport health to the V12 guardian (L5)
            self._real_executor.api_observer = self.v12.record_api

        self.sor = SmartOrderRouter(executor=self.executor, max_slippage_pct=0.0012, core_engine=self)
        self.omni_scanner = GlobalOmniScanner(self.executor)
        self.stream_feed_instance = None
        self.instance_id = "LOCAL_SOLO"

    def _on_task_done(self, task):
        self._active_tasks.discard(task)
        if not task.cancelled() and task.exception():
            logger.error(f"[X-RAY] BACKGROUND TASK CRASHED: {task.exception()}", exc_info=task.exception())

    def track_task(self, coro: Any, is_critical: bool = False):
        if len(self._active_tasks) > 350 and not is_critical:
            now = time.time()
            if now - self._last_overflow_log > 5.0:
                logger.warning(f"[X-RAY] TASK LIMIT EXCEEDED ({len(self._active_tasks)} > 350). Shedding non-critical task.")
                self._last_overflow_log = now
            if asyncio.iscoroutine(coro):
                async def _safe_close(c):
                    try:
                        c.close()
                    except Exception:
                        pass
                asyncio.create_task(_safe_close(coro))
            dummy = asyncio.Future()
            dummy.set_result(None)
            return dummy

        task = asyncio.create_task(coro)
        self._active_tasks.add(task)
        task.add_done_callback(self._on_task_done)
        return task

    def _load_live_params(self) -> dict:
        default_params = {"sl_atr_mult": 2.5, "rr_ratio": 2.0, "LEVERAGE_CAP": 2.0}
        try:
            if os.path.exists("params.json"):
                with open("params.json", "r") as f:
                    return {**default_params, **json.load(f)}
        except Exception:
            pass
        return default_params

    async def _get_true_equity_usdt(self) -> float:
        for attempt in range(1, 6):
            try:
                bal = await self.executor.get_wallet_balance_usdt()
                if bal > 0.0:
                    return bal
            except Exception as e:
                logger.warning(f"[X-RAY] Equity probe attempt {attempt}/5 failed: {e}")
            await asyncio.sleep(attempt * 1.5)
        return 0.0

    async def _prune_dead_symbols(self):
        """Zero-leak asset eviction: completely cleans up state dictionaries and engines."""
        active_set = set(
            self.asset_basket + self.shadow_basket + 
            list(self.active_positions_map.keys()) + list(self.in_flight_symbols.keys())
        )
        for key in list(self.stat_engines.keys()):
            if key not in active_set:
                self.symbol_locks.pop(key, None)
                self.tick_history.pop(key, None)
                self.screener_memory.pop(key, None)
                self.orderbook_snapshots.pop(key, None)
                self.last_exit_direction.pop(key, None)
                self.active_contexts.pop(key, None)
                self.exit_states.pop(key, None)
                self.circuit_breakers.pop(key, None)
                self.screener_metrics.pop(key, None)
                self.volatility_baseline.pop(key, None)
                self.ram_dna_cache.pop(key, None)
                self.stat_engines.pop(key, None)
                self.feature_engines.pop(key, None)
                self.entry_matrices.pop(key, None)
                self.last_eval_time = {k: v for k, v in self.last_eval_time.items() if not k.startswith(key)}
                logger.info(f"[PRUNE] Cleanly evicted inactive asset memory for {key}.")

    def _load_sgd_state_from_disk(self) -> dict:
        try:
            storage_path = os.getenv("PERSISTENT_STORAGE_PATH", ".")
            target_path = os.path.join(storage_path, "sgd_state.json")
            if os.path.exists(target_path):
                with open(target_path, "r") as f:
                    data = json.load(f)
                    if not isinstance(data, dict):
                        return {}
                    if "_version" in data and "symbols" in data and isinstance(data["symbols"], dict):
                        return data["symbols"]
                    return data
        except Exception as e:
            logger.warning(f"[X-RAY] Recovering from unreadable or corrupt RLS state cache: {e}")
        return {}

    async def _save_sgd_state(self):
        state_snapshot = {
            "_version": 1,
            "saved_at": time.time(),
            "symbols": {}
        }
        for sym, engine in self.stat_engines.items():
            if hasattr(engine, 'export_state'):
                state_snapshot["symbols"][sym] = engine.export_state()

        def _write_file():
            import tempfile
            try:
                storage_path = os.getenv("PERSISTENT_STORAGE_PATH", ".")
                target_path = os.path.join(storage_path, "sgd_state.json")
                fd, path = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(target_path)) or ".")
                with os.fdopen(fd, 'w') as f:
                    json.dump(state_snapshot, f)
                os.replace(path, target_path)
            except Exception as e:
                logger.debug(f"[X-RAY] Failed RLS disk serialization: {e}")

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.io_pool, _write_file)

    def _initialize_symbol_structures(self, symbols: List[str]):
        if not hasattr(self, "saved_sgd_state"):
            self.saved_sgd_state = self._load_sgd_state_from_disk()

        for s in symbols:
            if s not in self.stat_engines:
                engine = ContinuousMicrostructureEngine(symbol=s)
                if s in self.saved_sgd_state and isinstance(self.saved_sgd_state[s], dict):
                    engine.load_state(self.saved_sgd_state[s])
                self.stat_engines[s] = engine

            if s not in self.feature_engines:
                self.feature_engines[s] = AdaptiveFeatureEngine(
                    memory_window_long=1800, base_timeframe=self.bar_series_interval
                )
            if s not in self.entry_matrices:
                self.entry_matrices[s] = QuantumEntryMatrix(window_size=10)
            if s not in self.symbol_locks:
                self.symbol_locks[s] = asyncio.Lock()
            if s not in self.tick_history:
                self.tick_history[s] = deque(maxlen=2000)
            if s not in self.screener_memory:
                self.screener_memory[s] = {
                    "prices": deque(maxlen=1440), "highs": deque(maxlen=150),
                    "lows": deque(maxlen=150), "volumes": deque(maxlen=1440),
                    "last_update_time": 0.0
                }
            if s not in self.screener_metrics:
                self.screener_metrics[s] = {"vol_mult": 1.0}
            if s not in self.volatility_baseline:
                self.volatility_baseline[s] = 0.0
            if s not in self.ram_dna_cache:
                self.ram_dna_cache[s] = {"is_armed": True, "win_rate": 0.50}
            if s not in self.last_eval_time:
                self.last_eval_time[s] = 0.0
            if s not in self.orderbook_snapshots:
                self.orderbook_snapshots[s] = {}

    def _safe_telegram_dispatch_sync(self, message: str, is_html: bool = True, message_type: str = "SUCCESS"):
        if not os.getenv("TELEGRAM_BOT_TOKEN") or len(os.getenv("TELEGRAM_BOT_TOKEN", "")) < 5:
            return
        try:
            self.telegram_queue.put_nowait((message, is_html, message_type))
        except asyncio.QueueFull:
            logger.warning("[X-RAY] Telegram queue full. Dropping telemetry message.")

    async def _safe_telegram_dispatch(self, message: str, is_html: bool = True, message_type: str = "SUCCESS"):
        self._safe_telegram_dispatch_sync(message, is_html, message_type)

    async def run_telegram_worker(self):
        logger.info("  TELEGRAM WORKER ONLINE: Async queue telemetry active.")
        while True:
            try:
                message, is_html, msg_type = await self.telegram_queue.get()
                if not os.getenv("TELEGRAM_BOT_TOKEN"):
                    self.telegram_queue.task_done()
                    continue

                if is_html:
                    await self.telegram.send_html_report(message)
                else:
                    await self.telegram.log_message(message, msg_type)
                self.telegram_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Telegram dispatch fault: {e}")
                self.telegram_queue.task_done()

    async def run_cloud_lease_heartbeat(self):
        """Dedicated 15s lease heartbeat to maintain single-leader cloud mutex (<45s TTL)."""
        logger.info("  CLOUD LEASE HEARTBEAT ONLINE: 15s Cadence.")
        while not self.fsm.is_emergency_locked():
            try:
                if self.memory and self.memory.supabase and self.instance_id != "LOCAL_SOLO":
                    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    await asyncio.to_thread(
                        self.memory.supabase.table("swarm_instance_lease")
                        .update({"last_heartbeat": now_iso})
                        .eq("environment", "PRODUCTION")
                        .eq("instance_id", self.instance_id)
                        .execute
                    )
            except Exception as e:
                logger.debug(f"[LEASE] Heartbeat renewal bypassed: {e}")
            await asyncio.sleep(15.0)

    async def run_correlation_engine(self):
        logger.info("  CORRELATION ENGINE ONLINE: 15s High-Frequency Covariance Tracking.")
        while True:
            await asyncio.sleep(15.0)
            if not self.fsm.can_execute_trades:
                continue
            try:
                price_histories = {}
                for sym, mem in self.screener_memory.items():
                    if len(mem.get("prices", [])) >= 60:
                        price_histories[sym] = list(mem["prices"])[-60:]
                if price_histories:
                    is_stressed = any(
                        getattr(eng, 'jump_z', 0.0) > 2.0 or getattr(eng, 'changepoint_prob', 0.0) > 0.50
                        for eng in self.stat_engines.values()
                    )
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        self.math_pool, 
                        self.risk_vault.update_correlation_matrix, 
                        price_histories, 
                        is_stressed
                    )
            except Exception as e:
                logger.debug(f"[X-RAY] Fast Correlation Matrix update fault: {e}")

    async def synchronize_exchange_state(self):
        try:
            open_orders_res = await self.executor.safe_call("GET", "/v5/order/realtime", category="linear", settleCoin="USDT")
            open_orders = open_orders_res.get("result", {}).get("list", [])
            if open_orders:
                logger.warning(f"  RECONCILIATION: Found {len(open_orders)} resting orders. Canceling...")
                for o in open_orders:
                    await self.executor.safe_call("POST", "/v5/order/cancel", is_execution=True, category="linear", symbol=o["symbol"], orderId=o["orderId"])

            pos_response = await self.executor.safe_call("GET", "/v5/position/list", category="linear", settleCoin="USDT")
            active_orphans = [p for p in pos_response.get("result", {}).get("list", []) if float(p.get("size", 0.0)) > 0]
            if not active_orphans:
                return

            target_lev = int(self.live_params.get("LEVERAGE_CAP", 2.0))
            logger.critical(f"  RECOVERY ENGAGED: Found {len(active_orphans)} open positions. Adopting into matrix.")
            for pos in active_orphans:
                symbol = pos["symbol"]
                self._initialize_symbol_structures([symbol])
                await self.sor._fetch_exchange_limits(symbol)
                specs = self.sor.instrument_cache.get(symbol, {})
                qty_step_str = str(specs.get("qty_step", "0.001"))

                qty, entry_price = float(pos["size"]), float(pos["avgPrice"])
                direction = "BUY" if pos["side"].upper() == "BUY" else "SELL"

                feature_eng = self.feature_engines.get(symbol)
                computed_atr = feature_eng.get_computed_atr() if feature_eng else (entry_price * 0.004)
                atr = computed_atr if computed_atr > 0 else (entry_price * 0.004)

                self.state_actor.dispatch(symbol, "REGISTER_POSITION", {"direction": direction, "notional": qty * entry_price})
                risk_matrix = {
                    "allocated_value_usdt": qty * entry_price, 
                    "size": qty, 
                    "arrival_price": entry_price, 
                    "qty_step": qty_step_str
                }

                self.daemon_tasks[symbol] = self.track_task(self._position_lifecycle_daemon(
                    symbol, str(uuid.uuid4()), direction, entry_price, atr, risk_matrix, target_lev, "RANGING", is_recovery=True
                ), is_critical=True)
        except Exception as e:
            logger.error(f"[X-RAY] Failed synchronizing exchange state: {e}", exc_info=True)

    async def run_fast_state_invariant_reconciliation(self):
        logger.info("  10s STATE INVARIANT RECONCILIATION DAEMON ONLINE.")
        while True:
            await asyncio.sleep(10.0)
            try:
                now = time.time()
                expired_flights = [sym for sym, exp in self.in_flight_symbols.items() if now > exp]
                for sym in expired_flights:
                    self.in_flight_symbols.pop(sym, None)
                    self.in_flight_notionals.pop(sym, None)
                    logger.warning(f"[X-RAY] IN-FLIGHT TTL EXPIRED: Purged lock for {sym}")

                pos_response = await self.executor.safe_call("GET", "/v5/position/list", category="linear", settleCoin="USDT")
                if pos_response.get("retCode") != 0:
                    if self.v12 is not None:
                        self.v12.set_reconciliation(False, f"position query failed (retCode {pos_response.get('retCode')})")
                    continue
                active_on_exchange = {p["symbol"]: p for p in pos_response.get("result", {}).get("list", []) if float(p.get("size", 0.0)) > 0}

                # V12 (item 11): the exchange is the source of truth. Any symbol the
                # bot and the exchange disagree about blocks NEW entries until the
                # adoption/release below has brought them back into agreement.
                if self.v12 is not None:
                    mismatch = []
                    for s_ in set(active_on_exchange) | set(self.active_positions_map):
                        if s_ in self.in_flight_symbols:
                            continue
                        ex = active_on_exchange.get(s_)
                        local = self.active_positions_map.get(s_)
                        if ex is None or local is None:
                            mismatch.append(s_)
                        elif str(local).upper() in ("BUY", "SELL") and \
                                ("BUY" if str(ex.get("side", "")).upper() == "BUY" else "SELL") != str(local).upper():
                            mismatch.append(s_)
                    self.v12.set_positions({
                        s_: ("BUY" if str(p.get("side", "")).upper() == "BUY" else "SELL",
                             _finite_float(p.get("size")) * _finite_float(p.get("markPrice") or p.get("avgPrice")))
                        for s_, p in active_on_exchange.items()})
                    self.v12.set_reconciliation(
                        not mismatch, "ok" if not mismatch else f"bot and exchange disagree on {sorted(mismatch)}")

                # 1. Purge desynchronized local positions
                for tracked_sym in list(self.active_positions_map.keys()):
                    if tracked_sym not in active_on_exchange and tracked_sym not in self.in_flight_symbols:
                        if not (daemon := self.daemon_tasks.get(tracked_sym)) or daemon.done():
                            logger.warning(f"[X-RAY] INVARIANT ENFORCED: Releasing closed position for {tracked_sym}")
                            self.state_actor.dispatch(tracked_sym, "LIQUIDATE_POSITION", {"direction": "NONE", "outcome": "RECONCILED"})

                # 2. Adopt orphaned exchange positions
                target_lev = int(self.live_params.get("LEVERAGE_CAP", 2.0))
                for ex_sym, pos_data in active_on_exchange.items():
                    if ex_sym not in self.active_positions_map and ex_sym not in self.in_flight_symbols:
                        logger.critical(f"[X-RAY] ORPHAN ADOPTED: Found untracked position for {ex_sym}.")
                        qty = float(pos_data["size"])
                        entry_price = float(pos_data.get("avgPrice", pos_data.get("markPrice", 0.0)))
                        direction = "BUY" if pos_data["side"].upper() == "BUY" else "SELL"

                        if ex_sym not in self.stat_engines:
                            self._initialize_symbol_structures([ex_sym])
                        
                        await self.sor._fetch_exchange_limits(ex_sym)
                        specs = self.sor.instrument_cache.get(ex_sym, {})
                        qty_step_str = str(specs.get("qty_step", "0.001"))

                        feature_eng = self.feature_engines.get(ex_sym)
                        computed_atr = feature_eng.get_computed_atr() if feature_eng else (entry_price * 0.004)
                        atr = computed_atr if computed_atr > 0 else (entry_price * 0.004)

                        self.state_actor.dispatch(ex_sym, "REGISTER_POSITION", {"direction": direction, "notional": qty * entry_price})
                        self.daemon_tasks[ex_sym] = self.track_task(self._position_lifecycle_daemon(
                            ex_sym, str(uuid.uuid4()), direction, entry_price, atr,
                            {"allocated_value_usdt": qty * entry_price, "size": qty, "arrival_price": entry_price, "qty_step": qty_step_str}, 
                            target_lev, "RANGING", is_recovery=True
                        ), is_critical=True)
            except Exception as e:
                logger.debug(f"[X-RAY] Invariant sync cycle bypassed: {e}")
                if self.v12 is not None:
                    self.v12.set_reconciliation(False, f"reconciliation cycle failed: {e}")

    async def run_system_heartbeat(self):
        start_time, loop_counter = time.time(), 0
        while True:
            await asyncio.sleep(60)
            loop_counter += 1
            uptime_hours = (time.time() - start_time) / 3600

            # AUDIT B8: clear a health halt once the subsystem is quiet again.
            self.fsm.maybe_auto_recover_health(quiet_period_seconds=300.0)
            self._active_tasks = {t for t in self._active_tasks if not t.done()}
            active_ticks = {s: len(eng.tick_prices) for s, eng in self.stat_engines.items()}
            calibrated_count = sum(1 for cnt in active_ticks.values() if cnt >= 50)
            logger.info(
                f"[RADAR] SWARM ACTIVE // Calibrated: {calibrated_count}/{len(self.asset_basket)} nodes | "
                f"Active Tasks: {len(self._active_tasks)} | Uptime: {uptime_hours:.2f}h"
            )

            if loop_counter % 5 == 0:
                self.global_state_cache["last_updated"] = time.time()
                await self._save_sgd_state()

                # AUDIT B11: refresh wallet balance and equity together.
                snap = await self._refresh_equity_snapshot()
                if snap is None:
                    continue
                current_vault_balance = snap.equity
                if current_vault_balance <= 0.0:
                    continue

                # Centralized Risk Vault Drawdown Update
                daily_dd, systemic_dd, is_breached = await self.risk_vault.update_balance_atomic(current_vault_balance)

                now_utc = datetime.datetime.now(datetime.timezone.utc)
                current_day = now_utc.strftime("%Y-%m-%d")
                if self.global_state_cache.get("current_day") != current_day:
                    self.global_state_cache["current_day"] = current_day
                    self.global_state_cache["start_of_day_balance"] = current_vault_balance

                try:
                    execution_stats = await self.memory.get_forensic_execution_summary(
                        now_utc.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                    ) if self.memory else {}
                except Exception:
                    execution_stats = {}

                execution_stats["rolling_pnl_array"] = list(self.recent_pnl_history) if self.recent_pnl_history else [0.0]
                actual_net_pnl = current_vault_balance - self.global_state_cache.get("lifetime_initial_balance", current_vault_balance)

                if is_breached and len(self.active_positions_map) > 0:
                    self.fsm.trigger_global_emergency_lock(reason=f"Drawdown breach: Systemic={systemic_dd:.2%}, Daily={daily_dd:.2%}")
                    self._safe_telegram_dispatch_sync(
                        f"<b>EMERGENCY DRAWDOWN BREAKER TRIPPED</b>\n"
                        f"Systemic Drawdown: {systemic_dd:.2%} | Daily: {daily_dd:.2%}. Engine shutting down.",
                        is_html=True
                    )
                    raise EmergencyShutdown(f"Drawdown breach detected: Systemic={systemic_dd:.2%}, Daily={daily_dd:.2%}")

                filled_blocks = min(10, int(systemic_dd * 100))
                self.global_state_cache.update({
                    "drawdown_bar": " " * (10 - filled_blocks) + " " * filled_blocks,
                    "actual_net_pnl": actual_net_pnl,
                    "current_vault_balance": current_vault_balance,
                    "drawdown_pct": systemic_dd
                })

            if loop_counter % 10 == 0:
                report = self.telegram.format_mission_control_dashboard(
                    uptime_hours, len(self.asset_basket), len(self.shadow_basket),
                    self.global_state_cache.get("current_vault_balance", 0.0),
                    self.global_state_cache.get("actual_net_pnl", 0.0),
                    self.global_state_cache.get("drawdown_pct", 0.0),
                    self.global_state_cache.get("drawdown_bar", " " * 10),
                    execution_stats
                )
                self._safe_telegram_dispatch_sync(report, is_html=True)

    async def _get_cluster_returns(self) -> Dict[str, List[float]]:
        now = time.time()
        if now - self._last_cluster_time < 1.0 and self._cached_cluster_returns:
            return self._cached_cluster_returns

        async with self._cluster_calc_lock:
            if now - self._last_cluster_time < 1.0 and self._cached_cluster_returns:
                return self._cached_cluster_returns

            def _compute_core():
                cluster_returns = {}
                for s, engine in self.stat_engines.items():
                    if len(engine.tick_prices) >= 40:
                        arr = np.array(list(engine.tick_prices)[-120:], dtype=np.float64)
                        cluster_returns[s] = np.diff(np.log(arr + 1e-9)).tolist()
                return cluster_returns

            loop = asyncio.get_running_loop()
            self._cached_cluster_returns = await loop.run_in_executor(self.math_pool, _compute_core)
            self._last_cluster_time = time.time()
            return self._cached_cluster_returns

    async def handle_incoming_orderbook_tick(self, rich_payload: Dict[str, Any]):
        symbol = rich_payload.get("symbol")
        if not symbol or (symbol not in self.asset_basket and symbol not in self.shadow_basket and symbol not in self.active_positions_map and symbol not in self.in_flight_symbols):
            return
        now = time.time()

        micro_price = float(rich_payload.get("micro_price", 0.0) or 0.0)
        if micro_price <= 0.0:
            return

        if self.paper_broker is not None:
            self.paper_broker.update_mark(symbol, micro_price)

        if self.v12 is not None:
            try:
                sp = rich_payload.get("spread_bps")
                self.v12.on_tick(symbol, now, micro_price,
                                 float(sp) if sp is not None else None,
                                 self._ob_age_sec(rich_payload, now))
            except Exception as e:
                logger.debug(f"[V12] tick ingest fault for {symbol}: {e}")

        if len(self._active_tasks) > 300:
            return

        async with self.symbol_locks[symbol]:
            self.screener_memory[symbol]["last_update_time"] = now
            self.orderbook_snapshots[symbol] = rich_payload

            if symbol in self.active_contexts:
                self.active_contexts[symbol]["latest_tick_price"] = micro_price
                if "markPrice" in rich_payload:
                    self.active_contexts[symbol]["mark_price"] = float(rich_payload["markPrice"])

            stat_engine = self.stat_engines.get(symbol)
            if stat_engine and hasattr(stat_engine, 'update_orderbook_pressure'):
                try:
                    stat_engine.update_orderbook_pressure(rich_payload.get("bids", []), rich_payload.get("asks", []))
                except Exception:
                    pass

            if now - self.last_eval_time.get(symbol + "_eval_throttle", 0.0) < 0.20:
                return

            if symbol in self._evaluating_symbols:
                return

            self.last_eval_time[symbol + "_eval_throttle"] = now
            self._evaluating_symbols.add(symbol)
            # AUDIT B25: only the LENGTH is used downstream; this copied up to
            # 2000 floats per evaluation, ~5x/sec/symbol, for nothing.
            tick_count = len(stat_engine.tick_prices) if stat_engine else 0

        self.track_task(self._eval_gate_wrapper(symbol, rich_payload, stat_engine, now, tick_count), is_critical=False)

    @staticmethod
    def _ob_age_sec(payload: dict, now: float) -> Optional[float]:
        """Age of an orderbook payload in seconds, or None if it carries no timestamp."""
        for key in ("ts", "timestamp", "received_at", "local_ts"):
            v = payload.get(key)
            if v is None:
                continue
            try:
                t = float(v)
            except (TypeError, ValueError):
                continue
            if t > 1e12:            # milliseconds
                t /= 1000.0
            return max(0.0, now - t)
        return None

    async def _eval_gate_wrapper(self, symbol: str, ob_payload: dict, stat_engine: ContinuousMicrostructureEngine, now: float, tick_count: int):
        try:
            await self._eval_gate(symbol, ob_payload, stat_engine, now, tick_count)
        finally:
            self._evaluating_symbols.discard(symbol)

    def handle_incoming_trade(self, trade_data: Dict[str, Any]):
        symbol = trade_data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket and symbol not in self.active_positions_map and symbol not in self.in_flight_symbols:
            return
        now = time.time()
        price = float(trade_data.get("price", 0.0))
        if price < 0.000001:
            return
        volume = float(trade_data.get("size", 0.0))
        is_buy = str(trade_data.get("side", "")).upper() == "BUY"
        exchange_timestamp = float(trade_data.get("timestamp", now * 1000)) / 1000.0

        if symbol in self.tick_history:
            self.tick_history[symbol].append((exchange_timestamp, price))
        if symbol in self.active_contexts:
            self.active_contexts[symbol]["latest_tick_price"] = price

        if self.paper_broker is not None:
            self.paper_broker.update_mark(symbol, price)

        stat_engine = self.stat_engines.get(symbol)
        if stat_engine:
            stat_engine.update_trades(price, exchange_timestamp, volume, is_buy)

    def handle_incoming_kline_update(self, data: Dict[str, Any]):
        symbol = data.get("symbol")
        if symbol not in self.asset_basket and symbol not in self.shadow_basket and symbol not in self.active_positions_map and symbol not in self.in_flight_symbols:
            return
        self._initialize_symbol_structures([symbol])
        interval, candle = str(data["interval"]), data["candle_data"]

        # AUDIT B6: Bybit pushes the FORMING candle roughly once per second.
        # Appending those built historical series out of ~1 Hz snapshots of a
        # partial bar: the "15m" deque held ~15 minutes of pushes, the "4h"
        # deque ~100 SECONDS. ATR, multi-timeframe momentum, HTF bias, dynamic
        # R:R, portfolio correlation and the shadow ledger were all computed on
        # data that was not bars. Only CLOSED candles may enter a bar series.
        is_closed = bool(candle.get("confirm", False))
        if not is_closed:
            return

        c_open, c_high, c_low, c_close, c_vol = map(
            float,
            [candle.get("open", 0), candle.get("high", 0), candle.get("low", 0), candle.get("close", 0), candle.get("volume", 0)]
        )

        if feature_engine := self.feature_engines.get(symbol):
            feature_engine.update_multi_timeframe_candle(
                timeframe=interval, open_p=c_open, high_p=c_high, low_p=c_low, close_p=c_close, volume=c_vol
            )
            # Correlation and shadow resolution read this series, so it is built
            # from one specific closed timeframe rather than from whatever
            # message happened to arrive.
            if str(interval) == str(self.bar_series_interval) and symbol in self.screener_memory:
                self.screener_memory[symbol].setdefault("highs", deque(maxlen=150)).append(c_high)
                self.screener_memory[symbol].setdefault("lows", deque(maxlen=150)).append(c_low)
                self.screener_memory[symbol].setdefault("prices", deque(maxlen=1440)).append(c_close)
                self.screener_memory[symbol]["last_update_time"] = time.time()
                self.screener_memory[symbol]["closed_bars"] = self.screener_memory[symbol].get("closed_bars", 0) + 1

    def handle_incoming_basket_screener_update(self, data: Dict[str, Any]):
        if (symbol := data.get("symbol")) not in self.asset_basket and symbol not in self.shadow_basket:
            return
        try:
            # AUDIT B13: this previously required a "raw_data" wrapper key that
            # MarketStateMatrix never sends, so the whole branch was dead:
            # vol_mult stayed pinned at 1.0 forever (it is the primary axis of
            # the DNA k-NN distance) and update_funding_metrics was never called,
            # leaving funding_bias and squeeze_risk -- features 16 and 17 of 19 --
            # permanently zero. Tickers arrive as a flat dict.
            raw_data = data.get("raw_data", data)
            if raw_data:
                if "turnover24h" in raw_data:
                    turnover = float(raw_data["turnover24h"])
                    if symbol not in self.screener_metrics:
                        self.screener_metrics[symbol] = {}
                    baseline = self.volatility_baseline.get(symbol, turnover)
                    if baseline > 0:
                        self.screener_metrics[symbol]["vol_mult"] = min(10.0, max(0.1, turnover / baseline))
                    self.volatility_baseline[symbol] = (baseline * 0.99) + (turnover * 0.01)

                if "fundingRate" in raw_data:
                    if symbol in self.stat_engines and hasattr(self.stat_engines[symbol], 'update_funding_metrics'):
                        self.stat_engines[symbol].update_funding_metrics(float(raw_data["fundingRate"]))
        except Exception as e:
            logger.debug(f"[X-RAY] Screener parse fault for {symbol}: {e}")

    def _unrealised_by_symbol(self) -> Dict[str, float]:
        """
        AUDIT B11: current unrealised PnL across every tracked position.

        Each lifecycle daemon publishes its own figure into its context on each
        tick, so this is a read of already-computed values rather than a
        recomputation.
        """
        out: Dict[str, float] = {}
        for sym, ctx in list(self.active_contexts.items()):
            try:
                out[sym] = float(ctx.get("unrealised_pnl", 0.0) or 0.0)
            except (TypeError, ValueError):
                out[sym] = 0.0
        return out

    async def _refresh_equity_snapshot(self) -> Optional[EquitySnapshot]:
        """
        AUDIT B11: capture wallet balance and equity together, from one response,
        so the two can never drift apart or be conflated by later callers.
        """
        try:
            payload = await self.executor.safe_call(
                "GET", "/v5/account/wallet-balance", accountType="UNIFIED"
            )
            snap = parse_wallet_response(payload, time.time())
            if snap is None:
                payload = await self.executor.safe_call(
                    "GET", "/v5/account/wallet-balance", accountType="CONTRACT"
                )
                snap = parse_wallet_response(payload, time.time())
            if snap is not None:
                self.equity_snapshot = snap
                self.global_state_cache["current_vault_balance"] = snap.equity
                self.global_state_cache["wallet_balance_realized"] = snap.wallet_balance
                METRICS.gauge("equity", snap.equity)
                METRICS.gauge("wallet_balance", snap.wallet_balance)
            if self.v12 is not None:
                self.v12.set_equity(snap.equity if snap is not None else None)   # unknown -> fail closed
            return snap
        except Exception as e:
            logger.warning(f"[X-RAY] Equity snapshot refresh failed: {e}")
            if self.v12 is not None:
                self.v12.set_equity(None)
            return None

    def _execution_budget_seconds(self) -> float:
        """
        AUDIT B10: the in-flight reservation must outlive the SLOWEST execution
        route. The old hardcoded 45s was shorter than a TWAP iceberg
        (up to 8 slices x (peg timeout + inter-slice interval)), so the
        reservation expired mid-execution and a second position could be opened
        on the same symbol.
        """
        max_slices = 8
        peg_timeout = 5.0
        max_interval = 8.0
        retry_budget = 10.0
        reconciliation_margin = 15.0
        computed = (max_slices * (peg_timeout + max_interval)) + retry_budget + reconciliation_margin
        return max(computed, self.config.execution_budget_sec)

    async def _try_reserve_entry(
        self, symbol: str, target_notional: float, sl_dist_pct: float, current_bal: float
    ) -> Tuple[bool, str]:
        """
        AUDIT B9: atomically verify capacity and reserve the slot.

        Everything that could change between checking and reserving is done
        under one lock: duplicate detection, slot capacity (counting in-flight
        reservations, which the vault ledger does not yet know about), portfolio
        heat, and the vault's own invariants.
        """
        async with self.reservation_lock:
            if symbol in self.active_positions_map or symbol in self.in_flight_symbols:
                return False, f"DUPLICATE_OR_IN_FLIGHT ({symbol})"

            # Slots must count in-flight reservations, not just filled positions.
            committed_slots = len(self.risk_vault.position_ledger) + len(self.in_flight_symbols)
            max_slots = self.risk_vault.get_max_allowed_slots()
            if committed_slots >= max_slots:
                return False, f"SLOT_CAP_REACHED ({committed_slots}/{max_slots} incl. in-flight)"

            # Portfolio heat must also include in-flight notional.
            vault_leverage_limit = float(self.live_params.get(
                "LEVERAGE_CAP", getattr(self.risk_vault, "max_leverage", 2.0)))
            max_heat = current_bal * max(1.0, vault_leverage_limit) * 0.95
            committed_notional = (
                sum(self.risk_vault.position_ledger.values())
                + sum(self.in_flight_notionals.values())
            )
            if committed_notional + target_notional > max_heat:
                return False, (
                    f"HEAT_CAP_EXCEEDED (committed ${committed_notional:.2f} + "
                    f"${target_notional:.2f} > ${max_heat:.2f})"
                )

            is_safe, reason = await self.risk_vault.evaluate_portfolio_safety(
                current_bal, target_notional, symbol, sl_dist_pct=sl_dist_pct
            )
            if not is_safe:
                return False, f"Risk Vault Veto ({reason})"

            ttl = self._execution_budget_seconds()
            self.in_flight_symbols[symbol] = time.time() + ttl
            self.in_flight_notionals[symbol] = target_notional

        self.state_actor.dispatch(symbol, "RESERVE_IN_FLIGHT", {"notional": target_notional})
        return True, "RESERVED"

    def _market_data_is_tradeable(self, ob_payload: dict, now: float) -> Tuple[bool, str]:
        """AUDIT B19/B20: delegate to the pure market-data quality contract."""
        return market_data_is_tradeable(ob_payload, now, self.config.max_orderbook_age_sec)

    async def _eval_gate(self, symbol: str, ob_payload: dict, stat_engine: ContinuousMicrostructureEngine, now: float, tick_count: int):
        async with self.circuit_breaker_lock:
            if self.circuit_breakers.get(symbol, 0.0) > now or self.circuit_breakers.get("GLOBAL_MAINTENANCE", 0.0) > now:
                return

        # AUDIT B8: new entries require a healthy AND capital-safe system.
        if not self.fsm.can_open_new_positions or self.fsm.is_asset_locked(symbol):
            return
        if not stat_engine or tick_count < 40:
            return

        is_active_fast = symbol in self.active_positions_map or symbol in self.in_flight_symbols
        if is_active_fast:
            if now - self.last_eval_time.get(symbol + "_learning_throttle", 0.0) < 1.0:
                return
            self.last_eval_time[symbol + "_learning_throttle"] = now

        in_flight_reserved = False
        try:
            price = ob_payload.get("micro_price", 0.0)
            if price <= 0.0:
                return

            # AUDIT B19/B20: fail closed on stale or depth-less market data.
            # Entry requires a book that is both FRESH and COMPLETE; the REST
            # BBO fallback is explicitly depth-less and cannot support sizing.
            fresh, stale_reason = self._market_data_is_tradeable(ob_payload, now)
            if not fresh:
                if now - self.last_eval_time.get(symbol + "_stale_veto", 0.0) > 60.0:
                    logger.warning(f"[RADAR] ENTRY_REJECTED_STALE_DATA // {symbol}: {stale_reason}")
                    self.last_eval_time[symbol + "_stale_veto"] = now
                METRICS.reason(ReasonCode.STALE_DATA)
                return

            log_mlofi_z = ob_payload.get("log_mlofi_z", 0.0)

            cluster_returns = await self._get_cluster_returns()
            sector_impulse, _ = await SectorEigenOracle.compute_sector_impulse(symbol, cluster_returns)

            feature_engine = self.feature_engines.get(symbol)
            atr = feature_engine.get_computed_atr() if feature_engine else (price * 0.005)

            sl_dist_pct = max((atr * self.live_params.get("sl_atr_mult", 2.5)) / (price + 1e-9), 0.010)
            dynamic_rr = feature_engine.get_dynamic_rr_ratio() if feature_engine else self.live_params.get("rr_ratio", 2.0)
            # APEX 2026-09: the exchange-native take-profit is placed HERE, at
            # entry. At 2R it closed trades before the exit engine's runner
            # policy (target 3R, trail from 1.5R) could act, recreating the
            # capped-winner profile from the live session. The floor keeps the
            # bracket consistent with EXIT_CONFIG.min_reward_r.
            if not EXIT_CONFIG.legacy:
                dynamic_rr = max(dynamic_rr, EXIT_CONFIG.min_reward_r)
            tp_dist_pct = sl_dist_pct * dynamic_rr

            sol_cluster = ["SOLUSDT", "JUPUSDT", "WIFUSDT", "PYTHUSDT", "RAYUSDT", "JTOUSDT", "BONKUSDT"]
            eth_cluster = ["ETHUSDT", "PEPEUSDT", "OPUSDT", "ARBUSDT", "LDOUSDT", "ENAUSDT", "LINKUSDT"]

            if symbol in sol_cluster:
                parent_flow = self.stream_feed_instance.log_mlofi_z.get("SOLUSDT", 0.0) if self.stream_feed_instance else 0.0
            elif symbol in eth_cluster:
                parent_flow = self.stream_feed_instance.log_mlofi_z.get("ETHUSDT", 0.0) if self.stream_feed_instance else 0.0
            else:
                parent_flow = self.stream_feed_instance.log_mlofi_z.get("BTCUSDT", 0.0) if self.stream_feed_instance else 0.0

            fusion_engine = self.entry_matrices.get(symbol)
            if fusion_engine and hasattr(fusion_engine, 'update_macro_flows'):
                fusion_engine.update_macro_flows(
                    asset_ofi_z=log_mlofi_z,
                    btc_ofi_z=parent_flow,
                    eth_ofi_z=parent_flow
                )

            extract_args = {
                "current_price": price, "log_mlofi_z": log_mlofi_z,
                "hawkes_z": getattr(stat_engine, 'marked_hawkes_z', 0.0),
                "sector_impulse": sector_impulse, "sl_dist_pct": sl_dist_pct, "tp_dist_pct": tp_dist_pct,
                "exchange_timestamp": ob_payload.get("timestamp", int(now * 1000)),
                "parent_mlofi_z": parent_flow
            }

            state = stat_engine.extract_statistical_state(**extract_args)

            if is_active_fast:
                if symbol in self.active_contexts:
                    self.active_contexts[symbol]["latest_state"] = state
                return

            prob_success = max(state["p_up"], state["p_down"])
            action = state["action_dir"]
            dynamic_gate = state.get("dynamic_gate", 0.52)
            dominant_regime = state.get("dominant_regime", "TRENDING")

            METRICS.incr("signals_generated")
            if action == "HOLD" or prob_success < dynamic_gate:
                METRICS.reason(ReasonCode.GATE_PROBABILITY)
                if now - self.last_eval_time.get(symbol + "_gate_diag", 0.0) > 120.0:
                    logger.info(f"[RADAR] {symbol} Filtered: Action={action} | Prob {prob_success:.1%} < Gate {dynamic_gate:.1%}")
                    self.last_eval_time[symbol + "_gate_diag"] = now
                return

            # Momentum Tape Filter
            if feature_engine:
                mom_matrix = feature_engine.extract_multi_timeframe_momentum()
                m_15m = mom_matrix.get("momentum_15", 0.0)
                if action == "SELL" and m_15m > 0.025:
                    if now - self.last_eval_time.get(symbol + "_mom_veto", 0.0) > 60.0:
                        logger.info(f"[RADAR] {symbol} SELL Vetoed: Proactive block against strong 15m pump ({m_15m:.1%}).")
                        self.last_eval_time[symbol + "_mom_veto"] = now
                    return
                if action == "BUY" and m_15m < -0.025:
                    if now - self.last_eval_time.get(symbol + "_mom_veto", 0.0) > 60.0:
                        logger.info(f"[RADAR] {symbol} BUY Vetoed: Proactive block against strong 15m dump ({m_15m:.1%}).")
                        self.last_eval_time[symbol + "_mom_veto"] = now
                    return

            # Anti-Whipsaw Directional Lockout
            last_exit = self.last_exit_direction.get(symbol)
            if last_exit and len(last_exit) >= 3:
                last_dir, last_time, last_outcome = last_exit
                if last_outcome == "LOSS" and (now - last_time) < 180.0 and action != last_dir:
                    if now - self.last_eval_time.get(symbol + "_whipsaw_veto", 0.0) > 60.0:
                        logger.info(
                            f"[RADAR] {symbol} {action} Vetoed: Anti-Whipsaw lockout active "
                            f"(Flipped from {last_dir} loss {now - last_time:.1f}s ago)."
                        )
                        self.last_eval_time[symbol + "_whipsaw_veto"] = now
                    return

            # AUDIT B14: the Directional Drift Gate was mathematically unreachable
            # and has been removed. `expected_drift` is the gated logit sum, and
            # action == "BUY" implies p_up > 0.5 implies logit > 0 implies
            # raw_score > 0 -- so "BUY with drift < -0.0005" could never hold
            # (symmetrically for SELL). It filtered nothing while appearing to.
            # Retained as telemetry only.
            expected_drift = float(state.get("expected_drift", 0.0))

            # Spread-to-ATR Friction Sieve
            # AUDIT B14: the payload now carries a real spread. Previously the
            # missing key defaulted to a literal 0.0001 in PRICE units, making
            # spread_bps ~1e-6 for BTC so this sieve never fired. A payload
            # without a spread is treated as unusable rather than as zero-cost.
            if "spread_bps" in ob_payload:
                spread_bps = float(ob_payload["spread_bps"])
                spread = float(ob_payload.get("spread", (spread_bps / 10000.0) * price))
            elif "spread" in ob_payload:
                spread = float(ob_payload["spread"])
                spread_bps = (spread / (price + 1e-9)) * 10000.0
            else:
                if now - self.last_eval_time.get(symbol + "_no_spread", 0.0) > 120.0:
                    logger.warning(f"[RADAR] ENTRY_REJECTED_STALE_DATA // {symbol}: snapshot carries no spread.")
                    self.last_eval_time[symbol + "_no_spread"] = now
                METRICS.reason(ReasonCode.NO_SPREAD)
                return
            atr_bps = (atr / (price + 1e-9)) * 10000.0
            if spread_bps > 8.0 or (atr_bps > 0 and (spread_bps / atr_bps) > 0.35):
                if now - self.last_eval_time.get(symbol + "_friction_veto", 0.0) > 120.0:
                    logger.info(f"[RADAR] {symbol} Filtered: Friction ratio excessive (Spread: {spread_bps:.1f} bps, ATR: {atr_bps:.1f} bps).")
                    self.last_eval_time[symbol + "_friction_veto"] = now
                return

            # Macro Trend Direction Filter
            if action == "SELL" and (parent_flow > 0.40 or sector_impulse > 0.15):
                if now - self.last_eval_time.get(symbol + "_macro_short_veto", 0.0) > 60.0:
                    logger.info(
                        f"[RADAR] {symbol} SELL Vetoed: Positive Macro Tailwinds "
                        f"(Parent OFI: {parent_flow:+.2f}, Sector: {sector_impulse:+.2f})"
                    )
                    self.last_eval_time[symbol + "_macro_short_veto"] = now
                return

            # Cold-Start Resilient DNA Filter
            dna_stats = self.ram_dna_cache.get(symbol, {"is_armed": True, "matched_samples": 0})
            is_armed = dna_stats.get("is_armed", True)
            if not is_armed:
                if now - self.last_eval_time.get(symbol + "_dna_diag", 0.0) > 120.0:
                    logger.info(f"[RADAR] {symbol} Filtered: DNA Quarantined in Shadow Ledger.")
                    self.last_eval_time[symbol + "_dna_diag"] = now
                else:
                    return
                METRICS.reason(ReasonCode.DNA_QUARANTINE)
                
                shadow_sig_id = str(uuid.uuid4())
                shadow_features = {
                    "symbol": symbol, 
                    "market_regime": dominant_regime,
                    "virtual_sl": price * (1.0 - sl_dist_pct) if action == "BUY" else price * (1.0 + sl_dist_pct),
                    "virtual_tp": price * (1.0 + tp_dist_pct) if action == "BUY" else price * (1.0 - tp_dist_pct),
                    "log_mlofi_z": log_mlofi_z, 
                    "vol_mult": self.screener_metrics.get(symbol, {}).get("vol_mult", 1.0),
                    "spread": spread, 
                    "execution_mode": "SHADOW",
                    "hawkes_z": getattr(stat_engine, 'marked_hawkes_z', 0.0),
                    "sector_impulse": sector_impulse, 
                    "bocd_cp_prob": state.get("bocd_cp_prob", 0.0),
                    "alpha_tensor_bps": state.get("alpha_tensor_bps", 0.0),
                    "expected_drift": expected_drift,
                    "topology": state.get("topology", "LAMINAR FLOW"),
                    "markov_beliefs": state.get("markov_beliefs", {})
                }
                if self.memory:
                    self.track_task(self.memory.commit_prediction(
                        shadow_sig_id, now, price, action, prob_success, shadow_features, is_shadow=True
                    ))
                return

            if not fusion_engine:
                return

            bids = ob_payload.get("bids", [])
            asks = ob_payload.get("asks", [])
            if not bids or not asks:
                return

            fusion_verdict = fusion_engine.fuse_signal_probability(symbol, prob_success, action, bids, asks)
            exec_weight = fusion_verdict["execution_weight"]

            current_bal = self.global_state_cache.get("current_vault_balance", 0.0)
            
            # Capital Floor Hard-Stop
            if current_bal < MIN_REQUIRED_EQUITY:
                if now - self.last_eval_time.get(symbol + "_equity_veto", 0.0) > 120.0:
                    logger.warning(f"[RISK] Account equity (${current_bal:.2f}) below minimum floor (${MIN_REQUIRED_EQUITY:.2f}). Trade halted.")
                    self.last_eval_time[symbol + "_equity_veto"] = now
                return

            # Proportional Merton-Kelly Sizing
            max_single_risk = float(getattr(self.risk_vault, "max_single_position_risk_pct", 0.025))
            kelly_f = state.get("kelly_fraction", 0.005)
            base_risk = (kelly_f / 0.010) * max_single_risk if kelly_f > 0 else (max_single_risk * 0.6)
            target_risk_pct = float(np.clip(base_risk * exec_weight, 0.002, max_single_risk))

            slippage_gap_buffer = max(0.0020, getattr(stat_engine, 'rough_vol', 0.001) * 1.5)
            total_risk_dist_pct = sl_dist_pct + slippage_gap_buffer

            dollar_risk_budget = current_bal * target_risk_pct
            raw_notional = dollar_risk_budget / total_risk_dist_pct

            corr_haircut = self.risk_vault.calculate_correlation_haircut(symbol)
            
            # Single-Ticket Cap (Max 25% account balance)
            max_ticket_ceiling = max(6.50, current_bal * 0.25)
            target_notional = min(raw_notional * corr_haircut, max_ticket_ceiling)

            # Risk Guard
            max_allowed_risk_dollars = current_bal * max_single_risk
            projected_tail_risk = target_notional * total_risk_dist_pct
            if projected_tail_risk > max_allowed_risk_dollars and projected_tail_risk > 0:
                scale_ratio = max_allowed_risk_dollars / projected_tail_risk
                target_notional = max(6.50, target_notional * scale_ratio)

            # Portfolio Heat Ceiling
            vault_leverage_limit = float(self.live_params.get("LEVERAGE_CAP", getattr(self.risk_vault, "max_leverage", 2.0)))
            safe_leverage_headroom = max(1.0, vault_leverage_limit) * 0.95
            max_portfolio_heat = current_bal * safe_leverage_headroom

            active_notional_sum = sum(self.risk_vault.position_ledger.values()) if hasattr(self.risk_vault, "position_ledger") else 0.0
            in_flight_notional_sum = sum(self.in_flight_notionals.values())
            remaining_notional_capacity = max(0.0, max_portfolio_heat - (active_notional_sum + in_flight_notional_sum))

            if remaining_notional_capacity < 15.00:
                if now - self.last_eval_time.get(symbol + "_heat_deadlock", 0.0) > 60.0:
                    logger.info(
                        f"[RADAR] {symbol} Filtered: Portfolio Headroom Exhausted "
                        f"(Active: ${active_notional_sum:.2f} + Flight: ${in_flight_notional_sum:.2f} >= Cap: ${max_portfolio_heat:.2f})"
                    )
                    self.last_eval_time[symbol + "_heat_deadlock"] = now
                return

            target_notional = float(np.clip(target_notional, 15.00, remaining_notional_capacity))

            # AUDIT B9/B10: capacity check and reservation are now ONE atomic
            # step. Previously the vault check ran outside the lock, so several
            # symbols evaluating concurrently could all pass against the same
            # stale snapshot and reserve -- making max_slots and the leverage
            # ceiling advisory rather than enforced.
            # ---- V12: expected value after costs + kill-switch hierarchy ----
            v12_verdict = None
            if self.v12_mode == "enforce" and self.v12 is None:
                METRICS.reason(ReasonCode.V12_UNAVAILABLE)
                return
            if self.v12 is not None:
                funding_8h = _finite_float(ob_payload.get("funding_rate"), 0.0)
                v12_verdict = await asyncio.to_thread(
                    self.v12.evaluate, symbol, action, now, current_bal, spread_bps, funding_8h, 6.50)
                if not v12_verdict.would_allow:
                    METRICS.reason(ReasonCode.V12_GUARDIAN if v12_verdict.decision.is_trade
                                   else ReasonCode.V12_NO_TRADE)
                    if now - self.last_eval_time.get(symbol + "_v12_veto", 0.0) > 60.0:
                        self.last_eval_time[symbol + "_v12_veto"] = now
                        logger.info(f"[V12] {symbol} {action} "
                                    f"{'BLOCKED' if self.v12_mode == 'enforce' else 'would block'}: "
                                    f"{v12_verdict.reason}")
                if not v12_verdict.allowed:
                    return
                if self.v12_mode == "enforce":
                    # V12 owns the stop, the target and the size. The size can only
                    # go DOWN from the legacy figure, never up.
                    sl_dist_pct = v12_verdict.stop_pct
                    dynamic_rr = v12_verdict.tp_r
                    tp_dist_pct = sl_dist_pct * dynamic_rr
                    total_risk_dist_pct = sl_dist_pct + slippage_gap_buffer
                    target_notional = min(target_notional, v12_verdict.notional)

            reserved_ok, risk_reason = await self._try_reserve_entry(
                symbol=symbol,
                target_notional=target_notional,
                sl_dist_pct=total_risk_dist_pct,
                current_bal=current_bal,
            )
            if not reserved_ok:
                if now - self.last_eval_time.get(symbol + "_vault_diag", 0.0) > 60.0:
                    logger.info(f"[RADAR] {symbol} Filtered: {risk_reason}")
                    self.last_eval_time[symbol + "_vault_diag"] = now
                METRICS.reason(ReasonCode.SLOT_CAP if "SLOT_CAP" in risk_reason
                               else ReasonCode.HEAT_CAP if "HEAT_CAP" in risk_reason
                               else ReasonCode.DUPLICATE if "DUPLICATE" in risk_reason
                               else ReasonCode.DRAWDOWN)
                return
            in_flight_reserved = True

            logger.critical(
                f"  ALPHA SIGNAL // {symbol} {action} | Regime: {dominant_regime} | "
                f"Prob: {prob_success:.2%} | Weight: {exec_weight:.2f}x | Haircut: {corr_haircut:.2f}x | Size: ${target_notional:.2f}"
            )

            try:
                await self.executor.adjust_leverage(symbol, int(vault_leverage_limit))
            except Exception as e:
                logger.warning(f"[X-RAY] Leverage adjustment bypassed for {symbol}: {e}")

            arrival_price = price
            gate = self.edge_gate.decide(prob_success - 0.5)
            METRICS.gauge("edge_gate_would_block", float(self.edge_gate.would_block_count))
            if not gate.allowed:
                METRICS.reason(ReasonCode.EDGE_GATE)
                logger.info(f"[EDGE_GATE] {symbol} {action} {gate.reason}")
                self.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})
                self.in_flight_symbols.pop(symbol, None)
                self.in_flight_notionals.pop(symbol, None)
                in_flight_reserved = False
                return
            if gate.would_block:
                METRICS.reason(ReasonCode.EDGE_GATE_SHADOW)
                logger.info(f"[EDGE_GATE] {symbol} {action} {gate.reason}")

            sl_price = price * (1.0 - sl_dist_pct) if action == "BUY" else price * (1.0 + sl_dist_pct)
            tp_price = price * (1.0 + tp_dist_pct) if action == "BUY" else price * (1.0 - tp_dist_pct)

            success, avg_fill_price, actual_qty_filled = await self.sor.execute_alpha_signal(
                symbol=symbol, direction=action, prob_success=prob_success, exec_weight=exec_weight,
                current_mid_price=price, sl_price=sl_price, tp_price=tp_price,
                inst_var=stat_engine.inst_variance, depth_snapshot=ob_payload,
                target_notional=target_notional, regime=dominant_regime
            )

            METRICS.incr("orders_submitted")
            if not success or actual_qty_filled <= 0:
                METRICS.incr("orders_rejected")
                self.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})
                self.in_flight_symbols.pop(symbol, None)
                self.in_flight_notionals.pop(symbol, None)
                in_flight_reserved = False
                return

            METRICS.incr("signals_executed")
            METRICS.incr("entries")
            sig_id = str(uuid.uuid4())
            self._entry_conviction[sig_id] = prob_success - 0.5
            if self.v12 is not None and v12_verdict is not None:
                try:
                    self.v12.on_fill(sig_id, v12_verdict, time.time(), avg_fill_price, actual_qty_filled)
                except Exception as e:
                    logger.warning(f"[V12] journal open failed for {symbol}: {e}")

            if stat_engine and hasattr(stat_engine, 'pending_trade_outcomes'):
                stat_engine.pending_trade_outcomes[sig_id] = {
                    "action": action, "features": state.get("raw_features", np.zeros(25)), "p_up": state.get("p_up", 0.5),
                    "notional": target_notional,
                    "beliefs": [
                        state.get("markov_beliefs", {}).get("trend", 0.25),
                        state.get("markov_beliefs", {}).get("range", 0.25),
                        state.get("markov_beliefs", {}).get("disloc", 0.25),
                        state.get("markov_beliefs", {}).get("cascade", 0.25)
                    ]
                }

            safe_features = {
                "symbol": symbol, 
                "market_regime": dominant_regime,
                "virtual_sl": sl_price, 
                "virtual_tp": tp_price, 
                "log_mlofi_z": log_mlofi_z, 
                "hawkes_z": getattr(stat_engine, 'marked_hawkes_z', 0.0),
                "sector_impulse": sector_impulse, 
                "bid_ask_spread": spread,
                "alpha_tensor_bps": state.get("alpha_tensor_bps", 0.0),
                "expected_drift": expected_drift,
                "topology": state.get("topology", "LAMINAR FLOW"),
                "markov_beliefs": state.get("markov_beliefs", {}),
                "bocd_cp_prob": state.get("bocd_cp_prob", 0.0),
                "target_notional": target_notional
            }

            if self.memory:
                await self.memory.commit_prediction(sig_id, time.time(), price, action, prob_success, safe_features, False)

            # APEX 2026-09: the ticket used to pass edge_bps=0.0 (hard-coded) and
            # labelled target_notional/equity as "risk". It now reports the edge
            # the model's probability is actually worth over its own 60-second
            # horizon, the round-trip cost it has to beat, the FILLED exposure,
            # and the true risk to the stop.
            atr_pct_now = (atr / price) if price > 0 else 0.0
            p_up_now = float(state.get("p_up", 0.5))
            real_edge = horizon_edge_bps(p_up_now, action == "BUY", atr_pct_now,
                                         horizon_sec=float(os.getenv("MICRO_HORIZON_SEC", "60")),
                                         bar_minutes=float(self.bar_series_interval or 5))
            cost_bps = (2.0 * getattr(self.sor, "taker_fee_rate", 0.00055) + 0.0004) * 1e4
            ticket_msg = self.telegram.format_entry_ticket(
                symbol, action, avg_fill_price, actual_qty_filled, real_edge,
                (target_notional / current_bal) if current_bal else 0.0, dominant_regime, safe_features,
                equity=current_bal, cost_bps=cost_bps, gate_note=gate.reason,
            )
            if v12_verdict is not None:
                import html as _html
                ticket_msg += "\n<pre>" + _html.escape(v12_verdict.card()) + "</pre>"
            self.track_task(self._safe_telegram_dispatch(ticket_msg, is_html=True))

            specs = self.sor.instrument_cache.get(symbol, {})
            qty_step_str = str(specs.get("qty_step", Decimal("0.1")))

            self.daemon_tasks[symbol] = self.track_task(self._position_lifecycle_daemon(
                symbol, sig_id, action, avg_fill_price, atr,
                {"allocated_value_usdt": target_notional, "size": actual_qty_filled, "arrival_price": arrival_price, "qty_step": qty_step_str,
                 "v12_stop_pct": v12_verdict.stop_pct if (v12_verdict is not None and self.v12_mode == "enforce") else None},
                int(vault_leverage_limit), dominant_regime, realigned_tp=tp_price, dynamic_rr_ratio=dynamic_rr, realigned_sl=sl_price
            ), is_critical=True)

        except Exception as e:
            logger.error(f"[X-RAY] Trade evaluation fault for {symbol}: {e}", exc_info=True)
            self.fsm.record_module_error("_eval_gate")
            if in_flight_reserved:
                self.state_actor.dispatch(symbol, "RELEASE_IN_FLIGHT", {})
                self.in_flight_symbols.pop(symbol, None)
                self.in_flight_notionals.pop(symbol, None)

    async def run_dna_prewarmer(self):
        logger.info("  RAM PRE-WARMER ONLINE.")
        while True:
            try:
                await asyncio.wait_for(self.force_dna_refresh.wait(), timeout=300.0)
                self.force_dna_refresh.clear()
            except asyncio.TimeoutError:
                pass

            try:
                async def _safe_fetch(sym, dna):
                    try:
                        if not self.memory:
                            return {"is_armed": True, "win_rate": 0.50}
                        async with self.db_semaphore:
                            return await asyncio.wait_for(self.memory.compute_latent_dna_edge(dna, 30), timeout=2.0)
                    except Exception:
                        return {"is_armed": True, "win_rate": 0.50}

                fetch_tasks = {}
                for sym in list(self.asset_basket):
                    engine = self.stat_engines.get(sym)
                    fetch_tasks[sym] = _safe_fetch(
                        sym,
                        {"vol_mult": self.screener_metrics.get(sym, {}).get("vol_mult", 1.0),
                         "log_mlofi_z": engine.clean_ofi_z if engine else 0.0,
                         "spread_pct": 0.001, "symbol": sym}
                    )

                if not fetch_tasks:
                    continue
                results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
                for sym, result in zip(list(fetch_tasks.keys()), results):
                    if isinstance(result, Exception):
                        self.ram_dna_cache[sym] = {"is_armed": True, "win_rate": 0.50}
                    else:
                        self.ram_dna_cache[sym] = result
            except Exception as e:
                logger.error(f"[X-RAY] DNA Prewarmer error: {e}")

    async def run_shadow_resolution_daemon(self):
        logger.info("  GHOST FORENSICS ONLINE.")
        interval_mins = float(self.timeframe)
        while True:
            await asyncio.sleep(300)
            try:
                active_syms = list(self.active_positions_map.keys())
                current_prices = {
                    sym: {
                        "prices": list(self.screener_memory[sym]["prices"]),
                        "highs": list(self.screener_memory[sym].get("highs", [])),
                        "lows": list(self.screener_memory[sym].get("lows", []))
                    }
                    for sym in self.asset_basket + self.shadow_basket
                    if self.screener_memory.get(sym) and self.screener_memory[sym].get("prices") and sym not in active_syms
                }
                if current_prices and self.memory:
                    async with self.db_semaphore:
                        try:
                            await asyncio.wait_for(
                                self.memory.resolve_batch_historical_predictions(
                                    list(current_prices.keys()), current_prices, 60.0, interval_mins
                                ),
                                timeout=15.0
                            )
                        except Exception as e:
                            logger.debug(f"[X-RAY] Shadow resolution timeout: {e}")
            except Exception as e:
                logger.error(f"[X-RAY] Shadow resolution error: {e}")

    async def run_omni_swarm_director(self):
        logger.info("  60s OMNI-SWARM DIRECTOR ONLINE.")
        while True:
            await asyncio.sleep(60)
            try:
                protected_symbols = set(self.active_positions_map.keys()) | set(self.in_flight_symbols.keys())
                dead_sym, hot_sym = await self.omni_scanner.scan_and_rank_universe(self.asset_basket, protected_symbols=protected_symbols)

                if dead_sym and hot_sym and not any(b in hot_sym for b in BANNED_ASSET_KEYWORDS):
                    tick_res = await self.executor.safe_call("GET", "/v5/market/tickers", category="linear", symbol=hot_sym)
                    if tick_res.get("retCode") == 0 and tick_res.get("result", {}).get("list"):
                        t_data = tick_res["result"]["list"][0]
                        bid = float(t_data.get("bid1Price", 0.0) or 0.0)
                        ask = float(t_data.get("ask1Price", 0.0) or 0.0)
                        turnover = float(t_data.get("turnover24h", 0.0) or 0.0)
                        spread_bps = ((ask - bid) / (bid + 1e-9)) * 10000.0 if bid > 0 else 999.0

                        if bid > 0 and ask > bid and turnover >= 15_000_000.0 and spread_bps <= 8.0:
                            if dead_sym in self.asset_basket:
                                self.asset_basket.remove(dead_sym)
                            if hot_sym not in self.asset_basket:
                                self.asset_basket.append(hot_sym)
                            self._initialize_symbol_structures([hot_sym])
                            await self._prune_dead_symbols()
                            if self.stream_feed_instance and hasattr(self.stream_feed_instance, 'hot_swap_socket_stream'):
                                await self.stream_feed_instance.hot_swap_socket_stream(dead_sym, hot_sym)
                            logger.critical(f"[X-RAY] DYNAMIC SWAP // {hot_sym} injected into matrix (Replaced {dead_sym}).")
            except Exception as e:
                logger.error(f"[X-RAY] Omni-Swarm Director error: {e}")

    async def run_universe_refresher(self):
        try:
            logger.info("  MATRIX REFRESH: Scanning High-Velocity Universe...")
            await self.sor._fetch_exchange_limits("BTCUSDT")

            dynamic_basket = await self.executor.get_top_volatile_assets(limit=40, min_turnover=15_000_000.0)
            if not dynamic_basket or len(dynamic_basket) < 10:
                dynamic_basket = await self.executor.get_top_volatile_assets(limit=40, min_turnover=5_000_000.0)

            # AUDIT B26: exact base-asset matching, one central ban list.
            dynamic_basket = [s for s in dynamic_basket if not is_banned_symbol(s)]

            current_bal = self.global_state_cache.get("current_vault_balance", MIN_REQUIRED_EQUITY)
            if current_bal < 250.0:
                active_limit = 4
                shadow_limit = 4
                logger.info(f"  MICRO-CAPITAL DETECTED (${current_bal:.2f} < $250). Clamping active basket to {active_limit} pairs.")
            else:
                active_limit = 12
                shadow_limit = 8

            # AUDIT B3: the basket used to be swapped WITHOUT touching the
            # WebSocket subscriptions, so the engine went blind to its own
            # universe until an unrelated stream restart. Diff and resubscribe.
            previous_subscribed = set(self.asset_basket) | set(self.shadow_basket)

            pinned_syms = [s for s in list(self.active_positions_map.keys()) + list(self.in_flight_symbols.keys()) if s]
            dynamic_basket = list(dict.fromkeys(pinned_syms + dynamic_basket))
            self.asset_basket = dynamic_basket[:max(active_limit, len(pinned_syms))]
            self.shadow_basket = dynamic_basket[active_limit:active_limit + shadow_limit]
            new_subscribed = set(self.asset_basket) | set(self.shadow_basket)

            # Never unsubscribe a symbol we still hold or are executing into.
            protected = set(self.active_positions_map.keys()) | set(self.in_flight_symbols.keys())
            to_add = sorted(new_subscribed - previous_subscribed)
            to_drop = sorted((previous_subscribed - new_subscribed) - protected)

            self._initialize_symbol_structures(self.asset_basket + self.shadow_basket)
            await self._resubscribe_universe(to_add, to_drop)
            await self._prune_dead_symbols()
            self.force_dna_refresh.set()

            logger.info(
                f"  MATRIX REFRESHED: {len(self.asset_basket)} Live | {len(self.shadow_basket)} Shadow "
                f"| +{len(to_add)} -{len(to_drop)} subscriptions"
            )
            if to_add:
                self.track_task(self._verify_new_symbol_data(to_add))
        except Exception as e:
            logger.error(f"[X-RAY] Universe refresher error: {e}")

    async def _resubscribe_universe(self, to_add: List[str], to_drop: List[str]):
        """
        AUDIT B3: bring the WebSocket subscriptions in line with the basket.

        `hot_swap_socket_stream` swaps one symbol for another, so pairs are
        swapped together and any remainder is added or dropped on its own. If the
        socket is down there is nothing to resubscribe -- request a restart so
        the new basket is picked up on reconnect rather than silently ignored.
        """
        if not to_add and not to_drop:
            return

        feed = self.stream_feed_instance
        if not feed or not getattr(feed, "active_ws", None) or feed.active_ws.closed:
            logger.warning(
                "[X-RAY] UNIVERSE CHANGED while stream is down. Requesting restart "
                f"so {len(to_add)} new symbol(s) are subscribed on reconnect."
            )
            self.stream_restart_event.set()
            return

        try:
            for i in range(max(len(to_add), len(to_drop))):
                add_sym = to_add[i] if i < len(to_add) else None
                drop_sym = to_drop[i] if i < len(to_drop) else None

                if add_sym and drop_sym:
                    await feed.hot_swap_socket_stream(drop_sym, add_sym)
                elif add_sym:
                    await feed.subscribe_symbol(add_sym)
                elif drop_sym:
                    await feed.unsubscribe_symbol(drop_sym)

            self.subscription_state = set(self.asset_basket) | set(self.shadow_basket)
            logger.info(f"[X-RAY] SUBSCRIPTIONS SYNCED // +{len(to_add)} -{len(to_drop)}")
        except Exception as e:
            logger.error(
                f"[X-RAY] Resubscription failed ({e}). Forcing stream restart to avoid blindness."
            )
            self.stream_restart_event.set()

    async def _verify_new_symbol_data(self, symbols: List[str], timeout: float = 45.0):
        """
        AUDIT B3: prove the four-step chain actually completed --
        universe change -> subscription change -> incoming data -> evaluable.

        A subscription that is accepted but delivers nothing is the failure this
        guards against, so silence is escalated loudly rather than assumed fine.
        """
        deadline = time.time() + timeout
        pending = set(symbols)
        while time.time() < deadline and pending:
            await asyncio.sleep(2.0)
            for sym in list(pending):
                snap = self.orderbook_snapshots.get(sym) or {}
                if snap.get("as_of"):
                    pending.discard(sym)

        if pending:
            logger.critical(
                f"[X-RAY] 🛑 SUBSCRIPTION BLIND SPOT // no market data for "
                f"{sorted(pending)} within {timeout:.0f}s of subscribing. "
                f"Forcing stream restart."
            )
            self.stream_restart_event.set()
        else:
            logger.info(f"[X-RAY] Data confirmed flowing for {len(symbols)} newly added symbol(s).")

    async def _universe_refresher_loop(self):
        while True:
            await asyncio.sleep(900)
            await self.run_universe_refresher()

    async def stream_manager_loop(self):
        while True:
            stream_feed = MarketStateMatrix(
                basket=list(dict.fromkeys(self.asset_basket + self.shadow_basket[:4] + list(self.active_positions_map.keys()))),
                intervals=self.kline_intervals,
                orderbook_callback=self.handle_incoming_orderbook_tick,
                screener_callback=self.handle_incoming_basket_screener_update,
                kline_callback=self.handle_incoming_kline_update,
                trade_callback=self.handle_incoming_trade,
                engine_reference=self
            )
            self.stream_feed_instance = stream_feed
            stream_task = asyncio.create_task(stream_feed.initialize_multiplexed_stream())

            def _on_stream_done(t):
                if not t.cancelled() and not self.stream_restart_event.is_set():
                    self.stream_restart_event.set()

            stream_task.add_done_callback(_on_stream_done)
            await self.stream_restart_event.wait()
            stream_task.cancel()
            stream_feed.terminate_all_feeds()
            self.stream_restart_event.clear()
            self.last_socket_reconnect = time.time()
            await asyncio.sleep(2)

    async def _fast_verify_entry_fill(self, symbol: str, expected_qty: float) -> Tuple[bool, float, float]:
        try:
            pos_res = await self.executor.safe_call("GET", "/v5/position/list", category="linear", symbol=symbol)
            pos_data = pos_res.get("result", {}).get("list", [])
            if pos_data and float(pos_data[0].get("size", 0.0)) > 0:
                avg_price = float(pos_data[0].get("avgPrice", 0.0))
                size = float(pos_data[0].get("size", expected_qty))
                return True, avg_price, size
        except Exception as e:
            logger.debug(f"[X-RAY] Fast position verify bypass: {e}")
        return False, 0.0, 0.0

    async def _reconstruct_pnl_from_fills(self, symbol: str, ctx: dict):
        """
        AUDIT B12: fallback settlement evidence from execution records.

        Returns (net_pnl, fees, exit_price) or None when the fills are
        insufficient. None means UNKNOWN -- it must never be coerced to zero.
        """
        try:
            res = await self.executor.safe_call(
                "GET", "/v5/execution/list", category="linear", symbol=symbol, limit=50
            )
            if not isinstance(res, dict) or res.get("retCode") != 0:
                return None

            rows = res.get("result", {}).get("list", [])
            if not rows:
                return None

            opened_at_ms = float(ctx.get("daemon_start_time", 0.0)) * 1000.0
            entry = float(ctx.get("actual_entry", 0.0))
            is_buy = bool(ctx.get("is_buy", True))
            closing_side = "Sell" if is_buy else "Buy"

            qty_sum = 0.0
            value_sum = 0.0
            fee_sum = 0.0
            opening_fee_sum = 0.0
            # APEX 2026-09: fills are filtered from slightly BEFORE the monitor
            # started, because the entry fill precedes it. The old filter kept
            # only closing-side fills, so the opening fee was never charged:
            # each of the 5 live trades settled this way recorded exactly one
            # taker leg, overstating net PnL by the other (one LINK "win" was a
            # loss). Opening-side fees are now included; opening-side quantity
            # is not, since it does not close anything.
            window_start_ms = opened_at_ms - 120_000.0
            for r in rows:
                try:
                    if float(r.get("execTime", 0.0)) < window_start_ms:
                        continue
                    side = str(r.get("side", ""))
                    fee = float(r.get("execFee", 0.0) or 0.0)
                    if side != closing_side:
                        if float(r.get("execTime", 0.0)) <= opened_at_ms + 5_000.0:
                            opening_fee_sum += fee
                        continue
                    if float(r.get("execTime", 0.0)) < opened_at_ms:
                        continue
                    q = float(r.get("execQty", 0.0) or 0.0)
                    px = float(r.get("execPrice", 0.0) or 0.0)
                    if q <= 0.0 or px <= 0.0:
                        continue
                    qty_sum += q
                    value_sum += q * px
                    fee_sum += fee
                except (TypeError, ValueError):
                    continue
            fee_sum += opening_fee_sum

            if qty_sum <= 0.0 or entry <= 0.0:
                return None

            avg_exit = value_sum / qty_sum
            gross = (avg_exit - entry) * qty_sum if is_buy else (entry - avg_exit) * qty_sum
            return gross - fee_sum, fee_sum, avg_exit
        except Exception as e:
            logger.debug(f"[X-RAY] Fill-based PnL reconstruction failed for {symbol}: {e}")
            return None

    async def _state_settle_trade(self, ctx: dict):
        """Shielded post-trade accounting, reconciliation sweep, and true equity update."""
        async def _settle():
            symbol, actual_entry = ctx["symbol"], ctx["actual_entry"]

            # 1. Sweep any residual size on exchange
            for _ in range(4):
                pos_res = await self.executor.safe_call("GET", "/v5/position/list", category="linear", symbol=symbol)
                pos_list = pos_res.get("result", {}).get("list", [])
                if not pos_list or float(pos_list[0].get("size", 0.0)) <= 0:
                    break

                remaining_qty = float(pos_list[0].get("size", 0.0))
                side = "Sell" if pos_list[0]["side"] == "Buy" else "Buy"
                qty_str = self.sor._format_qty_str(remaining_qty, symbol)

                await self.executor.safe_call(
                    "POST", "/v5/order/create", is_execution=True,
                    category="linear", symbol=symbol, side=side,
                    orderType="Market", qty=qty_str, timeInForce="IOC", reduceOnly=True,
                    positionIdx=self.sor.position_idx,
                    smpType="CancelMaker"
                )
                await asyncio.sleep(0.5)

            # 2. Fetch realized PnL record from exchange
            # AUDIT B12: an unresolved settlement must never masquerade as a
            # zero-PnL scratch. Previously a failed poll left net_pnl at 0.0 with
            # outcome "RECONCILED", which then fed the Kelly sizer, the RLS
            # labels, the win-rate metric and the DNA ledger as a LOSS.
            net_pnl, real_outcome, slippage_bps, fees, exit_price = 0.0, "UNKNOWN", 0.0, 0.0, actual_entry
            settlement_resolved = False

            for _ in range(5):
                await asyncio.sleep(1.0)
                try:
                    closed_data = await self.executor.safe_call("GET", "/v5/position/closed-pnl", category="linear", symbol=symbol, limit=5)
                    closed_list = closed_data.get("result", {}).get("list", [])

                    valid_close = None
                    for pnl_record in closed_list:
                        if (time.time() - float(pnl_record.get("updatedTime", 0)) / 1000.0) < 180.0:
                            valid_close = pnl_record
                            break

                    if valid_close:
                        net_pnl = float(valid_close.get("closedPnl", 0.0))
                        real_outcome = "PROFIT" if net_pnl > 0 else "LOSS"
                        # APEX 2026-09: the closed-pnl response has NO `execFee`
                        # field -- it carries `openFee` and `closeFee` (Bybit V5
                        # docs). Reading `execFee` returned 0.0 on every trade, so
                        # 22 of 27 live receipts showed zero fees and a "gross"
                        # equal to net. `closedPnl` itself is already net of both
                        # fees and funding (Bybit P&L docs), so net_pnl was right;
                        # the fee figure and everything derived from it were not.
                        fees = (_finite_float(valid_close.get("openFee"))
                                + _finite_float(valid_close.get("closeFee")))
                        exit_price = float(valid_close.get("avgExitPrice", actual_entry))
                        settlement_resolved = True
                        break
                except Exception as e:
                    logger.debug(f"[X-RAY] Closed PnL polling retry for {symbol}: {e}")

            # Fallback evidence: reconstruct from execution records before giving up.
            if not settlement_resolved:
                recon = await self._reconstruct_pnl_from_fills(symbol, ctx)
                if recon is not None:
                    net_pnl, fees, exit_price = recon
                    real_outcome = "PROFIT" if net_pnl > 0 else "LOSS"
                    settlement_resolved = True
                    logger.warning(
                        f"[X-RAY] {symbol}: closed-PnL unavailable; settled from execution "
                        f"records instead (net ${net_pnl:.4f})."
                    )

            if not settlement_resolved:
                logger.error(
                    f"[X-RAY] ⚠️ SETTLEMENT UNKNOWN // {symbol}: no closed-PnL record and no "
                    f"usable fills. Outcome excluded from Kelly, RLS labels and win-rate."
                )
                METRICS.reason(ReasonCode.SETTLE_UNKNOWN)

            arrival_price = ctx.get("arrival_price", actual_entry)
            trigger_price = ctx.get("exit_trigger_price", exit_price)

            if ctx["is_buy"]:
                entry_slip = ((actual_entry - arrival_price) / (arrival_price + 1e-9)) * 10000.0
                exit_slip = ((trigger_price - exit_price) / (trigger_price + 1e-9)) * 10000.0
            else:
                entry_slip = ((arrival_price - actual_entry) / (arrival_price + 1e-9)) * 10000.0
                exit_slip = ((exit_price - trigger_price) / (trigger_price + 1e-9)) * 10000.0

            slippage_bps = max(-500.0, min(500.0, entry_slip + exit_slip))

            ctx["exec_details"]["tca_entry_slippage_bps"] = entry_slip
            ctx["exec_details"]["tca_exit_slippage_bps"] = exit_slip
            ctx["exec_details"]["tca_total_slippage_bps"] = slippage_bps
            ctx["exec_details"]["fees_usdt"] = fees
            ctx["exec_details"]["settlement_status"] = "RESOLVED" if settlement_resolved else "UNKNOWN"

            # APEX sections 20-22: MFE/MAE are tracked live in ProfitProtectionState
            # and drive the CAMB ladder, then were discarded at resolution. Carrying
            # them to the ledger is what lets excursion analysis separate an ENTRY
            # problem (trades go straight against us) from an EXIT problem (trades
            # reach +2R and close at +0.3R). A win rate cannot tell those apart.
            # Left absent rather than zeroed when unavailable -- a 0.0 excursion is
            # the claim "the trade never moved", which is not the same as "we did
            # not record it".
            _exit_state = self.exit_states.get(symbol)
            _p_state = getattr(_exit_state, "profit_state", None) if _exit_state else None
            if _p_state is not None:
                ctx["exec_details"]["mfe_r"] = getattr(_p_state, "mfe_r", None)
                ctx["exec_details"]["mae_r"] = getattr(_p_state, "mae_r", None)

            if self.v12 is not None:
                try:
                    res = self.v12.on_settle(
                        ctx["signal_id"], time.time(), exit_price,
                        net_pnl if settlement_resolved else None,
                        fees if settlement_resolved else None,
                        ctx.get("exit_reason") or ("UNKNOWN" if not settlement_resolved else "OTHER"),
                        ctx["exec_details"].get("mfe_r"), ctx["exec_details"].get("mae_r"))
                    if res and res.get("health") == "HALT" and self.v12_mode == "enforce":
                        self._safe_telegram_dispatch_sync(
                            "<b>V12 STRATEGY HEALTH: HALT</b>\nNew entries stopped; open positions are "
                            "still managed. Review, then restart to clear.\n<pre>"
                            + self.v12.health.report(self.v12.guard.equity).render(self.mode.value)
                            + "</pre>", is_html=True)
                except Exception as e:
                    logger.warning(f"[V12] settlement journal/learning failed: {e}")

            if settlement_resolved:
                self.recent_pnl_history.append(net_pnl)
                conv = self._entry_conviction.pop(ctx.get("signal_id"), None)
                filled_notional = float(ctx.get("actual_qty_filled", 0.0)) * float(actual_entry or 0.0)
                if conv is not None and filled_notional > 0:
                    self.edge_gate.record(conv, net_pnl / filled_notional)
            else:
                self._entry_conviction.pop(ctx.get("signal_id"), None)

            # Audit P1 #13 Fix: Re-query live balance to eliminate synthetic equity drift
            current_cached = self.global_state_cache.get("current_vault_balance", MIN_REQUIRED_EQUITY)
            try:
                live_bal = await self.executor.get_wallet_balance_usdt()
                new_balance = live_bal if live_bal > 0.0 else max(0.01, current_cached + net_pnl)
            except Exception:
                new_balance = max(0.01, current_cached + net_pnl)
            
            daily_dd, systemic_dd, is_breached = await self.risk_vault.update_balance_atomic(new_balance)
            self.global_state_cache["current_vault_balance"] = new_balance
            self.global_state_cache["drawdown_pct"] = systemic_dd
            if self.v12 is not None:
                self.v12.set_equity(new_balance)

            duration_mins = (time.time() - ctx["daemon_start_time"]) / 60.0

            # 3. Post-Loss Asset Quarantine (only on a KNOWN loss)
            if settlement_resolved and net_pnl < 0:
                async with self.circuit_breaker_lock:
                    self.circuit_breakers[symbol] = time.time() + 300.0
                logger.warning(f"[RISK] Post-loss quarantine engaged for {symbol}: locked for 180s. Net PnL: ${net_pnl:.4f}")
                self.last_exit_direction[symbol] = (ctx["direction"], time.time(), "LOSS")
            else:
                self.last_exit_direction[symbol] = (ctx["direction"], time.time(), "WIN")

            if self.memory:
                await self.memory.log_live_execution_result(ctx["signal_id"], net_pnl, slippage_bps, real_outcome, ctx["exec_details"])

            # AUDIT B12: never train on an outcome we could not establish.
            if settlement_resolved and ctx.get("stat_engine") and hasattr(ctx["stat_engine"], "resolve_trade_outcome"):
                signal_notional = ctx.get("actual_qty_filled", 1.0) * ctx.get("actual_entry", MIN_REQUIRED_EQUITY)
                ctx["stat_engine"].resolve_trade_outcome(ctx["signal_id"], net_pnl, signal_notional)
            elif not settlement_resolved:
                stat_engine = ctx.get("stat_engine")
                if stat_engine and hasattr(stat_engine, "discard_pending_outcome"):
                    stat_engine.discard_pending_outcome(ctx["signal_id"])

            self._safe_telegram_dispatch_sync(
                self.telegram.format_execution_receipt(symbol, net_pnl, slippage_bps, fees, duration_mins, net_pnl > 0),
                is_html=True
            )

            if is_breached:
                self.fsm.trigger_global_emergency_lock(reason=f"Post-settlement drawdown breach: {systemic_dd:.2%}")
                self._safe_telegram_dispatch_sync(
                    f"<b>EMERGENCY DRAWDOWN BREAKER TRIPPED</b>\n"
                    f"Post-settlement drawdown breach: {systemic_dd:.2%}. Halting swarm.",
                    is_html=True
                )
                raise EmergencyShutdown(f"Drawdown breach on trade settlement: {systemic_dd:.2%}")

        await asyncio.shield(_settle())

    async def _position_lifecycle_daemon(
        self, symbol: str, signal_id: str, direction: str, current_price: float, atr: float,
        risk_matrix: dict, target_leverage: int = 2, market_regime: str = "TRENDING",
        is_recovery: bool = False, realigned_tp: float = None, dynamic_rr_ratio: float = 2.0,
        realigned_sl: float = None, historical_favorable_price: float = None
    ):
        specs = self.sor.instrument_cache.get(symbol, {})
        qty_step_str = str(specs.get("qty_step", risk_matrix.get("qty_step", "0.001")))

        ctx = {
            "symbol": symbol, "signal_id": signal_id, "direction": direction, "is_buy": direction == "BUY",
            "current_price": current_price, "atr": atr, "target_leverage": target_leverage,
            "arrival_price": risk_matrix.get("arrival_price", current_price),
            "actual_entry": current_price,
            "actual_qty_filled": risk_matrix.get("size", 1.0),
            "regime": market_regime, "daemon_start_time": time.time(),
            "qty_step": qty_step_str,
            "stat_engine": self.stat_engines.get(symbol),
            "last_ob": {},
            "latest_tick_price": current_price,
            "mark_price": current_price,
            "current_vault_balance": self.global_state_cache.get("current_vault_balance", MIN_REQUIRED_EQUITY),
            "drawdown_pct": self.global_state_cache.get("drawdown_pct", 0.0),
            "max_drawdown_pct": self.risk_vault.max_drawdown_pct,
            "active_positions_count": len(self.active_positions_map),
            "payload_features": {},
            "exec_details": {},
            "position_idx": self.sor.position_idx,
            "dynamic_rr_ratio": dynamic_rr_ratio,
            "test_mode": self.test_mode,
            "last_stress_check_time": time.time(),
            "last_liq_check_time": time.time(),
            "last_amend_time": time.time(),
            "amend_cooldown": 1.20,
            "last_exchange_sl": realigned_sl if realigned_sl else current_price,
            "taker_fee_rate": getattr(self.sor, "taker_fee_rate", 0.00055),
            "slippage_buffer_pct": 0.0004,
            "risk_dist_from_v12": bool(risk_matrix.get("v12_stop_pct")),
            "exit_reason": None,
        }
        if self.v12 is not None and self.v12_mode == "enforce":
            ctx["exit_config"] = self.v12.exit_cfg

        async with self.execution_semaphore:
            self.state_actor.dispatch(symbol, "RESERVE_IN_FLIGHT", {"notional": risk_matrix.get("allocated_value_usdt", 0.0)})
            await asyncio.sleep(0.15)
            verified, v_price, v_qty = await self._fast_verify_entry_fill(symbol, ctx["actual_qty_filled"])
            if verified:
                ctx["actual_entry"] = v_price if v_price > 0 else ctx["actual_entry"]
                ctx["actual_qty_filled"] = v_qty if v_qty > 0 else ctx["actual_qty_filled"]

            self.state_actor.dispatch(
                symbol, "REGISTER_POSITION",
                {"direction": direction, "notional": ctx["actual_qty_filled"] * ctx["actual_entry"]}
            )

        if symbol not in self.exit_states:
            self.exit_states[symbol] = PositionExitState(
                position_id=signal_id, entry_time=time.time(), entry_price=ctx["actual_entry"],
                exit_side="Sell" if direction == "BUY" else "Buy", entry_balance=ctx["current_vault_balance"],
                actual_qty=ctx["actual_qty_filled"], base_qty=ctx["actual_qty_filled"],
                execution_state="OBSERVE"
            )

        state = self.exit_states[symbol]
        self.active_contexts[symbol] = ctx

        try:
            current_active_sl = realigned_sl if realigned_sl else (
                ctx["actual_entry"] - (ctx["atr"] * 2.5) if ctx["is_buy"] else ctx["actual_entry"] + (ctx["atr"] * 2.5)
            )
            current_active_tp = realigned_tp if realigned_tp else (
                ctx["actual_entry"] + (ctx["atr"] * 2.5 * dynamic_rr_ratio) if ctx["is_buy"] else ctx["actual_entry"] - (ctx["atr"] * 2.5 * dynamic_rr_ratio)
            )
            ctx["last_exchange_sl"] = current_active_sl

            loop_state = "ACTIVE_MONITORING"

            while loop_state == "ACTIVE_MONITORING":
                await asyncio.sleep(0.050)

                current_price = ctx["latest_tick_price"]
                if current_price <= 0.0:
                    continue

                # One clock reading per iteration: the freshness check, the
                # stale-duration counter and the log throttle below must all
                # agree on "now", and re-reading time.time() per use lets them
                # disagree by a scheduling slice.
                now_sec = time.time()

                time_since_eval = now_sec - ctx.get("last_eval_time", 0)
                if current_price == ctx.get("last_eval_price") and time_since_eval < 1.0:
                    continue

                ctx["last_eval_price"] = current_price
                ctx["last_eval_time"] = now_sec

                ob = self.orderbook_snapshots.get(symbol, {})
                best_bid = float(ob.get("best_bid", 0.0) or 0.0)
                best_ask = float(ob.get("best_ask", 0.0) or 0.0)
                if best_bid <= 0.0:
                    best_bid = current_price
                if best_ask <= 0.0:
                    best_ask = current_price

                # AUDIT B19/B20 (residual): freshness was enforced on the ENTRY
                # path only. The exit loop read whatever snapshot was in memory,
                # so a frozen feed left the CAMB trailing stop being driven by a
                # stale book -- the failure mode that most endangers an OPEN
                # position. Stale data now suspends software exit decisions
                # while exchange-native stops and reconciliation continue.
                ob_age = (now_sec - float(ob["as_of"])) if ob.get("as_of") else None
                market_data_stale = (
                    ob_age is None or ob_age > self.config.max_orderbook_age_sec
                )
                ctx["market_data_stale"] = market_data_stale
                ctx["orderbook_age_sec"] = ob_age

                if market_data_stale:
                    ctx["stale_since"] = ctx.get("stale_since") or now_sec
                    stale_for = now_sec - ctx["stale_since"]
                    METRICS.reason(ReasonCode.STALE_DATA)
                    if now_sec - ctx.get("last_stale_log", 0.0) > 30.0:
                        ctx["last_stale_log"] = now_sec
                        logger.critical(
                            f"[EXIT_SENTRY] STALE MARKET DATA // {symbol}: book age "
                            f"{'unknown' if ob_age is None else f'{ob_age:.1f}s'} "
                            f"(stale {stale_for:.0f}s). Suspending software exit decisions; "
                            f"exchange-native stops remain in force and reconciliation continues."
                        )
                    await asyncio.sleep(0.25)
                    continue
                ctx["stale_since"] = None

                ctx["last_ob"] = {"best_bid": best_bid, "best_ask": best_ask}
                ctx["safe_c_price"] = best_bid if ctx["is_buy"] else best_ask
                ctx["mark_price"] = float(ob.get("markPrice", current_price) or current_price)
                if ctx["stat_engine"] and getattr(ctx["stat_engine"], "true_micro_price", 0.0) > 0:
                    ctx["safe_c_price"] = ctx["stat_engine"].true_micro_price

                now_sec = time.time()
                ctx["now"] = now_sec
                
                # AUDIT B11: intra-poll equity is rebuilt from WALLET BALANCE plus
                # current unrealised across ALL tracked positions. The previous
                # form added this position's unrealised PnL to `totalEquity`,
                # which already contained it -- double-counting the very figure
                # that triggers the emergency market exit at 15%.
                unrealized_pnl = (current_price - ctx["actual_entry"]) * ctx["actual_qty_filled"] if ctx["is_buy"] else \
                                 (ctx["actual_entry"] - current_price) * ctx["actual_qty_filled"]
                ctx["unrealised_pnl"] = unrealized_pnl

                live_equity = estimate_live_equity(
                    self.equity_snapshot, self._unrealised_by_symbol()
                )
                if live_equity is None:
                    live_equity = self.global_state_cache.get("current_vault_balance", MIN_REQUIRED_EQUITY)

                baseline_bal = self.risk_vault.peak_balance if self.risk_vault.peak_balance > 0 else live_equity
                live_drawdown = compute_drawdown(baseline_bal, live_equity)

                ctx["current_vault_balance"] = live_equity
                ctx["drawdown_pct"] = live_drawdown
                ctx["active_positions_count"] = len(self.active_positions_map)
                ctx["initial_risk_dist"] = abs(ctx["actual_entry"] - current_active_sl)
                ctx["current_sl"] = current_active_sl
                ctx["current_tp"] = current_active_tp

                # Liquidation Proximity Sentry
                if now_sec - ctx["last_liq_check_time"] >= (7.0 + random.uniform(0.0, 1.5)):
                    ctx["last_liq_check_time"] = now_sec
                    try:
                        pos_res = await self.executor.safe_call("GET", "/v5/position/list", category="linear", symbol=symbol)
                        pos_list = pos_res.get("result", {}).get("list", [])
                        if pos_list:
                            pos_sz = float(pos_list[0].get("size", 0.0) or 0.0)
                            if pos_sz <= 0.0:
                                logger.info(f"[SOR_RECON] Exchange-native stop resolved position for {symbol}. Settling...")
                                ctx["exit_trigger_price"] = current_price
                                ctx["exit_reason"] = "EXCHANGE_BRACKET"
                                break

                            liq_price = float(pos_list[0].get("liqPrice", 0.0) or 0.0)
                            mark_price = float(pos_list[0].get("markPrice", current_price) or current_price)
                            if liq_price > 0.0:
                                liq_dist_pct = abs(mark_price - liq_price) / mark_price
                                if liq_dist_pct <= max(0.015, (ctx["atr"] * 2.5) / mark_price):
                                    logger.critical(
                                        f"[RISK BREACH] LIQUIDATION PROXIMITY SENTRY // {symbol} Mark: {mark_price:.4f} "
                                        f"Liq: {liq_price:.4f} (Dist: {liq_dist_pct:.2%}). Forcing emergency liquidation!"
                                    )
                                    ctx["exit_trigger_price"] = current_price
                                    await self._execute_emergency_escape(symbol, current_price, ctx["actual_qty_filled"], is_sell=ctx["is_buy"])
                                    break
                    except Exception as e:
                        logger.debug(f"[RISK] Liquidation distance probe warning: {e}")

                # Interval-Gated Stress Evaluation
                if now_sec - ctx["last_stress_check_time"] >= 5.0 and ctx["stat_engine"] and hasattr(ctx["stat_engine"], 'evaluate_active_trade_stress'):
                    ctx["last_stress_check_time"] = now_sec
                    should_eject, stress_reason = ctx["stat_engine"].evaluate_active_trade_stress(ctx["is_buy"])
                    if should_eject:
                        logger.critical(f"[X-RAY] ADVERSE STRESS SENTRY // {symbol}: {stress_reason}. Ejecting!")
                        ctx["exit_trigger_price"] = current_price
                        ctx["exit_reason"] = "STRESS_SENTRY"
                        self.fsm.trigger_asset_lock(symbol, 300)
                        await ExecutionGovernorFSM.manage_execution(
                            decision=ExitDecision("EXIT", 0.0, "FLASH_IOC", current_price, 0.0, 0.0, stress_reason, ""),
                            state=state, ctx=ctx, executor=self.executor
                        )
                        break

                # Continuous Adaptive Microstructure Barrier (CAMB) Evaluation
                decision = IntelligentExitEngine.evaluate(ctx, state)

                if decision.action in ["EXIT", "CLOSE", "EMERGENCY"]:
                    logger.critical(f"[X-RAY] SOVEREIGN EXIT FIRED // {symbol}: {decision.reason}")
                    ctx["exit_trigger_price"] = current_price
                    ctx["exit_reason"] = str(decision.reason).split(" ")[0]
                    await ExecutionGovernorFSM.manage_execution(decision, state, ctx, self.executor)
                    break

                elif decision.action == "SCALE_OUT":
                    logger.info(f"[X-RAY] PARTIAL SCALE-OUT // {symbol}: {decision.reason}")
                    await ExecutionGovernorFSM.manage_execution(decision, state, ctx, self.executor)

                else:
                    await ExecutionGovernorFSM.manage_execution(decision, state, ctx, self.executor)

                # Server-Side Monotonic Stop Advancement
                target_sl = decision.exchange_ts_price
                target_tp = decision.dynamic_tp_price

                if target_sl > 0:
                    atr_val = ctx["atr"]
                    last_sl = ctx.get("last_exchange_sl", current_active_sl)
                    
                    is_progress = (target_sl > last_sl + (atr_val * 0.15)) if ctx["is_buy"] else (target_sl < last_sl - (atr_val * 0.15))
                    time_elapsed = now_sec - ctx.get("last_amend_time", 0.0)
                    amend_cooldown = ctx.get("amend_cooldown", 1.20)

                    if is_progress and time_elapsed >= amend_cooldown:
                        ctx["last_amend_time"] = now_sec
                        amended_ok = await self.sor._amend_trailing_stop(symbol, target_sl, target_tp)
                        if amended_ok:
                            ctx["last_exchange_sl"] = target_sl
                            current_active_sl = target_sl
                            current_active_tp = target_tp
                            ctx["amend_cooldown"] = 1.20
                            logger.info(f"[CAMB] EXCHANGE STOP ADVANCED // {symbol} SL: {target_sl:.4f} | TP: {target_tp:.4f}")
                        else:
                            ctx["amend_cooldown"] = min(8.0, amend_cooldown * 1.5)
                            logger.warning(
                                f"[CAMB] Stop amendment rejected by exchange for {symbol}. "
                                f"Backing off for {ctx['amend_cooldown']:.1f}s. Retaining active SL: {current_active_sl:.4f}"
                            )

                if state.q_retained <= 0.01 and state.execution_state == "OBSERVE":
                    ctx["exit_trigger_price"] = current_price
                    break

            self.state_actor.dispatch(symbol, "LIQUIDATE_POSITION", {"direction": ctx["direction"], "outcome": "CLOSED"})
            
            # AUDIT B7: settlement runs in EVERY mode. Previously PAPER/TESTNET
            # skipped it, so those runs produced no outcomes and no learning data.
            self.track_task(self._state_settle_trade(ctx), is_critical=True)

        except Exception as e:
            logger.error(f"[X-RAY] Position lifecycle daemon fault for {symbol}: {e}", exc_info=True)
            self.fsm.record_module_error("_position_lifecycle_daemon")
            self.state_actor.dispatch(symbol, "LIQUIDATE_POSITION", {"direction": ctx["direction"], "outcome": "ERROR"})

    async def _execute_emergency_escape(self, symbol: str, current_price: float, qty: float, is_sell: bool):
        async def _escape():
            side = "Sell" if is_sell else "Buy"
            qty_str = self.sor._format_qty_str(qty, symbol)
            logger.critical(f"[X-RAY] VERIFIED EMERGENCY FLATTEN // {symbol} {side} {qty_str} units via Market IOC.")
            
            for attempt in range(5):
                await self.executor.safe_call(
                    "POST", "/v5/order/create", is_execution=True,
                    category="linear", symbol=symbol, side=side,
                    orderType="Market", qty=qty_str, timeInForce="IOC", reduceOnly=True,
                    positionIdx=self.sor.position_idx,
                    smpType="CancelMaker"
                )
                await asyncio.sleep(0.35)
                
                pos_res = await self.executor.safe_call("GET", "/v5/position/list", category="linear", symbol=symbol)
                pos_list = pos_res.get("result", {}).get("list", [])
                remaining_size = float(pos_list[0].get("size", 0.0)) if pos_list else 0.0
                
                if remaining_size <= 0.0:
                    logger.critical(f"  EMERGENCY ESCAPE VERIFIED // {symbol} inventory completely cleared.")
                    return
                else:
                    qty_str = self.sor._format_qty_str(remaining_size, symbol)
                    logger.warning(f"  Partial escape fill on {symbol}. Remaining: {remaining_size}. Retrying ({attempt + 1}/5)...")
                    
            logger.critical(f"  FATAL: Emergency escape failed to zero {symbol} after 5 attempts. Engaging global emergency lock.")
            self.fsm.trigger_global_emergency_lock(reason=f"Emergency escape failed to zero {symbol}")

        await asyncio.shield(_escape())

    async def _acquire_cloud_instance_lease(self) -> str:
        instance_id = str(uuid.uuid4())
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        if not self.memory or not self.memory.supabase:
            logger.warning("[LEASE] Supabase unavailable. Running with local instance lease.")
            return instance_id
            
        try:
            res = await asyncio.to_thread(
                self.memory.supabase.table("swarm_instance_lease")
                .select("*")
                .eq("environment", "PRODUCTION")
                .execute
            )
            rows = res.data if res else []
            if rows:
                last_hb = datetime.datetime.fromisoformat(rows[0]["last_heartbeat"].replace('Z', '+00:00'))
                age_seconds = (datetime.datetime.now(datetime.timezone.utc) - last_hb).total_seconds()
                if age_seconds < 45.0 and rows[0]["instance_id"] != instance_id:
                    logger.critical(f"FATAL: Active instance already running ({rows[0]['instance_id']}). Aborting twin boot.")
                    raise EmergencyShutdown("Twin instance collision detected.")

            await asyncio.to_thread(
                self.memory.supabase.table("swarm_instance_lease")
                .upsert({"environment": "PRODUCTION", "instance_id": instance_id, "last_heartbeat": now_iso})
                .execute
            )
            logger.info(f"  CLOUD LEASE ACQUIRED: Instance ID {instance_id}")
        except EmergencyShutdown:
            raise
        except Exception as e:
            logger.warning(f"[LEASE] Cloud mutex lease unfulfilled ({e}). Falling back to standalone mode.")
            
        return instance_id

    async def graceful_shutdown(self, emergency: Optional[bool] = None):
        """
        AUDIT B8: a PLANNED restart must not liquidate healthy positions.

        Previously every exit path -- including a clean SIGTERM during a deploy --
        ran the flatten sequence, so routine restarts closed the book at market.
        Inventory is now flattened only on a real capital emergency; otherwise
        resting orders are cancelled and positions are left for the next
        instance to adopt via reconciliation.
        """
        if emergency is None:
            emergency = self.fsm.is_emergency_locked()

        logger.critical(
            f"  INITIATING SHUTDOWN SEQUENCE... (emergency={emergency}, "
            f"reason={self.fsm.lock_reason or 'planned'})"
        )

        if hasattr(self, 'telegram'):
            try:
                await self.telegram.send_html_report("  <b>CRITICAL: SYSTEM SHUTDOWN INITIATED</b>\nFlattening open inventory.")
            except Exception:
                pass

        symbols_to_cancel = list(self.active_positions_map.keys())

        for symbol in symbols_to_cancel:
            try:
                await self.executor.safe_call("POST", "/v5/order/cancel-all", is_execution=True, category="linear", symbol=symbol)
            except Exception as e:
                logger.error(f"[X-RAY] Cancel failed for {symbol}: {e}")

        if emergency:
            for symbol in symbols_to_cancel:
                try:
                    pos_res = await self.executor.safe_call("GET", "/v5/position/list", category="linear", symbol=symbol)
                    pos_list = pos_res.get("result", {}).get("list", [])
                    if pos_list and float(pos_list[0].get("size", 0.0)) > 0:
                        qty = float(pos_list[0]["size"])
                        side = "Sell" if pos_list[0]["side"] == "Buy" else "Buy"
                        current_p = float(pos_list[0].get("markPrice", pos_list[0].get("avgPrice", 0.0)))
                        await self._execute_emergency_escape(symbol, current_p, qty, side == "Sell")
                except Exception as e:
                    logger.error(f"[X-RAY] Flatten failed for {symbol}: {e}")
        elif symbols_to_cancel:
            logger.critical(
                f"  PLANNED SHUTDOWN: leaving {len(symbols_to_cancel)} position(s) OPEN with "
                f"exchange-native stops attached. They will be adopted by the next instance "
                f"via reconciliation. Positions: {sorted(symbols_to_cancel)}"
            )

        if hasattr(self, 'memory') and self.memory:
            await self.memory.flush_and_close()
        if hasattr(self, 'telegram'):
            await self.telegram.close()

        if hasattr(self, 'executor') and self.executor and hasattr(self.executor, 'close'):
            try:
                await self.executor.close()
            except Exception as e:
                logger.debug(f"Executor close warning absorbed: {e}")

        self.math_pool.shutdown(wait=False)
        self.io_pool.shutdown(wait=False)
        logger.critical("  MATRIX DISCONNECTED.")

    async def run_v12_warm_start(self):
        """
        Feed the last ~5 hours of 1-minute closes through the V12 forecaster and
        regime classifier for every traded symbol, so a restart does not mean
        hours of NO TRADE while they re-measure. Causal: each close is observed
        in time order, predictions are scored only against later closes. Public
        market data only; failures just leave that symbol to warm up live.
        """
        if self.v12 is None:
            return
        await asyncio.sleep(20.0)
        done: Set[str] = set()
        while True:
            for symbol in list(self.asset_basket):
                if symbol in done:
                    continue
                try:
                    res = await self.executor.safe_call("GET", "/v5/market/kline", category="linear",
                                                        symbol=symbol, interval="1", limit=300)
                    rows = res.get("result", {}).get("list", []) if isinstance(res, dict) else []
                    bars = sorted((float(r[0]) / 1000.0, float(r[4])) for r in rows if len(r) >= 5)
                    now = time.time()
                    bars = [(t + 60.0, c) for t, c in bars if t + 60.0 <= now]   # closed bars only
                    if len(bars) >= 60:
                        await asyncio.to_thread(self.v12.warm_start, symbol, bars)
                        logger.info(f"[V12] warm start {symbol}: {len(bars)} closed 1m bars")
                    done.add(symbol)
                except Exception as e:
                    logger.debug(f"[V12] warm start skipped for {symbol}: {e}")
                await asyncio.sleep(0.5)
            await asyncio.sleep(300.0)

    async def run_v12_reporter(self):
        """Hourly expectancy-first strategy health report to Telegram."""
        if self.v12 is None:
            return
        interval = float(os.getenv("V12_REPORT_INTERVAL_SEC", "3600"))
        import html as _html
        while True:
            await asyncio.sleep(interval)
            try:
                self._safe_telegram_dispatch_sync(
                    "<b>V12 STRATEGY HEALTH</b>\n<pre>" + _html.escape(self.v12.health_text()) + "</pre>",
                    is_html=True)
            except Exception as e:
                logger.warning(f"[V12] health report failed: {e}")

    async def run_engine_forever(self):
        try:
            self.instance_id = await self._acquire_cloud_instance_lease()

            boot_bal = await self._get_true_equity_usdt()
            if boot_bal <= 0.0:
                self.fsm.trigger_global_emergency_lock(reason="Zero or unverified wallet balance on boot")
                logger.critical("  FATAL BOOT FAULT: Could not verify real Bybit wallet balance. Swarm locked.")
                raise EmergencyShutdown("Zero or unverified wallet balance on boot.")

            if boot_bal < MIN_REQUIRED_EQUITY:
                self.fsm.trigger_global_emergency_lock(reason=f"Insufficient bankroll: ${boot_bal:.2f} < ${MIN_REQUIRED_EQUITY:.2f}")
                logger.critical(
                    f"  FATAL BOOT FAULT: Verified Bybit wallet balance (${boot_bal:.2f}) is below "
                    f"the configured capital floor (${MIN_REQUIRED_EQUITY:.2f}). Swarm locked."
                )
                raise EmergencyShutdown(
                    f"Insufficient bankroll: ${boot_bal:.2f} < ${MIN_REQUIRED_EQUITY:.2f} minimum required."
                )

            self.global_state_cache["start_of_day_balance"] = boot_bal
            self.global_state_cache["wallet_baseline"] = boot_bal
            self.global_state_cache["lifetime_initial_balance"] = boot_bal
            self.global_state_cache["current_vault_balance"] = boot_bal
            self.global_state_cache["last_updated"] = time.time()
            self.global_state_cache["current_day"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

            self.risk_vault.sync_watermarks(boot_bal)

            is_safe, reason = await self.risk_vault.evaluate_portfolio_safety(boot_bal, 0.0, "")
            if not is_safe:
                self.fsm.trigger_global_emergency_lock(reason=f"Vault boot check failed: {reason}")
                logger.critical(f"  BOOT SAFETY LATCH ENGAGED: Vault rejected ({reason}). Swarm locked.")
                raise EmergencyShutdown(f"Risk Vault boot check failed: {reason}")
            else:
                self.fsm.release_global_emergency_lock()
                self.risk_vault.reset_circuit_breaker()
                logger.info(f"  WALLET LOCKED & VERIFIED: Active Bankroll = ${boot_bal:.2f} USDT")
                if self.v12 is not None:
                    self.v12.set_equity(boot_bal)

        except Exception as e:
            self.fsm.trigger_global_emergency_lock(reason=f"Boot equity initialization failed: {e}")
            # `from e` keeps the original traceback attached: without it the
            # operator sees only "boot failed" and loses whether it was a
            # network error, a bad key or a rejected vault check.
            raise EmergencyShutdown(f"Boot equity initialization failed: {e}") from e

        try:
            await self.executor.connect_ws()
        except Exception as e:
            logger.error(f"[X-RAY] Failed to bind WebSocket: {e}")

        await asyncio.sleep(0.3)
        try:
            await self.executor.safe_call("POST", "/v5/position/switch-mode", is_execution=True, category="linear", coin="USDT", mode=0)
            logger.info("  Bybit Unified Account confirmed in One-Way Mode.")
        except Exception:
            pass

        await asyncio.sleep(0.3)
        try:
            fee_schedule = await self.executor.get_fee_rates("BTCUSDT")
            self.sor.taker_fee_rate = fee_schedule["taker"]
            self.sor.maker_fee_rate = fee_schedule["maker"]
            self.yield_engine.taker_fee_rate = fee_schedule["taker"]
            self.yield_engine.maker_fee_rate = fee_schedule["maker"]
            logger.info(
                f"  EXCHANGE FEES SYNCED // Taker: {fee_schedule['taker']*10000:.1f} bps | Maker: {fee_schedule['maker']*10000:.1f} bps"
            )
        except Exception as e:
            logger.warning(f"[X-RAY] Dynamic fee schedule check bypassed: {e}")

        if hasattr(self, 'memory') and self.memory:
            await self.memory.start()
            try:
                async with self.memory._db_lock:
                    rows = list(self.memory.local_cursor.execute(
                        "SELECT * FROM quantitative_ledger WHERE resolved = 1 AND is_shadow = 0"))
                loaded = self.edge_gate.load_from_ledger_rows(rows)
                logger.info(f"[EDGE_GATE] mode={self.edge_gate.cfg.mode} // loaded {loaded} settled trades "
                            f"from the ledger. {self.edge_gate.snapshot()['bands']}")
            except Exception as e:
                logger.warning(f"[EDGE_GATE] could not load history ({e}); starting empty.")

        await asyncio.sleep(0.3)
        try:
            await self.sor._fetch_exchange_limits("BTCUSDT")
            await self.synchronize_exchange_state()
        except Exception:
            pass

        await asyncio.sleep(0.3)
        await self.run_universe_refresher()

        daemons = [
            self.state_actor.start,
            self.run_telegram_worker,
            self.run_cloud_lease_heartbeat,  # Dedicated 15s Cloud Mutex Sentry
            self.run_dna_prewarmer,
            self.stream_manager_loop,
            self.run_system_heartbeat,
            self.run_shadow_resolution_daemon,
            self._universe_refresher_loop,
            self.run_omni_swarm_director,
            self.run_fast_state_invariant_reconciliation,
            self.run_correlation_engine,
            self.run_v12_warm_start,
            self.run_v12_reporter,
        ]

        tasks = [asyncio.create_task(safe_daemon_wrapper(d, self)) for d in daemons]

        while not self.fsm.is_emergency_locked():
            await asyncio.sleep(0.5)

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def main():
    engine = DistributedQuantEngine()
    was_emergency = False
    try:
        await engine.run_engine_forever()
        was_emergency = engine.fsm.is_emergency_locked()
    except EmergencyShutdown:
        logger.critical("  Engine halted via Emergency Breaker.")
        was_emergency = True
    except asyncio.CancelledError:
        # AUDIT B8: cancellation is a PLANNED stop (SIGTERM / deploy), not a
        # capital emergency. It must not liquidate the book.
        logger.critical("  Engine cancelled -- treating as planned shutdown.")
        was_emergency = False
    except Exception as e:
        logger.critical(f"  Engine crashed: {e}", exc_info=True)
        was_emergency = True
    finally:
        await engine.graceful_shutdown(emergency=was_emergency)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)