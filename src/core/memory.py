"""
V44.2 APEX TITAN: PURE-ASYNC FORENSIC & TCA MEMORY LEDGER
--------------------------------------------------------------------------------
Hyper-optimized persistent database connector and Transaction Cost Analysis (TCA) ledger.

Production Hardening & Quantitative Upgrades (V44.2 Hotfix):
1. Cold-Start Arming Activation: Defaults new and freshly scanned tokens to armed (is_armed=True)
   on boot unless explicitly flagged for demotion, eradicating the 0-trade live execution lockout.
2. Holographic Fallback Arming: Guarantees live execution remains active even when 
   in-memory local holographic samples are sparse during initial warmup.
3. Graceful SQLite-Only Degradation: Bypasses fatal boot crashes when SUPABASE credentials
   are missing or unreachable, operating seamlessly in WAL mode.
4. Shadow/Live Forensic Decoupling: Queries strictly constrain shadow resolutions to 
   `WHERE resolved = 0 AND is_shadow = 1` to prevent overriding active real inventory.
5. High-Throughput Micro-Batching: Asynchronously drains write queues into batched 
   upserts/inserts with non-blocking SQLite checkpoints and clean task cancellation teardown.
"""

import os
import time
import math
import logging
import asyncio
import sqlite3
import numpy as np
from datetime import datetime, timezone
from typing import Tuple, List, Dict, Any, Optional

try:
    from supabase import create_client, Client
    HAS_SUPABASE = True
except ImportError:
    HAS_SUPABASE = False
    Client = Any

logger = logging.getLogger("QUANT_CORE.MEMORY")


class MemoryBank:
    """
    V44.2 PURE-ASYNC FORENSIC LEDGER
    Drives distributed trade forensics, shadow promotion gating, and Bayesian
    DNA clustering with resilient on-disk SQLite WAL reads and batched cloud persistence.
    """
    def __init__(self, db_path: Optional[str] = None):
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")

        # Graceful degradation to local mode if Supabase credentials missing
        self.supabase: Optional[Any] = None
        if HAS_SUPABASE and url and key:
            try:
                self.supabase = create_client(url, key)
                logger.info("🛸 CLOUD LEDGER BOUND: Connected successfully to Supabase cluster.")
            except Exception as e:
                logger.warning(f"[MEMORY] Cloud connection failed ({e}). Degraded to Local SQLite-Only Mode.")
        else:
            logger.warning("[MEMORY] Supabase credentials missing or client unavailable. Operating in Local SQLite-Only Mode.")

        # Storage directory resolution for persistent on-disk WAL
        self.storage_dir = os.environ.get("PERSISTENT_STORAGE_PATH", ".")
        os.makedirs(self.storage_dir, exist_ok=True)
        self.db_path = db_path or os.path.join(self.storage_dir, "titan_memory_ledger.db")

        # Tier-1 Caches
        self.dna_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self.cache_ttl_seconds: float = 120.0

        # Concurrency Lock for SQLite Operations
        self._db_lock = asyncio.Lock()

        # Tier-2 Fast-Path Cache (Persistent On-Disk SQLite WAL)
        self._init_sqlite()

        # Holographic Local Fallback Matrix
        self.holo_capacity = 25000
        self.holo_features = np.zeros((self.holo_capacity, 3), dtype=np.float32)
        self.holo_outcomes = np.zeros(self.holo_capacity, dtype=np.float32)
        self.holo_pointer = 0
        self.holo_warmed_up = False

        # Async Micro-Batched Write Buffer
        self.write_queue: Optional[asyncio.Queue] = None
        self._bg_task: Optional[asyncio.Task] = None
        self._is_shutting_down = False

    def _init_sqlite(self):
        """Initializes the persistent on-disk SQLite ledger configured for WAL concurrency."""
        self.local_db = sqlite3.connect(self.db_path, check_same_thread=False)
        self.local_db.row_factory = sqlite3.Row
        self.local_cursor = self.local_db.cursor()

        # High-throughput WAL mode and synchronous=NORMAL
        self.local_cursor.execute("PRAGMA journal_mode = WAL;")
        self.local_cursor.execute("PRAGMA synchronous = NORMAL;")
        self.local_cursor.execute("PRAGMA busy_timeout = 5000;")
        self.local_cursor.execute("PRAGMA cache_size = -64000;")  # 64MB Page Cache

        self.local_cursor.execute('''
            CREATE TABLE IF NOT EXISTS quantitative_ledger (
                signal_id TEXT PRIMARY KEY,
                timestamp TEXT,
                symbol TEXT,
                predicted_direction TEXT,
                price_at_prediction REAL,
                is_correct BOOLEAN,
                vol_mult REAL,
                log_mlofi_z REAL,
                spread REAL,
                net_pnl REAL,
                actual_outcome TEXT,
                resolved BOOLEAN,
                is_shadow BOOLEAN,
                fees_usdt REAL,
                slippage_drag REAL,
                holding_minutes REAL,
                virtual_sl REAL,
                virtual_tp REAL,
                target_notional REAL DEFAULT 10.0
            )
        ''')

        # Covering indexes for real-time KNN edge scans and decoupled shadow forensics
        self.local_cursor.execute('CREATE INDEX IF NOT EXISTS idx_sym_res_ts ON quantitative_ledger(symbol, resolved, timestamp DESC)')
        self.local_cursor.execute('CREATE INDEX IF NOT EXISTS idx_unresolved_shadow ON quantitative_ledger(resolved, is_shadow, timestamp ASC)')
        self.local_cursor.execute('CREATE INDEX IF NOT EXISTS idx_forensic_covering ON quantitative_ledger(timestamp DESC, resolved, is_shadow)')
        self.local_db.commit()

    async def _warm_sqlite_from_cloud(self):
        """Pre-warms the persistent SQLite ledger from Supabase on boot if empty and cloud available."""
        if not self.supabase:
            return

        async with self._db_lock:
            self.local_cursor.execute("SELECT COUNT(*) FROM quantitative_ledger")
            count = self.local_cursor.fetchone()[0]

        if count >= 1000:
            logger.info(f"✅ Local SQLite WAL warm with {count} verified records. Bypassing cloud sync.")
            return

        logger.info("🔥 Warming local SQLite fast-path cache from Supabase...")
        try:
            query = (
                self.supabase.table("quantitative_ledger")
                .select("signal_id, timestamp, symbol, predicted_direction, price_at_prediction, is_correct, vol_mult, log_mlofi_z, spread, net_pnl, actual_outcome, resolved, is_shadow, fees_usdt, slippage_drag, holding_minutes, virtual_sl, virtual_tp, target_notional")
                .order("timestamp", desc=True)
                .limit(15000)
            )
            response = await self._safe_execute_async(query)
            rows = response.data if response else []

            async with self._db_lock:
                for r in rows:
                    self.local_cursor.execute('''
                        INSERT OR IGNORE INTO quantitative_ledger 
                        (signal_id, timestamp, symbol, predicted_direction, price_at_prediction, is_correct, vol_mult, log_mlofi_z, spread, net_pnl, actual_outcome, resolved, is_shadow, fees_usdt, slippage_drag, holding_minutes, virtual_sl, virtual_tp, target_notional)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        r.get("signal_id"), r.get("timestamp"), r.get("symbol"), r.get("predicted_direction"),
                        r.get("price_at_prediction"), 1 if r.get("is_correct") else 0, r.get("vol_mult"),
                        r.get("log_mlofi_z"), r.get("spread"), r.get("net_pnl") or 0.0, r.get("actual_outcome"),
                        1 if r.get("resolved") else 0, 1 if r.get("is_shadow") else 0,
                        r.get("fees_usdt") or 0.0, r.get("slippage_drag") or 0.0, r.get("holding_minutes") or 0.0,
                        r.get("virtual_sl") or 0.0, r.get("virtual_tp") or 0.0, r.get("target_notional") or 10.0
                    ))
                self.local_db.commit()
            logger.info(f"✅ SQLite warm-up complete. Pre-loaded {len(rows)} historical records.")
        except Exception as e:
            logger.warning(f"⚠️ SQLite warm-up failed, continuing with active local cache: {e}")

    async def start(self):
        """Initializes the async write queue, pre-warms local state, and starts the batching worker."""
        await self._warm_sqlite_from_cloud()
        self.write_queue = asyncio.Queue(maxsize=50000)
        self._is_shutting_down = False
        self._bg_task = asyncio.create_task(self._async_sync_worker())
        logger.info("🛡️ ASYNC MICRO-BATCH WORKER ONLINE: DB writes non-blocking & batched.")

    async def flush_and_close(self):
        """
        Drains and flushes all pending database mutations and cleanly checkpoints 
        SQLite WAL files before shutting down.
        """
        logger.info("⏳ Halting async DB worker and flushing forensic ledger...")
        self._is_shutting_down = True

        if self.write_queue:
            pending_tasks = []
            while not self.write_queue.empty():
                try:
                    task = self.write_queue.get_nowait()
                    if task is not None:
                        pending_tasks.append(task)
                except asyncio.QueueEmpty:
                    break

            if pending_tasks and self.supabase:
                logger.info(f"💾 Flushing {len(pending_tasks)} pending execution records in batches...")
                chunk_size = 50
                for i in range(0, len(pending_tasks), chunk_size):
                    chunk = pending_tasks[i:i + chunk_size]
                    try:
                        await self._dispatch_batch(chunk)
                    except Exception as e:
                        logger.error(f"Flush execution batch error: {e}")

        if self._bg_task and not self._bg_task.done():
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                pass

        async with self._db_lock:
            try:
                self.local_cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                self.local_db.commit()
                self.local_db.close()
                logger.info("🔒 Local SQLite database closed and WAL checkpointed.")
            except Exception as e:
                logger.error(f"Error closing SQLite database: {e}")

        logger.info("✅ Ledger teardown complete.")

    async def _async_sync_worker(self):
        """Processes database mutations using high-throughput micro-batching."""
        while not self._is_shutting_down:
            try:
                first_task = await self.write_queue.get()
                if first_task is None:
                    self.write_queue.task_done()
                    break

                batch = [first_task]
                while len(batch) < 50 and not self.write_queue.empty():
                    try:
                        task = self.write_queue.get_nowait()
                        if task is None:
                            break
                        batch.append(task)
                    except asyncio.QueueEmpty:
                        break

                if self.supabase:
                    await self._dispatch_batch(batch)

                for _ in range(len(batch)):
                    self.write_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[X-RAY] Async background batch sync failed: {e}")
                await asyncio.sleep(0.5)

    async def _dispatch_batch(self, batch: List[Tuple]):
        """Groups mutations by type and table to execute batched queries."""
        if not self.supabase:
            return

        inserts_by_table: Dict[str, List[Dict[str, Any]]] = {}
        upserts_by_table: Dict[str, List[Dict[str, Any]]] = {}
        updates: List[Tuple] = []

        for op_type, table, payload, match_col, match_val in batch:
            if op_type == "INSERT":
                if isinstance(payload, list):
                    inserts_by_table.setdefault(table, []).extend(payload)
                else:
                    inserts_by_table.setdefault(table, []).append(payload)
            elif op_type == "UPSERT":
                if isinstance(payload, list):
                    upserts_by_table.setdefault(table, []).extend(payload)
                else:
                    upserts_by_table.setdefault(table, []).append(payload)
            elif op_type == "UPDATE":
                updates.append((table, payload, match_col, match_val))

        for table, records in inserts_by_table.items():
            query = self.supabase.table(table).insert(records)
            await self._safe_execute_async(query)

        for table, records in upserts_by_table.items():
            query = self.supabase.table(table).upsert(records)
            await self._safe_execute_async(query)

        for table, payload, match_col, match_val in updates:
            query = self.supabase.table(table).update(payload).eq(match_col, match_val)
            await self._safe_execute_async(query)

    async def _safe_execute_async(self, query_builder, max_retries: int = 2):
        """Executes Supabase queries in background threads with strict timeout guards."""
        if not self.supabase:
            return None

        for attempt in range(max_retries):
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(query_builder.execute),
                    timeout=7.0
                )
            except asyncio.TimeoutError:
                if attempt == max_retries - 1:
                    logger.debug(f"[X-RAY] Supabase call timed out after {max_retries} attempts.")
                    return None
                await asyncio.sleep(0.1)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.debug(f"[X-RAY] Supabase fault absorbed after {max_retries} attempts: {e}")
                    return None
                await asyncio.sleep(0.05)

    def _ingest_hologram_data(self, rows: List[Dict[str, Any]]):
        """Organically feeds the local Hologram with verified historical records."""
        if not rows:
            return

        for r in rows:
            idx = self.holo_pointer % self.holo_capacity
            self.holo_features[idx, 0] = min(float(r.get("vol_mult", 1.0) or 1.0), 10.0)
            self.holo_features[idx, 1] = float(r.get("log_mlofi_z", 0.0) or 0.0)

            h_price = float(r.get("price_at_prediction", 1.0) or 1.0)
            h_spread_raw = float(r.get("spread", 0.0) or 0.0)
            self.holo_features[idx, 2] = (h_spread_raw / h_price) * 1000.0 if h_price > 0 else 0.001

            self.holo_outcomes[idx] = 1.0 if r.get("is_correct") is True else 0.0
            self.holo_pointer += 1

        if self.holo_pointer >= 100:
            self.holo_warmed_up = True

    def _parse_iso_timestamp(self, ts_str: str) -> datetime:
        if ts_str.endswith('Z'):
            ts_str = ts_str.replace('Z', '+00:00')
        return datetime.fromisoformat(ts_str)

    async def commit_prediction(
        self, 
        signal_id: str, 
        timestamp: float, 
        price: float, 
        direction: str, 
        confidence: float, 
        features: Optional[Dict[str, Any]] = None, 
        is_shadow: bool = False
    ):
        """Persists the full state vector to local SQLite (Instant) and Supabase (Batched)."""
        if not self.write_queue:
            return
        if features is None:
            features = {}

        market_regime = features.get("market_regime", "UNKNOWN")
        log_mlofi_z = features.get("log_mlofi_z", 0.0)
        hawkes_z = features.get("hawkes_z", 0.0)
        sector_impulse = features.get("sector_impulse", 0.0)
        swd_z = features.get("swd_z", 0.0)
        accel_z = features.get("accel_z", 0.0)
        micro_dislocation_z = features.get("micro_dislocation_z", 0.0)
        hurst_h = features.get("hurst_h", 0.5)
        bocd_cp_prob = features.get("bocd_cp_prob", 0.0)
        ou_divergence_z = features.get("ou_divergence_z", 0.0)
        cvd_z = features.get("cvd_z", 0.0)

        vol_mult = features.get("vol_mult", features.get("liquidity_density_ratio", 1.0))
        spread = features.get("bid_ask_spread", features.get("spread", 0.0))
        symbol = features.get("symbol", "UNKNOWN")

        kelly_fraction = features.get("kelly_fraction", 0.0)
        conformal_gate = features.get("conformal_gate", features.get("dynamic_gate", 0.52))

        sl_price = float(features.get("virtual_sl", price * 0.99))
        tp_price = float(features.get("virtual_tp", price * 1.015))
        target_notional = float(features.get("target_notional", 10.0))
        iso_timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()

        # 1. Update SQLite Local Fast-Path Replica (Protected by Async Lock)
        try:
            async with self._db_lock:
                self.local_cursor.execute('''
                    INSERT INTO quantitative_ledger 
                    (signal_id, timestamp, symbol, predicted_direction, price_at_prediction, is_correct, vol_mult, log_mlofi_z, spread, net_pnl, actual_outcome, resolved, is_shadow, fees_usdt, slippage_drag, holding_minutes, virtual_sl, virtual_tp, target_notional)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(signal_id), iso_timestamp, symbol, str(direction).upper(), float(price),
                    0, float(vol_mult), float(log_mlofi_z), float(spread), 0.0, None, 0, 1 if is_shadow else 0,
                    0.0, 0.0, 0.0, sl_price, tp_price, target_notional
                ))
                self.local_db.commit()
        except Exception as e:
            logger.debug(f"Local SQLite insert fault: {e}")

        # 2. Queue for Batched Cloud Storage if Supabase is available
        if self.supabase:
            payload = {
                "signal_id": str(signal_id),
                "timestamp": iso_timestamp,
                "symbol": symbol if symbol != "UNKNOWN" else "UNKNOWN",
                "predicted_direction": str(direction).upper(),
                "price_at_prediction": float(price),
                "ai_confidence": float(confidence),
                "market_regime": str(market_regime),
                "log_mlofi_z": float(log_mlofi_z),
                "hawkes_z": float(hawkes_z),
                "sector_impulse": float(sector_impulse),
                "swd_z": float(swd_z),
                "accel_z": float(accel_z),
                "micro_dislocation_z": float(micro_dislocation_z),
                "hurst_h": float(hurst_h),
                "bocd_cp_prob": float(bocd_cp_prob),
                "ou_divergence_z": float(ou_divergence_z),
                "cvd_z": float(cvd_z),
                "vol_mult": float(vol_mult),
                "spread": float(spread),
                "kelly_fraction": float(kelly_fraction),
                "conformal_gate": float(conformal_gate),
                "virtual_sl": sl_price,
                "virtual_tp": tp_price,
                "target_notional": target_notional,
                "is_shadow": is_shadow,
                "execution_mode": "SHADOW" if is_shadow else str(features.get("execution_mode", "LIVE")),
                "resolved": False,
                "fees_usdt": 0.0,
                "funding_usdt": 0.0,
                "leverage": 1.0,
                "holding_minutes": 0.0,
                "tca_entry_slippage_bps": 0.0,
                "tca_exit_slippage_bps": 0.0,
                "tca_total_slippage_bps": 0.0,
                "exec_details": {}
            }

            try:
                self.write_queue.put_nowait(("INSERT", "quantitative_ledger", payload, None, None))
            except asyncio.QueueFull:
                logger.error(f"❌ Write queue overflow. Dropping signal {signal_id[:8]}")

        label = "🦇 SHADOW" if is_shadow else "💾 CORE"
        logger.info(f"[X-RAY] {label} LEDGER ROUTED // ID: {signal_id[:8]}... | {symbol} | SL: {sl_price:.4f} | TP: {tp_price:.4f}")

    async def log_live_execution_result(
        self, 
        signal_id: str, 
        net_pnl: float, 
        slippage: float, 
        outcome: str, 
        execution_details: Optional[Dict[str, Any]] = None
    ):
        """Resolves live trade outcomes with Transaction Cost Analysis (TCA) metrics across SQLite and Supabase."""
        is_correct = True if net_pnl > 0 else False
        if execution_details is None:
            execution_details = {}

        fees = float(execution_details.get("fees_usdt", 0.0))
        duration_minutes = 0.0
        row = None

        try:
            async with self._db_lock:
                self.local_cursor.execute("SELECT timestamp FROM quantitative_ledger WHERE signal_id = ?", (str(signal_id),))
                row = self.local_cursor.fetchone()

                if row:
                    start_dt = self._parse_iso_timestamp(row["timestamp"])
                    duration_minutes = (datetime.now(timezone.utc) - start_dt).total_seconds() / 60.0

                    # Update SQLite Fast-Path Replica
                    self.local_cursor.execute('''
                        UPDATE quantitative_ledger
                        SET resolved = 1, actual_outcome = ?, net_pnl = ?, is_correct = ?, fees_usdt = ?, slippage_drag = ?, holding_minutes = ?
                        WHERE signal_id = ?
                    ''', (str(outcome), float(net_pnl), 1 if is_correct else 0, fees, float(slippage), duration_minutes, str(signal_id)))
                    self.local_db.commit()

            if row and self.supabase and self.write_queue:
                update_payload = {
                    "resolved": True,
                    "actual_outcome": str(outcome),
                    "net_pnl": float(net_pnl),
                    "slippage_drag": float(slippage),
                    "is_correct": is_correct,
                    "tca_entry_slippage_bps": float(execution_details.get("tca_entry_slippage_bps", 0.0)),
                    "tca_exit_slippage_bps": float(execution_details.get("tca_exit_slippage_bps", 0.0)),
                    "tca_total_slippage_bps": float(execution_details.get("tca_total_slippage_bps", slippage)),
                    "fees_usdt": fees,
                    "funding_usdt": float(execution_details.get("funding_usdt", 0.0)),
                    "leverage": float(execution_details.get("leverage", 1.0)),
                    "execution_mode": str(execution_details.get("execution_mode", "LIVE")).upper(),
                    "holding_minutes": round(duration_minutes, 2),
                    "exec_details": execution_details
                }
                self.write_queue.put_nowait(("UPDATE", "quantitative_ledger", update_payload, "signal_id", str(signal_id)))
                logger.info(f"[X-RAY] 🎯 ATTRIBUTION DISPATCHED // Signal {signal_id[:8]}... PnL: ${net_pnl:.4f} | Total Slippage: {slippage:+.1f} bps")

        except Exception as e:
            logger.error(f"Database update route failed: {e}")

    async def resolve_batch_historical_predictions(
        self, 
        assets: List[str], 
        current_prices: Dict[str, Any], 
        age_cutoff: float, 
        interval_mins: float = 15.0
    ) -> int:
        """
        Resolves shadow signals against price history using the local SQLite queue.
        Constrained strictly to is_shadow = 1 to prevent overriding active live trades.
        """
        resolved_count = 0
        try:
            async with self._db_lock:
                self.local_cursor.execute('''
                    SELECT signal_id, timestamp, symbol, price_at_prediction, predicted_direction, virtual_sl, virtual_tp, target_notional
                    FROM quantitative_ledger
                    WHERE resolved = 0 AND is_shadow = 1
                    ORDER BY timestamp ASC
                    LIMIT 500
                ''')
                unresolved_rows = [dict(r) for r in self.local_cursor.fetchall()]

            if not unresolved_rows:
                return 0

            update_batch = []
            now_ts = datetime.now(timezone.utc)

            for row in unresolved_rows:
                symbol = row.get("symbol")
                entry_price = float(row["price_at_prediction"])
                prediction = str(row["predicted_direction"]).upper()

                sl_price = float(row.get("virtual_sl") or (entry_price * 0.99 if prediction == "BUY" else entry_price * 1.01))
                tp_price = float(row.get("virtual_tp") or (entry_price * 1.02 if prediction == "BUY" else entry_price * 0.98))
                p_data = current_prices.get(symbol)

                row_time = self._parse_iso_timestamp(row["timestamp"])
                elapsed_minutes = (now_ts - row_time).total_seconds() / 60.0

                if p_data is None:
                    if elapsed_minutes >= 60.0:
                        row.update({
                            "resolved": True, "actual_outcome": "TIMEOUT", "is_correct": False,
                            "net_pnl": 0.0, "holding_minutes": round(elapsed_minutes, 2)
                        })
                        update_batch.append(row)
                        resolved_count += 1
                    continue

                if isinstance(p_data, dict):
                    closes = p_data.get("prices", [])
                    highs = p_data.get("highs", p_data.get("prices", []))
                    lows = p_data.get("lows", p_data.get("prices", []))
                elif isinstance(p_data, (list, np.ndarray)):
                    closes = highs = lows = [float(p) for p in p_data]
                else:
                    continue

                if len(closes) == 0:
                    continue

                current_price = closes[-1]
                is_terminated = False
                exit_price = entry_price
                bars_held = 0

                tf_interval = max(1.0, float(interval_mins))
                bars_elapsed = int(math.ceil(elapsed_minutes / tf_interval))
                candles_to_check = max(1, min(len(closes), bars_elapsed + 2))
                start_index = max(0, len(closes) - candles_to_check)

                highs_arr = np.array(highs[start_index:])
                lows_arr = np.array(lows[start_index:])

                if prediction == "BUY":
                    tp_hits = np.where(highs_arr >= tp_price)[0]
                    sl_hits = np.where(lows_arr <= sl_price)[0]
                elif prediction == "SELL":
                    tp_hits = np.where(lows_arr <= tp_price)[0]
                    sl_hits = np.where(highs_arr >= sl_price)[0]
                else:
                    tp_hits = sl_hits = np.array([])

                first_tp_idx = tp_hits[0] if len(tp_hits) > 0 else float('inf')
                first_sl_idx = sl_hits[0] if len(sl_hits) > 0 else float('inf')

                if first_tp_idx != float('inf') or first_sl_idx != float('inf'):
                    is_terminated = True
                    if first_sl_idx <= first_tp_idx:
                        exit_price, bars_held = sl_price, int(first_sl_idx)
                    else:
                        exit_price, bars_held = tp_price, int(first_tp_idx)

                if not is_terminated and elapsed_minutes >= 60.0:
                    is_terminated, exit_price, bars_held = True, current_price, len(highs_arr)

                if is_terminated:
                    is_win = (prediction == "BUY" and exit_price > entry_price) or (prediction == "SELL" and exit_price < entry_price)
                    entry_price_safe = entry_price if entry_price > 0 else 1e-9
                    sl_distance_pct = max(0.005, abs(sl_price - entry_price_safe) / entry_price_safe)
                    simulated_leverage = max(1.0, min(5.0, float(math.floor(1.0 / (sl_distance_pct * 1.5)))))

                    gross_return = abs(exit_price - entry_price_safe) / entry_price_safe
                    if not is_win:
                        gross_return = -gross_return

                    net_pnl = (gross_return - 0.0011) * simulated_leverage

                    row.update({
                        "resolved": True,
                        "actual_outcome": "WIN" if is_win else "LOSS",
                        "is_correct": is_win,
                        "net_pnl": float(net_pnl),
                        "leverage": float(simulated_leverage),
                        "holding_minutes": round(min(elapsed_minutes, float(bars_held * interval_mins)), 2)
                    })
                    update_batch.append(row)
                    resolved_count += 1

            if update_batch:
                async with self._db_lock:
                    for row in update_batch:
                        self.local_cursor.execute('''
                            UPDATE quantitative_ledger
                            SET resolved = 1, actual_outcome = ?, net_pnl = ?, is_correct = ?, holding_minutes = ?
                            WHERE signal_id = ?
                        ''', (row["actual_outcome"], row["net_pnl"], 1 if row["is_correct"] else 0, row["holding_minutes"], row["signal_id"]))
                    self.local_db.commit()

                if self.supabase and self.write_queue:
                    chunk_size = 100
                    for i in range(0, len(update_batch), chunk_size):
                        chunk = update_batch[i:i + chunk_size]
                        self.write_queue.put_nowait(("UPSERT", "quantitative_ledger", chunk, None, None))
                    logger.info(f"[X-RAY] 📊 GHOST FORENSICS: Enqueued {len(update_batch)} paths for batched cloud sync.")

            return resolved_count

        except Exception as e:
            logger.error(f"Batch resolution fault: {e}")
            return 0

    async def evaluate_shadow_promotion(self, target_symbol: str, window_trades: int = 35) -> Dict[str, Any]:
        """Assesses shadow asset performance instantly via Local SQLite Fast-Path."""
        try:
            async with self._db_lock:
                self.local_cursor.execute('''
                    SELECT net_pnl, is_correct
                    FROM quantitative_ledger
                    WHERE resolved = 1 AND symbol = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                ''', (target_symbol, window_trades))
                rows = self.local_cursor.fetchall()

            if len(rows) < 35:
                return {
                    "should_promote": False, "should_demote": False, "shadow_sharpe": 0.0,
                    "shadow_win_rate": 0.50, "sample_count": len(rows),
                    "reason": f"Insufficient shadow samples ({len(rows)}/35 min)"
                }

            pnls = np.array([float(r["net_pnl"]) for r in rows])
            wins = sum(1 for r in rows if r["is_correct"])
            total = len(rows)
            win_rate = wins / total

            mean_pnl = np.mean(pnls)
            std_pnl = np.std(pnls) + 1e-9
            shadow_sharpe = float((mean_pnl / std_pnl) * math.sqrt(365.0)) if std_pnl > 1e-6 else 0.0

            should_promote = (win_rate >= 0.55) and (shadow_sharpe >= 1.5)
            should_demote = (win_rate < 0.45) or (shadow_sharpe < -0.5)

            reason = "STABLE"
            if should_promote:
                reason = f"PROMOTION TRIGGERED // Win Rate: {win_rate:.1%}, Sharpe: {shadow_sharpe:.2f}"
            elif should_demote:
                reason = f"DEMOTION TRIGGERED // Win Rate: {win_rate:.1%}, Sharpe: {shadow_sharpe:.2f}"

            return {
                "should_promote": should_promote, "should_demote": should_demote,
                "shadow_sharpe": round(shadow_sharpe, 2), "shadow_win_rate": round(win_rate, 4),
                "sample_count": total, "reason": reason
            }

        except Exception as e:
            return {
                "should_promote": False, "should_demote": False, "shadow_sharpe": 0.0,
                "shadow_win_rate": 0.50, "sample_count": 0, "reason": f"Evaluation exception: {e}"
            }

    async def compute_latent_dna_edge(self, current_dna: Dict[str, Any], k_neighbors: int = 30) -> Dict[str, Any]:
        """
        Computes k-NN Bayesian win probability instantly via Local SQLite Fast-Path.
        Arms trading on cold starts unless an asset explicitly fails promotion gating.
        """
        c_vol = min(float(current_dna.get("vol_mult", 1.0) or 1.0), 10.0)
        c_log_mlofi = float(current_dna.get("log_mlofi_z", 0.0) or 0.0)
        c_spread = float(current_dna.get("spread_pct", 0.001) or 0.001) * 1000.0
        target_symbol = current_dna.get("symbol", "UNKNOWN")

        vol_bucket = round(c_vol * 2.0) / 2.0
        mlofi_bucket = round(c_log_mlofi * 2.0) / 2.0
        spread_bucket = round(c_spread, 2)

        dna_hash = f"{target_symbol}_{vol_bucket}_{mlofi_bucket}_{spread_bucket}"
        current_time = time.time()

        if dna_hash in self.dna_cache:
            cached_time, cached_result = self.dna_cache[dna_hash]
            if current_time - cached_time < self.cache_ttl_seconds:
                return cached_result

        try:
            async with self._db_lock:
                self.local_cursor.execute('''
                    SELECT is_correct, vol_mult, log_mlofi_z, spread, price_at_prediction
                    FROM quantitative_ledger
                    WHERE resolved = 1 AND symbol = ?
                    ORDER BY timestamp DESC
                    LIMIT 2000
                ''', (target_symbol,))
                rows = self.local_cursor.fetchall()

            historical_data = [{
                "is_correct": bool(r["is_correct"]),
                "vol_mult": r["vol_mult"],
                "log_mlofi_z": r["log_mlofi_z"],
                "spread": r["spread"],
                "price_at_prediction": r["price_at_prediction"]
            } for r in rows]

            self._ingest_hologram_data(historical_data)
            promo_eval = await self.evaluate_shadow_promotion(target_symbol)

            # Cold-start resolution: Arm live trading unless the asset has explicitly triggered demotion
            if len(historical_data) < k_neighbors:
                is_armed_default = not promo_eval.get("should_demote", False)
                result_payload = {
                    "bayesian_edge": 0.55,
                    "is_armed": is_armed_default,
                    "matched_samples": len(historical_data),
                    "cluster_win_rate": 0.50,
                    "win_rate": 0.50,
                    "shadow_sharpe": promo_eval.get("shadow_sharpe", 0.0),
                    "promotion_event": "COLD_START_ARMED" if is_armed_default else "COLD_START_DISARMED"
                }
                self.dna_cache[dna_hash] = (current_time, result_payload)
                return result_payload

            h_vols = np.array([min(float(r["vol_mult"] or 1.0), 10.0) for r in historical_data])
            h_mlofis = np.array([float(r["log_mlofi_z"] or 0.0) for r in historical_data])
            h_spreads_raw = np.array([float(r["spread"] or 0.0) for r in historical_data])
            h_prices = np.array([float(r["price_at_prediction"] or 1.0) for r in historical_data])

            h_spreads = np.where(h_prices > 0, (h_spreads_raw / h_prices) * 1000.0, 0.001)
            h_outcomes = np.array([1.0 if r["is_correct"] else 0.0 for r in historical_data])

            std_vol = np.std(h_vols) + 1e-9
            std_mlofi = np.std(h_mlofis) + 1e-9
            std_spread = np.std(h_spreads) + 1e-9

            norm_vol = (c_vol - h_vols) / std_vol
            norm_mlofi = (c_log_mlofi - h_mlofis) / std_mlofi
            norm_spread = (c_spread - h_spreads) / std_spread

            distances_sq = (1.5 * norm_vol)**2 + (2.0 * norm_mlofi)**2 + (1.0 * norm_spread)**2

            k_actual = min(k_neighbors, len(historical_data))
            nearest_idx = np.argpartition(distances_sq, k_actual - 1)[:k_actual]

            wins = np.sum(h_outcomes[nearest_idx])
            total = k_actual

            bayesian_edge = (wins + 2.0) / (total + 4.0)
            is_armed = (bayesian_edge >= 0.52) or promo_eval["should_promote"]
            if promo_eval["should_demote"] and not promo_eval["should_promote"]:
                is_armed = False

            win_rate_calc = round(float(wins / total), 4) if total > 0 else 0.50

            promotion_event = "STABLE"
            if promo_eval["should_promote"]:
                promotion_event = "PROMOTED_FROM_SHADOW"
            elif promo_eval["should_demote"]:
                promotion_event = "DEMOTED_TO_SHADOW"

            result_payload = {
                "bayesian_edge": round(float(bayesian_edge), 4),
                "is_armed": is_armed,
                "matched_samples": int(total),
                "cluster_win_rate": win_rate_calc,
                "win_rate": win_rate_calc,
                "shadow_sharpe": promo_eval["shadow_sharpe"],
                "promotion_event": promotion_event
            }

            self.dna_cache[dna_hash] = (current_time, result_payload)
            return result_payload

        except Exception as e:
            logger.error(f"[X-RAY] 🛑 LOCAL DB FAULT: SQLite lookup failed ({e}). Engaging HOLOGRAPHIC FALLBACK.")

            # Safe cold-start arming on holographic fallback
            if not self.holo_warmed_up or self.holo_pointer == 0:
                logger.info("[X-RAY] ⚡ Hologram uninitialized. Defaulting to armed for live execution.")
                return {
                    "bayesian_edge": 0.55, 
                    "is_armed": True, 
                    "matched_samples": 0,
                    "cluster_win_rate": 0.50, 
                    "win_rate": 0.50, 
                    "shadow_sharpe": 0.0,
                    "promotion_event": "COLD_START_ARMED_SAFE"
                }

            active_size = min(self.holo_pointer, self.holo_capacity)
            f_view = self.holo_features[:active_size]

            std_vol = np.std(f_view[:, 0]) + 1e-9
            std_mlofi = np.std(f_view[:, 1]) + 1e-9
            std_spread = np.std(f_view[:, 2]) + 1e-9

            norm_vol = (c_vol - f_view[:, 0]) / std_vol
            norm_mlofi = (c_log_mlofi - f_view[:, 1]) / std_mlofi
            norm_spread = (c_spread - f_view[:, 2]) / std_spread

            distances_sq = (1.5 * norm_vol)**2 + (2.0 * norm_mlofi)**2 + (1.0 * norm_spread)**2

            k_actual = min(k_neighbors, active_size)
            nearest_idx = np.argpartition(distances_sq, k_actual - 1)[:k_actual]

            k_outcomes = self.holo_outcomes[nearest_idx]
            wins = np.sum(k_outcomes)
            total = k_actual

            bayesian_edge = (wins + 2.0) / (total + 4.0)
            is_armed = bool(bayesian_edge >= 0.52)

            logger.info(f"[X-RAY] 🌌 HOLOGRAPHIC SURVIVAL // Local Edge: {bayesian_edge:.2%} | Armed: {is_armed}")

            return {
                "bayesian_edge": round(float(bayesian_edge), 4),
                "is_armed": is_armed,
                "matched_samples": int(total),
                "cluster_win_rate": round(float(wins / total), 4) if total > 0 else 0.5,
                "win_rate": round(float(wins / total), 4) if total > 0 else 0.5,
                "shadow_sharpe": 0.0,
                "promotion_event": "HOLOGRAPHIC_SURVIVAL"
            }

    async def get_forensic_execution_summary(self, today_iso_start: str) -> Dict[str, Any]:
        """Queries today's executed trades via the fast-path SQLite covering index."""
        try:
            async with self._db_lock:
                self.local_cursor.execute('''
                    SELECT net_pnl, fees_usdt, slippage_drag, holding_minutes, is_correct, symbol
                    FROM quantitative_ledger
                    WHERE resolved = 1 AND is_shadow = 0 AND timestamp >= ?
                ''', (today_iso_start,))
                rows = self.local_cursor.fetchall()

            if not rows:
                return {
                    "trade_count": 0, "net_pnl": 0.0, "fees_paid": 0.0,
                    "avg_slippage_bps": 0.0, "avg_holding_mins": 0.0, "win_rate": 0.0
                }

            pnls = [float(r["net_pnl"]) for r in rows]
            fees = [float(r["fees_usdt"]) for r in rows]
            slips = [float(r["slippage_drag"]) for r in rows]
            durations = [float(r["holding_minutes"]) for r in rows]
            wins = sum(1 for r in rows if r["is_correct"])

            return {
                "trade_count": len(rows),
                "net_pnl": round(sum(pnls), 4),
                "fees_paid": round(sum(fees), 4),
                "avg_slippage_bps": round(float(np.mean(slips)), 2) if slips else 0.0,
                "avg_holding_mins": round(float(np.mean(durations)), 1) if durations else 0.0,
                "win_rate": round(wins / len(rows), 4)
            }

        except Exception as e:
            logger.debug(f"[X-RAY] Forensic summary fetch failed: {e}")
            return {
                "trade_count": 0, "net_pnl": 0.0, "fees_paid": 0.0,
                "avg_slippage_bps": 0.0, "avg_holding_mins": 0.0, "win_rate": 0.0
            }