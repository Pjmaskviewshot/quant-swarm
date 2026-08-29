"""
💎 V25.0 APEX QUANTUM PRIME: PURE-ASYNC MEMORY LEDGER
----------------------------------------------------------------
Hyper-optimized Supabase connector featuring:
- 100% Non-blocking Cloud execution via Asyncio Event Loop Offloading
- Guaranteed Data Retention via Synchronous Queue Flushing on Shutdown
- Holographic Memory Fallback (Zero-Downtime NumPy Local Tensor Matrix)
- Pure NumPy vectorization for shadow OHLC forensics and KNN Distance

Architectural Upgrades (V25.0):
- Purged all local SQLite (`quant_memory.db`) dependencies to enforce a Single Source of Truth (SSOT).
- Aligned payload schema with the new 18-D Volterra-Hermite Tensor features (`log_mlofi_z`, `hawkes_z`, `sector_impulse`).
"""

import os
import time
import math
import logging
import asyncio
import numpy as np
from datetime import datetime, timezone
from typing import Tuple, List, Dict, Any, Optional
from supabase import create_client, Client

logger = logging.getLogger("QUANT_CORE.MEMORY")

class MemoryBank:
    """
    🚀 V25.0 PURE-ASYNC FORENSIC LEDGER
    Serves as the ultimate forensic ledger and probabilistic memory engine.
    Ensures zero high-frequency loop starvation by offloading all I/O.
    """
    def __init__(self, db_path: str = None):
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        
        if not url or not key:
            logger.critical("❌ DB CONFIGURATION FAULT: SUPABASE_URL or SUPABASE_KEY missing.")
            raise ValueError("Missing Supabase credentials in environment variables.")
            
        try:
            self.supabase: Client = create_client(url, key)
            logger.info("🛸 CLOUD LEDGER BOUND: Connected successfully to Supabase cluster.")
        except Exception as e:
            logger.critical(f"❌ CONNECTION BOUND FAULT: Could not initialize Supabase client: {e}", exc_info=True)
            raise

        self.dna_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {} 
        self.cache_ttl_seconds: float = 120.0 
        
        # 🚀 HOLOGRAPHIC MEMORY MATRIX
        self.holo_capacity = 25000
        self.holo_features = np.zeros((self.holo_capacity, 3), dtype=np.float32)
        self.holo_outcomes = np.zeros(self.holo_capacity, dtype=np.float32)
        self.holo_pointer = 0
        self.holo_warmed_up = False

        # 🚀 ASYNC WRITE BUFFER STATE
        self.write_queue: Optional[asyncio.Queue] = None
        self._bg_task: Optional[asyncio.Task] = None
        self._is_shutting_down = False

    async def start(self):
        """Initializes the async write queue and background daemon within the event loop."""
        self.write_queue = asyncio.Queue(maxsize=50000)
        self._is_shutting_down = False
        self._bg_task = asyncio.create_task(self._async_sync_worker())
        logger.info("🛡️ ASYNC BACKGROUND SYNC WORKER ONLINE: DB writes decoupled.")

    async def flush_and_close(self):
        """
        Forces immediate execution of all pending writes to prevent data loss.
        Replaces the broken 'poison pill' pattern.
        """
        logger.info("⏳ Halting async DB worker and flushing forensic ledger...")
        self._is_shutting_down = True
        if self._bg_task:
            self._bg_task.cancel()
            
        if not self.write_queue:
            return

        pending_tasks = []
        while not self.write_queue.empty():
            try:
                task = self.write_queue.get_nowait()
                if task is not None:
                    pending_tasks.append(task)
            except asyncio.QueueEmpty:
                break

        if pending_tasks:
            logger.info(f"💾 Flushing {len(pending_tasks)} pending execution records to cloud...")
            for task in pending_tasks:
                try:
                    await self._process_task(task)
                except Exception as e:
                    logger.error(f"Flush execution error: {e}")
            logger.info("✅ Cloud ledger sync complete.")

    async def _async_sync_worker(self):
        """Processes database mutations asynchronously without blocking the HFT loop."""
        while not self._is_shutting_down:
            try:
                task = await self.write_queue.get()
                if task is None: 
                    break
                await self._process_task(task)
                self.write_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[X-RAY] Async background sync failed: {e}")
                await asyncio.sleep(1.0) 

    async def _process_task(self, task: Tuple):
        """Executes the specific Supabase mutation via a background thread context."""
        op_type, table, payload, match_col, match_val = task
        query = None
        
        if op_type == "INSERT":
            query = self.supabase.table(table).insert(payload)
        elif op_type == "UPDATE":
            query = self.supabase.table(table).update(payload).eq(match_col, match_val)
        elif op_type == "UPSERT":
            query = self.supabase.table(table).upsert(payload)
            
        if query:
            await self._safe_execute_async(query)

    async def _safe_execute_async(self, query_builder, max_retries: int = 2):
        """Wraps synchronous Supabase SDK calls in to_thread to prevent event loop blocking."""
        for attempt in range(max_retries):
            try:
                return await asyncio.to_thread(query_builder.execute)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.debug(f"[X-RAY] Supabase fault absorbed after {max_retries} attempts: {e}")
                    raise Exception(f"Supabase fault after {max_retries} attempts: {e}")
                await asyncio.sleep(0.05)

    def _ingest_hologram_data(self, rows: List[Dict[str, Any]]):
        """Organically feeds the local Hologram with verified cloud resolutions."""
        if not rows: return
        
        for r in rows:
            idx = self.holo_pointer % self.holo_capacity
            self.holo_features[idx, 0] = min(float(r.get("vol_mult", 1.0)), 10.0)
            # V25.0 FIX: Map to new log_mlofi_z vector instead of legacy z_obi
            self.holo_features[idx, 1] = float(r.get("log_mlofi_z", 0.0))
            
            h_price = float(r.get("price_at_prediction", 1.0))
            h_spread_raw = float(r.get("spread", 0.0))
            self.holo_features[idx, 2] = (h_spread_raw / h_price) * 1000 if h_price > 0 else 0.001
            
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
        if not self.write_queue: return
        if features is None: features = {}
            
        market_regime = features.get("market_regime", "UNKNOWN")
        # 🚀 V25.0 FIX: Support new Advanced Schema definitions
        log_mlofi_z = features.get("log_mlofi_z", 0.0)
        hawkes_z = features.get("hawkes_z", 0.0)
        sector_impulse = features.get("sector_impulse", 0.0)
        
        vol_mult = features.get("liquidity_density_ratio", 1.0)
        spread = features.get("bid_ask_spread", 0.0)
        symbol = features.get("symbol", "UNKNOWN")
        
        sl_price = float(features.get("virtual_sl", price * 0.99))
        tp_price = float(features.get("virtual_tp", price * 1.015))
        iso_timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()

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
            "vol_mult": float(vol_mult),
            "spread": float(spread),
            "resolved": False,
            "virtual_sl": sl_price,  
            "virtual_tp": tp_price,  
            "is_shadow": is_shadow,
            "fees_usdt": 0.0,
            "funding_usdt": 0.0,
            "leverage": 1.0,
            "holding_minutes": 0.0,
            "execution_mode": "SHADOW" if is_shadow else "LIVE"
        }

        try:
            self.write_queue.put_nowait(("INSERT", "quantitative_ledger", payload, None, None))
            label = "🦇 SHADOW" if is_shadow else "💾 CORE"
            logger.info(f"[X-RAY] {label} LEDGER ROUTED TO QUEUE // ID: {signal_id[:8]}... | Node: {symbol} | SL: {sl_price:.4f} | TP: {tp_price:.4f}")
        except asyncio.QueueFull:
            logger.error(f"❌ Write queue overflow. Dropping signal {signal_id[:8]}")

    async def log_live_execution_result(
        self, 
        signal_id: str, 
        net_pnl: float, 
        slippage: float, 
        outcome: str, 
        execution_details: Optional[Dict[str, Any]] = None
    ):
        if not self.write_queue: return
        is_correct = True if net_pnl > 0 else False
        if execution_details is None: execution_details = {}
            
        try:
            query = self.supabase.table("quantitative_ledger").select("timestamp").eq("signal_id", str(signal_id))
            response = await self._safe_execute_async(query)
            
            if response and response.data:
                start_dt = self._parse_iso_timestamp(response.data[0]["timestamp"])
                duration = (datetime.now(timezone.utc) - start_dt).total_seconds() / 60.0
                
                update_payload = {
                    "resolved": True,
                    "actual_outcome": str(outcome),
                    "net_pnl": float(net_pnl),
                    "slippage_drag": float(slippage),
                    "is_correct": is_correct,
                    "fees_usdt": float(execution_details.get("fees_usdt", 0.0)),
                    "funding_usdt": float(execution_details.get("funding_usdt", 0.0)),
                    "leverage": float(execution_details.get("leverage", 1.0)),
                    "execution_mode": str(execution_details.get("execution_mode", "LIVE")).upper(),
                    "holding_minutes": round(duration, 2)
                }
                
                self.write_queue.put_nowait(("UPDATE", "quantitative_ledger", update_payload, "signal_id", str(signal_id)))
                logger.info(f"[X-RAY] 🎯 ATTRIBUTION DISPATCHED // Signal {signal_id[:8]}... PnL: ${net_pnl:.4f}")
            else:
                logger.warning(f"[X-RAY] ⚠️ Live execution completed but no initial signal found for ID: {signal_id}")
                
        except Exception as e:
            logger.error(f"Database update route failed: {e}")

    async def resolve_batch_historical_predictions(
        self, 
        assets: List[str], 
        current_prices: Dict[str, Any], 
        age_cutoff: float, 
        interval_mins: float = 15.0
    ) -> int:
        if not self.write_queue: return 0
        resolved_count = 0

        try:
            query = self.supabase.table("quantitative_ledger").select("*").eq("resolved", False).order("timestamp", desc=False).limit(500)
            response = await self._safe_execute_async(query)

            unresolved_rows = response.data if response else []
            if not unresolved_rows: return 0

            update_batch = []
            now_ts = datetime.now(timezone.utc)

            for row in unresolved_rows:
                symbol = row.get("symbol")
                entry_price = float(row["price_at_prediction"])
                prediction = str(row["predicted_direction"]).upper()
                
                sl_price = float(row.get("virtual_sl", entry_price * 0.99))
                tp_price = float(row.get("virtual_tp", entry_price * 1.015))
                p_data = current_prices.get(symbol)
                
                row_time = self._parse_iso_timestamp(row["timestamp"])
                elapsed_minutes = (now_ts - row_time).total_seconds() / 60.0
                
                if p_data is None:
                    if elapsed_minutes >= 60.0:
                        row["resolved"] = True
                        row["actual_outcome"] = "TIMEOUT"
                        row["is_correct"] = False
                        row["net_pnl"] = 0.0
                        row["holding_minutes"] = round(elapsed_minutes, 2)
                        update_batch.append(row)
                        resolved_count += 1
                    continue

                if isinstance(p_data, dict):
                    closes, highs, lows = p_data.get("prices", []), p_data.get("highs", p_data.get("prices", [])), p_data.get("lows", p_data.get("prices", []))
                elif isinstance(p_data, (list, np.ndarray)):
                    closes = highs = lows = [float(p) for p in p_data]
                else:
                    continue

                if len(closes) == 0: continue

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
                    if not is_win: gross_return = -gross_return
                        
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
                chunk_size = 100
                for i in range(0, len(update_batch), chunk_size):
                    chunk = update_batch[i:i + chunk_size]
                    self.write_queue.put_nowait(("UPSERT", "quantitative_ledger", chunk, None, None))
                logger.info(f"[X-RAY] 📊 GHOST FORENSICS: Dispatched {len(update_batch)} paths to sync queue.")
                
            return resolved_count

        except Exception as e:
            logger.error(f"Batch resolution fault: {e}")
            return 0

    async def evaluate_shadow_promotion(self, target_symbol: str, window_trades: int = 35) -> Dict[str, Any]:
        try:
            query = (
                self.supabase.table("quantitative_ledger")
                .select("net_pnl, is_correct, actual_outcome")
                .eq("resolved", True)
                .eq("symbol", target_symbol)
                .order("timestamp", desc=True)
                .limit(window_trades)
            )
            response = await self._safe_execute_async(query)
            data = response.data if response else []

            if len(data) < 35:
                return {
                    "should_promote": False, "should_demote": False, "shadow_sharpe": 0.0,
                    "shadow_win_rate": 0.50, "sample_count": len(data),
                    "reason": f"Insufficient shadow samples ({len(data)}/35 min)"
                }

            pnls = np.array([float(r.get("net_pnl", 0.0)) for r in data])
            wins = sum(1 for r in data if r.get("is_correct") is True)
            total = len(data)
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
        c_vol = min(float(current_dna.get("vol_mult", 1.0)), 10.0) 
        # V25.0 FIX: Map new schema explicitly
        c_log_mlofi = float(current_dna.get("log_mlofi_z", 0.0))
        c_spread = float(current_dna.get("spread_pct", 0.001)) * 1000 
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
            query = (
                self.supabase.table("quantitative_ledger")
                # V25.0 FIX: Extract log_mlofi_z instead of z_obi
                .select("is_correct, vol_mult, log_mlofi_z, spread, price_at_prediction")
                .eq("resolved", True)
                .eq("symbol", target_symbol)
                .order("timestamp", desc=True)
                .limit(2000)
            )
            
            response = await self._safe_execute_async(query)
            historical_data = response.data if response else []
            
            self._ingest_hologram_data(historical_data)
            promo_eval = await self.evaluate_shadow_promotion(target_symbol)
            
            if len(historical_data) < k_neighbors:
                is_armed_default = promo_eval["should_promote"]
                return {
                    "bayesian_edge": 0.50, "is_armed": is_armed_default, 
                    "matched_samples": len(historical_data), "cluster_win_rate": 0.50,
                    "win_rate": 0.50, "shadow_sharpe": promo_eval["shadow_sharpe"],
                    "promotion_event": "PROMOTED" if is_armed_default else "INSUFFICIENT_DATA"
                }

            h_vols = np.array([min(float(r.get("vol_mult", 1.0)), 10.0) for r in historical_data])
            h_mlofis = np.array([float(r.get("log_mlofi_z", 0.0)) for r in historical_data])
            h_spreads_raw = np.array([float(r.get("spread", 0.0)) for r in historical_data])
            h_prices = np.array([float(r.get("price_at_prediction", 1.0)) for r in historical_data])
            
            h_spreads = np.where(h_prices > 0, (h_spreads_raw / h_prices) * 1000, 0.001)
            h_outcomes = np.array([1.0 if r.get("is_correct") else 0.0 for r in historical_data])
            
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
            is_armed = (bayesian_edge >= 0.55) or promo_eval["should_promote"]
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
            logger.error(f"[X-RAY] 🛑 CLOUD DISCONNECT: Supabase fault ({e}). Engaging HOLOGRAPHIC FALLBACK.")
            
            if not self.holo_warmed_up:
                logger.error("[X-RAY] 💀 Hologram not warmed up yet. Executing STRICT FAIL-CLOSED.")
                return {
                    "bayesian_edge": 0.0, "is_armed": False, "matched_samples": 0, 
                    "cluster_win_rate": 0.0, "win_rate": 0.0, "shadow_sharpe": 0.0,
                    "promotion_event": "CLOUD_FAULT_VETO"
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
            is_armed = bayesian_edge >= 0.55
            
            logger.info(f"[X-RAY] 🌌 HOLOGRAPHIC SURVIVAL // Local Edge Computed: {bayesian_edge:.2%}")
            
            return {
                "bayesian_edge": round(float(bayesian_edge), 4),
                "is_armed": bool(is_armed),
                "matched_samples": int(total),
                "cluster_win_rate": round(float(wins/total), 4) if total > 0 else 0.5,
                "win_rate": round(float(wins/total), 4) if total > 0 else 0.5,
                "shadow_sharpe": 0.0,
                "promotion_event": "HOLOGRAPHIC_SURVIVAL"
            }

    async def get_forensic_execution_summary(self, today_iso_start: str) -> Dict[str, Any]:
        try:
            query = (
                self.supabase.table("quantitative_ledger")
                .select("net_pnl, fees_usdt, slippage_drag, holding_minutes, is_correct, symbol")
                .eq("resolved", True)
                .eq("is_shadow", False)
                .gte("timestamp", today_iso_start)
            )
            res = await self._safe_execute_async(query)
            rows = res.data if res else []

            if not rows:
                return {
                    "trade_count": 0, "net_pnl": 0.0, "fees_paid": 0.0,
                    "avg_slippage_bps": 0.0, "avg_holding_mins": 0.0, "win_rate": 0.0
                }

            pnls = [float(r.get("net_pnl", 0.0)) for r in rows]
            fees = [float(r.get("fees_usdt", 0.0)) for r in rows]
            slips = [float(r.get("slippage_drag", 0.0)) for r in rows]
            durations = [float(r.get("holding_minutes", 0.0)) for r in rows]
            wins = sum(1 for r in rows if r.get("is_correct") is True)

            return {
                "trade_count": len(rows),
                "net_pnl": round(sum(pnls), 4),
                "fees_paid": round(sum(fees), 4),
                "avg_slippage_bps": round(float(np.mean(slips)) * 10000.0, 2) if slips else 0.0,
                "avg_holding_mins": round(float(np.mean(durations)), 1) if durations else 0.0,
                "win_rate": round(wins / len(rows), 4)
            }

        except Exception as e:
            logger.debug(f"[X-RAY] Forensic summary fetch failed: {e}")
            return {
                "trade_count": 0, "net_pnl": 0.0, "fees_paid": 0.0,
                "avg_slippage_bps": 0.0, "avg_holding_mins": 0.0, "win_rate": 0.0
            }