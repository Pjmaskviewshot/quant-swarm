import os
import time
import math
import logging
import numpy as np
from datetime import datetime, timezone
from typing import Tuple, List, Dict, Any
from supabase import create_client, Client

logger = logging.getLogger("QUANT_CORE.MEMORY")

class MemoryBank:
    """
    🌌 V19.3 GENESIS: ATOMIC MEMORY LEDGER
    Hyper-optimized Supabase connector. Single-trip atomic updates.
    Standardized KNN Feature Matrix.
    """
    def __init__(self, db_path: str = None):
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        
        if not url or not key:
            logger.critical("❌ DB CONFIGURATION FAULT: SUPABASE_URL or SUPABASE_KEY environment variables missing.")
            raise ValueError("Missing Supabase credentials in environment variables.")
            
        try:
            self.supabase: Client = create_client(url, key)
            logger.info("🛰️ CLOUD LEDGER BOUND: Connected successfully to Supabase cluster.")
        except Exception as e:
            logger.critical(f"❌ CONNECTION BOUND FAULT: Could not initialize Supabase client: {e}", exc_info=True)
            raise

        self.dna_cache = {} 
        self.cache_ttl_seconds = 120.0 

    def _safe_execute(self, query_builder, max_retries: int = 3, base_delay: float = 1.0):
        for attempt in range(max_retries):
            try:
                return query_builder.execute()
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"❌ SUPABASE FATAL: Operation failed permanently after {max_retries} attempts. {e}", exc_info=True)
                    raise e
                
                sleep_time = base_delay * (1.5 ** attempt)
                logger.warning(f"⚠️ Supabase connection transient fault. Retrying in {sleep_time:.2f}s... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(sleep_time)

    def _parse_iso_timestamp(self, ts_str: str) -> datetime:
        if ts_str.endswith('Z'):
            ts_str = ts_str.replace('Z', '+00:00')
        return datetime.fromisoformat(ts_str)

    def commit_prediction(self, signal_id: str, timestamp: float, price: float, direction: str, confidence: float, features: Dict[str, Any] = None, is_shadow: bool = False):
        if features is None:
            features = {}
            
        market_regime = features.get("market_regime", "UNKNOWN")
        z_obi = features.get("adaptive_obi_z", 0.0)
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
            "z_obi": float(z_obi),
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
            self._safe_execute(self.supabase.table("quantitative_ledger").insert(payload))
            label = "🦇 SHADOW" if is_shadow else "💾 CORE"
            logger.info(f"{label} LEDGER COMMIT // ID: {signal_id[:8]}... | Node: {symbol} | SL: {sl_price:.4f} | TP: {tp_price:.4f}")
        except Exception as e:
            logger.error(f"❌ DATABASE INSERT TRANSACTION EXCEPTION for signal {signal_id}: {e}", exc_info=True)

    def log_live_execution_result(self, signal_id: str, net_pnl: float, slippage: float, outcome: str, execution_details: Dict[str, Any] = None):
        is_correct = True if net_pnl > 0 else False
        if execution_details is None:
            execution_details = {}
            
        try:
            response = self._safe_execute(self.supabase.table("quantitative_ledger").select("timestamp").eq("signal_id", str(signal_id)))
            
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
                
                update_res = self._safe_execute(self.supabase.table("quantitative_ledger").update(update_payload).eq("signal_id", str(signal_id)))
                
                if update_res and update_res.data:
                    logger.info(f"🎯 ATTRIBUTION MATCHED & VERIFIED // Signal {signal_id[:8]}... updated with PnL: ${net_pnl:.4f} | Mode: {update_payload['execution_mode']}")
                else:
                    logger.error(f"❌ VERIFICATION FAILED: Ledger rejected update for signal {signal_id}")
            else:
                logger.warning(f"⚠️ Live execution completed but no initial signal found in ledger for ID: {signal_id}")
                
        except Exception as e:
            logger.error(f"❌ DATABASE UPDATE TRANSACTION EXCEPTION for signal {signal_id}: {e}", exc_info=True)

    def resolve_batch_historical_predictions(self, assets: List[str], current_prices: Dict[str, Any], age_cutoff: float) -> int:
        resolved_count = 0

        try:
            query = self.supabase.table("quantitative_ledger").select("*").eq("resolved", False)
            response = self._safe_execute(query.order("timestamp", desc=False).limit(500))

            unresolved_rows = response.data if response else []
            if not unresolved_rows:
                return 0

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
                    closes = p_data.get("prices", [])
                    highs = p_data.get("highs", closes)
                    lows = p_data.get("lows", closes)
                elif isinstance(p_data, (list, np.ndarray)):
                    closes = [float(p) for p in p_data]
                    highs = closes
                    lows = closes
                else:
                    continue

                if len(closes) == 0:
                    continue

                current_price = closes[-1]
                is_terminated = False
                exit_price = entry_price
                bars_held = 0

                candles_to_check = max(1, int(elapsed_minutes) + 2) 
                start_index = max(0, len(closes) - candles_to_check)

                for idx, i in enumerate(range(start_index, len(closes))):
                    high = highs[i]
                    low = lows[i]
                    bars_held = idx

                    if prediction == "BUY":
                        hit_tp = high >= tp_price
                        hit_sl = low <= sl_price
                        if hit_tp and hit_sl:
                            is_terminated, exit_price = True, sl_price
                            break
                        elif hit_tp:
                            is_terminated, exit_price = True, tp_price
                            break
                        elif hit_sl:
                            is_terminated, exit_price = True, sl_price
                            break
                    elif prediction == "SELL":
                        hit_tp = low <= tp_price
                        hit_sl = high >= sl_price
                        if hit_tp and hit_sl:
                            is_terminated, exit_price = True, sl_price
                            break
                        elif hit_tp:
                            is_terminated, exit_price = True, tp_price
                            break
                        elif hit_sl:
                            is_terminated, exit_price = True, sl_price
                            break

                if not is_terminated and elapsed_minutes >= 60.0:
                    is_terminated = True
                    exit_price = current_price

                if is_terminated:
                    simulated_leverage = max(5.0, min(15.0, 5.0 + (abs(row.get("z_obi", 0.0)) * 2.0)))
                    TAKER_ROUND_TRIP = 0.0011
                    
                    gross_return = abs(exit_price - entry_price) / entry_price
                    
                    is_win = False
                    if prediction == "BUY" and exit_price > entry_price:
                        is_win = True
                    elif prediction == "SELL" and exit_price < entry_price:
                        is_win = True
                        
                    if not is_win:
                        gross_return = -gross_return
                        
                    net_pnl = (gross_return - TAKER_ROUND_TRIP) * simulated_leverage

                    row["resolved"] = True
                    row["actual_outcome"] = "WIN" if is_win else "LOSS"
                    row["is_correct"] = is_win
                    row["net_pnl"] = float(net_pnl)
                    row["holding_minutes"] = round(min(elapsed_minutes, float(bars_held)), 2)
                    
                    update_batch.append(row)
                    resolved_count += 1
                
            if update_batch:
                self._safe_execute(self.supabase.table("quantitative_ledger").upsert(update_batch))
                logger.info(f"📊 GHOST FORENSICS: Traversed and settled {len(update_batch)} predictive ledger paths.")
                
            return resolved_count

        except Exception as e:
            logger.error(f"❌ KINETIC RESOLUTION ENGINE FAILURE: {e}", exc_info=True)
            return 0

    def compute_latent_dna_edge(self, current_dna: Dict[str, float], k_neighbors: int = 30) -> Dict[str, Any]:
        c_vol = min(current_dna.get("vol_mult", 1.0), 10.0) 
        c_obi = current_dna.get("z_obi", 0.0)
        c_spread = current_dna.get("spread_pct", 0.001) * 1000 
        
        vol_bucket = round(c_vol * 2.0) / 2.0  
        obi_bucket = round(c_obi * 2.0) / 2.0  
        spread_bucket = round(c_spread, 2)
        
        dna_hash = f"{vol_bucket}_{obi_bucket}_{spread_bucket}"
        current_time = time.time()
        
        if dna_hash in self.dna_cache:
            cached_time, cached_result = self.dna_cache[dna_hash]
            if current_time - cached_time < self.cache_ttl_seconds:
                return cached_result

        try:
            query = self.supabase.table("quantitative_ledger")\
                .select("is_correct, vol_mult, z_obi, spread, price_at_prediction")\
                .eq("resolved", True)\
                .order("timestamp", desc=True)\
                .limit(2000)
                
            response = self._safe_execute(query)
            historical_data = response.data if response else []
            
            if len(historical_data) < k_neighbors:
                return {"bayesian_edge": 0.50, "is_armed": False, "matched_samples": len(historical_data), "cluster_win_rate": 0.50}

            distances = []
            for row in historical_data:
                h_vol = min(float(row.get("vol_mult", 1.0)), 10.0)
                h_obi = float(row.get("z_obi", 0.0))
                h_price = float(row.get("price_at_prediction", 1.0))
                h_spread_raw = float(row.get("spread", 0.0))
                h_spread_pct = (h_spread_raw / h_price) * 1000 if h_price > 0 else 0.001
                
                # 🚀 V19.3 FIX: STANDARDIZED EUCLIDEAN METRIC (Issue #14)
                # Prevents massive spread spikes from overpowering the OBI vector
                norm_vol = (c_vol - h_vol) / 2.0
                norm_obi = (c_obi - h_obi) / 1.5
                norm_spread = (c_spread - h_spread_pct) / 5.0
                
                dist = math.sqrt(
                    (1.5 * norm_vol)**2 + 
                    (2.0 * norm_obi)**2 + 
                    (1.0 * norm_spread)**2
                )
                
                distances.append({
                    "distance": dist,
                    "is_correct": 1.0 if row.get("is_correct") is True else 0.0
                })

            distances.sort(key=lambda x: x["distance"])
            nearest_neighbors = distances[:k_neighbors]
            
            wins = sum(n["is_correct"] for n in nearest_neighbors)
            total = len(nearest_neighbors)
            
            bayesian_edge = (wins + 2.0) / (total + 4.0)
            
            is_armed = bayesian_edge >= 0.55
            
            result_payload = {
                "bayesian_edge": round(bayesian_edge, 4),
                "is_armed": is_armed,
                "matched_samples": total,
                "cluster_win_rate": round(wins / total, 4)
            }
            
            self.dna_cache[dna_hash] = (current_time, result_payload)
            return result_payload

        except Exception as e:
            logger.error(f"❌ LATENT DNA ENGINE MATCHING FAILED: {e}", exc_info=True)
            return {"bayesian_edge": 0.50, "is_armed": False, "matched_samples": 0, "cluster_win_rate": 0.50}