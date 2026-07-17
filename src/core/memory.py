import os
import time
import logging
import numpy as np
from datetime import datetime, timezone
from typing import Tuple, List, Dict, Any
from supabase import create_client, Client

logger = logging.getLogger("QUANT_CORE.MEMORY")

class MemoryBank:
    def __init__(self, db_path: str = None):
        """
        Initializes the Cloud-Native Supabase Analytics Engine connection layer.
        """
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        
        if not url or not key:
            logger.critical("❌ DB CONFIGURATION FAULT: SUPABASE_URL or SUPABASE_KEY environment variables missing.")
            raise ValueError("Missing Supabase credentials in environment variables.")
            
        try:
            self.supabase: Client = create_client(url, key)
            logger.info("🛰️ CLOUD LEDGER BOUND: Connected successfully to Supabase cluster.")
        except Exception as e:
            logger.critical(f"❌ CONNECTION BOUND FAULT: Could not initialize Supabase client: {e}")
            raise

    def _safe_execute(self, query_builder, max_retries: int = 3, base_delay: float = 1.0):
        """
        🛡️ EXPONENTIAL BACKOFF WRAPPER
        Prevents transient cloud network drops from permanently blinding the quantitative ledger.
        """
        for attempt in range(max_retries):
            try:
                return query_builder.execute()
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"❌ SUPABASE FATAL: Operation failed permanently after {max_retries} attempts. {e}")
                    raise e
                
                sleep_time = base_delay * (1.5 ** attempt)
                logger.warning(f"⚠️ Supabase connection transient fault. Retrying in {sleep_time:.2f}s... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(sleep_time)

    def _parse_iso_timestamp(self, ts_str: str) -> datetime:
        """Robust ISO timestamp parser for Supabase datetimes."""
        if ts_str.endswith('Z'):
            ts_str = ts_str.replace('Z', '+00:00')
        return datetime.fromisoformat(ts_str)

    def commit_prediction(self, signal_id: str, timestamp: float, price: float, direction: str, confidence: float, features: Dict[str, Any] = None, is_shadow: bool = False):
        """Saves a fresh prediction cleanly using the dedicated schema columns."""
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
            "execution_mode": "GHOST"
        }

        try:
            self._safe_execute(self.supabase.table("quantitative_ledger").insert(payload))
            label = "🦇 SHADOW" if is_shadow else "💾 CORE"
            logger.info(f"{label} LEDGER COMMIT // ID: {signal_id[:8]}... | Node: {symbol} | SL: {sl_price:.4f} | TP: {tp_price:.4f}")
        except Exception as e:
            logger.error(f"❌ DATABASE INSERT TRANSACTION EXCEPTION for signal {signal_id}: {e}")

    def log_live_execution_result(self, signal_id: str, net_pnl: float, slippage: float, outcome: str, execution_details: Dict[str, Any] = None):
        """ Updates a live trade signal with actual financial execution data. """
        is_correct = True if net_pnl > 0 else False
        if execution_details is None:
            execution_details = {}
            
        try:
            response = self._safe_execute(self.supabase.table("quantitative_ledger").select("*").eq("signal_id", str(signal_id)))
            
            if response and response.data:
                row = response.data[0]
                row["resolved"] = True
                row["actual_outcome"] = str(outcome)
                row["net_pnl"] = float(net_pnl)
                row["slippage_drag"] = float(slippage)
                row["is_correct"] = is_correct
                
                row["fees_usdt"] = float(execution_details.get("fees_usdt", 0.0))
                row["funding_usdt"] = float(execution_details.get("funding_usdt", 0.0))
                row["leverage"] = float(execution_details.get("leverage", 1.0))
                row["execution_mode"] = str(execution_details.get("execution_mode", "GHOST")).upper()
                
                if "timestamp" in row:
                    try:
                        start_dt = self._parse_iso_timestamp(row["timestamp"])
                        duration = (datetime.now(timezone.utc) - start_dt).total_seconds() / 60.0
                        row["holding_minutes"] = round(duration, 2)
                    except Exception:
                        row["holding_minutes"] = 0.0
                
                self._safe_execute(self.supabase.table("quantitative_ledger").upsert(row))
                
                verify = self._safe_execute(
                    self.supabase.table("quantitative_ledger")
                    .select("resolved")
                    .eq("signal_id", str(signal_id))
                )
                
                if verify and verify.data and verify.data[0].get("resolved"):
                    logger.info(f"🎯 ATTRIBUTION MATCHED & VERIFIED // Signal {signal_id[:8]}... updated with PnL: ${net_pnl:.4f} | Mode: {row['execution_mode']}")
                else:
                    logger.error(f"❌ VERIFICATION FAILED: Ledger did not save outcome for signal {signal_id}")
            else:
                logger.warning(f"⚠️ Live execution completed but no initial signal found in ledger for ID: {signal_id}")
        except Exception as e:
            logger.error(f"❌ DATABASE UPDATE TRANSACTION EXCEPTION for signal {signal_id}: {e}")

    def resolve_batch_historical_predictions(self, assets: List[str], current_prices: Dict[str, Any], age_cutoff: float) -> int:
        """
        🚀 pillar 1: GLOBAL PATH-TRAVERSAL RESOLUTION ENGINE
        Optimized to drop narrow asset collection filtering, allowing the background 
        shadow basket to systematically settle via live candles or the 60-minute time decay layer.
        """
        resolved_count = 0

        try:
            # 🛑 UNCONSTRAINED APEX: Pulls any active unresolved records to evaluate the full universe
            query = self.supabase.table("quantitative_ledger").select("*").eq("resolved", False)
                
            response = self._safe_execute(
                query.order("timestamp", desc=False).limit(500)
            )

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
                actual = "HOLD"
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
                            actual = "SELL"  
                            is_terminated = True
                            exit_price = sl_price
                            break
                        elif hit_tp:
                            actual = "BUY"
                            is_terminated = True
                            exit_price = tp_price
                            break
                        elif hit_sl:
                            actual = "SELL"
                            is_terminated = True
                            exit_price = sl_price
                            break
                    elif prediction == "SELL":
                        hit_tp = low <= tp_price
                        hit_sl = high >= sl_price
                        if hit_tp and hit_sl:
                            actual = "BUY"  
                            is_terminated = True
                            exit_price = sl_price
                            break
                        elif hit_tp:
                            actual = "SELL"
                            is_terminated = True
                            exit_price = tp_price
                            break
                        elif hit_sl:
                            actual = "BUY"
                            is_terminated = True
                            exit_price = sl_price
                            break

                if not is_terminated and elapsed_minutes >= 60.0:
                    is_terminated = True
                    exit_price = current_price
                    if current_price > entry_price:
                        actual = "BUY"
                    elif current_price < entry_price:
                        actual = "SELL"
                    else:
                        actual = "HOLD"

                if is_terminated and actual != "HOLD":
                    GHOST_LEVERAGE = 10.0
                    TAKER_ROUND_TRIP = 0.0011
                    if prediction == "BUY":
                        gross_return = (exit_price - entry_price) / entry_price
                    else:
                        gross_return = (entry_price - exit_price) / entry_price
                        
                    net_pnl = (gross_return - TAKER_ROUND_TRIP) * GHOST_LEVERAGE

                    row["resolved"] = True
                    row["actual_outcome"] = "WIN" if prediction == actual else "LOSS"
                    row["is_correct"] = (prediction == actual)
                    row["net_pnl"] = float(net_pnl)
                    row["holding_minutes"] = round(min(elapsed_minutes, float(bars_held)), 2)
                    row["execution_mode"] = "GHOST"
                    update_batch.append(row)
                    resolved_count += 1
                
            if update_batch:
                self._safe_execute(self.supabase.table("quantitative_ledger").upsert(update_batch))
                logger.info(f"📊 GHOST FORENSICS: Traversed and settled {len(update_batch)} predictive ledger paths.")
                
            return resolved_count

        except Exception as e:
            logger.error(f"❌ KINETIC RESOLUTION ENGINE FAILURE: {e}")
            return 0

    def compute_rolling_accuracy(self, window_size: int = 150, core_basket: List[str] = None) -> Tuple[float, int]:
        """
        🚀 pillar 2: ADAPTIVE GLOBAL CALIBRATION SELECTOR
        Grades system rolling accuracy. Dynamically includes the unconstrained background 
        shadow pool if no production live history exists, allowing the FSM to efficiently bootstrap.
        """
        try:
            # Check if any production, non-shadow live trades exist in the database
            check_query = self.supabase.table("quantitative_ledger").select("is_correct").eq("resolved", True).eq("is_shadow", False)
            if core_basket:
                check_query = check_query.in_("symbol", core_basket)
            
            check_res = self._safe_execute(check_query.limit(5))
            has_live_history = len(check_res.data) > 0 if check_res else False

            # Instantiate base query context
            query = self.supabase.table("quantitative_ledger").select("is_correct").eq("resolved", True)
            
            if has_live_history:
                # Production Mode: Enforce strict live validation on your selected active core pairs
                query = query.eq("is_shadow", False)
                if core_basket:
                    query = query.in_("symbol", core_basket)
                logger.debug("FSM Mode: Production (Strict Live Attribution Active)")
            else:
                # Calibration Mode Fallback: Unleash calculations over ALL background shadow data across all pairs
                logger.debug("FSM Mode: Calibration (Shadow Validation Active - Basket Restrictions Lifted)")
                
            response = self._safe_execute(
                query.order("timestamp", desc=True).limit(window_size)
            )
                
            results = response.data if response else []
            total_resolved = len(results)
            
            if total_resolved == 0:
                return 0.50, 0

            correct_array = [1.0 if row.get("is_correct") is True else 0.0 for row in results]
            stable_accuracy = sum(correct_array) / total_resolved
                
            return float(stable_accuracy), total_resolved

        except Exception as e:
            logger.error(f"❌ ENGINE HORIZON EVALUATION EXCEPTION: {e}")
            return 0.50, 0