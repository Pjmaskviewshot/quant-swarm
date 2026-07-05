import os
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

    def commit_prediction(self, signal_id: str, timestamp: float, price: float, direction: str, confidence: float, features: Dict[str, Any] = None, is_shadow: bool = False):
        """Saves a fresh prediction cleanly using the new dedicated schema columns."""
        if features is None:
            features = {}
            
        market_regime = features.get("market_regime", "UNKNOWN")
        z_obi = features.get("adaptive_obi_z", 0.0)
        vol_mult = features.get("liquidity_density_ratio", 1.0)
        spread = features.get("bid_ask_spread", 0.0)
        symbol = features.get("symbol", "UNKNOWN")
        
        # 🚀 NO MORE HACKS: Using dedicated virtual columns
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
            "virtual_sl": sl_price,  # Clean database schema mapping
            "virtual_tp": tp_price,  # Clean database schema mapping
            "is_shadow": is_shadow   # Separates core trades from background noise
        }

        try:
            self.supabase.table("quantitative_ledger").insert(payload).execute()
            label = "🦇 SHADOW" if is_shadow else "💾 CORE"
            logger.info(f"{label} LEDGER COMMIT // ID: {signal_id[:8]}... | Node: {symbol} | SL: {sl_price:.4f} | TP: {tp_price:.4f}")
        except Exception as e:
            logger.error(f"❌ DATABASE INSERT TRANSACTION EXCEPTION for signal {signal_id}: {e}")

    def log_live_execution_result(self, signal_id: str, net_pnl: float, slippage: float, outcome: str):
        """ Updates a live trade signal with actual financial execution data. """
        is_correct = True if net_pnl > 0 else False
        
        try:
            response = self.supabase.table("quantitative_ledger").select("*").eq("signal_id", str(signal_id)).execute()
            if response.data:
                row = response.data[0]
                row["resolved"] = True
                row["actual_outcome"] = str(outcome)
                row["net_pnl"] = float(net_pnl)
                row["slippage_drag"] = float(slippage)
                row["is_correct"] = is_correct
                
                self.supabase.table("quantitative_ledger").upsert(row).execute()
                logger.info(f"🎯 ATTRIBUTION MATCHED // Signal {signal_id[:8]}... updated with PnL: ${net_pnl:.4f}")
        except Exception as e:
            logger.error(f"❌ DATABASE UPDATE TRANSACTION EXCEPTION for signal {signal_id}: {e}")

    def resolve_batch_historical_predictions(self, assets: List[str], current_prices: Dict[str, Any], age_cutoff: float) -> int:
        """
        🚀pillar 1: KINETIC PATH-TRAVERSAL RESOLUTION ENGINE
        Resolves predictions by scanning the high-frequency price sequences to catch inside-candle bracket hits.
        """
        resolved_count = 0

        try:
            response = self.supabase.table("quantitative_ledger")\
                .select("*")\
                .eq("resolved", False)\
                .in_("symbol", assets)\
                .execute()

            unresolved_rows = response.data if response else []
            
            if not unresolved_rows:
                return 0

            update_batch = []
            now_ts = datetime.now(timezone.utc)

            for row in unresolved_rows:
                symbol = row.get("symbol")
                entry_price = float(row["price_at_prediction"])
                prediction = str(row["predicted_direction"]).upper()
                
                # 🚀 Pulling from clean schema columns now
                sl_price = float(row.get("virtual_sl", entry_price * 0.99))
                tp_price = float(row.get("virtual_tp", entry_price * 1.015))
                
                p_data = current_prices.get(symbol)
                if p_data is None:
                    continue

                # Dynamic Data Parsing Strategy (Supports Floats, Dicts, or Sequence Lists)
                if isinstance(p_data, dict):
                    current_price = float(p_data.get("close", p_data.get("current", entry_price)))
                    price_sequence = p_data.get("sequence", [current_price])
                elif isinstance(p_data, (list, np.ndarray)):
                    if len(p_data) == 0:
                        continue
                    price_sequence = [float(p) for p in p_data]
                    current_price = price_sequence[-1]
                else:
                    # Fallback for standard backwards compatible float tracking
                    current_price = float(p_data)
                    price_sequence = [current_price]

                if current_price <= 0:
                    continue

                row_time = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
                elapsed_minutes = (now_ts - row_time).total_seconds() / 60.0

                is_terminated = False
                actual = "HOLD"

                # Chronological microstructural path verification loop
                for p in price_sequence:
                    if prediction == "BUY":
                        if p >= tp_price:
                            actual = "BUY"
                            is_terminated = True
                            break
                        elif p <= sl_price:
                            actual = "SELL"
                            is_terminated = True
                            break
                    elif prediction == "SELL":
                        if p <= tp_price:
                            actual = "SELL"
                            is_terminated = True
                            break
                        elif p >= sl_price:
                            actual = "BUY"
                            is_terminated = True
                            break

                # ⏳ MASTER UPGRADE: Raised from 15.0 to 60.0 minutes to prevent label truncation
                if not is_terminated and elapsed_minutes >= 60.0:
                    is_terminated = True
                    if current_price > entry_price:
                        actual = "BUY"
                    elif current_price < entry_price:
                        actual = "SELL"

                if is_terminated:
                    row["resolved"] = True
                    row["actual_outcome"] = actual
                    row["is_correct"] = True if prediction == actual else False
                    update_batch.append(row)
                    resolved_count += 1
                
            if update_batch:
                self.supabase.table("quantitative_ledger").upsert(update_batch).execute()
                logger.info(f"⚡ KINETIC RESOLUTION CYCLE: Successfully processed {len(update_batch)} paths via sequential verification matrix.")
                
            return resolved_count

        except Exception as e:
            logger.error(f"❌ KINETIC RESOLUTION ENGINE FAILURE: {e}")
            return 0

    def compute_rolling_accuracy(self, window_size: int = 150, core_basket: List[str] = None) -> Tuple[float, int]:
        """
        🚀pillar 2: STABILIZED HORIZON MANAGEMENT
        Calculates a true rolling moving average accuracy over the fixed sample size.
        Strictly ignores shadow/background trades to grade the FSM purely on core assets.
        """
        try:
            query = self.supabase.table("quantitative_ledger")\
                .select("is_correct")\
                .eq("resolved", True)\
                .eq("is_shadow", False)
            
            # 🚀 THE FIX: Only grade FSM accuracy on the core operational basket
            if core_basket:
                query = query.in_("symbol", core_basket)
                
            response = query.order("timestamp", desc=True).limit(window_size).execute()

            results = response.data if response else []
            total_resolved = len(results)
            
            if total_resolved == 0:
                return 0.0, 0

            # Isolate raw boolean outcomes
            correct_array = [1.0 if row.get("is_correct") is True else 0.0 for row in results]
            
            # Calculate a perfectly unweighted rolling accuracy across the window
            stable_accuracy = sum(correct_array) / total_resolved
                
            return float(stable_accuracy), total_resolved

        except Exception as e:
            logger.error(f"❌ ENGINE HORIZON EVALUATION EXCEPTION: {e}")
            return 0.0, 0