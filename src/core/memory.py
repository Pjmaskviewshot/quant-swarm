import os
import logging
from datetime import datetime, timezone
from typing import Tuple, List, Dict, Any
from supabase import create_client, Client

logger = logging.getLogger("QUANT_CORE.MEMORY")

class MemoryBank:
    def __init__(self, db_path: str = None):
        """
        Initializes the Cloud-Native Supabase Analytics Engine connection layer.
        Accepts an optional db_path parameter to preserve interface compatibility with main.py.
        """
        # Retrieve configuration details from the host environment
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

    def commit_prediction(self, signal_id: str, timestamp: float, price: float, direction: str, confidence: float, features: Dict[str, Any] = None):
        """Saves a fresh prediction sequence with full structural feature vectors for future analytics."""
        if features is None:
            features = {}
            
        # Parse feature metrics with defensive fallback bounds
        market_regime = features.get("market_regime", "UNKNOWN")
        z_obi = features.get("adaptive_obi_z", 0.0)
        vol_mult = features.get("liquidity_density_ratio", 1.0)
        spread = features.get("bid_ask_spread", 0.0)
        symbol = features.get("symbol", "UNKNOWN") # Use symbol from feature payload fallback

        # Map the incoming Unix Epoch float cleanly into a standardized ISO-8601 string for PostgreSQL TIMESTAMPTZ
        iso_timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()

        payload = {
            "signal_id": str(signal_id),
            "timestamp": iso_timestamp,
            "symbol": symbol if symbol != "UNKNOWN" else "UNKNOWN", # Guard clause filled downstream by engine orchestration
            "predicted_direction": str(direction).upper(),
            "price_at_prediction": float(price),
            "ai_confidence": float(confidence),
            "market_regime": str(market_regime),
            "z_obi": float(z_obi),
            "vol_mult": float(vol_mult),
            "spread": float(spread),
            "resolved": False
        }

        try:
            self.supabase.table("quantitative_ledger").insert(payload).execute()
            logger.info(f"💾 LEDGER COMMIT SIGNED // ID: {signal_id[:8]}... | Symbol: {symbol} | Dir: {direction}")
        except Exception as e:
            logger.error(f"❌ DATABASE INSERT TRANSACTION EXCEPTION for signal {signal_id}: {e}")

    def log_live_execution_result(self, signal_id: str, net_pnl: float, slippage: float, outcome: str):
        """ Updates a live trade signal with actual financial execution data for deep post-trade analytics. """
        is_correct = True if net_pnl > 0 else False
        
        update_payload = {
            "signal_id": str(signal_id),  # Required for targeting
            "resolved": True,
            "actual_outcome": str(outcome),
            "net_pnl": float(net_pnl),
            "slippage_drag": float(slippage),
            "is_correct": is_correct
        }

        try:
            self.supabase.table("quantitative_ledger").upsert(update_payload).execute()
            logger.info(f"🎯 ATTRIBUTION MATCHED // Signal {signal_id[:8]}... updated with PnL: ${net_pnl:.4f}")
        except Exception as e:
            logger.error(f"❌ DATABASE UPDATE TRANSACTION EXCEPTION for signal {signal_id}: {e}")
            
    def resolve_historical_predictions(self, current_price: float, age_cutoff: float) -> int:
        """Compares expired, unresolved ghost predictions against current market price using batch upserts."""
        cutoff_iso = datetime.fromtimestamp(age_cutoff, tz=timezone.utc).isoformat()

        try:
            response = self.supabase.table("quantitative_ledger")\
                .select("signal_id, price_at_prediction, predicted_direction")\
                .eq("resolved", False)\
                .lte("timestamp", cutoff_iso)\
                .execute()

            unresolved_rows = response.data if response else []
            
            if not unresolved_rows:
                return 0

            update_batch = []
            for row in unresolved_rows:
                sig_id = row["signal_id"]
                entry_price = float(row["price_at_prediction"])
                prediction = str(row["predicted_direction"]).upper()
                
                actual = "HOLD"
                if current_price > entry_price:
                    actual = "BUY"
                elif current_price < entry_price:
                    actual = "SELL"

                is_correct = True if prediction == actual else False
                
                update_batch.append({
                    "signal_id": sig_id,
                    "resolved": True,
                    "actual_outcome": actual,
                    "is_correct": is_correct
                })
                
            if update_batch:
                self.supabase.table("quantitative_ledger").upsert(update_batch).execute()
                return len(update_batch)
                
            return 0

        except Exception as e:
            logger.error(f"❌ GHOST RESOLUTION ENGINE FAILURE: {e}")
            return 0

    def resolve_batch_historical_predictions(self, assets: List[str], current_prices: Dict[str, float], age_cutoff: float) -> int:
        """
        Resolves historical predictions for multiple assets in a single, highly efficient batched database query.
        Prevents cross-contamination of asset prices and eliminates network strangulation.
        """
        cutoff_iso = datetime.fromtimestamp(age_cutoff, tz=timezone.utc).isoformat()

        try:
            response = self.supabase.table("quantitative_ledger")\
                .select("signal_id, symbol, price_at_prediction, predicted_direction")\
                .eq("resolved", False)\
                .in_("symbol", assets)\
                .lte("timestamp", cutoff_iso)\
                .execute()

            unresolved_rows = response.data if response else []
            
            if not unresolved_rows:
                return 0

            update_batch = []
            for row in unresolved_rows:
                sig_id = row["signal_id"]
                symbol = row.get("symbol")
                entry_price = float(row["price_at_prediction"])
                prediction = str(row["predicted_direction"]).upper()
                
                current_price = current_prices.get(symbol)
                if not current_price or current_price <= 0:
                    continue
                
                actual = "HOLD"
                if current_price > entry_price:
                    actual = "BUY"
                elif current_price < entry_price:
                    actual = "SELL"

                is_correct = True if prediction == actual else False
                
                update_batch.append({
                    "signal_id": sig_id,
                    "resolved": True,
                    "actual_outcome": actual,
                    "is_correct": is_correct
                })
                
            if update_batch:
                self.supabase.table("quantitative_ledger").upsert(update_batch).execute()
                logger.info(f"🦇 Batched resolution completed for {len(update_batch)} historical ghost trades across {len(assets)} assets natively.")
                return len(update_batch)
                
            return 0

        except Exception as e:
            logger.error(f"❌ BATCH GHOST RESOLUTION ENGINE FAILURE: {e}")
            return 0

    def compute_rolling_accuracy(self, window_size: int = 50) -> Tuple[float, int]:
        """Calculates rolling system accuracy metric over the target baseline sample window."""
        try:
            response = self.supabase.table("quantitative_ledger")\
                .select("is_correct")\
                .eq("resolved", True)\
                .order("timestamp", desc=True)\
                .limit(window_size)\
                .execute()

            results = response.data if response else []
            total_resolved = len(results)
            
            if total_resolved == 0:
                return 0.0, 0

            correct_predictions = sum(1 for row in results if row.get("is_correct") is True)
            accuracy = correct_predictions / total_resolved
            return accuracy, total_resolved

        except Exception as e:
            logger.error(f"❌ ENGINE ACCURACY METRIC EVALUATION EXCEPTION: {e}")
            return 0.0, 0