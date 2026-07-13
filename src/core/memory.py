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
            "is_shadow": is_shadow   
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
        🚀 pillar 1: KINETIC PATH-TRAVERSAL RESOLUTION ENGINE
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
                
                sl_price = float(row.get("virtual_sl", entry_price * 0.99))
                tp_price = float(row.get("virtual_tp", entry_price * 1.015))
                
                p_data = current_prices.get(symbol)
                if p_data is None:
                    continue

                # Parse multi-dimensional arrays out of modern screener payload securely
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
                row_time = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
                elapsed_minutes = (now_ts - row_time).total_seconds() / 60.0

                is_terminated = False
                actual = "HOLD"
                exit_price = entry_price

                # Chronological microstructural path verification loop using Highs/Lows for wicks
                for i in range(len(closes)):
                    high = highs[i]
                    low = lows[i]
                    
                    if prediction == "BUY":
                        if high >= tp_price:
                            actual = "BUY"
                            is_terminated = True
                            exit_price = tp_price
                            break
                        elif low <= sl_price:
                            actual = "SELL"
                            is_terminated = True
                            exit_price = sl_price
                            break
                    elif prediction == "SELL":
                        if low <= tp_price:
                            actual = "SELL"
                            is_terminated = True
                            exit_price = tp_price
                            break
                        elif high >= sl_price:
                            actual = "BUY"
                            is_terminated = True
                            exit_price = sl_price
                            break

                # Handle Master Timeout (60-minute window cutoff)
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
                    # 🚀 CRITICAL FIX: Calculate nominal statistical return for Ghost Ledger tracking
                    if prediction == "BUY":
                        net_pnl = ((exit_price - entry_price) / entry_price) * 10.0  # Normalized to an arbitrary $10 unit
                    else:
                        net_pnl = ((entry_price - exit_price) / entry_price) * 10.0

                    row["resolved"] = True
                    row["actual_outcome"] = "WIN" if prediction == actual else "LOSS"
                    row["is_correct"] = (prediction == actual)
                    row["net_pnl"] = float(net_pnl)
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
        🚀 pillar 2: STABILIZED HORIZON MANAGEMENT
        Calculates a true rolling moving average accuracy over the fixed sample size.
        Strictly ignores shadow/background trades to grade the FSM purely on core assets.
        """
        try:
            query = self.supabase.table("quantitative_ledger")\
                .select("is_correct")\
                .eq("resolved", True)\
                .eq("is_shadow", False)
            
            if core_basket:
                query = query.in_("symbol", core_basket)
                
            response = query.order("timestamp", desc=True).limit(window_size).execute()
            results = response.data if response else []
            total_resolved = len(results)
            
            # 🚀 CRITICAL FIX: Handle the Bayesian Uninformative Prior Baseline
            if total_resolved == 0:
                return 0.50, 0

            correct_array = [1.0 if row.get("is_correct") is True else 0.0 for row in results]
            stable_accuracy = sum(correct_array) / total_resolved
                
            return float(stable_accuracy), total_resolved

        except Exception as e:
            logger.error(f"❌ ENGINE HORIZON EVALUATION EXCEPTION: {e}")
            return 0.50, 0