"""
🎯 V5.0 INTELLIGENT EXIT & HARVEST ENGINE
-----------------------------------------------------------------
Operates on Level-2 Liquidity Wall Evaporation, Hawkes Climax Exits,
and Volatility-Surface Trailing Stops to optimize trade lifecycles.
"""

import math
import time
import logging
import numpy as np
from collections import deque
from typing import Dict, Any, Tuple

logger = logging.getLogger("QUANT_CORE.INTELLIGENT_EXIT")


class IntelligentExitEngine:
    def __init__(self):
        pass

    @staticmethod
    def evaluate_microstructure_exit(ctx: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Evaluates real-time Level-2 depth, Hawkes acceleration, and Macro Flow
        to determine if the position should exit immediately.
        """
        is_buy = ctx["is_buy"]
        symbol = ctx["symbol"]
        r_mult = ctx.get("r_multiple", 0.0)
        current_r = ctx.get("current_r", 0.0)
        
        stat_engine = ctx.get("stat_engine")
        if not stat_engine:
            return False, "NO_STAT_ENGINE"

        # 1. LIQUIDITY WALL EVAPORATION CHECK
        hist = ctx.get("kinetic_history", {})
        if hist and len(hist.get("bid_refill", [])) >= 5:
            if is_buy:
                # If ask resistance is swelling while bid support evaporated
                recent_bids = list(hist["bid_refill"])[-3:]
                recent_asks = list(hist["ask_refill"])[-3:]
                if sum(recent_asks) > (sum(recent_bids) * 3.5 + 1e-6) and current_r > 0.30:
                    logger.critical(f"[X-RAY] 🧱 ORDERBOOK WALL EVAPORATION // {symbol} Bid support collapsed. Securing gains.")
                    return True, "BID_WALL_EVAPORATED"
            else:
                recent_bids = list(hist["bid_refill"])[-3:]
                recent_asks = list(hist["ask_refill"])[-3:]
                if sum(recent_bids) > (sum(recent_asks) * 3.5 + 1e-6) and current_r > 0.30:
                    logger.critical(f"[X-RAY] 🧱 ORDERBOOK WALL EVAPORATION // {symbol} Ask support collapsed. Securing gains.")
                    return True, "ASK_WALL_EVAPORATED"

        # 2. HAWKES CLIMAX EXHAUSTION (Peak Wick Sniping)
        hawkes_z = getattr(stat_engine, "hawkes_z", 0.0)
        if r_mult >= 0.75:
            # Trade was up significantly, but momentum hit a volume exhaustion wall
            if is_buy and hawkes_z < -2.8:
                logger.critical(f"[X-RAY] ⚡ HAWKES EXHAUSTION // {symbol} Peak buy sweep exhausted ({hawkes_z:.2f}σ). Cashing out.")
                return True, "HAWKES_CLIMAX_EXHAUSTION"
            elif not is_buy and hawkes_z > 2.8:
                logger.critical(f"[X-RAY] ⚡ HAWKES EXHAUSTION // {symbol} Peak sell sweep exhausted ({hawkes_z:.2f}σ). Cashing out.")
                return True, "HAWKES_CLIMAX_EXHAUSTION"

        # 3. PROFIT RETRACEMENT GUARD (Dynamic Wick Protector)
        if r_mult >= 1.20:
            retrace_pct = (r_mult - current_r) / (r_mult + 1e-9)
            if retrace_pct >= 0.30:
                logger.warning(f"[X-RAY] 💰 PROFIT RETRACEMENT // {symbol} Retraced 30% from peak of +{r_mult:.2f}R. Banking remainder.")
                return True, "PROFIT_RETRACEMENT_FLOOR"

        return False, "HOLD"

    @staticmethod
    def compute_dynamic_trailing_stop(ctx: Dict[str, Any]) -> float:
        """
        Computes an adaptive stop level anchored to Instantaneous Variance and Micro-Structure Support.
        """
        is_buy = ctx["is_buy"]
        current_sl = ctx["current_sl"]
        entry_price = ctx["actual_entry"]
        max_price = ctx.get("max_favorable_price", entry_price)
        r_mult = ctx.get("r_multiple", 0.0)
        
        stat_engine = ctx.get("stat_engine")
        inst_var = getattr(stat_engine, "inst_variance", 0.001)
        vol_buffer = max(ctx["atr"] * 1.2, entry_price * math.sqrt(inst_var) * 1.5)
        
        new_sl = current_sl
        
        # 1. Break-Even + Exchange Fee Lock at +0.6R
        if r_mult >= 0.60:
            fee_buffer = entry_price * 0.0015
            be_price = (entry_price + fee_buffer) if is_buy else (entry_price - fee_buffer)
            new_sl = max(new_sl, be_price) if is_buy else min(new_sl, be_price)

        # 2. Structural Profit Ratchet at +1.0R and beyond
        if r_mult >= 1.0:
            tight_trail = max_price - vol_buffer if is_buy else max_price + vol_buffer
            new_sl = max(new_sl, tight_trail) if is_buy else min(new_sl, tight_trail)

        return new_sl