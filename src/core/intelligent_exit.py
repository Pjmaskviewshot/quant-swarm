"""
💎 V6.0 QUANTUM TRAJECTORY ORACLE: CONTINUOUS MICROSTRUCTURE STOPPING ENGINE
-----------------------------------------------------------------------------
Abolishes static R-multiple heuristics. Evaluates the live stochastic trajectory 
of active positions in real-time from t_0 entry.

Features:
- Optimal Stopping Formulation (EV Drift Inversion)
- Hawkes Excitation Velocity Exhaustion
- Passive Iceberg Absorption / OFI Divergence Detection
- Micro-Price Elasticity Collapse Guards
"""

import math
import time
import logging
import numpy as np
from collections import deque
from typing import Tuple, Dict, Any

logger = logging.getLogger("QUANT_CORE.TRAJECTORY_EXIT")


class IntelligentExitEngine:
    """
    Continuous Trajectory Stopping Engine:
    Evaluates whether the active trade's underlying micro-alpha vector is still expanding 
    or has mathematically exhausted its forward expected value.
    """

    @staticmethod
    def evaluate_microstructure_exit(ctx: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Calculates the real-time optimal stopping probability based on 
        live order flow exhaustion, hidden liquidity absorption, and micro-drift.
        """
        is_buy = ctx["is_buy"]
        current_r = ctx.get("current_r", 0.0)
        max_r = ctx.get("r_multiple", 0.0)
        hawkes_z = ctx.get("hawkes_z", 0.0)
        
        stat_engine = ctx.get("stat_engine")
        feature_engine = ctx.get("feature_engine")
        
        # Extract live microstructure dynamics
        ofi_z = getattr(stat_engine, "ofi_fast_z", 0.0) if stat_engine else 0.0
        inst_var = getattr(stat_engine, "inst_variance", 1e-6) if stat_engine else 1e-6
        
        # 1. 🧠 ICEBERG ABSORPTION DIVERGENCE (Whale Absorption Detection)
        # Price is at/near peak, but aggressive trade flow (OFI) has completely flipped against us.
        if current_r > 0.30:
            if is_buy and ofi_z < -2.2:
                # Buyers hitting the bid / massive passive seller blocking the offer
                return True, f"ICEBERG_ABSORPTION_DETECTED (OFI Z: {ofi_z:.2f} | R: +{current_r:.2f})"
            elif not is_buy and ofi_z > 2.2:
                # Sellers hitting the ask / massive passive buyer blocking the bid
                return True, f"ICEBERG_ABSORPTION_DETECTED (OFI Z: {ofi_z:.2f} | R: +{current_r:.2f})"

        # 2. ⚡ HAWKES EXCITATION EXHAUSTION (Kinetic Momentum Burnout)
        # The predatory market sweep that pushed the trade into profit has hit velocity zero.
        if current_r > 0.40:
            if is_buy and hawkes_z < -2.5:
                return True, f"HAWKES_VELOCITY_BURNOUT (Sell Sweep Inversion Z: {hawkes_z:.2f})"
            elif not is_buy and hawkes_z > 2.5:
                return True, f"HAWKES_VELOCITY_BURNOUT (Buy Sweep Inversion Z: {hawkes_z:.2f})"

        # 3. 📉 CONTINUOUS OPTIMAL STOPPING DRIFT INVERSION (Hamilton-Jacobi-Bellman)
        # Calculates if the forward expected drift of the trade has turned negative
        if current_r > 0.20:
            # Directional alpha drift per second
            alpha_drift = (ofi_z * 0.00015) if is_buy else (-ofi_z * 0.00015)
            # Volatility penalty (variance drag)
            vol_penalty = math.sqrt(inst_var) * 1.5
            
            # Forward EV Drift
            forward_ev_drift = alpha_drift - vol_penalty
            
            # If the trade has gained profit and forward drift becomes negative with decay
            if forward_ev_drift < -0.0008 and (max_r - current_r) > 0.15:
                return True, f"EV_DRIFT_INVERSION (Drift: {forward_ev_drift*10000:.1f} bps | Peak R: +{max_r:.2f})"

        # 4. 🛑 L2 BOOK-WALL DISLOCATION COLLAPSE
        last_ob = ctx.get("last_ob", {})
        ask_v = last_ob.get("ask_size", 1.0)
        bid_v = last_ob.get("bid_size", 1.0)
        
        if current_r > 0.25:
            if is_buy:
                book_ratio = bid_v / (ask_v + 1e-9)
                if book_ratio < 0.12:  # 88% of supporting bids vanished
                    return True, f"LIQUIDITY_FOUNDATION_COLLAPSE (Bid Depth: {book_ratio*100:.1f}%)"
            else:
                book_ratio = ask_v / (bid_v + 1e-9)
                if book_ratio < 0.12:  # 88% of supporting asks vanished
                    return True, f"LIQUIDITY_FOUNDATION_COLLAPSE (Ask Depth: {book_ratio*100:.1f}%)"

        return False, "HOLD"

    @staticmethod
    def compute_dynamic_trailing_stop(ctx: Dict[str, Any]) -> float:
        """
        Dynamically calculates the stop price based on the volatility manifold 
        and micro-price variance rather than static tick distances.
        """
        is_buy = ctx["is_buy"]
        entry = ctx["actual_entry"]
        risk = ctx["initial_risk"]
        max_p = ctx["max_favorable_price"]
        current_p = ctx.get("safe_c_price", entry)
        current_r = ctx.get("current_r", 0.0)
        max_r = ctx.get("r_multiple", 0.0)
        current_sl = ctx.get("current_sl", entry)

        new_sl = current_sl

        # Dynamic Volatility Envelope
        stat_engine = ctx.get("stat_engine")
        inst_var = getattr(stat_engine, "inst_variance", 1e-5) if stat_engine else 1e-5
        vol_buffer = max(ctx.get("atr", risk) * 0.8, entry * math.sqrt(inst_var) * 2.0)

        # 1. Continuous Variance-Adjusted Break-Even Ratchet
        # Activates dynamically as soon as the trade clears the round-trip fee hurdle
        fee_hurdle_r = (entry * 0.0016) / (risk + 1e-9)
        if max_r > fee_hurdle_r:
            be_price = entry * 1.0015 if is_buy else entry * 0.9985
            new_sl = max(new_sl, be_price) if is_buy else min(new_sl, be_price)

        # 2. Continuous Parabolic Surface Trail
        # As max_r expands, the volatility buffer contracts non-linearly
        if max_r > 0.80:
            compression_factor = max(0.25, 1.0 / (1.0 + (max_r * 0.5)))
            dynamic_trailing_dist = vol_buffer * compression_factor
            
            trail_price = max_p - dynamic_trailing_dist if is_buy else max_p + dynamic_trailing_dist
            new_sl = max(new_sl, trail_price) if is_buy else min(new_sl, trail_price)

        return new_sl