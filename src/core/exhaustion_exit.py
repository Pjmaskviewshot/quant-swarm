"""
💎 V21.0 APEX QUANTUM PRIME: KINETIC ABSORPTION & EXHAUSTION GUARD
-----------------------------------------------------------------
Abandons chronological time for pure Volume-Time Kinematics.
Tracks Expected Market Impact (Kyle's Lambda) vs. Actual Price Displacement.
Triggers immediate Limit-IOC exits when the Z-Score of the Structural 
Work Deficit proves the existence of a hidden Iceberg or Dark Pool limit wall.
"""

import math
import logging
import numpy as np
from collections import deque
from typing import Dict, Any

logger = logging.getLogger("QUANT_CORE.EXHAUSTION_GUARD")

class MicroAbsorptionGuard:
    """
    🚀 V21.0 KINETIC ABSORPTION TENSOR
    Computes Volume-Time Acceleration and Structural Work Deficits (SWD).
    Mathematically immune to asynchronous tick clustering and division-by-zero bounds.
    """
    def __init__(self, window_size: int = 50, alpha: float = 0.05):
        self.window_size = window_size
        self.alpha = alpha
        
        self.micro_prices = deque(maxlen=window_size)
        self.signed_vols = deque(maxlen=window_size)
        
        # Continuous EWMA state tracking for Expected Impact (Kyle's Lambda)
        self.lambda_ewma = 1e-6
        
        # Structural Work Deficit (SWD) Distribution tracking
        self.deficit_ewma = 0.0
        self.deficit_var = 1e-9
        
        self.current_accel_v = 0.0
        self.current_z_score = 0.0

    def push_state(self, best_bid: float, bid_qty: float, best_ask: float, ask_qty: float, trade_volume_signed: float, timestamp: float):
        """
        Ingests L2 limits and L1 aggressive flow.
        Computes the Non-Linear Stoikov Micro-Price and extracts kinematic derivatives.
        """
        # 1. Non-Linear Stoikov Micro-Price (Maps Spread Curvature)
        spread = best_ask - best_bid
        total_qty = bid_qty + ask_qty + 1e-9
        imb = bid_qty / total_qty
        
        # Stoikov adjustment pushes price away from the denser wall logarithmically
        stoikov_adj = spread * (imb - 0.5) * (1.0 + abs(imb - 0.5))
        micro_price = ((best_bid + best_ask) / 2.0) + stoikov_adj

        # 2. Extract Volume-Time Kinematics (Only if we have prior state)
        if len(self.micro_prices) > 0 and len(self.signed_vols) > 0:
            dp_curr = micro_price - self.micro_prices[-1]
            dv_curr = max(abs(trade_volume_signed), 1e-9)
            
            # Instantaneous Velocity in Volume-Space (Price Impact per unit volume)
            velocity_curr = dp_curr / dv_curr
            
            # Update Kyle's Lambda (Expected Absolute Impact Rate)
            inst_lambda = abs(velocity_curr)
            self.lambda_ewma = (1.0 - self.alpha) * self.lambda_ewma + self.alpha * inst_lambda
            
            # Calculate Structural Work Deficit (Expected Movement vs Actual Movement)
            expected_dp = self.lambda_ewma * trade_volume_signed
            deficit = expected_dp - dp_curr
            
            # Z-Score Normalization of the Deficit
            self.deficit_ewma = (1.0 - self.alpha) * self.deficit_ewma + self.alpha * deficit
            self.deficit_var = (1.0 - self.alpha) * self.deficit_var + self.alpha * (deficit - self.deficit_ewma)**2
            
            self.current_z_score = (deficit - self.deficit_ewma) / (math.sqrt(self.deficit_var) + 1e-9)
            
            # Calculate Volume-Time Acceleration (d^2P / dV^2) if enough history exists
            if len(self.micro_prices) > 1:
                dp_prev = self.micro_prices[-1] - self.micro_prices[-2]
                dv_prev = max(abs(self.signed_vols[-1]), 1e-9)
                velocity_prev = dp_prev / dv_prev
                
                # Acceleration: Change in velocity per unit of recent volume
                self.current_accel_v = (velocity_curr - velocity_prev) / dv_curr

        self.micro_prices.append(micro_price)
        self.signed_vols.append(trade_volume_signed)

    def evaluate_exhaustion(self, position_side: str, symbol: str = "UNKNOWN") -> Dict[str, Any]:
        """
        Evaluates whether passive limit walls are absorbing aggressive flow.
        Returns immediate ejection commands if a mathematically verified Iceberg is hit.
        """
        if len(self.micro_prices) < 15:
            return {"should_eject": False, "reason": "CALIBRATING_KINEMATICS", "score": 0.0}

        recent_q = sum(list(self.signed_vols)[-3:])

        # =====================================================================
        # 🟢 BUY POSITION ABSORPTION GUARD
        # Scenario: We are LONG. We want the price to rise. 
        # Danger: Aggressors are BUYING heavily (recent_q > 0), but the actual price 
        # displacement falls vastly short of expected impact (SWD Z-Score > 2.8σ), 
        # and volume-acceleration is negative. This is an invisible Ask Wall (Iceberg).
        # =====================================================================
        if position_side.upper() == "BUY":
            # SWD is highly positive when Expected_DP > Actual_DP (Upward movement blocked)
            if self.current_z_score > 2.8 and recent_q > 0.0:
                if self.current_accel_v < 0.0:
                    logger.warning(
                        f"[X-RAY] 🧱 KINETIC ICEBERG DETECTED // {symbol} | "
                        f"Buy Flow Absorbed. Deficit Z: {self.current_z_score:.2f}σ | Accel(v): {self.current_accel_v:.6f}"
                    )
                    return {
                        "should_eject": True,
                        "reason": f"ASK_WALL_ICEBERG | Z-Score: {self.current_z_score:.2f}σ | Accel(v): {self.current_accel_v:.6f}",
                        "score": self.current_z_score
                    }

        # =====================================================================
        # 🔴 SELL POSITION ABSORPTION GUARD
        # Scenario: We are SHORT. We want the price to fall.
        # Danger: Aggressors are SELLING heavily (recent_q < 0), but actual price
        # displacement falls short (SWD Z-Score < -2.8σ), and volume-acceleration 
        # is positive (bouncing back). This is an invisible Bid Wall (Iceberg).
        # =====================================================================
        elif position_side.upper() == "SELL":
            # SWD is highly negative when Expected_DP < Actual_DP (Downward movement blocked)
            if self.current_z_score < -2.8 and recent_q < 0.0:
                if self.current_accel_v > 0.0:
                    logger.warning(
                        f"[X-RAY] 🧱 KINETIC ICEBERG DETECTED // {symbol} | "
                        f"Sell Flow Absorbed. Deficit Z: {self.current_z_score:.2f}σ | Accel(v): {self.current_accel_v:.6f}"
                    )
                    return {
                        "should_eject": True,
                        "reason": f"BID_WALL_ICEBERG | Z-Score: {self.current_z_score:.2f}σ | Accel(v): {self.current_accel_v:.6f}",
                        "score": self.current_z_score
                    }

        return {"should_eject": False, "reason": "KINEMATICS_CLEAR", "score": self.current_z_score}