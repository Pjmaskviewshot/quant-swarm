"""
💎 V58.0 TITANIUM APEX: MICRO-PRICE ABSORPTION & EXHAUSTION GUARD
-----------------------------------------------------------------
Tracks continuous micro-price acceleration vs. aggressor trade flow divergence.
Triggers an immediate Limit-IOC exit when heavy aggressor buying/selling is 
completely absorbed by passive limit walls (icebergs), anticipating a reversal.
"""

import math
import logging
import numpy as np
from collections import deque
from typing import Dict, Any

logger = logging.getLogger("QUANT_CORE.EXHAUSTION_GUARD")

class MicroAbsorptionGuard:
    """
    Computes the Absorption Divergence Index (D_absorb).
    Identifies when aggressive flow hits a brick wall of passive liquidity.
    """
    def __init__(self, window_size: int = 30):
        self.window_size = window_size
        
        self.micro_prices = deque(maxlen=window_size)
        self.timestamps = deque(maxlen=window_size)
        self.trade_flows = deque(maxlen=window_size)
        
        self.current_accel = 0.0
        self.absorption_score = 0.0

    def push_state(self, best_bid: float, bid_qty: float, best_ask: float, ask_qty: float, trade_volume_signed: float, timestamp: float):
        """
        Ingests the latest top-of-book metrics and signed trade volume.
        Calculates and stores the volume-weighted Micro-Price.
        """
        total_qty = bid_qty + ask_qty + 1e-9
        # Micro-Price pulls toward the side with the weaker limit wall
        micro_price = (best_bid * ask_qty + best_ask * bid_qty) / total_qty

        self.micro_prices.append(micro_price)
        self.timestamps.append(timestamp)
        self.trade_flows.append(trade_volume_signed)

    def evaluate_exhaustion(self, position_side: str, symbol: str = "UNKNOWN") -> Dict[str, Any]:
        """
        Evaluates whether passive limit walls are absorbing aggressive flow against the position.
        
        Args:
            position_side: "BUY" or "SELL"
            symbol: Asset ticker for telemetry logging
            
        Returns:
            Dict containing ejection flag and diagnostic reasoning.
        """
        # Require at least 5 ticks to calculate meaningful acceleration (2nd derivative)
        if len(self.micro_prices) < 5:
            return {"should_eject": False, "reason": "CALIBRATING", "score": 0.0}

        p = list(self.micro_prices)
        t = list(self.timestamps)
        q = list(self.trade_flows)

        # Calculate time deltas, clamping to prevent ZeroDivisionError
        dt_curr = max(0.001, t[-1] - t[-2])
        dt_prev = max(0.001, t[-2] - t[-3])

        # 1st Derivative: Velocity
        v_curr = (p[-1] - p[-2]) / dt_curr
        v_prev = (p[-2] - p[-3]) / dt_prev

        # 2nd Derivative: Acceleration
        self.current_accel = (v_curr - v_prev) / dt_curr

        # Aggregate the last 3 ticks of signed trade flow
        recent_q = sum(q[-3:])

        self.absorption_score = 0.0

        # =====================================================================
        # 🟢 BUY POSITION ABSORPTION GUARD
        # Scenario: We are LONG. Aggressors are buying heavily (recent_q > 0), 
        # but the price is decelerating rapidly (accel < 0). A sell wall is absorbing them.
        # =====================================================================
        if position_side.upper() == "BUY":
            if recent_q > 0 and self.current_accel < -0.0005:
                # Score = Buy Volume * Magnitude of Deceleration
                self.absorption_score = recent_q * (-self.current_accel)
                
                if self.absorption_score > 2.0:
                    logger.warning(
                        f"[X-RAY] 🧱 ABSORPTION WALL HIT // {symbol} | "
                        f"Heavy Buy Flow Absorbed. Score: {self.absorption_score:.2f} | Accel: {self.current_accel:.5f}"
                    )
                    return {
                        "should_eject": True,
                        "reason": f"BUY_FLOW_ABSORBED | Accel: {self.current_accel:.5f} | Score: {self.absorption_score:.2f}",
                        "score": self.absorption_score
                    }

        # =====================================================================
        # 🔴 SELL POSITION ABSORPTION GUARD
        # Scenario: We are SHORT. Aggressors are selling heavily (recent_q < 0), 
        # but the price is accelerating upwards (accel > 0). A buy wall is absorbing them.
        # =====================================================================
        elif position_side.upper() == "SELL":
            if recent_q < 0 and self.current_accel > 0.0005:
                # Score = Sell Volume * Magnitude of Upward Acceleration
                self.absorption_score = (-recent_q) * self.current_accel
                
                if self.absorption_score > 2.0:
                    logger.warning(
                        f"[X-RAY] 🧱 ABSORPTION WALL HIT // {symbol} | "
                        f"Heavy Sell Flow Absorbed. Score: {self.absorption_score:.2f} | Accel: {self.current_accel:.5f}"
                    )
                    return {
                        "should_eject": True,
                        "reason": f"SELL_FLOW_ABSORBED | Accel: {self.current_accel:.5f} | Score: {self.absorption_score:.2f}",
                        "score": self.absorption_score
                    }

        return {"should_eject": False, "reason": "CLEAR", "score": self.absorption_score}