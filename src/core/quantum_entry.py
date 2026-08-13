"""
ðŸ’Ž V1.0 TENSOR-PRIME ENTRY MATRIX (PRECISION SNIPER CORE)
-------------------------------------------------------------------------
Re-instates Binary Veto Gates while maintaining Dynamic Empirical CDF 
Ranking. Only dispatches entries that represent true statistical anomalies 
(>=85th Percentile) with positive macro-flow alignment. Eliminates log spam.
"""

import math
import logging
import numpy as np
from collections import deque
from typing import Dict, Any, List

logger = logging.getLogger("QUANT_CORE.TENSOR_PRIME_ENTRY")


class QuantumEntryMatrix:
    """
    ðŸš€ DYNAMIC BINARY TENSOR ALPHA ENGINE
    Enforces strict pass/fail gates. Filters noise early before 
    sending payloads to the Capital Auction Queue.
    """
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        
        # 1. Microstructure Vectors
        self.mlofi_history = deque(maxlen=window_size)
        self.mlofi_velocity = deque(maxlen=window_size)
        self.mlofi_acceleration = deque(maxlen=window_size)
        
        # 2. Dynamic Covariance Tensors
        self.asset_flow_history = deque(maxlen=window_size)
        self.btc_flow_history = deque(maxlen=window_size)
        self.eth_flow_history = deque(maxlen=window_size)
        
        # 3. Empirical CDF Ranking Buffers
        self.convexity_history = deque(maxlen=window_size)
        self.composite_alpha_history = deque(maxlen=window_size)

    def _get_percentile_rank(self, value: float, history_buffer: deque) -> float:
        """
        ðŸš€ EMPIRICAL CDF (eCDF) RANKING
        Calculates exactly what percentile the current value is compared 
        to the rolling historical window.
        """
        if len(history_buffer) < 30:
            return 0.50  # Default to 50th percentile while calibrating
            
        arr = np.array(history_buffer)
        return float(np.sum(arr < value) / len(arr))

    def update_macro_flows(self, asset_ofi_z: float, btc_ofi_z: float, eth_ofi_z: float):
        """Ingests high-speed flow Z-scores for macro alignment."""
        self.asset_flow_history.append(asset_ofi_z)
        self.btc_flow_history.append(btc_ofi_z)
        self.eth_flow_history.append(eth_ofi_z)

    def update_mlofi_state(self, current_mlofi: float, dt: float = 1.0):
        """Tracks 1st and 2nd derivatives of Log-MLOFI for tape acceleration."""
        self.mlofi_history.append(current_mlofi)
        if len(self.mlofi_history) >= 2:
            velocity = (self.mlofi_history[-1] - self.mlofi_history[-2]) / max(0.001, dt)
            self.mlofi_velocity.append(velocity)
            if len(self.mlofi_velocity) >= 2:
                accel = (self.mlofi_velocity[-1] - self.mlofi_velocity[-2]) / max(0.001, dt)
                self.mlofi_acceleration.append(accel)

    def calculate_depth_convexity(self, bids: List[List[float]], asks: List[List[float]], decay_alpha: float = 0.3) -> float:
        """Calculates exponential orderbook depth convexity."""
        if not bids or not asks:
            return 1.0
        limit = min(10, len(bids), len(asks))
        bid_weighted_vol, ask_weighted_vol = 0.0, 0.0
        for i in range(limit):
            try:
                weight = math.exp(-decay_alpha * i)
                bid_weighted_vol += float(bids[i][1]) * weight
                ask_weighted_vol += float(asks[i][1]) * weight
            except (IndexError, ValueError):
                continue
        ratio = bid_weighted_vol / (ask_weighted_vol + 1e-9)
        self.convexity_history.append(ratio)
        return ratio

    def evaluate_entry_alpha(
        self, 
        symbol: str, 
        raw_prob: float, 
        intended_action: str, 
        bids: List[List[float]], 
        asks: List[List[float]], 
        htf_bias: float
    ) -> Dict[str, Any]:
        """
        ðŸš€ BINARY SNIPER EVALUATOR
        Hard-blocks weak signals, macro conflicts, and noisy order flow.
        Returns approved=True ONLY when alpha energy exceeds strict bounds.
        """
        # 1. HARD GATE: Calibrating Check
        if len(self.mlofi_acceleration) < 30 or len(self.convexity_history) < 30:
            return {"approved": False, "alpha_score": 0.0, "reason": "CALIBRATING_DATA_BUFFERS"}

        base_score = max(0.0, (raw_prob - 0.50) * 100.0)

        # 2. Acceleration Derivative Rank
        current_accel = self.mlofi_acceleration[-1] if self.mlofi_acceleration else 0.0
        accel_percentile = self._get_percentile_rank(current_accel, self.mlofi_acceleration)
        
        accel_score = 0.0
        if intended_action == "BUY" and accel_percentile > 0.70:
            accel_score = (accel_percentile - 0.70) * 100.0
        elif intended_action == "SELL" and accel_percentile < 0.30:
            accel_score = ((1.0 - accel_percentile) - 0.70) * 100.0

        # 3. Macro Composite Sync & Hard Conflict Veto
        btc_z = self.btc_flow_history[-1] if self.btc_flow_history else 0.0
        eth_z = self.eth_flow_history[-1] if self.eth_flow_history else 0.0
        macro_composite = (btc_z * 0.6) + (eth_z * 0.4)

        # HARD VETO: Do not trade directly against strong macro flow
        if intended_action == "BUY" and macro_composite < -1.5:
            return {"approved": False, "alpha_score": 0.0, "reason": f"HARD_MACRO_BEARISH_VETO ({macro_composite:.2f}Ïƒ)"}
        elif intended_action == "SELL" and macro_composite > 1.5:
            return {"approved": False, "alpha_score": 0.0, "reason": f"HARD_MACRO_BULLISH_VETO ({macro_composite:.2f}Ïƒ)"}

        # 4. Depth Convexity Rank
        convexity_ratio = self.calculate_depth_convexity(bids, asks)
        convexity_percentile = self._get_percentile_rank(convexity_ratio, self.convexity_history)
        
        convexity_score = 0.0
        if intended_action == "BUY" and convexity_percentile > 0.70:
            convexity_score = (convexity_percentile - 0.70) * 50.0
        elif intended_action == "SELL" and convexity_percentile < 0.30:
            convexity_score = ((1.0 - convexity_percentile) - 0.70) * 50.0

        # Compute Raw Alpha Score
        raw_alpha_score = base_score + accel_score + convexity_score
        self.composite_alpha_history.append(raw_alpha_score)

        current_alpha_percentile = self._get_percentile_rank(raw_alpha_score, self.composite_alpha_history)

        # 5. HARD BINARY APPROVAL THRESHOLD
        # Requires score to be in top 15% (>= 85th percentile) AND score > 12.0
        if current_alpha_percentile >= 0.85 and raw_alpha_score >= 12.0:
            execution_weight = min(1.5, max(0.5, raw_alpha_score / 20.0))
            
            logger.critical(
                f"ðŸ”¥ TENSOR SNIPER ENTRY APPROVED // {symbol} {intended_action} | "
                f"Alpha Score: {raw_alpha_score:.1f} (P{current_alpha_percentile*100:.1f}) | Exec Weight: {execution_weight:.2f}x"
            )
            return {
                "approved": True,
                "alpha_score": raw_alpha_score,
                "execution_weight": execution_weight,
                "reason": f"QUALIFIED_ANOMALY_P{current_alpha_percentile*100:.1f}"
            }

        # Silent Rejection for anything not in the top tier (Stops terminal spam)
        return {
            "approved": False,
            "alpha_score": raw_alpha_score,
            "execution_weight": 0.0,
            "reason": f"INSUFFICIENT_ALPHA_ENERGY (Score: {raw_alpha_score:.1f}, Rank: P{current_alpha_percentile*100:.1f})"
        }