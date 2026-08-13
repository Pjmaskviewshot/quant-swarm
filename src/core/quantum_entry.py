"""
💎 V4.0 ALPHA FUSION ENGINE (FORMERLY ENTRY MATRIX)
-------------------------------------------------------------------------
Abolishes arbitrary "veto gates". Fuses Microstructure acceleration, 
Orderbook Convexity, and Macro-Flow alignment directly into the signal's 
base probability. The system now trades purely on unified Expected Value (EV).
"""

import math
import logging
import numpy as np
from collections import deque
from typing import Dict, Any, List

logger = logging.getLogger("QUANT_CORE.ALPHA_FUSION")

class QuantumEntryMatrix:
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.mlofi_history = deque(maxlen=window_size)
        self.mlofi_velocity = deque(maxlen=window_size)
        self.mlofi_acceleration = deque(maxlen=window_size)
        
        self.asset_flow_history = deque(maxlen=window_size)
        self.btc_flow_history = deque(maxlen=window_size)
        self.eth_flow_history = deque(maxlen=window_size)
        
        self.convexity_history = deque(maxlen=window_size)

    def _get_percentile_rank(self, value: float, history_buffer: deque) -> float:
        if len(history_buffer) < 20:
            return 0.50 
        arr = np.array(history_buffer)
        return float(np.sum(arr < value) / len(arr))

    def update_macro_flows(self, asset_ofi_z: float, btc_ofi_z: float, eth_ofi_z: float):
        self.asset_flow_history.append(asset_ofi_z)
        self.btc_flow_history.append(btc_ofi_z)
        self.eth_flow_history.append(eth_ofi_z)

    def update_mlofi_state(self, current_mlofi: float, dt: float = 1.0):
        self.mlofi_history.append(current_mlofi)
        if len(self.mlofi_history) >= 2:
            velocity = (self.mlofi_history[-1] - self.mlofi_history[-2]) / max(0.001, dt)
            self.mlofi_velocity.append(velocity)
            if len(self.mlofi_velocity) >= 2:
                accel = (self.mlofi_velocity[-1] - self.mlofi_velocity[-2]) / max(0.001, dt)
                self.mlofi_acceleration.append(accel)

    def calculate_depth_convexity(self, bids: List[List[float]], asks: List[List[float]], decay_alpha: float = 0.3) -> float:
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

    def fuse_signal_probability(
        self, 
        symbol: str, 
        raw_prob: float, 
        intended_action: str, 
        bids: List[List[float]], 
        asks: List[List[float]]
    ) -> Dict[str, Any]:
        """
        🚀 UNIFIED PROBABILITY FUSION
        Instead of gating the trade, this organically scales the probability of success
        up or down based on supporting order book and macro evidence.
        """
        if len(self.mlofi_acceleration) < 15 or len(self.convexity_history) < 15:
            return {"fused_prob": raw_prob, "execution_weight": 1.0, "reason": "CALIBRATING_BUFFERS"}

        prob_modifier = 1.0

        # 1. Macro-Flow Alignment (BTC/ETH Correlation)
        btc_z = self.btc_flow_history[-1] if self.btc_flow_history else 0.0
        eth_z = self.eth_flow_history[-1] if self.eth_flow_history else 0.0
        macro_composite = (btc_z * 0.6) + (eth_z * 0.4)

        if intended_action == "BUY":
            if macro_composite > 1.5: prob_modifier *= 1.15   # Strong tailwind
            elif macro_composite < -1.5: prob_modifier *= 0.60  # Severe headwind (slashes probability)
        elif intended_action == "SELL":
            if macro_composite < -1.5: prob_modifier *= 1.15
            elif macro_composite > 1.5: prob_modifier *= 0.60

        # 2. Tape Acceleration (Micro-Momentum)
        current_accel = self.mlofi_acceleration[-1] if self.mlofi_acceleration else 0.0
        accel_percentile = self._get_percentile_rank(current_accel, self.mlofi_acceleration)
        
        if intended_action == "BUY":
            if accel_percentile > 0.75: prob_modifier *= 1.10
            elif accel_percentile < 0.25: prob_modifier *= 0.85
        elif intended_action == "SELL":
            if accel_percentile < 0.25: prob_modifier *= 1.10
            elif accel_percentile > 0.75: prob_modifier *= 0.85

        # 3. Order Book Convexity (Liquidity Support)
        convexity_ratio = self.calculate_depth_convexity(bids, asks)
        convexity_percentile = self._get_percentile_rank(convexity_ratio, self.convexity_history)
        
        if intended_action == "BUY":
            if convexity_percentile > 0.70: prob_modifier *= 1.10
            elif convexity_percentile < 0.30: prob_modifier *= 0.85
        elif intended_action == "SELL":
            if convexity_percentile < 0.30: prob_modifier *= 1.10
            elif convexity_percentile > 0.70: prob_modifier *= 0.85

        # 🚀 Final Fused Probability (Clamped between 5% and 95%)
        fused_prob = min(0.95, max(0.05, raw_prob * prob_modifier))
        
        # Execution weight maps directly to the confidence of the modifier
        execution_weight = min(2.0, max(0.5, prob_modifier))
        
        # Only log to the terminal if it's a massive setup
        if prob_modifier > 1.2:
            logger.info(f"🔥 ALPHA FUSION SURGE // {symbol} {intended_action} | Raw Prob: {raw_prob:.2%} -> Fused Prob: {fused_prob:.2%} (Mod: {prob_modifier:.2f}x)")

        return {
            "fused_prob": fused_prob,
            "execution_weight": execution_weight,
            "reason": f"FUSION_COMPLETE_MOD_{prob_modifier:.2f}"
        }