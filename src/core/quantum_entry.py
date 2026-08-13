"""
💎 V2.0 ADAPTIVE DYNAMIC ENTRY MATRIX (REGIME-AWARE SNIPER)
-------------------------------------------------------------------------
Eradicates fixed static percentages. Dynamically scales entry thresholds, 
macro-flow alignment, and execution weights based on real-time Kaufman 
Market Efficiency and Order Book Convexity.
"""

import math
import logging
import numpy as np
from collections import deque
from typing import Dict, Any, List

logger = logging.getLogger("QUANT_CORE.TENSOR_PRIME_ENTRY")


class QuantumEntryMatrix:
    """
    🚀 V2.0 DYNAMIC ADAPTIVE ALPHA ENGINE
    Dynamically adjusts signal approval thresholds based on real-time market efficiency.
    """
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.mlofi_history = deque(maxlen=window_size)
        self.mlofi_velocity = deque(maxlen=window_size)
        self.mlofi_acceleration = deque(maxlen=window_size)
        
        self.asset_flow_history = deque(maxlen=window_size)
        self.btc_flow_history = deque(maxlen=window_size)
        self.eth_flow_history = deque(maxlen=window_size)
        
        self.convexity_history = deque(maxlen=window_size)
        self.composite_alpha_history = deque(maxlen=window_size)
        self.efficiency_history = deque(maxlen=window_size)

    def _get_percentile_rank(self, value: float, history_buffer: deque) -> float:
        if len(history_buffer) < 20:
            return 0.50 
        arr = np.array(history_buffer)
        return float(np.sum(arr < value) / len(arr))

    def _calculate_market_efficiency(self, window: int = 20) -> float:
        """
        Calculates Kaufman Efficiency Ratio (KER).
        1.0 = Pure Trend (Low friction, lower gate threshold)
        0.0 = Pure Chop (High friction, strict gate threshold)
        """
        if len(self.mlofi_history) < window:
            return 0.50
        
        recent = list(self.mlofi_history)[-window:]
        net_change = abs(recent[-1] - recent[0])
        total_path = sum(abs(recent[i] - recent[i-1]) for i in range(1, len(recent))) + 1e-9
        
        ker = net_change / total_path
        self.efficiency_history.append(ker)
        return ker

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

    def evaluate_entry_alpha(
        self, 
        symbol: str, 
        raw_prob: float, 
        intended_action: str, 
        bids: List[List[float]], 
        asks: List[List[float]], 
        htf_bias: float
    ) -> Dict[str, Any]:
        if len(self.mlofi_acceleration) < 20 or len(self.convexity_history) < 20:
            return {"approved": False, "alpha_score": 0.0, "reason": "CALIBRATING_BUFFERS"}

        # 1. Real-Time Efficiency Rating
        market_efficiency = self._calculate_market_efficiency()

        # 2. Dynamic Threshold Shift
        # High efficiency (trending) -> Gate lowers to 60th percentile
        # Low efficiency (choppy) -> Gate tightens to 88th percentile
        dynamic_percentile_gate = 0.88 - (market_efficiency * 0.28)
        dynamic_min_score = 12.0 - (market_efficiency * 7.0)

        base_score = max(0.0, (raw_prob - 0.50) * 100.0)

        current_accel = self.mlofi_acceleration[-1] if self.mlofi_acceleration else 0.0
        accel_percentile = self._get_percentile_rank(current_accel, self.mlofi_acceleration)
        
        accel_score = 0.0
        if intended_action == "BUY" and accel_percentile > 0.55:
            accel_score = (accel_percentile - 0.55) * 100.0
        elif intended_action == "SELL" and accel_percentile < 0.45:
            accel_score = ((1.0 - accel_percentile) - 0.55) * 100.0

        # Macro Alignment
        btc_z = self.btc_flow_history[-1] if self.btc_flow_history else 0.0
        eth_z = self.eth_flow_history[-1] if self.eth_flow_history else 0.0
        macro_composite = (btc_z * 0.6) + (eth_z * 0.4)

        if intended_action == "BUY" and macro_composite < -1.8:
            return {"approved": False, "alpha_score": 0.0, "reason": "MACRO_BEARISH_CONFLICT"}
        elif intended_action == "SELL" and macro_composite > 1.8:
            return {"approved": False, "alpha_score": 0.0, "reason": "MACRO_BULLISH_CONFLICT"}

        convexity_ratio = self.calculate_depth_convexity(bids, asks)
        convexity_percentile = self._get_percentile_rank(convexity_ratio, self.convexity_history)
        
        convexity_score = 0.0
        if intended_action == "BUY" and convexity_percentile > 0.55:
            convexity_score = (convexity_percentile - 0.55) * 50.0
        elif intended_action == "SELL" and convexity_percentile < 0.45:
            convexity_score = ((1.0 - convexity_percentile) - 0.55) * 50.0

        raw_alpha_score = base_score + accel_score + convexity_score
        self.composite_alpha_history.append(raw_alpha_score)

        current_alpha_percentile = self._get_percentile_rank(raw_alpha_score, self.composite_alpha_history)

        # 3. Dynamic Approval Evaluation
        if current_alpha_percentile >= dynamic_percentile_gate and raw_alpha_score >= dynamic_min_score:
            execution_weight = min(1.8, max(0.4, raw_alpha_score / 15.0))
            
            logger.critical(
                f"🔥 ADAPTIVE ENTRY APPROVED // {symbol} {intended_action} | "
                f"Alpha: {raw_alpha_score:.1f} (P{current_alpha_percentile*100:.1f} >= P{dynamic_percentile_gate*100:.1f}) | "
                f"KER: {market_efficiency:.2f} | Exec Weight: {execution_weight:.2f}x"
            )
            return {
                "approved": True,
                "alpha_score": raw_alpha_score,
                "execution_weight": execution_weight,
                "reason": f"DYNAMIC_QUALIFIED_KER_{market_efficiency:.2f}"
            }

        return {
            "approved": False,
            "alpha_score": raw_alpha_score,
            "execution_weight": 0.0,
            "reason": f"BELOW_DYNAMIC_GATE (Score: {raw_alpha_score:.1f} < {dynamic_min_score:.1f})"
        }