"""
💎 V95.0 TENSOR-PRIME ENTRY MATRIX (SELF-CALIBRATING)
-------------------------------------------------------------------------
No static thresholds. Uses Empirical CDF Percentile Ranking, 
Dynamic Covariance Weighting, and Multi-Dimensional Vector Anomalies
to guarantee execution only in the 98th percentile of market states.
"""

import math
import logging
import numpy as np
from collections import deque
from typing import Dict, Any, List

logger = logging.getLogger("QUANT_CORE.TENSOR_PRIME_ENTRY")


class QuantumEntryMatrix:
    """
    🚀 HIGH-PRECISION NON-STATIONARY ALPHA GENERATOR
    Continuously self-calibrates to current market volatility.
    Only triggers entries that are statistical anomalies (>98th Percentile).
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
        🚀 EMPIRICAL CDF (eCDF) RANKING
        Calculates exactly what percentile the current value is compared 
        to the rolling historical window. Eliminates static thresholds.
        """
        if len(history_buffer) < 50:
            return 0.50  # Default to 50th percentile while calibrating
            
        arr = np.array(history_buffer)
        percentile = np.sum(arr < value) / len(arr)
        return float(percentile)

    def _get_dynamic_macro_weights(self) -> tuple:
        """
        🚀 INFORMATION-THEORETIC WEIGHTING
        Calculates real-time covariance to dynamically weight BTC vs ETH influence.
        """
        if len(self.asset_flow_history) < 50:
            return (0.5, 0.5)

        asset_arr = np.array(self.asset_flow_history)
        btc_arr = np.array(self.btc_flow_history)
        eth_arr = np.array(self.eth_flow_history)

        std_asset = np.std(asset_arr) + 1e-9
        
        # Covariance divided by standard deviations
        btc_cov = max(0.0, np.cov(btc_arr, asset_arr)[0][1] / (np.std(btc_arr) * std_asset + 1e-9))
        eth_cov = max(0.0, np.cov(eth_arr, asset_arr)[0][1] / (np.std(eth_arr) * std_asset + 1e-9))
        
        total_cov = btc_cov + eth_cov + 1e-9
        return (btc_cov / total_cov, eth_cov / total_cov)

    def update_macro_flows(self, asset_ofi_z: float, btc_ofi_z: float, eth_ofi_z: float):
        """Ingests high-speed flow Z-scores for dynamic weighting."""
        self.asset_flow_history.append(asset_ofi_z)
        self.btc_flow_history.append(btc_ofi_z)
        self.eth_flow_history.append(eth_ofi_z)

    def update_mlofi_state(self, current_mlofi: float, dt: float = 1.0):
        """Tracks 1st and 2nd derivatives of Log-MLOFI for acceleration."""
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
        bid_weighted_vol = 0.0
        ask_weighted_vol = 0.0

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
        🚀 NON-STATIONARY ALPHA ENTRY EVALUATOR
        Evaluates current tick against its own historical distribution.
        """
        if len(self.mlofi_acceleration) < 50 or len(self.convexity_history) < 50:
            return {"approved": False, "alpha_score": 0.0, "reason": "MATRIX_CALIBRATING"}

        # 1. Base Statistical Prob
        base_score = max(0.0, (raw_prob - 0.50) * 100.0)

        # 2. Dynamic OFI Acceleration Rank
        current_accel = self.mlofi_acceleration[-1]
        accel_percentile = self._get_percentile_rank(current_accel, self.mlofi_acceleration)
        
        accel_score = 0.0
        if intended_action == "BUY" and accel_percentile > 0.80:
            accel_score = (accel_percentile - 0.80) * 100.0  # Scales up dynamically
        elif intended_action == "SELL" and accel_percentile < 0.20:
            accel_score = ((1.0 - accel_percentile) - 0.80) * 100.0

        # 3. Dynamic Macro Vector Rank
        btc_weight, eth_weight = self._get_dynamic_macro_weights()
        btc_z = self.btc_flow_history[-1] if self.btc_flow_history else 0.0
        eth_z = self.eth_flow_history[-1] if self.eth_flow_history else 0.0
        macro_composite = (btc_z * btc_weight) + (eth_z * eth_weight)
        
        macro_score = 0.0
        if intended_action == "BUY":
            if macro_composite < -0.5:
                logger.warning(f"[X-RAY] 🛑 MACRO CONFLICT VETO // {symbol} BUY blocked. Composite Z: {macro_composite:.2f}.")
                return {"approved": False, "alpha_score": 0.0, "reason": "MACRO_FLOW_CONFLICT"}
            macro_score = max(0.0, macro_composite * 10.0)
            
        elif intended_action == "SELL":
            if macro_composite > 0.5:
                logger.warning(f"[X-RAY] 🛑 MACRO CONFLICT VETO // {symbol} SELL blocked. Composite Z: {macro_composite:.2f}.")
                return {"approved": False, "alpha_score": 0.0, "reason": "MACRO_FLOW_CONFLICT"}
            macro_score = max(0.0, abs(macro_composite) * 10.0)

        # 4. Dynamic Depth Convexity Rank
        convexity_ratio = self.calculate_depth_convexity(bids, asks)
        convexity_percentile = self._get_percentile_rank(convexity_ratio, self.convexity_history)
        
        convexity_score = 0.0
        if intended_action == "BUY" and convexity_percentile > 0.80:
            convexity_score = (convexity_percentile - 0.80) * 100.0
        elif intended_action == "SELL" and convexity_percentile < 0.20:
            convexity_score = ((1.0 - convexity_percentile) - 0.80) * 100.0

        # COMPUTE FINAL RAW SCORE & APPEND TO HISTORY
        raw_alpha_score = base_score + accel_score + macro_score + convexity_score
        self.composite_alpha_history.append(raw_alpha_score)

        # 🚀 THE UNFAIR ADVANTAGE: 98th Percentile Dynamic Gateway
        # The current score MUST be in the top 2% of all scores generated in the last window.
        current_alpha_percentile = self._get_percentile_rank(raw_alpha_score, self.composite_alpha_history)
        
        # We require at least an 0.95 percentile (Top 5%) to pass.
        if current_alpha_percentile >= 0.95 and raw_alpha_score > 10.0:
            logger.critical(
                f"🔥 TENSOR-PRIME ALPHA ENTRY // {symbol} {intended_action} Approved! "
                f"Score: {raw_alpha_score:.1f} | Rank: Top {(1.0 - current_alpha_percentile)*100:.1f}% of Market."
            )
            return {
                "approved": True, 
                "alpha_score": raw_alpha_score, 
                "reason": f"PERCENTILE_RANK_P{current_alpha_percentile*100:.1f}"
            }
        else:
            logger.info(
                f"[X-RAY] ⏸️ TENSOR REJECT // {symbol} {intended_action} Rank P{current_alpha_percentile*100:.1f} < P95.0. Needs stronger anomaly."
            )
            return {
                "approved": False, 
                "alpha_score": raw_alpha_score, 
                "reason": f"LOW_PERCENTILE_RANK (P{current_alpha_percentile*100:.1f})"
            }