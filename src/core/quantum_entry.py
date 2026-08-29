"""
💎 V25.0 APEX QUANTUM PRIME: CROSS-ASSET EXECUTION TENSOR
------------------------------------------------------------------------
Fuses Cross-Asset Macro Order Flows (BTC/ETH) and Orderbook Convexity 
to dynamically shape execution sizing (w_exec) and Sector Impulse.

Architectural Supremacy (V25.0):
1. Eradication of Probability Tampering: Stripped out the Bounded Logit 
   Manifold Projection. We now trust the 18-D Volterra-Hermite RLS tensor 
   to output pure, mathematically calibrated probabilities. This module 
   acts as a strict passthrough for `fused_prob`.
2. Focus on Kelly Sizing: Re-engineered to calculate `execution_weight` 
   based on real-time Orderbook Convexity and Macro Synergy, scaling 
   capital allocation up only when all vectors align.
3. Sector Impulse Generation: Derives the instantaneous `sector_impulse` 
   to be fed directly back into `micro_models.py`.
"""

import math
import logging
import numpy as np
from collections import deque
from typing import Dict, List, Any

logger = logging.getLogger("QUANT_CORE.QUANTUM_ENTRY")


class QuantumEntryMatrix:
    """
    🚀 V25.0 Multi-Factor Alpha Fusion Engine:
    Evaluates real-time macro OFI cross-correlation and orderbook curvature
    to dynamically scale execution weights without distorting Bayesian probability.
    """

    def __init__(self, window_size: int = 10):
        # 🚀 V25.0: Massively reduced window size. We only care about instantaneous macro flow.
        self.btc_ofi_history = deque(maxlen=window_size)
        self.eth_ofi_history = deque(maxlen=window_size)

    def update_macro_flows(self, asset_ofi_z: float, btc_ofi_z: float, eth_ofi_z: float):
        """Ingests synchronized tick-level OFI Z-scores across macro drivers."""
        if math.isfinite(btc_ofi_z):
            self.btc_ofi_history.append(btc_ofi_z)
        if math.isfinite(eth_ofi_z):
            self.eth_ofi_history.append(eth_ofi_z)

    def update_mlofi_state(self, mlofi_z: float):
        """🚀 V25.0 DEPRECATED: Local OFI is now handled natively inside micro_models.py."""
        pass

    @staticmethod
    def _compute_orderbook_convexity(bids: List[List[float]], asks: List[List[float]]) -> float:
        """
        Calculates non-linear slope/curvature of top-5 book depth in pure O(1) time.
        Positive (>0) = Bid-heavy convexity (support thickening deeper in book).
        Negative (<0) = Ask-heavy convexity (resistance thickening deeper in book).
        """
        if not bids or not asks or len(bids) < 3 or len(asks) < 3:
            return 0.0

        try:
            bid_vols = [float(b[1]) for b in bids[:5]]
            ask_vols = [float(a[1]) for a in asks[:5]]
            
            # Linear decay weighting to measure cumulative depth density. Precomputed exp(-0.35 * x)
            weights = np.array([1.0, 0.704, 0.496, 0.349, 0.246])
            
            weighted_bids = np.sum(np.array(bid_vols) * weights[:len(bid_vols)])
            weighted_asks = np.sum(np.array(ask_vols) * weights[:len(ask_vols)])
            
            total_vol = weighted_bids + weighted_asks + 1e-9
            convexity = (weighted_bids - weighted_asks) / total_vol
            return float(np.clip(convexity, -1.0, 1.0))
        except Exception:
            return 0.0

    def _calculate_macro_synergy(self, intended_action: str) -> float:
        """
        Computes directional alignment between local asset and BTC/ETH drivers.
        Returns a normalized Macro Z-Score.
        """
        if len(self.btc_ofi_history) < 3 or len(self.eth_ofi_history) < 3:
            return 0.0

        btc_z = float(np.mean(self.btc_ofi_history))
        eth_z = float(np.mean(self.eth_ofi_history))
        
        # 65% BTC / 35% ETH Beta Weighting
        macro_z = (0.65 * btc_z) + (0.35 * eth_z)

        is_buy = intended_action.upper() == "BUY"
        return macro_z if is_buy else -macro_z

    def fuse_signal_probability(
        self, 
        symbol: str, 
        raw_prob: float, 
        intended_action: str, 
        bids: List[List[float]], 
        asks: List[List[float]]
    ) -> Dict[str, Any]:
        """
        🚀 V25.0 EXECUTION SIZING MANIFOLD
        Probabilities are passed through cleanly. Calculates Orderbook Convexity 
        and Macro Synergy to dynamically scale the execution weight (Kelly fraction).
        """
        # 1. Stateless Orderbook Convexity
        convexity = self._compute_orderbook_convexity(bids, asks)
        is_buy = intended_action.upper() == "BUY"
        convexity_boost = convexity if is_buy else -convexity

        # 2. Macro Driver Synergy
        macro_z = self._calculate_macro_synergy(intended_action)
        synergy_scalar = 1.0 + (math.tanh(macro_z * 0.5) * 0.35)

        # 3. Dynamic Execution Sizing Weight (w_exec)
        # Scales allocation up when Macro Flow and Local Book Convexity agree
        exec_weight = float(np.clip(synergy_scalar * (1.0 + (convexity_boost * 0.4)), 0.50, 1.75))

        return {
            "symbol": symbol,
            "fused_prob": raw_prob,  # 🚀 V25.0 FIX: Pure passthrough. Do not distort RLS probabilities.
            "execution_weight": exec_weight,
            "convexity_score": convexity,
            "macro_z_score": macro_z,
            "synergy_multiplier": synergy_scalar,
            "action": intended_action,
            "sector_impulse": macro_z  # 🚀 Outputs directly to micro_models.py
        }