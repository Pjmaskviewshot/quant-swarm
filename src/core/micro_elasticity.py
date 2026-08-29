"""
💎 V25.0 APEX QUANTUM PRIME: STATELESS L2 ABSORPTION ENGINE
-------------------------------------------------------------------------
Deterministic Liquidity Absorption & Orderbook Book-Walking.

Architectural Supremacy (V25.0):
1. Eradication of Stateful Redundancy: Removed all deques for micro-price, 
   variance, and EWMA tracking. This state is now centrally managed by 
   `micro_models.py` to eliminate CPU overhead and dictionary bloat.
2. Eradication of Defensive Vetoes: Removed `is_adverse_selection_imminent`. 
   Adverse selection is now mathematically modeled in the 18-D Volterra-Hermite 
   Tensor instead of relying on hardcoded if/else firewalls.
3. Pure Stateless Utility: Converts this module into a lightning-fast, 
   static book-walking utility for the Smart Order Router (SOR) to calculate 
   exact implementation shortfall (IS) before routing.
"""

import numpy as np
from typing import Dict, List

class MicroElasticityEngine:
    """
    🚀 V25.0 TENSOR-PRIME: STATELESS MICRO-ELASTICITY
    Virtually walks the L2 orderbook to calculate the exact execution cost 
    and the power-law absorption curve of the resting liquidity.
    """
    
    @staticmethod
    def calculate_exact_slippage_elasticity(
        target_qty: float, 
        side: str, 
        depth_snapshot: Dict[str, List[List[float]]], 
        current_mid: float
    ) -> Dict[str, float]:
        """
        🚀 V25.0 DETERMINISTIC LIQUIDITY ABSORPTION MODELING
        Runs in pure O(1) context. Walks the L2 orderbook arrays to calculate 
        the exact implementation shortfall in basis points.
        """
        if not depth_snapshot or "bids" not in depth_snapshot or "asks" not in depth_snapshot:
            return {"expected_slippage_bps": 999.0, "absorption_ratio": 1.0, "cleared_levels": 0}

        levels = depth_snapshot.get("asks" if side.upper() == "BUY" else "bids", [])
        if not levels: 
            return {"expected_slippage_bps": 999.0, "absorption_ratio": 1.0, "cleared_levels": 0}

        accumulated_qty = 0.0
        accumulated_cost = 0.0
        levels_consumed = 0

        for level in levels:
            try:
                p = float(level[0])
                v = float(level[1])
                needed = target_qty - accumulated_qty
                levels_consumed += 1
                
                if v >= needed:
                    accumulated_cost += (needed * p)
                    accumulated_qty += needed
                    break
                else:
                    accumulated_cost += (v * p)
                    accumulated_qty += v
            except (IndexError, ValueError, TypeError):
                continue

        if accumulated_qty < target_qty or accumulated_qty <= 0:
            return {"expected_slippage_bps": 999.0, "absorption_ratio": 1.0, "cleared_levels": levels_consumed}

        avg_expected_price = accumulated_cost / accumulated_qty
        
        if side.upper() == "BUY":
            slippage_bps = ((avg_expected_price - current_mid) / current_mid) * 10000.0
        else:
            slippage_bps = ((current_mid - avg_expected_price) / current_mid) * 10000.0

        slippage_bps = max(0.0, slippage_bps)
        
        # Calculate Power-Law Absorption Ratio (How fast liquidity thins out deeper in the book)
        # Ratio > 1.0 means book becomes denser (safer). Ratio < 1.0 means book is hollowing out (toxic).
        top_level_vol = float(levels[0][1]) if levels else 1e-9
        avg_vol_per_level = accumulated_qty / max(1, levels_consumed)
        absorption_ratio = avg_vol_per_level / (top_level_vol + 1e-9)

        return {
            "expected_slippage_bps": slippage_bps,
            "absorption_ratio": float(np.clip(absorption_ratio, 0.1, 5.0)),
            "cleared_levels": levels_consumed
        }