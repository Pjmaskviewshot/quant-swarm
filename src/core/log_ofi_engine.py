"""
💎 V1.1 TITANIUM APEX: STATIONARIZED LOG-MLOFI ENGINE
-------------------------------------------------------
Calculates Spoof-Resistant Logarithmic Order Flow Imbalance across depth levels.
Upgraded with Price-Coordinate Hash Mapping (Cont-Kukanov-Stoikov formulation)
to eliminate index-shifted array corruption during order book insertions.
"""

import math
import numpy as np
import logging
from collections import deque
from typing import List, Dict

logger = logging.getLogger("QUANT_CORE.LOG_OFI")

class StationarizedLogOFI:
    """
    Computes Log-MLOFI (Multi-Level Order Flow Imbalance) and its rolling Z-Score.
    Provides deep-book spoofing immunity for altcoins and highly skewed orderbooks.
    """
    def __init__(self, depth_levels: int = 5, decay_alpha: float = 0.40, window_size: int = 100):
        self.depth_levels = depth_levels
        self.decay_alpha = decay_alpha
        self.window_size = window_size

        self.prev_bids: List[List[float]] = []
        self.prev_asks: List[List[float]] = []
        
        # 🚀 V1.1 Hash Maps for absolute price-level tracking
        self.prev_bid_map: Dict[float, float] = {}
        self.prev_ask_map: Dict[float, float] = {}

        self.log_mlofi_history = deque(maxlen=window_size)
        self.log_mlofi_z_history = deque(maxlen=window_size)
        
        self.current_z_score = 0.0
        self.current_raw_mlofi = 0.0

    def update_depth(self, bids: List[List[float]], asks: List[List[float]]) -> float:
        """
        Processes L2 depth updates using logarithmic volume damping.
        Calculates the instantaneous Log-MLOFI Z-Score based on exact price coordinates.
        
        Args:
            bids: List of [price, size] for bids.
            asks: List of [price, size] for asks.
            
        Returns:
            float: The rolling Log-MLOFI Z-Score.
        """
        if not bids or not asks:
            return self.current_z_score
            
        try:
            best_bid = float(bids[0][0])
            best_ask = float(asks[0][0])
            mid_price = (best_bid + best_ask) / 2.0
        except (IndexError, ValueError, TypeError):
            return self.current_z_score

        # Hash map conversion for exact level-to-level matching
        curr_bid_map = {float(p): float(v) for p, v in bids[:self.depth_levels]}
        curr_ask_map = {float(p): float(v) for p, v in asks[:self.depth_levels]}

        if not self.prev_bid_map or not self.prev_ask_map:
            self.prev_bid_map = curr_bid_map
            self.prev_ask_map = curr_ask_map
            self.prev_bids = bids[:self.depth_levels]
            self.prev_asks = asks[:self.depth_levels]
            self.log_mlofi_history.append(0.0)
            self.log_mlofi_z_history.append(0.0)
            return 0.0

        log_mlofi_t = 0.0

        # 🚀 LOGARITHMIC BID VOLUME DELTA (Coordinate Mapped)
        all_bid_prices = set(curr_bid_map.keys()) | set(self.prev_bid_map.keys())
        for p in all_bid_prices:
            curr_v = curr_bid_map.get(p, 0.0)
            prev_v = self.prev_bid_map.get(p, 0.0)
            
            delta_bid = math.log1p(curr_v) - math.log1p(prev_v)
            
            # Decay weight mapped physically to distance from mid-price (assumes ~5bps per level)
            dist_bps = abs(p - mid_price) / (mid_price + 1e-9) * 10000.0
            weight = math.exp(-self.decay_alpha * (dist_bps / 5.0))
            
            log_mlofi_t += delta_bid * weight

        # 🚀 LOGARITHMIC ASK VOLUME DELTA (Coordinate Mapped)
        all_ask_prices = set(curr_ask_map.keys()) | set(self.prev_ask_map.keys())
        for p in all_ask_prices:
            curr_v = curr_ask_map.get(p, 0.0)
            prev_v = self.prev_ask_map.get(p, 0.0)
            
            delta_ask = math.log1p(curr_v) - math.log1p(prev_v)
            
            dist_bps = abs(p - mid_price) / (mid_price + 1e-9) * 10000.0
            weight = math.exp(-self.decay_alpha * (dist_bps / 5.0))
            
            log_mlofi_t -= delta_ask * weight

        # Update cache
        self.prev_bid_map = curr_bid_map
        self.prev_ask_map = curr_ask_map
        self.prev_bids = bids[:self.depth_levels]
        self.prev_asks = asks[:self.depth_levels]
        
        self.log_mlofi_history.append(log_mlofi_t)
        self.current_raw_mlofi = log_mlofi_t

        # 🚀 ROLLING Z-SCORE CALCULATION
        if len(self.log_mlofi_history) >= 20:
            arr = np.array(self.log_mlofi_history)
            mean = np.mean(arr)
            std = np.std(arr) + 1e-9
            self.current_z_score = float((log_mlofi_t - mean) / std)
        else:
            self.current_z_score = 0.0

        self.log_mlofi_z_history.append(self.current_z_score)
        
        return self.current_z_score

    def get_latest_metrics(self) -> dict:
        """Returns the most recent Log-MLOFI metrics for downstream feature fusion."""
        return {
            "log_mlofi_raw": self.current_raw_mlofi,
            "log_mlofi_z": self.current_z_score,
            "is_calibrated": len(self.log_mlofi_history) >= 20
        }