"""
ðŸ’Ž V1.0 TITANIUM APEX: STATIONARIZED LOG-MLOFI ENGINE
-------------------------------------------------------
Calculates Spoof-Resistant Logarithmic Order Flow Imbalance across depth levels.
Dampens institutional quote manipulation (fake walls) while preserving genuine 
aggressor intent using a logarithmic transformation and exponential depth decay.
"""

import math
import numpy as np
import logging
from collections import deque
from typing import List

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

        self.log_mlofi_history = deque(maxlen=window_size)
        self.log_mlofi_z_history = deque(maxlen=window_size)
        
        self.current_z_score = 0.0
        self.current_raw_mlofi = 0.0

    def update_depth(self, bids: List[List[float]], asks: List[List[float]]) -> float:
        """
        Processes L2 depth updates using logarithmic volume damping.
        Calculates the instantaneous Log-MLOFI Z-Score.
        
        Args:
            bids: List of [price, size] for bids.
            asks: List of [price, size] for asks.
            
        Returns:
            float: The rolling Log-MLOFI Z-Score.
        """
        if not self.prev_bids or not self.prev_asks:
            self.prev_bids = bids[:self.depth_levels]
            self.prev_asks = asks[:self.depth_levels]
            self.log_mlofi_history.append(0.0)
            self.log_mlofi_z_history.append(0.0)
            return 0.0

        curr_bids = bids[:self.depth_levels]
        curr_asks = asks[:self.depth_levels]

        limit = min(self.depth_levels, len(curr_bids), len(self.prev_bids), len(curr_asks), len(self.prev_asks))
        log_mlofi_t = 0.0

        for i in range(limit):
            try:
                c_bp, c_bv = float(curr_bids[i][0]), float(curr_bids[i][1])
                p_bp, p_bv = float(self.prev_bids[i][0]), float(self.prev_bids[i][1])

                # ðŸš€ LOGARITHMIC BID VOLUME DELTA
                if c_bp > p_bp:
                    delta_bid = math.log1p(c_bv)
                elif c_bp == p_bp:
                    delta_bid = math.log1p(c_bv) - math.log1p(p_bv)
                else:
                    delta_bid = -math.log1p(p_bv)

                c_ap, c_av = float(curr_asks[i][0]), float(curr_asks[i][1])
                p_ap, p_av = float(self.prev_asks[i][0]), float(self.prev_asks[i][1])

                # ðŸš€ LOGARITHMIC ASK VOLUME DELTA
                if c_ap < p_ap:
                    delta_ask = math.log1p(c_av)
                elif c_ap == p_ap:
                    delta_ask = math.log1p(c_av) - math.log1p(p_av)
                else:
                    delta_ask = -math.log1p(p_av)

                # Apply exponential decay to deeper levels to prioritize top-of-book
                weight = math.exp(-self.decay_alpha * i)
                log_mlofi_t += (delta_bid - delta_ask) * weight

            except (IndexError, ValueError, TypeError):
                continue

        self.prev_bids = curr_bids
        self.prev_asks = curr_asks
        self.log_mlofi_history.append(log_mlofi_t)
        self.current_raw_mlofi = log_mlofi_t

        # ðŸš€ ROLLING Z-SCORE CALCULATION
        if len(self.log_mlofi_history) >= 20:
            # Using NumPy for fast array variance computation
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