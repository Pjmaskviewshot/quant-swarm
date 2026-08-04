"""
💎 V56.2 QUANTUM SWARM: INSTITUTIONAL TOXICITY MONITOR
---------------------------------------------------------------------
Replaces legacy volume-synchronized buckets (VPIN) with a lightning-fast
Lag-1 Trade-Sign Autocorrelation engine. Detects predatory order flow sweeping
and adverse selection in real-time, operating in true O(1) complexity.
"""

import time
import logging
import numpy as np
import math
from collections import deque
from typing import Dict, Any, List

logger = logging.getLogger("QUANT_CORE.TOXICITY_MONITOR")

class TradeSignToxicityMonitor:
    """
    🚀 V56.2 UPGRADE: Institutional Trade-Sign Autocorrelation.
    Measures order flow toxicity by calculating the 1st-order autocorrelation of trade signs,
    replacing the lagging VPIN architecture to detect algorithmic sweeping instantly.
    """
    def __init__(self, bucket_volume: float = 1_000_000.0, window_size: int = 50, symbol: str = "GENERIC"):
        # We accept bucket_volume to safely absorb legacy kwargs from main.py
        self.symbol = symbol
        self.window_size = window_size
        
        self.trade_signs = deque(maxlen=window_size)
        self.toxicity_z_history = deque(maxlen=200)
        
        # Backward compatibility deque for main.py fallback logic
        self.vpin_history = deque(maxlen=200) 

    def process_tick(self, price: float, volume: float, is_buyer_maker: bool) -> List[Dict[str, Any]]:
        """
        Ingests raw exchange ticks. Evaluates the binary sequence of trade signs to flag
        highly correlated (predatory/algorithmic) order flow.
        """
        # If buyer is maker, the taker was a SELLER (-1). Otherwise BUYER (+1).
        sign = -1.0 if is_buyer_maker else 1.0
        self.trade_signs.append(sign)

        if len(self.trade_signs) < 10:
            return [{"valid": False}]

        # Calculate 1st-order autocorrelation (Lag-1)
        signs_array = np.array(self.trade_signs)
        mean_sign = np.mean(signs_array)
        var_sign = np.var(signs_array)

        # If variance is zero (e.g., all trades are exactly the same size/direction),
        # the flow is highly toxic/artificial, but math requires a safeguard.
        if var_sign == 0:
            return [{"valid": False}]

        # np.correlate mode='full' calculates cross-correlation. We extract the Lag-1 index.
        autocorr = np.correlate(signs_array - mean_sign, signs_array - mean_sign, mode='full')
        lag_1_corr = autocorr[len(signs_array)] / (var_sign * len(signs_array))

        # Exponential transformation to scale toxicity based on directional magnitude
        toxicity_raw = lag_1_corr * math.exp(abs(mean_sign)) 
        
        self.toxicity_z_history.append(toxicity_raw)
        self.vpin_history.append(toxicity_raw) # Append to legacy deque for FSM access

        if len(self.toxicity_z_history) >= 20:
            hist_array = np.array(self.toxicity_z_history)
            mean_tox = np.mean(hist_array)
            std_tox = np.std(hist_array) + 1e-9
            
            # Final Z-Score representing current orderbook toxicity
            z_score = float((toxicity_raw - mean_tox) / std_tox)
            
            # Log extreme toxicity spikes for X-Ray Telemetry
            if z_score > 2.5:
                logger.info(f"[X-RAY] ⚡ HIGH-SPEED TOXICITY SPIKE // {self.symbol} | Z-Score: {z_score:.2f} (Predatory algorithmic sweeping detected).")
                
            return [{"valid": True, "vpin_z_score": z_score}]
            
        return [{"valid": False}]


# 🚀 V56.2 ALIAS: Seamless drop-in replacement to prevent import crashes in main.py
VolumeSynchronizedClock = TradeSignToxicityMonitor