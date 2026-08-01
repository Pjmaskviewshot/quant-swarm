"""
💎 V55.2 QUANTUM SWARM: O(1) VOLUME SYNCHRONIZED CLOCK (VPIN ENGINE)
---------------------------------------------------------------------
Measures Volume-Synchronized Probability of Toxicity (VPIN) and Institutional 
Orderbook Absorption. Operates in O(1) constant time with Dynamic Elastic Buckets
to prevent false toxicity spikes during high-volatility regimes.
"""

import time
import logging
import numpy as np
from collections import deque
from typing import Dict, Any, List

logger = logging.getLogger("QUANT_CORE.VPIN")

class VolumeSynchronizedClock:
    """
    🚀 V55.2 APEX: ELASTIC VOLUME SYNCHRONIZED CLOCK
    Upgraded with Dynamic Bucket Scaling to prevent high-volatility saturation
    and mathematical Running Sums to eliminate array traversals.
    """
    def __init__(self, bucket_volume: float = 1_000_000.0, window_size: int = 50, symbol: str = "GENERIC"):
        self.symbol = symbol
        self.base_bucket_volume = bucket_volume
        self.window_size = window_size
        
        self.current_bucket_buy_vol = 0.0
        self.current_bucket_sell_vol = 0.0
        self.current_bucket_total_vol = 0.0
        
        # Footprint and Absorption Tracking
        self.current_bucket_open_price = 0.0
        self.current_bucket_ticks = 0
        self.total_buckets_closed = 0
        
        # Deques for historical tracking
        self.bucket_imbalances = deque(maxlen=window_size)
        self.directional_imbalances = deque(maxlen=window_size)
        self.vpin_history = deque(maxlen=window_size * 2)
        
        # 🚀 V55.2 FIX: Tick Volume History for Dynamic Scaling
        self.volume_history = deque(maxlen=1000)

        # O(1) Running Sum Cache
        self._running_abs_imbalance = 0.0
        self._running_dir_imbalance = 0.0
        self._last_log_time = {}

    def _throttled_log(self, category: str, message: str, throttle_sec: float = 60.0):
        now = time.time()
        last = self._last_log_time.get(category, 0.0)
        if now - last > throttle_sec:
            self._last_log_time[category] = now
            logger.info(message)

    def process_tick(self, price: float, volume: float, is_buyer_maker: bool) -> List[Dict[str, Any]]:
        """
        Ingests raw exchange ticks. Handles 'Whale Overflow' by splitting 
        massive orders across multiple volume buckets mathematically.
        Returns a list of bucket manifests.
        """
        manifests = []
        remaining_volume = max(0.0, volume)
        self.volume_history.append(remaining_volume)

        # 🚀 V55.2 FIX: Dynamic Bucket Sizing. 
        # Smoothly adapts bucket size to current liquidity regime to prevent false VPIN spikes
        if len(self.volume_history) >= 100:
            avg_tick_vol = float(np.mean(self.volume_history))
            # Target ~75 ticks to fill a bucket for robust statistical significance
            dynamic_bucket_target = max(self.base_bucket_volume * 0.1, min(self.base_bucket_volume * 3.0, avg_tick_vol * 75.0))
        else:
            dynamic_bucket_target = self.base_bucket_volume

        # Fractional Tick Splitting Loop
        while remaining_volume > 0:
            if self.current_bucket_total_vol == 0.0:
                self.current_bucket_open_price = price
                self.current_bucket_ticks = 0

            available_space = dynamic_bucket_target - self.current_bucket_total_vol
            chunk_vol = min(remaining_volume, available_space)
            
            # is_buyer_maker = True means trade was initiated by a market seller
            if is_buyer_maker:
                self.current_bucket_sell_vol += chunk_vol
            else:
                self.current_bucket_buy_vol += chunk_vol
                
            self.current_bucket_total_vol += chunk_vol
            self.current_bucket_ticks += 1
            remaining_volume -= chunk_vol

            # Bucket Complete
            if self.current_bucket_total_vol >= dynamic_bucket_target:
                manifest = self._close_bucket(price, dynamic_bucket_target)
                if manifest.get("valid"):
                    manifests.append(manifest)

        return manifests

    def _close_bucket(self, current_price: float, dynamic_bucket_size: float) -> Dict[str, Any]:
        self.total_buckets_closed += 1
        
        buy_v = self.current_bucket_buy_vol
        sell_v = self.current_bucket_sell_vol
        price_delta = current_price - self.current_bucket_open_price
        
        abs_imbalance = abs(buy_v - sell_v)
        signed_imbalance = buy_v - sell_v
        
        # Maintain O(1) running sums
        if len(self.bucket_imbalances) == self.window_size:
            self._running_abs_imbalance -= self.bucket_imbalances[0]
            self._running_dir_imbalance -= self.directional_imbalances[0]

        self.bucket_imbalances.append(abs_imbalance)
        self.directional_imbalances.append(signed_imbalance)
        
        self._running_abs_imbalance += abs_imbalance
        self._running_dir_imbalance += signed_imbalance
        
        avg_trade_size = dynamic_bucket_size / max(1, self.current_bucket_ticks)

        # Reset bucket parameters
        self.current_bucket_buy_vol = 0.0
        self.current_bucket_sell_vol = 0.0
        self.current_bucket_total_vol = 0.0
        self.current_bucket_ticks = 0
        self.current_bucket_open_price = 0.0

        # Wait for window warm-up
        if len(self.bucket_imbalances) < self.window_size:
            return {"valid": False}

        divisor = (self.window_size * dynamic_bucket_size) + 1e-9
        
        vpin_score = self._running_abs_imbalance / divisor
        self.vpin_history.append(vpin_score)

        directional_bias = self._running_dir_imbalance / divisor

        # Calculate Anomaly Z-Score
        vpin_z_score = 0.0
        if len(self.vpin_history) >= 20:
            hist_array = np.array(self.vpin_history)
            mean = np.mean(hist_array)
            std = np.std(hist_array) + 1e-9
            vpin_z_score = float((vpin_score - mean) / std)

        # THE ABSORPTION DETECTOR
        is_absorption_anomaly = False
        if abs(directional_bias) >= 0.15:
            if directional_bias > 0 and price_delta <= 0:
                is_absorption_anomaly = True  # Heavy buying absorbed by hidden sell wall
                self._throttled_log(
                    f"absorp_{self.symbol}", 
                    f"[X-RAY] 🧊 ABSORPTION DETECTED // {self.symbol} | Heavy BUY flow absorbed by hidden sellers."
                )
            elif directional_bias < 0 and price_delta >= 0:
                is_absorption_anomaly = True  # Heavy selling absorbed by hidden buy wall
                self._throttled_log(
                    f"absorp_{self.symbol}", 
                    f"[X-RAY] 🧊 ABSORPTION DETECTED // {self.symbol} | Heavy SELL flow absorbed by hidden buyers."
                )

        if vpin_z_score > 2.5:
            self._throttled_log(
                f"vpin_spike_{self.symbol}", 
                f"[X-RAY] ⚡ VPIN TOXICITY SPIKE // {self.symbol} | Z-Score: {vpin_z_score:.2f} (Informed flow active)."
            )

        return {
            "valid": True,
            "vpin_score": round(vpin_score, 4),
            "vpin_z_score": round(vpin_z_score, 2),
            "directional_bias": round(directional_bias, 4),
            "suggested_direction": "BUY" if directional_bias > 0 else "SELL",
            "is_absorption_anomaly": is_absorption_anomaly,
            "avg_trade_size": round(avg_trade_size, 2),
            "dynamic_bucket_size": round(dynamic_bucket_size, 2),
            "current_price": current_price,
            "timestamp": time.time()
        }