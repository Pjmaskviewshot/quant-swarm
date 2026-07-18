import time
import math
import numpy as np
from collections import deque
from typing import Dict, Any, List

class VolumeSynchronizedClock:
    def __init__(self, bucket_volume: float = 1_000_000.0, window_size: int = 50):
        self.bucket_volume = bucket_volume
        self.window_size = window_size
        
        self.current_bucket_buy_vol = 0.0
        self.current_bucket_sell_vol = 0.0
        self.current_bucket_total_vol = 0.0
        
        # 🚀 APEX METRICS: Footprint and Absorption Tracking
        self.current_bucket_open_price = 0.0
        self.current_bucket_ticks = 0
        
        # 🛑 MANDATORY FOR MAIN.PY PIPELINE (Do not delete)
        self.total_buckets_closed = 0
        
        # Stores absolute imbalances for VPIN calculation
        self.bucket_imbalances = deque(maxlen=window_size)
        # Stores signed imbalances for directional bias
        self.directional_imbalances = deque(maxlen=window_size)
        # Stores VPIN history for anomaly detection (Z-score)
        self.vpin_history = deque(maxlen=window_size * 2)

    def process_tick(self, price: float, volume: float, is_buyer_maker: bool) -> List[Dict[str, Any]]:
        """
        Ingests raw exchange ticks. Handles 'Whale Overflow' by splitting 
        massive orders across multiple volume buckets mathematically.
        Returns a list of bucket manifests (usually empty or 1, but can be multiple on whale ticks).
        """
        manifests = []
        remaining_volume = volume

        # Fractional Tick Splitting Loop
        while remaining_volume > 0:
            # Initialize bucket open price on the very first drop of volume
            if self.current_bucket_total_vol == 0.0:
                self.current_bucket_open_price = price
                self.current_bucket_ticks = 0

            available_space = self.bucket_volume - self.current_bucket_total_vol
            chunk_vol = min(remaining_volume, available_space)
            
            # is_buyer_maker = True means the trade was initiated by a market seller
            if is_buyer_maker:
                self.current_bucket_sell_vol += chunk_vol
            else:
                self.current_bucket_buy_vol += chunk_vol
                
            self.current_bucket_total_vol += chunk_vol
            self.current_bucket_ticks += 1
            remaining_volume -= chunk_vol

            # The Clock Strikes: Bucket is exactly full
            if self.current_bucket_total_vol >= self.bucket_volume:
                manifest = self._close_bucket(price)
                if manifest["valid"]:
                    manifests.append(manifest)

        return manifests

    def _close_bucket(self, current_price: float) -> Dict[str, Any]:
        # 1. Update master execution counter for main.py cooldowns
        self.total_buckets_closed += 1
        
        # 2. Calculate Imbalances & Price Deltas
        buy_v = self.current_bucket_buy_vol
        sell_v = self.current_bucket_sell_vol
        price_delta = current_price - self.current_bucket_open_price
        
        abs_imbalance = abs(buy_v - sell_v)
        signed_imbalance = buy_v - sell_v
        
        self.bucket_imbalances.append(abs_imbalance)
        self.directional_imbalances.append(signed_imbalance)
        
        # 3. Calculate Institutional Footprint (Avg trade size per bucket)
        avg_trade_size = self.bucket_volume / max(1, self.current_bucket_ticks)

        # 4. Reset the clock for the next bucket
        self.current_bucket_buy_vol = 0.0
        self.current_bucket_sell_vol = 0.0
        self.current_bucket_total_vol = 0.0
        self.current_bucket_ticks = 0
        self.current_bucket_open_price = 0.0

        # 5. Wait for statistical significance
        if len(self.bucket_imbalances) < self.window_size:
            return {"valid": False}

        # 6. Calculate Toxicity (VPIN)
        total_imbalance = sum(self.bucket_imbalances)
        vpin_score = total_imbalance / (self.window_size * self.bucket_volume)
        self.vpin_history.append(vpin_score)

        # 7. Calculate Directional Bias (-1.0 to 1.0)
        total_directional = sum(self.directional_imbalances)
        directional_bias = total_directional / (self.window_size * self.bucket_volume)

        # 8. Calculate Anomaly Z-Score
        vpin_z_score = 0.0
        if len(self.vpin_history) >= 20:
            hist_array = np.array(self.vpin_history)
            mean = np.mean(hist_array)
            std = np.std(hist_array) + 1e-6
            vpin_z_score = (vpin_score - mean) / std

        # 🚀 9. THE ABSORPTION DETECTOR
        # If massive directional volume pushes into the market, but price moves the OPPOSITE way,
        # it proves an institutional limit wall is absorbing the retail flow.
        is_absorption_anomaly = False
        if abs(directional_bias) >= 0.15:  # Requires significant skew to trigger
            if directional_bias > 0 and price_delta <= 0:
                is_absorption_anomaly = True  # Heavy buying, but price dropped (Hidden Sellers)
            elif directional_bias < 0 and price_delta >= 0:
                is_absorption_anomaly = True  # Heavy selling, but price rose (Hidden Buyers)

        return {
            "valid": True,
            "vpin_score": round(vpin_score, 4),
            "vpin_z_score": round(vpin_z_score, 2),
            "directional_bias": round(directional_bias, 4),
            "suggested_direction": "BUY" if directional_bias > 0 else "SELL",
            "is_absorption_anomaly": is_absorption_anomaly,
            "avg_trade_size": round(avg_trade_size, 2),
            "current_price": current_price,
            "timestamp": time.time()
        }