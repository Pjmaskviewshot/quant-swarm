import math
import numpy as np
import logging
from collections import deque

logger = logging.getLogger("QUANT_CORE.TENSOR_ORACLE")

class CrossAssetTensorOracle:
    """
    🌌 V29.1 APEX: STRICT LEAD-LAG TENSOR MATRIX
    Computes real-time cross-asset impulse propagation.
    Upgraded: Uses exact Exchange Timestamps (floored to 1-second bins) 
    and strict [t-1] lagging to entirely eradicate Look-Ahead Bias.
    """
    def __init__(self, history_len: int = 300):
        # Stores tuples of (1-second-binned-timestamp, last_price_in_bin)
        self.btc_prices = deque(maxlen=history_len)
        self.alt_prices = {}
        self.history_len = history_len

    def ingest_tick(self, symbol: str, price: float, exchange_timestamp: float):
        """Stores real-time tick prices, aligned by strict 1-second bins."""
        binned_ts = int(exchange_timestamp)
        
        if symbol == "BTCUSDT":
            # Update the price for the current second bin, or append a new one
            if self.btc_prices and self.btc_prices[-1][0] == binned_ts:
                self.btc_prices[-1] = (binned_ts, price)
            else:
                self.btc_prices.append((binned_ts, price))
        else:
            if symbol not in self.alt_prices:
                self.alt_prices[symbol] = deque(maxlen=self.history_len)
                
            alt_deque = self.alt_prices[symbol]
            if alt_deque and alt_deque[-1][0] == binned_ts:
                alt_deque[-1] = (binned_ts, price)
            else:
                alt_deque.append((binned_ts, price))

    def compute_lead_lag_signal(self, target_symbol: str) -> float:
        """
        Calculates cross-covariance tensor. 
        Strictly maps BTC[t-1] to ALT[t] to guarantee no future data leakage.
        """
        if target_symbol == "BTCUSDT" or target_symbol not in self.alt_prices:
            return 0.0
            
        btc_p = list(self.btc_prices)
        alt_p = list(self.alt_prices[target_symbol])
        
        if len(btc_p) < 30 or len(alt_p) < 30: 
            return 0.0
            
        # 1. Align time series based on exact exchange timestamps
        aligned_b = []
        aligned_a = []
        
        # We need alt prices and the BTC price from exactly 1 second BEFORE it
        btc_dict = {ts: price for ts, price in btc_p}
        
        for i in range(1, len(alt_p)):
            alt_ts, alt_price = alt_p[i]
            prev_alt_price = alt_p[i-1][1]
            
            # 🛡️ Look-ahead Bias Prevention: We look for BTC's price at alt_ts - 1
            # If not exactly found, we look at alt_ts - 2. Never current or future.
            lagged_btc_price = btc_dict.get(alt_ts - 1) or btc_dict.get(alt_ts - 2)
            prev_lagged_btc_price = btc_dict.get(alt_ts - 2) or btc_dict.get(alt_ts - 3)
            
            if lagged_btc_price and prev_lagged_btc_price:
                a_ret = math.log(alt_price / (prev_alt_price + 1e-9))
                b_ret = math.log(lagged_btc_price / (prev_lagged_btc_price + 1e-9))
                
                aligned_a.append(a_ret)
                aligned_b.append(b_ret)
                
        if len(aligned_a) < 20:
            return 0.0

        # 2. Compute true lagged Pearson correlation
        correlation = np.corrcoef(aligned_b, aligned_a)[0, 1]
        if np.isnan(correlation):
            return 0.0
            
        # 3. Compute leading momentum vector from BTC
        btc_momentum = np.mean(aligned_b[-10:])
        
        if abs(btc_momentum) > 0.0002 and correlation > 0.60:
            alpha_signal = np.sign(btc_momentum) * min(1.0, abs(correlation))
            return float(alpha_signal)
            
        return 0.0