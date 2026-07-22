import numpy as np
from collections import deque
import math
import logging
import time
from typing import List

logger = logging.getLogger("QUANT_CORE.EDGE_GATE")

class MicrostructureEdgeGate:
    def __init__(self, window_size=100, mlofi_levels=5, decay_alpha=0.5):
        """
        🚀 V27.2 SIGNAL APEX: CALIBRATED EDGE GATE
        Fixed Amihud calculation to use Notional Volume buckets ($2000+) to prevent 
        micro-tick noise from locking the execution engine.
        """
        self.window_size = window_size
        self.mlofi_levels = mlofi_levels
        self.decay_alpha = decay_alpha  
        
        self.prices = deque(maxlen=window_size)
        self.ofis = deque(maxlen=window_size)     
        self.mlofis = deque(maxlen=window_size)   
        self.lambda_history = deque(maxlen=window_size)
        self.amihud_history = deque(maxlen=window_size)
        
        self.prev_bids = []
        self.prev_asks = []
        self.rolling_volume = 0.0
        self.amihud_anchor_price = 0.0
        
        self._last_log_time = {}

    def _throttled_warn(self, category: str, message: str, throttle_sec: float = 60.0):
        now = time.time()
        last = self._last_log_time.get(category, 0.0)
        if now - last > throttle_sec:
            self._last_log_time[category] = now
            logger.warning(message)

    def update_trade_volume(self, volume: float):
        self.rolling_volume += volume

    def update_orderbook_state(self, bids: List[List[float]], asks: List[List[float]], mid_price: float):
        if not self.prev_bids or not self.prev_asks:
            self.prev_bids = bids[:self.mlofi_levels]
            self.prev_asks = asks[:self.mlofi_levels]
            self.prices.append(mid_price)
            self.ofis.append(0.0)
            self.mlofis.append(0.0)
            self.amihud_anchor_price = mid_price
            return

        current_bids = bids[:self.mlofi_levels]
        current_asks = asks[:self.mlofi_levels]
        
        mlofi_t = 0.0
        l1_ofi_t = 0.0

        limit = min(self.mlofi_levels, len(current_bids), len(self.prev_bids), len(current_asks), len(self.prev_asks))
        
        for i in range(limit):
            try:
                curr_bid_p, curr_bid_s = float(current_bids[i][0]), float(current_bids[i][1])
                prev_bid_p, prev_bid_s = float(self.prev_bids[i][0]), float(self.prev_bids[i][1])
                
                delta_bid = 0.0
                if curr_bid_p > prev_bid_p: delta_bid = curr_bid_s
                elif curr_bid_p == prev_bid_p: delta_bid = curr_bid_s - prev_bid_s
                else: delta_bid = -prev_bid_s

                curr_ask_p, curr_ask_s = float(current_asks[i][0]), float(current_asks[i][1])
                prev_ask_p, prev_ask_s = float(self.prev_asks[i][0]), float(self.prev_asks[i][1])
                
                delta_ask = 0.0
                if curr_ask_p < prev_ask_p: delta_ask = curr_ask_s
                elif curr_ask_p == prev_ask_p: delta_ask = curr_ask_s - prev_ask_s
                else: delta_ask = -prev_ask_s

                level_ofi = delta_bid - delta_ask
                weight = math.exp(-self.decay_alpha * i)
                mlofi_t += level_ofi * weight
                
                if i == 0:
                    l1_ofi_t = level_ofi
                    
            except (IndexError, ValueError, TypeError):
                continue

        self.ofis.append(l1_ofi_t)
        self.mlofis.append(mlofi_t)
        self.prices.append(mid_price)

        self.prev_bids = current_bids
        self.prev_asks = current_asks
        
        # 🚀 V27.2 FIX: Stable Notional Volume Buckets for Amihud Ratio
        notional_vol = self.rolling_volume * mid_price
        if notional_vol >= 2000.0:
            if self.amihud_anchor_price > 0:
                price_change = abs(math.log(mid_price / (self.amihud_anchor_price + 1e-9)))
                illiquidity = price_change / notional_vol
                self.amihud_history.append(illiquidity)
            
            # Reset bucket
            self.rolling_volume = 0.0
            self.amihud_anchor_price = mid_price

        if len(self.prices) >= 20 and len(self.prices) % 10 == 0:
            lmbda = self._calculate_instantaneous_lambda()
            if lmbda > 0:
                self.lambda_history.append(lmbda)

    def _calculate_instantaneous_lambda(self) -> float:
        p_array = np.array(self.prices)
        dp = np.diff(p_array)
        ofi_array = np.array(self.mlofis)[1:] 
        
        if np.std(ofi_array) == 0: return 0.0
            
        variance = np.var(ofi_array)
        if variance == 0: return 0.0
            
        covariance = np.cov(ofi_array, dp)[0][1]
        return max(0.0, covariance / (variance + 1e-9))

    def compute_roll_spread(self) -> float:
        if len(self.prices) < 10: return 0.0
        p_array = np.array(self.prices)
        dp = np.diff(p_array)
        if len(dp) < 3: return 0.0
        
        cov = np.cov(dp[1:], dp[:-1])[0][1]
        if cov >= 0: return 0.0 
        return 2.0 * math.sqrt(-cov)

    def evaluate_structural_edge(self, symbol: str, vpin_z: float) -> dict:
        if len(self.mlofis) < 20 or len(self.lambda_history) < 5:
            return {"action": "HOLD", "confidence": 0.0, "reasoning": "CALIBRATING_DEEP_BOOK"}

        current_mlofi = np.mean(list(self.mlofis)[-5:])
        mlofi_std = np.std(self.mlofis)
        
        if mlofi_std == 0 or abs(current_mlofi) < (mlofi_std * 0.5):
            return {"action": "HOLD", "confidence": 0.0, "reasoning": "MLOFI_FLAT"}

        direction = "BUY" if current_mlofi > 0 else "SELL"
        
        current_lambda = self._calculate_instantaneous_lambda()
        baseline_lambda = np.mean(self.lambda_history)
        roll_spread = self.compute_roll_spread()
        
        # 0. 🕳️ AMIHUD LIQUIDITY VACUUM CHECK
        if len(self.amihud_history) >= 10:
            current_amihud = self.amihud_history[-1]
            amihud_mean = np.mean(list(self.amihud_history)[-10:])
            if amihud_mean > 0 and current_amihud > (amihud_mean * 4.0):
                self._throttled_warn(f"vacuum_{symbol}", f"🕳️ LIQUIDITY VACUUM // {symbol} | Stable Amihud spike detected.")
                return {"action": "HOLD", "confidence": 0.0, "reasoning": f"AMIHUD_LIQUIDITY_VACUUM | Spike: {current_amihud/max(1e-9, amihud_mean):.1f}x"}

        # 1. 🧊 HIDDEN WHALE ABSORPTION (DEEP BOOK TRAP)
        if abs(current_mlofi) > (mlofi_std * 3.0) and current_lambda < (baseline_lambda * 0.1):
            self._throttled_warn(f"iceberg_{symbol}", f"🧊 DEEP ICEBERG WALL DETECTED // {symbol} | Extreme MLOFI Surge completely absorbed.")
            return {
                "action": "HOLD", 
                "confidence": 0.0, 
                "reasoning": f"DEEP_BOOK_ABSORPTION | MLOFI_Z: {abs(current_mlofi)/max(1e-9, mlofi_std):.2f}"
            }

        # 2. 📉 RETAIL NOISE BOUNCE
        if roll_spread > 0 and current_lambda < baseline_lambda:
            return {"action": "HOLD", "confidence": 0.0, "reasoning": f"RETAIL_CHOP | Roll Spread: {roll_spread:.6f}"}

        # 3. 🚀 TOXIC INSTITUTIONAL BREAKOUT
        if abs(vpin_z) >= 1.5 and current_lambda >= (baseline_lambda * 0.8):
            lambda_expansion = min(1.5, current_lambda / max(baseline_lambda, 1e-9))
            confidence = min(0.99, 0.50 + (lambda_expansion * 0.20) + (abs(vpin_z) * 0.05))
            
            return {
                "action": direction,
                "confidence": confidence,
                "reasoning": f"DEEP_BOOK_BREAKOUT | Elasticity: {lambda_expansion:.2f}x, MLOFI confirms {direction}"
            }

        return {"action": "HOLD", "confidence": 0.0, "reasoning": "EDGE_GATE_UNDECIDED"}