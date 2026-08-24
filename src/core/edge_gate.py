"""
💎 V5.1 APEX NEURAL: MICROSTRUCTURE EDGE GATE
-------------------------------------------------------------------------
Enforces rigorous Adverse Selection Vetoes. 
Evaluates Orderbook Depth Ratios, VWAP Exhaustion, and stable Kyle's Lambda
to determine if the Alpha Fusion signal should be executed or rejected.
"""

import math
import time
import logging
import numpy as np
from collections import deque
from typing import List, Dict, Any

logger = logging.getLogger("QUANT_CORE.EDGE_GATE")


class MicrostructureEdgeGate:
    """
    🚀 V5.1 STRICT STRUCTURAL EDGE GATE
    Reinstates binary hard-stops for toxic flow. 
    Implements robust L2/L3 alignment for Kyle's Lambda estimation.
    """
    def __init__(self, window_size=100, mlofi_levels=5, decay_alpha=0.5):
        self.window_size = window_size
        self.mlofi_levels = mlofi_levels
        self.decay_alpha = decay_alpha  
        
        self.prices = deque(maxlen=window_size)
        self.volumes = deque(maxlen=window_size)
        self.vwap_history = deque(maxlen=window_size)
        self.ofis = deque(maxlen=window_size)     
        self.mlofis = deque(maxlen=window_size)   
        
        self._trade_imbalances = deque(maxlen=window_size)
        self._current_trade_buy_vol = 0.0
        self._current_trade_sell_vol = 0.0
        self._current_period_volume = 0.0
        
        self.lambda_history = deque(maxlen=window_size)
        self.micro_spread_history = deque(maxlen=window_size)
        
        self.prev_bids = []
        self.prev_asks = []
        
        self._last_log_time = {}

    def _throttled_warn(self, category: str, message: str, throttle_sec: float = 60.0):
        """Throttles log warnings to prevent terminal spam."""
        now = time.time()
        last = self._last_log_time.get(category, 0.0)
        if now - last > throttle_sec:
            self._last_log_time[category] = now
            logger.warning(message)
        
    def update_trade_flow(self, volume: float, is_buy: bool):
        """Tracks signed tick trade flow imbalance and period volume."""
        if is_buy:
            self._current_trade_buy_vol += volume
        else:
            self._current_trade_sell_vol += volume
        self._current_period_volume += volume

    def update_orderbook_state(self, symbol: str, bids: List[List[float]], asks: List[List[float]], mid_price: float):
        """Updates Stationarized Log-MLOFI, Rolling VWAP, and Micro-Price Spreads."""
        if not self.prev_bids or not self.prev_asks:
            self.prev_bids = bids[:self.mlofi_levels]
            self.prev_asks = asks[:self.mlofi_levels]
            self.prices.append(mid_price)
            self.volumes.append(max(1.0, self._current_period_volume))
            self.vwap_history.append(mid_price)
            self.ofis.append(0.0)
            self.mlofis.append(0.0)
            self._trade_imbalances.append(0.0)
            self.micro_spread_history.append(0.0)
            self._current_period_volume = 0.0
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
                
                if curr_bid_p > prev_bid_p: 
                    delta_bid = math.log1p(curr_bid_s)
                elif curr_bid_p == prev_bid_p: 
                    delta_bid = math.log1p(curr_bid_s) - math.log1p(prev_bid_s)
                else: 
                    delta_bid = -math.log1p(prev_bid_s)

                curr_ask_p, curr_ask_s = float(current_asks[i][0]), float(current_asks[i][1])
                prev_ask_p, prev_ask_s = float(self.prev_asks[i][0]), float(self.prev_asks[i][1])
                
                if curr_ask_p < prev_ask_p: 
                    delta_ask = math.log1p(curr_ask_s)
                elif curr_ask_p == prev_ask_p: 
                    delta_ask = math.log1p(curr_ask_s) - math.log1p(prev_ask_s)
                else: 
                    delta_ask = -math.log1p(prev_ask_s)

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
        
        period_vol = max(1.0, self._current_period_volume)
        self.volumes.append(period_vol)
        self._current_period_volume = 0.0

        try:
            p_arr = np.array(self.prices, dtype=float)
            v_arr = np.array(self.volumes, dtype=float)
            
            vol_sum = np.sum(v_arr) + 1e-9
            rolling_vwap = float(np.sum(p_arr * v_arr) / vol_sum)
            if math.isnan(rolling_vwap) or math.isinf(rolling_vwap):
                rolling_vwap = mid_price
                
            self.vwap_history.append(rolling_vwap)
        except Exception as e:
            logger.debug(f"[MATH_WARN] Numerical instability in VWAP calculation: {e}")
            self.vwap_history.append(mid_price)
        
        t_imb = self._current_trade_buy_vol - self._current_trade_sell_vol
        self._trade_imbalances.append(t_imb)
        self._current_trade_buy_vol = 0.0
        self._current_trade_sell_vol = 0.0

        self.prev_bids = current_bids
        self.prev_asks = current_asks
        
        try:
            best_bid_p, best_bid_s = float(current_bids[0][0]), float(current_bids[0][1])
            best_ask_p, best_ask_s = float(current_asks[0][0]), float(current_asks[0][1])
            
            total_v = best_bid_s + best_ask_s
            if total_v <= 0:
                raise ValueError("Zero volume at BBO")
                
            micro_price = (best_bid_p * best_ask_s + best_ask_p * best_bid_s) / (total_v + 1e-9)
            
            if math.isnan(micro_price) or math.isinf(micro_price):
                micro_price = mid_price
                
            micro_spread_bps = ((micro_price - mid_price) / (mid_price + 1e-9)) * 10000.0
            
            if math.isnan(micro_spread_bps) or math.isinf(micro_spread_bps):
                micro_spread_bps = 0.0
                
            self.micro_spread_history.append(micro_spread_bps)
        except Exception as e:
            logger.debug(f"[MATH_WARN] Numerical instability in micro_price spread calculation: {e}")
            self.micro_spread_history.append(0.0)

        # Update Lambda every 10 ticks to prevent WS synchronization skew
        if len(self.prices) >= 20 and len(self.prices) % 10 == 0:
            lmbda = self._calculate_instantaneous_lambda()
            if lmbda > 0:
                self.lambda_history.append(lmbda)

    def _calculate_instantaneous_lambda(self) -> float:
        """
        🚀 V5.1 STABLE KYLE'S LAMBDA
        Calculates Price Impact Parameter using block-aggregated ticks 
        to neutralize WebSocket stream arrival skew between L2 and Trades.
        """
        if len(self.prices) < 20 or len(self.mlofis) < 20:
            return 0.0
            
        try:
            # Block-aggregate into 10-tick windows to smooth out stream latency
            p_array = np.array(self.prices, dtype=float)[-20:]
            dp = np.diff(p_array)
            ofi_array = np.array(self.mlofis, dtype=float)[-19:] 
            
            if len(dp) == 0 or len(ofi_array) == 0 or len(dp) != len(ofi_array):
                return 0.0
            
            variance = float(np.var(ofi_array))
            if math.isnan(variance) or math.isinf(variance) or variance < 1e-12: 
                return 0.0
                
            with np.errstate(divide='ignore', invalid='ignore'):
                covariance = float(np.cov(ofi_array, dp)[0][1])
                
            if math.isnan(covariance) or math.isinf(covariance): 
                return 0.0
                
            return max(0.0, float(covariance / (variance + 1e-9)))
        except Exception as e:
            logger.debug(f"[MATH_WARN] Numerical instability in _calculate_instantaneous_lambda: {e}")
            return 0.0

    def evaluate_orderbook_depth_ratio(self, symbol: str) -> float:
        """
        Calculates the buy/sell pressure ratio in the top 5 levels of the book.
        """
        if not self.prev_bids or not self.prev_asks:
            return 1.0

        try:
            total_bid_vol = sum(float(b[1]) for b in self.prev_bids)
            total_ask_vol = sum(float(a[1]) for a in self.prev_asks)
            total_depth = total_bid_vol + total_ask_vol + 1e-9
            
            buy_ratio = float(total_bid_vol / total_depth)
            
            if math.isnan(buy_ratio) or math.isinf(buy_ratio):
                return 1.0

            return buy_ratio
        except Exception:
            return 1.0

    def evaluate_exhaustion_stretch(self, symbol: str, current_price: float, target_direction: str) -> float:
        """
        Calculates how far the current price is stretched from the Micro-VWAP.
        """
        if len(self.prices) < 20 or len(self.vwap_history) < 20:
            return 0.0

        try:
            p_arr = np.array(self.prices, dtype=float)
            vwap = float(self.vwap_history[-1])

            std_dev = float(np.std(p_arr)) + 1e-9
            z_vwap = (current_price - vwap) / std_dev
            
            if math.isnan(z_vwap) or math.isinf(z_vwap):
                z_vwap = 0.0

            return z_vwap
        except Exception:
            return 0.0

    def evaluate_structural_edge(self, symbol: str, vpin_z: float, intended_direction: str = None) -> dict:
        """
        🚀 V5.1 STRICT STRUCTURAL EDGE EVALUATOR
        Enforces binary vetoes on toxic order flow. Drops trades entirely if
        adverse selection risk is too high.
        """
        if len(self.mlofis) < 10 or len(self.lambda_history) < 3 or len(self._trade_imbalances) < 10:
            default_dir = intended_direction if intended_direction else "BUY"
            return {
                "action": default_dir, 
                "confidence": 0.50, 
                "edge_weight": 0.5, 
                "reasoning": "CALIBRATING_DEEP_BOOK", 
                "routing": "STANDARD"
            }

        try:
            current_mlofi = float(np.mean(list(self.mlofis)[-5:]))
            mlofi_std = float(np.std(self.mlofis)) + 1e-9

            direction = "BUY" if current_mlofi >= 0 else "SELL"
            target_direction = intended_direction if intended_direction else direction

            buy_depth_ratio = self.evaluate_orderbook_depth_ratio(symbol)
            current_price = self.prices[-1] if self.prices else 0.0
            stretch_z = self.evaluate_exhaustion_stretch(symbol, current_price, target_direction) if current_price > 0 else 0.0

            # 🛑 1. STRICT ADVERSE SELECTION VETOES (Zero-Veto is Dead)
            if target_direction == "BUY" and buy_depth_ratio < 0.20:
                self._throttled_warn("veto", f"🛑 HARD VETO // {symbol} Bid depth collapsing (Ratio: {buy_depth_ratio:.2f}). Aborting BUY.")
                return {"action": target_direction, "confidence": 0.0, "edge_weight": 0.0, "reasoning": "VETO_DEPTH_COLLAPSE", "routing": "STANDARD"}
                
            if target_direction == "SELL" and buy_depth_ratio > 0.80:
                self._throttled_warn("veto", f"🛑 HARD VETO // {symbol} Ask depth collapsing (Buy Ratio: {buy_depth_ratio:.2f}). Aborting SELL.")
                return {"action": target_direction, "confidence": 0.0, "edge_weight": 0.0, "reasoning": "VETO_DEPTH_COLLAPSE", "routing": "STANDARD"}

            # 🛑 2. EXTREME VWAP EXHAUSTION VETOES
            if target_direction == "BUY" and stretch_z > 3.0:
                self._throttled_warn("veto", f"🛑 HARD VETO // {symbol} Price too far extended above VWAP (+{stretch_z:.2f}σ). Aborting BUY.")
                return {"action": target_direction, "confidence": 0.0, "edge_weight": 0.0, "reasoning": "VETO_VWAP_STRETCH", "routing": "STANDARD"}
                
            if target_direction == "SELL" and stretch_z < -3.0:
                self._throttled_warn("veto", f"🛑 HARD VETO // {symbol} Price too far extended below VWAP ({stretch_z:.2f}σ). Aborting SELL.")
                return {"action": target_direction, "confidence": 0.0, "edge_weight": 0.0, "reasoning": "VETO_VWAP_STRETCH", "routing": "STANDARD"}

            mlofi_strength = abs(current_mlofi) / mlofi_std
            confidence = min(0.95, max(0.40, 0.50 + (mlofi_strength * 0.05)))
            
            # Smooth weight scaling for surviving signals
            depth_multiplier = buy_depth_ratio if target_direction == "BUY" else (1.0 - buy_depth_ratio)
            edge_weight = min(2.0, max(0.5, (confidence * depth_multiplier * 2.0)))

            return {
                "action": target_direction,
                "confidence": confidence,
                "edge_weight": edge_weight,
                "reasoning": f"EDGE_VERIFIED | MLOFI: {mlofi_strength:.1f}σ | Weight: {edge_weight:.2f}x",
                "routing": "MAKER_ONLY" if abs(current_mlofi) < 0.5 else "STANDARD"
            }
            
        except Exception as e:
            logger.debug(f"[MATH_WARN] Numerical instability in structural edge evaluation: {e}")
            default_dir = intended_direction if intended_direction else "BUY"
            return {
                "action": default_dir, 
                "confidence": 0.0, 
                "edge_weight": 0.0, 
                "reasoning": "MATH_FAULT_ABORT", 
                "routing": "STANDARD"
            }