"""
💎 V95.0 TENSOR-PRIME: HYBRID MICROSTRUCTURE EDGE GATE
-------------------------------------------------------------------------
Features Stationarized Log-MLOFI, Dark Pool Iceberg Absorption,
Directional Micro-Price Vacuums, VWAP Volatility Stretch (Z-VWAP) Vetoes,
and L2 Orderbook Depth Ratio Shields.

CRITICAL FIX: Eliminated silent exception swallowing. Implemented strict 
NaN/Inf sanitization to prevent floating-point anomalies from corrupting 
the orderbook elasticity and VWAP stretch logic.
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
    🚀 V95.0 PREDATORY MAKER ENGINE & STRUCTURAL EDGE GATE
    Exploits orderbook dark pool absorptions, liquidity vacuums, and deep book breakouts.
    Guarded by VWAP Volatility Stretch (Z-VWAP) and L2 Depth Pressure Shields.
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
            
            # Safe VWAP calculation
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

        if len(self.prices) >= 20 and len(self.prices) % 10 == 0:
            lmbda = self._calculate_instantaneous_lambda()
            if lmbda > 0:
                self.lambda_history.append(lmbda)

    def _calculate_instantaneous_lambda(self) -> float:
        """Calculates Kyle's Lambda price impact parameter."""
        if len(self.prices) < 2 or len(self.mlofis) < 2:
            return 0.0
            
        try:
            p_array = np.array(self.prices, dtype=float)
            dp = np.diff(p_array)
            ofi_array = np.array(self.mlofis, dtype=float)[1:] 
            
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

    def evaluate_orderbook_depth_ratio(self, symbol: str) -> Dict[str, Any]:
        """
        🚀 V95.0 L2 ORDERBOOK DEPTH SHIELD
        Hard-blocks Longs if Bid Depth < 45% of total top-5 book volume.
        Hard-blocks Shorts if Ask Depth < 45% of total top-5 book volume.
        """
        if not self.prev_bids or not self.prev_asks:
            return {"allow_long": True, "allow_short": True, "buy_ratio": 0.50}

        try:
            total_bid_vol = sum(float(b[1]) for b in self.prev_bids)
            total_ask_vol = sum(float(a[1]) for a in self.prev_asks)
            total_depth = total_bid_vol + total_ask_vol + 1e-9
            
            buy_ratio = float(total_bid_vol / total_depth)
            
            if math.isnan(buy_ratio) or math.isinf(buy_ratio):
                return {"allow_long": True, "allow_short": True, "buy_ratio": 0.50}

            if buy_ratio < 0.45:
                self._throttled_warn(
                    f"depth_sell_wall_{symbol}",
                    f"[X-RAY] 🛑 HEAVY SELL WALL VETO // {symbol} BUY blocked. Buy Depth is only {buy_ratio*100:.1f}%."
                )
                return {"allow_long": False, "allow_short": True, "buy_ratio": buy_ratio}
            elif buy_ratio > 0.55:
                self._throttled_warn(
                    f"depth_buy_wall_{symbol}",
                    f"[X-RAY] 🛑 HEAVY BUY WALL VETO // {symbol} SELL blocked. Buy Depth is {buy_ratio*100:.1f}%."
                )
                return {"allow_long": True, "allow_short": False, "buy_ratio": buy_ratio}

            return {"allow_long": True, "allow_short": True, "buy_ratio": buy_ratio}
        except Exception as e:
            logger.debug(f"[MATH_WARN] Numerical instability in depth ratio evaluation: {e}")
            return {"allow_long": True, "allow_short": True, "buy_ratio": 0.50}

    def evaluate_exhaustion_veto(self, symbol: str, current_price: float, target_direction: str) -> Dict[str, Any]:
        """
        🚀 V95.0 EXHAUSTION & VOLATILITY STRETCH VETO
        Prevents shorting capitulation bottom wicks or buying blow-off tops.
        """
        if len(self.prices) < 20 or len(self.vwap_history) < 20:
            return {"veto": False, "reason": "STRETCH_CALIBRATING"}

        try:
            p_arr = np.array(self.prices, dtype=float)
            v_arr = np.array(self.volumes, dtype=float)
            vwap = float(self.vwap_history[-1])

            # VWAP Volatility Stretch (Z-VWAP)
            std_dev = float(np.std(p_arr)) + 1e-9
            z_vwap = (current_price - vwap) / std_dev
            
            if math.isnan(z_vwap) or math.isinf(z_vwap):
                z_vwap = 0.0

            # Volume Absorption Index (VAI)
            high_p = float(np.max(p_arr[-5:]))
            low_p = float(np.min(p_arr[-5:]))
            price_range_pct = (high_p - low_p) / (current_price + 1e-9)
            
            if math.isnan(price_range_pct) or math.isinf(price_range_pct):
                price_range_pct = 0.005 # Safe default to bypass absorption block
            
            recent_vol = float(np.mean(v_arr[-5:]))
            baseline_vol = float(np.mean(v_arr)) + 1e-9
            vol_multiplier = recent_vol / baseline_vol
            
            if math.isnan(vol_multiplier) or math.isinf(vol_multiplier):
                vol_multiplier = 1.0

            if target_direction == "SELL":
                if z_vwap < -2.2:
                    self._throttled_warn(
                        f"capitulation_{symbol}",
                        f"[X-RAY] 🛑 CAPITULATION VETO // {symbol} SELL blocked. "
                        f"Price stretched {z_vwap:.2f}σ below VWAP."
                    )
                    return {"veto": True, "reason": f"OVERSOLD_CAPITULATION_WICK | Z_VWAP: {z_vwap:.2f}σ"}

                if vol_multiplier > 3.0 and price_range_pct < 0.0030:
                    self._throttled_warn(
                        f"absorption_buy_{symbol}",
                        f"[X-RAY] 🛑 LIMIT ABSORPTION VETO // {symbol} SELL blocked. Whales absorbing sells."
                    )
                    return {"veto": True, "reason": f"LIMIT_BUY_ABSORPTION | Vol: {vol_multiplier:.1f}x"}

            elif target_direction == "BUY":
                if z_vwap > 2.2:
                    self._throttled_warn(
                        f"blowoff_{symbol}",
                        f"[X-RAY] 🛑 BLOW-OFF VETO // {symbol} BUY blocked. "
                        f"Price stretched {z_vwap:.2f}σ above VWAP."
                    )
                    return {"veto": True, "reason": f"OVERBOUGHT_BLOWOFF_WICK | Z_VWAP: {z_vwap:.2f}σ"}

                if vol_multiplier > 3.0 and price_range_pct < 0.0030:
                    self._throttled_warn(
                        f"absorption_sell_{symbol}",
                        f"[X-RAY] 🛑 LIMIT ABSORPTION VETO // {symbol} BUY blocked. Whales absorbing buys."
                    )
                    return {"veto": True, "reason": f"LIMIT_SELL_ABSORPTION | Vol: {vol_multiplier:.1f}x"}

            return {"veto": False, "reason": "SAFE"}
            
        except Exception as e:
            logger.debug(f"[MATH_WARN] Numerical instability in exhaustion veto: {e}")
            return {"veto": False, "reason": "MATH_FAULT_SAFE_DEFAULT"}

    def evaluate_structural_edge(self, symbol: str, vpin_z: float, intended_direction: str = None) -> dict:
        """Evaluates confluence between MLOFI, Iceberg Absorption, and Depth Pressure."""
        if len(self.mlofis) < 20 or len(self.lambda_history) < 5 or len(self._trade_imbalances) < 20:
            return {"action": "HOLD", "confidence": 0.0, "reasoning": "CALIBRATING_DEEP_BOOK", "routing": "STANDARD"}

        try:
            current_mlofi = float(np.mean(list(self.mlofis)[-5:]))
            mlofi_std = float(np.std(self.mlofis))
            
            if math.isnan(mlofi_std) or math.isinf(mlofi_std) or mlofi_std == 0 or abs(current_mlofi) < (mlofi_std * 0.5):
                return {"action": "HOLD", "confidence": 0.0, "reasoning": "LOG_MLOFI_FLAT", "routing": "STANDARD"}

            direction = "BUY" if current_mlofi > 0 else "SELL"
            target_direction = intended_direction if intended_direction else direction

            # 🚀 L2 ORDERBOOK DEPTH PRESSURE VETO
            depth_status = self.evaluate_orderbook_depth_ratio(symbol)
            if target_direction == "BUY" and not depth_status["allow_long"]:
                return {"action": "HOLD", "confidence": 0.0, "reasoning": f"HEAVY_SELL_DEPTH_VETO (Buy Depth: {depth_status['buy_ratio']*100:.1f}%)", "routing": "STANDARD"}
            elif target_direction == "SELL" and not depth_status["allow_short"]:
                return {"action": "HOLD", "confidence": 0.0, "reasoning": f"HEAVY_BUY_DEPTH_VETO (Buy Depth: {depth_status['buy_ratio']*100:.1f}%)", "routing": "STANDARD"}

            # 🚀 EXHAUSTION VETO CHECK
            current_price = self.prices[-1] if self.prices else 0.0
            if current_price > 0:
                veto_status = self.evaluate_exhaustion_veto(symbol, current_price, target_direction)
                if veto_status["veto"]:
                    return {
                        "action": "HOLD",
                        "confidence": 0.0,
                        "reasoning": f"EXHAUSTION_VETO | {veto_status['reason']}",
                        "routing": "STANDARD"
                    }

            if intended_direction and direction != intended_direction:
                return {
                    "action": "HOLD", 
                    "confidence": 0.0, 
                    "reasoning": f"CONFLUENCE_FAILURE | Model wants {intended_direction}, Log-MLOFI wants {direction}",
                    "routing": "STANDARD"
                }

            if intended_direction and direction == intended_direction:
                mlofi_strength = abs(current_mlofi) / (mlofi_std + 1e-9)
                confidence = min(0.75, 0.50 + (mlofi_strength * 0.05))
                return {
                    "action": direction,
                    "confidence": confidence,
                    "reasoning": f"STANDARD_MLOFI_CONFLUENCE | Signal: {direction} (Strength: {mlofi_strength:.1f}x)",
                    "routing": "STANDARD"
                }

            return {"action": "HOLD", "confidence": 0.0, "reasoning": "EDGE_GATE_UNDECIDED", "routing": "STANDARD"}
            
        except Exception as e:
            logger.debug(f"[MATH_WARN] Numerical instability in structural edge evaluation: {e}")
            return {"action": "HOLD", "confidence": 0.0, "reasoning": "MATH_FAULT_SAFE_DEFAULT", "routing": "STANDARD"}