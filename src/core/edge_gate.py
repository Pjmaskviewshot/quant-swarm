"""
💎 V50.0 QUANTUM SWARM: MICROSTRUCTURE EDGE GATE & PREDATORY MAKER ENGINE
-------------------------------------------------------------------------
Evaluates L2 Multi-Level Order Flow Imbalance (MLOFI), Dark Pool Iceberg Absorption,
Amihud Liquidity Vacuums, and Roll Implicit Spreads. Features X-Ray Diagnostic Telemetry.
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
    🚀 V50.0 PREDATORY MAKER ENGINE & STRUCTURAL EDGE GATE
    Exploits orderbook dark pool absorptions and liquidity vacuums using Maker Pegging.
    Features strictly calibrated Institutional Amihud Thresholds and X-Ray Telemetry.
    """
    def __init__(self, window_size=100, mlofi_levels=5, decay_alpha=0.5):
        self.window_size = window_size
        self.mlofi_levels = mlofi_levels
        self.decay_alpha = decay_alpha  
        
        self.prices = deque(maxlen=window_size)
        self.ofis = deque(maxlen=window_size)     
        self.mlofis = deque(maxlen=window_size)   
        
        self._trade_imbalances = deque(maxlen=window_size)
        self._current_trade_buy_vol = 0.0
        self._current_trade_sell_vol = 0.0
        
        self.lambda_history = deque(maxlen=window_size)
        self.amihud_history = deque(maxlen=window_size)
        
        self.prev_bids = []
        self.prev_asks = []
        self.rolling_volume = 0.0
        self.amihud_anchor_price = 0.0
        
        self._last_log_time = {}

    def _throttled_warn(self, category: str, message: str, throttle_sec: float = 60.0):
        """Helper to throttle repeating log warnings and avoid terminal log spam."""
        now = time.time()
        last = self._last_log_time.get(category, 0.0)
        if now - last > throttle_sec:
            self._last_log_time[category] = now
            logger.warning(message)

    def update_trade_volume(self, volume: float):
        """Updates cumulative rolling volume for Amihud calculation."""
        self.rolling_volume += volume
        
    def update_trade_flow(self, volume: float, is_buy: bool):
        """Tracks signed tick trade flow imbalance."""
        self.rolling_volume += volume
        if is_buy:
            self._current_trade_buy_vol += volume
        else:
            self._current_trade_sell_vol += volume

    def update_orderbook_state(self, symbol: str, bids: List[List[float]], asks: List[List[float]], mid_price: float):
        """
        Updates L2 Multi-Level Order Flow Imbalance (MLOFI) across book depth levels.
        Calculates rolling Kyle's Lambda (price impact) and Amihud illiquidity metrics.
        """
        if not self.prev_bids or not self.prev_asks:
            self.prev_bids = bids[:self.mlofi_levels]
            self.prev_asks = asks[:self.mlofi_levels]
            self.prices.append(mid_price)
            self.ofis.append(0.0)
            self.mlofis.append(0.0)
            self._trade_imbalances.append(0.0)
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
        
        t_imb = self._current_trade_buy_vol - self._current_trade_sell_vol
        self._trade_imbalances.append(t_imb)
        self._current_trade_buy_vol = 0.0
        self._current_trade_sell_vol = 0.0

        self.prev_bids = current_bids
        self.prev_asks = current_asks
        
        # 🚀 Institutional Amihud Thresholds (Scaled by Asset Tier)
        if "BTC" in symbol:
            amihud_threshold = 2_500_000.0  
        elif "ETH" in symbol or "SOL" in symbol:
            amihud_threshold = 1_000_000.0   
        else:
            amihud_threshold = 250_000.0   

        notional_vol = self.rolling_volume * mid_price
        
        if notional_vol >= amihud_threshold:
            if self.amihud_anchor_price > 0:
                price_change = abs(math.log(mid_price / (self.amihud_anchor_price + 1e-9)))
                illiquidity = price_change / notional_vol
                self.amihud_history.append(illiquidity)
            
            self.rolling_volume = 0.0
            self.amihud_anchor_price = mid_price

        if len(self.prices) >= 20 and len(self.prices) % 10 == 0:
            lmbda = self._calculate_instantaneous_lambda()
            if lmbda > 0:
                self.lambda_history.append(lmbda)

    def _calculate_instantaneous_lambda(self) -> float:
        """Calculates Kyle's Lambda (instantaneous price impact parameter)."""
        if len(self.prices) < 2 or len(self.mlofis) < 2:
            return 0.0
        p_array = np.array(self.prices)
        dp = np.diff(p_array)
        ofi_array = np.array(self.mlofis)[1:] 
        
        if len(dp) == 0 or len(ofi_array) == 0 or len(dp) != len(ofi_array):
            return 0.0
        
        variance = np.var(ofi_array)
        if variance < 1e-12: return 0.0
            
        try:
            with np.errstate(divide='ignore', invalid='ignore'):
                covariance = np.cov(ofi_array, dp)[0][1]
            if np.isnan(covariance): return 0.0
            return max(0.0, float(covariance / (variance + 1e-9)))
        except Exception:
            return 0.0

    def compute_roll_spread(self) -> float:
        """Estimates Richard Roll's implicit bid-ask spread from serial autocovariance of price changes."""
        if len(self.prices) < 10: return 0.0
        p_array = np.array(self.prices)
        dp = np.diff(p_array)
        if len(dp) < 3: return 0.0
        
        try:
            with np.errstate(divide='ignore', invalid='ignore'):
                cov = np.cov(dp[1:], dp[:-1])[0][1]
            if np.isnan(cov) or cov >= 0: return 0.0 
            return 2.0 * math.sqrt(-cov)
        except Exception:
            return 0.0

    def evaluate_structural_edge(self, symbol: str, vpin_z: float, intended_direction: str = None) -> dict:
        """
        🚀 V50.0 STRUCTURAL EDGE EVALUATOR
        Evaluates confluence between statistical prediction, Order Flow Imbalance, Dark Pool Iceberg
        absorption, and Amihud liquidity vacuums. Emits X-Ray diagnostics.
        """
        if len(self.mlofis) < 20 or len(self.lambda_history) < 5 or len(self._trade_imbalances) < 20:
            return {"action": "HOLD", "confidence": 0.0, "reasoning": "CALIBRATING_DEEP_BOOK", "routing": "STANDARD"}

        current_mlofi = np.mean(list(self.mlofis)[-5:])
        mlofi_std = np.std(self.mlofis)
        
        current_t_imb = np.mean(list(self._trade_imbalances)[-5:])
        t_imb_std = np.std(self._trade_imbalances)
        
        if mlofi_std == 0 or abs(current_mlofi) < (mlofi_std * 0.5):
            return {"action": "HOLD", "confidence": 0.0, "reasoning": "MLOFI_FLAT", "routing": "STANDARD"}

        direction = "BUY" if current_mlofi > 0 else "SELL"
        
        current_lambda = self._calculate_instantaneous_lambda()
        raw_baseline = np.mean(self.lambda_history) if self.lambda_history else current_lambda
        baseline_lambda = max(1e-6, float(raw_baseline))
        
        # 1. PREDATORY MAKER MODE: Amihud Liquidity Vacuum Detection
        if len(self.amihud_history) >= 10:
            current_amihud = self.amihud_history[-1]
            amihud_mean = np.mean(list(self.amihud_history)[-10:])
            if amihud_mean > 0 and current_amihud > (amihud_mean * 4.0):
                self._throttled_warn(
                    f"vacuum_{symbol}", 
                    f"[X-RAY] 🎯 PREDATORY MAKER ENGAGED // {symbol} | Exploiting Liquidity Vacuum (Spike: {current_amihud/max(1e-9, amihud_mean):.1f}x)."
                )
                return {
                    "action": intended_direction if intended_direction else direction, 
                    "confidence": 0.75, 
                    "reasoning": f"PREDATORY_MAKER_VACUUM | Spike: {current_amihud/max(1e-9, amihud_mean):.1f}x",
                    "routing": "MAKER_ONLY"
                }

        # 2. DARK POOL ICEBERG ABSORPTION DETECTION
        if t_imb_std > 0 and current_lambda < (baseline_lambda * 0.1):
            t_imb_z = current_t_imb / t_imb_std
            if t_imb_z < -2.5: 
                self._throttled_warn(f"darkpool_buy_{symbol}", f"[X-RAY] 🧊 DARK POOL ABSORPTION // {symbol} | Heavy selling absorbed. Reversing to BUY.")
                return {
                    "action": "BUY", 
                    "confidence": 0.85, 
                    "reasoning": f"DARK_POOL_ICEBERG_SUPPORT | T_IMB_Z: {t_imb_z:.2f}",
                    "routing": "STANDARD"
                }
            elif t_imb_z > 2.5: 
                self._throttled_warn(f"darkpool_sell_{symbol}", f"[X-RAY] 🧊 DARK POOL ABSORPTION // {symbol} | Heavy buying absorbed. Reversing to SELL.")
                return {
                    "action": "SELL", 
                    "confidence": 0.85, 
                    "reasoning": f"DARK_POOL_ICEBERG_RESISTANCE | T_IMB_Z: {t_imb_z:.2f}",
                    "routing": "STANDARD"
                }

        # 3. CONFLUENCE GUARD: Model Direction vs. Microstructure OFI
        if intended_direction and direction != intended_direction:
            return {
                "action": "HOLD", 
                "confidence": 0.0, 
                "reasoning": f"CONFLUENCE_FAILURE | Model wants {intended_direction}, MLOFI wants {direction}",
                "routing": "STANDARD"
            }

        # 4. DEEP BOOK BREAKOUT: High VPIN & Expanding Price Impact
        if abs(vpin_z) >= 1.5 and current_lambda >= (baseline_lambda * 0.8):
            lambda_expansion = min(1.5, current_lambda / max(baseline_lambda, 1e-9))
            confidence = min(0.95, 0.50 + (lambda_expansion * 0.20) + (abs(vpin_z) * 0.05))
            
            return {
                "action": direction,
                "confidence": confidence,
                "reasoning": f"DEEP_BOOK_BREAKOUT | Elasticity: {lambda_expansion:.2f}x",
                "routing": "STANDARD"
            }

        return {"action": "HOLD", "confidence": 0.0, "reasoning": "EDGE_GATE_UNDECIDED", "routing": "STANDARD"}