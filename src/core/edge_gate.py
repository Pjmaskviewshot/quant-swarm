"""
💎 V68.5 TITANIUM APEX: MICROSTRUCTURE EDGE GATE & EXHAUSTION VETO ENGINE
-------------------------------------------------------------------------
Evaluates Stationarized Log-MLOFI (Spoof-Resistant), Dark Pool Iceberg Absorption,
Micro-Price Effective Spreads (Liquidity Vacuums), Roll Implicit Spreads, and
VWAP Volatility Stretch (Z-VWAP) / Limit Order Absorption Vetoes.
Patched to eliminate shorting capitulation wicks or buying blow-off tops.
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
    🚀 V68.5 PREDATORY MAKER ENGINE & STRUCTURAL EDGE GATE
    Exploits orderbook dark pool absorptions, liquidity vacuums, and deep book breakouts.
    Upgraded with VWAP Volatility Stretch (Z-VWAP) and Limit Order Absorption Vetoes.
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
        """Helper to throttle repeating log warnings and avoid terminal log spam."""
        now = time.time()
        last = self._last_log_time.get(category, 0.0)
        if now - last > throttle_sec:
            self._last_log_time[category] = now
            logger.warning(message)
        
    def update_trade_flow(self, volume: float, is_buy: bool):
        """Tracks signed tick trade flow imbalance (True Aggressor Side) and total period volume."""
        if is_buy:
            self._current_trade_buy_vol += volume
        else:
            self._current_trade_sell_vol += volume
        self._current_period_volume += volume

    def update_orderbook_state(self, symbol: str, bids: List[List[float]], asks: List[List[float]], mid_price: float):
        """
        🚀 V68.5 UPGRADE: Updates Stationarized Log-MLOFI, Rolling VWAP,
        Kyle's Lambda (price impact), and Micro-Price Spread metrics.
        """
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
                
                # Log-OFI: Damps spoofing walls using math.log1p
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
        
        # Track volume & calculate Rolling VWAP
        period_vol = max(1.0, self._current_period_volume)
        self.volumes.append(period_vol)
        self._current_period_volume = 0.0

        p_arr = np.array(self.prices)
        v_arr = np.array(self.volumes)
        rolling_vwap = float(np.sum(p_arr * v_arr) / (np.sum(v_arr) + 1e-9))
        self.vwap_history.append(rolling_vwap)
        
        t_imb = self._current_trade_buy_vol - self._current_trade_sell_vol
        self._trade_imbalances.append(t_imb)
        self._current_trade_buy_vol = 0.0
        self._current_trade_sell_vol = 0.0

        self.prev_bids = current_bids
        self.prev_asks = current_asks
        
        try:
            best_bid_p, best_bid_s = float(current_bids[0][0]), float(current_bids[0][1])
            best_ask_p, best_ask_s = float(current_asks[0][0]), float(current_asks[0][1])
            
            micro_price = (best_bid_p * best_ask_s + best_ask_p * best_bid_s) / (best_bid_s + best_ask_s + 1e-9)
            micro_spread_bps = ((micro_price - mid_price) / (mid_price + 1e-9)) * 10000.0
            self.micro_spread_history.append(micro_spread_bps)
        except Exception:
            self.micro_spread_history.append(0.0)

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

    def evaluate_exhaustion_veto(self, symbol: str, current_price: float, target_direction: str) -> Dict[str, Any]:
        """
        🚀 V68.5 EXHAUSTION & VOLATILITY STRETCH VETO
        Evaluates VWAP Standard Deviation Stretch (Z-VWAP) and Volume Absorption Index (VAI)
        to prevent shorting capitulation wicks or buying blow-off tops.
        """
        if len(self.prices) < 20 or len(self.vwap_history) < 20:
            return {"veto": False, "reason": "STRETCH_CALIBRATING"}

        p_arr = np.array(self.prices)
        v_arr = np.array(self.volumes)
        vwap = self.vwap_history[-1]

        # 1. VWAP VOLATILITY STRETCH (Z-VWAP)
        std_dev = np.std(p_arr) + 1e-9
        z_vwap = (current_price - vwap) / std_dev

        # 2. VOLUME ABSORPTION INDEX (VAI)
        high_p = np.max(p_arr[-5:])
        low_p = np.min(p_arr[-5:])
        price_range_pct = (high_p - low_p) / (current_price + 1e-9)
        
        recent_vol = np.mean(v_arr[-5:])
        baseline_vol = np.mean(v_arr) + 1e-9
        vol_multiplier = recent_vol / baseline_vol

        # 🚀 SHORT VETO: Shorting an oversold capitulation wick
        if target_direction == "SELL":
            if z_vwap < -2.2:
                self._throttled_warn(
                    f"capitulation_{symbol}",
                    f"[X-RAY] 🛑 CAPITULATION VETO // {symbol} SELL blocked. "
                    f"Price stretched {z_vwap:.2f}σ below VWAP (Oversold Bottom Wick)."
                )
                return {"veto": True, "reason": f"OVERSOLD_CAPITULATION_WICK | Z_VWAP: {z_vwap:.2f}σ"}

            if vol_multiplier > 3.0 and price_range_pct < 0.0030:
                self._throttled_warn(
                    f"absorption_buy_{symbol}",
                    f"[X-RAY] 🛑 LIMIT ABSORPTION VETO // {symbol} SELL blocked. "
                    f"Volume surge {vol_multiplier:.1f}x on small price range ({price_range_pct*100:.2f}%). Whales absorbing sells."
                )
                return {"veto": True, "reason": f"LIMIT_BUY_ABSORPTION | Vol: {vol_multiplier:.1f}x"}

        # 🚀 BUY VETO: Buying an overbought blow-off top wick
        elif target_direction == "BUY":
            if z_vwap > 2.2:
                self._throttled_warn(
                    f"blowoff_{symbol}",
                    f"[X-RAY] 🛑 BLOW-OFF VETO // {symbol} BUY blocked. "
                    f"Price stretched {z_vwap:.2f}σ above VWAP (Overbought Top Wick)."
                )
                return {"veto": True, "reason": f"OVERBOUGHT_BLOWOFF_WICK | Z_VWAP: {z_vwap:.2f}σ"}

            if vol_multiplier > 3.0 and price_range_pct < 0.0030:
                self._throttled_warn(
                    f"absorption_sell_{symbol}",
                    f"[X-RAY] 🛑 LIMIT ABSORPTION VETO // {symbol} BUY blocked. "
                    f"Volume surge {vol_multiplier:.1f}x on small price range ({price_range_pct*100:.2f}%). Whales absorbing buys."
                )
                return {"veto": True, "reason": f"LIMIT_SELL_ABSORPTION | Vol: {vol_multiplier:.1f}x"}

        return {"veto": False, "reason": "SAFE"}

    def evaluate_structural_edge(self, symbol: str, vpin_z: float, intended_direction: str = None) -> dict:
        """
        🚀 V68.5 STRUCTURAL EDGE EVALUATOR
        Evaluates confluence between statistical prediction, Log-MLOFI, Dark Pool Iceberg
        absorption, Micro-Price Liquidity Vacuums, and Exhaustion Volatility Stretch.
        """
        if len(self.mlofis) < 20 or len(self.lambda_history) < 5 or len(self._trade_imbalances) < 20:
            return {"action": "HOLD", "confidence": 0.0, "reasoning": "CALIBRATING_DEEP_BOOK", "routing": "STANDARD"}

        current_mlofi = np.mean(list(self.mlofis)[-5:])
        mlofi_std = np.std(self.mlofis)
        
        current_t_imb = np.mean(list(self._trade_imbalances)[-5:])
        t_imb_std = np.std(self._trade_imbalances)
        
        # Anti-Starvation Gate: Require active order flow imbalance
        if mlofi_std == 0 or abs(current_mlofi) < (mlofi_std * 0.5):
            return {"action": "HOLD", "confidence": 0.0, "reasoning": "LOG_MLOFI_FLAT", "routing": "STANDARD"}

        direction = "BUY" if current_mlofi > 0 else "SELL"
        target_direction = intended_direction if intended_direction else direction
        
        if intended_direction and direction != intended_direction:
            return {
                "action": "HOLD", 
                "confidence": 0.0, 
                "reasoning": f"CONFLUENCE_FAILURE | Model wants {intended_direction}, Log-MLOFI wants {direction}",
                "routing": "STANDARD"
            }

        # 🚀 V68.5 EXHAUSTION & VOLATILITY STRETCH VETO CHECK
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

        current_lambda = self._calculate_instantaneous_lambda()
        raw_baseline = np.mean(self.lambda_history) if self.lambda_history else current_lambda
        baseline_lambda = max(1e-6, float(raw_baseline))
        
        # 1. PREDATORY MAKER MODE: Directional Micro-Price Effective Spread Vacuum Detection
        if len(self.micro_spread_history) >= 20:
            current_micro_spread = self.micro_spread_history[-1]
            abs_spread_mean = np.mean(np.abs(list(self.micro_spread_history)[-20:]))
            
            if abs_spread_mean > 0 and abs(current_micro_spread) > (abs_spread_mean * 4.0) and abs(current_micro_spread) > 1.0:
                vacuum_dir = "BUY" if current_micro_spread > 0 else "SELL"
                
                if vacuum_dir == direction:
                    self._throttled_warn(
                        f"vacuum_{symbol}", 
                        f"[X-RAY] 🎯 PREDATORY MAKER ENGAGED // {symbol} | Exploiting {vacuum_dir} Liquidity Vacuum (Spike: {abs(current_micro_spread)/abs_spread_mean:.1f}x)."
                    )
                    return {
                        "action": direction, 
                        "confidence": 0.75, 
                        "reasoning": f"PREDATORY_MAKER_VACUUM | Micro-Spread: {current_micro_spread:.2f} bps",
                        "routing": "MAKER_ONLY"
                    }

        # 2. DARK POOL ICEBERG ABSORPTION DETECTION
        if t_imb_std > 0 and current_lambda < (baseline_lambda * 0.1):
            t_imb_z = current_t_imb / t_imb_std
            if t_imb_z < -2.5 and direction == "BUY": 
                self._throttled_warn(f"darkpool_buy_{symbol}", f"[X-RAY] 🧊 DARK POOL ABSORPTION // {symbol} | Heavy selling absorbed. Reversing to BUY.")
                return {
                    "action": "BUY", 
                    "confidence": 0.85, 
                    "reasoning": f"DARK_POOL_ICEBERG_SUPPORT | T_IMB_Z: {t_imb_z:.2f}",
                    "routing": "STANDARD"
                }
            elif t_imb_z > 2.5 and direction == "SELL": 
                self._throttled_warn(f"darkpool_sell_{symbol}", f"[X-RAY] 🧊 DARK POOL ABSORPTION // {symbol} | Heavy buying absorbed. Reversing to SELL.")
                return {
                    "action": "SELL", 
                    "confidence": 0.85, 
                    "reasoning": f"DARK_POOL_ICEBERG_RESISTANCE | T_IMB_Z: {t_imb_z:.2f}",
                    "routing": "STANDARD"
                }

        # 3. DEEP BOOK BREAKOUT: High VPIN & Expanding Price Impact
        if abs(vpin_z) >= 1.5 and current_lambda >= (baseline_lambda * 0.8):
            lambda_expansion = min(1.5, current_lambda / max(baseline_lambda, 1e-9))
            confidence = min(0.95, 0.50 + (lambda_expansion * 0.20) + (abs(vpin_z) * 0.05))
            
            return {
                "action": direction,
                "confidence": confidence,
                "reasoning": f"DEEP_BOOK_BREAKOUT | Elasticity: {lambda_expansion:.2f}x",
                "routing": "STANDARD"
            }

        # 4. STANDARD CONFLUENCE TRADES
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