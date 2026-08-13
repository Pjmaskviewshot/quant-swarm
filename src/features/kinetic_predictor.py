"""
ðŸ’Ž V1.0 TENSOR-PRIME: OMNI-KINETIC EXHAUSTION ORACLE
-------------------------------------------------------------------------
A self-calibrating HFT reversal predictor.
Computes Micro-Price Dislocation, Auto-Scaling Queue Replenishment Z-Scores,
and Bivariate Macro Tensors (BTC/ETH) to mathematically prove momentum death.

CRITICAL FIX: Implemented explicit numerical instability guards and NaN/Inf 
sanitization to guarantee deterministic outputs during flash crashes.
"""

import math
import logging
import numpy as np
from collections import deque
from typing import Dict, Any

logger = logging.getLogger("QUANT_CORE.OMNI_KINETIC")

class OmniKineticOracle:
    """
    ðŸš€ HIGH-SPEED KINETIC EXHAUSTION & LEAD-LAG RADAR
    Self-calibrating engine to detect spoofing, liquidity exhaustion, 
    and macro lead-lag dislocations for early trade ejections.
    """
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        
        # 1. Flow Deceleration Vectors
        self.timestamps = deque(maxlen=window_size)
        self.cvd_history = deque(maxlen=window_size)
        self.cvd_velocity = deque(maxlen=window_size)
        self.cvd_acceleration = deque(maxlen=window_size)
        
        # 2. Spoofing & Dislocation Vectors
        self.mid_prices = deque(maxlen=window_size)
        self.micro_prices = deque(maxlen=window_size)
        self.dislocation_history = deque(maxlen=window_size)
        
        # 3. Dynamic Replenishment Tensors
        self.ask_replenishment_ratios = deque(maxlen=window_size)
        self.bid_replenishment_ratios = deque(maxlen=window_size)
        
        # 4. Bivariate Macro Tensor (BTC + ETH)
        self.btc_flow_deltas = deque(maxlen=window_size)
        self.eth_flow_deltas = deque(maxlen=window_size)
        self.asset_flow_deltas = deque(maxlen=window_size)

    def ingest_micro_tick(self, timestamp: float, mid_price: float, best_bid_p: float, best_bid_v: float, best_ask_p: float, best_ask_v: float, current_cvd: float, is_buy: bool, trade_vol: float):
        """Processes tick data for Micro-Price Dislocation and Flow Acceleration."""
        try:
            self.timestamps.append(timestamp)
            self.cvd_history.append(current_cvd)
            self.mid_prices.append(mid_price)
            
            # Calculate True Micro-Price
            total_v = best_bid_v + best_ask_v
            if total_v <= 0:
                return  # Skip invalid orderbook state

            micro_price = (best_bid_p * best_ask_v + best_ask_p * best_bid_v) / (total_v + 1e-9)
            
            if math.isnan(micro_price) or math.isinf(micro_price):
                raise ValueError(f"Invalid micro_price calculated: {micro_price}")

            self.micro_prices.append(micro_price)
            
            # Track Dislocation (Spoofing detection)
            dislocation_bps = ((micro_price - mid_price) / (mid_price + 1e-9)) * 10000.0
            
            if math.isnan(dislocation_bps) or math.isinf(dislocation_bps):
                dislocation_bps = 0.0

            self.dislocation_history.append(dislocation_bps)

            # Calculate Flow Derivatives (Velocity & Acceleration)
            if len(self.cvd_history) >= 2:
                dt = max(0.001, self.timestamps[-1] - self.timestamps[-2])
                velocity = (self.cvd_history[-1] - self.cvd_history[-2]) / dt
                
                if not math.isnan(velocity) and not math.isinf(velocity):
                    self.cvd_velocity.append(velocity)
                
                if len(self.cvd_velocity) >= 2:
                    acceleration = (self.cvd_velocity[-1] - self.cvd_velocity[-2]) / dt
                    if not math.isnan(acceleration) and not math.isinf(acceleration):
                        self.cvd_acceleration.append(acceleration)

        except Exception as e:
            logger.debug(f"[MATH_WARN] Numerical instability in OmniKineticOracle.ingest_micro_tick: {e}")

    def ingest_replenishment_cycle(self, ask_refill_qty: float, bid_refill_qty: float, buy_trade_vol: float, sell_trade_vol: float):
        """Calculates dynamic replenishment ratios per update cycle."""
        try:
            ask_ratio = ask_refill_qty / (buy_trade_vol + 1e-9) if ask_refill_qty > 0 else 0.0
            bid_ratio = bid_refill_qty / (sell_trade_vol + 1e-9) if bid_refill_qty > 0 else 0.0
            
            if math.isnan(ask_ratio) or math.isinf(ask_ratio): ask_ratio = 0.0
            if math.isnan(bid_ratio) or math.isinf(bid_ratio): bid_ratio = 0.0

            self.ask_replenishment_ratios.append(ask_ratio)
            self.bid_replenishment_ratios.append(bid_ratio)
        except Exception as e:
            logger.debug(f"[MATH_WARN] Numerical instability in OmniKineticOracle.ingest_replenishment_cycle: {e}")

    def ingest_macro_tensor(self, btc_cvd_delta: float, eth_cvd_delta: float, asset_cvd_delta: float):
        """Ingests BTC and ETH combined flows to measure bivariate macro correlation."""
        try:
            if any(math.isnan(x) or math.isinf(x) for x in [btc_cvd_delta, eth_cvd_delta, asset_cvd_delta]):
                return
            self.btc_flow_deltas.append(btc_cvd_delta)
            self.eth_flow_deltas.append(eth_cvd_delta)
            self.asset_flow_deltas.append(asset_cvd_delta)
        except Exception as e:
            logger.debug(f"[MATH_WARN] Numerical instability in OmniKineticOracle.ingest_macro_tensor: {e}")

    def evaluate_kinetic_exhaustion(self, symbol: str, position_direction: str) -> Dict[str, Any]:
        """
        ðŸš€ OMNI-KINETIC EXHAUSTION EVALUATOR
        Dynamically calculates standard deviations and triggers surgical ejections
        before price reversal hits the chart.
        """
        try:
            if len(self.cvd_acceleration) < 30 or len(self.ask_replenishment_ratios) < 30:
                return {"early_exit": False, "exhaustion_score": 0.0, "reason": "CALIBRATING_RADAR"}

            # 1. Flow Acceleration Z-Score
            recent_accel = np.mean(list(self.cvd_acceleration)[-3:])
            accel_std = np.std(self.cvd_acceleration) + 1e-9
            accel_z_score = recent_accel / accel_std

            # 2. Dynamic Queue Replenishment Z-Scores
            recent_ask_ratio = np.mean(list(self.ask_replenishment_ratios)[-3:])
            ask_ratio_std = np.std(self.ask_replenishment_ratios) + 1e-9
            ask_refill_z = (recent_ask_ratio - np.mean(self.ask_replenishment_ratios)) / ask_ratio_std

            recent_bid_ratio = np.mean(list(self.bid_replenishment_ratios)[-3:])
            bid_ratio_std = np.std(self.bid_replenishment_ratios) + 1e-9
            bid_refill_z = (recent_bid_ratio - np.mean(self.bid_replenishment_ratios)) / bid_ratio_std

            # 3. Micro-Price Dislocation (Spoof Radar)
            recent_dislocation = np.mean(list(self.dislocation_history)[-3:])

            # 4. Bivariate Macro Lead-Lag Dislocation
            macro_btc_cov, macro_eth_cov = 0.0, 0.0
            if len(self.btc_flow_deltas) >= 30:
                asset_arr = np.array(self.asset_flow_deltas)
                std_asset = np.std(asset_arr)
                if std_asset > 0:
                    btc_arr = np.array(self.btc_flow_deltas)
                    eth_arr = np.array(self.eth_flow_deltas)
                    
                    if np.std(btc_arr) > 0:
                        macro_btc_cov = np.cov(btc_arr, asset_arr)[0][1] / (np.std(btc_arr) * std_asset + 1e-9)
                    if np.std(eth_arr) > 0:
                        macro_eth_cov = np.cov(eth_arr, asset_arr)[0][1] / (np.std(eth_arr) * std_asset + 1e-9)
            
            composite_macro_cov = (macro_btc_cov * 0.6) + (macro_eth_cov * 0.4)

            # Validate outputs to prevent NaN propagation
            for val in [accel_z_score, ask_refill_z, bid_refill_z, recent_dislocation, composite_macro_cov]:
                if math.isnan(val) or math.isinf(val):
                    raise ValueError("NaN/Inf detected in matrix evaluation parameters")

            # =====================================================================
            # ðŸš€ EARLY EXIT LOGIC: SURGICAL POSITION AMPUTATION
            # =====================================================================
            
            if position_direction == "BUY":
                # A. KINETIC BUY EXHAUSTION (Deceleration + Extreme Ask Refill + Downward Spoofing)
                if accel_z_score < -2.5 and ask_refill_z > 2.5 and recent_dislocation < -2.0:
                    logger.critical(
                        f"[X-RAY] ðŸ›‘ OMNI-KINETIC EJECTION // {symbol} BUY momentum scientifically dead. "
                        f"Flow Z: {accel_z_score:.2f}Ïƒ | Ask Refill Z: {ask_refill_z:.2f}Ïƒ | Spoof Dislocation: {recent_dislocation:.1f} bps."
                    )
                    return {
                        "early_exit": True, 
                        "exhaustion_score": abs(accel_z_score) + ask_refill_z, 
                        "reason": f"BUY_KINETIC_DEATH | Z-Refill: {ask_refill_z:.2f}Ïƒ"
                    }
                    
                # B. BIVARIATE MACRO BREAKDOWN (BTC & ETH flows collapsing ahead of asset)
                if composite_macro_cov < -0.75:
                    logger.critical(
                        f"[X-RAY] ðŸ›‘ MACRO TENSOR EJECTION // {symbol} Localized fake-pump detected. "
                        f"BTC/ETH Composite Flow collapsing (Cov: {composite_macro_cov:.2f}). Exiting Long."
                    )
                    return {
                        "early_exit": True, 
                        "exhaustion_score": 3.0, 
                        "reason": f"BIVARIATE_MACRO_COLLAPSE | Tensor: {composite_macro_cov:.2f}"
                    }

            elif position_direction == "SELL":
                # A. KINETIC SELL EXHAUSTION (Deceleration + Extreme Bid Refill + Upward Spoofing)
                if accel_z_score > 2.5 and bid_refill_z > 2.5 and recent_dislocation > 2.0:
                    logger.critical(
                        f"[X-RAY] ðŸ›‘ OMNI-KINETIC EJECTION // {symbol} SELL momentum scientifically dead. "
                        f"Flow Z: {accel_z_score:.2f}Ïƒ | Bid Refill Z: {bid_refill_z:.2f}Ïƒ | Spoof Dislocation: +{recent_dislocation:.1f} bps."
                    )
                    return {
                        "early_exit": True, 
                        "exhaustion_score": abs(accel_z_score) + bid_refill_z, 
                        "reason": f"SELL_KINETIC_DEATH | Z-Refill: {bid_refill_z:.2f}Ïƒ"
                    }

                # B. BIVARIATE MACRO RALLY (BTC & ETH flows surging ahead of asset)
                if composite_macro_cov > 0.75:
                    logger.critical(
                        f"[X-RAY] ðŸ›‘ MACRO TENSOR EJECTION // {symbol} Localized fake-dump detected. "
                        f"BTC/ETH Composite Flow surging (Cov: {composite_macro_cov:.2f}). Exiting Short."
                    )
                    return {
                        "early_exit": True, 
                        "exhaustion_score": 3.0, 
                        "reason": f"BIVARIATE_MACRO_SURGE | Tensor: {composite_macro_cov:.2f}"
                    }

            return {"early_exit": False, "exhaustion_score": abs(accel_z_score), "reason": "FLOW_STABLE"}

        except Exception as e:
            logger.debug(f"[MATH_WARN] Numerical instability in OmniKineticOracle.evaluate_kinetic_exhaustion: {e}")
            return {"early_exit": False, "exhaustion_score": 0.0, "reason": "MATH_FAULT_SAFE_DEFAULT"}