"""
💎 V55.2 QUANTUM SWARM: MICROSTRUCTURE MACHINE LEARNING MODELS
--------------------------------------------------------------
Houses the Adaptive Session Clock, Permutation Entropy calculators, 
and the Recursive Least Squares (RLS) Online Learning Engine.
Features fully data-driven, parameter-free Symmetrical Entropy Gating, 
mathematically stationary Permutation Entropy, and Pure Mixture-of-Experts RLS.
"""

import math
import time
import numpy as np
import datetime
import logging
from collections import deque
from itertools import permutations
from typing import Tuple

logger = logging.getLogger("QUANT_CORE.MICRO_MODELS")

class AdaptiveSessionClock:
    """
    Handles Weekend vs. Weekday regime adjustments to prevent signal starvation
    during quiet trading sessions.
    """
    @staticmethod
    def is_weekend() -> bool:
        # UTC weekday: 5 = Saturday, 6 = Sunday
        return datetime.datetime.now(datetime.timezone.utc).weekday() in (5, 6)

    @classmethod
    def get_turnover_threshold(cls) -> float:
        # Lower volume requirements on weekends to prevent asset starvation
        return 15_000_000.0 if cls.is_weekend() else 30_000_000.0

    @classmethod
    def get_ev_floor(cls, routing_mode: str) -> float:
        if routing_mode == "MAKER_ONLY":
            return 0.00005  # +0.5 bps EV for maker limit orders
        return 0.00010       # +1.0 bps EV for taker/iceberg orders


class ClusterWarmStartRLS:
    @staticmethod
    def get_cluster_priors(symbol: str):
        if any(m in symbol for m in ["BTC", "ETH", "SOL"]):
            w_trend = np.array([0.22, 0.18, 0.15, 0.08, 0.12, 0.10, 0.05, 0.05, 0.05])
            w_range = np.array([0.08, 0.15, 0.05, 0.22, 0.18, 0.05, 0.12, 0.08, 0.07])
            p_scale = 1.0
        elif any(m in symbol for m in ["AVAX", "LINK", "XRP", "ADA", "DOT", "NEAR"]):
            w_trend = np.array([0.20, 0.16, 0.14, 0.10, 0.10, 0.10, 0.08, 0.06, 0.06])
            w_range = np.array([0.09, 0.14, 0.06, 0.20, 0.16, 0.06, 0.11, 0.09, 0.09])
            p_scale = 2.0
        else:
            w_trend = np.array([0.15, 0.12, 0.10, 0.15, 0.08, 0.10, 0.10, 0.10, 0.10])
            w_range = np.array([0.10, 0.10, 0.08, 0.18, 0.14, 0.08, 0.12, 0.10, 0.10])
            p_scale = 0.5 
        return w_trend, w_range, np.eye(9) * p_scale


def compute_permutation_entropy(series: list, order: int = 3, delay: int = 1) -> float:
    """
    Calculates Shannon Permutation Entropy to detect market chaos.
    1.0 = Pure random noise. Lower = Highly predictable/structured.
    MUST be fed stationary data (e.g., log-returns), not raw prices.
    """
    if len(series) < (order * delay): return 1.0
    sub_vectors = [[series[i + j * delay] for j in range(order)] for i in range(len(series) - (order - 1) * delay)]
    perm_counts = {perm: 0 for perm in permutations(range(order))}
    
    for vec in sub_vectors:
        perm_counts[tuple(np.argsort(vec))] += 1
        
    total = len(sub_vectors)
    entropy = sum(- (c / total) * math.log2(c / total) for c in perm_counts.values() if c > 0)
    return float(entropy / math.log2(math.factorial(order)))


class ContinuousMicrostructureEngine:
    def __init__(self, symbol: str = "GENERIC", memory_depth=500):
        self.symbol = symbol
        self.prev_bid, self.prev_bid_size, self.prev_ask, self.prev_ask_size = 0.0, 0.0, 0.0, 0.0
        
        self.ofi_fast_ewma, self.ofi_fast_ewmvar, self.ofi_fast_z = 0.0, 1.0, 0.0
        self.ofi_slow_ewma, self.ofi_slow_ewmvar, self.ofi_slow_z = 0.0, 1.0, 0.0
        self.micro_price_skew, self.true_micro_price = 0.0, 0.0
        
        self.last_trade_time, self.hawkes_pressure_state = 0.0, 0.0
        self.hawkes_ewma, self.hawkes_ewmvar, self.hawkes_z = 0.0, 1.0, 0.0
        self.hawkes_velocity, self.hawkes_acceleration = 0.0, 0.0
        self.hawkes_z_prev, self.hawkes_v_prev = 0.0, 0.0
        
        self.prices = deque(maxlen=memory_depth)
        self.log_returns = deque(maxlen=memory_depth)
        self.inst_variance, self.vol_ewma = 1e-6, 0.0 
        
        self.hurst, self.kaufman_er = 0.5, 0.5
        self.last_hurst_time, self.last_price_time = 0.0, 0.0  
        self.shannon_entropy = 1.0
        self.entropy_history = deque(maxlen=200) 
        
        w_t, w_r, P_init = ClusterWarmStartRLS.get_cluster_priors(symbol)
        self.weights_trending, self.weights_ranging = w_t, w_r
        self.P_trending, self.P_ranging = P_init.copy(), P_init.copy()
        
        self.prediction_buffer = deque(maxlen=50000)
        self.historical_probs = deque(maxlen=2000) 
        self.rls_updates, self.ewma_mse = 100, 0.25 
        
        self._last_log_time = {}

    def _throttled_log(self, category: str, message: str, throttle_sec: float = 300.0):
        now = time.time()
        last = self._last_log_time.get(category, 0.0)
        if now - last > throttle_sec:
            self._last_log_time[category] = now
            logger.debug(message)

    def get_dynamic_decays(self):
        vol_scalar = min(1.0, max(0.0, self.inst_variance * 5000.0))
        alpha_fast = np.clip(0.05 + (vol_scalar * 0.25) + (self.kaufman_er * 0.05), 0.05, 0.35)
        return alpha_fast, alpha_fast / 5.0, np.clip(1.0 + (vol_scalar * 4.0), 1.0, 5.0)

    def update_orderbook_pressure(self, best_bid: float, bid_vol: float, best_ask: float, ask_vol: float):
        delta_W = 0.0
        if best_bid > self.prev_bid: delta_W += bid_vol
        elif best_bid == self.prev_bid: delta_W += (bid_vol - self.prev_bid_size)
        else: delta_W -= self.prev_bid_size
            
        if best_ask < self.prev_ask: delta_W -= ask_vol
        elif best_ask == self.prev_ask: delta_W -= (ask_vol - self.prev_ask_size)
        else: delta_W += self.prev_ask_size
            
        self.prev_bid, self.prev_bid_size, self.prev_ask, self.prev_ask_size = best_bid, bid_vol, best_ask, ask_vol
        alpha_fast, alpha_slow, _ = self.get_dynamic_decays()
        
        self.ofi_fast_ewma = (1 - alpha_fast) * self.ofi_fast_ewma + alpha_fast * delta_W
        self.ofi_fast_ewmvar = (1 - alpha_fast) * self.ofi_fast_ewmvar + alpha_fast * (delta_W - self.ofi_fast_ewma)**2
        self.ofi_fast_z = (delta_W - self.ofi_fast_ewma) / (math.sqrt(self.ofi_fast_ewmvar) + 1e-9)
        
        self.ofi_slow_ewma = (1 - alpha_slow) * self.ofi_slow_ewma + alpha_slow * delta_W
        self.ofi_slow_ewmvar = (1 - alpha_slow) * self.ofi_slow_ewmvar + alpha_slow * (delta_W - self.ofi_slow_ewma)**2
        self.ofi_slow_z = (delta_W - self.ofi_slow_ewma) / (math.sqrt(self.ofi_slow_ewmvar) + 1e-9)
        
        current_mid = (best_bid + best_ask) / 2.0
        self.true_micro_price = (best_bid * ask_vol + best_ask * bid_vol) / (bid_vol + ask_vol + 1e-9)
        if current_mid > 0:
            self.micro_price_skew = ((self.true_micro_price - current_mid) / (current_mid + 1e-9)) * 10000.0 

    def update_trades(self, price: float, volume: float, is_buy: bool, current_time: float):
        # 1-Minute Price Sampling for Real-Time Variance
        if current_time - self.last_price_time >= 60.0:
            self.prices.append(price)
            if len(self.prices) > 2:
                safe_curr = max(1e-9, self.prices[-1])
                safe_prev = max(1e-9, self.prices[-2])
                ret = math.log(safe_curr / safe_prev)
                
                if not math.isnan(ret) and not math.isinf(ret):
                    self.log_returns.append(ret)
                    self.vol_ewma = (1 - 0.01) * self.vol_ewma + 0.01 * abs(ret)
            if len(self.log_returns) > 10:
                log_rets_arr = np.fromiter(self.log_returns, dtype=float, count=len(self.log_returns))
                self.inst_variance = float(np.var(log_rets_arr[-10:]) + 1e-9)
            self.last_price_time = current_time

        alpha_fast, alpha_slow, hawkes_decay = self.get_dynamic_decays()
        volume_signed = volume if is_buy else -volume
        
        if self.last_trade_time > 0:
            dt = current_time - self.last_trade_time
            self.hawkes_pressure_state = self.hawkes_pressure_state * math.exp(-hawkes_decay * dt) + volume_signed
        else:
            self.hawkes_pressure_state = volume_signed
            
        self.last_trade_time = current_time
        
        self.hawkes_ewma = (1 - alpha_fast) * self.hawkes_ewma + alpha_fast * self.hawkes_pressure_state
        self.hawkes_ewmvar = (1 - alpha_slow) * self.hawkes_ewmvar + alpha_slow * (self.hawkes_pressure_state - self.hawkes_ewma)**2
        self.hawkes_z = (self.hawkes_pressure_state - self.hawkes_ewma) / (math.sqrt(self.hawkes_ewmvar) + 1e-9)

        self.hawkes_velocity = self.hawkes_z - self.hawkes_z_prev
        self.hawkes_acceleration = self.hawkes_velocity - self.hawkes_v_prev
        self.hawkes_z_prev, self.hawkes_v_prev = self.hawkes_z, self.hawkes_velocity

        # RLS Online Machine Learning Update
        if len(self.prediction_buffer) > 0:
            while self.prediction_buffer and current_time - self.prediction_buffer[0][0] >= 300.0:
                old_time, old_price, features_array, old_pred_prob, virt_sl, virt_tp, action_dir, r_blend = self.prediction_buffer.popleft()
                
                if price != old_price and old_price > 0:
                    
                    # 🚀 V55.2 FIX: Corrected RLS Target Mapping (Predicting Market Direction, not Trade Success)
                    y_target = 0.5 
                    if price >= virt_tp: 
                        y_target = 1.0 if action_dir == "BUY" else 0.0
                    elif price <= virt_sl: 
                        y_target = 0.0 if action_dir == "BUY" else 1.0
                    
                    if y_target == 0.5:
                        y_target = 1.0 if price > old_price else 0.0

                    # Reconstruct the original p_up probability
                    old_p_up = old_pred_prob if action_dir == "BUY" else (1.0 - old_pred_prob)
                    error = y_target - old_p_up 
                    
                    self.ewma_mse = (0.98 * self.ewma_mse) + (0.02 * (error ** 2))
                    x = features_array.reshape(-1, 1)
                    
                    # 🚀 V55.2 FIX: Pure Mixture-of-Experts RLS (Hard Gating, No var_pi Denominator Hacks)
                    dynamic_lambda = max(0.990, min(0.9995, 0.990 + (self.shannon_entropy * 0.0095)))
                    
                    if r_blend > 0.5:
                        # Train Trending Expert exclusively
                        P_x_t = self.P_trending @ x
                        den_t = dynamic_lambda + float((x.T @ P_x_t)[0][0])
                        K_t = P_x_t / den_t
                        self.weights_trending += (K_t.flatten() * error)
                        self.P_trending = (self.P_trending - (K_t @ (x.T @ self.P_trending))) / dynamic_lambda
                        
                        trace_t = np.trace(self.P_trending)
                        if trace_t > 1000.0: 
                            self.P_trending = (self.P_trending * (1000.0 / trace_t)) + (np.eye(9) * 1e-3)
                            self._throttled_log(f"kalman_{self.symbol}", f"[X-RAY] 🔄 KALMAN RESET // {self.symbol} Trending matrix normalized.", 120.0)
                        else: 
                            self.P_trending += np.eye(9) * 1e-3

                    else:
                        # Train Ranging Expert exclusively
                        P_x_r = self.P_ranging @ x
                        den_r = dynamic_lambda + float((x.T @ P_x_r)[0][0])
                        K_r = P_x_r / den_r
                        self.weights_ranging += (K_r.flatten() * error)
                        self.P_ranging = (self.P_ranging - (K_r @ (x.T @ self.P_ranging))) / dynamic_lambda
                        
                        trace_r = np.trace(self.P_ranging)
                        if trace_r > 1000.0: 
                            self.P_ranging = (self.P_ranging * (1000.0 / trace_r)) + (np.eye(9) * 1e-3)
                            self._throttled_log(f"kalman_r_{self.symbol}", f"[X-RAY] 🔄 KALMAN RESET // {self.symbol} Ranging matrix normalized.", 120.0)
                        else: 
                            self.P_ranging += np.eye(9) * 1e-3
                    
                    self.P_trending = (self.P_trending + self.P_trending.T) / 2.0 + (np.eye(9) * 1e-6)
                    self.P_ranging = (self.P_ranging + self.P_ranging.T) / 2.0 + (np.eye(9) * 1e-6)
                    self.rls_updates += 1

    def calibrate_confidence(self, prob: float, regime: str, mse: float) -> float:
        # 🚀 V55.2 FIX: Synchronized Calibration Constants across backtest & live
        floor, ceiling = 0.48, 0.85
        if regime in ["TRENDING_BULL", "TRENDING_BEAR", "TRENDING"]:
            ceiling, floor = min(0.92, ceiling + 0.07), max(0.45, floor - 0.02)
        elif regime == "LIQUIDITY_VACUUM":
            ceiling, floor = min(0.90, ceiling + 0.05), max(0.50, floor + 0.02)
        else:
            ceiling, floor = min(0.80, ceiling - 0.05), max(0.50, floor + 0.02)
            
        mse_penalty = min(0.08, mse * 0.3)
        return max(floor, min(ceiling - mse_penalty, prob))

    def extract_statistical_state(self, current_price: float, vpin_z: float, tensor_alpha: float, sl_dist_pct: float, tp_dist_pct: float, exchange_timestamp: float) -> dict:
        # Calculate Permutation Entropy using stationary log_returns, NOT prices
        if len(self.log_returns) > 10:
            self.shannon_entropy = compute_permutation_entropy(list(self.log_returns)[-20:])
            self.entropy_history.append(self.shannon_entropy) 
        
        # Kaufman ER still correctly uses absolute prices
        if len(self.prices) >= 20:
            prices_arr = np.array(list(self.prices)[-20:])
            self.kaufman_er = float(abs(prices_arr[-1] - prices_arr[0]) / (np.sum(np.abs(np.diff(prices_arr))) + 1e-9))

        ofi_delta_z = self.ofi_fast_z - self.ofi_slow_z
        base_features = np.array([self.ofi_fast_z / 3.0, ofi_delta_z / 6.0, self.hawkes_z / 3.0, self.micro_price_skew / 10.0, vpin_z / 4.0])
        cross_momentum = (self.ofi_fast_z / 3.0) * (self.hawkes_z / 3.0)            
        cross_skew_abs = (self.micro_price_skew / 10.0) * (ofi_delta_z / 6.0)       
        liquidation_div = (self.hawkes_acceleration / 3.0) * (self.micro_price_skew / 10.0) * -1.0
        
        features = np.clip(np.concatenate([base_features, [cross_momentum, cross_skew_abs, liquidation_div, tensor_alpha]]), -1.0, 1.0)
        attention_temp = max(0.15, min(0.48, 0.18 + 0.30 * (1.0 - self.kaufman_er)))
        exp_f = np.exp(np.abs(features) / attention_temp)
        attended_features = features * (exp_f / (np.sum(exp_f) + 1e-9)) * len(features)

        r_blend = 1.0 / (1.0 + math.exp(-12.0 * (self.kaufman_er - 0.35)))
        logit_fused = (r_blend * np.dot(self.weights_trending, attended_features)) + ((1.0 - r_blend) * np.dot(self.weights_ranging, attended_features))
        
        logit = max(-5.0, min(5.0, logit_fused))
        p_up = 1.0 / (1.0 + math.exp(-logit))  
        prob_success = max(p_up, 1.0 - p_up)
        action_dir = "BUY" if p_up > 0.5 else "SELL"
        
        self.historical_probs.append(prob_success)
        
        # True Institutional Quantile & Symmetrical Damped Gate
        if len(self.historical_probs) >= 30:
            prob_arr = np.fromiter(self.historical_probs, dtype=float, count=len(self.historical_probs))
            baseline_gate = float(np.percentile(prob_arr, 60))
            dynamic_ceiling = min(0.98, float(np.percentile(prob_arr, 95)) + 0.05)
        else:
            baseline_gate = 0.55
            dynamic_ceiling = 0.90

        # Symmetrical Entropy Adjustment
        if len(self.entropy_history) > 10:
            ent_arr = np.fromiter(self.entropy_history, dtype=float, count=len(self.entropy_history))
            ent_mean = float(np.mean(ent_arr))
            ent_std = float(np.std(ent_arr)) + 1e-9
            entropy_z = (self.shannon_entropy - ent_mean) / ent_std
            entropy_multiplier = 1.0 + (entropy_z * 0.04)
        else:
            entropy_multiplier = 1.0

        error_scaler = 1.0 + max(0.0, (self.ewma_mse - 0.25) * 0.5)
        raw_gate = baseline_gate * entropy_multiplier * error_scaler
        dynamic_gate = max(0.50, min(dynamic_ceiling, raw_gate))
        
        virt_sl = current_price * (1 - sl_dist_pct) if action_dir == "BUY" else current_price * (1 + sl_dist_pct)
        virt_tp = current_price * (1 + tp_dist_pct) if action_dir == "BUY" else current_price * (1 - tp_dist_pct)

        self.prediction_buffer.append((exchange_timestamp, current_price, attended_features, prob_success, virt_sl, virt_tp, action_dir, r_blend))
        
        return {
            "p_up": p_up, "p_down": 1.0 - p_up, "action_dir": action_dir, 
            "entropy": self.shannon_entropy, "r_blend": r_blend, 
            "dynamic_gate": dynamic_gate, "virtual_sl": virt_sl, "virtual_tp": virt_tp
        }