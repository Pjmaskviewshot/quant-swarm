"""
💎 V5.1 TITANIUM APEX: MULTI-SCALE PREDICTIVE ENGINE
--------------------------------------------------------------
Features:
- Inverted Entropy-Adaptive Forgetting Factor (Fast adaptation during chaos)
- Asymmetric Bivariate Hawkes Kernels (Liquidations & Panics)
- 4D Online Gram-Schmidt Orthogonalization (OFI, Hawkes, Meso, Sector)
"""

import math
import time
import numpy as np
import datetime
import logging
from collections import deque
from itertools import permutations
from typing import Tuple, Dict, Any

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
        # Lower volume requirements ($3M weekend, $5M weekday) to expand micro-cap coverage
        return 3_000_000.0 if cls.is_weekend() else 5_000_000.0

    @classmethod
    def get_ev_floor(cls, routing_mode: str) -> float:
        if routing_mode == "MAKER_ONLY":
            return 0.00001  # Near-zero EV floor for maker limit orders
        return 0.00002       # 0.2 bps EV floor for taker/iceberg orders


class ClusterWarmStartRLS:
    @staticmethod
    def get_cluster_priors(symbol: str) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        # 🚀 UPGRADED: 4-Dimensional Priors (OFI, Hawkes, Meso-Trend, Sector)
        if any(m in symbol for m in ["BTC", "ETH", "SOL"]):
            w_trend = np.array([0.35, 0.25, 0.30, 0.10])
            w_range = np.array([0.15, 0.25, 0.40, 0.20])
            p_scale = 1.0
        elif any(m in symbol for m in ["AVAX", "LINK", "XRP", "ADA", "DOT", "NEAR"]):
            w_trend = np.array([0.30, 0.30, 0.30, 0.10])
            w_range = np.array([0.20, 0.30, 0.30, 0.20])
            p_scale = 2.0
        else:
            w_trend = np.array([0.25, 0.25, 0.35, 0.15])
            w_range = np.array([0.25, 0.25, 0.25, 0.25])
            p_scale = 0.5 
        return w_trend, w_range, np.eye(4) * p_scale


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
    def __init__(self, symbol: str = "GENERIC", memory_depth=1000):
        self.symbol = symbol
        
        # 1. Orderbook State Tracking
        self.prev_bid, self.prev_bid_size, self.prev_ask, self.prev_ask_size = 0.0, 0.0, 0.0, 0.0
        self.ofi_fast_ewma, self.ofi_fast_ewmvar, self.ofi_fast_z = 0.0, 1.0, 0.0
        self.ofi_slow_ewma, self.ofi_slow_ewmvar, self.ofi_slow_z = 0.0, 1.0, 0.0
        self.true_micro_price = 0.0
        
        # 2. Asymmetric Hawkes State Tracking
        self.last_trade_time = 0.0
        self.hawkes_buy_state = 0.0
        self.hawkes_sell_state = 0.0
        self.hawkes_ewma, self.hawkes_ewmvar, self.hawkes_z = 0.0, 1.0, 0.0
        self.hawkes_velocity, self.hawkes_acceleration = 0.0, 0.0
        self.hawkes_z_prev, self.hawkes_v_prev = 0.0, 0.0
        
        # 3. Meso-Trend Structural Momentum (1m/5m proxy)
        self.tick_prices = deque(maxlen=2000)
        self.meso_fast_ema = None
        self.meso_slow_ema = None
        self.meso_momentum_z = 0.0

        # VPIN & Variance State
        self.vpin_z = 0.0
        self.prices = deque(maxlen=memory_depth)
        self.log_returns = deque(maxlen=memory_depth)
        self.inst_variance, self.vol_ewma = 1e-6, 0.0 
        
        self.kaufman_er = 0.5
        self.last_price_time = 0.0  
        self.shannon_entropy = 1.0
        self.entropy_history = deque(maxlen=200) 
        
        # 🚀 4x4 Gram-Schmidt Covariance State Initialization
        self.gs_cov = np.eye(4)
        self.gs_alpha = 0.02
        
        # 🚀 4D RLS Matrices
        w_t, w_r, P_init = ClusterWarmStartRLS.get_cluster_priors(symbol)
        self.weights_trending, self.weights_ranging = w_t, w_r
        self.P_trending, self.P_ranging = P_init.copy(), P_init.copy()
        
        self.p_scale_init = P_init[0][0]
        
        self.prediction_buffer = deque(maxlen=50000)
        self.historical_probs = deque(maxlen=2000) 
        self.rls_updates, self.ewma_mse = 100, 0.25 

    def get_dynamic_decays(self) -> Tuple[float, float, float]:
        vol_scalar = min(1.0, max(0.0, self.inst_variance * 5000.0))
        alpha_fast = np.clip(0.05 + (vol_scalar * 0.25) + (self.kaufman_er * 0.05), 0.05, 0.35)
        return alpha_fast, alpha_fast / 5.0, np.clip(1.0 + (vol_scalar * 4.0), 1.0, 5.0)

    def update_orderbook_pressure(self, best_bid: float, bid_vol: float, best_ask: float, ask_vol: float):
        """Processes Level 2 updates to calculate true micro-price and Order Flow Imbalance (OFI)."""
        delta_W = 0.0
        if best_bid > self.prev_bid: 
            delta_W += bid_vol
        elif best_bid == self.prev_bid: 
            delta_W += (bid_vol - self.prev_bid_size)
        else: 
            delta_W -= self.prev_bid_size
            
        if best_ask < self.prev_ask: 
            delta_W -= ask_vol
        elif best_ask == self.prev_ask: 
            delta_W -= (ask_vol - self.prev_ask_size)
        else: 
            delta_W += self.prev_ask_size
            
        self.prev_bid, self.prev_bid_size, self.prev_ask, self.prev_ask_size = best_bid, bid_vol, best_ask, ask_vol
        alpha_fast, alpha_slow, _ = self.get_dynamic_decays()
        
        self.ofi_fast_ewma = (1 - alpha_fast) * self.ofi_fast_ewma + alpha_fast * delta_W
        self.ofi_fast_ewmvar = (1 - alpha_fast) * self.ofi_fast_ewmvar + alpha_fast * (delta_W - self.ofi_fast_ewma)**2
        self.ofi_fast_z = (delta_W - self.ofi_fast_ewma) / (math.sqrt(self.ofi_fast_ewmvar) + 1e-9)
        
        self.ofi_slow_ewma = (1 - alpha_slow) * self.ofi_slow_ewma + alpha_slow * delta_W
        self.ofi_slow_ewmvar = (1 - alpha_slow) * self.ofi_slow_ewmvar + alpha_slow * (delta_W - self.ofi_slow_ewma)**2
        self.ofi_slow_z = (delta_W - self.ofi_slow_ewma) / (math.sqrt(self.ofi_slow_ewmvar) + 1e-9)
        
        self.true_micro_price = (best_bid * ask_vol + best_ask * bid_vol) / (bid_vol + ask_vol + 1e-9)

    def update_trades(self, price: float, volume: float = 0.0, is_buy: bool = True, current_time: float = 0.0):
        """Processes executed market trades to update Asymmetric Hawkes pressure, volatility, and Meso-Trend."""
        if current_time == 0.0:
            current_time = time.time()

        # 🚀 Meso-Trend Structural Computation
        self.tick_prices.append(price)
        alpha_meso_fast = 2.0 / (51.0)
        alpha_meso_slow = 2.0 / (301.0)
        
        if self.meso_fast_ema is None:
            self.meso_fast_ema = price
            self.meso_slow_ema = price
        else:
            self.meso_fast_ema = (price - self.meso_fast_ema) * alpha_meso_fast + self.meso_fast_ema
            self.meso_slow_ema = (price - self.meso_slow_ema) * alpha_meso_slow + self.meso_slow_ema
            
        trend_divergence = (self.meso_fast_ema - self.meso_slow_ema) / (self.meso_slow_ema + 1e-9)
        self.meso_momentum_z = trend_divergence / (math.sqrt(self.inst_variance) + 1e-9)

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

        # 🚀 ASYMMETRIC HAWKES CASCADE CALCULUS
        alpha_fast, alpha_slow, decay_base = self.get_dynamic_decays()
        
        if self.last_trade_time > 0:
            dt = current_time - self.last_trade_time
            # Panics (Sells) linger longer; FOMO (Buys) decays faster.
            self.hawkes_buy_state *= math.exp(-decay_base * 1.2 * max(0.0, dt))
            self.hawkes_sell_state *= math.exp(-decay_base * 0.7 * max(0.0, dt))
            
        if is_buy:
            self.hawkes_buy_state += volume * 0.50
        else:
            self.hawkes_sell_state += volume * 0.85 # Stronger downside impact

        composite_pressure = self.hawkes_buy_state - self.hawkes_sell_state
        self.last_trade_time = current_time
        
        self.hawkes_ewma = (1 - alpha_fast) * self.hawkes_ewma + alpha_fast * composite_pressure
        self.hawkes_ewmvar = (1 - alpha_slow) * self.hawkes_ewmvar + alpha_slow * (composite_pressure - self.hawkes_ewma)**2
        self.hawkes_z = (composite_pressure - self.hawkes_ewma) / (math.sqrt(self.hawkes_ewmvar) + 1e-9)

        self.hawkes_velocity = self.hawkes_z - self.hawkes_z_prev
        self.hawkes_acceleration = self.hawkes_velocity - self.hawkes_v_prev
        self.hawkes_z_prev, self.hawkes_v_prev = self.hawkes_z, self.hawkes_velocity

    def _process_rls_feedback_loop(self, current_time: float, price: float):
        """Processes historical predictions and computes online error correction (SGD)."""
        if len(self.prediction_buffer) > 0:
            while self.prediction_buffer and current_time - self.prediction_buffer[0][0] >= 60.0:
                old_time, old_price, features_array, old_pred_prob, virt_sl, virt_tp, action_dir, r_blend = self.prediction_buffer.popleft()
                
                if price != old_price and old_price > 0:
                    price_delta = price - old_price
                    risk_distance = abs(old_price - virt_sl) + 1e-9
                    realized_r = price_delta / risk_distance
                    
                    if action_dir == "BUY":
                        y_target = np.clip(0.5 + (realized_r / 4.0), 0.0, 1.0)
                    else:
                        y_target = np.clip(0.5 - (realized_r / 4.0), 0.0, 1.0)

                    old_p_up = old_pred_prob if action_dir == "BUY" else (1.0 - old_pred_prob)
                    error = y_target - old_p_up 
                    
                    self.ewma_mse = (0.98 * self.ewma_mse) + (0.02 * (error ** 2))
                    
                    # Reshape to 4x1 vector for matrix operations
                    x = features_array.reshape(-1, 1)
                    
                    # 🚀 INVERTED ENTROPY SCALING: Higher chaos -> Smaller lambda -> Faster model adaptation
                    entropy_norm = min(1.0, max(0.0, self.shannon_entropy))
                    dynamic_lambda = max(0.965, min(0.998, 0.998 - (entropy_norm * 0.030)))
                    
                    # Trending Matrix Update (4x4)
                    P_x_t = self.P_trending @ x
                    den_t = dynamic_lambda + float((x.T @ P_x_t)[0][0])
                    K_t = P_x_t / den_t
                    self.weights_trending = self.weights_trending + (K_t.flatten() * error * r_blend)
                    self.P_trending = (self.P_trending - (K_t @ (x.T @ self.P_trending))) / dynamic_lambda
                    
                    trace_t = np.trace(self.P_trending)
                    if trace_t > 1000.0: 
                        self.P_trending = (self.P_trending * (1000.0 / trace_t)) + (np.eye(4) * 1e-3)
                    else: 
                        self.P_trending += np.eye(4) * 1e-3
                    
                    # Ranging Matrix Update (4x4)
                    P_x_r = self.P_ranging @ x
                    den_r = dynamic_lambda + float((x.T @ P_x_r)[0][0])
                    K_r = P_x_r / den_r
                    self.weights_ranging = self.weights_ranging + (K_r.flatten() * error * (1.0 - r_blend))
                    self.P_ranging = (self.P_ranging - (K_r @ (x.T @ self.P_ranging))) / dynamic_lambda
                    
                    trace_r = np.trace(self.P_ranging)
                    if trace_r > 1000.0: 
                        self.P_ranging = (self.P_ranging * (1000.0 / trace_r)) + (np.eye(4) * 1e-3)
                    else: 
                        self.P_ranging += np.eye(4) * 1e-3
                    
                    # Enforce Matrix Symmetry
                    self.P_trending = (self.P_trending + self.P_trending.T) / 2.0 + (np.eye(4) * 1e-6)
                    self.P_ranging = (self.P_ranging + self.P_ranging.T) / 2.0 + (np.eye(4) * 1e-6)
                    self.rls_updates += 1

                    # Adaptive Regularization
                    if self.ewma_mse > 0.40:
                        if trace_t < (self.p_scale_init * 1.5):
                            self.P_trending += np.eye(4) * (self.p_scale_init * 0.5)
                        if trace_r < (self.p_scale_init * 1.5):
                            self.P_ranging += np.eye(4) * (self.p_scale_init * 0.5)

    def calibrate_confidence(self, prob: float, regime: str, mse: float) -> float:
        """Dynamically clamps signal confidence bounds based on current HMM regime."""
        floor, ceiling = 0.48, 0.85
        if regime in ["TRENDING_BULL", "TRENDING_BEAR", "TRENDING"]:
            ceiling, floor = min(0.92, ceiling + 0.07), max(0.45, floor - 0.02)
        elif regime == "LIQUIDITY_VACUUM":
            ceiling, floor = min(0.90, ceiling + 0.05), max(0.50, floor + 0.02)
        else:
            ceiling, floor = min(0.80, ceiling - 0.05), max(0.50, floor + 0.02)
            
        mse_penalty = min(0.08, mse * 0.3)
        return max(floor, min(ceiling - mse_penalty, prob))

    def extract_statistical_state(self, current_price: float, log_mlofi_z: float, hawkes_z: float, sector_impulse: float, sl_dist_pct: float, tp_dist_pct: float, exchange_timestamp: float) -> Dict[str, Any]:
        """
        🚀 V5.1 TITANIUM APEX: 4D MACRO FEATURE EXTRACTION
        Ingests Log-MLOFI, Hawkes, Meso-Momentum, and Sector Impulse,
        orthogonalizes via Online Gram-Schmidt, and queries the RLS matrices.
        """
        # 1. Update rolling environment metrics
        if len(self.log_returns) > 10:
            self.shannon_entropy = compute_permutation_entropy(list(self.log_returns)[-20:])
            self.entropy_history.append(self.shannon_entropy) 
        
        if len(self.prices) >= 20:
            prices_arr = np.array(list(self.prices)[-20:])
            self.kaufman_er = float(abs(prices_arr[-1] - prices_arr[0]) / (np.sum(np.abs(np.diff(prices_arr))) + 1e-9))

        # Process asynchronous SGD updates
        self._process_rls_feedback_loop(exchange_timestamp, current_price)

        # 2. ONLINE GRAM-SCHMIDT ORTHOGONALIZATION (4-Dimensional)
        raw_vec = np.array([
            log_mlofi_z,
            hawkes_z,
            self.meso_momentum_z,
            sector_impulse
        ], dtype=float)

        v = raw_vec.copy()
        for i in range(4):
            for j in range(i):
                proj = (np.dot(raw_vec, self.gs_cov[:, j])) / (self.gs_cov[j, j] + 1e-9)
                v[i] -= proj * self.gs_cov[i, j]
            self.gs_cov[i, i] = (1 - self.gs_alpha) * self.gs_cov[i, i] + self.gs_alpha * (v[i] ** 2)

        features = np.clip(v / (np.sqrt(np.diag(self.gs_cov)) + 1e-9) / 4.0, -1.0, 1.0)

        # 3. KAUFMAN-ATTENDED SIGMOID PREDICTION
        attention_temp = max(0.15, min(0.48, 0.18 + 0.30 * (1.0 - self.kaufman_er)))
        exp_f = np.exp(np.abs(features) / attention_temp)
        attended_features = features * (exp_f / (np.sum(exp_f) + 1e-9)) * 4.0

        r_blend = 1.0 / (1.0 + math.exp(-12.0 * (self.kaufman_er - 0.35)))
        logit_fused = (r_blend * np.dot(self.weights_trending, attended_features)) + ((1.0 - r_blend) * np.dot(self.weights_ranging, attended_features))
        
        logit = max(-5.0, min(5.0, logit_fused))
        p_up = 1.0 / (1.0 + math.exp(-logit))  
        prob_success = max(p_up, 1.0 - p_up)
        action_dir = "BUY" if p_up > 0.5 else "SELL"
        
        self.historical_probs.append(prob_success)
        
        # 4. ANTI-STARVATION CONFIDENCE GATING
        if len(self.historical_probs) >= 30:
            prob_arr = np.fromiter(self.historical_probs, dtype=float, count=len(self.historical_probs))
            baseline_gate = float(np.percentile(prob_arr, 60))
            dynamic_ceiling = min(0.72, float(np.percentile(prob_arr, 95)) + 0.05)
        else:
            baseline_gate = 0.55
            dynamic_ceiling = 0.72

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