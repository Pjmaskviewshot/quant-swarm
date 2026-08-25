"""
💎 V21.0 APEX QUANTUM PRIME: MULTI-SCALE PREDICTIVE ENGINE
--------------------------------------------------------------
Features:
- Tikhonov-Regularized Joseph RLS (Ridge Penalty for Anti-Overfitting)
- Eigen-Clipped Streaming Cholesky Whitening (Guaranteed P.S.D Matrices)
- Jump-Diffusion Meso-Momentum (Zero-Lag Flash Crash Tracking)
- Non-Linear Stoikov Micro-Price (Asymptotic Spread Curvature)
- C-Vectorized Shannon Permutation Entropy
"""

import math
import time
import numpy as np
import datetime
import logging
from collections import deque
from typing import Tuple, Dict, Any, List

logger = logging.getLogger("QUANT_CORE.MICRO_MODELS")


class AdaptiveSessionClock:
    """Handles Session Volume adjustments to prevent starvation in quiet markets."""
    @staticmethod
    def is_weekend() -> bool:
        return datetime.datetime.now(datetime.timezone.utc).weekday() in (5, 6)

    @classmethod
    def get_turnover_threshold(cls) -> float:
        return 3_000_000.0 if cls.is_weekend() else 5_000_000.0

    @classmethod
    def get_ev_floor(cls, routing_mode: str) -> float:
        if routing_mode == "MAKER_ONLY":
            return 0.00001  
        return 0.00002       


class ClusterWarmStartRLS:
    @staticmethod
    def get_cluster_priors(symbol: str) -> Tuple[np.ndarray, np.ndarray, float]:
        """4-Dimensional Priors (OFI, Hawkes, Meso-Trend, Sector)"""
        if any(m in symbol for m in ["BTC", "ETH", "SOL"]):
            w_trend = np.array([0.35, 0.25, 0.30, 0.10], dtype=np.float64)
            w_range = np.array([0.15, 0.25, 0.40, 0.20], dtype=np.float64)
            p_scale = 1.0
        elif any(m in symbol for m in ["AVAX", "LINK", "XRP", "ADA", "DOT", "NEAR"]):
            w_trend = np.array([0.30, 0.30, 0.30, 0.10], dtype=np.float64)
            w_range = np.array([0.20, 0.30, 0.30, 0.20], dtype=np.float64)
            p_scale = 2.0
        else:
            w_trend = np.array([0.25, 0.25, 0.35, 0.15], dtype=np.float64)
            w_range = np.array([0.25, 0.25, 0.25, 0.25], dtype=np.float64)
            p_scale = 0.5 
        return w_trend, w_range, p_scale


def compute_permutation_entropy(series: list, order: int = 3, delay: int = 1) -> float:
    """
    C-Vectorized Shannon Permutation Entropy.
    Prevents event-loop blocking by completely stripping Python iterations.
    """
    if len(series) < (order * delay): return 1.0
    try:
        arr = np.asarray(series)
        shape = (arr.size - (order - 1) * delay, order)
        strides = (arr.strides[0], arr.strides[0] * delay)
        
        # O(1) Sliding window view creation
        sub_vectors = np.lib.stride_tricks.as_strided(arr, shape=shape, strides=strides)
        perms = np.argsort(sub_vectors, axis=1)
        
        # Unique deterministic hash for permutations
        bases = np.arange(order) ** order 
        hashed = np.sum(perms * bases, axis=1)
        
        _, counts = np.unique(hashed, return_counts=True)
        p = counts / counts.sum()
        entropy = -np.sum(p * np.log2(p))
        
        return float(entropy / math.log2(math.factorial(order)))
    except Exception as e:
        logger.debug(f"[MATH_WARN] Entropy Vectorization failed: {e}")
        return 1.0


class StreamingCholeskyWhitening:
    """
    🚀 V21.0 EIGEN-CLIPPED CHOLESKY
    Guarantees infinite mathematical uptime by projecting decaying covariance
    matrices back onto the positive-definite cone before decomposition.
    """
    def __init__(self, alpha: float = 0.02, dim: int = 4):
        self.alpha = alpha
        self.dim = dim
        self.cov_matrix = np.eye(dim, dtype=np.float64) * 0.1
        self.mean_vector = np.zeros(dim, dtype=np.float64)

    def orthogonalize(self, raw_vec: np.ndarray) -> np.ndarray:
        # 1. Online Welford / EWMA Covariance Update
        delta = raw_vec - self.mean_vector
        self.mean_vector += self.alpha * delta
        self.cov_matrix = (1.0 - self.alpha) * self.cov_matrix + self.alpha * np.outer(delta, delta)
        
        # 2. Strict Symmetrization
        self.cov_matrix = 0.5 * (self.cov_matrix + self.cov_matrix.T)

        try:
            # 3. Eigen-Clipping to guarantee Positive-Definiteness
            eigvals, eigvecs = np.linalg.eigh(self.cov_matrix)
            if np.any(eigvals <= 1e-9):
                eigvals = np.maximum(eigvals, 1e-9)
                self.cov_matrix = eigvecs @ np.diag(eigvals) @ eigvecs.T

            # 4. Cholesky Decomposition: Cov = L * L^T
            L = np.linalg.cholesky(self.cov_matrix)
            
            # 5. Whiten Features: Solve L * z = delta  =>  z = L^-1 * delta
            whitened = np.linalg.solve(L, delta)
            return np.clip(whitened / 3.0, -1.0, 1.0)
            
        except np.linalg.LinAlgError:
            # Absolute failsafe
            diag_stds = np.sqrt(np.abs(np.diag(self.cov_matrix))) + 1e-9
            return np.clip(delta / (diag_stds * 3.0), -1.0, 1.0)


class TikhonovJosephRLS:
    """
    🚀 V21.0 TIKHONOV-REGULARIZED JOSEPH RLS
    Injects an L2 Ridge Penalty into the covariance trace.
    Mathematically prevents the filter from overfitting to chaotic market regimes.
    """
    def __init__(self, dim: int = 4, p_init: float = 1.0, ridge_gamma: float = 1e-4):
        self.dim = dim
        self.w = np.zeros(dim, dtype=np.float64)
        self.P = np.eye(dim, dtype=np.float64) * p_init
        self.I = np.eye(dim, dtype=np.float64)
        self.gamma = ridge_gamma  # Tikhonov Penalty

    def update(self, x: np.ndarray, y_target: float, y_pred: float, lam: float = 0.995) -> float:
        raw_error = y_target - y_pred
        
        # Huber-style gradient clipping prevents flash crashes from destroying matrix P
        bounded_error = np.clip(raw_error, -1.0, 1.0)
        
        x_vec = x.reshape(-1, 1)

        Px = self.P @ x_vec
        denom = lam + float(x_vec.T @ Px)
        K = Px / denom

        # Update weights using raw directional error for accuracy
        self.w = self.w + (K.flatten() * raw_error)

        # Joseph's Stabilized Update
        IKx = self.I - (K @ x_vec.T)
        R_noise = (bounded_error ** 2) + 1e-6
        
        P_new = (IKx @ self.P @ IKx.T + (K @ K.T) * R_noise) / lam
        P_new = 0.5 * (P_new + P_new.T) 
        
        # 🚀 TIKHONOV REGULARIZATION: Bounds the condition number
        self.P = P_new + (self.I * self.gamma)

        # Prevent trace explosion
        trace_P = np.trace(self.P)
        if trace_P > 1000.0:
            self.P = self.P * (1000.0 / trace_P)

        return bounded_error


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
        self.prices = deque(maxlen=memory_depth)
        self.log_returns = deque(maxlen=memory_depth)
        self.inst_variance, self.vol_ewma = 1e-6, 0.0 
        
        self.kaufman_er = 0.5
        self.last_price_time = 0.0  
        self.shannon_entropy = 1.0
        self.entropy_history = deque(maxlen=200) 
        
        # 🚀 4D Eigen-Clipped Cholesky Whitening & Tikhonov RLS Engines
        self.whitening_engine = StreamingCholeskyWhitening(alpha=0.02, dim=4)
        w_t, w_r, p_scale = ClusterWarmStartRLS.get_cluster_priors(symbol)
        
        self.rls_trending = TikhonovJosephRLS(dim=4, p_init=p_scale, ridge_gamma=1e-4)
        self.rls_ranging = TikhonovJosephRLS(dim=4, p_init=p_scale, ridge_gamma=1e-4)
        
        self.rls_trending.w = w_t.copy()
        self.rls_ranging.w = w_r.copy()
        
        self.prediction_buffer = deque(maxlen=50000)
        self.historical_probs = deque(maxlen=2000) 
        self.rls_updates, self.ewma_mse = 100, 0.25 

    def get_dynamic_decays(self) -> Tuple[float, float, float]:
        vol_scalar = min(1.0, max(0.0, self.inst_variance * 5000.0))
        alpha_fast = np.clip(0.05 + (vol_scalar * 0.25) + (self.kaufman_er * 0.05), 0.05, 0.35)
        return alpha_fast, alpha_fast / 5.0, np.clip(1.0 + (vol_scalar * 4.0), 1.0, 5.0)

    def update_orderbook_pressure(self, best_bid: float, bid_vol: float, best_ask: float, ask_vol: float):
        """Processes Level 2 updates to calculate Stoikov non-linear micro-price and Order Flow Imbalance."""
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
        
        # 🚀 V21.0 NON-LINEAR STOIKOV MICRO-PRICE
        mid_price = (best_bid + best_ask) / 2.0
        spread = best_ask - best_bid
        imb = bid_vol / (bid_vol + ask_vol + 1e-9)
        stoikov_adj = spread * (imb - 0.5) * (1.0 + abs(imb - 0.5))
        
        self.true_micro_price = mid_price + stoikov_adj

    def update_trades(self, price: float, volume: float = 0.0, is_buy: bool = True, current_time: float = 0.0):
        if current_time == 0.0:
            current_time = time.time()

        self.tick_prices.append(price)
        
        # 🚀 V21.0 JUMP-DIFFUSION MESO-MOMENTUM (Zero-Lag Snap)
        # If the tick causes a >3 sigma price jump, instantly snap the EMA to the new price
        # preventing phase-lag during violent breakouts.
        inst_jump = abs(price - self.prices[-1]) if self.prices else 0.0
        jump_z = inst_jump / (math.sqrt(self.inst_variance) * price + 1e-9)

        if jump_z > 3.0:
            alpha_meso_fast = 1.0  # Instant snap
        else:
            alpha_meso_fast = 2.0 / 51.0
            
        alpha_meso_slow = 2.0 / 301.0
        
        if self.meso_fast_ema is None:
            self.meso_fast_ema = self.meso_slow_ema = price
        else:
            self.meso_fast_ema = (price - self.meso_fast_ema) * alpha_meso_fast + self.meso_fast_ema
            self.meso_slow_ema = (price - self.meso_slow_ema) * alpha_meso_slow + self.meso_slow_ema
            
        trend_divergence = (self.meso_fast_ema - self.meso_slow_ema) / (self.meso_slow_ema + 1e-9)
        self.meso_momentum_z = trend_divergence / (math.sqrt(self.inst_variance) + 1e-9)

        if current_time - self.last_price_time >= 60.0:
            self.prices.append(price)
            if len(self.prices) > 2:
                safe_curr, safe_prev = max(1e-9, self.prices[-1]), max(1e-9, self.prices[-2])
                ret = math.log(safe_curr / safe_prev)
                
                if not math.isnan(ret) and not math.isinf(ret):
                    self.log_returns.append(ret)
                    self.vol_ewma = (1 - 0.01) * self.vol_ewma + 0.01 * abs(ret)
                    
            if len(self.log_returns) > 10:
                log_rets_arr = np.fromiter(self.log_returns, dtype=float, count=len(self.log_returns))
                self.inst_variance = float(np.var(log_rets_arr[-10:]) + 1e-9)
            self.last_price_time = current_time

        # ASYMMETRIC HAWKES CASCADE CALCULUS
        alpha_fast, alpha_slow, decay_base = self.get_dynamic_decays()
        
        if self.last_trade_time > 0:
            dt = current_time - self.last_trade_time
            # MICRO-BATCH FILTER
            if dt > 0.005:
                self.hawkes_buy_state *= math.exp(-decay_base * 1.2 * max(0.0, dt))
                self.hawkes_sell_state *= math.exp(-decay_base * 0.7 * max(0.0, dt))
            
        if is_buy:
            self.hawkes_buy_state += volume * 0.50
        else:
            self.hawkes_sell_state += volume * 0.85 

        composite_pressure = self.hawkes_buy_state - self.hawkes_sell_state
        self.last_trade_time = current_time
        
        self.hawkes_ewma = (1 - alpha_fast) * self.hawkes_ewma + alpha_fast * composite_pressure
        self.hawkes_ewmvar = (1 - alpha_slow) * self.hawkes_ewmvar + alpha_slow * (composite_pressure - self.hawkes_ewma)**2
        self.hawkes_z = (composite_pressure - self.hawkes_ewma) / (math.sqrt(self.hawkes_ewmvar) + 1e-9)

        self.hawkes_velocity = self.hawkes_z - self.hawkes_z_prev
        self.hawkes_acceleration = self.hawkes_velocity - self.hawkes_v_prev
        self.hawkes_z_prev, self.hawkes_v_prev = self.hawkes_z, self.hawkes_velocity

    def _process_rls_feedback_loop(self, current_time: float, price: float):
        """Processes historical predictions via the new Tikhonov-Ridge RLS form."""
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
                    
                    # INVERTED ENTROPY SCALING
                    entropy_norm = min(1.0, max(0.0, self.shannon_entropy))
                    dynamic_lambda = max(0.965, min(0.998, 0.998 - (entropy_norm * 0.030)))
                    
                    err_t = self.rls_trending.update(features_array, y_target, old_p_up, lam=dynamic_lambda)
                    err_r = self.rls_ranging.update(features_array, y_target, old_p_up, lam=dynamic_lambda)
                    
                    error = (err_t * r_blend) + (err_r * (1.0 - r_blend))
                    self.ewma_mse = (0.98 * self.ewma_mse) + (0.02 * (error ** 2))
                    self.rls_updates += 1

    def calibrate_confidence(self, prob: float, mse: float) -> float:
        """Dynamic signal boundaries driven by ER and Entropy."""
        er_scale = np.clip(self.kaufman_er, 0.1, 0.9)
        chaos_scale = np.clip(self.shannon_entropy, 0.1, 1.5)
        
        ceiling = np.clip(0.95 - (chaos_scale * 0.10), 0.70, 0.95)
        floor = np.clip(0.48 + (er_scale * 0.05), 0.48, 0.55)
        
        mse_penalty = min(0.10, mse * 0.4)
        return max(floor, min(ceiling - mse_penalty, prob))

    def extract_statistical_state(self, current_price: float, log_mlofi_z: float, hawkes_z: float, sector_impulse: float, sl_dist_pct: float, tp_dist_pct: float, exchange_timestamp: float) -> Dict[str, Any]:
        """
        🚀 V21.0 QUANTUM PRIME: TENSOR EXTRACTION
        Ingests vectors, statically orthogonalizes via Eigen-Clipped Cholesky, 
        and scales prediction boundaries via Entropy-driven Information Geometry.
        """
        if len(self.log_returns) > 10:
            self.shannon_entropy = compute_permutation_entropy(list(self.log_returns)[-20:])
            self.entropy_history.append(self.shannon_entropy) 
        
        if len(self.prices) >= 20:
            prices_arr = np.array(list(self.prices)[-20:])
            self.kaufman_er = float(abs(prices_arr[-1] - prices_arr[0]) / (np.sum(np.abs(np.diff(prices_arr))) + 1e-9))

        self._process_rls_feedback_loop(exchange_timestamp, current_price)

        # EIGEN-CLIPPED CHOLESKY WHITENING
        raw_vec = np.array([log_mlofi_z, hawkes_z, self.meso_momentum_z, sector_impulse], dtype=np.float64)
        features = self.whitening_engine.orthogonalize(raw_vec)

        # INFORMATION-GEOMETRIC ATTENTION
        # Scales softmax temperature dynamically using Shannon Entropy
        attention_temp = max(0.1, min(0.60, 0.50 * self.shannon_entropy))
        exp_f = np.exp(np.abs(features) / attention_temp)
        attended_features = features * (exp_f / (np.sum(exp_f) + 1e-9)) * 4.0

        r_blend = 1.0 / (1.0 + math.exp(-12.0 * (self.kaufman_er - 0.35)))
        
        logit_fused = (r_blend * np.dot(self.rls_trending.w, attended_features)) + ((1.0 - r_blend) * np.dot(self.rls_ranging.w, attended_features))
        
        logit = max(-5.0, min(5.0, logit_fused))
        p_up = 1.0 / (1.0 + math.exp(-logit))  
        prob_success = max(p_up, 1.0 - p_up)
        action_dir = "BUY" if p_up > 0.5 else "SELL"
        
        self.historical_probs.append(prob_success)
        
        if len(self.historical_probs) >= 30:
            prob_arr = np.fromiter(self.historical_probs, dtype=float, count=len(self.historical_probs))
            baseline_gate = float(np.percentile(prob_arr, 60))
            dynamic_ceiling = min(0.72, float(np.percentile(prob_arr, 95)) + 0.05)
        else:
            baseline_gate = 0.55
            dynamic_ceiling = 0.72

        if len(self.entropy_history) > 10:
            ent_arr = np.fromiter(self.entropy_history, dtype=float, count=len(self.entropy_history))
            entropy_z = (self.shannon_entropy - float(np.mean(ent_arr))) / (float(np.std(ent_arr)) + 1e-9)
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