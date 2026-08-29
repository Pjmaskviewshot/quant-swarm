"""
💎 V25.4 APEX QUANTUM PRIME: ADVERSARIAL OMEGA-FRAMEWORK
--------------------------------------------------------------------------------
The undisputed apex predator of High-Frequency Microstructure execution.
Shifts from defensive signal filtration to aggressive adversarial exploitation.

Architectural Supremacy (V25.4 - Final Audit Resolutions):
1. Calibrated Logistic RLS (Online Platt Scaling): Explicitly optimizes for log-loss 
   via a Gauss-Newton approximation, producing true calibrated probabilities.
2. Realized PnL Feedback Loop: Exposes `resolve_trade_outcome` for true labels.
3. Gradient Contamination Fix: RLS updates are now explicitly weighted by Markov 
   Belief likelihoods inside the gradient step to prevent regime degradation.
4. Conformal Prediction Gating: Drops rigid thresholds for dynamic empirical quantile gating.
5. Joseph Form Stability: Uses true Fisher Information (logistic variance) as measurement 
   noise to prevent covariance matrix drift and deflation bias.
"""

import math
import time
import numpy as np
import logging
from collections import deque
from typing import Tuple, Dict, Any, List

logger = logging.getLogger("QUANT_CORE.MICRO_MODELS")


class InformationTimeClock:
    """
    🚀 V25.0 INFORMATION-VOLUME TIME WARPING
    Transforms physical time (dt) into Information Time (dτ).
    High Volatility = Time slows down (high resolution).
    Low Liquidity = Time speeds up (ignores dead periods).
    """
    def __init__(self):
        self.tau = 0.0
        self.last_physical_time = time.time()
        self.base_volume_ewma = 100.0
        
    def tick(self, volume: float, spread_bps: float, physical_time: float) -> float:
        self.base_volume_ewma = (0.99 * self.base_volume_ewma) + (0.01 * max(1.0, volume))
        norm_vol = max(0.01, volume) / self.base_volume_ewma
        
        # Information Quantum: Volume weighted by the cost of liquidity (spread)
        info_quantum = norm_vol * max(1.0, spread_bps)
        
        d_tau = info_quantum
        self.tau += d_tau
        self.last_physical_time = physical_time
        return d_tau


class ClusterWarmStartRLS:
    @staticmethod
    def get_cluster_priors(symbol: str, dim: int = 18) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, float]:
        w_trend, w_range, w_spoof, w_cascade = (np.zeros(dim, dtype=np.float64) for _ in range(4))
        
        if any(m in symbol for m in ["BTC", "ETH", "SOL"]): p_scale = 1.0
        elif any(m in symbol for m in ["AVAX", "LINK", "XRP", "ADA", "DOT", "NEAR", "SUI"]): p_scale = 2.0
        else: p_scale = 3.0

        w_trend[:] = 0.1
        w_range[:] = 0.05
        w_spoof[:] = -0.1
        w_cascade[:] = 0.2
        return w_trend, w_range, w_spoof, w_cascade, p_scale


def compute_permutation_entropy(series: list, order: int = 3, delay: int = 1) -> float:
    if len(series) < (order * delay): return 1.0
    try:
        arr = np.asarray(series, dtype=np.float64)
        shape = (arr.size - (order - 1) * delay, order)
        strides = (arr.strides[0], arr.strides[0] * delay)

        sub_vectors = np.lib.stride_tricks.as_strided(arr, shape=shape, strides=strides)
        perms = np.argsort(sub_vectors, axis=1)

        bases = order ** np.arange(order)
        hashed = np.sum(perms * bases, axis=1)

        _, counts = np.unique(hashed, return_counts=True)
        p = counts / counts.sum()
        p = p[p > 0]
        
        entropy = -np.sum(p * np.log2(p))
        max_entropy = math.log2(math.factorial(order))
        return float(np.clip(entropy / max_entropy, 0.0, 1.0))
    except Exception: return 1.0


class KineticAbsorptionTensor:
    """
    🚀 V25.0 OMNI-KINETIC FUSION
    Absorbs the external edge_gate, exhaustion_exit, and kinetic_predictor into a single native tensor.
    Tracks Kyle's Lambda, Volume-Time Acceleration, and Structural Work Deficits.
    """
    def __init__(self, alpha: float = 0.05):
        self.alpha = alpha
        self.lambda_ewma = 1e-6
        self.deficit_ewma, self.deficit_var = 0.0, 1e-9
        self.velocity_prev = 0.0
        self.accel_ewma, self.accel_var = 0.0, 1e-9
        self.swd_z, self.accel_z = 0.0, 0.0

    def update(self, dp: float, dv: float, trade_volume_signed: float) -> Tuple[float, float]:
        dv_safe = max(abs(dv), 1e-9)
        velocity_curr = dp / dv_safe

        # Expected Impact (Kyle's Lambda)
        inst_lambda = abs(velocity_curr)
        self.lambda_ewma = (1.0 - self.alpha) * self.lambda_ewma + self.alpha * inst_lambda

        # Structural Work Deficit (SWD)
        expected_dp = self.lambda_ewma * trade_volume_signed
        deficit = expected_dp - dp
        
        self.deficit_ewma = (1.0 - self.alpha) * self.deficit_ewma + self.alpha * deficit
        self.deficit_var = (1.0 - self.alpha) * self.deficit_var + self.alpha * (deficit - self.deficit_ewma)**2
        self.swd_z = float(np.clip((deficit - self.deficit_ewma) / (math.sqrt(self.deficit_var) + 1e-9), -5.0, 5.0))

        # Kinematic Acceleration
        accel = (velocity_curr - self.velocity_prev) / dv_safe
        self.velocity_prev = velocity_curr
        
        self.accel_ewma = (1.0 - self.alpha) * self.accel_ewma + self.alpha * accel
        self.accel_var = (1.0 - self.alpha) * self.accel_var + self.alpha * (accel - self.accel_ewma)**2
        self.accel_z = float(np.clip((accel - self.accel_ewma) / (math.sqrt(self.accel_var) + 1e-9), -5.0, 5.0))
        
        return self.swd_z, self.accel_z


class AdversarialSpoofingKernel:
    def __init__(self, fleeting_window_ms: float = 300.0):
        self.fleeting_window = fleeting_window_ms / 1000.0
        self.quote_history, self.recent_cancels = deque(maxlen=200), deque(maxlen=200)
        self.fleeting_ratio, self.cfi_z, self.cfi_ewma, self.cfi_ewmvar = 0.0, 0.0, 0.0, 1.0

    def process_l2_quote(self, physical_time: float, best_bid: float, bid_vol: float, best_ask: float, ask_vol: float, prev_bid: float, prev_bid_vol: float, prev_ask: float, prev_ask_vol: float) -> Tuple[float, float, float]:
        now = physical_time
        bid_canceled, ask_canceled = 0.0, 0.0

        if best_bid == prev_bid and bid_vol < prev_bid_vol:
            bid_canceled = prev_bid_vol - bid_vol
            self.recent_cancels.append((now, "BUY", bid_canceled))
        elif best_bid < prev_bid:
            bid_canceled = prev_bid_vol
            self.recent_cancels.append((now, "BUY", bid_canceled))

        if best_ask == prev_ask and ask_vol < prev_ask_vol:
            ask_canceled = prev_ask_vol - ask_vol
            self.recent_cancels.append((now, "SELL", ask_canceled))
        elif best_ask > prev_ask:
            ask_canceled = prev_ask_vol
            self.recent_cancels.append((now, "SELL", ask_canceled))

        delta_W_raw = 0.0
        if best_bid > prev_bid: delta_W_raw += bid_vol
        elif best_bid == prev_bid: delta_W_raw += (bid_vol - prev_bid_vol)
        else: delta_W_raw -= prev_bid_vol

        if best_ask < prev_ask: delta_W_raw -= ask_vol
        elif best_ask == prev_ask: delta_W_raw -= (ask_vol - prev_ask_vol)
        else: delta_W_raw += prev_ask_vol

        cutoff = now - self.fleeting_window
        while self.quote_history and self.quote_history[0][0] < cutoff: 
            self.quote_history.popleft()
            
        # 🚀 BUG FIX: Prevent memory leak & linear scan CPU spike
        while self.recent_cancels and self.recent_cancels[0][0] < cutoff:
            self.recent_cancels.popleft()

        fleeting_cancel_vol = sum(c[2] for c in self.recent_cancels if c[0] >= cutoff)
        total_depth = (bid_vol + ask_vol) + 1e-9
        self.fleeting_ratio = float(np.clip(fleeting_cancel_vol / total_depth, 0.0, 1.0))

        delta_cfi = ask_canceled - bid_canceled
        alpha_cfi = 0.15
        self.cfi_ewma = (1.0 - alpha_cfi) * self.cfi_ewma + alpha_cfi * delta_cfi
        self.cfi_ewmvar = (1.0 - alpha_cfi) * self.cfi_ewmvar + alpha_cfi * ((delta_cfi - self.cfi_ewma) ** 2)
        self.cfi_z = float(np.clip((delta_cfi - self.cfi_ewma) / (math.sqrt(self.cfi_ewmvar) + 1e-9), -5.0, 5.0))

        spoof_penalty = delta_cfi * self.fleeting_ratio
        clean_delta_w = delta_W_raw - spoof_penalty

        self.quote_history.append((now, best_bid, bid_vol, best_ask, ask_vol))
        return clean_delta_w, self.cfi_z, self.fleeting_ratio


class OUMicroReversionKernel:
    def __init__(self, memory_window: int = 200):
        self.price_buffer: deque = deque(maxlen=memory_window)
        self.theta, self.kappa, self.sigma, self.ou_divergence_z = 0.0, 0.5, 1e-4, 0.0
        self.tick_counter = 0  # 🚀 V25.3 FIX: CPU Throttler

    def update(self, price: float) -> float:
        self.price_buffer.append(price)
        self.tick_counter += 1
        
        if len(self.price_buffer) < 30:
            self.theta = price
            return 0.0

        # 🚀 V25.3 FIX: Only run expensive OLS matrix math every 10 ticks
        if self.tick_counter % 10 == 0:
            prices = np.asarray(self.price_buffer, dtype=np.float64)
            x_prev, x_curr = prices[:-1], prices[1:]
            dx = x_curr - x_prev
            A = np.vstack([x_prev, np.ones_like(x_prev)]).T
            
            try:
                res, _, _, _ = np.linalg.lstsq(A, dx, rcond=None)
                a, b = res[0], res[1]

                if a < -1e-6:
                    self.kappa = float(np.clip(-a * 50.0, 0.05, 10.0))
                    self.theta = float(-b / a)
                    self.sigma = float(max(1e-6, np.std(dx - (a * x_prev + b))))
                else:
                    self.theta, self.kappa, self.sigma = float(np.mean(prices)), 0.1, float(max(1e-6, np.std(dx)))
            except Exception: pass

        # Always update the live Z-score against the cached parameters
        stationary_std = self.sigma / (math.sqrt(2.0 * max(0.01, self.kappa)) + 1e-9)
        self.ou_divergence_z = float(np.clip((price - self.theta) / (stationary_std + 1e-9), -5.0, 5.0))

        return self.ou_divergence_z


class QuantumMarkovRegimeDetector:
    def __init__(self):
        self.beliefs = np.array([0.25, 0.25, 0.25, 0.25], dtype=np.float64)
        self.TPM = np.array([
            [0.94, 0.02, 0.02, 0.02],  # S0: Laminar Trend
            [0.02, 0.94, 0.02, 0.02],  # S1: Ergodic Range
            [0.05, 0.05, 0.85, 0.05],  # S2: Adversarial Vacuum (Spoofing)
            [0.05, 0.05, 0.05, 0.85]   # S3: Flash Cascade (Volatility)
        ], dtype=np.float64)

    def update_beliefs(self, er: float, entropy: float, fleeting: float, jump_z: float) -> np.ndarray:
        prior = self.TPM.T @ self.beliefs
        l_trend = math.exp(-2.0 * ((1.0 - er)**2) - 1.5 * (entropy**2) - 3.0 * (fleeting**2))
        l_range = math.exp(-2.5 * (er**2) - 1.5 * ((1.0 - entropy)**2) - 2.0 * (fleeting**2))
        l_spoof = math.exp(-3.0 * ((1.0 - fleeting)**2) - 1.0 * (er**2))
        l_cascade = math.exp(-1.0 * ((3.0 - min(3.0, abs(jump_z)))**2))
        
        likelihoods = np.array([l_trend, l_range, l_spoof, l_cascade], dtype=np.float64) + 1e-6
        unnormalized = prior * likelihoods
        self.beliefs = unnormalized / (np.sum(unnormalized) + 1e-9)
        return self.beliefs


class StreamingTikhonovCholeskyWhitener:
    def __init__(self, dim: int = 11, alpha: float = 0.001): # 🚀 V25.1 FIX: 0.02 -> 0.001 for matrix stability
        self.dim = dim
        self.alpha = alpha
        self.mean_vector = np.zeros(dim, dtype=np.float64)
        self.cov_matrix = np.eye(dim, dtype=np.float64) * 0.1
        self.I = np.eye(dim, dtype=np.float64)

    def orthogonalize(self, raw_vec: np.ndarray) -> np.ndarray:
        delta = raw_vec - self.mean_vector
        self.mean_vector += self.alpha * delta
        self.cov_matrix = (1.0 - self.alpha) * self.cov_matrix + self.alpha * np.outer(delta, delta)
        self.cov_matrix = 0.5 * (self.cov_matrix + self.cov_matrix.T)
        stable_cov = self.cov_matrix + (self.I * 1e-5)
        
        try:
            L = np.linalg.cholesky(stable_cov)
            return np.clip(np.linalg.solve(L, delta) / 3.0, -2.5, 2.5)
        except np.linalg.LinAlgError:
            diag_stds = np.sqrt(np.maximum(1e-8, np.diag(stable_cov)))
            return np.clip(delta / (diag_stds * 3.0), -2.5, 2.5)


class LogisticVFF_RLS:
    """
    🚀 V25.4 LOGISTIC RLS UPGRADE & MATHEMATICAL RECTIFICATION
    Implements a Gauss-Newton step for logistic regression to optimize log-loss.
    Includes the critical audit fixes for Kalman Gain formulation, Gradient Contamination,
    and True Joseph Form covariance stabilization.
    """
    def __init__(self, dim: int = 18, p_init: float = 1.0):
        self.dim = dim
        self.w = np.zeros(dim, dtype=np.float64)
        self.P = np.eye(dim, dtype=np.float64) * p_init
        self.I = np.eye(dim, dtype=np.float64)
        self.error_history = deque(maxlen=50)

    def update(self, x: np.ndarray, y_target: float, p_pred: float, weight: float = 1.0) -> float:
        # y_target must be exactly 1.0 or 0.0 for logistic loss
        raw_error = float(y_target - p_pred)
        self.error_history.append(abs(raw_error))

        recent_err = np.mean(self.error_history) if len(self.error_history) >= 10 else abs(raw_error)
        dynamic_lambda = max(0.90, min(0.999, 0.998 - (0.08 * recent_err)))

        x_vec = x.reshape(-1, 1)
        
        # Logistic Variance (Fisher Information approximation)
        variance = p_pred * (1.0 - p_pred) + 1e-4  # Prevent zero variance
        
        Px = self.P @ x_vec
        denom = dynamic_lambda + float(x_vec.T @ Px) * variance
        
        if denom < 1e-9: return raw_error

        # 🚀 MATHEMATICAL FIX: Correct Kalman Gain formulation
        K = (Px * variance) / denom
        
        # 🚀 GRADIENT CONTAMINATION FIX: Scale weight update strictly by regime likelihood
        self.w = self.w + (K.flatten() * raw_error * weight)

        IKx = self.I - (K @ x_vec.T)
        
        # 🚀 CRITICAL FIX: Replaced constant 1.0 with true measurement noise (variance)
        self.P = (IKx @ self.P @ IKx.T + (K @ K.T) * variance) / dynamic_lambda
        self.P = 0.5 * (self.P + self.P.T) + (self.I * 1e-4)

        trace_P = np.trace(self.P)
        if trace_P > 600.0: self.P = self.P * (600.0 / trace_P)

        return raw_error


class ContinuousMicrostructureEngine:
    def __init__(self, symbol: str = "GENERIC", memory_depth: int = 1000):
        self.symbol = symbol
        self.feature_dim = 18

        self.info_clock = InformationTimeClock()
        self.anti_spoof_kernel = AdversarialSpoofingKernel()
        self.ou_kernel = OUMicroReversionKernel()
        self.kinetic_tensor = KineticAbsorptionTensor()
        self.regime_detector = QuantumMarkovRegimeDetector()
        
        self.prev_bid, self.prev_bid_size, self.prev_ask, self.prev_ask_size = 0.0, 0.0, 0.0, 0.0
        self.clean_ofi_z = 0.0
        self.ofi_fast_z = 0.0 
        self.true_micro_price = 0.0
        self.micro_elasticity_z = 0.0

        self.hawkes_fast, self.hawkes_slow, self.rough_hawkes_z = 0.0, 0.0, 0.0 

        self.prices, self.log_returns = deque(maxlen=memory_depth), deque(maxlen=memory_depth)
        self.tick_prices = deque(maxlen=2000)
        self.meso_fast_ema, self.meso_slow_ema, self.meso_momentum_z = None, None, 0.0
        self.jump_z = 0.0

        self.inst_variance, self.kaufman_er, self.shannon_entropy = 1e-6, 0.5, 1.0

        self.whitening_engine = StreamingTikhonovCholeskyWhitener(dim=11)
        wt, wr, ws, wc, p_scale = ClusterWarmStartRLS.get_cluster_priors(symbol)

        # 🚀 V25.4 FIX: Replaced linear MSE filter with Logistic Log-Loss filter
        self.rls_trend = LogisticVFF_RLS(dim=18, p_init=p_scale)
        self.rls_range = LogisticVFF_RLS(dim=18, p_init=p_scale)
        self.rls_spoof = LogisticVFF_RLS(dim=18, p_init=p_scale)
        self.rls_cascade = LogisticVFF_RLS(dim=18, p_init=p_scale)
        
        self.rls_trend.w, self.rls_range.w, self.rls_spoof.w, self.rls_cascade.w = wt.copy(), wr.copy(), ws.copy(), wc.copy()

        self.pending_trade_outcomes: Dict[str, dict] = {} # Tracks live trades awaiting PnL resolution
        self.historical_probs = deque(maxlen=2000)
        self.ewma_mse = 0.20
        
        # 🚀 ENHANCEMENT: Conformal Prediction Calibration Buffer
        self.calibration_errors = deque(maxlen=300)

    def update_orderbook_pressure(self, best_bid: float, bid_vol: float, best_ask: float, ask_vol: float):
        now = time.time()
        spread_bps = ((best_ask - best_bid) / (best_bid + 1e-9)) * 10000.0
        
        d_tau = self.info_clock.tick(bid_vol + ask_vol, spread_bps, now)

        clean_delta_w, cfi_z, fleeting = self.anti_spoof_kernel.process_l2_quote(
            now, best_bid, bid_vol, best_ask, ask_vol, self.prev_bid, self.prev_bid_size, self.prev_ask, self.prev_ask_size
        )
        self.prev_bid, self.prev_bid_size = best_bid, bid_vol
        self.prev_ask, self.prev_ask_size = best_ask, ask_vol

        alpha_tau = np.clip(d_tau * 0.1, 0.05, 0.5)
        
        self.clean_ofi_z = (1.0 - alpha_tau) * self.clean_ofi_z + alpha_tau * clean_delta_w
        self.ofi_fast_z = self.clean_ofi_z

        mid = (best_bid + best_ask) / 2.0
        imb = bid_vol / (bid_vol + ask_vol + 1e-9)
        self.true_micro_price = mid + (max(1e-8, best_ask - best_bid) * (imb - 0.5) * (1.0 + abs(imb - 0.5)))

        total_depth = bid_vol + ask_vol + 1e-9
        self.micro_elasticity_z = float(np.clip((clean_delta_w / total_depth) / (math.sqrt(self.inst_variance) + 1e-5), -5.0, 5.0))

    def update_trades(self, price: float, exchange_timestamp: float = 0.0, volume: float = 0.0, is_buy: bool = True):
        self.tick_prices.append(price)
        
        dp = price - self.tick_prices[-2] if len(self.tick_prices) > 1 else 0.0
        self.kinetic_tensor.update(dp, volume, volume if is_buy else -volume)
        
        if len(self.tick_prices) > 1:
            ret = math.log(max(1e-9, price) / max(1e-9, self.tick_prices[-2]))
            if not (math.isnan(ret) or math.isinf(ret)):
                self.inst_variance = (0.95 * self.inst_variance) + (0.05 * (ret ** 2))

        self.ou_kernel.update(price)
        self.jump_z = abs(dp) / (math.sqrt(self.inst_variance) * price + 1e-9)

        if self.meso_fast_ema is None: self.meso_fast_ema = self.meso_slow_ema = price
        else:
            self.meso_fast_ema = (price - self.meso_fast_ema) * (1.0 if self.jump_z > 3.0 else (2.0 / 51.0)) + self.meso_fast_ema
            self.meso_slow_ema = (price - self.meso_slow_ema) * (2.0 / 301.0) + self.meso_slow_ema

        self.meso_momentum_z = ((self.meso_fast_ema - self.meso_slow_ema) / (self.meso_slow_ema + 1e-9)) / (math.sqrt(self.inst_variance) + 1e-9)

        vol_signed = volume if is_buy else -volume
        self.hawkes_fast = (0.80 * self.hawkes_fast) + vol_signed 
        self.hawkes_slow = (0.98 * self.hawkes_slow) + vol_signed 
        self.rough_hawkes_z = float(np.clip((self.hawkes_fast + self.hawkes_slow) / (math.sqrt(self.inst_variance * volume) + 1e-5), -5.0, 5.0))

        if len(self.tick_prices) % 50 == 0:
            prices_arr = np.array(self.tick_prices)[-50:]
            self.kaufman_er = float(np.clip(abs(prices_arr[-1] - prices_arr[0]) / (np.sum(np.abs(np.diff(prices_arr))) + 1e-9), 0.0, 1.0))
            if len(self.tick_prices) > 100:
                rets = np.diff(np.log(list(self.tick_prices)[-100:]))
                self.shannon_entropy = compute_permutation_entropy(rets.tolist())

    def resolve_trade_outcome(self, signal_id: str, net_pnl: float):
        """
        🚀 V25.4 FIX: RLS Gradient Contamination & Conformal Error Tracking
        Isolates learning by injecting the Markov belief directly into the gradient step.
        """
        if signal_id not in self.pending_trade_outcomes:
            return

        trade_context = self.pending_trade_outcomes.pop(signal_id)
        action_dir = trade_context["action"]
        feats = trade_context["features"]
        old_p_up = trade_context["p_up"]
        beliefs = trade_context["beliefs"]

        # Logistic Target (1.0 = Win if BUY / Loss if SELL -> market went UP)
        is_win = net_pnl > 0
        if action_dir == "BUY":
            y_up = 1.0 if is_win else 0.0
        else:
            y_up = 0.0 if is_win else 1.0 

        # Update Conformal Error Buffer
        non_conformity = abs(y_up - old_p_up)
        self.calibration_errors.append(non_conformity)

        # 🚀 Gradient-Attributed RLS Update
        # The update signature natively scales the learning rate by the belief probability 
        e_t = self.rls_trend.update(feats, y_up, old_p_up, weight=beliefs[0]) 
        e_r = self.rls_range.update(feats, y_up, old_p_up, weight=beliefs[1]) 
        e_s = self.rls_spoof.update(feats, y_up, old_p_up, weight=beliefs[2]) 
        e_c = self.rls_cascade.update(feats, y_up, old_p_up, weight=beliefs[3]) 

        # The global MSE is weighted by the regime beliefs
        err = (e_t * beliefs[0]) + (e_r * beliefs[1]) + (e_s * beliefs[2]) + (e_c * beliefs[3])
        self.ewma_mse = (0.98 * self.ewma_mse) + (0.02 * (err ** 2))
        
        logger.debug(f"[X-RAY] 🧠 RLS Neural Weights Updated for {self.symbol} | PnL: {net_pnl:.4f} | Target: {y_up}")

    def extract_statistical_state(self, current_price: float, log_mlofi_z: float, hawkes_z: float, sector_impulse: float, sl_dist_pct: float, tp_dist_pct: float, exchange_timestamp: float) -> Dict[str, Any]:
        beliefs = self.regime_detector.update_beliefs(self.kaufman_er, self.shannon_entropy, self.anti_spoof_kernel.fleeting_ratio, self.jump_z)
        p_t, p_r, p_s, p_c = beliefs

        # 11-Dimensional Baseline Input Vector
        raw_vec = np.array([
            log_mlofi_z,
            self.rough_hawkes_z, 
            self.meso_momentum_z, 
            sector_impulse,
            self.micro_elasticity_z, 
            self.ou_kernel.ou_divergence_z, 
            self.anti_spoof_kernel.cfi_z,
            self.jump_z, 
            self.shannon_entropy,
            self.kinetic_tensor.swd_z, 
            self.kinetic_tensor.accel_z
        ], dtype=np.float64)

        f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11 = self.whitening_engine.orthogonalize(raw_vec)

        exploit_spoof = f7 * -f3      
        predict_iceberg = f1 * f10    
        kinetic_reversal = f11 * f2   
        survive_cascade = f8 * f5     
        exploit_macro = f4 * f1       
        exploit_reversion = f6 * f2   
        
        volterra = np.array([
            f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11,
            exploit_spoof, predict_iceberg, kinetic_reversal, survive_cascade, exploit_macro, exploit_reversion,
            1.0 # Bias
        ], dtype=np.float64)

        norm = np.linalg.norm(volterra) + 1e-9
        volterra_att = volterra / norm

        l_t = float(np.dot(self.rls_trend.w, volterra_att))
        l_r = float(np.dot(self.rls_range.w, volterra_att))
        l_s = float(np.dot(self.rls_spoof.w, volterra_att))
        l_c = float(np.dot(self.rls_cascade.w, volterra_att))

        logit = float(np.clip((p_t * l_t) + (p_r * l_r) + (p_s * l_s) + (p_c * l_c), -5.0, 5.0))
        p_up = 1.0 / (1.0 + math.exp(-logit))
        
        action_dir = "BUY" if p_up > 0.5 else "SELL"
        prob = max(p_up, 1.0 - p_up)
        self.historical_probs.append(prob)

        if p_s > 0.5 and prob > 0.65:
            logger.info(f"[X-RAY] 🛡️ ADVERSARIAL SPOOF REGIME // {self.symbol} | Vacuum Detected. Proceeding with caution.")
        if p_c > 0.5 and prob > 0.65:
            logger.warning(f"[X-RAY] 🌊 VOLATILITY CASCADE // {self.symbol} | Flash crash elasticity identified. Dampening exposure.")

        # 🚀 ENHANCEMENT: Conformal Prediction Gating (Dynamic Confidence Hurdle)
        if len(self.calibration_errors) >= 50:
            # Require confidence to exceed the 85th percentile of recent model errors
            q_threshold = float(np.percentile(self.calibration_errors, 85))
        else:
            q_threshold = 0.12
            
        conformal_floor = 0.50 + (q_threshold * 0.5)

        virt_sl = current_price * (1.0 - sl_dist_pct) if action_dir == "BUY" else current_price * (1.0 + sl_dist_pct)
        virt_tp = current_price * (1.0 + tp_dist_pct) if action_dir == "BUY" else current_price * (1.0 - tp_dist_pct)

        dominant_regime = "TRENDING" if p_t > 0.5 else "RANGING"

        return {
            "p_up": p_up, "p_down": 1.0 - p_up, "action_dir": action_dir,
            "entropy": self.shannon_entropy, "r_blend": p_t, 
            "dynamic_gate": conformal_floor, 
            "virtual_sl": virt_sl, "virtual_tp": virt_tp,
            "markov_beliefs": {"trend": float(p_t), "range": float(p_r), "disloc": float(p_s), "cascade": float(p_c)},
            "dominant_regime": dominant_regime,
            "ou_divergence_z": self.ou_kernel.ou_divergence_z, "cfi_z": self.anti_spoof_kernel.cfi_z,
            "fleeting_ratio": self.anti_spoof_kernel.fleeting_ratio, "clean_ofi_z": self.clean_ofi_z,
            "raw_features": volterra_att 
        }