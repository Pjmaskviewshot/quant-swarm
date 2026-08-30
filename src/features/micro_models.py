"""
💎 V36.3 APEX TITAN: THE ULTIMATE MICROSTRUCTURE ENGINE
--------------------------------------------------------------------------------
Resuming the main branch. Integrates advanced continuous-time alignment, 
exact Bayesian Changepoint Detection, Obizhaeva-Wang LOB Resilience, and 
Merton Jump-Diffusion optimal control into the 25D Volterra-Riemannian Manifold.

Architectural Supremacy (V36.3 Integration):
- Volterra Indexing Resolution: Repaired the Ecosystem Propagator cross-product 
  (Index 15) to restore the primary cross-asset alpha vector.
- True Kelly Rejection: The Merton Jump-Diffusion Kelly formula now accurately returns 
  0.0 for negative edge states, actively rejecting trades rather than defaulting to 0.1%.
- L1-Regularized Riemannian RLS: Applies Proximal Soft-Thresholding to force sparsity.
- Dynamic BOCD Hazard: Scales changepoint hazard rate dynamically via jump volatility.
- HMM Regime Integration: Properly routes features through the Quantum Markov TPM.
- Deadlock Resolution: RLS weights initialized with gaussian noise to break 0.5 parity.
"""

import math
import time
import numpy as np
import logging
from collections import deque
from typing import Tuple, Dict, Any

logger = logging.getLogger("QUANT_CORE.MICRO_MODELS")


class AsynchronousStateAligner:
    """
    🚀 V35.0 SUPERIORITY: Replaces the O(N) loop example with O(1) Continuous-Time Math.
    Aligns irregularly arriving multi-timeframe features instantly using Laplace decay.
    """
    def __init__(self, dim: int, max_age: float = 5.0):
        self.state = np.zeros(dim, dtype=np.float64)
        self.last_times = np.zeros(dim, dtype=np.float64)
        self.kappa = 2.0 / max_age  # Exponential decay constant
        
    def update(self, idx: int, value: float, current_time: float):
        dt = max(0.0, current_time - self.last_times[idx])
        decay = math.exp(-self.kappa * dt)
        self.state[idx] = self.state[idx] * decay + value * (1.0 - decay)
        self.last_times[idx] = current_time
        
    def get_aligned_vector(self, current_time: float) -> np.ndarray:
        dt_array = np.clip(current_time - self.last_times, 0.0, 10.0)
        decay_array = np.exp(-self.kappa * dt_array)
        return self.state * decay_array


class AdamsMacKayBOCD:
    """
    🚀 V36.0 SUPERIORITY: Exact Vectorized Bayesian Online Changepoint Detection.
    Uses Normal-Gamma conjugate priors for true statistical flash-crash detection,
    now featuring Dynamic Hazard scaling.
    """
    def __init__(self, base_hazard: float = 0.01):
        self.base_hazard = base_hazard
        self.run_length_probs = np.array([1.0], dtype=np.float64)
        
        # Conjugate Base Priors
        self.mu0, self.kappa0, self.alpha0, self.beta0 = 0.0, 1.0, 1.0, 1e-4
        
        # Sufficient Statistics Arrays
        self.muT = np.array([self.mu0])
        self.kappaT = np.array([self.kappa0])
        self.alphaT = np.array([self.alpha0])
        self.betaT = np.array([self.beta0])
        
    def update(self, x: float, jump_z: float = 0.0) -> float:
        # 🚀 V36.0 FIX: Dynamic Hazard Rate based on jump volatility
        hazard = float(np.clip(self.base_hazard * (1.0 + abs(jump_z)), 0.001, 0.2))

        # Student-T predictive likelihood
        pred_probs = np.zeros(len(self.muT), dtype=np.float64)
        for i in range(len(self.muT)):
            df_i = 2.0 * self.alphaT[i]
            scale_i = max(1e-8, math.sqrt(self.betaT[i] * (self.kappaT[i] + 1.0) / (self.alphaT[i] * self.kappaT[i])))
            diff_i = x - self.muT[i]
            
            # Log Student-T PDF
            log_pred = (
                math.lgamma((df_i + 1.0) / 2.0) - math.lgamma(df_i / 2.0)
                - 0.5 * math.log(math.pi * df_i) - math.log(scale_i)
                - 0.5 * (df_i + 1.0) * math.log1p((diff_i / scale_i)**2 / df_i)
            )
            pred_probs[i] = math.exp(max(-20.0, log_pred))
        
        growth_probs = self.run_length_probs * pred_probs * (1.0 - hazard)
        cp_prob = float(np.sum(self.run_length_probs * pred_probs * hazard))
        
        self.run_length_probs = np.insert(growth_probs, 0, cp_prob)
        self.run_length_probs /= (np.sum(self.run_length_probs) + 1e-12)
        
        # Bounded Hypothesis Truncation (O(1) memory)
        if len(self.run_length_probs) > 30:
            self.run_length_probs = self.run_length_probs[:30]
            self.run_length_probs /= np.sum(self.run_length_probs)
            
        K = len(self.run_length_probs)
        
        # Conjugate updates
        new_kappa = self.kappaT[:K-1] + 1.0
        new_mu = (self.kappaT[:K-1] * self.muT[:K-1] + x) / new_kappa
        new_alpha = self.alphaT[:K-1] + 0.5
        new_beta = self.betaT[:K-1] + (self.kappaT[:K-1] * (x - self.muT[:K-1])**2) / (2.0 * new_kappa)
        
        self.kappaT = np.insert(new_kappa, 0, self.kappa0)
        self.muT = np.insert(new_mu, 0, self.mu0)
        self.alphaT = np.insert(new_alpha, 0, self.alpha0)
        self.betaT = np.insert(new_beta, 0, self.beta0)
        
        return float(self.run_length_probs[0])


class ObizhaevaWangExecutionSentry:
    """
    🚀 V35.0 SUPERIORITY: Obizhaeva-Wang (2013) Transient Impact Model.
    Replaces static Almgren-Chriss with dynamic Limit Order Book resilience profiling.
    """
    def __init__(self, resilience_rho: float = 0.15, lambda_impact: float = 0.05):
        self.rho = resilience_rho  # Rate at which the LOB replenishes
        self.lambda_impact = lambda_impact
        self.transient_impact = 0.0
        self.last_time = time.time()

    def evaluate_trajectory(self, is_buy: bool, spread_bps: float, volatility: float, hawkes_z: float, trade_qty: float = 1.0) -> Tuple[bool, str]:
        now = time.time()
        dt = max(1e-4, now - self.last_time)
        
        # Impact decays as liquidity naturally returns to the book
        self.transient_impact *= math.exp(-self.rho * dt)
        
        # New instantaneous impact scaled by Hawkes burst intensity
        impact_shock = self.lambda_impact * trade_qty * (1.0 + abs(hawkes_z) * volatility * 100)
        self.transient_impact += impact_shock
        self.last_time = now
        
        # If transient impact completely overwhelms the spread, abort
        if self.transient_impact > (spread_bps * 3.5):
            return True, f"OBIZHAEVA_WANG_LOB_COLLAPSE (Impact: {self.transient_impact:.1f}bps)"
            
        return False, "HEALTHY"


class MertonJumpKellySizer:
    """
    🚀 V36.3 SUPERIORITY: Continuous-Time Merton Jump-Diffusion Kelly.
    Dynamically shrinks the Kelly fraction during Hawkes-identified liquidation cascades.
    """
    def __init__(self):
        self.win_rate = 0.50
        self.avg_win = 1.0
        self.avg_loss = 1.0

    def update(self, net_pnl: float, return_pct: float):
        alpha = 0.05
        if net_pnl > 0:
            self.win_rate = (1 - alpha) * self.win_rate + alpha * 1.0
            self.avg_win = (1 - alpha) * self.avg_win + alpha * abs(return_pct)
        else:
            self.win_rate = (1 - alpha) * self.win_rate + alpha * 0.0
            self.avg_loss = (1 - alpha) * self.avg_loss + alpha * abs(return_pct)

    def compute(self, inst_variance: float, hawkes_intensity: float) -> float:
        b = self.avg_win / max(1e-9, self.avg_loss)
        p = self.win_rate
        q = 1.0 - p
        
        # Standard Kelly
        kelly_f = (b * p - q) / b if b > 0 else 0.0
        
        # Jump-Diffusion Penalty
        jump_penalty = abs(hawkes_intensity) * 0.015  # 1.5% adverse slippage risk per jump
        variance_penalty = 1.0 / (1.0 + inst_variance * 500.0)
        
        f_star = kelly_f * variance_penalty - jump_penalty
        
        # 🚀 V36.3 FIX: Refuse to allocate size if negative edge is detected
        if f_star <= 0.0:
            return 0.0
            
        # Fractional limit mapping to max 1.5% risk allowed by Risk Vault
        return float(np.clip(f_star * 0.25, 0.001, 0.015))


# --- [Keep Existing Supporting Architecture] ---
class InformationTimeClock:
    def __init__(self):
        self.tau = 0.0
        self.last_physical_time = time.time()
        self.base_volume_ewma = 100.0
        
    def tick(self, volume: float, spread_bps: float, physical_time: float) -> float:
        self.base_volume_ewma = (0.99 * self.base_volume_ewma) + (0.01 * max(1.0, volume))
        norm_vol = max(0.01, volume) / self.base_volume_ewma
        d_tau = norm_vol * max(1.0, spread_bps)
        self.tau += d_tau
        self.last_physical_time = physical_time
        return d_tau

class FractionalBrownianHurstEstimator:
    def __init__(self, lags: Tuple[int, ...] = (1, 2, 4, 8, 16)):
        self.lags = lags
        self.prices = deque(maxlen=max(lags) + 2)
        self.means = {lag: 0.0 for lag in lags}
        self.M2 = {lag: 1e-9 for lag in lags}
        self.counts = {lag: 0 for lag in lags}
        self.H = 0.5
        self.rough_volatility = 1e-6

    def update(self, price: float) -> Tuple[float, float]:
        self.prices.append(price)
        if len(self.prices) < max(self.lags) + 1:
            return 0.5, 1e-6

        variances = []
        for lag in self.lags:
            ret = math.log(price / self.prices[-1 - lag])
            self.counts[lag] += 1
            delta = ret - self.means[lag]
            self.means[lag] += delta / min(100, self.counts[lag])
            delta2 = ret - self.means[lag]
            self.M2[lag] = 0.98 * self.M2[lag] + 0.02 * (delta * delta2)
            variances.append(max(1e-12, self.M2[lag]))

        x = np.log(self.lags)
        y = np.log(variances)
        cov_matrix = np.cov(x, y)
        slope = cov_matrix[0, 1] / (cov_matrix[0, 0] + 1e-9)
        
        self.H = float(np.clip(slope / 2.0, 0.01, 0.99))
        self.rough_volatility = math.sqrt(variances[0]) * math.exp(self.H - 0.5)
        return self.H, self.rough_volatility

class MarkedHawkesProcess:
    def __init__(self, decay_rate: float = 2.0):
        self.decay = decay_rate
        self.intensity_buy = 0.0
        self.intensity_sell = 0.0
        self.last_time = time.time()
        self.baseline = 0.05
        self.impact_ewma = 1e-6

    def update(self, volume: float, is_buy: bool, current_time: float) -> float:
        dt = max(1e-4, current_time - self.last_time)
        self.last_time = current_time
        
        decay_factor = math.exp(-self.decay * dt)
        self.intensity_buy *= decay_factor
        self.intensity_sell *= decay_factor

        if volume > 0:
            self.impact_ewma = (0.95 * self.impact_ewma) + (0.05 * volume)
            mark = math.log1p(volume) / math.log1p(self.impact_ewma)
            if is_buy: self.intensity_buy += mark
            else: self.intensity_sell += mark

        lambda_b = self.baseline + self.intensity_buy
        lambda_s = self.baseline + self.intensity_sell
        
        imbalance = (lambda_b - lambda_s) / (lambda_b + lambda_s + 1e-9)
        return float(np.clip(imbalance * 5.0, -5.0, 5.0))

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

        clean_delta_w = delta_W_raw - (delta_cfi * self.fleeting_ratio)
        self.quote_history.append((now, best_bid, bid_vol, best_ask, ask_vol))
        return clean_delta_w, self.cfi_z, self.fleeting_ratio

class KineticAbsorptionTensor:
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

        inst_lambda = abs(velocity_curr)
        self.lambda_ewma = (1.0 - self.alpha) * self.lambda_ewma + self.alpha * inst_lambda

        expected_dp = self.lambda_ewma * trade_volume_signed
        deficit = expected_dp - dp
        
        self.deficit_ewma = (1.0 - self.alpha) * self.deficit_ewma + self.alpha * deficit
        self.deficit_var = (1.0 - self.alpha) * self.deficit_var + self.alpha * (deficit - self.deficit_ewma)**2
        self.swd_z = float(np.clip((deficit - self.deficit_ewma) / (math.sqrt(self.deficit_var) + 1e-9), -5.0, 5.0))

        accel = (velocity_curr - self.velocity_prev) / dv_safe
        self.velocity_prev = velocity_curr
        
        self.accel_ewma = (1.0 - self.alpha) * self.accel_ewma + self.alpha * accel
        self.accel_var = (1.0 - self.alpha) * self.accel_var + self.alpha * (accel - self.accel_ewma)**2
        self.accel_z = float(np.clip((accel - self.accel_ewma) / (math.sqrt(self.accel_var) + 1e-9), -5.0, 5.0))
        
        return self.swd_z, self.accel_z

class CumulativeVolumeDeltaEngine:
    def __init__(self, memory_ticks: int = 500):
        self.cvd = 0.0
        self.cvd_history = deque(maxlen=memory_ticks)
        self.price_history = deque(maxlen=memory_ticks)
        self.cvd_mean = 0.0
        self.cvd_var = 1.0

    def update_trade(self, price: float, volume: float, is_buy: bool) -> Tuple[float, float, float]:
        trade_delta = volume if is_buy else -volume
        self.cvd += trade_delta
        self.cvd_history.append(self.cvd)
        self.price_history.append(price)
        
        delta_stat = self.cvd - self.cvd_mean
        self.cvd_mean += 0.02 * delta_stat
        self.cvd_var = (1.0 - 0.02) * self.cvd_var + 0.02 * (delta_stat ** 2)
        cvd_z = float(np.clip((self.cvd - self.cvd_mean) / (math.sqrt(self.cvd_var) + 1e-9), -5.0, 5.0))
        
        divergence_score = 0.0
        if len(self.price_history) >= 60:
            p_slice = np.array(list(self.price_history)[-60:])
            c_slice = np.array(list(self.cvd_history)[-60:])
            p_delta = p_slice[-1] - p_slice[0]
            c_delta = c_slice[-1] - c_slice[0]
            
            if c_delta < 0 and p_delta >= 0:
                divergence_score = abs(c_delta) / (np.std(c_slice) + 1e-9)
            elif c_delta > 0 and p_delta <= 0:
                divergence_score = -abs(c_delta) / (np.std(c_slice) + 1e-9)
                
        divergence_z = float(np.clip(divergence_score, -5.0, 5.0))
        return self.cvd, cvd_z, divergence_z

class OUMicroReversionKernel:
    def __init__(self, memory_window: int = 200):
        self.price_buffer: deque = deque(maxlen=memory_window)
        self.theta, self.kappa, self.sigma, self.ou_divergence_z = 0.0, 0.5, 1e-4, 0.0
        self.tick_counter = 0

    def update(self, price: float) -> float:
        self.price_buffer.append(price)
        self.tick_counter += 1
        
        if len(self.price_buffer) < 30:
            self.theta = price
            return 0.0

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

        stationary_std = self.sigma / (math.sqrt(2.0 * max(0.01, self.kappa)) + 1e-9)
        self.ou_divergence_z = float(np.clip((price - self.theta) / (stationary_std + 1e-9), -5.0, 5.0))
        return self.ou_divergence_z

class PerpetualFundingOracle:
    def __init__(self):
        self.funding_rate = 0.0
        self.history = deque(maxlen=200)

    def update(self, rate: float):
        self.funding_rate = rate
        self.history.append(rate)

    def get_squeeze_vector(self) -> Tuple[float, float]:
        bias = -float(np.tanh(self.funding_rate * 5000.0))
        if len(self.history) < 10: return bias, 0.0
        arr = np.array(self.history)
        z_score = (self.funding_rate - np.mean(arr)) / (np.std(arr) + 1e-9)
        squeeze_risk = float(np.clip(abs(z_score) / 3.0, 0.0, 1.0))
        return bias, squeeze_risk

class EcosystemPropagator:
    def __init__(self, memory_horizon: int = 40, gamma_decay: float = 0.55):
        self.horizon = memory_horizon
        self.parent_ofi_history = deque(maxlen=memory_horizon)
        lags = np.arange(1, memory_horizon + 1, dtype=np.float64)
        self.weights = lags ** (-gamma_decay)
        self.weights /= np.sum(self.weights)

    def update(self, parent_mlofi_z: float) -> float:
        self.parent_ofi_history.append(parent_mlofi_z)
        if len(self.parent_ofi_history) < 5: return 0.0
        n = len(self.parent_ofi_history)
        w = self.weights[-n:]
        arr = np.array(self.parent_ofi_history)
        return float(np.clip(np.dot(w, arr) / (np.sum(w) + 1e-9), -5.0, 5.0))

class QuantumMarkovRegimeDetector:
    def __init__(self):
        self.beliefs = np.array([0.25, 0.25, 0.25, 0.25], dtype=np.float64)
        self.TPM = np.array([
            [0.94, 0.02, 0.02, 0.02],
            [0.02, 0.94, 0.02, 0.02],
            [0.05, 0.05, 0.85, 0.05],
            [0.05, 0.05, 0.05, 0.85]
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

class InformationGeometricRLS:
    """
    🚀 V36.3 EMPIRICAL HARDENING: L1-Regularized Riemannian Natural Gradient.
    Applies Proximal Soft-Thresholding to force sparsity, crushing overfit Volterra 
    interaction weights to exactly zero if they lack persistent predictive power.
    """
    def __init__(self, dim: int, p_init: float = 1.0, l1_penalty: float = 1e-4):
        self.dim = dim
        # 🚀 V36.3 FIX: Initialize with tiny random noise to break the 0.5 probability deadlock
        self.w = np.random.normal(0, 0.01, dim).astype(np.float64)
        self.F_inv = np.eye(dim, dtype=np.float64) * p_init
        self.I = np.eye(dim, dtype=np.float64)
        self.l1_penalty = l1_penalty

    def update(self, x: np.ndarray, y_target: float, p_pred: float, weight: float = 1.0) -> float:
        err = float(y_target - p_pred)
        x_vec = x.reshape(-1, 1)
        
        fisher_var = p_pred * (1.0 - p_pred) + 1e-6
        Fx = self.F_inv @ x_vec
        lambda_reg = 0.999 # Extended memory to ~1000 ticks
        denom = lambda_reg + float(x_vec.T @ Fx) * fisher_var
        
        if denom < 1e-9: return err

        # Riemannian Natural Gradient step
        natural_grad = (Fx * fisher_var) / denom
        w_temp = self.w + (natural_grad.flatten() * err * weight)

        # 🚀 V36.3 L1 Soft-Thresholding (Proximal Operator for Sparsity)
        self.w = np.sign(w_temp) * np.maximum(np.abs(w_temp) - self.l1_penalty, 0.0)

        # Sherman-Morrison Fisher Inverse Update
        IFx = self.I - (natural_grad @ x_vec.T)
        self.F_inv = (IFx @ self.F_inv @ IFx.T + (natural_grad @ natural_grad.T) * fisher_var) / lambda_reg
        self.F_inv = 0.5 * (self.F_inv + self.F_inv.T) + (self.I * 1e-5)

        tr = np.trace(self.F_inv)
        if tr > 2000.0: self.F_inv *= (2000.0 / tr)

        return err

class BoundedAdaptiveWhitener:
    def __init__(self, dim: int = 16, base_alpha: float = 0.001):
        self.dim = dim
        self.base_alpha = base_alpha
        self.mean_vector = np.zeros(dim, dtype=np.float64)
        self.cov_matrix = np.eye(dim, dtype=np.float64) * 0.1
        self.I = np.eye(dim, dtype=np.float64)
        self.baseline_var = 1e-6

    def get_adaptive_alpha(self, inst_variance: float) -> float:
        self.baseline_var = 0.99 * self.baseline_var + 0.01 * max(1e-9, inst_variance)
        normalized_var = (inst_variance - self.baseline_var) / (self.baseline_var + 1e-9)
        return float(np.clip(self.base_alpha * (1.0 + np.tanh(normalized_var)), 0.0005, 0.008))

    def orthogonalize(self, raw_vec: np.ndarray, inst_variance: float) -> np.ndarray:
        alpha = self.get_adaptive_alpha(inst_variance)
        delta = raw_vec - self.mean_vector
        self.mean_vector += alpha * delta
        
        self.cov_matrix = (1.0 - alpha) * self.cov_matrix + alpha * np.outer(delta, delta)
        self.cov_matrix = 0.5 * (self.cov_matrix + self.cov_matrix.T)
        stable_cov = self.cov_matrix + (self.I * 1e-5)
        
        try:
            L = np.linalg.cholesky(stable_cov)
            return np.clip(np.linalg.solve(L, delta) / 3.0, -3.0, 3.0)
        except np.linalg.LinAlgError:
            diag_stds = np.sqrt(np.maximum(1e-8, np.diag(stable_cov)))
            return np.clip(delta / (diag_stds * 3.0), -3.0, 3.0)

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


class ContinuousMicrostructureEngine:
    """
    🚀 V36.3 APEX TITAN: ASYNC-ALIGNED MASTER ENGINE
    """
    def __init__(self, symbol: str = "GENERIC", memory_depth: int = 1000):
        self.symbol = symbol
        
        self.raw_dim = 19
        self.feature_dim = 25

        self.info_clock = InformationTimeClock()
        self.anti_spoof_kernel = AdversarialSpoofingKernel()
        self.ou_kernel = OUMicroReversionKernel()
        self.kinetic_tensor = KineticAbsorptionTensor()
        self.cvd_engine = CumulativeVolumeDeltaEngine()
        self.funding_oracle = PerpetualFundingOracle()
        self.regime_detector = QuantumMarkovRegimeDetector()
        
        self.bocd = AdamsMacKayBOCD()
        self.obizhaeva_wang_sentry = ObizhaevaWangExecutionSentry()
        self.jump_kelly_sizer = MertonJumpKellySizer()
        self.async_aligner = AsynchronousStateAligner(dim=self.raw_dim)
        
        self.hurst_estimator = FractionalBrownianHurstEstimator()
        self.marked_hawkes = MarkedHawkesProcess()
        self.ecosystem_propagator = EcosystemPropagator()
        self.whitening_engine = BoundedAdaptiveWhitener(dim=self.raw_dim)

        p_scale = 1.0 if any(m in symbol for m in ["BTC", "ETH", "SOL"]) else 2.0
        self.rls_trend = InformationGeometricRLS(dim=self.feature_dim, p_init=p_scale)
        self.rls_range = InformationGeometricRLS(dim=self.feature_dim, p_init=p_scale)
        self.rls_spoof = InformationGeometricRLS(dim=self.feature_dim, p_init=p_scale)
        self.rls_cascade = InformationGeometricRLS(dim=self.feature_dim, p_init=p_scale)

        self.prev_bid = self.prev_bid_size = self.prev_ask = self.prev_ask_size = 0.0
        self.clean_ofi_z = 0.0
        self.true_micro_price = 0.0
        self.micro_elasticity_z = 0.0
        self.meso_fast_ema = self.meso_slow_ema = None
        self.meso_momentum_z = 0.0
        self.p_bid_deplete = 0.5
        self.changepoint_prob = 0.0

        self.tick_prices = deque(maxlen=2000)
        self.inst_variance = 1e-6
        self.kaufman_er = 0.5
        self.shannon_entropy = 1.0
        self.jump_z = 0.0
        self.marked_hawkes_z = 0.0
        self.hurst_h = 0.5
        self.rough_vol = 1e-4

        self.pending_trade_outcomes: Dict[str, dict] = {}
        self.historical_probs = deque(maxlen=2000)
        self.calibration_errors = deque(maxlen=300)
        self.rls_updates = 0

    def update_funding_metrics(self, funding_rate: float):
        self.funding_oracle.update(funding_rate)

    def update_orderbook_pressure(self, bids: list, asks: list):
        if not bids or not asks: return
        
        now = time.time()
        best_bid, bid_vol = float(bids[0][0]), float(bids[0][1])
        best_ask, ask_vol = float(asks[0][0]), float(asks[0][1])
        spread_bps = ((best_ask - best_bid) / (best_bid + 1e-9)) * 10000.0
        
        d_tau = self.info_clock.tick(bid_vol + ask_vol, spread_bps, now)

        deep_bid_vol = sum(float(bids[i][1]) * (0.5 ** i) for i in range(min(5, len(bids))))
        deep_ask_vol = sum(float(asks[i][1]) * (0.5 ** i) for i in range(min(5, len(asks))))

        clean_delta_w, self.cfi_z, self.fleeting_ratio = self.anti_spoof_kernel.process_l2_quote(
            now, best_bid, deep_bid_vol, best_ask, deep_ask_vol, 
            self.prev_bid, self.prev_bid_size, self.prev_ask, self.prev_ask_size
        )
        
        self.prev_bid, self.prev_bid_size = best_bid, deep_bid_vol
        self.prev_ask, self.prev_ask_size = best_ask, deep_ask_vol

        alpha_tau = np.clip(d_tau * 0.1, 0.05, 0.5)
        self.clean_ofi_z = (1.0 - alpha_tau) * self.clean_ofi_z + alpha_tau * clean_delta_w

        mid = (best_bid + best_ask) / 2.0
        imb = deep_bid_vol / (deep_bid_vol + deep_ask_vol + 1e-9)
        
        denom = math.sqrt(deep_bid_vol**2 + deep_ask_vol**2 + 1e-9)
        ratio = float(np.clip((deep_bid_vol - deep_ask_vol) / denom, -0.9999, 0.9999))
        self.p_bid_deplete = (1.0 / math.pi) * math.acos(ratio)

        self.true_micro_price = mid + (max(1e-8, best_ask - best_bid) * (imb - 0.5) * (1.0 + abs(imb - 0.5)))
        total_depth = deep_bid_vol + deep_ask_vol + 1e-9
        self.micro_elasticity_z = float(np.clip((clean_delta_w / total_depth) / (math.sqrt(self.inst_variance) + 1e-5), -5.0, 5.0))

    def update_trades(self, price: float, exchange_timestamp: float = 0.0, volume: float = 0.0, is_buy: bool = True):
        self.tick_prices.append(price)
        dp = price - self.tick_prices[-2] if len(self.tick_prices) > 1 else 0.0
        now = time.time()
        
        self.swd_z, self.accel_z = self.kinetic_tensor.update(dp, volume, volume if is_buy else -volume)
        _, self.cvd_z, self.div_z = self.cvd_engine.update_trade(price, volume, is_buy)
        self.ou_divergence_z = self.ou_kernel.update(price)
        self.hurst_h, self.rough_vol = self.hurst_estimator.update(price)
        self.marked_hawkes_z = self.marked_hawkes.update(volume, is_buy, now)

        self.jump_z = abs(dp) / (math.sqrt(self.inst_variance) * price + 1e-9)

        if len(self.tick_prices) > 1:
            ret = math.log(max(1e-9, price) / max(1e-9, self.tick_prices[-2]))
            if math.isfinite(ret):
                self.inst_variance = (0.95 * self.inst_variance) + (0.05 * (ret ** 2))
                self.jump_z = abs(dp) / (math.sqrt(self.inst_variance) * price + 1e-9)
                # 🚀 V36.0: Pass jump_z to BOCD
                self.changepoint_prob = self.bocd.update(ret, self.jump_z)

        if self.meso_fast_ema is None: 
            self.meso_fast_ema = self.meso_slow_ema = price
        else:
            self.meso_fast_ema = (price - self.meso_fast_ema) * (1.0 if self.jump_z > 3.0 else (2.0 / 51.0)) + self.meso_fast_ema
            self.meso_slow_ema = (price - self.meso_slow_ema) * (2.0 / 301.0) + self.meso_slow_ema

        self.meso_momentum_z = ((self.meso_fast_ema - self.meso_slow_ema) / (self.meso_slow_ema + 1e-9)) / (math.sqrt(self.inst_variance) + 1e-9)

        if len(self.tick_prices) % 50 == 0:
            prices_arr = np.array(self.tick_prices)[-50:]
            self.kaufman_er = float(np.clip(abs(prices_arr[-1] - prices_arr[0]) / (np.sum(np.abs(np.diff(prices_arr))) + 1e-9), 0.0, 1.0))
            if len(self.tick_prices) > 100:
                rets = np.diff(np.log(list(self.tick_prices)[-100:]))
                self.shannon_entropy = compute_permutation_entropy(rets.tolist())

    def evaluate_active_trade_stress(self, is_buy: bool) -> Tuple[bool, str]:
        spread_bps = ((self.prev_ask - self.prev_bid) / (self.prev_bid + 1e-9)) * 10000.0
        # Obizhaeva-Wang Limit Order Book Resilience Tracker replaces static Almgren-Chriss
        return self.obizhaeva_wang_sentry.evaluate_trajectory(is_buy, spread_bps, self.rough_vol, self.marked_hawkes_z, 1.0)

    def extract_statistical_state(self, current_price: float, log_mlofi_z: float, hawkes_z: float, sector_impulse: float, sl_dist_pct: float, tp_dist_pct: float, exchange_timestamp: float, parent_mlofi_z: float = 0.0) -> Dict[str, Any]:
        now = time.time()
        
        # 🚀 V36.1 FIX: Actually use the QuantumMarkovRegimeDetector
        regime_weights = self.regime_detector.update_beliefs(
            self.kaufman_er, 
            self.shannon_entropy, 
            getattr(self, 'fleeting_ratio', 0.0), 
            self.jump_z
        )
        p_t, p_r, p_s, p_c = regime_weights

        funding_bias, squeeze_risk = self.funding_oracle.get_squeeze_vector()
        ecosystem_alpha = self.ecosystem_propagator.update(parent_mlofi_z)

        raw_updates = [
            log_mlofi_z, self.marked_hawkes_z, self.meso_momentum_z, sector_impulse,
            self.micro_elasticity_z, self.ou_divergence_z, getattr(self, 'cfi_z', 0),
            self.jump_z, self.shannon_entropy, self.swd_z, self.accel_z,
            self.hurst_h - 0.5, self.p_bid_deplete, self.cvd_z, self.div_z, ecosystem_alpha,
            funding_bias, squeeze_risk, 0.0
        ]
        
        for i, val in enumerate(raw_updates):
            self.async_aligner.update(i, val, now)
        
        aligned_raw_vec = self.async_aligner.get_aligned_vector(now)

        f = self.whitening_engine.orthogonalize(aligned_raw_vec, self.inst_variance)

        volterra = np.array([
            f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], 
            f[8], f[9], f[10], f[11], f[12], f[13], f[14], f[15], f[16], f[17], f[18],
            f[11] * f[1],  # 19: Hurst x Hawkes 
            f[17] * f[0],  # 20: Squeeze Risk x MLOFI
            f[15] * f[0],  # 🚀 V36.3 FIX: Restored Ecosystem Propagator Vectorization
            f[14] * f[2],  # 22: Absorption Divergence x Micro-Velocity
            f[5] * f[1],   # 23: OU x Hawkes
            1.0            # 24: Bias
        ], dtype=np.float64)

        v_att = volterra / (np.linalg.norm(volterra) + 1e-9)

        l_t = float(np.dot(self.rls_trend.w, v_att))
        l_r = float(np.dot(self.rls_range.w, v_att))
        l_s = float(np.dot(self.rls_spoof.w, v_att))
        l_c = float(np.dot(self.rls_cascade.w, v_att))

        logit = float(np.clip((p_t * l_t) + (p_r * l_r) + (p_s * l_s) + (p_c * l_c), -5.0, 5.0))
        p_up = 1.0 / (1.0 + math.exp(-logit))
        
        execution_style = "MAKER_ONLY" if self.hurst_h < 0.55 else "FLASH_IOC"

        action_dir = "BUY" if p_up > 0.5 else "SELL"
        prob = max(p_up, 1.0 - p_up)
        self.historical_probs.append(prob)

        if len(self.calibration_errors) >= 30:
            q_threshold = float(np.percentile(self.calibration_errors, 70))
        else:
            q_threshold = 0.08
            
        conformal_floor = float(np.clip(0.50 + (q_threshold * 0.3), 0.52, 0.65))

        kelly_target = self.jump_kelly_sizer.compute(self.inst_variance, self.marked_hawkes_z)

        virt_sl = current_price * (1.0 - sl_dist_pct) if action_dir == "BUY" else current_price * (1.0 + sl_dist_pct)
        virt_tp = current_price * (1.0 + tp_dist_pct) if action_dir == "BUY" else current_price * (1.0 - tp_dist_pct)
        dominant_regime = "TRENDING" if p_t > 0.5 else "RANGING"

        return {
            "p_up": p_up, "p_down": 1.0 - p_up, "action_dir": action_dir,
            "execution_style": execution_style,
            "kelly_fraction": kelly_target,
            "dynamic_gate": conformal_floor, 
            "virtual_sl": virt_sl, "virtual_tp": virt_tp,
            "markov_beliefs": {"trend": float(p_t), "range": float(p_r), "disloc": float(p_s), "cascade": float(p_c)},
            "dominant_regime": dominant_regime,
            "hurst_h": self.hurst_h, "bocd_cp_prob": self.changepoint_prob,
            "raw_features": v_att 
        }

    # 🚀 V36.1 FIX 3: Accept allocated_notional to compute exact return percentage
    def resolve_trade_outcome(self, signal_id: str, net_pnl: float, allocated_notional: float = 21.0):
        if signal_id not in self.pending_trade_outcomes:
            return

        ctx = self.pending_trade_outcomes.pop(signal_id)
        action_dir = ctx["action"]
        feats = ctx["features"]
        old_p = ctx["p_up"]
        beliefs = ctx["beliefs"]

        is_win = net_pnl > 0
        y_up = 1.0 if (action_dir == "BUY" and is_win) or (action_dir == "SELL" and not is_win) else 0.0

        non_conformity = abs(y_up - old_p)
        self.calibration_errors.append(non_conformity)

        # 🚀 V36.1 FIX 3: True percentage return against capital at risk
        true_return_pct = net_pnl / max(allocated_notional, 1.0)
        self.jump_kelly_sizer.update(net_pnl, true_return_pct)

        self.rls_trend.update(feats, y_up, old_p, weight=beliefs[0]) 
        self.rls_range.update(feats, y_up, old_p, weight=beliefs[1]) 
        self.rls_spoof.update(feats, y_up, old_p, weight=beliefs[2]) 
        self.rls_cascade.update(feats, y_up, old_p, weight=beliefs[3]) 
        
        self.rls_updates += 1
        if self.rls_updates % 25 == 0:
            logger.info(f"[X-RAY] 🧠 RLS Weights Health Check (Trend Norm): {np.linalg.norm(self.rls_trend.w):.4f} | Kelly: {self.jump_kelly_sizer.win_rate:.1%}")

        logger.debug(f"[X-RAY] 🌌 Riemannian FIM Weights Updated | PnL: {net_pnl:.4f} | Hurst: {self.hurst_h:.2f}")