"""
V39.1 APEX TITAN: ZERO-ALLOCATION STATISTICAL MICROSTRUCTURE ENGINE
--------------------------------------------------------------------------------
Ultra-low latency continuous-time microstructure forecasting engine. Integrates 
pre-allocated zero-allocation feature buffers, closed-form Ornstein-Uhlenbeck 
calibration, vectorized Adams-MacKay BOCD, spectrally clamped Joseph-form RLS, 
and Bayesian-prior Merton Jump-Diffusion optimal control into the 25D Manifold.

Architectural Supremacy (V39.1 Production Fixes):
- Uniform 25D Manifold Normalization: Eradicates the unit hyper-cylinder distortion
  where dynamic feature norms inflated during volatility shocks, causing the static
  intercept to dominate RLS predictions.
- Conservative Bayesian Prior Anchors: Replaced overly optimistic 58% win-rate priors
  with break-even baseline conjugate priors (50% win rate, 1.05 payoff, weight=5.0)
  to eliminate capital oversizing and drawdown vulnerability on initial boot.
- Production RLS Weight Freeze Gate: Introduces an execution freeze gate via
  FREEZE_RLS_WEIGHTS to prevent parameter degradation and catastrophic forgetting
  from high-frequency trade noise during live execution.
"""

import os
import math
import time
import numpy as np
import logging
from collections import deque
from typing import Tuple, Dict, Any

logger = logging.getLogger("QUANT_CORE.MICRO_MODELS")


class AsynchronousStateAligner:
    """
    Continuous-Time State Synchronizer: Aligns irregularly arriving 
    orderbook and trade telemetry using continuous Laplace decay kernels.
    """
    def __init__(self, dim: int, max_age: float = 5.0):
        self.dim = dim
        self.state = np.zeros(dim, dtype=np.float64)
        self.last_times = np.zeros(dim, dtype=np.float64)
        self.kappa = 2.0 / max_age
        self._aligned_buf = np.zeros(dim, dtype=np.float64)

    def update(self, idx: int, value: float, current_time: float):
        if self.last_times[idx] == 0.0:
            self.state[idx] = value
            self.last_times[idx] = current_time
            return

        dt = max(0.0, current_time - self.last_times[idx])
        decay = math.exp(-self.kappa * dt)
        self.state[idx] = self.state[idx] * decay + value * (1.0 - decay)
        self.last_times[idx] = current_time

    def get_aligned_vector(self, current_time: float) -> np.ndarray:
        for i in range(self.dim):
            t_last = self.last_times[i]
            if t_last > 0.0:
                dt = min(10.0, max(0.0, current_time - t_last))
                self._aligned_buf[i] = self.state[i] * math.exp(-self.kappa * dt)
            else:
                self._aligned_buf[i] = 0.0
        return self._aligned_buf


class AdamsMacKayBOCD:
    """
    Zero-Allocation Vectorized Bayesian Online Changepoint Detection (BOCD) with 
    Normal-Gamma conjugate priors and jump-scaled volatility hazard rates.
    """
    def __init__(self, base_hazard: float = 0.01, max_run_length: int = 30):
        self.base_hazard = base_hazard
        self.max_run_length = max_run_length
        self.curr_len = 1

        self.run_length_probs = np.zeros(max_run_length, dtype=np.float64)
        self.run_length_probs[0] = 1.0

        # Conjugate Base Hyperparameters
        self.mu0 = 0.0
        self.kappa0 = 1.0
        self.alpha0 = 1.0
        self.beta0 = 1e-4

        self.muT = np.zeros(max_run_length, dtype=np.float64)
        self.kappaT = np.zeros(max_run_length, dtype=np.float64)
        self.alphaT = np.zeros(max_run_length, dtype=np.float64)
        self.betaT = np.zeros(max_run_length, dtype=np.float64)

        self.muT[0] = self.mu0
        self.kappaT[0] = self.kappa0
        self.alphaT[0] = self.alpha0
        self.betaT[0] = self.beta0

    def update(self, x: float, jump_z: float = 0.0) -> float:
        hazard = float(np.clip(self.base_hazard * (1.0 + abs(jump_z)), 0.001, 0.25))
        k = self.curr_len

        # Vectorized Student-T Predictive Distribution
        active_alpha = self.alphaT[:k]
        active_beta = self.betaT[:k]
        active_kappa = self.kappaT[:k]
        active_mu = self.muT[:k]

        df = 2.0 * active_alpha
        scale = np.sqrt(np.maximum(1e-12, active_beta * (active_kappa + 1.0) / (active_alpha * active_kappa)))
        diff = x - active_mu

        # Numerically stable log Student-T PDF
        log_pred = (
            np.asarray([math.lgamma((d + 1.0) / 2.0) - math.lgamma(d / 2.0) for d in df])
            - 0.5 * np.log(np.pi * df)
            - np.log(scale)
            - 0.5 * (df + 1.0) * np.log1p((diff / scale) ** 2 / df)
        )
        pred_probs = np.exp(np.clip(log_pred, -30.0, 0.0))

        # Recursive changepoint message propagation
        r_active = self.run_length_probs[:k]
        growth_probs = r_active * pred_probs * (1.0 - hazard)
        cp_prob = float(np.sum(r_active * pred_probs * hazard))

        next_len = min(k + 1, self.max_run_length)
        update_k = next_len - 1

        # Posterior conjugate statistic updates (In-place slice assignment)
        new_kappa = active_kappa[:update_k] + 1.0
        new_mu = (active_kappa[:update_k] * active_mu[:update_k] + x) / new_kappa
        new_alpha = active_alpha[:update_k] + 0.5
        new_beta = active_beta[:update_k] + (active_kappa[:update_k] * (x - active_mu[:update_k]) ** 2) / (2.0 * new_kappa)

        self.muT[1:next_len] = new_mu
        self.muT[0] = self.mu0
        self.kappaT[1:next_len] = new_kappa
        self.kappaT[0] = self.kappa0
        self.alphaT[1:next_len] = new_alpha
        self.alphaT[0] = self.alpha0
        self.betaT[1:next_len] = new_beta
        self.betaT[0] = self.beta0

        self.run_length_probs[0] = cp_prob
        self.run_length_probs[1:next_len] = growth_probs[:update_k]

        total_p = float(np.sum(self.run_length_probs[:next_len]) + 1e-12)
        self.run_length_probs[:next_len] /= total_p
        if next_len < self.max_run_length:
            self.run_length_probs[next_len:] = 0.0

        self.curr_len = next_len
        return float(self.run_length_probs[0])


class ObizhaevaWangExecutionSentry:
    """
    Transient Market Impact and Orderbook Resilience Monitor (Obizhaeva & Wang 2013).
    Ejects orders when transient liquidity displacement exceeds book recovery capacity.
    """
    def __init__(self, resilience_rho: float = 0.20, lambda_impact: float = 0.04):
        self.rho = resilience_rho
        self.lambda_impact = lambda_impact
        self.transient_impact = 0.0
        self.last_time = time.time()

    def evaluate_trajectory(self, is_buy: bool, spread_bps: float, volatility: float, hawkes_z: float, trade_qty: float = 1.0) -> Tuple[bool, str]:
        now = time.time()
        dt = max(1e-4, now - self.last_time)
        self.transient_impact *= math.exp(-self.rho * dt)

        impact_shock = self.lambda_impact * trade_qty * (1.0 + abs(hawkes_z) * max(volatility, 1e-6) * 100.0)
        self.transient_impact += impact_shock
        self.last_time = now

        if not math.isfinite(self.transient_impact):
            self.transient_impact = 0.0

        if self.transient_impact > max(1.5, spread_bps * 3.2):
            return True, f"OBIZHAEVA_WANG_COLLAPSE (Impact: {self.transient_impact:.1f}bps > SpreadMult: {spread_bps * 3.2:.1f}bps)"

        return False, "HEALTHY"


class MertonJumpKellySizer:
    """
    Continuous-Time Merton Jump-Diffusion Kelly Capital Allocator.
    Anchored with conservative Bayesian conjugate priors to prevent early position oversizing.
    """
    def __init__(self, prior_win_rate: float = 0.50, prior_payoff: float = 1.05, prior_weight: float = 5.0):
        self.prior_w = prior_weight
        self.wins_accum = prior_win_rate * prior_weight
        self.trials_accum = prior_weight

        # Symmetric baseline initial returns
        self.win_return_sum = prior_payoff * 2.5
        self.win_return_count = 2.5
        self.loss_return_sum = 1.0 * 2.5
        self.loss_return_count = 2.5

        self.win_rate = prior_win_rate
        self.avg_win = prior_payoff
        self.avg_loss = 1.0

    def update(self, net_pnl: float, return_pct: float):
        ret_mag = max(1e-4, abs(return_pct))
        self.trials_accum += 1.0

        if net_pnl > 0:
            self.wins_accum += 1.0
            self.win_return_sum += ret_mag
            self.win_return_count += 1.0
        else:
            self.loss_return_sum += ret_mag
            self.loss_return_count += 1.0

        self.win_rate = self.wins_accum / self.trials_accum
        self.avg_win = self.win_return_sum / self.win_return_count
        self.avg_loss = self.loss_return_sum / self.loss_return_count

    def compute(self, inst_variance: float, hawkes_intensity: float) -> float:
        b = self.avg_win / max(1e-6, self.avg_loss)
        p = self.win_rate
        q = 1.0 - p

        raw_kelly = (b * p - q) / b if b > 0.0 else 0.0
        jump_penalty = abs(hawkes_intensity) * 0.012
        variance_dampener = 1.0 / (1.0 + inst_variance * 400.0)

        f_star = (raw_kelly * variance_dampener) - jump_penalty

        if f_star <= 0.002:
            return 0.0

        return float(np.clip(f_star * 0.25, 0.002, 0.015))


class InformationTimeClock:
    """
    Sub-Second Volume-Synchronized Clock (Easley, López de Prado, O'Hara).
    Measures information arrival velocity instead of physical wall-clock intervals.
    """
    def __init__(self):
        self.tau = 0.0
        self.last_physical_time = time.time()
        self.base_volume_ewma = 100.0

    def tick(self, volume: float, spread_bps: float, physical_time: float) -> float:
        safe_vol = max(1.0, volume) if math.isfinite(volume) else 1.0
        self.base_volume_ewma = (0.99 * self.base_volume_ewma) + (0.01 * safe_vol)
        norm_vol = max(0.01, safe_vol) / self.base_volume_ewma
        d_tau = norm_vol * max(1.0, spread_bps)
        self.tau += d_tau
        self.last_physical_time = physical_time
        return d_tau


class FractionalBrownianHurstEstimator:
    """
    O(1) Memory Analytical Rescaled Variance Hurst Exponent Estimator.
    Uses precomputed analytical OLS coordinates to eliminate per-tick covariance loops.
    """
    def __init__(self, lags: Tuple[int, ...] = (1, 2, 4, 8, 16)):
        self.lags = np.array(lags, dtype=np.float64)
        self.prices = deque(maxlen=int(max(lags)) + 2)
        self.means = {lag: 0.0 for lag in lags}
        self.m2 = {lag: 1e-9 for lag in lags}
        self.counts = {lag: 0 for lag in lags}

        self.x_vals = np.log(self.lags)
        self.x_mean = float(np.mean(self.x_vals))
        self.x_diff = self.x_vals - self.x_mean
        self.ss_x = float(np.sum(self.x_diff ** 2) + 1e-9)

        self.hurst_h = 0.5
        self.rough_volatility = 1e-6

    def update(self, price: float) -> Tuple[float, float]:
        self.prices.append(price)
        if len(self.prices) < int(self.lags[-1]) + 1:
            return 0.5, 1e-6

        variances = []
        for lag in self.lags:
            lag_int = int(lag)
            ret = math.log(max(1e-9, price) / max(1e-9, self.prices[-1 - lag_int]))
            self.counts[lag_int] += 1
            delta = ret - self.means[lag_int]
            self.means[lag_int] += delta / min(100, self.counts[lag_int])
            delta2 = ret - self.means[lag_int]
            self.m2[lag_int] = 0.98 * self.m2[lag_int] + 0.02 * (delta * delta2)
            variances.append(max(1e-12, self.m2[lag_int]))

        y_vals = np.log(variances)
        slope = float(np.sum(self.x_diff * (y_vals - np.mean(y_vals))) / self.ss_x)

        self.hurst_h = float(np.clip(slope / 2.0, 0.01, 0.99))
        self.rough_volatility = math.sqrt(variances[0]) * math.exp(self.hurst_h - 0.5)
        return self.hurst_h, self.rough_volatility


class MarkedHawkesProcess:
    """
    Self-Exciting Bivariate Point Process with Asymmetric Volume Marks.
    Quantifies aggressive order flow clustering and institutional cascade intensity.
    """
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

        if volume > 0.0 and math.isfinite(volume):
            self.impact_ewma = (0.95 * self.impact_ewma) + (0.05 * volume)
            mark = math.log1p(volume) / (math.log1p(self.impact_ewma) + 1e-9)
            if is_buy:
                self.intensity_buy += mark
            else:
                self.intensity_sell += mark

        lambda_b = self.baseline + self.intensity_buy
        lambda_s = self.baseline + self.intensity_sell

        imbalance = (lambda_b - lambda_s) / (lambda_b + lambda_s + 1e-9)
        return float(np.clip(imbalance * 5.0, -5.0, 5.0))


class AdversarialSpoofingKernel:
    """
    L2 Depth Fleet Tracking and Fleeting Cancellation Flow Kernel.
    Separates genuine orderbook commitment from deceptive algorithmic spoofing.
    """
    def __init__(self, fleeting_window_ms: float = 300.0):
        self.fleeting_window = fleeting_window_ms / 1000.0
        self.quote_history = deque(maxlen=200)
        self.recent_cancels = deque(maxlen=200)
        self.fleeting_ratio = 0.0
        self.cfi_z = 0.0
        self.cfi_ewma = 0.0
        self.cfi_ewmvar = 1.0

    def process_l2_quote(
        self, physical_time: float, best_bid: float, bid_vol: float,
        best_ask: float, ask_vol: float, prev_bid: float, prev_bid_vol: float,
        prev_ask: float, prev_ask_vol: float
    ) -> Tuple[float, float, float]:
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

        delta_w_raw = 0.0
        if best_bid > prev_bid:
            delta_w_raw += bid_vol
        elif best_bid == prev_bid:
            delta_w_raw += (bid_vol - prev_bid_vol)
        else:
            delta_w_raw -= prev_bid_vol

        if best_ask < prev_ask:
            delta_w_raw -= ask_vol
        elif best_ask == prev_ask:
            delta_w_raw -= (ask_vol - prev_ask_vol)
        else:
            delta_w_raw += prev_ask_vol

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

        clean_delta_w = delta_w_raw - (delta_cfi * self.fleeting_ratio)
        self.quote_history.append((now, best_bid, bid_vol, best_ask, ask_vol))
        return clean_delta_w, self.cfi_z, self.fleeting_ratio


class KineticAbsorptionTensor:
    """
    Structural Work Deficit (SWD) and Kinematic Order Flow Acceleration Engine.
    Detects hidden iceberg absorption when price fails to move despite heavy volume.
    """
    def __init__(self, alpha: float = 0.05):
        self.alpha = alpha
        self.lambda_ewma = 1e-6
        self.deficit_ewma = 0.0
        self.deficit_var = 1e-9
        self.velocity_prev = 0.0
        self.accel_ewma = 0.0
        self.accel_var = 1e-9
        self.swd_z = 0.0
        self.accel_z = 0.0

    def update(self, dp: float, dv: float, trade_volume_signed: float) -> Tuple[float, float]:
        dv_safe = max(abs(dv), 1e-9)
        velocity_curr = dp / dv_safe

        inst_lambda = abs(velocity_curr)
        self.lambda_ewma = (1.0 - self.alpha) * self.lambda_ewma + self.alpha * inst_lambda

        expected_dp = self.lambda_ewma * trade_volume_signed
        deficit = expected_dp - dp

        self.deficit_ewma = (1.0 - self.alpha) * self.deficit_ewma + self.alpha * deficit
        self.deficit_var = (1.0 - self.alpha) * self.deficit_var + self.alpha * ((deficit - self.deficit_ewma) ** 2)
        self.swd_z = float(np.clip((deficit - self.deficit_ewma) / (math.sqrt(self.deficit_var) + 1e-9), -5.0, 5.0))

        accel = (velocity_curr - self.velocity_prev) / dv_safe
        self.velocity_prev = velocity_curr

        self.accel_ewma = (1.0 - self.alpha) * self.accel_ewma + self.alpha * accel
        self.accel_var = (1.0 - self.alpha) * self.accel_var + self.alpha * ((accel - self.accel_ewma) ** 2)
        self.accel_z = float(np.clip((accel - self.accel_ewma) / (math.sqrt(self.accel_var) + 1e-9), -5.0, 5.0))

        return self.swd_z, self.accel_z


class CumulativeVolumeDeltaEngine:
    """
    Sub-Second Cumulative Volume Delta (CVD) and Tick Divergence Tracker.
    """
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

            std_c = float(np.std(c_slice) + 1e-9)
            if c_delta < 0.0 and p_delta >= 0.0:
                divergence_score = abs(c_delta) / std_c
            elif c_delta > 0.0 and p_delta <= 0.0:
                divergence_score = -abs(c_delta) / std_c

        divergence_z = float(np.clip(divergence_score, -5.0, 5.0))
        return self.cvd, cvd_z, divergence_z


class OUMicroReversionKernel:
    """
    Closed-Form Analytical Ornstein-Uhlenbeck (OU) Mean Reversion Kernel.
    Replaces matrix pseudo-inverses with direct moments for zero-latency parameter solves.
    """
    def __init__(self, memory_window: int = 200):
        self.price_buffer = deque(maxlen=memory_window)
        self.theta = 0.0
        self.kappa = 0.5
        self.sigma = 1e-4
        self.ou_divergence_z = 0.0
        self.tick_counter = 0

    def update(self, price: float) -> float:
        self.price_buffer.append(price)
        self.tick_counter += 1

        if len(self.price_buffer) < 30:
            self.theta = price
            return 0.0

        if self.tick_counter % 10 == 0:
            prices = np.asarray(self.price_buffer, dtype=np.float64)
            x_prev = prices[:-1]
            dx = prices[1:] - x_prev

            # Analytical OLS solution: dx = a * x_prev + b + epsilon
            x_m = np.mean(x_prev)
            dx_m = np.mean(dx)
            ss_xx = np.sum((x_prev - x_m) ** 2) + 1e-9
            ss_xdx = np.sum((x_prev - x_m) * (dx - dx_m))

            a = float(ss_xdx / ss_xx)
            b = float(dx_m - a * x_m)

            if a < -1e-6:
                self.kappa = float(np.clip(-a * 50.0, 0.05, 10.0))
                self.theta = float(-b / a)
                residuals = dx - (a * x_prev + b)
                self.sigma = float(max(1e-6, np.std(residuals)))
            else:
                self.theta = float(x_m)
                self.kappa = 0.1
                self.sigma = float(max(1e-6, np.std(dx)))

        stationary_std = self.sigma / (math.sqrt(2.0 * max(0.01, self.kappa)) + 1e-9)
        self.ou_divergence_z = float(np.clip((price - self.theta) / (stationary_std + 1e-9), -5.0, 5.0))
        return self.ou_divergence_z


class PerpetualFundingOracle:
    """
    Perpetual Funding Rate Dislocation and Short/Long Squeeze Vectorizer.
    """
    def __init__(self):
        self.funding_rate = 0.0
        self.history = deque(maxlen=200)

    def update(self, rate: float):
        self.funding_rate = rate
        self.history.append(rate)

    def get_squeeze_vector(self) -> Tuple[float, float]:
        bias = -float(np.tanh(self.funding_rate * 5000.0))
        if len(self.history) < 10:
            return bias, 0.0
        arr = np.array(self.history)
        z_score = (self.funding_rate - np.mean(arr)) / (np.std(arr) + 1e-9)
        squeeze_risk = float(np.clip(abs(z_score) / 3.0, 0.0, 1.0))
        return bias, squeeze_risk


class EcosystemPropagator:
    """
    Cross-Asset Lead-Lag Flow Filter: Decays parent network MLOFI (BTC/ETH) 
    using power-law kernel weights to predict altcoin momentum propagation.
    """
    def __init__(self, memory_horizon: int = 40, gamma_decay: float = 0.55):
        self.horizon = memory_horizon
        self.parent_ofi_history = deque(maxlen=memory_horizon)
        lags = np.arange(1, memory_horizon + 1, dtype=np.float64)
        self.weights = lags ** (-gamma_decay)
        self.weights /= np.sum(self.weights)

    def update(self, parent_mlofi_z: float) -> float:
        self.parent_ofi_history.append(parent_mlofi_z)
        if len(self.parent_ofi_history) < 5:
            return 0.0
        n = len(self.parent_ofi_history)
        w = self.weights[-n:]
        arr = np.array(self.parent_ofi_history)
        return float(np.clip(np.dot(w, arr) / (np.sum(w) + 1e-9), -5.0, 5.0))


class QuantumMarkovRegimeDetector:
    """
    4-State Hidden Markov Model (HMM) using Bayesian Online Transition Likelihoods.
    Regimes: 0=Trend, 1=Range, 2=Spoof/Dislocation, 3=Liquidation Cascade.
    """
    def __init__(self):
        self.beliefs = np.array([0.25, 0.25, 0.25, 0.25], dtype=np.float64)
        self.tpm = np.array([
            [0.94, 0.02, 0.02, 0.02],
            [0.02, 0.94, 0.02, 0.02],
            [0.05, 0.05, 0.85, 0.05],
            [0.05, 0.05, 0.05, 0.85]
        ], dtype=np.float64)

    def update_beliefs(self, er: float, entropy: float, fleeting: float, jump_z: float) -> np.ndarray:
        prior = self.tpm.T @ self.beliefs

        l_trend = math.exp(-2.0 * ((1.0 - er) ** 2) - 1.5 * (entropy ** 2) - 3.0 * (fleeting ** 2))
        l_range = math.exp(-2.5 * (er ** 2) - 1.5 * ((1.0 - entropy) ** 2) - 2.0 * (fleeting ** 2))
        l_spoof = math.exp(-3.0 * ((1.0 - fleeting) ** 2) - 1.0 * (er ** 2))
        l_cascade = math.exp(-1.0 * ((3.0 - min(3.0, abs(jump_z))) ** 2))

        likelihoods = np.array([l_trend, l_range, l_spoof, l_cascade], dtype=np.float64) + 1e-6
        unnormalized = prior * likelihoods
        self.beliefs = unnormalized / (np.sum(unnormalized) + 1e-9)
        return self.beliefs


class InformationGeometricRLS:
    """
    L1-Regularized Riemannian Recursive Least Squares with Exact Sherman-Morrison
    Joseph-Stabilized Covariance Updates, O(d) Diagonal Floor & Amortized Spectral Clamping.
    """
    def __init__(self, dim: int, p_init: float = 1.0, l1_penalty: float = 1e-4):
        self.dim = dim
        self.w = np.random.normal(0, 0.01, dim).astype(np.float64)
        self.f_inv = np.eye(dim, dtype=np.float64) * p_init
        self.eye = np.eye(dim, dtype=np.float64)
        self.l1_penalty = l1_penalty
        self.lambda_reg = 0.9995
        self._update_counter = 0

    def update(self, x: np.ndarray, y_target: float, p_pred: float, weight: float = 1.0) -> float:
        err = float(y_target - p_pred)
        x_vec = x.reshape(-1, 1)

        # Bounded Fisher Variance: p(1 - p) clamped safely away from singular endpoints
        p_clamped = float(np.clip(p_pred, 0.01, 0.99))
        fisher_var = max(1e-4, p_clamped * (1.0 - p_clamped))

        # Woodbury Gain Projection
        fx = self.f_inv @ x_vec
        denom = self.lambda_reg + float(x_vec.T @ fx) * fisher_var
        if denom < 1e-9:
            return err

        kalman_gain = (fx * fisher_var) / denom

        # Riemannian Natural Gradient step with L1 Proximal Soft-Thresholding
        w_temp = self.w + (kalman_gain.flatten() * err * weight)
        self.w = np.sign(w_temp) * np.maximum(np.abs(w_temp) - self.l1_penalty, 0.0)

        # Exact Joseph Stabilized Covariance Form: (I - K x^T) F^-1 (I - K x^T)^T + K R K^T
        i_kx = self.eye - (kalman_gain @ x_vec.T)
        bounded_r = min(1000.0, 1.0 / fisher_var)
        noise_cov = (kalman_gain @ kalman_gain.T) * bounded_r
        self.f_inv = (i_kx @ self.f_inv @ i_kx.T + noise_cov) / self.lambda_reg
        self.f_inv = 0.5 * (self.f_inv + self.f_inv.T)

        # 1. Per-tick O(d) Diagonal Floor & Trace Ceiling
        np.fill_diagonal(self.f_inv, np.maximum(np.diag(self.f_inv), 1e-5))
        tr = float(np.trace(self.f_inv))
        if tr > 1500.0:
            self.f_inv *= (1500.0 / tr)

        # 2. Amortized O(d^3) Spectral Projection (Every 500 ticks off hot path)
        self._update_counter += 1
        if self._update_counter % 500 == 0:
            try:
                eigvals, eigvecs = np.linalg.eigh(self.f_inv)
                if eigvals.min() < 1e-5 or eigvals.max() > 1e5:
                    eigvals = np.clip(eigvals, 1e-5, 1e5)
                    self.f_inv = eigvecs @ np.diag(eigvals) @ eigvecs.T
                    self.f_inv = 0.5 * (self.f_inv + self.f_inv.T)
            except np.linalg.LinAlgError:
                self.f_inv = np.eye(self.dim, dtype=np.float64) * 0.1

        # Bound weight norm to prevent runaway logits
        w_norm = float(np.linalg.norm(self.w))
        if w_norm > 50.0:
            self.w *= (50.0 / w_norm)

        return err


class BoundedAdaptiveWhitener:
    """
    Streaming 19D Regularized Whitening Engine.
    Uses dynamic Tikhonov loading and spectral fallbacks to guarantee invertibility.
    """
    def __init__(self, dim: int = 19, base_alpha: float = 0.001):
        self.dim = dim
        self.base_alpha = base_alpha
        self.mean_vector = np.zeros(dim, dtype=np.float64)
        self.cov_matrix = np.eye(dim, dtype=np.float64) * 0.1
        self.eye = np.eye(dim, dtype=np.float64)
        self.baseline_var = 1e-6
        self._whitened_buf = np.zeros(dim, dtype=np.float64)

    def get_adaptive_alpha(self, inst_variance: float) -> float:
        self.baseline_var = 0.99 * self.baseline_var + 0.01 * max(1e-9, inst_variance)
        norm_v = (inst_variance - self.baseline_var) / (self.baseline_var + 1e-9)
        return float(np.clip(self.base_alpha * (1.0 + np.tanh(norm_v)), 0.0005, 0.008))

    def orthogonalize(self, raw_vec: np.ndarray, inst_variance: float) -> np.ndarray:
        alpha = self.get_adaptive_alpha(inst_variance)
        delta = raw_vec - self.mean_vector
        self.mean_vector += alpha * delta

        self.cov_matrix = (1.0 - alpha) * self.cov_matrix + alpha * np.outer(delta, delta)
        self.cov_matrix = 0.5 * (self.cov_matrix + self.cov_matrix.T)

        # Adaptive Tikhonov diagonal regularization
        tr = np.trace(self.cov_matrix)
        reg = max(1e-5, (tr / self.dim) * 1e-4)
        stable_cov = self.cov_matrix + (self.eye * reg)

        try:
            l = np.linalg.cholesky(stable_cov)
            self._whitened_buf[:] = np.clip(np.linalg.solve(l, delta) / 3.0, -3.0, 3.0)
            return self._whitened_buf
        except np.linalg.LinAlgError:
            try:
                evals, evecs = np.linalg.eigh(stable_cov)
                evals_clamped = np.maximum(evals, 1e-6)
                inv_sqrt = 1.0 / np.sqrt(evals_clamped)
                whitened = (evecs @ np.diag(inv_sqrt) @ evecs.T) @ delta
                self._whitened_buf[:] = np.clip(whitened / 3.0, -3.0, 3.0)
                return self._whitened_buf
            except Exception:
                diag_stds = np.sqrt(np.maximum(1e-8, np.diag(stable_cov)))
                self._whitened_buf[:] = np.clip(delta / (diag_stds * 3.0), -3.0, 3.0)
                return self._whitened_buf


def compute_permutation_entropy(series: list, order: int = 3, delay: int = 1) -> float:
    """
    Permutation Shannon Entropy with Gaussian Micro-Dither to break identical price ties.
    """
    if len(series) < (order * delay):
        return 1.0
    try:
        arr = np.asarray(series, dtype=np.float64)
        tie_breaker = np.sin(np.arange(len(arr))) * 1e-11
        arr_jittered = arr + tie_breaker

        shape = (arr_jittered.size - (order - 1) * delay, order)
        strides = (arr_jittered.strides[0], arr_jittered.strides[0] * delay)
        sub_vectors = np.lib.stride_tricks.as_strided(arr_jittered, shape=shape, strides=strides)
        perms = np.argsort(sub_vectors, axis=1)

        bases = order ** np.arange(order)
        hashed = np.sum(perms * bases, axis=1)
        _, counts = np.unique(hashed, return_counts=True)
        p = counts / counts.sum()
        p = p[p > 0]

        entropy = -np.sum(p * np.log2(p))
        max_entropy = math.log2(math.factorial(order))
        return float(np.clip(entropy / max_entropy, 0.0, 1.0))
    except Exception:
        return 1.0


class ContinuousMicrostructureEngine:
    """
    V39.1 APEX TITAN: ZERO-ALLOCATION STATISTICAL MASTER ENGINE
    """
    def __init__(self, symbol: str = "GENERIC", memory_depth: int = 1000):
        self.symbol = symbol

        self.raw_dim = 19
        self.feature_dim = 25

        # Pre-allocated zero-allocation contiguous buffers
        self._raw_vec = np.zeros(self.raw_dim, dtype=np.float64)
        self._volterra_vec = np.zeros(self.feature_dim, dtype=np.float64)
        self._v_att = np.zeros(self.feature_dim, dtype=np.float64)

        self.info_clock = InformationTimeClock()
        self.anti_spoof_kernel = AdversarialSpoofingKernel()
        self.ou_kernel = OUMicroReversionKernel()
        self.kinetic_tensor = KineticAbsorptionTensor()
        self.cvd_engine = CumulativeVolumeDeltaEngine()
        self.funding_oracle = PerpetualFundingOracle()
        self.regime_detector = QuantumMarkovRegimeDetector()

        self.bocd = AdamsMacKayBOCD()
        self.obizhaeva_wang_sentry = ObizhaevaWangExecutionSentry()
        
        # Conservative Bayesian Prior Anchors: 50% Win Rate, 1.05 Payoff, Weight=5.0
        self.jump_kelly_sizer = MertonJumpKellySizer(prior_win_rate=0.50, prior_payoff=1.05, prior_weight=5.0)
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
        self.micro_dislocation_z = 0.0
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
        
        # Production Online Learning Gate
        self.freeze_rls = os.getenv("FREEZE_RLS_WEIGHTS", "true").lower() == "true"

    def update_funding_metrics(self, funding_rate: float):
        self.funding_oracle.update(funding_rate)

    def update_orderbook_pressure(self, bids: list, asks: list):
        if not bids or not asks:
            return

        now = time.time()
        best_bid, bid_vol = float(bids[0][0]), float(bids[0][1])
        best_ask, ask_vol = float(asks[0][0]), float(asks[0][1])
        spread = max(1e-8, best_ask - best_bid)
        spread_bps = (spread / (best_bid + 1e-9)) * 10000.0

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

        denom = math.sqrt(deep_bid_vol ** 2 + deep_ask_vol ** 2 + 1e-9)
        ratio = float(np.clip((deep_bid_vol - deep_ask_vol) / denom, -0.9999, 0.9999))
        self.p_bid_deplete = (1.0 / math.pi) * math.acos(ratio)

        # Stoikov Micro-Price with Non-Linear Asymmetry
        self.true_micro_price = mid + (spread * (imb - 0.5) * (1.0 + abs(imb - 0.5)))
        self.micro_dislocation_z = float(np.clip((self.true_micro_price - mid) / (spread + 1e-9) * 2.0, -5.0, 5.0))

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
        return self.obizhaeva_wang_sentry.evaluate_trajectory(is_buy, spread_bps, self.rough_vol, self.marked_hawkes_z, 1.0)

    def extract_statistical_state(
        self, current_price: float, log_mlofi_z: float, hawkes_z: float,
        sector_impulse: float, sl_dist_pct: float, tp_dist_pct: float,
        exchange_timestamp: float, parent_mlofi_z: float = 0.0
    ) -> Dict[str, Any]:
        now = time.time()

        # Update Bayesian Regime Detector
        regime_weights = self.regime_detector.update_beliefs(
            self.kaufman_er,
            self.shannon_entropy,
            getattr(self, 'fleeting_ratio', 0.0),
            self.jump_z
        )
        p_t, p_r, p_s, p_c = regime_weights

        funding_bias, squeeze_risk = self.funding_oracle.get_squeeze_vector()
        ecosystem_alpha = self.ecosystem_propagator.update(parent_mlofi_z)

        # In-Place 19D Raw Microstructure State Vector (Zero Allocations)
        self._raw_vec[0] = log_mlofi_z
        self._raw_vec[1] = self.marked_hawkes_z
        self._raw_vec[2] = self.meso_momentum_z
        self._raw_vec[3] = sector_impulse
        self._raw_vec[4] = self.micro_elasticity_z
        self._raw_vec[5] = self.ou_divergence_z
        self._raw_vec[6] = getattr(self, 'cfi_z', 0.0)
        self._raw_vec[7] = self.jump_z
        self._raw_vec[8] = self.shannon_entropy
        self._raw_vec[9] = self.swd_z
        self._raw_vec[10] = self.accel_z
        self._raw_vec[11] = self.hurst_h - 0.5
        self._raw_vec[12] = self.p_bid_deplete
        self._raw_vec[13] = self.cvd_z
        self._raw_vec[14] = self.div_z
        self._raw_vec[15] = ecosystem_alpha
        self._raw_vec[16] = funding_bias
        self._raw_vec[17] = squeeze_risk
        self._raw_vec[18] = self.micro_dislocation_z

        for i in range(self.raw_dim):
            self.async_aligner.update(i, self._raw_vec[i], now)

        aligned_raw_vec = self.async_aligner.get_aligned_vector(now)
        f = self.whitening_engine.orthogonalize(aligned_raw_vec, self.inst_variance)

        # In-Place 25D Non-Linear Volterra Feature Expansion (Zero Allocations)
        self._volterra_vec[:19] = f
        self._volterra_vec[19] = f[11] * f[1]  # 19: Hurst x Hawkes interaction
        self._volterra_vec[20] = f[17] * f[0]  # 20: Squeeze Risk x MLOFI
        self._volterra_vec[21] = f[15] * f[0]  # 21: Macro Spillover x MLOFI
        self._volterra_vec[22] = f[14] * f[2]  # 22: CVD Divergence x Meso Momentum
        self._volterra_vec[23] = f[5] * f[1]   # 23: OU Mean Reversion x Hawkes
        self._volterra_vec[24] = 1.0          # 24: Affine Bias Intercept

        # Uniform 25D Hypersphere Projection:
        # Normalizes the full vector across all 25 dimensions uniformly, eradicating
        # hyper-cylinder distortion where dynamic feature collapse causes intercept dominance.
        full_norm = math.sqrt(float(np.dot(self._volterra_vec, self._volterra_vec))) + 1e-9
        self._v_att[:] = self._volterra_vec / full_norm

        l_t = float(np.dot(self.rls_trend.w, self._v_att))
        l_r = float(np.dot(self.rls_range.w, self._v_att))
        l_s = float(np.dot(self.rls_spoof.w, self._v_att))
        l_c = float(np.dot(self.rls_cascade.w, self._v_att))

        logit = float(np.clip((p_t * l_t) + (p_r * l_r) + (p_s * l_s) + (p_c * l_c), -5.0, 5.0))
        p_up = 1.0 / (1.0 + math.exp(-logit))

        execution_style = "MAKER_ONLY" if self.hurst_h < 0.52 else "FLASH_IOC"
        action_dir = "BUY" if p_up > 0.5 else "SELL"
        prob = max(p_up, 1.0 - p_up)
        self.historical_probs.append(prob)

        # Split-Conformal Prediction Coverage Gate (85% Coverage)
        if len(self.calibration_errors) >= 30:
            q_threshold = float(np.percentile(self.calibration_errors, 85))
        else:
            q_threshold = 0.08

        conformal_floor = float(np.clip(0.51 + (q_threshold * 0.25), 0.52, 0.65))
        kelly_target = self.jump_kelly_sizer.compute(self.inst_variance, self.marked_hawkes_z)

        virt_sl = current_price * (1.0 - sl_dist_pct) if action_dir == "BUY" else current_price * (1.0 + sl_dist_pct)
        virt_tp = current_price * (1.0 + tp_dist_pct) if action_dir == "BUY" else current_price * (1.0 - tp_dist_pct)
        dominant_regime = "TRENDING" if p_t > 0.5 else "RANGING"

        return {
            "p_up": p_up,
            "p_down": 1.0 - p_up,
            "action_dir": action_dir,
            "execution_style": execution_style,
            "kelly_fraction": kelly_target,
            "dynamic_gate": conformal_floor,
            "virtual_sl": virt_sl,
            "virtual_tp": virt_tp,
            "markov_beliefs": {"trend": float(p_t), "range": float(p_r), "disloc": float(p_s), "cascade": float(p_c)},
            "dominant_regime": dominant_regime,
            "hurst_h": self.hurst_h,
            "bocd_cp_prob": self.changepoint_prob,
            "raw_features": self._v_att.copy()
        }

    def resolve_trade_outcome(self, signal_id: str, net_pnl: float, allocated_notional: float = 21.0):
        if signal_id not in self.pending_trade_outcomes:
            return

        ctx = self.pending_trade_outcomes.pop(signal_id)
        action_dir = ctx["action"]
        feats = ctx["features"]
        old_p = ctx["p_up"]
        beliefs = ctx["beliefs"]

        is_win = net_pnl > 0.0
        y_up = 1.0 if (action_dir == "BUY" and is_win) or (action_dir == "SELL" and not is_win) else 0.0

        non_conformity = abs(y_up - old_p)
        self.calibration_errors.append(non_conformity)

        # Capital-weighted percentage return
        true_return_pct = net_pnl / max(allocated_notional, 1.0)
        self.jump_kelly_sizer.update(net_pnl, true_return_pct)

        # Online RLS Weight Governance:
        # Prevents parameter degradation and catastrophic forgetting on high-frequency live noise.
        # Weights remain frozen unless explicitly commanded by configuration.
        if not self.freeze_rls:
            self.rls_trend.update(feats, y_up, old_p, weight=beliefs[0])
            self.rls_range.update(feats, y_up, old_p, weight=beliefs[1])
            self.rls_spoof.update(feats, y_up, old_p, weight=beliefs[2])
            self.rls_cascade.update(feats, y_up, old_p, weight=beliefs[3])

            self.rls_updates += 1
            if self.rls_updates % 25 == 0:
                logger.info(
                    f"[X-RAY] RLS Weights Health Check (Trend Norm): {np.linalg.norm(self.rls_trend.w):.4f} | "
                    f"Kelly Win Rate: {self.jump_kelly_sizer.win_rate:.1%} | "
                    f"Payoff (B): {self.jump_kelly_sizer.avg_win / max(1e-6, self.jump_kelly_sizer.avg_loss):.2f}"
                )
            logger.debug(f"[X-RAY] Riemannian FIM Weights Updated | PnL: {net_pnl:.4f} | Hurst: {self.hurst_h:.2f}")
        else:
            logger.debug(f"[X-RAY] Online RLS Frozen: Evaluating out-of-sample without parameter drift.")