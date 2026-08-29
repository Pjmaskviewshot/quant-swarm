"""
💎 V25.0 APEX QUANTUM PRIME: O(1) ASYMMETRIC BIVARIATE HAWKES ENGINE
-------------------------------------------------------------------------
Models the arrival intensity of algorithmic trade cascades.
Upgraded with Asset-Agnostic Relative Volume Normalization and 
Dynamic Decay Calibration (β) for universal cross-coin execution.

Architectural Supremacy (V25.0):
- O(1) Algebraic Eigenvalue Solver: Eradicates np.linalg.eigvals. Computes 
  the spectral radius using the exact trace/determinant quadratic formula.
- Welford EWMA State Recursion: Stripped all deques and np.std/np.mean arrays 
  to track inter-arrival variances and imbalance Z-scores in constant time.
"""

import time
import math
import numpy as np
import logging
from typing import Tuple

logger = logging.getLogger("QUANT_CORE.HAWKES")

class BivariateHawkesEngine:
    """
    🚀 V25.0 APEX: Asymmetric Self-Calibrating Bivariate Hawkes Process
    Node 0: Aggressive BUY trades
    Node 1: Aggressive SELL trades
    """
    def __init__(self, symbol: str = "GENERIC", calibration_window: int = 1000):
        self.symbol = symbol
        
        # Base parameters to anchor the calibration
        self.base_mu = np.array([0.1, 0.1], dtype=np.float64) 
        self.base_alpha = np.array([
            [0.6, 0.2],  
            [0.2, 0.6]   
        ], dtype=np.float64)
        self.base_beta = np.array([
            [1.5, 2.0],  
            [2.0, 1.5]   
        ], dtype=np.float64)
        
        # Live parameters (These will mathematically mutate in real-time)
        self.mu = np.copy(self.base_mu)
        self.alpha = np.copy(self.base_alpha)
        self.beta = np.copy(self.base_beta)
        
        # Recursive state matrix
        self.I = np.zeros((2, 2), dtype=np.float64)
        self.last_update_time = 0.0
        
        # 🚀 V25.0 Asymmetric Tracking
        self.last_buy_time = 0.0
        self.last_sell_time = 0.0
        
        # Rolling EWMA for Asset-Agnostic Volume Normalization
        self.vol_ewma = 0.0
        self.vol_alpha = 0.05 
        
        self.calibration_window = calibration_window
        
        # 🚀 V25.0 Pure O(1) Inter-Arrival Variance Tracking (Welford EWMA)
        self.dt_ewma_alpha = 0.02
        self.mean_dt_buy, self.var_dt_buy = 0.0, 1e-9
        self.mean_dt_sell, self.var_dt_sell = 0.0, 1e-9
        
        # 🚀 V25.0 Pure O(1) Imbalance Z-Score Tracking
        self.imb_ewma_alpha = 0.05
        self.mean_imb, self.var_imb = 0.0, 1e-9
        self.current_imbalance_z = 0.0

        self.tick_count = 0
        self._last_log_time = 0.0

    def _update_dt_stats(self, dt: float, is_buy: bool):
        """O(1) Recursive Welford Variance update for trade arrival times."""
        if is_buy:
            delta = dt - self.mean_dt_buy
            self.mean_dt_buy += self.dt_ewma_alpha * delta
            self.var_dt_buy = (1.0 - self.dt_ewma_alpha) * (self.var_dt_buy + self.dt_ewma_alpha * (delta ** 2))
        else:
            delta = dt - self.mean_dt_sell
            self.mean_dt_sell += self.dt_ewma_alpha * delta
            self.var_dt_sell = (1.0 - self.dt_ewma_alpha) * (self.var_dt_sell + self.dt_ewma_alpha * (delta ** 2))

    def _calibrate_engine(self):
        """
        🚀 V25.0 FULLY DECOUPLED ASYMMETRIC CALIBRATION
        Calculates the Coefficient of Variation (CV) of the trade stream independently 
        using O(1) moments. Dynamically adjusts Excitation (α) and Decay (β).
        """
        # Guard against micro-burst zero-variance
        if self.mean_dt_buy < 1e-6 or self.mean_dt_sell < 1e-6: 
            return
            
        std_buy = math.sqrt(max(1e-12, self.var_dt_buy))
        std_sell = math.sqrt(max(1e-12, self.var_dt_sell))
        
        # 1. DYNAMIC ASYMMETRIC DECAY CALIBRATION (β)
        speed_factor_buy = max(0.5, min(3.0, 1.0 / max(0.1, self.mean_dt_buy)))
        speed_factor_sell = max(0.5, min(3.0, 1.0 / max(0.1, self.mean_dt_sell)))
        
        self.beta[:, 0] = self.base_beta[:, 0] * speed_factor_buy
        self.beta[:, 1] = self.base_beta[:, 1] * speed_factor_sell
        
        # 2. ASYMMETRIC EXCITATION SCALING (α) via Implied Branching Ratio
        cv_buy = std_buy / self.mean_dt_buy
        cv_sell = std_sell / self.mean_dt_sell
        
        implied_rho_buy = max(0.05, min(0.85, 1.0 - (1.0 / max(cv_buy, 1.001))))
        implied_rho_sell = max(0.05, min(0.85, 1.0 - (1.0 / max(cv_sell, 1.001))))
        
        scale_buy = implied_rho_buy / 0.5
        scale_sell = implied_rho_sell / 0.5
        
        self.alpha[:, 0] = self.base_alpha[:, 0] * scale_buy
        self.alpha[:, 1] = self.base_alpha[:, 1] * scale_sell
        
        # 3. BACKGROUND NOISE CALIBRATION (μ)
        noise_buy = 1.0 - implied_rho_buy
        noise_sell = 1.0 - implied_rho_sell
        
        self.mu[0] = self.base_mu[0] * (noise_buy / 0.5)
        self.mu[1] = self.base_mu[1] * (noise_sell / 0.5)
        
        # 4. O(1) ALGEBRAIC STATIONARITY CLAMP (Spectral Radius < 0.95)
        b00 = self.alpha[0, 0] / (self.beta[0, 0] + 1e-9)
        b01 = self.alpha[0, 1] / (self.beta[0, 1] + 1e-9)
        b10 = self.alpha[1, 0] / (self.beta[1, 0] + 1e-9)
        b11 = self.alpha[1, 1] / (self.beta[1, 1] + 1e-9)
        
        trace = b00 + b11
        det = (b00 * b11) - (b01 * b10)
        discriminant = trace**2 - 4 * det
        
        if discriminant >= 0:
            eig1 = abs((trace + math.sqrt(discriminant)) / 2.0)
            eig2 = abs((trace - math.sqrt(discriminant)) / 2.0)
            spectral_radius = max(eig1, eig2)
        else:
            # Complex eigenvalues: magnitude is strictly sqrt(det)
            spectral_radius = math.sqrt(abs(det))
            
        if spectral_radius >= 0.95:
            stationarity_correction = 0.90 / (spectral_radius + 1e-9)
            self.alpha *= stationarity_correction
            
            now = time.time()
            if now - self._last_log_time > 300.0:  
                logger.debug(
                    f"[X-RAY] ⚠️ HAWKES STATIONARITY CLAMP // {self.symbol} | "
                    f"Spectral Radius reached {spectral_radius:.3f}. Matrix rescaled by {stationarity_correction:.2f}x."
                )
                self._last_log_time = now

    def apply_tick(self, timestamp: float, is_buy: bool, trade_volume: float) -> Tuple[float, float]:
        """
        Processes a single websocket trade tick in O(1) constant time.
        Incorporates Relative Volume Normalization to standardize across all market caps.
        """
        if self.last_update_time == 0.0:
            global_dt = 0.001
        else:
            global_dt = max(0.001, min(60.0, timestamp - self.last_update_time))
        
        # 🚀 V25.0: Asymmetric tracking with O(1) Welford Moments
        if is_buy:
            if self.last_buy_time > 0.0:
                dt_buy = max(0.001, min(60.0, timestamp - self.last_buy_time))
                self._update_dt_stats(dt_buy, True)
            self.last_buy_time = timestamp
        else:
            if self.last_sell_time > 0.0:
                dt_sell = max(0.001, min(60.0, timestamp - self.last_sell_time))
                self._update_dt_stats(dt_sell, False)
            self.last_sell_time = timestamp
            
        self.tick_count += 1
        
        # Asset-Agnostic Volume Normalization
        if self.vol_ewma == 0.0:
            self.vol_ewma = trade_volume
        else:
            self.vol_ewma = (1.0 - self.vol_alpha) * self.vol_ewma + self.vol_alpha * trade_volume
            
        normalized_volume = trade_volume / (self.vol_ewma + 1e-9)
        
        if self.tick_count % self.calibration_window == 0:
            self._calibrate_engine()
        
        # 1. Exponential Decay of existing intensities: I(t) = I(t_last) * e^(-beta * dt)
        self.I *= np.exp(-self.beta * global_dt)
        
        # 2. Apply Excitation Jump (Marked by Normalized Volume)
        event_idx = 0 if is_buy else 1
        volume_mark = math.log1p(max(0.0, normalized_volume)) 
        
        self.I[:, event_idx] += self.alpha[:, event_idx] * volume_mark
        self.last_update_time = timestamp
        
        # 3. Calculate Instantaneous Intensity: λ(t) = μ + Σ I(t)
        lambda_buy, lambda_sell = self.mu + np.sum(self.I, axis=1)
        
        self._update_imbalance_metrics(float(lambda_buy), float(lambda_sell))
        
        return float(lambda_buy), float(lambda_sell)

    def _update_imbalance_metrics(self, lambda_buy: float, lambda_sell: float):
        """O(1) Rolling Z-Score of the Hawkes Intensity Imbalance."""
        total_intensity = lambda_buy + lambda_sell
        imb = float((lambda_buy - lambda_sell) / total_intensity) if total_intensity > 1e-9 else 0.0
        
        delta = imb - self.mean_imb
        self.mean_imb += self.imb_ewma_alpha * delta
        self.var_imb = (1.0 - self.imb_ewma_alpha) * (self.var_imb + self.imb_ewma_alpha * (delta ** 2))
        
        self.current_imbalance_z = float((imb - self.mean_imb) / (math.sqrt(self.var_imb) + 1e-9))

    def calculate_imbalance_delta(self, current_timestamp: float = None) -> float:
        """
        Returns the raw normalized probability imbalance between buy and sell cascades.
        Returns a value between -1.0 (pure sell cascade) and 1.0 (pure buy cascade).
        """
        if current_timestamp and self.last_update_time > 0:
            dt = max(0.001, min(60.0, current_timestamp - self.last_update_time))
            decayed_I = self.I * np.exp(-self.beta * dt)
        else:
            decayed_I = self.I

        lambda_buy, lambda_sell = self.mu + np.sum(decayed_I, axis=1)
        total_intensity = lambda_buy + lambda_sell
        
        if total_intensity <= 1e-9:
            return 0.0
            
        return float(np.clip((lambda_buy - lambda_sell) / total_intensity, -1.0, 1.0))

    def get_imbalance_z_score(self) -> float:
        """Returns the rolling statistical Z-Score of the Hawkes Intensity Imbalance."""
        return self.current_imbalance_z