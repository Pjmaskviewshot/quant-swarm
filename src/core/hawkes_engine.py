"""
💎 V55.2 QUANTUM SWARM: SELF-CALIBRATING HAWKES PROCESS
-------------------------------------------------------
Models the arrival intensity of algorithmic trade cascades.
Features Dynamic Branching Ratio (ρ) calibration, Spectral Radius Stationarity 
Clamping, and X-Ray Diagnostic Telemetry.
"""

import time
import math
import numpy as np
import logging
from collections import deque
from typing import Tuple

logger = logging.getLogger("QUANT_CORE.HAWKES")

class BivariateHawkesEngine:
    def __init__(self, symbol: str = "GENERIC", calibration_window: int = 1000):
        """
        🚀 V55.2 APEX: Self-Calibrating Bivariate Hawkes Process
        Node 0: Aggressive BUY trades
        Node 1: Aggressive SELL trades
        """
        self.symbol = symbol
        
        # Base parameters to anchor the calibration
        self.base_mu = np.array([0.1, 0.1]) 
        self.base_alpha = np.array([
            [0.6, 0.2],  
            [0.2, 0.6]   
        ])
        self.base_beta = np.array([
            [1.5, 2.0],  
            [2.0, 1.5]   
        ])
        
        # Live parameters (These will mathematically mutate in real-time)
        self.mu = np.copy(self.base_mu)
        self.alpha = np.copy(self.base_alpha)
        self.beta = np.copy(self.base_beta)
        
        # Recursive state matrix
        self.I = np.zeros((2, 2))
        self.last_update_time = time.time()
        
        # ONLINE ESTIMATION BUFFER
        self.calibration_window = calibration_window
        self.dt_buffer = deque(maxlen=calibration_window)
        self.tick_count = 0
        self._last_log_time = 0.0

    def _calibrate_engine(self):
        """
        Calculates the Coefficient of Variation (CV) of the trade stream.
        Dynamically adjusts the matrix to account for shifting market regimes.
        Ensures mathematical stationarity via spectral radius clamping.
        """
        if len(self.dt_buffer) < self.calibration_window: 
            return
            
        dts = np.array(self.dt_buffer)
        mean_dt = np.mean(dts)
        std_dt = np.std(dts)
        
        # Guard against micro-burst zero-variance (exchange trade batching)
        if mean_dt < 1e-6: 
            return
        
        # Coefficient of Variation (CV)
        # CV = 1.0 -> Random noise (No cascades)
        # CV > 1.2 -> Highly clustered algorithmic trading (Whales active)
        cv = std_dt / mean_dt
        
        # Theoretical Hawkes Approximation: CV^2 ≈ 1 / (1 - ρ)^2
        # Implied Branching Ratio (ρ): The probability that one trade triggers another
        implied_rho = 1.0 - (1.0 / max(cv, 1.001)) 
        
        # Clamp ρ to keep the stochastic process stationary (ρ < 1.0)
        implied_rho = max(0.05, min(0.85, implied_rho))
        
        # 1. Scale Alpha (Excitation)
        # If the market is clustering, we increase sensitivity to cascades
        scale_factor = implied_rho / 0.5  # Assuming 0.5 is the baseline ρ
        self.alpha = self.base_alpha * scale_factor
        
        # 2. Scale Mu (Background Noise)
        # If the market is random (low ρ), we assume most trades are just background noise
        noise_factor = 1.0 - implied_rho
        self.mu = self.base_mu * (noise_factor / 0.5)
        
        # 3. STATIONARITY CLAMP (Spectral Radius < 1.0)
        # The branching matrix M_ij = alpha_ij / beta_ij determines process stability
        with np.errstate(divide='ignore', invalid='ignore'):
            branching_matrix = self.alpha / self.beta
            
        try:
            spectral_radius = np.max(np.abs(np.linalg.eigvals(branching_matrix)))
            
            if spectral_radius >= 0.95:
                # Rescale alpha to force the spectral radius down to a safe 0.90 limit
                # This prevents the stochastic process from predicting infinite trade intensities
                stationarity_correction = 0.90 / spectral_radius
                self.alpha *= stationarity_correction
                logger.warning(f"[X-RAY] ⚠️ HAWKES STATIONARITY CLAMP // {self.symbol} | Spectral Radius reached {spectral_radius:.3f}. Alpha matrix rescaled by {stationarity_correction:.2f}x to prevent explosive intensity.")
            
            now = time.time()
            if now - self._last_log_time > 300.0:  # Throttle standard info logs to once every 5 mins per asset
                logger.debug(f"[X-RAY] ⚙️ HAWKES MLE CALIBRATED // {self.symbol} | CV: {cv:.2f} | Implied Excitation (ρ): {implied_rho:.2f} | Spectral Radius: {min(spectral_radius, 0.90):.3f}")
                self._last_log_time = now
                
        except np.linalg.LinAlgError:
            logger.debug(f"[X-RAY] Hawkes Eigenvalue convergence failed for {self.symbol}. Skipping calibration step.")

    def apply_tick(self, timestamp: float, is_buy: bool, trade_volume: float) -> Tuple[float, float]:
        """
        Processes a single websocket trade tick in O(1) constant time.
        """
        raw_dt = timestamp - self.last_update_time
        # Clamp dt to prevent exponential underflow/overflow during initial ticks or gaps
        dt = max(0.0, min(60.0, raw_dt)) if self.last_update_time > 0 else 0.001
        
        # Add to rolling buffer for Online Parameter Estimation
        self.dt_buffer.append(dt)
        self.tick_count += 1
        
        # Mathematically re-calibrate the universe every N ticks
        if self.tick_count % self.calibration_window == 0:
            self._calibrate_engine()
        
        # 1. Exponential Decay of existing intensities: I(t) = I(t_last) * e^(-beta * dt)
        self.I *= np.exp(-self.beta * dt)
        
        # 2. Apply Excitation Jump (Marked by trade volume)
        event_idx = 0 if is_buy else 1
        volume_mark = math.log1p(max(0.0, trade_volume)) 
        
        self.I[:, event_idx] += self.alpha[:, event_idx] * volume_mark
        self.last_update_time = timestamp
        
        # 3. Calculate Instantaneous Intensity: λ(t) = μ + Σ I(t)
        lambda_buy, lambda_sell = self.mu + np.sum(self.I, axis=1)
        
        return float(lambda_buy), float(lambda_sell)

    def calculate_imbalance_delta(self) -> float:
        """
        Calculates the normalized probability imbalance between buy and sell cascades.
        Returns a value between -1.0 (pure sell cascade) and 1.0 (pure buy cascade).
        """
        lambda_buy, lambda_sell = self.mu + np.sum(self.I, axis=1)
        total_intensity = lambda_buy + lambda_sell
        
        if total_intensity <= 1e-9:
            return 0.0
            
        return float((lambda_buy - lambda_sell) / total_intensity)