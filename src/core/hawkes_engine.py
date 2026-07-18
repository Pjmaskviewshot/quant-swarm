import time
import math
import numpy as np
import logging
from collections import deque

logger = logging.getLogger("QUANT_CORE.HAWKES")

class BivariateHawkesEngine:
    def __init__(self, calibration_window: int = 1000):
        """
        🚀 THE APEX UPGRADE: Self-Calibrating Bivariate Hawkes Process
        Node 0: Aggressive BUY trades
        Node 1: Aggressive SELL trades
        """
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
        
        # 🚀 ONLINE ESTIMATION BUFFER
        self.calibration_window = calibration_window
        self.dt_buffer = deque(maxlen=calibration_window)
        self.tick_count = 0

    def _calibrate_engine(self):
        """
        Calculates the Coefficient of Variation (CV) of the trade stream.
        Dynamically adjusts the matrix to account for shifting market regimes.
        """
        if len(self.dt_buffer) < self.calibration_window: 
            return
            
        dts = np.array(self.dt_buffer)
        mean_dt = np.mean(dts)
        std_dt = np.std(dts)
        
        if mean_dt == 0: return
        
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
        
        logger.info(f"⚙️ HAWKES MLE CALIBRATED | CV: {cv:.2f} | Implied Excitation (ρ): {implied_rho:.2f} | Sensitivity Matrix Scaled: {scale_factor:.2f}x")

    def apply_tick(self, timestamp: float, is_buy: bool, trade_volume: float) -> tuple[float, float]:
        """
        Processes a single websocket trade tick in O(1) constant time.
        """
        dt = timestamp - self.last_update_time
        if dt < 0: dt = 0.001 
        
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
        volume_mark = math.log1p(trade_volume) 
        
        self.I[:, event_idx] += self.alpha[:, event_idx] * volume_mark
        self.last_update_time = timestamp
        
        # 3. Calculate Instantaneous Intensity: λ(t) = μ + Σ I(t)
        lambda_buy, lambda_sell = self.mu + np.sum(self.I, axis=1)
        
        return lambda_buy, lambda_sell

    def calculate_imbalance_delta(self) -> float:
        """
        Calculates the normalized probability imbalance between buy and sell cascades.
        Returns a value between -1.0 (pure sell cascade) and 1.0 (pure buy cascade).
        """
        lambda_buy, lambda_sell = self.mu + np.sum(self.I, axis=1)
        total_intensity = lambda_buy + lambda_sell
        
        if total_intensity == 0:
            return 0.0
            
        return (lambda_buy - lambda_sell) / total_intensity