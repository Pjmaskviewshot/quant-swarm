"""
💎 V22.5 APEX QUANTUM PRIME: ASYMMETRIC BIVARIATE HAWKES ENGINE
-------------------------------------------------------------------------
Models the arrival intensity of algorithmic trade cascades.
Upgraded with Asset-Agnostic Relative Volume Normalization and 
Dynamic Decay Calibration (β) for universal cross-coin execution.

Audit Fixes (V22.5):
- Decoupled Asymmetric Kernel: Replaced the single shared decay matrix with a fully 
  decoupled 4-parameter kernel. This mathematically accounts for the asymmetric 
  half-lives of buy vs. sell cascades inherent to crypto microstructure.
"""

import time
import math
import numpy as np
import logging
from collections import deque
from typing import Tuple

logger = logging.getLogger("QUANT_CORE.HAWKES")

class BivariateHawkesEngine:
    """
    🚀 V22.5 APEX: Asymmetric Self-Calibrating Bivariate Hawkes Process
    Node 0: Aggressive BUY trades
    Node 1: Aggressive SELL trades
    """
    def __init__(self, symbol: str = "GENERIC", calibration_window: int = 1000):
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
        self.last_update_time = 0.0
        
        # 🚀 V22.5 Asymmetric Tracking
        self.last_buy_time = 0.0
        self.last_sell_time = 0.0
        
        # Rolling EWMA for Asset-Agnostic Volume Normalization
        self.vol_ewma = 0.0
        self.vol_alpha = 0.05 
        
        # ONLINE ESTIMATION BUFFERS
        self.calibration_window = calibration_window
        self.dt_buffer_buy = deque(maxlen=calibration_window)
        self.dt_buffer_sell = deque(maxlen=calibration_window)
        
        self.imbalance_history = deque(maxlen=200)
        self.current_imbalance_z = 0.0

        self.tick_count = 0
        self._last_log_time = 0.0

    def _calibrate_engine(self):
        """
        🚀 V22.5 FULLY DECOUPLED ASYMMETRIC CALIBRATION
        Calculates the Coefficient of Variation (CV) of the trade stream independently 
        for buy and sell cascades. Dynamically adjusts Excitation (α) and Decay (β) 
        per dimension to account for shifting, asymmetric micro-regimes.
        """
        if len(self.dt_buffer_buy) < 50 or len(self.dt_buffer_sell) < 50: 
            return
            
        dts_buy = np.array(self.dt_buffer_buy)
        dts_sell = np.array(self.dt_buffer_sell)
        
        mean_buy, std_buy = float(np.mean(dts_buy)), float(np.std(dts_buy))
        mean_sell, std_sell = float(np.mean(dts_sell)), float(np.std(dts_sell))
        
        # Guard against micro-burst zero-variance
        if mean_buy < 1e-6 or mean_sell < 1e-6: 
            return
            
        # 1. DYNAMIC ASYMMETRIC DECAY CALIBRATION (β)
        speed_factor_buy = max(0.5, min(3.0, 1.0 / max(0.1, mean_buy)))
        speed_factor_sell = max(0.5, min(3.0, 1.0 / max(0.1, mean_sell)))
        
        self.beta[:, 0] = self.base_beta[:, 0] * speed_factor_buy
        self.beta[:, 1] = self.base_beta[:, 1] * speed_factor_sell
        
        # 2. ASYMMETRIC EXCITATION SCALING (α) via Implied Branching Ratio
        cv_buy = std_buy / mean_buy
        cv_sell = std_sell / mean_sell
        
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
        
        # 4. STATIONARITY CLAMP (Spectral Radius < 0.95)
        with np.errstate(divide='ignore', invalid='ignore'):
            branching_matrix = self.alpha / self.beta
            
        try:
            spectral_radius = float(np.max(np.abs(np.linalg.eigvals(branching_matrix))))
            
            if spectral_radius >= 0.95:
                # Rescale entire alpha matrix to force the spectral radius down to a safe 0.90 limit
                stationarity_correction = 0.90 / spectral_radius
                self.alpha *= stationarity_correction
                
                logger.warning(
                    f"[X-RAY] ⚠️ HAWKES STATIONARITY CLAMP // {self.symbol} | "
                    f"Spectral Radius reached {spectral_radius:.3f}. Matrix rescaled by {stationarity_correction:.2f}x."
                )
            
            now = time.time()
            if now - self._last_log_time > 300.0:  
                logger.debug(
                    f"[X-RAY] ⚙️ HAWKES ASYMMETRIC MLE // {self.symbol} | "
                    f"ρ_Buy: {implied_rho_buy:.2f} | ρ_Sell: {implied_rho_sell:.2f} | "
                    f"β_Buy: {speed_factor_buy:.2f}x | β_Sell: {speed_factor_sell:.2f}x | "
                    f"Radius: {min(spectral_radius, 0.90):.3f}"
                )
                self._last_log_time = now
                
        except np.linalg.LinAlgError:
            logger.debug(f"[X-RAY] Hawkes Eigenvalue convergence failed for {self.symbol}. Skipping clamp.")

    def apply_tick(self, timestamp: float, is_buy: bool, trade_volume: float) -> Tuple[float, float]:
        """
        Processes a single websocket trade tick in O(1) constant time.
        Incorporates Relative Volume Normalization to standardize across all market caps.
        """
        if self.last_update_time == 0.0:
            global_dt = 0.001
        else:
            global_dt = max(0.001, min(60.0, timestamp - self.last_update_time))
        
        # 🚀 V22.5: Asymmetric tracking for decoupled kernel calibration
        if is_buy:
            if self.last_buy_time > 0.0:
                self.dt_buffer_buy.append(max(0.001, min(60.0, timestamp - self.last_buy_time)))
            self.last_buy_time = timestamp
        else:
            if self.last_sell_time > 0.0:
                self.dt_buffer_sell.append(max(0.001, min(60.0, timestamp - self.last_sell_time)))
            self.last_sell_time = timestamp
            
        self.tick_count += 1
        
        # Asset-Agnostic Volume Normalization
        if self.vol_ewma == 0.0:
            self.vol_ewma = trade_volume
        else:
            self.vol_ewma = (1 - self.vol_alpha) * self.vol_ewma + self.vol_alpha * trade_volume
            
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
        
        self._update_imbalance_metrics(lambda_buy, lambda_sell)
        
        return float(lambda_buy), float(lambda_sell)

    def _update_imbalance_metrics(self, lambda_buy: float, lambda_sell: float):
        """Maintains a rolling Z-Score of the Hawkes Intensity Imbalance for downstream consumption."""
        total_intensity = lambda_buy + lambda_sell
        
        if total_intensity <= 1e-9:
            imb = 0.0
        else:
            imb = float((lambda_buy - lambda_sell) / total_intensity)
            
        self.imbalance_history.append(imb)
        
        if len(self.imbalance_history) >= 20:
            arr = np.array(self.imbalance_history)
            mean = np.mean(arr)
            std = np.std(arr) + 1e-9
            self.current_imbalance_z = float((imb - mean) / std)
        else:
            self.current_imbalance_z = 0.0

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