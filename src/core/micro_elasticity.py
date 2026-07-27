import math
import time
import numpy as np
import logging
from collections import deque
from typing import Dict, Any, Tuple, Optional

logger = logging.getLogger("QUANT_CORE.MICRO_ELASTICITY")

class MicroElasticityEngine:
    """
    🔬 V37.0 APEX: MICRO-PRICE ELASTICITY & ANTI-ADVERSE SELECTION ENGINE
    Computes sub-second Orderbook Elasticity (\lambda_OB) and Micro-Price Variance.
    Provides real-time toxicity cancellation triggers for Maker Pegging.
    """
    def __init__(self, window_ticks: int = 100):
        self.window_ticks = window_ticks
        
        self.micro_prices = deque(maxlen=window_ticks)
        self.log_returns = deque(maxlen=window_ticks)
        self.timestamps = deque(maxlen=window_ticks)
        
        # Depth Depletion Tracking
        self.prev_top_bid_qty = 0.0
        self.prev_top_ask_qty = 0.0
        self.last_update_time = 0.0
        
        # Rolling Elasticity & Variance Metrics
        self.instant_variance = 1e-8
        self.orderbook_elasticity = 1.0

    def update_depth_state(self, best_bid: float, bid_qty: float, best_ask: float, ask_qty: float, ofi_fast_z: float, timestamp: float) -> dict:
        """
        Calculates Micro-Price, Real-Time Variance, and Orderbook Elasticity on every L2 tick.
        """
        dt = max(0.001, timestamp - self.last_update_time) if self.last_update_time > 0 else 0.1
        self.last_update_time = timestamp
        
        # 1. Compute Weighted Micro-Price
        total_qty = bid_qty + ask_qty + 1e-9
        micro_price = (best_bid * ask_qty + best_ask * bid_qty) / total_qty
        
        if len(self.micro_prices) > 0 and micro_price > 0 and self.micro_prices[-1] > 0:
            log_ret = math.log(micro_price / self.micro_prices[-1])
            self.log_returns.append(log_ret)
            
            if len(self.log_returns) >= 10:
                self.instant_variance = float(np.var(list(self.log_returns)[-20:]) + 1e-9)
                
            # 2. Compute Orderbook Elasticity (\lambda_OB)
            price_delta_bps = abs((micro_price - self.micro_prices[-1]) / self.micro_prices[-1]) * 10000.0
            self.orderbook_elasticity = price_delta_bps / (abs(ofi_fast_z) + 1.0)

        self.micro_prices.append(micro_price)
        self.timestamps.append(timestamp)
        
        # 3. Detect Toxic Depth Depletion Velocity
        bid_depletion_pct = (self.prev_top_bid_qty - bid_qty) / (self.prev_top_bid_qty + 1e-9) if self.prev_top_bid_qty > 0 else 0.0
        ask_depletion_pct = (self.prev_top_ask_qty - ask_qty) / (self.prev_top_ask_qty + 1e-9) if self.prev_top_ask_qty > 0 else 0.0
        
        self.prev_top_bid_qty = bid_qty
        self.prev_top_ask_qty = ask_qty
        
        return {
            "micro_price": micro_price,
            "instant_variance": self.instant_variance,
            "elasticity": self.orderbook_elasticity,
            "bid_depletion_rate": bid_depletion_pct / dt,
            "ask_depletion_rate": ask_depletion_pct / dt
        }

    def compute_dynamic_micro_brackets(self, current_price: float, side: str, risk_multiplier: float = 1.5) -> Tuple[float, float]:
        """
        🚀 V37.0 FIX: Computes SL and TP distances from sub-second Micro-Price Volatility 
        and Elasticity instead of static percentage or 5m ATR.
        """
        # Convert sub-second variance to 1-minute equivalent standard deviation
        vol_sigma = math.sqrt(self.instant_variance) * math.sqrt(60.0)
        
        # Dynamic Multiplier scaled by Orderbook Elasticity
        elasticity_scalar = max(0.8, min(2.5, self.orderbook_elasticity))
        
        # Micro-Price Stop Loss Distance (%)
        sl_dist_pct = max(0.004, min(0.030, vol_sigma * risk_multiplier * elasticity_scalar))
        tp_dist_pct = sl_dist_pct * 2.0  # Maintain 1:2 Minimum Risk-Reward
        
        if side.upper() == "BUY":
            sl_price = current_price * (1.0 - sl_dist_pct)
            tp_price = current_price * (1.0 + tp_dist_pct)
        else:
            sl_price = current_price * (1.0 + sl_dist_pct)
            tp_price = current_price * (1.0 - tp_dist_pct)
            
        return sl_price, tp_price

    def is_adverse_selection_imminent(self, side: str, depth_metrics: dict) -> bool:
        """
        ⚡ ANTI-ADVERSE SELECTION CANCEL TRIGGER
        Evaluates whether an active PostOnly order is about to be toxically filled.
        Returns True if the order book is collapsing toward our order.
        """
        bid_depletion_rate = depth_metrics.get("bid_depletion_rate", 0.0)
        ask_depletion_rate = depth_metrics.get("ask_depletion_rate", 0.0)
        
        if side.upper() == "BUY":
            # Toxic sell orderbook sweep collapsing bid liquidity
            if bid_depletion_rate > 3.0 and self.orderbook_elasticity > 1.8:
                logger.warning("🛡️ ADVERSE SELECTION GUARD // Toxic Bid Sweep Detected! Aborting Limit Peg.")
                return True
        else:
            # Toxic buy orderbook sweep collapsing ask liquidity
            if ask_depletion_rate > 3.0 and self.orderbook_elasticity > 1.8:
                logger.warning("🛡️ ADVERSE SELECTION GUARD // Toxic Ask Sweep Detected! Aborting Limit Peg.")
                return True
                
        return False