"""
💎 V50.0 QUANTUM SWARM: MICRO-PRICE ELASTICITY & ADVERSE SELECTION ENGINE
-------------------------------------------------------------------------
Computes sub-second Orderbook Elasticity (λ_OB) and Micro-Price Variance.
Provides real-time toxicity cancellation triggers for the Predatory Maker Grid.
Optimized with NumPy zero-allocation iterators and strict orderbook sanity guards.
"""

import math
import time
import numpy as np
import logging
from collections import deque
from typing import Dict, Any, Tuple, Optional

logger = logging.getLogger("QUANT_CORE.MICRO_ELASTICITY")

class MicroElasticityEngine:
    """
    🚀 V50.0 APEX: MICRO-PRICE ELASTICITY ENGINE
    Maps the "stretchiness" of the orderbook. High elasticity means the book is 
    hollow and vulnerable to toxic sweeps. Low elasticity means it's dense and safe.
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
        Protected against ZeroDivision, crossed books, and high-frequency memory allocation spikes.
        """
        # 🚀 SANITY PRECONDITION: Reject crossed, zero, or invalid orderbook states
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask or bid_qty < 0 or ask_qty < 0:
            return {
                "micro_price": self.micro_prices[-1] if self.micro_prices else 0.0,
                "instant_variance": self.instant_variance,
                "elasticity": self.orderbook_elasticity,
                "bid_depletion_rate": 0.0,
                "ask_depletion_rate": 0.0
            }

        dt = max(0.001, timestamp - self.last_update_time) if self.last_update_time > 0 else 0.1
        self.last_update_time = timestamp
        
        # 1. Compute Weighted Micro-Price
        total_qty = bid_qty + ask_qty + 1e-9
        micro_price = (best_bid * ask_qty + best_ask * bid_qty) / total_qty
        
        if len(self.micro_prices) > 0 and micro_price > 0 and self.micro_prices[-1] > 0:
            try:
                log_ret = math.log(micro_price / self.micro_prices[-1])
                self.log_returns.append(log_ret)
                
                # 🚀 OPTIMIZATION: Use np.fromiter instead of list casting for zero allocation overhead
                if len(self.log_returns) >= 10:
                    log_rets_arr = np.fromiter(self.log_returns, dtype=float, count=len(self.log_returns))
                    self.instant_variance = float(np.var(log_rets_arr[-20:]) + 1e-9)
                
                # 2. Compute Orderbook Elasticity (λ_OB)
                price_delta_bps = abs((micro_price - self.micro_prices[-1]) / self.micro_prices[-1]) * 10000.0
                self.orderbook_elasticity = price_delta_bps / (abs(ofi_fast_z) + 1.0)
            except Exception:
                pass # Swallow rare float anomalies without breaking the ingestion thread

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
        🚀 V50.0 NANO-BRACKETING: 
        Computes SL and TP distances mathematically derived from sub-second Micro-Price 
        Volatility and Elasticity instead of rigid, static percentages.
        """
        vol_sigma = math.sqrt(self.instant_variance) * math.sqrt(60.0)
        elasticity_scalar = max(0.8, min(2.5, self.orderbook_elasticity))
        
        sl_dist_pct = max(0.004, min(0.030, vol_sigma * risk_multiplier * elasticity_scalar))
        tp_dist_pct = sl_dist_pct * 2.0  # Maintain 1:2 Minimum Risk-Reward
        
        if side.upper() == "BUY":
            sl_price = current_price * (1.0 - sl_dist_pct)
            tp_price = current_price * (1.0 + tp_dist_pct)
        else:
            sl_price = current_price * (1.0 + sl_dist_pct)
            tp_price = current_price * (1.0 - tp_dist_pct)
            
        return sl_price, tp_price

    def is_adverse_selection_imminent(self, side: str, depth_metrics: dict, symbol: str = "UNKNOWN") -> bool:
        """
        ⚡ V50.0 ANTI-ADVERSE SELECTION X-RAY GUARD
        Evaluates whether an active PostOnly order is about to be toxically filled.
        Returns True if the order book is collapsing toward our order.
        """
        bid_depletion_rate = depth_metrics.get("bid_depletion_rate", 0.0)
        ask_depletion_rate = depth_metrics.get("ask_depletion_rate", 0.0)
        
        if side.upper() == "BUY":
            if bid_depletion_rate > 3.0 and self.orderbook_elasticity > 1.8:
                logger.warning(f"[X-RAY] 🛡️ ADVERSE SELECTION GUARD // {symbol} Toxic Bid Sweep Detected! Aborting Maker Peg.")
                return True
        else:
            if ask_depletion_rate > 3.0 and self.orderbook_elasticity > 1.8:
                logger.warning(f"[X-RAY] 🛡️ ADVERSE SELECTION GUARD // {symbol} Toxic Ask Sweep Detected! Aborting Maker Peg.")
                return True
                
        return False