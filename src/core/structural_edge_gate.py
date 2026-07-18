import numpy as np
from collections import deque
import math
import logging

logger = logging.getLogger("QUANT_CORE.EDGE_GATE")

class MicrostructureEdgeGate:
    def __init__(self, window_size=100):
        """
        🚀 APEX MICROSTRUCTURE EDGE GATE
        Calculates Kyle's Lambda, Order Flow Imbalance (OFI), and Roll's Spread
        to replace AI heuristics with deterministic market physics.
        """
        self.window_size = window_size
        self.prices = deque(maxlen=window_size)
        self.ofis = deque(maxlen=window_size)
        self.lambda_history = deque(maxlen=window_size)
        
        self.prev_bid_price = 0.0
        self.prev_bid_size = 0.0
        self.prev_ask_price = 0.0
        self.prev_ask_size = 0.0

    def update_orderbook_state(self, best_bid: float, bid_size: float, best_ask: float, ask_size: float, mid_price: float):
        """
        Ingests Level 1 Order Book ticks to update OFI and Price structures instantly.
        """
        if self.prev_bid_price == 0.0:
            self.prev_bid_price, self.prev_bid_size = best_bid, bid_size
            self.prev_ask_price, self.prev_ask_size = best_ask, ask_size
            self.prices.append(mid_price)
            self.ofis.append(0.0)
            return

        # 1. Calculate Order Flow Imbalance (OFI)
        delta_bid_size = 0.0
        if best_bid > self.prev_bid_price:
            delta_bid_size = bid_size
        elif best_bid == self.prev_bid_price:
            delta_bid_size = bid_size - self.prev_bid_size
        else:
            delta_bid_size = -self.prev_bid_size

        delta_ask_size = 0.0
        if best_ask < self.prev_ask_price:
            delta_ask_size = ask_size
        elif best_ask == self.prev_ask_price:
            delta_ask_size = ask_size - self.prev_ask_size
        else:
            delta_ask_size = -self.prev_ask_size

        ofi_t = delta_bid_size - delta_ask_size

        self.ofis.append(ofi_t)
        self.prices.append(mid_price)

        self.prev_bid_price, self.prev_bid_size = best_bid, bid_size
        self.prev_ask_price, self.prev_ask_size = best_ask, ask_size
        
        # Periodically compute and store Lambda to establish a dynamic baseline
        if len(self.prices) >= 20 and len(self.prices) % 10 == 0:
            lmbda = self._calculate_instantaneous_lambda()
            if lmbda > 0:
                self.lambda_history.append(lmbda)

    def _calculate_instantaneous_lambda(self) -> float:
        """
        Calculates Kyle's Lambda via OLS regression: ΔP = λ * OFI + ε
        """
        p_array = np.array(self.prices)
        dp = np.diff(p_array)
        ofi_array = np.array(self.ofis)[1:] 
        
        if np.std(ofi_array) == 0:
            return 0.0
            
        variance = np.var(ofi_array)
        if variance == 0: 
            return 0.0
            
        covariance = np.cov(ofi_array, dp)[0][1]
        return max(0.0, covariance / variance)

    def compute_roll_spread(self) -> float:
        """
        Computes Roll's Implicit Spread Measure to identify retail chop.
        """
        if len(self.prices) < 10: return 0.0
        p_array = np.array(self.prices)
        dp = np.diff(p_array)
        if len(dp) < 3: return 0.0
        
        cov = np.cov(dp[1:], dp[:-1])[0][1]
        
        # Positive covariance implies a trend; the bounce spread model is invalid
        if cov >= 0: return 0.0 
        return 2.0 * math.sqrt(-cov)

    def evaluate_structural_edge(self, symbol: str, vpin_z: float) -> dict:
        """
        THE EDGE-GATE: Replaces the AI Debate Matrix. 
        Returns a deterministic execution verdict in microseconds.
        """
        if len(self.ofis) < 20 or len(self.lambda_history) < 5:
            return {"action": "HOLD", "confidence": 0.0, "reasoning": "CALIBRATING_MICROSTRUCTURE"}

        current_ofi = np.mean(list(self.ofis)[-5:])
        ofi_std = np.std(self.ofis)
        
        # If order flow is essentially flat, there is no edge.
        if ofi_std == 0 or abs(current_ofi) < (ofi_std * 0.5):
            return {"action": "HOLD", "confidence": 0.0, "reasoning": "OFI_FLAT"}

        direction = "BUY" if current_ofi > 0 else "SELL"
        
        current_lambda = self._calculate_instantaneous_lambda()
        baseline_lambda = np.mean(self.lambda_history)
        
        roll_spread = self.compute_roll_spread()
        
        # 1. 🧊 WHALE ABSORPTION (ICEBERG TRAP)
        # OFI is surging, but Kyle's Lambda has collapsed below 50% of the norm.
        # Millions are hitting the book, but the price is inelastic. 
        if abs(current_ofi) > (ofi_std * 1.5) and current_lambda < (baseline_lambda * 0.5):
            logger.warning(f"🧊 ICEBERG WALL DETECTED // {symbol} | OFI Surge absorbed by limit liquidity. Lambda Collapsed: {current_lambda:.8f}")
            return {
                "action": "HOLD", 
                "confidence": 0.0, 
                "reasoning": f"ICEBERG_ABSORPTION_TRAP | OFI_Z: {abs(current_ofi)/max(1e-9, ofi_std):.2f}, Lambda Drop: {current_lambda/max(1e-9, baseline_lambda):.2%}"
            }

        # 2. 📉 RETAIL NOISE BOUNCE
        # Spread is bouncing back and forth (Roll's Spread > 0, Lambda is low).
        if roll_spread > 0 and current_lambda < baseline_lambda:
            return {
                "action": "HOLD",
                "confidence": 0.0,
                "reasoning": f"RETAIL_SPREAD_BOUNCE | Roll Spread: {roll_spread:.6f}"
            }

        # 3. 🚀 TOXIC INSTITUTIONAL BREAKOUT (EXECUTION TRIGGER)
        # OFI matches VPIN, and Kyle's Lambda is expanding (Price is highly elastic to flow).
        if abs(vpin_z) >= 2.0 and current_lambda >= baseline_lambda:
            # Mathematical confidence scales with Lambda expansion and VPIN severity
            lambda_expansion = min(1.5, current_lambda / max(baseline_lambda, 1e-9))
            confidence = min(0.99, 0.50 + (lambda_expansion * 0.20) + (abs(vpin_z) * 0.05))
            
            return {
                "action": direction,
                "confidence": confidence,
                "reasoning": f"STRUCTURAL_BREAKOUT | Elasticity: {lambda_expansion:.2f}x, OFI confirms {direction}"
            }

        return {"action": "HOLD", "confidence": 0.0, "reasoning": "EDGE_GATE_UNDECIDED"}