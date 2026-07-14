import numpy as np
from collections import deque
from typing import Dict, Any, List
import time

class AdaptiveLiquidityEntropySurface:
    """
    Self-Organizing Liquidity Intelligence Layer.
    Learns optimal liquidity manifolds from trade outcomes, not human intuition.
    """
    def __init__(self, feature_dim: int = 7, lattice_size: int = 5, learning_rate: float = 0.1):
        self.feature_dim = feature_dim
        self.lattice_size = lattice_size
        self.lr = learning_rate
        
        # Initialize competitive neural lattice with small random perturbations
        # Each node: [vol_mult, spread_state, vol_state, tfis, obr, cmv, temporal]
        self.lattice = np.random.randn(lattice_size, lattice_size, feature_dim) * 0.1 + 1.0
        
        # Track outcome history per node for predictive weighting
        self.node_outcomes: Dict[tuple, deque] = {
            (i, j): deque(maxlen=50) for i in range(lattice_size) for j in range(lattice_size)
        }
        
        # Node confidence (how many times it's been activated)
        self.node_activations = np.zeros((lattice_size, lattice_size))
        
        # Winning node cache for hysteresis
        self.last_winners: Dict[str, tuple] = {}
        self.win_streak: Dict[str, int] = {}
        
    def _find_best_matching_unit(self, vector: np.ndarray) -> tuple:
        """
        Finds the lattice node with minimum weighted Euclidean distance.
        Weights features by their predictive power (learned from outcomes).
        """
        # Compute dynamic feature weights from outcome variance
        # Features that predict success/failure get higher weight
        weights = np.ones(self.feature_dim)
        for i in range(self.lattice_size):
            for j in range(self.lattice_size):
                outcomes = list(self.node_outcomes[(i, j)])
                if len(outcomes) >= 5:
                    # High variance in outcomes = this node is discriminating = weight its features
                    feature_idx = np.argmax(np.abs(self.lattice[i, j]))
                    weights[feature_idx] += np.std(outcomes) * 0.5
        
        weights = np.clip(weights, 0.5, 3.0)
        weights /= weights.sum()
        
        # Weighted distance across lattice
        diff = self.lattice - vector.reshape(1, 1, -1)
        weighted_diff = diff * weights.reshape(1, 1, -1)
        distances = np.sum(weighted_diff ** 2, axis=2)
        
        min_idx = np.unravel_index(np.argmin(distances), distances.shape)
        return min_idx
    
    def _update_lattice(self, winner: tuple, vector: np.ndarray, outcome: float = None):
        """
        Kohonen-style competitive learning. Winner moves toward vector.
        Neighbors move partially. Outcome reinforces or punishes.
        """
        i, j = winner
        
        # Learning rate decays with activations (stabilizes over time)
        local_lr = self.lr / (1.0 + 0.01 * self.node_activations[i, j])
        
        # Winner update
        self.lattice[i, j] += local_lr * (vector - self.lattice[i, j])
        
        # Neighbor update (Gaussian neighborhood)
        for ni in range(self.lattice_size):
            for nj in range(self.lattice_size):
                dist = np.sqrt((ni - i)**2 + (nj - j)**2)
                if dist < 2.0 and (ni, nj) != winner:
                    influence = np.exp(-dist**2 / 2.0) * local_lr * 0.3
                    self.lattice[ni, nj] += influence * (vector - self.lattice[ni, nj])
        
        self.node_activations[i, j] += 1
        
        # Outcome reinforcement (if provided: 1.0 = win, -1.0 = loss, 0.0 = neutral)
        if outcome is not None:
            self.node_outcomes[winner].append(outcome)
    
    def classify(self, symbol: str, features: Dict[str, float], current_time: float) -> Dict[str, Any]:
        """
        Classifies liquidity and returns adaptive threshold + confidence.
        """
        vector = np.array([
            max(0.0, features.get("vol_mult", 1.0)),
            max(0.0, features.get("spread_state", 1.0)),
            max(0.0, features.get("volatility_state", 1.0)),
            max(0.0, features.get("tfis", 0.0)),
            max(0.0, features.get("obr", 1.0)),
            max(0.0, features.get("cmv", 1.0)),
            max(0.0, features.get("temporal_anomaly", 1.0)),
        ])
        
        winner = self._find_best_matching_unit(vector)
        
        # Hysteresis: require 3 consecutive wins or overwhelming distance shift
        last_winner = self.last_winners.get(symbol)
        streak = self.win_streak.get(symbol, 0)
        
        if last_winner == winner:
            streak += 1
        else:
            # Check if distance delta justifies instant switch
            old_dist = np.linalg.norm(self.lattice[last_winner] - vector) if last_winner else float('inf')
            new_dist = np.linalg.norm(self.lattice[winner] - vector)
            if new_dist < old_dist * 0.7 or streak >= 3:
                streak = 1
            else:
                winner = last_winner  # Reject transition
                streak = max(0, streak - 1)
        
        self.last_winners[symbol] = winner
        self.win_streak[symbol] = streak
        
        # Compute adaptive threshold from node history
        outcomes = list(self.node_outcomes[winner])
        if len(outcomes) >= 10:
            win_rate = np.mean([1.0 if o > 0 else 0.0 for o in outcomes])
            # If this node historically wins, lower threshold (more aggressive)
            # If this node historically loses, raise threshold (more defensive)
            base_threshold = 0.35 + (1.0 - win_rate) * 1.0
        else:
            base_threshold = 0.65  # Conservative default for unexplored nodes
        
        # Node maturity bonus (explored nodes get tighter bounds)
        maturity = min(1.0, self.node_activations[winner] / 100.0)
        threshold_variance = 0.3 * (1.0 - maturity)
        
        # Volatility-of-outcomes adjustment
        if len(outcomes) >= 5:
            outcome_vol = np.std(outcomes)
            threshold_variance += outcome_vol * 0.2
        
        final_threshold = np.clip(base_threshold, 0.20, 2.0)
        
        # Predictive confidence: how well does this node's history predict?
        predictive_power = 0.0
        if len(outcomes) >= 20:
            recent = list(outcomes)[-20:]
            predictive_power = abs(np.mean(recent) - 0.5) * 2.0  # 0 = random, 1 = perfectly predictive
        
        return {
            "state": f"NODE_{winner[0]}_{winner[1]}",
            "threshold": round(final_threshold, 4),
            "confidence": round(predictive_power, 4),
            "maturity": round(maturity, 4),
            "win_rate": round(np.mean([1.0 if o > 0 else 0.0 for o in outcomes]) if outcomes else 0.5, 4),
            "action": self._derive_action(final_threshold, features, predictive_power)
        }
    
    def _derive_action(self, threshold: float, features: Dict[str, float], confidence: float) -> str:
        """Derives execution action from learned threshold."""
        vol_mult = features.get("vol_mult", 1.0)
        
        if vol_mult < 0.15:
            return "REJECT_CATATONIC"
        elif threshold > 1.5 and confidence > 0.6:
            return "REJECT_UNLESS_OVERWHELMING"
        elif threshold < 0.35 and confidence > 0.5:
            return "AGGRESSIVE_SCALP"
        elif features.get("volatility_state", 1.0) > 2.0 and features.get("spread_state", 1.0) > 2.0:
            return "REJECT_TOXIC"
        else:
            return "STANDARD_EVALUATION"
    
    def feedback(self, symbol: str, outcome: float):
        """
        CRITICAL: Call this after every resolved trade to train the lattice.
        outcome: +1.0 for win, -1.0 for loss, 0.0 for timeout/neutral
        """
        winner = self.last_winners.get(symbol)
        if winner:
            self._update_lattice(winner, self.lattice[winner], outcome)