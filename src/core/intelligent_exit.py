"""
💎 APEX OMEGA: CALIBRATION-READY ADVERSARIAL STOCHASTIC CONTROL
-----------------------------------------------------------------------
Stateful, Counterfactual Optimal-Stopping Engine.
Features:
- Counterfactual Trajectory Simulator (Monte Carlo CVaR95 / CVaR99).
- Defender vs. Assassin Architecture.
- Genuine OOD Probability via Chi-Square CDF on Mahalanobis Distance.
- Empirical Hazard Rate Function Framework.
- True Execution FSM (Awaits physical exchange fills, no hallucinated state).
"""

import math
import time
import logging
import numpy as np
from scipy.optimize import minimize_scalar
from scipy.stats import chi2
from dataclasses import dataclass, field
from typing import Dict, Any, Tuple, List

logger = logging.getLogger("QUANT_CORE.APEX_OMEGA")

# =====================================================================
# 1. RIGOROUS THESIS & STATE TRACKING
# =====================================================================

@dataclass
class ThesisVector:
    """5-Dimensional representation of the market state [OFI, Hawkes, Momentum, Depth, Volatility]."""
    features: np.ndarray  
    
    def mahalanobis_distance(self, other: 'ThesisVector', inv_cov_matrix: np.ndarray) -> float:
        """
        True statistical distance accounting for feature covariance.
        D_M = sqrt((x - mu)^T * Sigma^-1 * (x - mu))
        """
        delta = self.features - other.features
        try:
            distance_sq = np.dot(np.dot(delta.T, inv_cov_matrix), delta)
            return float(math.sqrt(max(0.0, distance_sq)))
        except Exception:
            return 0.0

@dataclass
class PositionExitState:
    """PERSISTENT state object living in the main FSM."""
    position_id: str
    entry_time: float
    entry_price: float
    exit_side: str
    
    entry_thesis: ThesisVector
    thesis_inv_cov: np.ndarray  # Historical inverse covariance matrix
    
    last_eval_time: float = field(default_factory=time.time)
    
    # Execution FSM
    execution_state: str = "OBSERVE" 
    exec_order_id: str = ""
    exec_post_time: float = 0.0
    target_q: float = 1.0  # Intent tracking (actual q is derived from exchange sync)

@dataclass
class ExitDecision:
    action: str
    target_q: float
    urgency: str
    limit_price: float
    reason: str
    log_output: str


# =====================================================================
# 2. ADVERSARIAL MODELS (ASSASSIN) & REGIME SHIFT
# =====================================================================

class RegimeChangeDetector:
    """Evaluates if the statistical process generating the market has broken."""
    @staticmethod
    def calculate_ood_probability(current_thesis: ThesisVector, state: PositionExitState) -> float:
        d_m = state.entry_thesis.mahalanobis_distance(current_thesis, state.thesis_inv_cov)
        
        # Genuine Out-Of-Distribution probability using Chi-Square CDF
        # Features: OFI, Hawkes, Momentum, Depth, Volatility -> 5 degrees of freedom
        d2 = d_m ** 2
        try:
            ood_prob = chi2.cdf(d2, df=5)
            return float(np.clip(ood_prob, 0.0, 1.0))
        except Exception:
            return 0.5


class EmpiricalHazardModel:
    """State-dependent empirical hazard calculation h(t|X_t)."""
    @staticmethod
    def calculate_hazard(ctx: Dict[str, Any], time_in_trade: float) -> float:
        """
        Calculates the instantaneous risk of failure.
        [REQUIRES CALIBRATION]: Currently uses a linear combination of normalized 
        stressors rather than a hardcoded Weibull assumption.
        """
        stat = ctx.get("stat_engine")
        is_buy = ctx["is_buy"]
        
        ofi = getattr(stat, "ofi_fast_z", 0.0) if stat else 0.0
        aligned_ofi = ofi if is_buy else -ofi
        
        # Stress features
        flow_stress = max(0.0, -aligned_ofi / 3.0)
        time_stress = min(1.0, time_in_trade / 3600.0) # Normalizes to 1 hr horizon
        
        # Simulated empirical hazard weights (to be replaced by trained log-hazard model)
        h_t = (0.6 * flow_stress) + (0.4 * time_stress)
        return float(np.clip(h_t, 0.0, 1.0))


# =====================================================================
# 3. COUNTERFACTUAL TRAJECTORY SIMULATOR (DEFENDER)
# =====================================================================

class CounterfactualTrajectorySimulator:
    """Generates Monte Carlo paths to calculate true CVaR95 and CVaR99."""
    
    @staticmethod
    def simulate_distributions(sigma_eff: float, p_cont: float, p_rev: float, ood_prob: float, n_paths: int = 2000) -> Tuple[float, float, float]:
        """
        Simulates 2000 price paths using a Jump-Diffusion approximation driven 
        by empirical probabilities and OOD regime shift risk.
        """
        # 1. Base Gaussian Paths (Continuation vs Reversal mass)
        # We model the expected return based on the continuation probability edge
        expected_drift = sigma_eff * 1.5 * (p_cont - p_rev)
        base_paths = np.random.normal(loc=expected_drift, scale=sigma_eff, size=n_paths)
        
        # 2. Tail / Jump Events (Liquidity Vacuum, Regime Flips)
        # The higher the OOD probability, the more likely extreme adverse jumps occur.
        jump_probability = ood_prob * 0.15 
        jumps = np.random.uniform(low=0.0, high=1.0, size=n_paths)
        
        # Apply catastrophic adverse jumps (-3 to -6 sigma) to affected paths
        jump_mask = jumps < jump_probability
        base_paths[jump_mask] -= np.random.uniform(3.0, 6.0, size=np.sum(jump_mask)) * sigma_eff
        
        # Sort paths to find terminal wealth distribution percentiles
        sorted_paths = np.sort(base_paths)
        
        # Calculate Risk Metrics
        ev = float(np.mean(sorted_paths))
        
        # VaR indices
        idx_95 = int(n_paths * 0.05)
        idx_99 = int(n_paths * 0.01)
        
        # True CVaR (Expected Shortfall): Mean of losses worse than VaR
        cvar_95 = float(np.mean(sorted_paths[:idx_95]))
        cvar_99 = float(np.mean(sorted_paths[:idx_99]))
        
        return ev, cvar_95, cvar_99


# =====================================================================
# 4. ROBUST STOCHASTIC CONTROL (q-OPTIMIZATION)
# =====================================================================

class ExecutionCostEngine:
    @staticmethod
    def get_friction(price: float, qty_exiting: float, last_ob: Dict[str, Any], sigma_eff: float, is_aggressive: bool, exit_side: str) -> float:
        if qty_exiting <= 0: return 0.0
        
        fee_bps = 5.5 if is_aggressive else 2.0
        depth_key = "bid_depth_10" if exit_side == "SELL" else "ask_depth_10"
        available_depth = last_ob.get(depth_key, 1.0)
        
        impact_ratio = min(1.0, qty_exiting / max(available_depth, 1e-9))
        market_impact_bps = (impact_ratio * 20.0) * (1.0 + (sigma_eff / price * 50.0))
        
        total_bps = fee_bps + market_impact_bps
        return price * (total_bps / 10000.0)

class RobustStochasticController:
    """Solves q* = argmax U(q) continuously."""
    
    @staticmethod
    def _utility_function(
        q: float, 
        gross_pnl: float, 
        scenario_ev: float, 
        cvar_95: float, 
        cvar_99: float, 
        sigma_eff: float,
        ood_prob: float,
        price: float,
        total_qty: float,
        last_ob: Dict[str, Any],
        exit_side: str
    ) -> float:
        
        qty_exiting = total_qty * (1.0 - q)
        
        # 1. Execution Cost (Dynamic friction feeding back into optimization)
        cost_pts = ExecutionCostEngine.get_friction(price, qty_exiting, last_ob, sigma_eff, is_aggressive=True, exit_side=exit_side)
        ev_exit = (gross_pnl * (1.0 - q)) - cost_pts if qty_exiting > 0 else 0.0
        
        # 2. Retained Future Value
        ev_retain = (gross_pnl * q) + (scenario_ev * q)
        
        # 3. Robust Optimization Penalties
        lambda_1 = 0.3  # CVaR95 aversion
        lambda_2 = 0.5  # CVaR99 aversion
        lambda_3 = 1.0 * ood_prob  # Uncertainty / Model failure aversion
        
        risk_penalty = (lambda_1 * abs(min(0, cvar_95)) * q) + \
                       (lambda_2 * abs(min(0, cvar_99)) * q) + \
                       (lambda_3 * sigma_eff * q)
                       
        opportunity_cost = 0.0005 * price * q # Assumed baseline alternative EV
        
        u_q = (ev_exit + ev_retain) - risk_penalty - opportunity_cost
        return -u_q # Minimize negative utility for scipy

    @staticmethod
    def solve_q(
        gross_pnl: float, scenario_ev: float, cvar_95: float, cvar_99: float, 
        sigma_eff: float, ood_prob: float, price: float, total_qty: float, last_ob: Dict[str, Any], exit_side: str
    ) -> float:
        res = minimize_scalar(
            RobustStochasticController._utility_function,
            bounds=(0.0, 1.0),
            args=(gross_pnl, scenario_ev, cvar_95, cvar_99, sigma_eff, ood_prob, price, total_qty, last_ob, exit_side),
            method='bounded'
        )
        return float(res.x) if res.success else 1.0


# =====================================================================
# 5. APEX OMEGA MASTER ORACLE
# =====================================================================

class IntelligentExitEngine:
    
    @staticmethod
    def _extract_thesis(ctx: Dict[str, Any]) -> ThesisVector:
        stat = ctx.get("stat_engine")
        ob = ctx.get("last_ob", {})
        bid = max(ob.get("bid_size", 1.0), 1e-9)
        ask = max(ob.get("ask_size", 1.0), 1e-9)
        depth_ratio = bid / ask if ctx["is_buy"] else ask / bid
        
        return ThesisVector(np.array([
            getattr(stat, "ofi_fast_z", 0.0) if stat else 0.0,
            getattr(stat, "hawkes_z", 0.0) if stat else 0.0,
            0.0, # Momentum
            depth_ratio,
            getattr(stat, "inst_variance", 1e-6) if stat else 1e-6
        ]))

    @staticmethod
    def evaluate(ctx: Dict[str, Any], state: PositionExitState) -> ExitDecision:
        is_buy = ctx["is_buy"]
        price = ctx.get("safe_c_price", state.entry_price)
        
        # QTY is now explicitly derived from the context (physical exchange state), not an internal hallucination.
        total_qty = ctx.get("actual_qty_filled", 1.0) 
        
        stat = ctx.get("stat_engine")
        inst_var = getattr(stat, "inst_variance", 1e-6) if stat else 1e-6
        sigma_eff = price * math.sqrt(max(inst_var, 1e-12)) * 1.5 
        
        gross_pnl = (price - state.entry_price) if is_buy else (state.entry_price - price)
        
        # 1. Regime Shift & Model Failure (OOD) - The Assassin Model
        current_thesis = IntelligentExitEngine._extract_thesis(ctx)
        ood_prob = RegimeChangeDetector.calculate_ood_probability(current_thesis, state)
        
        # 2. Hazard Rate Evolution
        time_held = max(0.1, time.time() - state.entry_time)
        hazard_rate = EmpiricalHazardModel.calculate_hazard(ctx, time_held)
        
        # 3. First-Passage Probability Approximation (Walk-forward placeholder)
        ofi_z = current_thesis.features[0]
        aligned_ofi = ofi_z if is_buy else -ofi_z
        p_cont = max(0.05, min(0.95, 0.50 + (aligned_ofi * 0.15)))
        p_rev = 1.0 - p_cont
        
        # 4. Counterfactual Monte Carlo Trajectories - The Defender Model
        ev_future, cvar_95, cvar_99 = CounterfactualTrajectorySimulator.simulate_distributions(sigma_eff, p_cont, p_rev, ood_prob, n_paths=2000)
        
        # 5. Robust Stochastic Control (q*)
        last_ob = ctx.get("last_ob", {})
        
        optimal_q = RobustStochasticController.solve_q(
            gross_pnl, ev_future, cvar_95, cvar_99, sigma_eff, ood_prob, price, total_qty, last_ob, state.exit_side
        )
        
        # 6. Execution Urgency & Routing Logic
        if ood_prob > 0.85 or hazard_rate > 0.60:
            urgency = "MARKET"
            action = "EMERGENCY"
            optimal_q = 0.0
        elif optimal_q < 0.95:
            action = "EXIT" if optimal_q < 0.05 else "REDUCE"
            spread_bps = abs(last_ob.get("best_ask", price) - last_ob.get("best_bid", price)) / price * 10000.0
            urgency = "AGGRESSIVE" if spread_bps < 3.0 else "PASSIVE"
        else:
            action = "HOLD"
            urgency = "NONE"
            
        limit_p = last_ob.get("best_bid", price) if state.exit_side == "SELL" else last_ob.get("best_ask", price)
        if urgency == "AGGRESSIVE":
            limit_p = limit_p * 0.9995 if state.exit_side == "SELL" else limit_p * 1.0005

        # 7. Terminal Log Generation
        log_str = (
            f"\n╔══════════════════════════════════════╗\n"
            f"║ APEX OMEGA DECISION                  ║\n"
            f"╠══════════════════════════════════════╣\n"
            f"║ Distribution Shift       {ood_prob:.3f}       ║\n"
            f"║ Hazard Rate              {hazard_rate:.3f}       ║\n"
            f"║ MC True EV              {ev_future/sigma_eff:+.2f}σ        ║\n"
            f"║ MC True CVaR95          {cvar_95/sigma_eff:+.2f}σ        ║\n"
            f"║ MC True CVaR99          {cvar_99/sigma_eff:+.2f}σ        ║\n"
            f"╠══════════════════════════════════════╣\n"
            f"║ OPTIMAL q*               {optimal_q:.3f}       ║\n"
            f"║ ACTUAL POS               {total_qty:.4f}       ║\n"
            f"║ ACTION                   {action:<10}  ║\n"
            f"║ URGENCY                  {urgency:<10}  ║\n"
            f"╚══════════════════════════════════════╝\n"
        )
        
        return ExitDecision(action, optimal_q, urgency, limit_p, f"Q_OPT_{optimal_q:.2f}", log_str)


# =====================================================================
# 6. STATEFUL EXECUTION GOVERNOR FSM
# =====================================================================

class ExecutionGovernorFSM:
    """
    OBSERVE -> POST -> MONITOR -> REPRICE -> MARKET
    """
    @staticmethod
    async def manage_execution(decision: ExitDecision, state: PositionExitState, ctx: Dict[str, Any], executor: Any) -> bool:
        if decision.action == "HOLD":
            return False
            
        symbol = ctx["symbol"]
        current_actual_qty = ctx.get("actual_qty_filled", 0.0) 
        
        # The math dictates how much we WANT to have remaining (target_q * current_actual_qty)
        target_retained_qty = current_actual_qty * decision.target_q
        qty_to_close = current_actual_qty - target_retained_qty
        
        if qty_to_close <= 0:
            return False
            
        if decision.urgency in ["MARKET", "EMERGENCY"]:
            state.execution_state = "MARKET"
        
        if state.execution_state == "OBSERVE":
            logger.critical(decision.log_output)
            state.target_q = decision.target_q # Update intent, wait for exchange confirmation to confirm reality
            
            if decision.urgency == "PASSIVE":
                res = await executor.safe_call(executor.client.place_order, category="linear", symbol=symbol, side=state.exit_side, orderType="Limit", price=str(decision.limit_price), qty=str(qty_to_close), timeInForce="PostOnly", reduceOnly=True)
                if res.get("retCode") == 0:
                    state.execution_state = "MONITOR"
                    state.exec_order_id = res["result"]["orderId"]
                    state.exec_post_time = time.time()
            elif decision.urgency == "AGGRESSIVE":
                res = await executor.safe_call(executor.client.place_order, category="linear", symbol=symbol, side=state.exit_side, orderType="Limit", price=str(decision.limit_price), qty=str(qty_to_close), timeInForce="IOC", reduceOnly=True)
                if res.get("retCode") == 0:
                    state.execution_state = "AWAITING_EXCHANGE_SYNC"
                    return True
                else:
                    state.execution_state = "MARKET" # IOC missed, escalate
                    
        elif state.execution_state == "MONITOR":
            if time.time() - state.exec_post_time > 5.0:
                await executor.safe_call(executor.client.cancel_order, category="linear", symbol=symbol, orderId=state.exec_order_id)
                state.execution_state = "REPRICE"
                
        elif state.execution_state in ["REPRICE", "MARKET"]:
            res = await executor.safe_call(executor.client.place_order, category="linear", symbol=symbol, side=state.exit_side, orderType="Market", qty=str(qty_to_close), timeInForce="IOC", reduceOnly=True)
            if res.get("retCode") == 0:
                state.execution_state = "AWAITING_EXCHANGE_SYNC"
                return True
                
        return False