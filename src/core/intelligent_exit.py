"""
💎 APEX OMEGA V13: THE HIERARCHY (PORTFOLIO & PROFIT DEFENDER)
-----------------------------------------------------------------------
Stateful, Hierarchical Optimal-Stopping Engine.
Architecture Pipeline:
LEVEL 0: Exchange Reality (Physical Sync & Reconciliation)
LEVEL 1: Portfolio Commander (Systemic Kill Switch)
LEVEL 2: Catastrophic Protection (Assassin / OOD / Hazard)
LEVEL 3: Profit Governor (State Machine / MFE / Giveback / Velocity)
LEVEL 4: Defender (Deterministically Seeded Monte Carlo CVaR)
LEVEL 5: Continuous Optimizer (q*)
LEVEL 6: Execution FSM (Observe -> Post -> Escalate -> Sync)
"""

import math
import time
import logging
import numpy as np
from scipy.optimize import minimize_scalar
from scipy.stats import chi2
from dataclasses import dataclass, field
from typing import Dict, Any, Tuple

logger = logging.getLogger("QUANT_CORE.APEX_OMEGA")

# =====================================================================
# 1. RIGOROUS THESIS & STATE TRACKING
# =====================================================================

@dataclass
class ThesisVector:
    features: np.ndarray  
    
    def mahalanobis_distance(self, other: 'ThesisVector', inv_cov_matrix: np.ndarray) -> float:
        delta = self.features - other.features
        try:
            distance_sq = np.dot(np.dot(delta.T, inv_cov_matrix), delta)
            return float(math.sqrt(max(0.0, distance_sq)))
        except Exception:
            return 0.0

@dataclass
class ProfitProtectionState:
    """Independent Profit Memory & Velocity Tracker."""
    state_id: str = "UNPROFITABLE" # UNPROFITABLE, PROFIT_FORMING, PROFIT_ARMED, PROFIT_LOCKED, GIVEBACK_WARNING, GIVEBACK_EXIT
    
    peak_pnl: float = 0.0
    peak_price: float = 0.0
    locked_pnl: float = -999999.0
    
    mfe: float = 0.0
    mae: float = 0.0
    
    last_pnl: float = 0.0
    last_pnl_time: float = field(default_factory=time.time)
    pnl_velocity: float = 0.0  # dpnl/dt

@dataclass
class PositionExitState:
    """PERSISTENT state object living in the main FSM."""
    # NON-DEFAULT FIELDS FIRST
    position_id: str
    entry_time: float
    entry_price: float
    exit_side: str
    entry_balance: float
    entry_thesis: ThesisVector
    thesis_inv_cov: np.ndarray  
    
    # DEFAULT FIELDS SECOND
    actual_qty: float = 0.0 
    base_qty: float = 0.0  # Safeguard for base quantity tracking
    
    profit_state: ProfitProtectionState = field(default_factory=ProfitProtectionState)
    last_eval_time: float = field(default_factory=time.time)
    q_retained: float = 1.0  # Mathematical intent
    
    # Execution FSM
    execution_state: str = "SYNC" # ALWAYS start by verifying reality
    exec_order_id: str = ""
    exec_post_time: float = 0.0
    target_q: float = 1.0

@dataclass
class ExitDecision:
    action: str
    target_q: float
    urgency: str
    limit_price: float
    reason: str
    log_output: str


# =====================================================================
# LEVEL 1 & 2: PORTFOLIO COMMANDER & ASSASSIN
# =====================================================================

class PortfolioCommander:
    @staticmethod
    def evaluate(ctx: Dict[str, Any]) -> Tuple[bool, str]:
        """Portfolio-level kill switch."""
        portfolio_dd = ctx.get("drawdown_pct", 0.0)
        live_count = ctx.get("active_positions_count", 1)
        
        if portfolio_dd > 0.10:
            return True, f"SYSTEMIC_DRAWDOWN_FLATTEN ({portfolio_dd*100:.1f}%)"
        if portfolio_dd > 0.06 and live_count > 4:
            return True, f"PORTFOLIO_CORRELATION_REDUCTION (DD: {portfolio_dd*100:.1f}% | {live_count} Pos)"
            
        return False, "SAFE"

class EmergencyAssassin:
    @staticmethod
    def evaluate(ctx: Dict[str, Any], state: PositionExitState, current_thesis: ThesisVector) -> Tuple[bool, str, float, float]:
        """Kills positions when the statistical thesis or regime collapses."""
        d_m = state.entry_thesis.mahalanobis_distance(current_thesis, state.thesis_inv_cov)
        
        # True OOD Upper-Tail Probability
        d2 = d_m ** 2
        try:
            ood_prob = float(np.clip(1.0 - chi2.cdf(d2, df=5), 0.0, 1.0))
        except Exception:
            ood_prob = 0.5
            
        time_held = max(0.1, time.time() - state.entry_time)
        stat = ctx.get("stat_engine")
        ofi = getattr(stat, "ofi_fast_z", 0.0) if stat else 0.0
        aligned_ofi = ofi if ctx["is_buy"] else -ofi
        
        flow_stress = max(0.0, -aligned_ofi / 3.0)
        time_stress = min(1.0, time_held / 3600.0)
        hazard_rate = float(np.clip((0.6 * flow_stress) + (0.4 * time_stress), 0.0, 1.0))
        
        if ood_prob > 0.85:
            return True, f"REGIME_OOD_COLLAPSE ({ood_prob:.2f})", ood_prob, hazard_rate
        if hazard_rate > 0.70:
            return True, f"CRITICAL_HAZARD ({hazard_rate:.2f})", ood_prob, hazard_rate
            
        return False, "SAFE", ood_prob, hazard_rate


# =====================================================================
# LEVEL 3: THE PROFIT GOVERNOR
# =====================================================================

class ProfitGovernor:
    @staticmethod
    def evaluate(ctx: Dict[str, Any], state: PositionExitState, ood_prob: float, hazard_rate: float) -> Tuple[bool, float, str]:
        """
        The Non-Negotiable Capital Preservation Layer.
        Tracks MFE/MAE, PnL velocity, and manages the Profit State Machine.
        Returns: (override_active, target_q, reason)
        """
        p_state = state.profit_state
        is_buy = ctx["is_buy"]
        price = ctx.get("safe_c_price", state.entry_price)
        qty = state.actual_qty 
        
        if qty <= 0: return False, 1.0, "SAFE"
            
        current_pnl = (price - state.entry_price) * qty if is_buy else (state.entry_price - price) * qty
        
        # 1. Update MFE / MAE
        if current_pnl > p_state.peak_pnl:
            p_state.peak_pnl = current_pnl
            p_state.peak_price = price
            p_state.mfe = max(p_state.mfe, current_pnl)
        if current_pnl < 0:
            p_state.mae = min(p_state.mae, current_pnl)
            
        # 2. PnL Velocity (dpnl/dt)
        now = time.time()
        dt = max(now - p_state.last_pnl_time, 0.001)
        p_state.pnl_velocity = (current_pnl - p_state.last_pnl) / dt
        p_state.last_pnl = current_pnl
        p_state.last_pnl_time = now
        
        # 3. Dynamic Thresholds (Calibrated to account equity)
        equity = max(ctx.get("current_vault_balance", state.entry_balance), 1.0)
        activation_pnl = equity * 0.01  # 1% account gain arms the system
        lock_pnl = equity * 0.03        # 3% account gain locks aggressive floor
        fee_hurdle = state.entry_price * qty * 0.0015 
        
        # 4. PROFIT STATE MACHINE
        if p_state.state_id == "UNPROFITABLE":
            if current_pnl > fee_hurdle:
                p_state.state_id = "PROFIT_FORMING"
                
        elif p_state.state_id == "PROFIT_FORMING":
            if current_pnl > activation_pnl:
                p_state.state_id = "PROFIT_ARMED"
                p_state.locked_pnl = fee_hurdle # Lock breakeven
            elif current_pnl < 0:
                p_state.state_id = "UNPROFITABLE"
                
        elif p_state.state_id == "PROFIT_ARMED":
            if current_pnl > lock_pnl:
                p_state.state_id = "PROFIT_LOCKED"
                p_state.locked_pnl = p_state.peak_pnl * 0.50
                
            giveback_pct = (p_state.peak_pnl - current_pnl) / max(p_state.peak_pnl, 1e-9)
            if giveback_pct > 0.40 or current_pnl <= p_state.locked_pnl:
                p_state.state_id = "GIVEBACK_EXIT"
                
        elif p_state.state_id == "PROFIT_LOCKED":
            p_state.locked_pnl = max(p_state.locked_pnl, p_state.peak_pnl * 0.60)
            
            giveback_pct = (p_state.peak_pnl - current_pnl) / max(p_state.peak_pnl, 1e-9)
            
            # Velocity & OOD sensitive giveback
            max_giveback = 0.25
            if ood_prob > 0.50 or p_state.pnl_velocity < 0:
                max_giveback = 0.15
                p_state.state_id = "GIVEBACK_WARNING"
                
            if giveback_pct > max_giveback or current_pnl <= p_state.locked_pnl:
                p_state.state_id = "GIVEBACK_EXIT"
                
        elif p_state.state_id == "GIVEBACK_WARNING":
            if p_state.pnl_velocity > 0 and current_pnl > p_state.locked_pnl:
                p_state.state_id = "PROFIT_LOCKED" # Recovered
            elif current_pnl <= p_state.locked_pnl:
                p_state.state_id = "GIVEBACK_EXIT"
                
        # 5. EXECUTE GOVERNOR VETO
        if p_state.state_id == "GIVEBACK_EXIT":
            return True, 0.0, f"PROFIT_DEFENDER_EXIT (Locked: ${p_state.locked_pnl:.2f} | Peak: ${p_state.peak_pnl:.2f})"
            
        # Optional Reduction Veto
        if p_state.state_id == "GIVEBACK_WARNING":
            return True, 0.50, f"GIVEBACK_WARNING_REDUCE (Vel: ${p_state.pnl_velocity:.2f}/s)"
            
        return False, 1.0, "PROFIT_SAFE"


# =====================================================================
# LEVEL 4 & 5: DEFENDER (MC) & OPTIMIZER (q*)
# =====================================================================

class CounterfactualTrajectorySimulator:
    @staticmethod
    def simulate(sigma_eff: float, p_cont: float, p_rev: float, ood_prob: float, seed_val: int, n_paths: int = 1500) -> Tuple[float, float, float]:
        """Deterministically seeded Monte Carlo to prevent evaluation jitter."""
        rng = np.random.RandomState(seed_val)
        
        expected_drift = sigma_eff * 1.5 * (p_cont - p_rev)
        base_paths = rng.normal(loc=expected_drift, scale=sigma_eff, size=n_paths)
        
        jumps = rng.uniform(0.0, 1.0, size=n_paths)
        jump_mask = jumps < (ood_prob * 0.15)
        base_paths[jump_mask] -= rng.uniform(3.0, 6.0, size=np.sum(jump_mask)) * sigma_eff
        
        sorted_paths = np.sort(base_paths)
        ev = float(np.mean(sorted_paths))
        cvar_95 = float(np.mean(sorted_paths[:int(n_paths * 0.05)]))
        cvar_99 = float(np.mean(sorted_paths[:int(n_paths * 0.01)]))
        
        return ev, cvar_95, cvar_99

class RobustStochasticController:
    @staticmethod
    def _utility_function(q: float, gross_pnl: float, scenario_ev: float, cvar_95: float, cvar_99: float, sigma_eff: float, ood_prob: float, price: float, total_qty: float, last_ob: Dict[str, Any], exit_side: str) -> float:
        qty_exiting = total_qty * (1.0 - q)
        
        fee_bps = 5.5
        depth_key = "bid_depth_10" if exit_side == "SELL" else "ask_depth_10"
        available_depth = last_ob.get(depth_key, 1.0)
        impact_ratio = min(1.0, qty_exiting / max(available_depth, 1e-9))
        market_impact_bps = (impact_ratio * 20.0) * (1.0 + (sigma_eff / price * 50.0))
        
        cost_pts = price * ((fee_bps + market_impact_bps) / 10000.0)
        ev_exit = (gross_pnl * (1.0 - q)) - cost_pts if qty_exiting > 0 else 0.0
        ev_retain = (gross_pnl * q) + (scenario_ev * q)
        
        risk_penalty = (0.3 * abs(min(0, cvar_95)) * q) + (0.5 * abs(min(0, cvar_99)) * q) + (ood_prob * sigma_eff * q)
        opportunity_cost = 0.0005 * price * q 
        
        u_q = (ev_exit + ev_retain) - risk_penalty - opportunity_cost
        return -u_q 

    @staticmethod
    def solve_q(gross_pnl: float, scenario_ev: float, cvar_95: float, cvar_99: float, sigma_eff: float, ood_prob: float, price: float, total_qty: float, last_ob: Dict[str, Any], exit_side: str) -> float:
        res = minimize_scalar(RobustStochasticController._utility_function, bounds=(0.0, 1.0), args=(gross_pnl, scenario_ev, cvar_95, cvar_99, sigma_eff, ood_prob, price, total_qty, last_ob, exit_side), method='bounded')
        return float(res.x) if res.success else 1.0


# =====================================================================
# THE APEX OMEGA MASTER ORACLE
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
            0.0, depth_ratio, getattr(stat, "inst_variance", 1e-6) if stat else 1e-6
        ]))

    @staticmethod
    def evaluate(ctx: Dict[str, Any], state: PositionExitState) -> ExitDecision:
        # LEVEL 0: Physical Exchange Sync
        if state.execution_state == "SYNC":
            return ExitDecision("HOLD", state.q_retained, "NONE", 0.0, "AWAITING_EXCHANGE_SYNC", "[LEVEL 0: SYNC REQUIRED]")

        is_buy = ctx["is_buy"]
        price = ctx.get("safe_c_price", state.entry_price)
        total_qty = state.actual_qty  # TRUE reality, not assumed q*
        
        if total_qty <= 0:
            return ExitDecision("HOLD", 0.0, "NONE", price, "ZERO_POSITION", "[GHOST POSITION]")
            
        stat = ctx.get("stat_engine")
        inst_var = getattr(stat, "inst_variance", 1e-6) if stat else 1e-6
        sigma_eff = price * math.sqrt(max(inst_var, 1e-12)) * 1.5 
        
        gross_pnl = (price - state.entry_price) if is_buy else (state.entry_price - price)
        current_thesis = IntelligentExitEngine._extract_thesis(ctx)
        
        # LEVEL 1: Portfolio Commander
        pf_override, pf_reason = PortfolioCommander.evaluate(ctx)
        if pf_override:
            return ExitDecision("EMERGENCY", 0.0, "MARKET", price, pf_reason, "[PORTFOLIO KILL SWITCH]")
            
        # LEVEL 2: Emergency Assassin
        ass_override, ass_reason, ood_prob, hazard_rate = EmergencyAssassin.evaluate(ctx, state, current_thesis)
        if ass_override:
            return ExitDecision("EMERGENCY", 0.0, "MARKET", price, ass_reason, "[ASSASSIN THESIS DESTRUCTION]")
            
        # LEVEL 3: Profit Governor
        prof_override, prof_target_q, prof_reason = ProfitGovernor.evaluate(ctx, state, ood_prob, hazard_rate)
        if prof_override:
            urgency = "MARKET" if prof_target_q == 0.0 else "AGGRESSIVE"
            return ExitDecision("EXIT" if prof_target_q == 0.0 else "REDUCE", prof_target_q, urgency, price, prof_reason, "[PROFIT DEFENDER VETO]")
            
        # LEVEL 4 & 5: Defender (MC) & Stochastic Optimizer (q*)
        ofi_z = current_thesis.features[0]
        aligned_ofi =ofi_z if is_buy else -ofi_z
        p_cont = max(0.05, min(0.95, 0.50 + (aligned_ofi * 0.15)))
        
        # Deterministic seed based on price to prevent flip-flopping MC paths
        seed_val = int(price * 100) % (2**32 - 1)
        ev_fut, cvar_95, cvar_99 = CounterfactualTrajectorySimulator.simulate(sigma_eff, p_cont, 1.0 - p_cont, ood_prob, seed_val)
        
        last_ob = ctx.get("last_ob", {})
        q_opt_raw = RobustStochasticController.solve_q(
            gross_pnl, ev_fut, cvar_95, cvar_99, sigma_eff, ood_prob, price, total_qty, last_ob, state.exit_side
        )
        
        # Final q* is bounded by the Profit Governor's ceiling
        optimal_q = min(q_opt_raw, prof_target_q)

        # LEVEL 6: Execution Routing
        if optimal_q < 0.95:
            action = "EXIT" if optimal_q < 0.05 else "REDUCE"
            spread_bps = abs(last_ob.get("best_ask", price) - last_ob.get("best_bid", price)) / max(price, 1e-9) * 10000.0
            urgency = "AGGRESSIVE" if spread_bps < 3.0 else "PASSIVE"
        else:
            action = "HOLD"
            urgency = "NONE"
            
        limit_p = last_ob.get("best_bid", price) if state.exit_side == "SELL" else last_ob.get("best_ask", price)
        if urgency == "AGGRESSIVE":
            limit_p = limit_p * 0.9995 if state.exit_side == "SELL" else limit_p * 1.0002

        log_str = (
            f"\n╔══════════════════════════════════════╗\n"
            f"║ APEX OMEGA V13: PROFIT DEFENDER      ║\n"
            f"╠══════════════════════════════════════╣\n"
            f"║ Profit State             {state.profit_state.state_id:<12}║\n"
            f"║ Peak PnL                ${state.profit_state.peak_pnl:+.2f}       ║\n"
            f"║ PnL Velocity (dpnl/dt)  ${state.profit_state.pnl_velocity:+.3f}/s    ║\n"
            f"║ OOD Anomaly Shift        {ood_prob:.3f}       ║\n"
            f"║ True CVaR95             {cvar_95/max(sigma_eff, 1e-9):+.2f}σ        ║\n"
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
    Physical mapping:
    OBSERVE -> POSTED -> MONITOR -> REPRICE -> MARKET -> SYNC -> OBSERVE
    """
    @staticmethod
    async def manage_execution(decision: ExitDecision, state: PositionExitState, ctx: Dict[str, Any], executor: Any) -> bool:
        if decision.action == "HOLD":
            return False
            
        symbol = ctx["symbol"]
        current_actual_qty = state.actual_qty 
        
        target_retained_qty = current_actual_qty * decision.target_q
        qty_to_close = current_actual_qty - target_retained_qty
        
        if qty_to_close <= 0:
            return False
            
        if decision.urgency in ["MARKET", "EMERGENCY"]:
            state.execution_state = "MARKET"
        
        if state.execution_state == "OBSERVE":
            logger.critical(decision.log_output)
            state.target_q = decision.target_q 
            
            if decision.urgency == "PASSIVE":
                res = await executor.safe_call(executor.client.place_order, category="linear", symbol=symbol, side=state.exit_side, orderType="Limit", price=str(decision.limit_price), qty=str(qty_to_close), timeInForce="PostOnly", reduceOnly=True)
                if res.get("retCode") == 0:
                    state.execution_state = "MONITOR"
                    state.exec_order_id = res["result"]["orderId"]
                    state.exec_post_time = time.time()
            elif decision.urgency == "AGGRESSIVE":
                res = await executor.safe_call(executor.client.place_order, category="linear", symbol=symbol, side=state.exit_side, orderType="Limit", price=str(decision.limit_price), qty=str(qty_to_close), timeInForce="IOC", reduceOnly=True)
                state.execution_state = "SYNC"
                return True
                    
        elif state.execution_state == "MONITOR":
            if time.time() - state.exec_post_time > 5.0:
                await executor.safe_call(executor.client.cancel_order, category="linear", symbol=symbol, orderId=state.exec_order_id)
                state.execution_state = "REPRICE"
                
        elif state.execution_state in ["REPRICE", "MARKET"]:
            res = await executor.safe_call(executor.client.place_order, category="linear", symbol=symbol, side=symbol, side=state.exit_side, orderType="Market", qty=str(qty_to_close), timeInForce="IOC", reduceOnly=True)
            state.execution_state = "SYNC"
            return True
                
        return False