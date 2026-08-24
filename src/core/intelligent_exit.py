"""
💎 APEX OMEGA V15: DETERMINISTIC HIERARCHY & KINETIC TP COMPRESSOR
-----------------------------------------------------------------------
Stateful, Hierarchical Optimal-Stopping Engine.
Architecture Pipeline:
LEVEL 0: Exchange Reality (Physical Sync & Reconciliation)
LEVEL 1: Portfolio Commander (Systemic Kill Switch)
LEVEL 2: Catastrophic Protection (Assassin / OOD / Hazard / Time-Decay)
LEVEL 3: Profit Governor (Continuous Sigmoidal Ratchet & Velocity)
LEVEL 3.5: Kinetic TP Compressor (Self-Aware Momentum Front-Runner)
LEVEL 4: Defender (Analytical Merton Jump-Diffusion CVaR)
LEVEL 5: Continuous Optimizer (Exact Closed-Form q*)
LEVEL 6: Execution FSM (Observe -> Post -> Escalate -> Sync)
"""

import math
import time
import logging
import numpy as np
from scipy.stats import chi2, norm
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
            distance_sq = float(np.dot(np.dot(delta.T, inv_cov_matrix), delta))
            return math.sqrt(max(0.0, distance_sq))
        except Exception:
            return 0.0

@dataclass
class ProfitProtectionState:
    """Independent Profit Memory & Velocity Tracker."""
    state_id: str = "UNPROFITABLE" 
    
    peak_pnl: float = 0.0
    peak_price: float = 0.0
    locked_pnl: float = -1e9
    
    mfe: float = 0.0
    mae: float = 0.0
    
    last_pnl: float = 0.0
    last_pnl_time: float = field(default_factory=time.time)
    pnl_velocity: float = 0.0  # dpnl/dt

@dataclass
class PositionExitState:
    """PERSISTENT state object living in the main FSM."""
    position_id: str
    entry_time: float
    entry_price: float
    exit_side: str
    entry_balance: float
    entry_thesis: ThesisVector
    thesis_inv_cov: np.ndarray  
    
    actual_qty: float = 0.0 
    base_qty: float = 0.0  
    
    profit_state: ProfitProtectionState = field(default_factory=ProfitProtectionState)
    last_eval_time: float = field(default_factory=time.time)
    q_retained: float = 1.0  
    
    execution_state: str = "SYNC" 
    exec_order_id: str = ""
    exec_post_time: float = 0.0
    target_q: float = 1.0

@dataclass
class ExitDecision:
    action: str
    target_q: float
    urgency: str
    limit_price: float
    exchange_ts_price: float  # Trailing SL coordinate
    dynamic_tp_price: float   # 🚀 NEW: Kinetic TP Compression coordinate
    reason: str
    log_output: str


# =====================================================================
# LEVEL 1 & 2: PORTFOLIO COMMANDER & ASSASSIN
# =====================================================================

class PortfolioCommander:
    @staticmethod
    def evaluate(ctx: Dict[str, Any]) -> Tuple[bool, str]:
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
        d_m = state.entry_thesis.mahalanobis_distance(current_thesis, state.thesis_inv_cov)
        
        try:
            ood_prob = float(np.clip(1.0 - chi2.cdf(d_m**2, df=5), 0.0, 1.0))
        except Exception:
            ood_prob = 0.5
            
        time_held_mins = max(0.1, (time.time() - state.entry_time) / 60.0)
        
        stat = ctx.get("stat_engine")
        ofi = getattr(stat, "ofi_fast_z", 0.0) if stat else 0.0
        aligned_ofi = ofi if ctx["is_buy"] else -ofi
        
        flow_stress = max(0.0, -aligned_ofi / 3.0)
        time_stress = min(1.0, time_held_mins / 60.0)
        hazard_rate = float(np.clip((0.6 * flow_stress) + (0.4 * time_stress), 0.0, 1.0))
        
        price = float(ctx.get("safe_c_price", state.entry_price))
        current_pnl = (price - state.entry_price) * state.actual_qty if ctx["is_buy"] else (state.entry_price - price) * state.actual_qty
        
        if current_pnl < 0 and time_held_mins > 45.0 and hazard_rate > 0.50:
            return True, f"TIME_DECAY_HAZARD ({time_held_mins:.1f}m held, cutting bad trade)", ood_prob, hazard_rate

        if ood_prob > 0.85:
            return True, f"REGIME_OOD_COLLAPSE ({ood_prob:.2f})", ood_prob, hazard_rate
        if hazard_rate > 0.75:
            return True, f"CRITICAL_HAZARD ({hazard_rate:.2f})", ood_prob, hazard_rate
            
        return False, "SAFE", ood_prob, hazard_rate


# =====================================================================
# LEVEL 3: THE PROFIT GOVERNOR & KINETIC TP COMPRESSOR
# =====================================================================

class ProfitGovernor:
    @staticmethod
    def evaluate(ctx: Dict[str, Any], state: PositionExitState, ood_prob: float, hazard_rate: float) -> Tuple[bool, float, str]:
        p_state = state.profit_state
        is_buy = ctx["is_buy"]
        price = float(ctx.get("safe_c_price", state.entry_price))
        qty = state.actual_qty 
        
        if qty <= 0: return False, 1.0, "SAFE"
            
        current_pnl = (price - state.entry_price) * qty if is_buy else (state.entry_price - price) * qty
        
        if current_pnl > p_state.peak_pnl:
            p_state.peak_pnl = current_pnl
            p_state.peak_price = price
            p_state.mfe = max(p_state.mfe, current_pnl)
        if current_pnl < 0:
            p_state.mae = min(p_state.mae, current_pnl)
            
        now = time.time()
        dt = max(now - p_state.last_pnl_time, 0.001)
        p_state.pnl_velocity = (current_pnl - p_state.last_pnl) / dt
        p_state.last_pnl = current_pnl
        p_state.last_pnl_time = now
        
        equity = max(float(ctx.get("current_vault_balance", state.entry_balance)), 1.0)
        activation_pnl = equity * 0.015 
        lock_pnl = equity * 0.035 
        parabolic_pnl = equity * 0.06 
        fee_hurdle = state.entry_price * qty * 0.0015 
        
        if p_state.state_id == "UNPROFITABLE":
            if current_pnl > fee_hurdle:
                p_state.state_id = "PROFIT_FORMING"
                
        elif p_state.state_id == "PROFIT_FORMING":
            if current_pnl > activation_pnl:
                p_state.state_id = "PROFIT_ARMED"
                p_state.locked_pnl = fee_hurdle 
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
            if current_pnl > parabolic_pnl:
                p_state.state_id = "PARABOLIC_TRAIL"
                p_state.locked_pnl = max(p_state.locked_pnl, p_state.peak_pnl * 0.85)
            else:
                p_state.locked_pnl = max(p_state.locked_pnl, p_state.peak_pnl * 0.60)
            
            giveback_pct = (p_state.peak_pnl - current_pnl) / max(p_state.peak_pnl, 1e-9)
            max_giveback = 0.25 - (0.10 * ood_prob)
            
            if ood_prob > 0.50 or p_state.pnl_velocity < 0:
                max_giveback = 0.15
                p_state.state_id = "GIVEBACK_WARNING"
                
            if giveback_pct > max_giveback or current_pnl <= p_state.locked_pnl:
                p_state.state_id = "GIVEBACK_EXIT"

        elif p_state.state_id == "PARABOLIC_TRAIL":
            p_state.locked_pnl = max(p_state.locked_pnl, p_state.peak_pnl * 0.85)
            giveback_pct = (p_state.peak_pnl - current_pnl) / max(p_state.peak_pnl, 1e-9)
            
            if giveback_pct > 0.15 or current_pnl <= p_state.locked_pnl:
                p_state.state_id = "GIVEBACK_EXIT"
                
        elif p_state.state_id == "GIVEBACK_WARNING":
            if p_state.pnl_velocity > 0 and current_pnl > p_state.locked_pnl:
                p_state.state_id = "PROFIT_LOCKED" 
            elif current_pnl <= p_state.locked_pnl:
                p_state.state_id = "GIVEBACK_EXIT"
                
        if p_state.state_id == "GIVEBACK_EXIT":
            return True, 0.0, f"PROFIT_DEFENDER_EXIT (Locked: ${p_state.locked_pnl:.2f} | Peak: ${p_state.peak_pnl:.2f})"
            
        if p_state.state_id == "GIVEBACK_WARNING":
            return True, 0.50, f"GIVEBACK_WARNING_REDUCE (Vel: ${p_state.pnl_velocity:.2f}/s)"
            
        return False, 1.0, "PROFIT_SAFE"

class KineticTPCompressor:
    """
    🚀 LEVEL 3.5: SELF-AWARE MOMENTUM FRONT-RUNNER
    Dynamically yanks the Take Profit coordinate down into current price if 
    microstructure order flow violently reverses while holding deep profits.
    """
    @staticmethod
    def evaluate(ctx: Dict[str, Any], state: PositionExitState, current_thesis: ThesisVector) -> Tuple[bool, float, str, float]:
        p_state = state.profit_state
        price = float(ctx.get("safe_c_price", state.entry_price))
        is_buy = ctx["is_buy"]

        # 1. Base TP calculation (if momentum is normal)
        tp_distance = abs(price - state.entry_price) * 1.5
        base_tp = price + tp_distance if is_buy else price - tp_distance

        # Only activate Kinetic compression if we have substantial profits to protect
        if p_state.state_id not in ["PROFIT_LOCKED", "PARABOLIC_TRAIL", "GIVEBACK_WARNING"]:
            return False, 1.0, "KINETIC_SAFE", base_tp

        ofi_z = current_thesis.features[0]
        hawkes_z = current_thesis.features[1]
        meso_z = current_thesis.features[2]
        
        # Align momentum vectors to trade direction (Positive = Surging, Negative = Reversing)
        aligned_ofi = ofi_z if is_buy else -ofi_z
        aligned_hawkes = hawkes_z if is_buy else -hawkes_z
        aligned_meso = meso_z if is_buy else -meso_z
        
        # 2. Compute Composite Kinetic Stress
        composite_momentum = (aligned_ofi * 0.45) + (aligned_hawkes * 0.40) + (aligned_meso * 0.15)
        
        # 3. SELF-AWARE SENSITIVITY: The more equity we've gained, the more paranoid we get.
        equity = max(float(ctx.get("current_vault_balance", state.entry_balance)), 1.0)
        profit_margin_pct = p_state.peak_pnl / equity
        
        # Scales from 1.0x (Normal) to 2.5x (Hyper-Paranoid if up 5%+ on account equity)
        paranoia_multiplier = 1.0 + min(1.5, (max(0.0, profit_margin_pct) / 0.05) * 1.5)
        
        # 4. Calculate Dynamic TP Compression
        compression_ratio = max(0.1, min(1.0, (composite_momentum + 2.0) / 4.0)) 
        compressed_tp = price + (tp_distance * compression_ratio) if is_buy else price - (tp_distance * compression_ratio)
        
        critical_exhaustion_threshold = -2.5 / paranoia_multiplier
        
        # 5. EXECUTION TRIGGERS
        if composite_momentum < critical_exhaustion_threshold:
            # Momentum has died violently. Slam the exit immediately.
            return True, 0.0, f"KINETIC_TP_SLAM (Mom: {composite_momentum:.2f} < Thr: {critical_exhaustion_threshold:.2f})", compressed_tp
            
        elif composite_momentum < (critical_exhaustion_threshold * 0.6):
            # Momentum is bleeding out. Scale out 50% immediately and pull the TP down.
            return True, 0.5, f"KINETIC_COMPRESSION_REDUCE (Mom: {composite_momentum:.2f})", compressed_tp
            
        return False, 1.0, "KINETIC_SAFE", compressed_tp


# =====================================================================
# LEVEL 4 & 5: DETERMINISTIC CVAR DEFENDER & EXACT OPTIMIZER
# =====================================================================

class AnalyticalJumpRiskEngine:
    @staticmethod
    def compute_analytical_cvar(sigma_eff: float, p_cont: float, p_ood: float, alpha_95: float = 0.05, alpha_99: float = 0.01) -> Tuple[float, float, float]:
        drift_cont = sigma_eff * 1.5 * (2.0 * p_cont - 1.0)
        jump_prob = float(np.clip(p_ood * 0.15, 0.0, 0.50))

        mean_jump = -4.5 * sigma_eff
        total_ev = ((1.0 - jump_prob) * drift_cont) + (jump_prob * mean_jump)

        var_cont = sigma_eff**2
        var_jump = (1.5 * sigma_eff) ** 2
        total_var = (((1.0 - jump_prob) * (var_cont + drift_cont**2)) + (jump_prob * (var_jump + mean_jump**2)) - (total_ev**2))
        sigma_total = math.sqrt(max(1e-12, total_var))

        z_95 = norm.ppf(alpha_95)
        z_99 = norm.ppf(alpha_99)

        cvar_95 = -(total_ev - (sigma_total * (norm.pdf(z_95) / max(alpha_95, 1e-9))))
        cvar_99 = -(total_ev - (sigma_total * (norm.pdf(z_99) / max(alpha_99, 1e-9))))

        return total_ev, float(cvar_95), float(cvar_99)

class ClosedFormInventoryOptimizer:
    @staticmethod
    def solve_optimal_q(gross_pnl: float, scenario_ev: float, cvar_95: float, cvar_99: float, sigma_eff: float, p_ood: float, price: float, total_qty: float, orderbook: Dict[str, Any], exit_side: str) -> float:
        if total_qty <= 0:
            return 0.0

        depth_key = "bid_depth_10" if exit_side == "SELL" else "ask_depth_10"
        available_depth = max(float(orderbook.get(depth_key, 1.0)), 1e-9)

        gamma = max(0.0001, (20.0 / 10000.0) * (1.0 + (sigma_eff / max(price, 1e-9) * 50.0)) / available_depth)
        risk_penalty = (0.30 * max(0.0, cvar_95)) + (0.50 * max(0.0, cvar_99)) + (p_ood * sigma_eff)

        marginal_edge = scenario_ev - risk_penalty - (0.0005 * price)
        unconstrained_q = 1.0 + (marginal_edge / (2.0 * gamma * total_qty * price))

        return float(np.clip(unconstrained_q, 0.0, 1.0))


# =====================================================================
# THE APEX OMEGA MASTER ORACLE
# =====================================================================

class IntelligentExitEngine:
    
    @staticmethod
    def _extract_thesis(ctx: Dict[str, Any]) -> ThesisVector:
        stat = ctx.get("stat_engine")
        ob = ctx.get("last_ob", {})
        bid = max(float(ob.get("bid_size", 1.0)), 1e-9)
        ask = max(float(ob.get("ask_size", 1.0)), 1e-9)
        depth_ratio = bid / ask if ctx["is_buy"] else ask / bid
        
        return ThesisVector(np.array([
            getattr(stat, "ofi_fast_z", 0.0) if stat else 0.0,
            getattr(stat, "hawkes_z", 0.0) if stat else 0.0,
            getattr(stat, "meso_momentum_z", 0.0) if stat else 0.0,
            depth_ratio, 
            getattr(stat, "inst_variance", 1e-6) if stat else 1e-6
        ]))

    @staticmethod
    def evaluate(ctx: Dict[str, Any], state: PositionExitState) -> ExitDecision:
        if state.actual_qty <= 0.0 and state.base_qty > 0.0:
            state.actual_qty = state.base_qty

        if state.execution_state == "SYNC":
            return ExitDecision("HOLD", state.q_retained, "NONE", 0.0, 0.0, 0.0, "AWAITING_EXCHANGE_SYNC", "[LEVEL 0: SYNC REQUIRED]")

        is_buy = ctx["is_buy"]
        price = float(ctx.get("safe_c_price", state.entry_price))
        total_qty = state.actual_qty  
        
        if total_qty <= 0:
            return ExitDecision("HOLD", 0.0, "NONE", price, 0.0, 0.0, "ZERO_POSITION", "[ZERO EXPOSURE]")
            
        stat = ctx.get("stat_engine")
        inst_var = getattr(stat, "inst_variance", 1e-6) if stat else 1e-6
        sigma_eff = price * math.sqrt(max(inst_var, 1e-12)) * 1.5 
        
        gross_pnl = (price - state.entry_price) * total_qty if is_buy else (state.entry_price - price) * total_qty
        current_thesis = IntelligentExitEngine._extract_thesis(ctx)
        
        # LEVEL 1: Portfolio Commander
        pf_override, pf_reason = PortfolioCommander.evaluate(ctx)
        if pf_override:
            return ExitDecision("EMERGENCY", 0.0, "MARKET", price, 0.0, 0.0, pf_reason, "[PORTFOLIO KILL SWITCH]")
            
        # LEVEL 2: Emergency Assassin
        ass_override, ass_reason, ood_prob, hazard_rate = EmergencyAssassin.evaluate(ctx, state, current_thesis)
        if ass_override:
            return ExitDecision("EMERGENCY", 0.0, "MARKET", price, 0.0, 0.0, ass_reason, "[ASSASSIN THESIS DESTRUCTION]")
            
        # LEVEL 3: Profit Governor
        prof_override, prof_target_q, prof_reason = ProfitGovernor.evaluate(ctx, state, ood_prob, hazard_rate)
        
        # LEVEL 3.5: Kinetic TP Compressor (Self-Aware Front-Runner)
        kin_override, kin_target_q, kin_reason, compressed_tp = KineticTPCompressor.evaluate(ctx, state, current_thesis)
        
        # Override the profit governor if Kinetic Compression determines an immediate dump is necessary
        if kin_override and kin_target_q < prof_target_q:
            prof_override = True
            prof_target_q = kin_target_q
            prof_reason = kin_reason
            
        if prof_override:
            urgency = "MARKET" if prof_target_q == 0.0 else "AGGRESSIVE"
            return ExitDecision("EXIT" if prof_target_q == 0.0 else "REDUCE", prof_target_q, urgency, price, 0.0, compressed_tp, prof_reason, f"[VETO TRIGGER: {prof_reason}]")
            
        # LEVEL 4 & 5: Analytical CVaR Defender & Exact q* Optimizer
        ofi_z = current_thesis.features[0]
        aligned_ofi = ofi_z if is_buy else -ofi_z
        p_cont = float(np.clip(0.50 + (aligned_ofi * 0.15), 0.05, 0.95))
        
        ev_fut, cvar_95, cvar_99 = AnalyticalJumpRiskEngine.compute_analytical_cvar(sigma_eff, p_cont, ood_prob)
        
        last_ob = ctx.get("last_ob", {})
        q_opt_raw = ClosedFormInventoryOptimizer.solve_optimal_q(
            gross_pnl, ev_fut, cvar_95, cvar_99, sigma_eff, ood_prob, price, total_qty, last_ob, state.exit_side
        )
        
        optimal_q = min(q_opt_raw, prof_target_q)

        # LEVEL 6: Execution Routing
        if optimal_q < 0.95:
            action = "EXIT" if optimal_q < 0.05 else "REDUCE"
            spread_bps = abs(float(last_ob.get("best_ask", price)) - float(last_ob.get("best_bid", price))) / max(price, 1e-9) * 10000.0
            urgency = "AGGRESSIVE" if spread_bps < 3.0 else "PASSIVE"
        else:
            action = "HOLD"
            urgency = "NONE"
            
        limit_p = float(last_ob.get("best_bid", price)) if state.exit_side == "SELL" else float(last_ob.get("best_ask", price))
        if urgency == "AGGRESSIVE":
            limit_p = limit_p * 0.9995 if state.exit_side == "SELL" else limit_p * 1.0005

        trailing_stop_price = 0.0
        if state.profit_state.locked_pnl > 0 and total_qty > 0:
            if is_buy:
                trailing_stop_price = state.entry_price + (state.profit_state.locked_pnl / total_qty)
            else:
                trailing_stop_price = state.entry_price - (state.profit_state.locked_pnl / total_qty)

        # Kinetic UI Vector Math
        aligned_mom = (current_thesis.features[0] * 0.45) + (current_thesis.features[1] * 0.40) + (current_thesis.features[2] * 0.15)
        if not is_buy: aligned_mom = -aligned_mom

        log_str = (
            f"\n╔══════════════════════════════════════╗\n"
            f"║ APEX OMEGA V15: PROFIT DEFENDER      ║\n"
            f"╠══════════════════════════════════════╣\n"
            f"║ Profit State             {state.profit_state.state_id:<12}║\n"
            f"║ Peak PnL                ${state.profit_state.peak_pnl:+.2f}       ║\n"
            f"║ PnL Velocity (dpnl/dt)  ${state.profit_state.pnl_velocity:+.3f}/s    ║\n"
            f"║ Kinetic Stress Vector    {aligned_mom:+.2f}          ║\n"
            f"║ OOD Anomaly Shift        {ood_prob:.3f}       ║\n"
            f"║ True CVaR95             {cvar_95/max(sigma_eff, 1e-9):+.2f}σ        ║\n"
            f"╠══════════════════════════════════════╣\n"
            f"║ OPTIMAL q*               {optimal_q:.3f}       ║\n"
            f"║ ACTUAL POS               {total_qty:.4f}       ║\n"
            f"║ ACTION                   {action:<10}  ║\n"
            f"║ URGENCY                  {urgency:<10}  ║\n"
            f"╚══════════════════════════════════════╝\n"
        )
        
        return ExitDecision(action, optimal_q, urgency, limit_p, trailing_stop_price, compressed_tp, f"Q_OPT_{optimal_q:.2f}", log_str)

# =====================================================================
# 6. STATEFUL EXECUTION GOVERNOR FSM
# =====================================================================

class ExecutionGovernorFSM:
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
            res = await executor.safe_call(executor.client.place_order, category="linear", symbol=symbol, side=state.exit_side, orderType="Market", qty=str(qty_to_close), timeInForce="IOC", reduceOnly=True)
            state.execution_state = "SYNC"
            return True
                
        return False