"""
💎 V20.0 APEX NEURAL-QUANTUM: TENSOR-FUSED OPTIMAL-STOPPING ENGINE
-----------------------------------------------------------------------------------------
Continuous-Time, Non-Stationary, Predictive Optimal-Stopping Matrix.

Hierarchy:
- LEVEL 0: Physical Exchange Synchronization & Reconciliation Guard
- LEVEL 1: Portfolio Variance & Cross-Asset Contagion Kill-Switch
- LEVEL 2: Dynamic Spread-Elastic Volatility Shield
- LEVEL 2.5: Hawkes Cascade Inversion & MLOFI Divergence
- LEVEL 3: Dual-Normalized Scale-Invariant Profit Ladder
- LEVEL 3.5: Tensor-Fused Information-Theoretic Alpha Decay (TF-ITAD V20.0)
- LEVEL 4: Closed-Form Merton Jump-Diffusion Analytical CVaR
- LEVEL 5: Adverse-Selection Adjusted HJB Continuous Inventory Optimizer
- LEVEL 6: Multi-Channel Asynchronous Execution Governor FSM
"""

import math
import time
import logging
import numpy as np
from scipy.stats import chi2, norm
from dataclasses import dataclass, field
from typing import Dict, Any, Tuple, List

logger = logging.getLogger("QUANT_CORE.V20_APEX_EXIT")

# =====================================================================
# 1. STATE AND THESIS DATA STRUCTURES
# =====================================================================

@dataclass
class ThesisVector:
    features: np.ndarray  # [OFI_z, Hawkes_z, Meso_z, Depth_Ratio, Inst_Variance]
    
    def mahalanobis_distance(self, other: 'ThesisVector', inv_cov_matrix: np.ndarray) -> float:
        delta = self.features - other.features
        try:
            distance_sq = float(np.dot(np.dot(delta.T, inv_cov_matrix), delta))
            return math.sqrt(max(0.0, distance_sq))
        except Exception:
            return 0.0

@dataclass
class ProfitProtectionState:
    state_id: str = "UNPROFITABLE" 
    peak_pnl: float = 0.0
    peak_price: float = 0.0
    locked_pnl: float = -1e9
    mfe: float = 0.0
    mae: float = 0.0
    last_pnl: float = 0.0
    last_pnl_time: float = field(default_factory=time.time)
    pnl_velocity: float = 0.0
    rolling_mlofi_peak: float = 0.0

@dataclass
class PositionExitState:
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
    exchange_ts_price: float
    dynamic_tp_price: float
    reason: str
    log_output: str

# =====================================================================
# LEVEL 1: PORTFOLIO VARIANCE & CROSS-ASSET COMMANDER
# =====================================================================

class PortfolioCommander:
    @staticmethod
    def evaluate(ctx: Dict[str, Any]) -> Tuple[bool, str]:
        portfolio_dd = float(ctx.get("drawdown_pct", 0.0))
        live_count = int(ctx.get("active_positions_count", 1))
        
        if portfolio_dd > 0.08:
            return True, f"SYSTEMIC_DRAWDOWN_BREACH ({portfolio_dd * 100:.1f}%)"
        if portfolio_dd > 0.04 and live_count > 4:
            return True, f"PORTFOLIO_CONTAGION_REDUCTION (DD: {portfolio_dd * 100:.1f}% | {live_count} Pos)"
            
        return False, "SAFE"

# =====================================================================
# LEVEL 2 & 2.5: DYNAMIC SPREAD-ELASTIC SHIELD & HAWKES INVERSION
# =====================================================================

class QuantumMicrostructurePredictor:
    @staticmethod
    def evaluate(
        ctx: Dict[str, Any], 
        state: PositionExitState, 
        current_thesis: ThesisVector, 
        ev_score_norm: float,
        sigma_eff: float
    ) -> Tuple[bool, float, str, float]:
        price = float(ctx.get("safe_c_price", state.entry_price))
        is_buy = ctx["is_buy"]
        qty = state.actual_qty
        
        if qty <= 0:
            return False, 1.0, "SAFE", 0.0
            
        current_pnl = (price - state.entry_price) * qty if is_buy else (state.entry_price - price) * qty
        
        # Spread-Elastic Volatility Shield
        spread_cost = float(ctx.get("payload_features", {}).get("bid_ask_spread", 0.0005))
        elastic_ev_threshold = 0.08 + (spread_cost * 100.0) # Requires higher EV to hold through wide spreads
        
        if ev_score_norm >= elastic_ev_threshold:
            return False, 1.0, "ELASTIC_EV_SHIELD_ACTIVE", 0.0

        ofi_z = float(current_thesis.features[0])
        hawkes_z = float(current_thesis.features[1])
        depth_ratio = float(current_thesis.features[3])
        
        aligned_ofi = ofi_z if is_buy else -ofi_z
        aligned_hawkes = hawkes_z if is_buy else -hawkes_z
        
        p_state = state.profit_state
        if aligned_ofi > p_state.rolling_mlofi_peak:
            p_state.rolling_mlofi_peak = aligned_ofi

        # Dynamic Hawkes Threshold based on Spread
        hawkes_trigger = -2.20 - (spread_cost * 500.0)
        if aligned_hawkes < hawkes_trigger and depth_ratio < 0.40:
            return True, 0.0, f"DYNAMIC_HAWKES_INVERSION ({aligned_hawkes:.2f}σ < {hawkes_trigger:.2f}σ)", price

        ofi_divergence = p_state.rolling_mlofi_peak - aligned_ofi
        if current_pnl > (state.entry_balance * 0.015) and ofi_divergence > 2.50 and depth_ratio < 0.45:
            return True, 0.0, f"MLOFI_EXHAUSTION_DIVERGENCE (Div: {ofi_divergence:.2f}σ)", price

        d_m = state.entry_thesis.mahalanobis_distance(current_thesis, state.thesis_inv_cov)
        try:
            ood_prob = float(np.clip(1.0 - chi2.cdf(d_m**2, df=5), 0.0, 1.0))
        except Exception:
            ood_prob = 0.5
            
        if ood_prob > 0.95 and current_pnl < 0 and ev_score_norm < -0.10:
            return True, 0.0, f"MAHALANOBIS_OOD_DESTRUCTION (Prob: {ood_prob:.2f})", price

        return False, 1.0, "SAFE", ood_prob

# =====================================================================
# LEVEL 3: DUAL-NORMALIZED SCALE-INVARIANT PROFIT LADDER
# =====================================================================

class ScaleInvariantProfitGovernor:
    @staticmethod
    def evaluate(ctx: Dict[str, Any], state: PositionExitState, ood_prob: float) -> Tuple[bool, float, str]:
        p_state = state.profit_state
        is_buy = ctx["is_buy"]
        price = float(ctx.get("safe_c_price", state.entry_price))
        qty = state.actual_qty 
        
        if qty <= 0:
            return False, 1.0, "SAFE"
            
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
        notional = price * qty
        atr = float(ctx.get("atr", price * 0.01))
        atr_pct = atr / max(price, 1e-9)
        
        fee_hurdle = notional * 0.0012
        base_unit = max(equity * 0.015, notional * atr_pct * 1.2)

        t1_threshold = base_unit * 0.75
        t2_threshold = base_unit * 1.75
        t3_threshold = base_unit * 3.00
        parabolic_threshold = base_unit * 4.50

        if p_state.peak_pnl >= parabolic_threshold:
            p_state.state_id = "PARABOLIC_TRAIL"
            p_state.locked_pnl = max(p_state.locked_pnl, p_state.peak_pnl * 0.85)
        elif p_state.peak_pnl >= t3_threshold:
            p_state.state_id = "PROFIT_LOCKED_TIER3"
            p_state.locked_pnl = max(p_state.locked_pnl, t3_threshold * 0.75) 
        elif p_state.peak_pnl >= t2_threshold:
            p_state.state_id = "PROFIT_LOCKED_TIER2"
            p_state.locked_pnl = max(p_state.locked_pnl, t2_threshold * 0.60)
        elif p_state.peak_pnl >= t1_threshold:
            p_state.state_id = "PROFIT_ARMED_TIER1"
            p_state.locked_pnl = max(p_state.locked_pnl, max(fee_hurdle * 1.5, t1_threshold * 0.40))
        elif current_pnl > fee_hurdle:
            p_state.state_id = "PROFIT_FORMING"
        else:
            p_state.state_id = "UNPROFITABLE"

        if p_state.locked_pnl > 0 and current_pnl <= p_state.locked_pnl:
            return True, 0.0, f"LOCKED_PROFIT_TRIGGERED (PnL: ${current_pnl:.3f} <= Lock: ${p_state.locked_pnl:.3f})"

        if p_state.peak_pnl >= t2_threshold and p_state.pnl_velocity < -(base_unit * 0.4) and current_pnl <= (p_state.peak_pnl - base_unit * 0.75) and ood_prob > 0.65:
            return True, 0.0, f"STRUCTURAL_VELOCITY_BLEED (Peak: ${p_state.peak_pnl:.3f})"

        return False, 1.0, "PROFIT_SAFE"

# =====================================================================
# LEVEL 3.5: TENSOR-FUSED INFORMATION-THEORETIC ALPHA DECAY (V20.0)
# =====================================================================

class TensorFusedAlphaDecay:
    """
    🚀 V20.0 NEURAL-QUANTUM ENGINE
    Features Cross-Asset Contagion Time-Warps and Fast Binned Volume Profiles (True HVN).
    """
    @staticmethod
    def _compute_true_hvn(price: float, atr: float, feature_engine: Any) -> float:
        """Fast Binned Volume Profile to find Point of Control (POC) density."""
        if not feature_engine or len(feature_engine.prices) < 100:
            return 1.0 # Neutral multiplier
            
        try:
            recent_prices = np.array(list(feature_engine.prices)[-300:])
            recent_vols = np.array(list(feature_engine.volumes)[-300:])
            
            # Create 20 dynamic price bins
            bins = np.linspace(np.min(recent_prices), np.max(recent_prices), 20)
            digitized = np.digitize(recent_prices, bins)
            
            vol_profile = np.bincount(digitized, weights=recent_vols, minlength=len(bins)+1)
            poc_bin = np.argmax(vol_profile)
            
            current_bin = np.digitize([price], bins)[0]
            
            # If current price is in or adjacent to the Point of Control
            if abs(current_bin - poc_bin) <= 1:
                return 1.45 # Massive tolerance, we are inside the HVN fortress
            elif vol_profile[current_bin] < (np.mean(vol_profile) * 0.5):
                return 0.85 # Low Volume Node (LVN) - thin ice, highly vulnerable
                
            return 1.0
        except Exception:
            return 1.0

    @staticmethod
    def evaluate(ctx: Dict[str, Any], state: PositionExitState, current_thesis: ThesisVector) -> Tuple[bool, float, str, float, float]:
        p_state = state.profit_state
        price = float(ctx.get("safe_c_price", state.entry_price))
        is_buy = ctx["is_buy"]

        ofi_z = float(current_thesis.features[0])
        hawkes_z = float(current_thesis.features[1])
        
        stat = ctx.get("stat_engine")
        feature_engine = ctx.get("feature_engine") # Passed from auction/lifecycle loop
        
        current_variance = getattr(stat, "inst_variance", 1e-6) if stat else 1e-6
        entry_variance = float(state.entry_thesis.features[4])
        
        # 1. 🌍 CONTEXT CALIBRATION: Liquidity & HVN
        vol_ratio = float(ctx.get("vol_mult", 1.0))
        lambda_t = float(np.clip(vol_ratio, 0.60, 1.80))
        
        atr = float(ctx.get("atr", price * 0.01))
        hvn_multiplier = TensorFusedAlphaDecay._compute_true_hvn(price, atr, feature_engine)

        # 2. ⚛️ MARKET PHYSICS: STRUCTURAL ABSORPTION
        aligned_force = (ofi_z * 0.6) + (hawkes_z * 0.4) if is_buy else -(ofi_z * 0.6) - (hawkes_z * 0.4)
        price_distance_dir = (price - state.entry_price) / state.entry_price if is_buy else (state.entry_price - price) / state.entry_price
        
        spread_cost = float(ctx.get("payload_features", {}).get("bid_ask_spread", 0.0005))
        var_scale = min(0.75, current_variance / max(entry_variance, 1e-9))
        
        # Spread-Elastic Force Threshold: Wider spreads require vastly more force to trigger a cut
        dynamic_force_threshold = (1.5 + (spread_cost * 200.0)) * lambda_t * (1.0 + var_scale) * hvn_multiplier

        if aligned_force > dynamic_force_threshold and price_distance_dir <= 0.0 and p_state.peak_pnl < (state.entry_balance * 0.005):
            return True, 0.0, f"EMPIRICAL_ABSORPTION (Force: {aligned_force:.2f}σ > Limit: {dynamic_force_threshold:.2f}σ)", price, price

        # 3. 🕸️ CROSS-ASSET CONTAGION TIME-WARP
        # If macro flows (e.g. BTC) breakdown, warp time forward.
        tensor_alpha = float(ctx.get("payload_features", {}).get("tensor_alpha", 0.0))
        contagion_multiplier = 1.0
        if is_buy and tensor_alpha < -0.3:
            contagion_multiplier = 1.5 + abs(tensor_alpha)
        elif not is_buy and tensor_alpha > 0.3:
            contagion_multiplier = 1.5 + abs(tensor_alpha)

        # 4. ⏳ FRACTAL STOCHASTIC CLOCK
        wall_clock_mins = (time.time() - state.entry_time) / 60.0
        info_velocity = (current_variance / max(entry_variance, 1e-9)) * contagion_multiplier
        effective_info_time = wall_clock_mins * info_velocity
        
        kaufman_er = getattr(stat, "kaufman_er", 0.5) if stat else 0.5
        base_horizon = 35.0 + (35.0 * (1.0 - kaufman_er))  # 35T to 70T
        dynamic_horizon = base_horizon * lambda_t * hvn_multiplier

        if effective_info_time > dynamic_horizon and p_state.peak_pnl <= (state.entry_balance * 0.002):
            return True, 0.0, f"FRACTAL_HORIZON_EXHAUSTED (Vol-Time: {effective_info_time:.1f}T > {dynamic_horizon:.1f}T)", price, price

        # 5. 📉 DYNAMIC TP COMPRESSION
        tp_distance = abs(price - state.entry_price) * 1.5
        compression_ratio = max(0.10, 1.0 - (effective_info_time / dynamic_horizon))
        compressed_tp = price + (tp_distance * compression_ratio) if is_buy else price - (tp_distance * compression_ratio)

        # 6. 🛡️ AUTO-BREAKEVEN DRAG (Contagion Adjusted)
        breakeven_sl = 0.0
        if p_state.peak_pnl > (state.entry_balance * 0.003) and aligned_force < (-1.0 * lambda_t * contagion_multiplier):
            fee_buffer = state.entry_price * 0.0015
            breakeven_sl = (state.entry_price + fee_buffer) if is_buy else (state.entry_price - fee_buffer)

        return False, 1.0, "INFO_SAFE", compressed_tp, breakeven_sl

# =====================================================================
# LEVEL 4 & 5: MERTON CVAR & ADVERSE-SELECTION HJB OPTIMIZER
# =====================================================================

class AnalyticalJumpRiskEngine:
    @staticmethod
    def compute_analytical_cvar(sigma_eff: float, p_cont: float, p_ood: float, alpha_95: float = 0.05, alpha_99: float = 0.01) -> Tuple[float, float, float]:
        drift_cont = sigma_eff * 1.5 * (2.0 * p_cont - 1.0)
        jump_prob = float(np.clip(p_ood * 0.15, 0.0, 0.50))
        mean_jump = -4.5 * sigma_eff
        total_ev = ((1.0 - jump_prob) * drift_cont) + (jump_prob * mean_jump)

        var_cont = sigma_eff**2
        var_jump = (1.5 * sigma_eff)**2
        total_var = (((1.0 - jump_prob) * (var_cont + drift_cont**2)) + (jump_prob * (var_jump + mean_jump**2)) - (total_ev**2))
        sigma_total = math.sqrt(max(1e-12, total_var))

        z_95 = norm.ppf(alpha_95) * 1.15
        z_99 = norm.ppf(alpha_99) * 1.25

        cvar_95 = -(total_ev - (sigma_total * (norm.pdf(z_95) / max(alpha_95, 1e-9))))
        cvar_99 = -(total_ev - (sigma_total * (norm.pdf(z_99) / max(alpha_99, 1e-9))))

        return total_ev, float(cvar_95), float(cvar_99)

class ContinuousHJBInventoryOptimizer:
    @staticmethod
    def solve_optimal_q(
        gross_pnl: float, 
        ev_fut: float, 
        cvar_95: float, 
        cvar_99: float, 
        sigma_eff: float, 
        p_ood: float, 
        price: float, 
        total_qty: float, 
        orderbook: Dict[str, Any], 
        exit_side: str,
        drawdown_pct: float,
        ofi_z: float # 🚀 V20.0 Adverse Selection Parameter
    ) -> float:
        if total_qty <= 0: return 0.0

        depth_key = "bid_depth_10" if exit_side == "SELL" else "ask_depth_10"
        available_depth = max(float(orderbook.get(depth_key, 1.0)), 1e-9)

        gamma_base = 0.0020
        gamma_t = gamma_base * (1.0 + 4.0 * (drawdown_pct**2))
        
        # 🚀 V20.0 Adverse Selection Hazard Penalty
        # If OFI is heavily against us, getting filled is a BAD sign (Adverse Selection).
        # We increase the hazard rate (kappa) virtually to suppress the reservation price edge.
        adverse_flow_penalty = max(0.0, -ofi_z * 0.5) if exit_side == "SELL" else max(0.0, ofi_z * 0.5)
        
        kappa = available_depth / (total_qty + 1e-9)
        safe_kappa = max(kappa, 0.05) + adverse_flow_penalty
        
        fill_hazard_boost = (1.0 / gamma_t) * math.log(1.0 + (gamma_t / safe_kappa))
        risk_penalty = (0.25 * max(0.0, cvar_95)) + (0.45 * max(0.0, cvar_99)) + (p_ood * sigma_eff)
        
        marginal_reservation_edge = (ev_fut + fill_hazard_boost) - risk_penalty - (0.0004 * price)

        unconstrained_q = 1.0 + (marginal_reservation_edge / (2.0 * gamma_t * total_qty * price + 1e-9))
        return float(np.clip(unconstrained_q, 0.0, 1.0))

# =====================================================================
# MASTER ORACLE DISPATCHER
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
            return ExitDecision("HOLD", state.q_retained, "NONE", 0.0, 0.0, 0.0, "AWAITING_EXCHANGE_SYNC", "")

        is_buy = ctx["is_buy"]
        price = float(ctx.get("safe_c_price", state.entry_price))
        total_qty = state.actual_qty  
        
        if total_qty <= 0:
            return ExitDecision("HOLD", 0.0, "NONE", price, 0.0, 0.0, "ZERO_POSITION", "")
            
        stat = ctx.get("stat_engine")
        inst_var = getattr(stat, "inst_variance", 1e-6) if stat else 1e-6
        sigma_eff = price * math.sqrt(max(inst_var, 1e-12)) * 1.5 
        gross_pnl = (price - state.entry_price) * total_qty if is_buy else (state.entry_price - price) * total_qty
        current_thesis = IntelligentExitEngine._extract_thesis(ctx)
        
        # LEVEL 1: Portfolio Contagion Check
        pf_override, pf_reason = PortfolioCommander.evaluate(ctx)
        if pf_override:
            return ExitDecision("EMERGENCY", 0.0, "MARKET", price, 0.0, 0.0, pf_reason, "")

        # LEVEL 4/5 Analytical Base
        ofi_z = float(current_thesis.features[0])
        aligned_ofi = ofi_z if is_buy else -ofi_z
        p_cont = float(np.clip(0.50 + (aligned_ofi * 0.15), 0.05, 0.95))
        
        d_m_for_ev = state.entry_thesis.mahalanobis_distance(current_thesis, state.thesis_inv_cov)
        try:
            ood_prob_base = float(np.clip(1.0 - chi2.cdf(d_m_for_ev**2, df=5), 0.0, 1.0))
        except Exception:
            ood_prob_base = 0.5
        
        ev_fut, cvar_95, cvar_99 = AnalyticalJumpRiskEngine.compute_analytical_cvar(sigma_eff, p_cont, ood_prob_base)
        ev_score_norm = ev_fut / max(sigma_eff, 1e-9)

        # LEVEL 2 & 2.5: Dynamic Spread-Elastic Shield & Hawkes Cascade Inversion
        micro_override, micro_target_q, micro_reason, micro_price = QuantumMicrostructurePredictor.evaluate(
            ctx, state, current_thesis, ev_score_norm, sigma_eff
        )
        if micro_override:
            urgency = "MARKET" if micro_target_q == 0.0 else "AGGRESSIVE"
            return ExitDecision(
                "EXIT" if micro_target_q == 0.0 else "REDUCE",
                micro_target_q, urgency, price, 0.0, 0.0,
                micro_reason, ""
            )

        # LEVEL 3: Scale-Invariant Stepped Profit Governor
        prof_override, prof_target_q, prof_reason = ScaleInvariantProfitGovernor.evaluate(ctx, state, ood_prob_base)
        
        # 🚀 LEVEL 3.5: V20.0 Tensor-Fused Alpha Decay
        info_override, info_target_q, info_reason, compressed_tp, dynamic_sl = TensorFusedAlphaDecay.evaluate(ctx, state, current_thesis)
        
        if info_override and info_target_q < prof_target_q:
            prof_override = True
            prof_target_q = info_target_q
            prof_reason = info_reason
            
        if prof_override:
            urgency = "MARKET" if prof_target_q == 0.0 else "AGGRESSIVE"
            return ExitDecision(
                "EXIT" if prof_target_q == 0.0 else "REDUCE",
                prof_target_q, urgency, price, 0.0, compressed_tp,
                prof_reason, ""
            )
            
        # LEVEL 5: Adverse-Selection Adjusted HJB Continuous Inventory Optimizer
        last_ob = ctx.get("last_ob", {})
        drawdown_pct = float(ctx.get("drawdown_pct", 0.0))
        q_opt_raw = ContinuousHJBInventoryOptimizer.solve_optimal_q(
            gross_pnl, ev_fut, cvar_95, cvar_99, sigma_eff, ood_prob_base, 
            price, total_qty, last_ob, state.exit_side, drawdown_pct, aligned_ofi
        )
        
        optimal_q = min(q_opt_raw, prof_target_q)

        # LEVEL 6: Asynchronous Multi-Channel Execution Routing
        if optimal_q < 0.95:
            action = "EXIT" if optimal_q < 0.05 else "REDUCE"
            urgency = "AGGRESSIVE"
        else:
            action = "HOLD"
            urgency = "NONE"
            
        limit_p = float(last_ob.get("best_bid", price)) if state.exit_side == "SELL" else float(last_ob.get("best_ask", price))

        trailing_stop_price = dynamic_sl 
        if state.profit_state.locked_pnl > 0 and total_qty > 0 and trailing_stop_price == 0.0:
            trailing_stop_price = (
                state.entry_price + (state.profit_state.locked_pnl / total_qty) 
                if is_buy else state.entry_price - (state.profit_state.locked_pnl / total_qty)
            )

        return ExitDecision(action, optimal_q, urgency, limit_p, trailing_stop_price, compressed_tp, f"HJB_Q_{optimal_q:.2f}", "")

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

        try:
            qty_step = ctx.get("qty_step", 0.1)
            precision = max(0, abs(int(math.floor(math.log10(qty_step)))))
            qty_str = f"{qty_to_close:.{precision}f}"
        except Exception:
            qty_str = str(qty_to_close)
            
        if decision.urgency in ["MARKET", "EMERGENCY", "AGGRESSIVE"]:
            res = await executor.safe_call(
                executor.client.place_order, category="linear", symbol=symbol,
                side=state.exit_side, orderType="Market", qty=qty_str,
                timeInForce="IOC", reduceOnly=True
            )
            state.execution_state = "SYNC"
            return True

        if state.execution_state == "OBSERVE":
            res = await executor.safe_call(
                executor.client.place_order, category="linear", symbol=symbol,
                side=state.exit_side, orderType="Limit", price=str(decision.limit_price),
                qty=qty_str, timeInForce="PostOnly", reduceOnly=True
            )
            state.execution_state = "SYNC"
            return True

        return False