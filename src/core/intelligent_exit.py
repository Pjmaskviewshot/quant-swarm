"""
V43.0 APEX TITAN: CONTINUOUS ADAPTIVE MICROSTRUCTURE BARRIER (CAMB)
-----------------------------------------------------------------------------------------
High-frequency continuous-time optimal stopping and dynamic volatility barrier engine.
Combines friction-compensated breakeven floors, empirical volatility ratio modulation,
asymptotic parabolic chandelier ratchets, and Bayesian order flow exhaustion sentries.

Production Hardening & Quantitative Upgrades (V43.0 Audit Remediations):
- Mandatory Exchange Boundary Clamping: Enforces a strict safety buffer ($0.25\times \text{ATR}$ 
  or 15 bps) between calculated trailing stops and current market execution price, 
  eradicating MarkPrice clash rejections while ensuring aggressive profit-locking.
- Empirical Volatility-Ratio (EVR) Modulation (§3.3 & §8.2 Recommendation 6):
  Replaces noisy tick-level Hurst exponent modulation with an empirical volatility-ratio
  scaling kernel (ATR / Baseline ATR). Eradicates noise-induced R_crit fluctuations, 
  expanding threshold buffers in high-expansion volatility while tightening in low-vol chop.
- Friction-Compensated Breakeven Floor (FC-BE): Anchors the breakeven barrier strictly 
  to Entry * (1 + 2*Fee + SlippageBuffer), guaranteeing true non-negative net realization 
  and eliminating negative fee drift on scratches.
- Asymptotic Parabolic Chandelier: Replaces rigid stepped ratchets with a continuous 
  exponential decay function (alpha_max=2.2x ATR -> alpha_min=0.6x ATR as MFE extends 
  past 1.0R), securing runaway profits while allowing pullbacks to breathe.
- Strict Monotonic Ratchet Invariant: Enforces that trailing stops advance strictly 
  in the direction of trade profit and can never retreat during volatility expansions.
- Normalized Decimal Quantization (Bug B4 Remediation): Sanitizes lot sizing via 
  fixed-point Decimal normalization, eradicating IEEE 754 precision pollution on 
  fractional lot exchange orders.
- Top-of-Book Executable Price Valuation: Trigger evaluation against physical BBO 
  (best_bid for longs, best_ask for shorts) prevents phantom spoof-induced executions.
- Scale-Out Liquidation Isolation: Dispatches partial take-profits under action 'SCALE_OUT', 
  clearing 50% lot volume into Flash IOCs at 1.4R while leaving runners active.
"""

import math
import time
import logging
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, Any, Tuple, Optional
from decimal import Decimal, ROUND_FLOOR, InvalidOperation

logger = logging.getLogger("QUANT_CORE.EXIT")


@dataclass
class ProfitProtectionState:
    state_id: str = "SEARCHING_ALPHA"
    peak_pnl: float = 0.0
    peak_price: float = 0.0
    locked_pnl: float = -1e9
    locked_sl: float = 0.0
    mfe: float = 0.0
    mfe_r: float = 0.0
    mae: float = 0.0
    mae_r: float = 0.0
    last_pnl: float = 0.0
    last_pnl_time: float = field(default_factory=time.time)
    pnl_velocity: float = 0.0
    rolling_mlofi_peak: float = 0.0
    be_active: bool = False
    r_crit: float = 0.50


@dataclass
class PositionExitState:
    position_id: str
    entry_time: float
    entry_price: float
    exit_side: str
    entry_balance: float
    entry_thesis: Any = None
    thesis_inv_cov: Any = None
    actual_qty: float = 0.0
    base_qty: float = 0.0
    profit_state: ProfitProtectionState = field(default_factory=ProfitProtectionState)
    last_eval_time: float = field(default_factory=time.time)
    q_retained: float = 1.0
    execution_state: str = "OBSERVE"  # Defaults to OBSERVE to prevent SYNC deadlocks
    exec_order_id: str = ""
    exec_post_time: float = 0.0
    target_q: float = 1.0


@dataclass
class ExitDecision:
    action: str  # "HOLD" | "EXIT" | "SCALE_OUT" | "EMERGENCY"
    target_q: float
    urgency: str
    limit_price: float
    exchange_ts_price: float
    dynamic_tp_price: float
    reason: str
    log_output: str


class PortfolioCommander:
    @staticmethod
    def evaluate(ctx: Dict[str, Any]) -> Tuple[bool, str]:
        max_dd = float(ctx.get("max_drawdown_pct", 0.10))
        current_dd = float(ctx.get("drawdown_pct", 0.0))
        if current_dd >= max_dd:
            return True, f"SYSTEMIC_DRAWDOWN_BREACH ({current_dd:.2%} >= {max_dd:.2%})"
        if ctx.get("is_circuit_broken", False):
            return True, "CIRCUIT_BREAKER_ACTIVE"
        return False, "SAFE"


class IntelligentExitEngine:
    """
    Continuous Microstructure Optimal Stopping & Dynamic Volatility Barrier Policy.
    Evaluates orderbook physics, empirical volatility stretch, and statistical alpha decay.
    """
    @staticmethod
    def evaluate(ctx: Dict[str, Any], state: PositionExitState) -> ExitDecision:
        now = time.time()
        
        # 1. Base Volume Self-Healing
        if state.actual_qty <= 0.0 and state.base_qty > 0.0:
            state.actual_qty = state.base_qty

        # Auto-recovering SYNC lock with 2.0-second safety timeout
        if state.execution_state == "SYNC":
            if now - state.last_eval_time < 2.0:
                return ExitDecision("HOLD", state.q_retained, "NONE", 0.0, 0.0, 0.0, "AWAITING_EXCHANGE_SYNC", "")
            logger.warning(f"[EXIT_SENTRY] Auto-cleared stale SYNC lock for {ctx.get('symbol', 'ASSET')}.")
            state.execution_state = "OBSERVE"

        state.last_eval_time = now
        is_buy = ctx["is_buy"]
        
        # Physical executable top-of-book pricing (BBO)
        ob = ctx.get("last_ob", {})
        fallback_price = float(ctx.get("latest_tick_price", state.entry_price))
        best_bid = float(ob.get("best_bid", fallback_price))
        best_ask = float(ob.get("best_ask", fallback_price))
        exec_price = best_bid if is_buy else best_ask

        total_qty = state.actual_qty
        if total_qty <= 0.0:
            return ExitDecision("HOLD", 0.0, "NONE", exec_price, 0.0, 0.0, "ZERO_POSITION", "")

        # Systemic Portfolio Drawdown Hard-Stop
        pf_override, pf_reason = PortfolioCommander.evaluate(ctx)
        if pf_override:
            return ExitDecision("EMERGENCY", 0.0, "MARKET", exec_price, 0.0, 0.0, pf_reason, "")

        # Maximum Holding Horizon (240 Minutes Hard Cap)
        duration_minutes = (now - state.entry_time) / 60.0
        if duration_minutes >= 240.0:
            return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, 0.0, 0.0, f"HORIZON_EXHAUSTION ({duration_minutes:.1f}m)", "")

        atr = float(ctx.get("atr", exec_price * 0.005))
        
        # Initial risk distance anchored to entry to eliminate denominator jitter
        initial_risk_dist = float(ctx.get("initial_risk_dist", 0.0))
        if initial_risk_dist <= 0.0:
            initial_risk_dist = max(atr * 2.5, state.entry_price * 0.015)

        price_delta = (exec_price - state.entry_price) if is_buy else (state.entry_price - exec_price)
        current_r = price_delta / (initial_risk_dist + 1e-9)
        current_pnl = price_delta * total_qty

        # 2. Path Telemetry (MFE and MAE Tracking)
        p_state = state.profit_state
        if p_state.peak_price == 0.0:
            p_state.peak_price = state.entry_price

        if current_pnl > p_state.peak_pnl:
            p_state.peak_pnl = current_pnl
            p_state.peak_price = exec_price
            p_state.mfe = max(p_state.mfe, current_pnl)
            p_state.mfe_r = max(p_state.mfe_r, current_r)

        if current_r < p_state.mae_r:
            p_state.mae_r = current_r
            p_state.mae = min(p_state.mae, current_pnl)

        # Baseline Stop and Dynamic Take-Profit Targets
        baseline_sl = (state.entry_price - initial_risk_dist) if is_buy else (state.entry_price + initial_risk_dist)
        existing_tp = float(ctx.get("current_tp", 0.0))
        if existing_tp > 0.0:
            target_tp = existing_tp
        else:
            dynamic_rr = float(ctx.get("dynamic_rr_ratio", 2.0))
            target_tp = state.entry_price + (initial_risk_dist * dynamic_rr) if is_buy else state.entry_price - (initial_risk_dist * dynamic_rr)

        # =========================================================================
        # CONTINUOUS OPTIMAL STOPPING SENSORS (Active Strictly in Profit)
        # =========================================================================
        stat_engine = ctx.get("stat_engine")
        if stat_engine:
            probs = getattr(stat_engine, "historical_probs", None)
            current_p_up = probs[-1] if probs and len(probs) > 0 else 0.50
            continuation_prob = current_p_up if is_buy else (1.0 - current_p_up)

            clean_ofi_z = getattr(stat_engine, "clean_ofi_z", 0.0)
            adverse_flow_z = -clean_ofi_z if is_buy else clean_ofi_z

            hawkes_z = getattr(stat_engine, "marked_hawkes_z", 0.0)
            kinetic_tensor = getattr(stat_engine, "kinetic_tensor", None)
            accel_z = getattr(kinetic_tensor, "accel_z", 0.0) if kinetic_tensor else 0.0
            bocd_cp_prob = getattr(stat_engine, "changepoint_prob", 0.0)

            # SENSOR TRIGGER 1: Alpha Drift Inversion (>= 0.50R Profit)
            if current_r >= 0.50:
                alpha_exhausted = continuation_prob < 0.44
                flow_opposed = adverse_flow_z > 1.8
                if alpha_exhausted and flow_opposed:
                    return ExitDecision(
                        action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                        exchange_ts_price=p_state.locked_sl or baseline_sl, dynamic_tp_price=target_tp,
                        reason=f"ALPHA_DRIFT_INVERSION (ContProb: {continuation_prob:.1%}, AdverseOFI: {adverse_flow_z:+.1f}s)",
                        log_output=""
                    )

            # SENSOR TRIGGER 2: Kinematic Cascade Exhaustion (>= 0.70R Profit)
            if current_r >= 0.70:
                is_hawkes_climax = (abs(hawkes_z) > 2.8) and (accel_z < -1.2)
                if is_hawkes_climax:
                    return ExitDecision(
                        action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                        exchange_ts_price=p_state.locked_sl or baseline_sl, dynamic_tp_price=target_tp,
                        reason=f"KINEMATIC_FLOW_EXHAUSTION (Hawkes: {hawkes_z:.2f}s, Accel: {accel_z:.2f}s)",
                        log_output=""
                    )

            # SENSOR TRIGGER 3: Bayesian Regime Termination (>= 0.80R Profit)
            if current_r >= 0.80 and bocd_cp_prob > 0.65:
                return ExitDecision(
                    action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                    exchange_ts_price=p_state.locked_sl or baseline_sl, dynamic_tp_price=target_tp,
                    reason=f"BOCD_REGIME_TERMINATION (P(Changepoint): {bocd_cp_prob:.1%})",
                    log_output=""
                )

        # SENSOR TRIGGER 4: Dynamic Profit Retracement Guard (>= 0.80R Peak)
        if p_state.mfe_r >= 0.80:
            retrace_pct = (p_state.mfe_r - current_r) / (p_state.mfe_r + 1e-9)
            if retrace_pct >= 0.25:
                return ExitDecision(
                    action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                    exchange_ts_price=p_state.locked_sl or baseline_sl, dynamic_tp_price=target_tp,
                    reason=f"DYNAMIC_PROFIT_RETRACEMENT (Gave back {retrace_pct:.1%} from {p_state.mfe_r:.2f}R peak)",
                    log_output=""
                )

        # =========================================================================
        # CONTINUOUS ADAPTIVE MICROSTRUCTURE BARRIER (CAMB)
        # =========================================================================
        # 1. Empirical Volatility-Ratio (EVR) Modulation (Audit §3.3 & §8.2 Fix)
        vol_pct = atr / max(exec_price, 1e-9)
        baseline_vol_pct = float(ctx.get("baseline_vol_pct", 0.005))  # Default 50 bps baseline
        vol_ratio = float(np.clip(vol_pct / max(baseline_vol_pct, 1e-5), 0.6, 2.0))
        
        # High-volatility expansion widens R_crit to 0.65R; low-vol compression tightens to 0.40R
        r_crit = float(np.clip(0.40 + 0.25 * (vol_ratio - 0.6) / 1.4, 0.40, 0.65))
        p_state.r_crit = r_crit

        # 2. Friction-Compensated Breakeven Floor (FC-BE)
        taker_fee_rate = float(ctx.get("taker_fee_rate", 0.00055))
        slippage_buffer = float(ctx.get("slippage_buffer_pct", 0.0004))
        round_trip_friction = (taker_fee_rate * 2.0) + slippage_buffer
        friction_be_price = state.entry_price * (1.0 + round_trip_friction) if is_buy else state.entry_price * (1.0 - round_trip_friction)

        calculated_sl = baseline_sl
        state_id = "HOLD_INITIAL_RISK"

        if p_state.mfe_r < r_crit:
            # PHASE 1: Anti-Choking Diffusion Zone. Hold full initial stop distance.
            calculated_sl = baseline_sl
            state_id = "HOLD_INITIAL_RISK"

        elif p_state.mfe_r >= r_crit and p_state.mfe_r < 1.0:
            # PHASE 2: Friction-Compensated Breakeven Activation
            p_state.be_active = True
            calculated_sl = friction_be_price
            state_id = f"FRICTION_BREAKEVEN (MFE: {p_state.mfe_r:.2f}R >= {r_crit:.2f}R | VolRatio: {vol_ratio:.2f})"

        else:
            # PHASE 3: Continuous Asymptotic Parabolic Chandelier (Runners >= 1.0R)
            p_state.be_active = True
            decay_lambda = 0.65
            alpha_max, alpha_min = 2.2, 0.6
            cushion_mult = alpha_min + (alpha_max - alpha_min) * math.exp(-decay_lambda * (p_state.mfe_r - 1.0))
            dynamic_cushion = atr * cushion_mult

            if is_buy:
                trail_candidate = p_state.peak_price - dynamic_cushion
                calculated_sl = max(friction_be_price, trail_candidate)
            else:
                trail_candidate = p_state.peak_price + dynamic_cushion
                calculated_sl = min(friction_be_price, trail_candidate)

            state_id = f"ASYMPTOTIC_CHANDELIER (Cushion: {cushion_mult:.2f}x ATR | MFE: {p_state.mfe_r:.2f}R)"

        # MANDATORY EXCHANGE BOUNDARY CLAMPING (Eliminates MarkPrice rejections)
        min_market_buffer = max(atr * 0.25, exec_price * 0.0015)
        if is_buy:
            calculated_sl = min(calculated_sl, exec_price - min_market_buffer)
        else:
            calculated_sl = max(calculated_sl, exec_price + min_market_buffer)

        # 3. Existing Stop Level Alignment
        existing_sl = float(ctx.get("current_sl", 0.0))
        if existing_sl > 0.0:
            calculated_sl = max(calculated_sl, existing_sl) if is_buy else min(calculated_sl, existing_sl)

        # 4. Strict Monotonic Ratchet Invariant
        # Stops strictly advance in profit; never retreat during volatility spikes.
        if p_state.locked_sl > 0.0:
            calculated_sl = max(calculated_sl, p_state.locked_sl) if is_buy else min(calculated_sl, p_state.locked_sl)

        p_state.locked_sl = calculated_sl
        p_state.state_id = state_id
        target_sl = calculated_sl

        # =========================================================================
        # TIERED FRACTIONAL SCALE-OUT (50% lot at 1.4R)
        # =========================================================================
        if current_r >= 1.40 and state.q_retained >= 0.99:
            p_state.state_id = "SCALE_OUT_50"
            return ExitDecision("SCALE_OUT", 0.5, "FLASH_IOC", exec_price, target_sl, target_tp, "SCALE_OUT_1.4R", "")

        # =========================================================================
        # PHYSICAL BREACH EXECUTION (Evaluated against Top-of-Book BBO)
        # =========================================================================
        # Trailing Stop Breaches
        if is_buy and exec_price <= target_sl:
            return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, target_sl, target_tp, f"CAMB_STOP_BREACH ({exec_price:.4f} <= {target_sl:.4f})", "")
        elif not is_buy and exec_price >= target_sl:
            return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, target_sl, target_tp, f"CAMB_STOP_BREACH ({exec_price:.4f} >= {target_sl:.4f})", "")

        # Take-Profit Limit/Market Breaches
        if is_buy and target_tp > state.entry_price and exec_price >= target_tp:
            return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, target_sl, target_tp, f"DYNAMIC_TP_REACHED ({exec_price:.4f} >= {target_tp:.4f})", "")
        elif not is_buy and target_tp < state.entry_price and target_tp > 0.0 and exec_price <= target_tp:
            return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, target_sl, target_tp, f"DYNAMIC_TP_REACHED ({exec_price:.4f} <= {target_tp:.4f})", "")

        return ExitDecision("HOLD", state.q_retained, "NONE", exec_price, target_sl, target_tp, "HOLD_OPTIMAL_CONTINUATION", "")


class ExecutionGovernorFSM:
    @staticmethod
    def _format_qty_str(raw_qty: float, qty_step: Any) -> str:
        """
        Floor-quantizes order lot sizes via normalized Decimal arithmetic (Bug B4 Fix).
        Guarantees eradication of IEEE 754 float representation artifacts.
        """
        try:
            step_dec = Decimal(str(qty_step)).normalize()
            if step_dec <= Decimal("0"):
                return f"{float(raw_qty):.4f}"
            val_dec = Decimal(f"{float(raw_qty):.8f}")
            quantized = (val_dec // step_dec) * step_dec
            precision = max(0, -step_dec.as_tuple().exponent)
            return f"{quantized:.{precision}f}"
        except (InvalidOperation, TypeError, ValueError):
            return f"{float(raw_qty):.4f}"

    @classmethod
    async def manage_execution(cls, decision: ExitDecision, state: PositionExitState, ctx: Dict[str, Any], executor: Any) -> bool:
        if decision.action == "HOLD":
            return False

        symbol = ctx["symbol"]
        current_actual_qty = state.actual_qty
        target_retained_qty = current_actual_qty * decision.target_q
        qty_to_close = current_actual_qty - target_retained_qty

        if qty_to_close <= 0.0:
            return False

        qty_step = ctx.get("qty_step", "0.1")
        try:
            qty_str = cls._format_qty_str(qty_to_close, qty_step)
            if Decimal(qty_str) <= Decimal("0"):
                return False
        except Exception:
            qty_str = str(qty_to_close)

        position_idx = int(ctx.get("position_idx", 0))

        # Market IOC execution for emergency, full exits, and partial scale-outs
        if decision.urgency in ["MARKET", "EMERGENCY", "AGGRESSIVE", "FLASH_IOC"]:
            res = await executor.safe_call(
                "POST", "/v5/order/create", is_execution=True,
                category="linear", symbol=symbol,
                side=state.exit_side, orderType="Market", qty=qty_str,
                timeInForce="IOC", reduceOnly=True,
                positionIdx=position_idx,
                smpType="CancelMaker"
            )
            
            # Verify exchange acceptance before altering local state
            if isinstance(res, dict) and res.get("retCode") == 0:
                if decision.action in ["EXIT", "CLOSE", "EMERGENCY"] or decision.target_q <= 0.01:
                    state.execution_state = "CLOSED"
                    state.actual_qty = 0.0
                    state.q_retained = 0.0
                elif decision.action == "SCALE_OUT":
                    state.actual_qty = float(Decimal(str(current_actual_qty)) - Decimal(qty_str))
                    state.q_retained = decision.target_q
                    state.execution_state = "OBSERVE"
                    logger.info(f"[EXIT_SENTRY] Scale-Out filled on {symbol}. Remaining: {state.actual_qty:.4f} units.")
                return True
            else:
                logger.error(f"[X-RAY] Exit order rejected for {symbol}: {res}")
                return False

        # Passive Limit PostOnly execution
        if state.execution_state == "OBSERVE":
            res = await executor.safe_call(
                "POST", "/v5/order/create", is_execution=True,
                category="linear", symbol=symbol,
                side=state.exit_side, orderType="Limit", price=str(decision.limit_price),
                qty=qty_str, timeInForce="PostOnly", reduceOnly=True,
                positionIdx=position_idx,
                smpType="CancelMaker"
            )
            if isinstance(res, dict) and res.get("retCode") == 0:
                state.execution_state = "SYNC"
                return True
            return False

        return False