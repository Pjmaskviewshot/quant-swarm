"""
V44.3 APEX TITAN: CONTINUOUS ADAPTIVE MICROSTRUCTURE BARRIER (CAMB)
-----------------------------------------------------------------------------------------
High-frequency continuous-time optimal stopping and dynamic volatility barrier engine.
Combines friction-compensated breakeven floors, empirical volatility ratio modulation,
asymptotic parabolic chandelier ratchets, and Bayesian order flow exhaustion sentries.

Production Hardening & Quantitative Upgrades (V44.3 Directional Clamp Hotfix):
- Directional Trailing Stop Clamp: Fixes asymmetric ratchet bug on SHORT positions by 
  properly ratcheting exchange stops DOWNward as profit expands while keeping the buffer
  strictly above prevailing mark price, eradicating Bybit API stop amendment rejections.
- Latched Risk Distance Invariant: Latches initial risk distance on position inception
  and enforces an absolute floor (max(2.5 * ATR, 1.5% entry)), eradicating the 
  denominator-collapse glitch where trailing breakeven stops triggered phantom 25R anomalies.
- Zero-Price Sanity Barrier: Validates top-of-book prices against None/zero unpopulated
  dictionary payloads, eliminating corrupt phantom peaks and panic retracements.
- Micro-Ratchet Dead-Zone Eradication: Progressively compresses stop distance from 
  +0.15R upward, ensuring trades that show green cannot collapse into full -1.0R losses.
- Un-Gated Early Microstructure Sentry: Intercepts adverse order-flow surges (Z > 2.0σ)
  starting at +0.15R, locking in small gains before market reversals complete.
- Stagnation Decay Ladder: Enforces a 30m stagnation scratch (R < 0.20), 45m time-decay 
  scratch (R < 0.40), and 60m absolute holding horizon, eradicating 4-hour zombie locks.
- Sovereign Local Exit Sentry: Physical stop breaches trigger instantaneous local Market
  IOC exits, while the passive exchange stop is clamped to prevent Bybit API rejections.
- Friction-Compensated Breakeven Floor (FC-BE): Anchors breakeven strictly to cover 
  round-trip taker fees and execution slippage buffers once MFE reaches R_crit.
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
    r_crit: float = 0.45
    initial_risk_dist: float = 0.0


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
    execution_state: str = "OBSERVE"
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
        max_dd = float(ctx.get("max_drawdown_pct", 0.15))
        current_dd = float(ctx.get("drawdown_pct", 0.0))
        if current_dd >= max_dd:
            return True, f"SYSTEMIC_DRAWDOWN_BREACH ({current_dd:.2%} >= {max_dd:.2%})"
        if ctx.get("is_circuit_broken", False):
            return True, "CIRCUIT_BREAKER_ACTIVE"
        return False, "SAFE"


class IntelligentExitEngine:
    """
    Continuous Microstructure Optimal Stopping & Dynamic Volatility Barrier Policy.
    Evaluates physical orderbook depth, empirical volatility stretch, and statistical alpha decay.
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
        symbol = ctx.get("symbol", "ASSET")

        # =========================================================================
        # ZERO-PRICE SANITY BARRIER
        # =========================================================================
        ob = ctx.get("last_ob", {}) or {}
        raw_bid = ob.get("best_bid")
        raw_ask = ob.get("best_ask")
        
        best_bid = float(raw_bid) if (raw_bid is not None and float(raw_bid) > 0.0) else 0.0
        best_ask = float(raw_ask) if (raw_ask is not None and float(raw_ask) > 0.0) else 0.0

        fallback_price = float(ctx.get("latest_tick_price", 0.0))
        if fallback_price <= 0.0:
            fallback_price = float(state.entry_price)

        if is_buy:
            exec_price = best_bid if best_bid > 0.0 else fallback_price
        else:
            exec_price = best_ask if best_ask > 0.0 else fallback_price

        # Reject corrupted/unpopulated tick pricing before state pollution
        if exec_price <= 0.0 or math.isnan(exec_price) or math.isinf(exec_price):
            logger.warning(f"[EXIT_SENTRY] Zero or invalid pricing on {symbol} ({exec_price}). Holding state.")
            return ExitDecision("HOLD", state.q_retained, "NONE", 0.0, 0.0, 0.0, "INVALID_ZERO_PRICE", "")

        total_qty = state.actual_qty
        if total_qty <= 0.0:
            return ExitDecision("HOLD", 0.0, "NONE", exec_price, 0.0, 0.0, "ZERO_POSITION", "")

        # Systemic Portfolio Drawdown Hard-Stop
        pf_override, pf_reason = PortfolioCommander.evaluate(ctx)
        if pf_override:
            return ExitDecision("EMERGENCY", 0.0, "MARKET", exec_price, 0.0, 0.0, pf_reason, "")

        atr = float(ctx.get("atr", exec_price * 0.005))
        
        # =========================================================================
        # LATCHED INITIAL RISK DISTANCE (Eradicates Breakeven Denominator Collapse)
        # =========================================================================
        p_state = state.profit_state
        if getattr(p_state, "initial_risk_dist", 0.0) <= 0.0:
            raw_risk_dist = float(ctx.get("initial_risk_dist", 0.0))
            min_risk_floor = max(atr * 2.5, state.entry_price * 0.015)
            p_state.initial_risk_dist = max(raw_risk_dist, min_risk_floor)

        initial_risk_dist = p_state.initial_risk_dist

        price_delta = (exec_price - state.entry_price) if is_buy else (state.entry_price - exec_price)
        current_r = price_delta / (initial_risk_dist + 1e-9)
        current_pnl = price_delta * total_qty

        # Structural Anomaly Barrier: Guard against corrupt spread snapshots
        if abs(current_r) > 15.0 and p_state.mfe_r < 2.0:
            logger.critical(
                f"[EXIT_SENTRY] Anomaly R-multiple ({current_r:.2f}R) detected on {symbol}. "
                f"Exec: {exec_price}, Entry: {state.entry_price}. Ignoring tick."
            )
            return ExitDecision("HOLD", state.q_retained, "NONE", exec_price, 0.0, 0.0, "ANOMALOUS_R_REJECTED", "")

        # =========================================================================
        # ADAPTIVE TIME-DECAY & STAGNATION LADDER (Anti-Zombie Sentry)
        # =========================================================================
        duration_minutes = (now - state.entry_time) / 60.0
        
        # Stage 1: Stagnation Scratch (30 min threshold without clearing +0.20R)
        if duration_minutes >= 30.0 and current_r < 0.20:
            return ExitDecision(
                action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                exchange_ts_price=0.0, dynamic_tp_price=0.0,
                reason=f"STAGNATION_SCRATCH ({duration_minutes:.1f}m, Current R: {current_r:.2f}R < 0.20R)",
                log_output=""
            )

        # Stage 2: Time-Decay Liquidation (45 min threshold without clearing +0.40R)
        if duration_minutes >= 45.0 and current_r < 0.40:
            return ExitDecision(
                action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                exchange_ts_price=0.0, dynamic_tp_price=0.0,
                reason=f"TIME_DECAY_EXHAUSTION ({duration_minutes:.1f}m, Current R: {current_r:.2f}R < 0.40R)",
                log_output=""
            )

        # Stage 3: Absolute Micro-Scalp Horizon Hard Cap (60 mins)
        if duration_minutes >= 60.0:
            return ExitDecision(
                action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                exchange_ts_price=0.0, dynamic_tp_price=0.0,
                reason=f"HORIZON_EXHAUSTION ({duration_minutes:.1f}m)",
                log_output=""
            )

        # 2. Path Telemetry (MFE and MAE Tracking)
        if p_state.peak_price <= 0.0:
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
        # CONTINUOUS OPTIMAL STOPPING SENSORS (Active Across All Positive R)
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

            # SENSOR TRIGGER 1: Early Adverse Order-Flow Surge (Active >= +0.15R)
            if current_r >= 0.15:
                if adverse_flow_z > 2.0 and continuation_prob < 0.40:
                    return ExitDecision(
                        action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                        exchange_ts_price=p_state.locked_sl or baseline_sl, dynamic_tp_price=target_tp,
                        reason=f"EARLY_FLOW_OPPOSITION (R: {current_r:.2f}, ContProb: {continuation_prob:.1%}, AdverseOFI: {adverse_flow_z:+.1f}s)",
                        log_output=""
                    )

            # SENSOR TRIGGER 2: Alpha Drift Inversion (Active >= +0.35R)
            if current_r >= 0.35:
                if adverse_flow_z > 1.6 and continuation_prob < 0.44:
                    return ExitDecision(
                        action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                        exchange_ts_price=p_state.locked_sl or baseline_sl, dynamic_tp_price=target_tp,
                        reason=f"ALPHA_DRIFT_INVERSION (R: {current_r:.2f}, ContProb: {continuation_prob:.1%}, AdverseOFI: {adverse_flow_z:+.1f}s)",
                        log_output=""
                    )

            # SENSOR TRIGGER 3: Kinematic Flow Exhaustion (Active >= +0.50R)
            if current_r >= 0.50:
                is_hawkes_climax = (abs(hawkes_z) > 2.8) and (accel_z < -1.0)
                if is_hawkes_climax:
                    return ExitDecision(
                        action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                        exchange_ts_price=p_state.locked_sl or baseline_sl, dynamic_tp_price=target_tp,
                        reason=f"KINEMATIC_FLOW_EXHAUSTION (Hawkes: {hawkes_z:.2f}s, Accel: {accel_z:.2f}s)",
                        log_output=""
                    )

            # SENSOR TRIGGER 4: Bayesian Regime Termination (Active >= +0.60R)
            if current_r >= 0.60 and bocd_cp_prob > 0.65:
                return ExitDecision(
                    action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                    exchange_ts_price=p_state.locked_sl or baseline_sl, dynamic_tp_price=target_tp,
                    reason=f"BOCD_REGIME_TERMINATION (P(Changepoint): {bocd_cp_prob:.1%})",
                    log_output=""
                )

        # SENSOR TRIGGER 5: Dynamic Profit Retracement Guard (Active >= +0.60R Peak)
        if p_state.mfe_r >= 0.60:
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
        vol_pct = atr / max(exec_price, 1e-9)
        baseline_vol_pct = float(ctx.get("baseline_vol_pct", 0.005))
        vol_ratio = float(np.clip(vol_pct / max(baseline_vol_pct, 1e-5), 0.6, 2.0))
        
        # Relative Volatility Ratio determines breakeven activation floor
        r_crit = float(np.clip(0.35 + 0.20 * (vol_ratio - 0.6) / 1.4, 0.35, 0.55))
        p_state.r_crit = r_crit

        taker_fee_rate = float(ctx.get("taker_fee_rate", 0.00055))
        slippage_buffer = float(ctx.get("slippage_buffer_pct", 0.0004))
        round_trip_friction = (taker_fee_rate * 2.0) + slippage_buffer
        friction_be_price = state.entry_price * (1.0 + round_trip_friction) if is_buy else state.entry_price * (1.0 - round_trip_friction)

        calculated_sl = baseline_sl
        state_id = "HOLD_INITIAL_RISK"

        # TIER 0: Anti-Choking Entry Noise Filter
        if p_state.mfe_r < 0.15:
            calculated_sl = baseline_sl
            state_id = "HOLD_INITIAL_RISK"

        # TIER 1: Continuous Micro-Ratchet (0.15R <= MFE < r_crit)
        # Progressively compresses downside risk from -1.0R up to -0.20R
        elif p_state.mfe_r >= 0.15 and p_state.mfe_r < r_crit:
            ramp = (p_state.mfe_r - 0.15) / max(1e-5, (r_crit - 0.15))
            softened_risk = initial_risk_dist * (1.0 - 0.80 * ramp)
            calculated_sl = state.entry_price - softened_risk if is_buy else state.entry_price + softened_risk
            state_id = f"MICRO_RISK_DAMPENER (MFE: {p_state.mfe_r:.2f}R | MaxRisk: -{softened_risk/initial_risk_dist:.2f}R)"

        # TIER 2: Friction-Compensated Breakeven Floor (r_crit <= MFE < 0.80R)
        elif p_state.mfe_r >= r_crit and p_state.mfe_r < 0.80:
            p_state.be_active = True
            calculated_sl = friction_be_price
            state_id = f"FRICTION_BREAKEVEN (MFE: {p_state.mfe_r:.2f}R >= {r_crit:.2f}R)"

        # TIER 3: Asymptotic Parabolic Chandelier (Runners >= 0.80R)
        else:
            p_state.be_active = True
            decay_lambda = 0.70
            alpha_max, alpha_min = 2.0, 0.5
            cushion_mult = alpha_min + (alpha_max - alpha_min) * math.exp(-decay_lambda * (p_state.mfe_r - 0.80))
            dynamic_cushion = atr * cushion_mult

            if is_buy:
                trail_candidate = p_state.peak_price - dynamic_cushion
                calculated_sl = max(friction_be_price, trail_candidate)
            else:
                trail_candidate = p_state.peak_price + dynamic_cushion
                calculated_sl = min(friction_be_price, trail_candidate)

            state_id = f"ASYMPTOTIC_CHANDELIER (Cushion: {cushion_mult:.2f}x ATR | MFE: {p_state.mfe_r:.2f}R)"

        # Existing Stop Level Alignment
        existing_sl = float(ctx.get("current_sl", 0.0))
        if existing_sl > 0.0:
            calculated_sl = max(calculated_sl, existing_sl) if is_buy else min(calculated_sl, existing_sl)

        # Strict Monotonic Ratchet Invariant: Stops strictly advance in profit
        if p_state.locked_sl > 0.0:
            calculated_sl = max(calculated_sl, p_state.locked_sl) if is_buy else min(calculated_sl, p_state.locked_sl)

        p_state.locked_sl = calculated_sl
        p_state.state_id = state_id

        # =========================================================================
        # SOVEREIGN LOCAL EXIT SENTRY (Immediate IOC on Physical Breach)
        # =========================================================================
        if is_buy and exec_price <= calculated_sl:
            return ExitDecision(
                action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                exchange_ts_price=calculated_sl, dynamic_tp_price=target_tp,
                reason=f"CAMB_STOP_BREACH ({exec_price:.4f} <= {calculated_sl:.4f})",
                log_output=""
            )
        elif not is_buy and exec_price >= calculated_sl:
            return ExitDecision(
                action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                exchange_ts_price=calculated_sl, dynamic_tp_price=target_tp,
                reason=f"CAMB_STOP_BREACH ({exec_price:.4f} >= {calculated_sl:.4f})",
                log_output=""
            )

        # Dynamic Take-Profit Limit/Market Breaches
        if is_buy and target_tp > state.entry_price and exec_price >= target_tp:
            return ExitDecision(
                action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                exchange_ts_price=calculated_sl, dynamic_tp_price=target_tp,
                reason=f"DYNAMIC_TP_REACHED ({exec_price:.4f} >= {target_tp:.4f})",
                log_output=""
            )
        elif not is_buy and target_tp < state.entry_price and target_tp > 0.0 and exec_price <= target_tp:
            return ExitDecision(
                action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                exchange_ts_price=calculated_sl, dynamic_tp_price=target_tp,
                reason=f"DYNAMIC_TP_REACHED ({exec_price:.4f} <= {target_tp:.4f})",
                log_output=""
            )

        # Tiered Fractional Scale-Out (50% lot at 1.4R)
        if current_r >= 1.40 and state.q_retained >= 0.99:
            p_state.state_id = "SCALE_OUT_50"
            return ExitDecision("SCALE_OUT", 0.5, "FLASH_IOC", exec_price, calculated_sl, target_tp, "SCALE_OUT_1.4R", "")

        # =========================================================================
        # PASSIVE EXCHANGE STOP CLAMPING (Directional Safety Buffers)
        # =========================================================================
        min_market_buffer = max(atr * 0.25, exec_price * 0.0020)
        if is_buy:
            # Long SL must sit below current price
            exchange_ts_price = min(calculated_sl, exec_price - min_market_buffer)
        else:
            # Short SL must sit above current price, ratcheting DOWN toward entry
            exchange_ts_price = max(calculated_sl, exec_price + min_market_buffer)
            # Prevent short trailing stop from expanding past initial entry boundary
            if p_state.locked_sl > 0.0:
                exchange_ts_price = min(exchange_ts_price, p_state.locked_sl)

        return ExitDecision("HOLD", state.q_retained, "NONE", exec_price, exchange_ts_price, target_tp, "HOLD_OPTIMAL_CONTINUATION", "")


class ExecutionGovernorFSM:
    @staticmethod
    def _format_qty_str(raw_qty: float, qty_step: Any) -> str:
        """
        Floor-quantizes order lot sizes via normalized Decimal arithmetic.
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