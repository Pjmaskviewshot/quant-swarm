"""
V50.0 APEX TITAN: SMART PREDATOR CONTINUOUS AWAKENING MICROSTRUCTURE BARRIER (CAMB)
-----------------------------------------------------------------------------------------
High-frequency continuous-time optimal stopping and dynamic volatility barrier engine.
Combines friction-compensated breakeven floors, empirical volatility ratio modulation,
asymptotic parabolic chandelier ratchets, and Bayesian order flow exhaustion sentries.

Production Hardening & Quantitative Upgrades (V50.0 Audit Resolutions):
- Price-Capped Limit IOC Exits: Replaces unconstrained Market IOC exit orders with 
  strict Limit IOC collars (max 15 bps slippage bound), eliminating destructive 
  book-wiping slippage spikes (e.g., -57.2 bps ETH fills).
- Dual-Basis Mark/Last Safety Clamping: Clamps server-side stops against MarkPrice and
  executable Top-of-Book, terminating Bybit Error 34036/110043 amendment rejections.
- Micro-Account Notional Scale-Out Guard: Converts partial exits (< $15.00) into deferred
  full exits (>= 1.60R) to eliminate exchange order rejection loops.
- Monotonic Ratchet Guarantee: Prevents volatility-induced stop degradation across both legs.
- Strict Decimal Quantization: Floors exit order lot sizes to exchange step sizes cleanly.
"""

import math
import time
import logging
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, Any, Tuple, Optional
from decimal import Decimal, InvalidOperation

logger = logging.getLogger("QUANT_CORE.EXIT")


@dataclass(frozen=True)
class ExitPolicyConfig:
    """
    How far a winner is allowed to run relative to how far a loser is allowed
    to run. Every distance is in R, where 1R = the initial stop distance.

    WHY THIS EXISTS. The live session of Sept 2026 closed 27 trades at a 66.7%
    win rate and still lost money: the average loss was 3.60x the average win,
    so the system needed 78.3% winners just to break even. The largest win of
    the session was about 0.5R; no trade came near the 2R target.

    The legacy ladder produced that shape by construction. It moved the stop to
    breakeven at ~+0.24R (a 0.36% move against a 1.5% stop) and closed any trade
    that gave back 22% of a +0.5R peak -- about 0.1R, ordinary one-minute noise
    on the alts traded. Winners were capped near 0.5R; losers ran to 1R and past.

    Measured with research/exit_lab.py, which drives THIS function over simulated
    paths with identical entries per policy (reports/live/exit_policy_*.json):
      * on pure noise the legacy ladder reproduces the live account's profile
        (about -13 bps/trade vs -12 live)
      * with a persistent trend present it LOSES money (-8.6 bps) where the
        runner policy below earns +12.0 bps (paired diff +20.6 bps, t = 7.5)
      * across 12 out-of-sample cells it is never significantly worse, and is
        significantly better wherever the edge persists beyond ~an hour

    What exits cannot do: create edge. On a driftless market every stopping
    rule has the same expectancy, and costs make it negative. The runner policy
    does not make a no-edge signal profitable; it stops discarding the edge a
    real momentum signal has, which lives in the trades that keep running.
    """
    legacy: bool = False                 # True reproduces the pre-2026-09 ladder exactly
    be_trigger_r: float = 0.80            # stop -> breakeven + costs only after +1R earned
    trail_start_r: float = 1.20           # begin trailing once +1.5R has been reached
    trail_distance_r: float = 0.60        # trail sits 1R behind the peak
    min_reward_r: float = 3.0            # take-profit no closer than 3R
    flow_exit_min_r: float = 1.5         # order-flow exits may act only on trades already
                                         # locked above breakeven -- never on noise-sized gains
    stagnation_minutes: float = 90.0     # a trade still losing after 90 min has failed
    stagnation_r: float = 0.35
    horizon_minutes: float = 180.0       # time stop applies only to trades that never
                                         # earned +1R; runners are managed by the trail


EXIT_CONFIG = ExitPolicyConfig()
LEGACY_EXIT_CONFIG = ExitPolicyConfig(legacy=True)


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
    r_crit: float = 0.28
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
        # PRICING SANITY & DUAL-BASIS RESOLUTION
        # =========================================================================
        ob = ctx.get("last_ob", {}) or {}
        raw_bid = ob.get("best_bid")
        raw_ask = ob.get("best_ask")
        
        best_bid = float(raw_bid) if (raw_bid is not None and float(raw_bid) > 0.0) else 0.0
        best_ask = float(raw_ask) if (raw_ask is not None and float(raw_ask) > 0.0) else 0.0

        fallback_price = float(ctx.get("latest_tick_price", 0.0))
        if fallback_price <= 0.0:
            fallback_price = float(state.entry_price)

        exec_price = (best_bid if best_bid > 0.0 else fallback_price) if is_buy else (best_ask if best_ask > 0.0 else fallback_price)
        mark_price = float(ctx.get("mark_price", 0.0) or exec_price)
        if mark_price <= 0.0:
            mark_price = exec_price

        # Reject corrupted/unpopulated tick pricing
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
        # LATCHED INITIAL RISK DISTANCE
        # =========================================================================
        p_state = state.profit_state
        if getattr(p_state, "initial_risk_dist", 0.0) <= 0.0:
            raw_risk_dist = float(ctx.get("initial_risk_dist", 0.0))
            if ctx.get("risk_dist_from_v12"):
                # V12 sized the position and set the exchange stop from measured
                # volatility; R must mean THAT stop, or the 1R/1.5R/3R thresholds
                # and the exchange TP drift apart from what the EV model priced.
                min_risk_floor = state.entry_price * 0.003
            else:
                min_risk_floor = max(atr * 2.5, state.entry_price * 0.010)
            p_state.initial_risk_dist = max(raw_risk_dist, min_risk_floor)

        initial_risk_dist = p_state.initial_risk_dist

        price_delta = (exec_price - state.entry_price) if is_buy else (state.entry_price - exec_price)
        current_r = price_delta / (initial_risk_dist + 1e-9)
        current_pnl = price_delta * total_qty

        # Structural Anomaly Filter
        if abs(current_r) > 15.0 and p_state.mfe_r < 2.0:
            logger.critical(
                f"[EXIT_SENTRY] Anomaly R-multiple ({current_r:.2f}R) detected on {symbol}. "
                f"Exec: {exec_price}, Entry: {state.entry_price}. Ignoring tick."
            )
            return ExitDecision("HOLD", state.q_retained, "NONE", exec_price, 0.0, 0.0, "ANOMALOUS_R_REJECTED", "")

        cfg = ctx.get("exit_config") or EXIT_CONFIG
        if not cfg.legacy:
            return IntelligentExitEngine._evaluate_runner(
                ctx, state, cfg, is_buy=is_buy, exec_price=exec_price,
                mark_price=mark_price, atr=atr, initial_risk_dist=initial_risk_dist,
                current_r=current_r, current_pnl=current_pnl, now=now,
            )

        # =========================================================================
        # ADAPTIVE TIME-DECAY & STAGNATION SENTRY
        # =========================================================================
        duration_minutes = (now - state.entry_time) / 60.0
        
        if duration_minutes >= 90.0 and current_r < -0.35:
            return ExitDecision(
                action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                exchange_ts_price=0.0, dynamic_tp_price=0.0,
                reason=f"ADVERSE_STAGNATION_SCRATCH ({duration_minutes:.1f}m, Current R: {current_r:.2f}R < -0.35R)",
                log_output=""
            )

        if duration_minutes >= 180.0:
            return ExitDecision(
                action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                exchange_ts_price=0.0, dynamic_tp_price=0.0,
                reason=f"HORIZON_EXHAUSTION ({duration_minutes:.1f}m)",
                log_output=""
            )

        # Path Telemetry Tracking
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

        baseline_sl = (state.entry_price - initial_risk_dist) if is_buy else (state.entry_price + initial_risk_dist)
        existing_tp = float(ctx.get("current_tp", 0.0))
        if existing_tp > 0.0:
            target_tp = existing_tp
        else:
            dynamic_rr = float(ctx.get("dynamic_rr_ratio", 2.0))
            target_tp = state.entry_price + (initial_risk_dist * dynamic_rr) if is_buy else state.entry_price - (initial_risk_dist * dynamic_rr)

        # =========================================================================
        # CONTINUOUS OPTIMAL STOPPING SENSORS
        # =========================================================================
        stat_engine = ctx.get("stat_engine")
        if stat_engine:
            # AUDIT B4: `historical_probs` stores max(p_up, 1-p_up) -- a
            # confidence with the sign discarded. Reading it as p_up made
            # continuation_prob >= 0.5 for every long (so the two rules below
            # could NEVER fire on a long) and <= 0.5 for every short (so they
            # fired on almost any adverse tick). Direction now comes from the
            # structured estimate.
            estimate = getattr(stat_engine, "latest_probability", None)
            if estimate is not None:
                continuation_prob = estimate.continuation_prob(is_buy)
            else:
                # No estimate -> indifference. Never reconstruct direction from
                # the legacy scalar; 0.5 disables these rules rather than
                # firing them asymmetrically.
                continuation_prob = 0.50

            clean_ofi_z = getattr(stat_engine, "clean_ofi_z", 0.0)
            adverse_flow_z = -clean_ofi_z if is_buy else clean_ofi_z

            hawkes_z = getattr(stat_engine, "marked_hawkes_z", 0.0)
            kinetic_tensor = getattr(stat_engine, "kinetic_tensor", None)
            accel_z = getattr(kinetic_tensor, "accel_z", 0.0) if kinetic_tensor else 0.0
            bocd_cp_prob = getattr(stat_engine, "changepoint_prob", 0.0)

            if current_r >= 0.25:
                if adverse_flow_z > 2.2 and continuation_prob < 0.38:
                    return ExitDecision(
                        action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                        exchange_ts_price=p_state.locked_sl or baseline_sl, dynamic_tp_price=target_tp,
                        reason=f"EARLY_FLOW_OPPOSITION (R: {current_r:.2f}, ContProb: {continuation_prob:.1%}, AdverseOFI: {adverse_flow_z:+.1f}s)",
                        log_output=""
                    )

            if current_r >= 0.40:
                if adverse_flow_z > 1.8 and continuation_prob < 0.42:
                    return ExitDecision(
                        action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                        exchange_ts_price=p_state.locked_sl or baseline_sl, dynamic_tp_price=target_tp,
                        reason=f"ALPHA_DRIFT_INVERSION (R: {current_r:.2f}, ContProb: {continuation_prob:.1%}, AdverseOFI: {adverse_flow_z:+.1f}s)",
                        log_output=""
                    )

            if current_r >= 0.50:
                is_hawkes_climax = (abs(hawkes_z) > 2.8) and (accel_z < -1.0)
                if is_hawkes_climax:
                    return ExitDecision(
                        action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                        exchange_ts_price=p_state.locked_sl or baseline_sl, dynamic_tp_price=target_tp,
                        reason=f"KINEMATIC_FLOW_EXHAUSTION (Hawkes: {hawkes_z:.2f}s, Accel: {accel_z:.2f}s)",
                        log_output=""
                    )

            if current_r >= 0.60 and bocd_cp_prob > 0.65:
                return ExitDecision(
                    action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                    exchange_ts_price=p_state.locked_sl or baseline_sl, dynamic_tp_price=target_tp,
                    reason=f"BOCD_REGIME_TERMINATION (P(Changepoint): {bocd_cp_prob:.1%})",
                    log_output=""
                )

        if p_state.mfe_r >= 0.70:
            retrace_pct = (p_state.mfe_r - current_r) / (p_state.mfe_r + 1e-9)
            if retrace_pct >= 0.28:
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
        
        r_crit = float(np.clip(0.24 + 0.12 * (vol_ratio - 0.6) / 1.4, 0.22, 0.38))
        p_state.r_crit = r_crit

        taker_fee_rate = float(ctx.get("taker_fee_rate", 0.00055))
        slippage_buffer = float(ctx.get("slippage_buffer_pct", 0.0004))
        round_trip_friction = (taker_fee_rate * 2.0) + slippage_buffer
        friction_be_price = state.entry_price * (1.0 + round_trip_friction) if is_buy else state.entry_price * (1.0 - round_trip_friction)

        calculated_sl = baseline_sl
        state_id = "HOLD_INITIAL_RISK"

        # TIER 0: Noise Band (< 0.10R)
        if p_state.mfe_r < 0.10:
            calculated_sl = baseline_sl
            state_id = "HOLD_INITIAL_RISK"

        # TIER 1: Proportional Risk Compression (0.10R <= MFE < r_crit)
        elif p_state.mfe_r >= 0.10 and p_state.mfe_r < r_crit:
            ramp = (p_state.mfe_r - 0.10) / max(1e-5, (r_crit - 0.10))
            softened_risk = initial_risk_dist * (1.0 - 0.80 * ramp)
            calculated_sl = state.entry_price - softened_risk if is_buy else state.entry_price + softened_risk
            state_id = f"PREDATOR_STALKING (MFE: {p_state.mfe_r:.2f}R)"

        # TIER 2: Breakeven Lock Zone (r_crit <= MFE < 0.70R)
        elif p_state.mfe_r >= r_crit and p_state.mfe_r < 0.70:
            p_state.be_active = True
            calculated_sl = friction_be_price
            state_id = f"PREDATOR_BE_SECURED (MFE: {p_state.mfe_r:.2f}R >= {r_crit:.2f}R)"

        # TIER 3: Adaptive Stalking Chandelier (Runners >= 0.70R)
        else:
            p_state.be_active = True
            decay_lambda = 0.75
            alpha_max, alpha_min = 1.8, 0.4
            cushion_mult = alpha_min + (alpha_max - alpha_min) * math.exp(-decay_lambda * (p_state.mfe_r - 0.70))
            dynamic_cushion = atr * cushion_mult

            if is_buy:
                trail_candidate = p_state.peak_price - dynamic_cushion
                calculated_sl = max(friction_be_price, trail_candidate)
            else:
                trail_candidate = p_state.peak_price + dynamic_cushion
                calculated_sl = min(friction_be_price, trail_candidate)

            state_id = f"PREDATOR_CHANDELIER_LATCHED (Cushion: {cushion_mult:.2f}x ATR)"

        # Monotonicity Enforcement
        existing_sl = float(ctx.get("current_sl", 0.0))
        if existing_sl > 0.0:
            calculated_sl = max(calculated_sl, existing_sl) if is_buy else min(calculated_sl, existing_sl)

        if p_state.locked_sl > 0.0:
            calculated_sl = max(calculated_sl, p_state.locked_sl) if is_buy else min(calculated_sl, p_state.locked_sl)

        p_state.locked_sl = calculated_sl
        p_state.state_id = state_id

        # =========================================================================
        # INSTANT REVERSAL STOP-OUT SENTRY
        # =========================================================================
        if p_state.mfe_r >= 0.50:
            retrace_from_peak = (p_state.mfe_r - current_r) / (p_state.mfe_r + 1e-9)
            if retrace_from_peak >= 0.22:
                return ExitDecision(
                    action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                    exchange_ts_price=calculated_sl, dynamic_tp_price=target_tp,
                    reason=f"PREDATOR_REVERSAL_STRIKE (Gave back {retrace_from_peak:.1%} from {p_state.mfe_r:.2f}R peak)",
                    log_output=""
                )

        # Physical Boundary Breaches
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

        # =========================================================================
        # SCALE-OUT & MICRO-ACCOUNT NOTIONAL GUARD
        # =========================================================================
        if current_r >= 1.30 and state.q_retained >= 0.99:
            child_notional = (state.actual_qty * 0.5) * exec_price
            if child_notional < 15.00:
                if current_r >= 1.60:
                    p_state.state_id = "SCALE_OUT_CONVERTED_EXIT"
                    return ExitDecision(
                        action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                        exchange_ts_price=calculated_sl, dynamic_tp_price=target_tp,
                        reason="FULL_EXIT_DUE_TO_MIN_NOTIONAL", log_output=""
                    )
                # Else: Hold position through the runner
            else:
                p_state.state_id = "SCALE_OUT_50"
                return ExitDecision("SCALE_OUT", 0.5, "FLASH_IOC", exec_price, calculated_sl, target_tp, "SCALE_OUT_1.3R", "")

        # =========================================================================
        # MARK-PRICE EXCHANGE STOP CLAMPING & SLIPPAGE-CAPPED EXIT COLLAR
        # =========================================================================
        min_market_buffer = max(atr * 0.20, mark_price * 0.0020)
        if is_buy:
            reference_boundary = min(exec_price, mark_price)
            exchange_ts_price = min(calculated_sl, reference_boundary - min_market_buffer)
            if p_state.locked_sl > 0.0:
                exchange_ts_price = max(exchange_ts_price, min(p_state.locked_sl, reference_boundary - min_market_buffer))
        else:
            reference_boundary = max(exec_price, mark_price)
            exchange_ts_price = max(calculated_sl, reference_boundary + min_market_buffer)
            if p_state.locked_sl > 0.0:
                clamped_short = max(p_state.locked_sl, reference_boundary + min_market_buffer)
                exchange_ts_price = min(exchange_ts_price, clamped_short)

        return ExitDecision("HOLD", state.q_retained, "NONE", exec_price, exchange_ts_price, target_tp, "HOLD_OPTIMAL_CONTINUATION", "")


    @staticmethod
    def _evaluate_runner(ctx: Dict[str, Any], state: "PositionExitState", cfg: ExitPolicyConfig,
                         *, is_buy: bool, exec_price: float, mark_price: float, atr: float,
                         initial_risk_dist: float, current_r: float, current_pnl: float,
                         now: float) -> "ExitDecision":
        """
        Symmetric-room exit policy. See ExitPolicyConfig for the evidence.

        Stop ladder, in R:
            -1R                         until the trade has earned +be_trigger_r
            +cost_r (a true scratch)    once it has
            peak - trail_distance_r     once it has reached +trail_start_r
        Take-profit at max(min_reward_r, dynamic_rr). The stop only ever moves
        in the trade's favour.
        """
        p_state = state.profit_state
        risk = initial_risk_dist
        entry = state.entry_price
        duration_minutes = (now - state.entry_time) / 60.0

        # ---- path telemetry ----------------------------------------------------
        if p_state.peak_price <= 0.0:
            p_state.peak_price = entry
        if current_pnl > p_state.peak_pnl:
            p_state.peak_pnl = current_pnl
            p_state.peak_price = exec_price
        p_state.mfe = max(p_state.mfe, current_pnl)
        p_state.mfe_r = max(p_state.mfe_r, current_r)
        if current_r < p_state.mae_r:
            p_state.mae_r = current_r
            p_state.mae = min(p_state.mae, current_pnl)

        def _exit(reason: str, sl: float = 0.0) -> "ExitDecision":
            return ExitDecision(action="EXIT", target_q=0.0, urgency="FLASH_IOC",
                                limit_price=exec_price, exchange_ts_price=sl,
                                dynamic_tp_price=target_tp, reason=reason, log_output="")

        rr = max(cfg.min_reward_r, float(ctx.get("dynamic_rr_ratio", 2.0)))
        target_tp = entry + risk * rr if is_buy else entry - risk * rr

        # ---- time exits (never on a trade that has earned its runner status) ----
        if duration_minutes >= cfg.stagnation_minutes and current_r < -cfg.stagnation_r:
            return _exit(f"ADVERSE_STAGNATION_SCRATCH ({duration_minutes:.1f}m, "
                         f"Current R: {current_r:.2f}R < -{cfg.stagnation_r:.2f}R)")
        if duration_minutes >= cfg.horizon_minutes and p_state.mfe_r < cfg.be_trigger_r - 1e-6:
            return _exit(f"HORIZON_EXHAUSTION ({duration_minutes:.1f}m, never reached "
                         f"+{cfg.be_trigger_r:.1f}R)")

        # ---- stop ladder ---------------------------------------------------------
        taker = float(ctx.get("taker_fee_rate", 0.00055))
        slip = float(ctx.get("slippage_buffer_pct", 0.0004))
        cost_r = (entry * (2.0 * taker + slip)) / max(risk, 1e-12)

        stop_r = -1.0
        state_id = "HOLD_INITIAL_RISK"
        # current_r divides by (risk + 1e-9), so a trade at exactly +1R reads
        # 0.9999999993R. A small tolerance keeps "reached 1R" meaning 1R.
        eps = 1e-6
        if p_state.mfe_r >= cfg.be_trigger_r - eps:
            stop_r = max(stop_r, cost_r)
            p_state.be_active = True
            state_id = f"RUNNER_BE_SECURED (MFE {p_state.mfe_r:.2f}R)"
        if p_state.mfe_r >= cfg.trail_start_r - eps:
            stop_r = max(stop_r, p_state.mfe_r - cfg.trail_distance_r)
            state_id = f"RUNNER_TRAILING (MFE {p_state.mfe_r:.2f}R, stop {stop_r:+.2f}R)"

        calculated_sl = entry + stop_r * risk if is_buy else entry - stop_r * risk

        existing_sl = float(ctx.get("current_sl", 0.0))
        if existing_sl > 0.0:
            calculated_sl = max(calculated_sl, existing_sl) if is_buy else min(calculated_sl, existing_sl)
        if p_state.locked_sl > 0.0:
            calculated_sl = max(calculated_sl, p_state.locked_sl) if is_buy else min(calculated_sl, p_state.locked_sl)
        p_state.locked_sl = calculated_sl
        p_state.state_id = state_id

        # ---- informed early exits: only on trades already locked in profit -------
        stat_engine = ctx.get("stat_engine")
        if stat_engine is not None and current_r >= cfg.flow_exit_min_r:
            estimate = getattr(stat_engine, "latest_probability", None)
            cont = estimate.continuation_prob(is_buy) if estimate is not None else 0.50
            ofi = getattr(stat_engine, "clean_ofi_z", 0.0)
            adverse = -ofi if is_buy else ofi
            if adverse > 2.2 and cont < 0.38:
                return _exit(f"FLOW_REVERSAL_AFTER_PROFIT (R: {current_r:.2f}, "
                             f"ContProb: {cont:.1%}, AdverseOFI: {adverse:+.1f}s)", calculated_sl)

        # ---- boundaries ----------------------------------------------------------
        if (is_buy and exec_price <= calculated_sl) or (not is_buy and exec_price >= calculated_sl):
            tag = "RUNNER_TRAIL_STOP" if stop_r > 0 else "CAMB_STOP_BREACH"
            return _exit(f"{tag} ({exec_price:.4f} vs {calculated_sl:.4f}, stop {stop_r:+.2f}R)",
                         calculated_sl)
        if (is_buy and exec_price >= target_tp) or (not is_buy and exec_price <= target_tp):
            return _exit(f"DYNAMIC_TP_REACHED ({exec_price:.4f}, {rr:.1f}R)", calculated_sl)

        # ---- exchange-native stop, kept clear of the market ----------------------
        min_market_buffer = max(atr * 0.20, mark_price * 0.0020)
        if is_buy:
            ref = min(exec_price, mark_price)
            exchange_ts_price = min(calculated_sl, ref - min_market_buffer)
        else:
            ref = max(exec_price, mark_price)
            exchange_ts_price = max(calculated_sl, ref + min_market_buffer)
        return ExitDecision("HOLD", state.q_retained, "NONE", exec_price, exchange_ts_price,
                            target_tp, state_id, "")


class ExitFillStatus:
    """
    AUDIT B2: explicit outcome states for an exit order.

    `retCode == 0` only means the exchange accepted the request. It says
    nothing about whether any quantity actually traded -- an IOC that crosses
    nothing is accepted and then immediately cancelled.
    """
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLED = "CANCELLED"
    UNFILLED = "UNFILLED"
    UNKNOWN = "UNKNOWN"


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Exchange payloads deliver numbers as strings, sometimes empty."""
    try:
        if value is None:
            return default
        text = str(value).strip()
        if text == "":
            return default
        parsed = float(text)
        return parsed if math.isfinite(parsed) else default
    except (TypeError, ValueError):
        return default


class ExecutionGovernorFSM:
    # Tolerance for treating a fill as complete (exchange rounding).
    FILL_COMPLETION_RATIO = 0.999
    # Below this absolute size the remaining position is treated as dust.
    DUST_QTY = 1e-12

    @staticmethod
    def _classify_fill_report(report: Dict[str, Any], requested_qty: float) -> Tuple[str, float, float]:
        """
        Map an exchange order record onto an explicit ExitFillStatus.
        Returns (status, filled_qty, avg_price).
        """
        if not report:
            return ExitFillStatus.UNKNOWN, 0.0, 0.0

        status_raw = str(report.get("orderStatus", "")).strip().lower()
        filled = _safe_float(report.get("cumExecQty"))
        avg_price = _safe_float(report.get("avgPrice"))

        if requested_qty > 0.0 and filled >= requested_qty * ExecutionGovernorFSM.FILL_COMPLETION_RATIO:
            return ExitFillStatus.FILLED, filled, avg_price

        if filled > 0.0:
            return ExitFillStatus.PARTIALLY_FILLED, filled, avg_price

        if status_raw == "rejected":
            return ExitFillStatus.CANCELLED, 0.0, avg_price
        if status_raw in ("cancelled", "canceled", "deactivated", "expired"):
            return ExitFillStatus.UNFILLED, 0.0, avg_price
        if status_raw == "filled":
            # Reported filled but cumExecQty unreadable -> not trustworthy.
            return ExitFillStatus.UNKNOWN, 0.0, avg_price
        if status_raw in ("new", "untriggered", "partiallyfilled"):
            return ExitFillStatus.UNFILLED, 0.0, avg_price

        return ExitFillStatus.UNKNOWN, 0.0, avg_price

    @classmethod
    async def _resolve_exit_fill(
        cls, executor: Any, symbol: str, order_id: str, requested_qty: float, timeout: float = 0.75
    ) -> Tuple[str, float, float]:
        """Ask the exchange what actually happened. Never infer from retCode."""
        report: Dict[str, Any] = {}

        if hasattr(executor, "await_ws_execution_report"):
            try:
                ws_report = await executor.await_ws_execution_report(order_id, timeout=timeout)
                if ws_report:
                    report = ws_report
            except Exception:
                report = {}

        if not report:
            for endpoint in ("/v5/order/realtime", "/v5/order/history"):
                try:
                    res = await executor.safe_call(
                        "GET", endpoint, category="linear", symbol=symbol, orderId=order_id, limit=1
                    )
                    rows = res.get("result", {}).get("list", []) if isinstance(res, dict) else []
                    if rows:
                        report = rows[0]
                        break
                except Exception:
                    continue

        return cls._classify_fill_report(report, requested_qty)

    @classmethod
    async def _fetch_position_size(
        cls, executor: Any, symbol: str, position_idx: Optional[int] = None
    ) -> Optional[float]:
        """
        Authoritative remaining size, or None when the exchange cannot be reached.
        None must never be interpreted as zero.

        AUDIT B2a: the response may carry one row per side. In hedge mode
        (positionIdx 1 = Buy, 2 = Sell) reading rows[0] blindly can return the
        OPPOSITE side's size, which would be wrong in the dangerous direction.
        When position_idx is supplied we match on it, and a non-match resolves to
        None (unknown) rather than 0.0 -- only a genuinely empty list means flat.
        """
        try:
            res = await executor.safe_call("GET", "/v5/position/list", category="linear", symbol=symbol)
            if not isinstance(res, dict) or res.get("retCode") != 0:
                return None

            rows = res.get("result", {}).get("list", [])
            if not rows:
                return 0.0

            if position_idx is None:
                return max(0.0, _safe_float(rows[0].get("size")))

            for row in rows:
                try:
                    row_idx = int(_safe_float(row.get("positionIdx"), default=-1))
                except (TypeError, ValueError):
                    continue
                if row_idx == int(position_idx):
                    return max(0.0, _safe_float(row.get("size")))

            # One-way accounts legitimately omit positionIdx from the payload.
            if len(rows) == 1 and rows[0].get("positionIdx") is None:
                return max(0.0, _safe_float(rows[0].get("size")))

            logger.warning(
                f"[GOVERNOR] {symbol}: no position row matched positionIdx={position_idx} "
                f"({len(rows)} row(s) returned). Treating size as UNKNOWN, not zero."
            )
            return None
        except Exception:
            return None

    @staticmethod
    def _format_qty_str(raw_qty: float, qty_step: Any) -> str:
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
                logger.warning(f"[GOVERNOR] Quantized close qty on {symbol} is zero ({qty_to_close} floored to step {qty_step}). Aborting.")
                return False
        except Exception:
            qty_str = str(qty_to_close)

        position_idx = int(ctx.get("position_idx", 0))

        if decision.urgency in ["MARKET", "EMERGENCY", "AGGRESSIVE", "FLASH_IOC"]:
            # Upgraded Slippage Collar: Use Limit IOC with 15 bps protective price collar 
            # instead of unconstrained Market IOC to prevent book-wiping spikes (e.g., -57.2 bps ETH fills).
            base_price = decision.limit_price
            if base_price <= 0.0:
                base_price = float(ctx.get("latest_tick_price", 0.0) or state.entry_price)
            
            collar_pct = 0.0015  # 15 bps max slippage collar
            if state.exit_side == "Sell":  # Closing a Long
                collar_price = base_price * (1.0 - collar_pct)
            else:  # Closing a Short
                collar_price = base_price * (1.0 + collar_pct)

            # Format price string using SOR helper or default formatting
            if hasattr(executor, 'core') and executor.core and hasattr(executor.core, 'sor'):
                price_str = executor.core.sor._format_price_str(collar_price, symbol)
            else:
                price_str = f"{collar_price:.4f}"

            res = await executor.safe_call(
                "POST", "/v5/order/create", is_execution=True,
                category="linear", symbol=symbol,
                side=state.exit_side, orderType="Limit", price=price_str, qty=qty_str,
                timeInForce="IOC", reduceOnly=True,
                positionIdx=position_idx,
                smpType="CancelMaker"
            )
            
            # AUDIT B2: acceptance is not execution. Resolve what actually
            # traded, then reconcile against exchange position state. The
            # position may only be declared closed on confirmed zero size.
            if not (isinstance(res, dict) and res.get("retCode") == 0):
                logger.error(
                    f"[GOVERNOR] Limit-Capped IOC execution rejected on {symbol}: "
                    f"{res.get('retMsg') if isinstance(res, dict) else res}"
                )
                return False

            order_id = str(res.get("result", {}).get("orderId", "") or "")
            requested_qty = float(Decimal(qty_str))

            status, filled_qty, avg_px = await cls._resolve_exit_fill(
                executor, symbol, order_id, requested_qty
            )
            remaining = await cls._fetch_position_size(executor, symbol, position_idx=position_idx)
            base_qty = state.base_qty if state.base_qty > 0.0 else current_actual_qty

            # --- Path 1: exchange confirms flat ------------------------------
            if remaining is not None and remaining <= cls.DUST_QTY:
                state.execution_state = "CLOSED"
                state.actual_qty = 0.0
                state.q_retained = 0.0
                logger.info(
                    f"[GOVERNOR] EXIT CONFIRMED FLAT // {symbol} "
                    f"(status={status}, filled={filled_qty:g} @ {avg_px:g})"
                )
                return True

            # --- Path 2: exchange reachable, size remains --------------------
            if remaining is not None:
                state.actual_qty = remaining
                state.q_retained = float(min(1.0, max(0.0, remaining / base_qty))) if base_qty > 0.0 else 0.0
                state.execution_state = "OBSERVE"
                log = logger.critical if filled_qty <= 0.0 else logger.warning
                log(
                    f"[GOVERNOR] EXIT INCOMPLETE // {symbol}: status={status}, "
                    f"requested={requested_qty:g}, filled={filled_qty:g}, "
                    f"remaining_on_exchange={remaining:g}. Position remains under "
                    f"active management (NOT closed)."
                )
                return filled_qty > 0.0

            # --- Path 3: exchange unreachable -- stay conservative -----------
            if filled_qty > 0.0:
                state.actual_qty = max(0.0, float(Decimal(str(current_actual_qty)) - Decimal(str(filled_qty))))
                state.q_retained = float(min(1.0, max(0.0, state.actual_qty / base_qty))) if base_qty > 0.0 else 0.0
            state.execution_state = "OBSERVE"
            logger.critical(
                f"[GOVERNOR] EXIT UNVERIFIED // {symbol}: status={status}, "
                f"filled={filled_qty:g}, exchange position unreadable. Retaining "
                f"monitoring and local qty {state.actual_qty:g}."
            )
            return filled_qty > 0.0

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