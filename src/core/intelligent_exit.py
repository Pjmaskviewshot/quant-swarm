"""
V41.1 APEX TITAN: CONTINUOUS OPTIMAL-STOPPING & MICROSTRUCTURE EXIT MATRIX
-----------------------------------------------------------------------------------------
Continuous-time optimal stopping engine combining instantaneous alpha drift,
order flow imbalance dynamics, kinematic point-process exhaustion, and 
monotonic volatility chandelier boundaries.

Production Hardening & Anti-Choking Resolutions:
- Premature Chandelier Choking Eradicated (P0 Resolution): Positions hold their full 
  initial stop distance (2.5x ATR / 1.50% minimum) without trailing until favorable 
  excursion reaches at least +0.40R. Eradicates the defect where trail_floor tightened
  stops onto the entry price on tick 1, causing immediate stop-outs in < 0.7 mins.
- Monotonic Locked Stop Floor: Anchors stops to `p_state.locked_sl` so trailing stops 
  strictly advance in favorable directions and never retreat during volatility spikes.
- Initial Risk Anchoring: Anchors R-multiples and distance metrics to `initial_risk_dist` 
  recorded at entry, eliminating tick-to-tick denominator oscillations.
- Scale-Out Liquidation Isolation: Dispatches partial take-profits under action 'SCALE_OUT', 
  allowing 50% lot unwinding at 1.4R while leaving the runner active in the market.
- Top-of-Book Executable Price Breaches: Physical triggers evaluate strictly against BBO 
  (best_bid for longs, best_ask for shorts) to prevent spoof-induced phantom executions.
- Sane Take-Profit Boundaries: Validates that dynamic TP levels exceed entry price 
  for longs and sit below entry price for shorts before checking limit breaches.
"""

import math
import time
import logging
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, Any, Tuple, Optional
from decimal import Decimal, ROUND_FLOOR

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
    Continuous Microstructure Optimal Stopping Policy.
    Evaluates orderbook physics and statistical alpha decay to lock in gains dynamically.
    """
    @staticmethod
    def evaluate(ctx: Dict[str, Any], state: PositionExitState) -> ExitDecision:
        now = time.time()
        
        # Self-healing fallback for base position volume
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
        
        # Physical executable top-of-book pricing
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
        
        # P0 RESOLUTION: Fixed baseline risk distance anchored to entry
        initial_risk_dist = float(ctx.get("initial_risk_dist", 0.0))
        if initial_risk_dist <= 0.0:
            initial_risk_dist = max(atr * 2.5, state.entry_price * 0.015)

        price_delta = (exec_price - state.entry_price) if is_buy else (state.entry_price - exec_price)
        current_r = price_delta / (initial_risk_dist + 1e-9)
        current_pnl = price_delta * total_qty

        # 1. Update R-Multiple, MFE, and MAE State
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

        peak_delta = abs(p_state.peak_price - state.entry_price)

        # Baseline Initial Boundary Levels
        target_sl = state.entry_price - initial_risk_dist if is_buy else state.entry_price + initial_risk_dist

        # Take-profit level preservation
        existing_tp = float(ctx.get("current_tp", 0.0))
        if existing_tp > 0.0:
            target_tp = existing_tp
        else:
            dynamic_rr = float(ctx.get("dynamic_rr_ratio", 2.0))
            target_tp = state.entry_price + (initial_risk_dist * dynamic_rr) if is_buy else state.entry_price - (initial_risk_dist * dynamic_rr)

        # =========================================================================
        # CONTINUOUS OPTIMAL STOPPING SENSORS (Activated strictly in profit)
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

            # SENSOR TRIGGER 1: ALPHA DRIFT INVERSION (>= 0.50R Profit)
            if current_r >= 0.50:
                alpha_exhausted = continuation_prob < 0.45
                flow_opposed = adverse_flow_z > 1.8
                if alpha_exhausted and flow_opposed:
                    return ExitDecision(
                        action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                        exchange_ts_price=target_sl, dynamic_tp_price=target_tp,
                        reason=f"ALPHA_DRIFT_INVERSION (ContProb: {continuation_prob:.1%}, AdverseOFI: {adverse_flow_z:+.1f}s)",
                        log_output=""
                    )

            # SENSOR TRIGGER 2: KINEMATIC CASCADE EXHAUSTION (>= 0.70R Profit)
            if current_r >= 0.70:
                is_hawkes_climax = (abs(hawkes_z) > 2.8) and (accel_z < -1.2)
                if is_hawkes_climax:
                    return ExitDecision(
                        action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                        exchange_ts_price=target_sl, dynamic_tp_price=target_tp,
                        reason=f"KINEMATIC_FLOW_EXHAUSTION (Hawkes: {hawkes_z:.2f}s, Accel: {accel_z:.2f}s)",
                        log_output=""
                    )

            # SENSOR TRIGGER 3: BAYESIAN REGIME TERMINATION (>= 0.80R Profit)
            if current_r >= 0.80 and bocd_cp_prob > 0.65:
                return ExitDecision(
                    action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                    exchange_ts_price=target_sl, dynamic_tp_price=target_tp,
                    reason=f"BOCD_REGIME_TERMINATION (P(Changepoint): {bocd_cp_prob:.1%})",
                    log_output=""
                )

        # SENSOR TRIGGER 4: DYNAMIC PROFIT RETRACEMENT GUARD (>= 0.80R Peak)
        if p_state.mfe_r >= 0.80:
            retrace_pct = (p_state.mfe_r - current_r) / (p_state.mfe_r + 1e-9)
            if retrace_pct >= 0.25:
                return ExitDecision(
                    action="EXIT", target_q=0.0, urgency="FLASH_IOC", limit_price=exec_price,
                    exchange_ts_price=target_sl, dynamic_tp_price=target_tp,
                    reason=f"DYNAMIC_PROFIT_RETRACEMENT (Gave back {retrace_pct:.1%} from {p_state.mfe_r:.2f}R peak)",
                    log_output=""
                )

        # -------------------------------------------------------------------------
        # PROGRESSIVE MONOTONIC VOLATILITY CHANDELIER (Anti-Choking Gated)
        # -------------------------------------------------------------------------
        regime_mult = 1.8 if ctx.get("regime") == "TRENDING" else 2.2
        dynamic_cushion = max(atr * regime_mult, state.entry_price * 0.008)
        existing_sl = float(ctx.get("current_sl", 0.0))

        if is_buy:
            if p_state.mfe_r >= 1.8:
                floor_price = state.entry_price + (peak_delta * 0.80)
                target_sl = max(target_sl, floor_price)
                p_state.state_id = "PARABOLIC_80"
            elif p_state.mfe_r >= 1.2:
                floor_price = state.entry_price + (peak_delta * 0.60)
                target_sl = max(target_sl, floor_price)
                p_state.state_id = "PROFIT_LOCKED_60"
            elif p_state.mfe_r >= 0.60:
                fee_buffer = state.entry_price * 0.0030
                be_floor = state.entry_price + fee_buffer
                target_sl = max(target_sl, be_floor)
                p_state.state_id = "SECURE_BREAKEVEN"
            elif p_state.mfe_r >= 0.40:
                # P0 RESOLUTION: Only trail after position establishes positive excursion
                trail_floor = p_state.peak_price - dynamic_cushion
                target_sl = max(target_sl, trail_floor)
                p_state.state_id = "EARLY_TRAIL"
            else:
                # P0 RESOLUTION: Hold initial risk stop without choking early trades
                p_state.state_id = "HOLD_INITIAL_RISK"

            if existing_sl > 0.0:
                target_sl = max(target_sl, existing_sl)

            # Strict Monotonic Ratchet Guarantee for Longs
            if p_state.locked_sl > 0.0:
                target_sl = max(target_sl, p_state.locked_sl)
            p_state.locked_sl = target_sl

        else:  # SHORT
            if p_state.mfe_r >= 1.8:
                ceiling_price = state.entry_price - (peak_delta * 0.80)
                target_sl = min(target_sl, ceiling_price)
                p_state.state_id = "PARABOLIC_80"
            elif p_state.mfe_r >= 1.2:
                ceiling_price = state.entry_price - (peak_delta * 0.60)
                target_sl = min(target_sl, ceiling_price)
                p_state.state_id = "PROFIT_LOCKED_60"
            elif p_state.mfe_r >= 0.60:
                fee_buffer = state.entry_price * 0.0030
                be_ceiling = state.entry_price - fee_buffer
                target_sl = min(target_sl, be_ceiling)
                p_state.state_id = "SECURE_BREAKEVEN"
            elif p_state.mfe_r >= 0.40:
                # P0 RESOLUTION: Only trail after position establishes positive excursion
                trail_ceiling = p_state.peak_price + dynamic_cushion
                target_sl = min(target_sl, trail_ceiling)
                p_state.state_id = "EARLY_TRAIL"
            else:
                # P0 RESOLUTION: Hold initial risk stop without choking early trades
                p_state.state_id = "HOLD_INITIAL_RISK"

            if existing_sl > 0.0:
                target_sl = min(target_sl, existing_sl)

            # Strict Monotonic Ratchet Guarantee for Shorts
            if p_state.locked_sl > 0.0:
                target_sl = min(target_sl, p_state.locked_sl)
            p_state.locked_sl = target_sl

        # -------------------------------------------------------------------------
        # TIERED FRACTIONAL SCALE-OUT (50% lot at 1.4R)
        # -------------------------------------------------------------------------
        if current_r >= 1.4 and state.q_retained >= 0.99:
            p_state.state_id = "SCALE_OUT_50"
            return ExitDecision("SCALE_OUT", 0.5, "FLASH_IOC", exec_price, target_sl, target_tp, "SCALE_OUT_1.4R", "")

        # -------------------------------------------------------------------------
        # PHYSICAL BREACH EXECUTION (SL & TP)
        # -------------------------------------------------------------------------
        # Trailing Stop Breaches
        if is_buy and exec_price <= target_sl:
            return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, target_sl, target_tp, f"TRAILING_SL_BREACH ({exec_price:.4f} <= {target_sl:.4f})", "")
        elif not is_buy and exec_price >= target_sl:
            return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, target_sl, target_tp, f"TRAILING_SL_BREACH ({exec_price:.4f} >= {target_sl:.4f})", "")

        # Take-Profit Limit/Market Breaches
        if is_buy and target_tp > state.entry_price and exec_price >= target_tp:
            return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, target_sl, target_tp, f"DYNAMIC_TP_REACHED ({exec_price:.4f} >= {target_tp:.4f})", "")
        elif not is_buy and target_tp < state.entry_price and target_tp > 0.0 and exec_price <= target_tp:
            return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, target_sl, target_tp, f"DYNAMIC_TP_REACHED ({exec_price:.4f} <= {target_tp:.4f})", "")

        return ExitDecision("HOLD", state.q_retained, "NONE", exec_price, target_sl, target_tp, "HOLD_OPTIMAL_CONTINUATION", "")


class ExecutionGovernorFSM:
    @staticmethod
    def _format_qty_str(raw_qty: float, qty_step: Any) -> str:
        """Floor-quantizes order lot sizes via normalized Decimal arithmetic."""
        step_dec = Decimal(str(qty_step)).normalize()
        if step_dec <= Decimal("0"):
            return f"{float(raw_qty):.4f}"
        val_dec = Decimal(str(raw_qty))
        quantized = (val_dec // step_dec) * step_dec
        precision = max(0, -step_dec.as_tuple().exponent)
        return f"{quantized:.{precision}f}"

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
            
            # Verify exchange acceptance before altering state
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