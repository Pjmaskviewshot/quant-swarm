"""
APEX TITAN: OPTIMAL STOPPING & INTELLIGENT EXIT MATRIX
-----------------------------------------------------------------------------------------
Continuous-time predictive optimal-stopping matrix anchored to position notional.

Production Hardening & Bug Fixes:
- True Executable Price Decoupling (P0 Resolution): Stop-loss triggers now evaluate 
  exclusively against the physical executable top-of-book (best_bid/best_ask). The 
  spoofable synthetic micro-price is confined to soft matrix indicators, eliminating 
  phantom stop-hunts triggered by adversary orderbook imbalance.
- Tiered Profit Scale-Outs (P1 Resolution): Introduces fractional position unwinding. 
  Positions safely clear 50% of retained volume into Flash IOCs upon crossing 1.5R, 
  locking in kinetic profit while leaving the runner exposed to the trailing Chandelier.
- Monotonic Ratchet Invariant: Enforces that trailing stops cannot degrade or move 
  backward against open positions when ATR cushions expand during volatility bursts. 
  Long stops are monotonically non-decreasing; short stops are monotonically non-increasing.
- Hard Maximum Holding Horizon: Implements a 240-minute (4-hour) time-stop exit.
- Dynamic Drawdown Sync: Inherits dynamic `max_drawdown_pct` directly from 
  the unified `ctx` payload rather than hardcoding static thresholds.
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
    state_id: str = "UNPROFITABLE"
    peak_pnl: float = 0.0
    peak_price: float = 0.0
    locked_pnl: float = -1e9
    mfe: float = 0.0
    mfe_r: float = 0.0
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
    entry_thesis: Any = None
    thesis_inv_cov: Any = None
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


class PortfolioCommander:
    @staticmethod
    def evaluate(ctx: Dict[str, Any]) -> Tuple[bool, str]:
        max_dd = ctx.get("max_drawdown_pct", 0.05)
        if ctx.get("drawdown_pct", 0.0) >= max_dd:
            return True, "SYSTEMIC_DRAWDOWN_BREACH (Risk Vault Lock)"
        return False, "SAFE"


class IntelligentExitEngine:
    """
    Evaluates real-time microstructure state to issue optimal-stopping exit decisions.
    """
    @staticmethod
    def evaluate(ctx: Dict[str, Any], state: PositionExitState) -> ExitDecision:
        if state.actual_qty <= 0.0 and state.base_qty > 0.0:
            state.actual_qty = state.base_qty

        if state.execution_state == "SYNC":
            return ExitDecision("HOLD", state.q_retained, "NONE", 0.0, 0.0, 0.0, "AWAITING_EXCHANGE_SYNC", "")

        is_buy = ctx["is_buy"]
        
        # ---------------------------------------------------------
        # P0 FIX: Decouple Stop Execution from Synthetic Micro-Price
        # ---------------------------------------------------------
        ob = ctx.get("last_ob", {})
        fallback_price = float(ctx.get("latest_tick_price", state.entry_price))
        best_bid = float(ob.get("best_bid", fallback_price))
        best_ask = float(ob.get("best_ask", fallback_price))
        
        # Real executable price for stop evaluation (we hit the bid if selling a long, ask if buying to cover a short)
        exec_price = best_bid if is_buy else best_ask
        
        # Theoretical price used solely for soft matrix indicators
        theoretical_price = float(ctx.get("safe_c_price", exec_price))
        # ---------------------------------------------------------

        total_qty = state.actual_qty

        if total_qty <= 0:
            return ExitDecision("HOLD", 0.0, "NONE", exec_price, 0.0, 0.0, "ZERO_POSITION", "")

        # Systemic Portfolio Drawdown Hard-Stop
        pf_override, pf_reason = PortfolioCommander.evaluate(ctx)
        if pf_override:
            return ExitDecision("EMERGENCY", 0.0, "MARKET", exec_price, 0.0, 0.0, pf_reason, "")

        # Maximum Holding Time Horizon Exit (4 Hours / 240 Minutes)
        duration_minutes = (time.time() - state.entry_time) / 60.0
        if duration_minutes >= 240.0:
            return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, 0.0, 0.0, f"MAX_HOLDING_TIME_EXCEEDED ({duration_minutes:.1f}m)", "")

        atr = float(ctx.get("atr", exec_price * 0.01))
        initial_risk_dist = atr * 2.5

        price_delta = (exec_price - state.entry_price) if is_buy else (state.entry_price - exec_price)
        current_r = price_delta / (initial_risk_dist + 1e-9)
        current_pnl = price_delta * total_qty

        # 1. Update R-Multiple & MFE State
        p_state = state.profit_state
        if p_state.peak_price == 0.0:
            p_state.peak_price = state.entry_price

        if current_pnl > p_state.peak_pnl:
            p_state.peak_pnl = current_pnl
            p_state.peak_price = exec_price
            p_state.mfe_r = max(p_state.mfe_r, current_r)

        stat_engine = ctx.get("stat_engine")

        # 2. Statistical Matrix Inversion Check
        if stat_engine and hasattr(stat_engine, 'historical_probs') and len(stat_engine.historical_probs) > 0:
            opp_prob = stat_engine.historical_probs[-1]
            dominant_flow = getattr(stat_engine, 'clean_ofi_z', 0.0)

            if is_buy and dominant_flow < -1.8 and opp_prob > 0.65:
                return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, 0.0, 0.0, f"MATRIX_INVERSION_BEAR ({opp_prob:.2f})", "")
            elif not is_buy and dominant_flow > 1.8 and opp_prob > 0.65:
                return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, 0.0, 0.0, f"MATRIX_INVERSION_BULL ({opp_prob:.2f})", "")

        # 3. Hawkes Cascade Exhaustion (Uses live marked_hawkes_z attribute)
        hawkes_z = getattr(stat_engine, "marked_hawkes_z", 0.0)
        if current_r >= 0.80:
            if is_buy and hawkes_z < -2.8:
                return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, 0.0, 0.0, f"HAWKES_CLIMAX_EXHAUSTION ({hawkes_z:.2f})", "")
            elif not is_buy and hawkes_z > 2.8:
                return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, 0.0, 0.0, f"HAWKES_CLIMAX_EXHAUSTION ({hawkes_z:.2f})", "")

        # 4. Kinetic Take-Profit Compression
        target_tp = state.entry_price + (initial_risk_dist * 2.5) if is_buy else state.entry_price - (initial_risk_dist * 2.5)

        if current_r >= 1.4:
            meso_z = getattr(stat_engine, "meso_momentum_z", 0.0)
            momentum_exhausted = (is_buy and meso_z < -0.5) or (not is_buy and meso_z > 0.5)

            if momentum_exhausted:
                compressed_tp = exec_price + (atr * 0.2 if is_buy else -atr * 0.2)
                target_tp = compressed_tp
                p_state.state_id = "KINETIC_COMPRESSION"

        # 5. Volatility Chandelier Trailing Stop (AT-SL)
        target_sl = state.entry_price - initial_risk_dist if is_buy else state.entry_price + initial_risk_dist

        vpin_z = 0.0
        regime_mult = 1.8 if ctx.get("regime") == "TRENDING" else 2.5
        dynamic_cushion = atr * regime_mult * (1.0 + max(0.0, vpin_z * 0.2))

        existing_sl = float(ctx.get("current_sl", 0.0))

        if is_buy:
            if p_state.mfe_r >= 2.5:
                parabolic_floor = state.entry_price + (price_delta * 0.80)
                target_sl = max(target_sl, parabolic_floor)
                p_state.state_id = "PARABOLIC_TRAIL"
            elif p_state.mfe_r >= 1.5:
                locked_floor = state.entry_price + (price_delta * 0.60)
                target_sl = max(target_sl, locked_floor)
                p_state.state_id = "PROFIT_LOCKED"
            elif p_state.mfe_r >= 0.75:
                be_floor = state.entry_price + (state.entry_price * 0.0015)
                target_sl = max(target_sl, be_floor)
                p_state.state_id = "BREAKEVEN_LOCKED"
            else:
                trail_floor = p_state.peak_price - dynamic_cushion
                target_sl = max(target_sl, trail_floor)

            # Monotonic Invariant: Long stop-loss can NEVER step downward
            if existing_sl > 0.0:
                target_sl = max(target_sl, existing_sl)

        else:
            if p_state.mfe_r >= 2.5:
                parabolic_ceiling = state.entry_price - (price_delta * 0.80)
                target_sl = min(target_sl, parabolic_ceiling)
                p_state.state_id = "PARABOLIC_TRAIL"
            elif p_state.mfe_r >= 1.5:
                locked_ceiling = state.entry_price - (price_delta * 0.60)
                target_sl = min(target_sl, locked_ceiling)
                p_state.state_id = "PROFIT_LOCKED"
            elif p_state.mfe_r >= 0.75:
                be_ceiling = state.entry_price - (state.entry_price * 0.0015)
                target_sl = min(target_sl, be_ceiling)
                p_state.state_id = "BREAKEVEN_LOCKED"
            else:
                trail_ceiling = p_state.peak_price + dynamic_cushion
                target_sl = min(target_sl, trail_ceiling)

            # Monotonic Invariant: Short stop-loss can NEVER step upward
            if existing_sl > 0.0:
                target_sl = min(target_sl, existing_sl)

        # ---------------------------------------------------------
        # P1 UPGRADE: Tiered Profit Taking (Scale-Out)
        # ---------------------------------------------------------
        if current_r >= 1.5 and state.q_retained == 1.0:
            p_state.state_id = "SCALE_OUT_50"
            state.q_retained = 0.5  # Retain 50%, dump 50%
            return ExitDecision("EXIT", 0.5, "FLASH_IOC", exec_price, target_sl, target_tp, "SCALE_OUT_1.5R", "")

        # 6. Physical Breach Verification
        if is_buy and exec_price <= target_sl:
            return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, target_sl, target_tp, f"TRAILING_SL_BREACH ({exec_price:.4f} <= {target_sl:.4f})", "")
        elif not is_buy and exec_price >= target_sl:
            return ExitDecision("EXIT", 0.0, "FLASH_IOC", exec_price, target_sl, target_tp, f"TRAILING_SL_BREACH ({exec_price:.4f} >= {target_sl:.4f})", "")

        return ExitDecision("HOLD", state.q_retained, "NONE", exec_price, target_sl, target_tp, "HOLD_DYNAMIC_TRAIL", "")


class ExecutionGovernorFSM:
    @staticmethod
    def _format_qty_str(raw_qty: float, qty_step: Any) -> str:
        """Floor-quantizes order lot sizes via Decimal arithmetic."""
        step_dec = Decimal(str(qty_step))
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

        if qty_to_close <= 0:
            return False

        qty_step = ctx.get("qty_step", "0.1")
        try:
            qty_str = cls._format_qty_str(qty_to_close, qty_step)
            if Decimal(qty_str) <= Decimal("0"):
                return False
        except Exception:
            qty_str = str(qty_to_close)

        # Immediate market execution for emergency & breach exits
        if decision.urgency in ["MARKET", "EMERGENCY", "AGGRESSIVE", "FLASH_IOC"]:
            await executor.safe_call(
                "POST", "/v5/order/create", is_execution=True,
                category="linear", symbol=symbol,
                side=state.exit_side, orderType="Market", qty=qty_str,
                timeInForce="IOC", reduceOnly=True
            )
            state.execution_state = "SYNC"
            return True

        if state.execution_state == "OBSERVE":
            await executor.safe_call(
                "POST", "/v5/order/create", is_execution=True,
                category="linear", symbol=symbol,
                side=state.exit_side, orderType="Limit", price=str(decision.limit_price),
                qty=qty_str, timeInForce="PostOnly", reduceOnly=True
            )
            state.execution_state = "SYNC"
            return True

        return False