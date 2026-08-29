"""
💎 V25.0 APEX QUANTUM PRIME: INTELLIGENT EXIT & EXECUTION GOVERNOR
-----------------------------------------------------------------------------------------
Continuous-Time Non-Reactive, Predictive Optimal-Stopping Matrix.
Mathematically anchored to Position Notional (Zero Equity-Bleed).

Architectural Supremacy (V25.0):
- Eradication of Redundant Vetoes: Stripped out QuantumMicrostructurePredictor, 
  TensorFusedAlphaDecay, and AnalyticalJumpRiskEngine. The exit engine no longer 
  recalculates order flow; it trusts the centralized 18-D Volterra-Hermite Tensor 
  to flag regime shifts and signal inversions natively.
- Pure Position Management: Refocused strictly on Scale-Invariant Profit 
  Protection (Trailing Stops) and Portfolio Contagion constraints.
- Pure Asyncio Execution: FSM Governor directly hits the V5 REST strings 
  over aiohttp, bypassing legacy Pybit adapters for zero-latency ejections.
"""

import math
import time
import logging
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, Any, Tuple, Optional

logger = logging.getLogger("QUANT_CORE.V25_EXIT")

@dataclass
class ThesisVector:
    """Retained purely for backward compatibility with existing main.py instantiations."""
    features: Any = None 

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
        portfolio_dd = float(ctx.get("drawdown_pct", 0.0))
        live_count = int(ctx.get("active_positions_count", 1))
        
        if portfolio_dd > 0.08:
            return True, f"SYSTEMIC_DRAWDOWN_BREACH ({portfolio_dd * 100:.1f}%)"
        if portfolio_dd > 0.04 and live_count > 4:
            return True, f"PORTFOLIO_CONTAGION_REDUCTION ({portfolio_dd * 100:.1f}%)"
            
        return False, "SAFE"


class ScaleInvariantProfitGovernor:
    @staticmethod
    def evaluate(ctx: Dict[str, Any], state: PositionExitState) -> Tuple[bool, float, str]:
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
        
        notional = price * qty
        atr = float(ctx.get("atr", price * 0.01))
        atr_pct = atr / max(price, 1e-9)
        
        fee_hurdle = notional * 0.0015
        
        # Anchor profit scaling purely to Notional to guarantee Tier 1 arms correctly
        base_unit = max(notional * 0.006, notional * atr_pct * 0.8)

        t1_threshold = base_unit * 1.0
        t2_threshold = base_unit * 1.5
        t3_threshold = base_unit * 2.5
        parabolic_threshold = base_unit * 4.0

        if p_state.peak_pnl >= parabolic_threshold:
            p_state.state_id = "PARABOLIC_TRAIL"
            p_state.locked_pnl = max(p_state.locked_pnl, p_state.peak_pnl * 0.85)
        elif p_state.peak_pnl >= t3_threshold:
            p_state.state_id = "PROFIT_LOCKED_TIER3"
            p_state.locked_pnl = max(p_state.locked_pnl, p_state.peak_pnl * 0.75) 
        elif p_state.peak_pnl >= t2_threshold:
            p_state.state_id = "PROFIT_LOCKED_TIER2"
            p_state.locked_pnl = max(p_state.locked_pnl, p_state.peak_pnl * 0.60)
        elif p_state.peak_pnl >= t1_threshold:
            p_state.state_id = "PROFIT_ARMED_TIER1"
            p_state.locked_pnl = max(p_state.locked_pnl, fee_hurdle * 2.0)
        elif current_pnl > fee_hurdle:
            p_state.state_id = "PROFIT_FORMING"
        else:
            p_state.state_id = "UNPROFITABLE"

        if p_state.locked_pnl > 0 and current_pnl <= p_state.locked_pnl:
            return True, 0.0, f"LOCKED_PROFIT_TRIGGERED (PnL: ${current_pnl:.3f} <= Lock: ${p_state.locked_pnl:.3f})"

        return False, 1.0, "PROFIT_SAFE"


class IntelligentExitEngine:
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
            
        pf_override, pf_reason = PortfolioCommander.evaluate(ctx)
        if pf_override:
            return ExitDecision("EMERGENCY", 0.0, "MARKET", price, 0.0, 0.0, pf_reason, "")

        prof_override, prof_target_q, prof_reason = ScaleInvariantProfitGovernor.evaluate(ctx, state)
        if prof_override:
            urgency = "MARKET" if prof_target_q == 0.0 else "AGGRESSIVE"
            return ExitDecision(
                "EXIT" if prof_target_q == 0.0 else "REDUCE",
                prof_target_q, urgency, price, 0.0, 0.0,
                prof_reason, ""
            )

        # 🚀 V25.0 Matrix Signal Inversion: Trust the centralized 18-D Tensor
        # If the adversarial engine fundamentally shifts its prediction, eject immediately.
        stat_engine = ctx.get("stat_engine")
        if stat_engine and hasattr(stat_engine, 'historical_probs') and stat_engine.historical_probs:
            latest_prob = stat_engine.historical_probs[-1]
            # Fast-path resolution of current dominant direction
            current_favored_dir = "BUY" if getattr(stat_engine, 'clean_ofi_z', 0.0) > 0 else "SELL"
            
            if is_buy and current_favored_dir == "SELL" and latest_prob > 0.65:
                return ExitDecision("EXIT", 0.0, "MARKET", price, 0.0, 0.0, f"MATRIX_INVERSION_SELL ({latest_prob:.2f})", "")
            elif not is_buy and current_favored_dir == "BUY" and latest_prob > 0.65:
                return ExitDecision("EXIT", 0.0, "MARKET", price, 0.0, 0.0, f"MATRIX_INVERSION_BUY ({latest_prob:.2f})", "")

        last_ob = ctx.get("last_ob", {})
        limit_p = float(last_ob.get("best_bid", price)) if state.exit_side == "SELL" else float(last_ob.get("best_ask", price))

        trailing_stop_price = 0.0 
        if state.profit_state.locked_pnl > 0 and total_qty > 0:
            trailing_stop_price = (
                state.entry_price + (state.profit_state.locked_pnl / total_qty) 
                if is_buy else state.entry_price - (state.profit_state.locked_pnl / total_qty)
            )

        return ExitDecision("HOLD", 1.0, "NONE", limit_p, trailing_stop_price, 0.0, "MATRIX_HOLD", "")


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
            
        # 🚀 V25.0: Pure Asyncio Execution Bypassing Legacy Adapters
        if decision.urgency in ["MARKET", "EMERGENCY", "AGGRESSIVE"]:
            res = await executor.safe_call(
                "POST", "/v5/order/create", is_execution=True,
                category="linear", symbol=symbol,
                side=state.exit_side, orderType="Market", qty=qty_str,
                timeInForce="IOC", reduceOnly=True
            )
            state.execution_state = "SYNC"
            return True

        if state.execution_state == "OBSERVE":
            res = await executor.safe_call(
                "POST", "/v5/order/create", is_execution=True,
                category="linear", symbol=symbol,
                side=state.exit_side, orderType="Limit", price=str(decision.limit_price),
                qty=qty_str, timeInForce="PostOnly", reduceOnly=True
            )
            state.execution_state = "SYNC"
            return True

        return False