"""
INSTITUTIONAL RISK VAULT: ASYNC PORTFOLIO RISK & CONTAGION GOVERNOR
--------------------------------------------------------------------------------
Enforces real-time portfolio invariant firewalls, multi-tier drawdown containment,
cross-asset covariance clustering, and capital-at-risk limits.

Production Hardening & Bug Fixes:
- Thread-Safe Covariance Engine (Audit P0 Resolution): Dedicated thread-level lock
  shields internal running EWMA buffers (`ewma_mean`, `ewma_cov`, `ewma_var`) during
  worker pool execution. Employs atomic pointer swaps on `self.correlation_matrix`
  so async coroutines reading correlation matrices never face torn state.
- Single-Position Risk Enforcement (Audit #3 Resolution): Wires 
  `max_single_position_risk_pct` directly into `evaluate_portfolio_safety`,
  vetoing orders where prospective dollar risk exceeds the account allocation cap.
- Breaker Synchronization & Recovery (Audit #4 Resolution): Implements an explicit
  `reset_circuit_breaker()` and `sync_watermarks()` interface, eliminating the
  permanent deadlock where the vault could not be unlocked following an FSM reset.
- Correlation Self-Index Bug Fix: Filters `s != symbol` in `calculate_correlation_haircut`
  and correlation vetting to prevent an asset from comparing against itself (corr=1.0).
- Finite Covariance Guard: Validates price histories against NaN/Inf values before 
  computing EWMA returns, preventing permanent matrix poisoning from corrupted ticks.
- Pure Asyncio Locking: Uses native `asyncio.Lock` for async balance updates while
  retaining thread-level primitives for CPU-bound worker tasks.
"""

import math
import logging
import asyncio
import threading
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from datetime import datetime, timezone

logger = logging.getLogger("QUANT_CORE.RISK_VAULT")


class InstitutionalRiskVault:
    """
    Strictly governs portfolio contagion, absolute drawdowns, and correlation clustering.
    Acts purely as an invariant firewall to protect trading bankroll.
    """
    def __init__(
        self, 
        max_drawdown_pct: float = 0.10,               
        max_single_position_risk_pct: float = 0.015, 
        exchange_min_notional: float = 6.50,
        max_slots: int = 5
    ):
        self.max_drawdown_pct = max_drawdown_pct
        self.max_single_position_risk_pct = max_single_position_risk_pct
        self.exchange_min_notional = exchange_min_notional
        self.max_slots = max_slots
        
        self.absolute_max_leverage: float = 2.0      
        self.max_leverage: float = self.absolute_max_leverage
        self.base_leverage: float = 1.0
        
        # Multi-Tier Drawdown Thresholds
        self.soft_freeze_drawdown_pct: float = 0.05  # Halts new entries; preserves active trails
        self.emergency_circuit_breaker: bool = False
        
        # Systemic Drawdown Trackers
        self.peak_balance: float = 0.0
        self.last_valid_equity: float = 0.0
        self.current_drawdown_state: float = 0.0
        
        # Daily Loss Limit Trackers (3.5% Intraday Ceiling)
        self.daily_high_watermark: float = 0.0
        self.current_day_utc = datetime.now(timezone.utc).date()
        self.daily_loss_limit_pct: float = 0.035      
        
        # Position Ledger (Aliased for compatibility)
        self.active_positions: Dict[str, float] = {}
        self.correlation_matrix: Optional[pd.DataFrame] = None
        
        # Async and Thread Locks (Separation of Concerns)
        self._state_lock = asyncio.Lock()
        self._cov_thread_lock = threading.Lock()
        
        # Continuous Covariance State (Protected by _cov_thread_lock)
        self.prev_symbols: List[str] = []
        self.ewma_mean: Optional[np.ndarray] = None
        self.ewma_var: Optional[np.ndarray] = None
        self.ewma_cov: Optional[np.ndarray] = None
        self.prev_prices: Optional[np.ndarray] = None

    @property
    def position_ledger(self) -> Dict[str, float]:
        """Provides dual-interface access for core engine notional tracking."""
        return self.active_positions

    def reset_circuit_breaker(self):
        """Explicitly resets the emergency breaker upon verified system recovery."""
        self.emergency_circuit_breaker = False
        logger.warning("[RISK_VAULT] Emergency circuit breaker manually reset.")

    def sync_watermarks(self, current_balance: float):
        """Synchronizes baseline balance across engines to prevent watermark drift."""
        if current_balance > 0 and math.isfinite(current_balance):
            if self.peak_balance <= 0.0 or current_balance > self.peak_balance:
                self.peak_balance = current_balance
            if self.daily_high_watermark <= 0.0 or current_balance > self.daily_high_watermark:
                self.daily_high_watermark = current_balance
            self.last_valid_equity = current_balance

    def update_correlation_matrix(self, price_histories: Dict[str, List[float]]):
        """
        Continuous Dual-EWMA Covariance calculation.
        Runs inside worker thread pool (math_pool). Uses _cov_thread_lock to shield
        mutable internal running states, followed by an atomic reference swap on
        self.correlation_matrix for thread-safe reads by asyncio coroutines.
        """
        try:
            if not price_histories:
                return
            symbols = sorted(list(price_histories.keys()))
            if len(symbols) < 2:
                return

            latest_prices = np.array([price_histories[sym][-1] for sym in symbols], dtype=np.float64)
            if not np.all(np.isfinite(latest_prices)) or np.any(latest_prices <= 0.0):
                return

            n = len(symbols)

            with self._cov_thread_lock:
                # Re-index state if symbol universe changed, preserving overlapping sub-matrices
                if self.prev_symbols != symbols or self.prev_prices is None or len(self.prev_prices) != n:
                    new_mean = np.zeros(n, dtype=np.float64)
                    new_var = np.ones(n, dtype=np.float64) * 1e-6
                    new_cov = np.eye(n, dtype=np.float64) * 1e-6

                    if (
                        self.prev_symbols 
                        and self.ewma_cov is not None 
                        and self.ewma_mean is not None 
                        and self.ewma_var is not None
                    ):
                        old_sym_map = {s: i for i, s in enumerate(self.prev_symbols)}
                        for i, s_i in enumerate(symbols):
                            if s_i in old_sym_map:
                                old_i = old_sym_map[s_i]
                                new_mean[i] = self.ewma_mean[old_i]
                                new_var[i] = self.ewma_var[old_i]
                                for j, s_j in enumerate(symbols):
                                    if s_j in old_sym_map:
                                        old_j = old_sym_map[s_j]
                                        new_cov[i, j] = self.ewma_cov[old_i, old_j]

                    self.prev_symbols = symbols
                    self.prev_prices = latest_prices
                    self.ewma_mean = new_mean
                    self.ewma_var = new_var
                    self.ewma_cov = new_cov
                    return

                # Compute instantaneous log returns
                returns = np.log(
                    np.maximum(latest_prices, 1e-9) / np.maximum(self.prev_prices, 1e-9)
                )
                self.prev_prices = latest_prices

                if not np.all(np.isfinite(returns)):
                    return

                # Beta stripping: Cross-sectional market mean subtraction
                market_mean = float(np.mean(returns))
                excess_returns = returns - market_mean

                # Continuous EWMA Updates (Alpha = 0.005 ~ 200-tick half-life)
                alpha = 0.005
                delta = excess_returns - self.ewma_mean
                self.ewma_mean += alpha * delta
                self.ewma_var = (1.0 - alpha) * self.ewma_var + alpha * (delta ** 2)
                self.ewma_cov = (1.0 - alpha) * self.ewma_cov + alpha * np.outer(delta, delta)

                # Derive correlation matrix
                stds = np.sqrt(np.maximum(self.ewma_var, 1e-9))
                corr = self.ewma_cov / np.outer(stds, stds)
                corr = np.clip(np.nan_to_num(corr, nan=0.0), -1.0, 1.0)

                # Ledoit-Wolf Linear Shrinkage toward Identity
                shrinkage_intensity = 0.20
                shrunk_corr = (1.0 - shrinkage_intensity) * corr + (shrinkage_intensity * np.eye(corr.shape[0]))
                np.fill_diagonal(shrunk_corr, 1.0)

                # Atomic Pointer Swap (Thread-Safe Publish)
                new_df = pd.DataFrame(shrunk_corr, index=symbols, columns=symbols)
                self.correlation_matrix = new_df

        except Exception as e:
            logger.debug(f"[MATH_WARN] EWMA Correlation update failure: {e}")

    def get_max_allowed_slots(self) -> int:
        return self.max_slots

    def calculate_correlation_haircut(self, symbol: str) -> float:
        """
        Progressively attenuates notional from 1.0 down to 0.25 between 0.65 and 0.85 correlation.
        Filters out self-correlation to ensure clean cross-asset measurements.
        """
        corr_df = self.correlation_matrix
        if corr_df is None or len(self.active_positions) == 0:
            return 1.0

        active_syms = [
            s for s in self.active_positions.keys() 
            if s in corr_df.index and s != symbol
        ]
        if not active_syms or symbol not in corr_df.index:
            return 1.0

        corrs = [abs(float(corr_df.loc[symbol, s])) for s in active_syms]
        avg_corr = float(np.mean(corrs))

        if avg_corr > 0.65:
            penalty = ((avg_corr - 0.65) / 0.20) ** 2
            return float(np.clip(1.0 - (penalty * 0.75), 0.25, 1.0))

        return 1.0

    async def update_balance_atomic(self, current_balance: float) -> Tuple[float, float, bool]:
        """
        Non-blocking async atomic update of watermarks and portfolio drawdown.
        Returns: (daily_drawdown_pct, systemic_drawdown_pct, is_breached)
        """
        async with self._state_lock:
            if math.isnan(current_balance) or math.isinf(current_balance) or current_balance <= 0.0:
                return 0.0, self.current_drawdown_state, self.emergency_circuit_breaker

            self.last_valid_equity = current_balance

            # Watermark bootstrapping
            if self.daily_high_watermark <= 0.0:
                self.daily_high_watermark = current_balance
            if self.peak_balance <= 0.0:
                self.peak_balance = current_balance

            # Intraday High-Watermark Check
            now_date = datetime.now(timezone.utc).date()
            if now_date != self.current_day_utc:
                self.current_day_utc = now_date
                self.daily_high_watermark = current_balance
                
            if current_balance > self.daily_high_watermark:
                self.daily_high_watermark = current_balance

            daily_drawdown = max(0.0, (self.daily_high_watermark - current_balance) / max(self.daily_high_watermark, 1e-9))

            # Systemic High-Watermark Check
            if current_balance > self.peak_balance:
                self.peak_balance = current_balance
                self.current_drawdown_state = 0.0
            elif self.peak_balance > 0:
                self.current_drawdown_state = max(0.0, (self.peak_balance - current_balance) / self.peak_balance)

            is_breached = (self.current_drawdown_state >= self.max_drawdown_pct) or (daily_drawdown >= self.daily_loss_limit_pct)
            if self.current_drawdown_state >= self.max_drawdown_pct:
                self.emergency_circuit_breaker = True

            return daily_drawdown, self.current_drawdown_state, is_breached

    async def evaluate_portfolio_safety(
        self, 
        current_balance: float, 
        new_position_notional: float = 0.0, 
        symbol: str = "",
        sl_dist_pct: Optional[float] = None
    ) -> Tuple[bool, str]:
        """
        Evaluates portfolio health invariants before order execution.
        Enforces 3-tier drawdown architecture, single-position risk cap, dynamic slot 
        limits, correlation ceilings, and aggregate leverage bounds.
        """
        if self.emergency_circuit_breaker:
            return False, "EMERGENCY_CIRCUIT_BREAKER_ACTIVE"

        if math.isnan(current_balance) or math.isinf(current_balance) or current_balance <= 0.0:
            logger.critical(f"[RISK_VAULT] 🛑 FAIL-CLOSED ENGAGED: Invalid balance (${current_balance}). Halting execution.")
            return False, "HALT_INVALID_BALANCE_STATE"

        daily_dd, systemic_dd, _ = await self.update_balance_atomic(current_balance)

        # 1. Tier-1 Intraday Loss Limit (3.5%)
        if daily_dd >= self.daily_loss_limit_pct:
            logger.warning(f"🚨 INTRADAY LOSS LIMIT REACHED ({daily_dd:.2%}). Suspending entries.")
            return False, f"DAILY_LOSS_LIMIT_REACHED_{daily_dd:.2%}"

        # 2. Tier-2 Soft Allocation Freeze (5.0%): Blocks new positions; active trades continue trailing
        if systemic_dd >= self.soft_freeze_drawdown_pct and systemic_dd < self.max_drawdown_pct:
            logger.warning(f"[RISK_VAULT] ⚠️ SOFT DRAWDOWN FREEZE ({systemic_dd:.2%}). Preserving existing positions.")
            return False, f"SOFT_DRAWDOWN_FREEZE_{systemic_dd:.2%}"

        # 3. Tier-3 Hard Absolute Drawdown Breach
        if systemic_dd >= self.max_drawdown_pct:
            logger.critical(f"🚨 ABSOLUTE MAX DRAWDOWN BREACHED ({systemic_dd:.2%}). SYSTEM LOCKDOWN.")
            return False, f"MAX_DRAWDOWN_BREACHED_{systemic_dd:.2%}"

        # Active Symbol Deduplication Lock
        if symbol and symbol in self.active_positions:
            return False, f"DUPLICATE_SYMBOL_LOCK ({symbol})"

        # Dynamic Slot Count Cap
        if len(self.active_positions) >= self.get_max_allowed_slots():
            return False, f"DYNAMIC_SLOT_CAP_REACHED ({len(self.active_positions)}/{self.get_max_allowed_slots()})"

        # 4. Enforce Single-Position Risk Cap (Audit #3 Resolution)
        if new_position_notional > 0.0:
            effective_sl_pct = max(0.005, sl_dist_pct if sl_dist_pct is not None else 0.025)
            estimated_loss_dollars = new_position_notional * effective_sl_pct
            max_allowed_loss_dollars = current_balance * self.max_single_position_risk_pct
            
            if estimated_loss_dollars > max_allowed_loss_dollars:
                logger.warning(
                    f"[RISK_VAULT] 🛑 SINGLE RISK CAP EXCEEDED // {symbol}: "
                    f"Est. Risk ${estimated_loss_dollars:.2f} ({effective_sl_pct:.2%} stop) > "
                    f"Max Budget ${max_allowed_loss_dollars:.2f} ({self.max_single_position_risk_pct:.1%})"
                )
                return False, f"SINGLE_RISK_CAP_EXCEEDED (${estimated_loss_dollars:.2f} > ${max_allowed_loss_dollars:.2f})"

        # 5. Portfolio Correlation Hard-Veto at 0.85 (Excluding self)
        corr_df = self.correlation_matrix
        if corr_df is not None and len(self.active_positions) >= 2 and symbol:
            active_symbols = [
                s for s in self.active_positions.keys() 
                if s in corr_df.index and s != symbol
            ]
            if symbol in corr_df.index and active_symbols:
                corrs = [abs(float(corr_df.loc[symbol, s])) for s in active_symbols]
                avg_corr = float(np.mean(corrs)) if corrs else 0.0
                if avg_corr > 0.85:
                    logger.warning(f"[RISK_VAULT] 🛑 CORRELATION VETO // {symbol} has {avg_corr:.2f} avg correlation (> 0.85 cap). Aborting.")
                    return False, f"PORTFOLIO_CORRELATION_VETO ({avg_corr:.2f} > 0.85)"

        # 6. Portfolio Leverage Headroom Bound
        max_heat_dollars = max(self.exchange_min_notional, current_balance * self.max_leverage)
        total_exposure = sum(self.active_positions.values()) + new_position_notional

        if total_exposure > max_heat_dollars:
            return False, f"HEAT_CAP_EXCEEDED (Req: ${total_exposure:.2f} > Max: ${max_heat_dollars:.2f})"
                
        return True, "SAFE"

    def update_position_ledger(self, symbol: str, notional_value: float):
        """Synchronous in-memory mutation called exclusively via the single-threaded GlobalStateActor."""
        if notional_value <= 0 or math.isnan(notional_value) or math.isinf(notional_value):
            self.active_positions.pop(symbol, None)
        else:
            self.active_positions[symbol] = notional_value

    def clear_ledger(self):
        """Resets the position ledger without acquiring locks."""
        self.active_positions.clear()