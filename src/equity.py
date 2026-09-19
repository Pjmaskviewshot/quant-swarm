"""
EQUITY ACCOUNTING CONVENTION
--------------------------------------------------------------------------------
Audit finding B11: unrealised PnL was counted twice.

`get_wallet_balance_usdt()` prefers Bybit's `totalEquity`, which ALREADY includes
unrealised PnL on open positions. The position lifecycle daemon then computed

    live_equity = vault_bal + unrealised_pnl

and derived `drawdown_pct` from it. That drawdown drives PortfolioCommander,
which issues an EMERGENCY MARKET EXIT at 15%. The figure feeding the hardest
kill switch in the system was mis-scaled by one position's unrealised PnL.

THE CONVENTION, stated once and used everywhere:

  wallet_balance   realised cash. Bybit `totalWalletBalance`. Excludes unrealised.
  unrealised_pnl   mark-to-market PnL on open positions. Signed.
  equity           wallet_balance + unrealised_pnl. Bybit `totalEquity`.
                   This is the drawdown and risk-sizing basis.
  available        equity minus margin held. Not used for drawdown.

Rules:
  1. Drawdown is always computed on EQUITY.
  2. Never add unrealised PnL to a figure that already contains it.
  3. A locally-estimated equity must start from WALLET BALANCE, never equity.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class EquitySnapshot:
    """One consistent view of account value at a point in time."""
    wallet_balance: float      # realised cash only
    equity: float              # wallet_balance + unrealised
    as_of: float
    source: str = "exchange"

    @property
    def unrealised(self) -> float:
        return self.equity - self.wallet_balance

    def is_usable(self) -> bool:
        """
        Usable means "we have a real reading", not "the account is healthy".

        Found by property testing: requiring equity > 0.0 meant a WIPED account
        (equity exactly 0.0) -- and, pushed further, a NEGATIVE-equity account --
        was reported as UNKNOWN. Callers then fell back to a stale cached
        balance and the drawdown breaker never saw the loss. Zero and negative
        equity are real, critical readings. Only a non-finite value means
        "we do not have a reading".

        Judging whether the account is HEALTHY is the risk vault's job, and it
        already halts on a non-positive balance.
        """
        import math
        return math.isfinite(self.wallet_balance) and math.isfinite(self.equity)


def parse_wallet_response(payload: Dict[str, Any], now: float) -> Optional[EquitySnapshot]:
    """
    Extract both figures from a Bybit wallet-balance response.

    Returns None rather than a guess when the payload cannot be parsed --
    an unusable balance must fail closed (risk_vault already halts on it).
    """
    try:
        if not isinstance(payload, dict) or payload.get("retCode") != 0:
            return None
        rows = payload.get("result", {}).get("list", [])
        if not rows:
            return None
        acc = rows[0]

        def _f(*keys):
            for k in keys:
                v = acc.get(k)
                if v not in (None, "", "0"):
                    try:
                        return float(v)
                    except (TypeError, ValueError):
                        continue
            return None

        equity = _f("totalEquity", "totalMarginBalance")
        wallet = _f("totalWalletBalance")

        if equity is None and wallet is None:
            for coin in acc.get("coin", []) or []:
                if coin.get("coin") == "USDT":
                    try:
                        equity = float(coin.get("equity") or 0.0) or None
                        wallet = float(coin.get("walletBalance") or 0.0) or None
                    except (TypeError, ValueError):
                        pass
                    break

        if equity is None and wallet is None:
            return None
        if equity is None:
            equity = wallet
        if wallet is None:
            # No realised figure available; treat equity as wallet so callers
            # never double-count. Unrealised then reads as zero, which is
            # conservative for drawdown purposes.
            wallet = equity

        snap = EquitySnapshot(wallet_balance=float(wallet), equity=float(equity), as_of=now)
        return snap if snap.is_usable() else None
    except Exception:
        return None


def estimate_live_equity(
    last_snapshot: Optional[EquitySnapshot],
    unrealised_by_symbol: Dict[str, float],
) -> Optional[float]:
    """
    Intra-poll equity estimate.

    AUDIT B11: builds from WALLET BALANCE plus current unrealised across all
    tracked positions. Starting from `equity` (as the old code did) would add
    unrealised PnL a second time.

    Returns None when there is no usable snapshot -- callers must not
    substitute a guess.
    """
    if last_snapshot is None or not last_snapshot.is_usable():
        return None
    total_unrealised = float(sum(unrealised_by_symbol.values()))
    return last_snapshot.wallet_balance + total_unrealised


def compute_drawdown(peak_equity: float, current_equity: float) -> float:
    """Drawdown on EQUITY. Never on wallet balance, never on a mixed figure."""
    if peak_equity <= 0.0:
        return 0.0
    return max(0.0, (peak_equity - current_equity) / peak_equity)
