# Risk Controls

Every control that can stop the system from losing money, what triggers it, and
what it does. If you change a number here, change it knowing what it guards.

## Layer 1 — order sizing

| control | env var | default | what it prevents |
|---|---|---|---|
| per-trade risk cap | `MAX_SINGLE_POSITION_RISK_PCT` | 0.025 | one trade taking more than 2.5% of equity |
| minimum equity floor | `MIN_REQUIRED_EQUITY` | 50.0 USDT | sizing down into dust instead of stopping |
| notional deviation guard | `NOTIONAL_DEVIATION_TOLERANCE` | 0.25 | **B1** — quantisation inflating an order |
| min-notional policy | `SKIP_BELOW_MIN_NOTIONAL` | `true` | rounding UP to the exchange minimum and spending more than intended |

**B1 in detail.** Rounding a sub-minimum quantity up to the exchange minimum was
reproduced turning a $500 intended order into $6,500 — 13x. The deviation guard
rejects any order whose realised notional exceeds the intention by more than the
tolerance, and `SKIP_BELOW_MIN_NOTIONAL=true` refuses the trade outright rather
than inflating it. Setting that to `false` re-enables the inflation behaviour.

## Layer 2 — market data gating

| control | env var | default | what it prevents |
|---|---|---|---|
| orderbook freshness | `MAX_ORDERBOOK_AGE_SEC` | 5.0 | trading on a frozen feed |
| depth requirement | — | — | sizing against a BBO-only REST fallback with no depth |
| unknown slippage | — | — | `SLIPPAGE_UNKNOWN` blocks execution rather than assuming zero |

Freshness applies to **both** the entry path and the exit path. The exit case
matters more: a stale book driving a CAMB trailing stop is the failure mode that
most endangers an already-open position. When data is stale, software exit
decisions are suspended while exchange-native stops and reconciliation continue
— the system does not guess, and it does not go blind.

## Layer 3 — portfolio

| control | env var | default | action on trip |
|---|---|---|---|
| drawdown breaker | `MAX_DRAWDOWN_PCT` | 0.15 | EMERGENCY MARKET EXIT of all positions |
| circuit breaker | — | — | halts new entries; existing positions still managed |
| symbol bans | — | — | temporary exclusion after repeated rejections |

Drawdown is computed on **equity**, never on wallet balance and never on a mixed
figure. The convention is stated once in `src/equity.py` and used everywhere:

```
wallet_balance = realised cash          (Bybit totalWalletBalance)
unrealised     = mark-to-market, signed
equity         = wallet_balance + unrealised   (Bybit totalEquity)
```

**B11** was this going wrong: `live_equity = vault_bal + unrealized_pnl`, where
`vault_bal` already came from `totalEquity` and so already contained unrealised
PnL. The figure driving the hardest kill switch in the system was inflated by
one position's unrealised PnL.

A wiped account (equity exactly 0.0) and a blown one (negative equity) are
reported as real readings, not as "unknown". A property test found that treating
them as unknown made callers fall back to a stale cached balance, so the
drawdown breaker never saw a 100% loss.

## Layer 4 — health state machine

`core/fsm.py` separates two questions that must never be conflated:

* **Can the system open new positions?** Degraded by module errors.
* **Can the system manage existing positions?** Degraded only by an emergency
  lock.

An error flood in a signal-evaluation module stops new entries and leaves
position management fully alive. The opposite design — flattening on error
count — turns a transient fault into a realised loss. `TRANSIENT` severity
errors never degrade health at all.

## Layer 5 — execution integrity

* **Reduce-only never increases exposure.** Property-tested over random
  quantities and prices.
* **A position is CLOSED only when the exchange says size is zero.** Never on an
  order acknowledgement — **B2**, where a `Filled` acknowledgement marked a
  position closed while the exchange still held 10 units, leaving it unmanaged
  and unprotected.
* **`positionIdx` is matched explicitly** in hedge mode — **B2a**, a defect in my
  own B2 fix, where an unfiltered `rows[0]` could read the opposite side.
* **A failed position query returns `None`.** Unknown is not flat.

## Mode gating

`TRADING_MODE` decides whether real orders are placed. It has no default that
reaches the exchange: absent configuration is PAPER. Credentials being present
does **not** imply LIVE. Live activation is an explicit human action.

`PaperBroker` cannot reach the live account: any attribute outside a small
read-only allow-list raises `PaperIsolationError` rather than falling through.

## Kill switches available to an operator

1. `TRADING_MODE=PAPER` and restart — stops all real orders.
2. `MAX_DRAWDOWN_PCT` lowered — force-flattens sooner.
3. `fsm.trigger_global_emergency_lock()` — halts entries immediately.
4. Revoke the Bybit API key — the hardest stop, and the one that does not depend
   on this process behaving correctly. **Use this if anything is genuinely
   wrong.**

## Controls that do NOT exist

Stated because their absence is easy to assume away:

* No hard per-day loss limit independent of drawdown.
* No maximum position count independent of per-trade risk.
* No cross-symbol correlation limit — twelve correlated alts can be held at once,
  each individually within its risk cap.
* No automated halt on consecutive losses (only a short cooldown).
* No liquidation-price guard in the paper broker (liquidation is not modelled).
