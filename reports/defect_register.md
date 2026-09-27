# Defect Register

Every defect found across Phase 1, Phase 0, the P0 safety gate and the APEX
overhaul, with its status on branch `phase2/apex-overhaul` and the test that
holds it fixed.

**Evidence class** is stated for each, because "I read the code and it looks
wrong" and "I ran it and watched it happen" are different claims and should
never be presented as the same one:

* `[R]` **reproduced** — executed against the real repository, observed
* `[C]` **confirmed by reading** — the code path is unambiguous
* `[?]` **unresolved** — could not be settled without a live exchange

---

## Capital-risk defects

| ID | Defect | Class | Status | Test |
|---|---|---|---|---|
| **B1** | Sub-minimum quantity rounded **up** to the exchange minimum. Reproduced turning a $500 intended order into $6,500 — 13x. | `[R]` | Fixed. `SKIP_BELOW_MIN_NOTIONAL` refuses; notional-deviation guard backstops. | `test_b1_notional.py` |
| **B2** | Position marked `CLOSED` on a `Filled` order acknowledgement. Reproduced with the exchange still holding 10 units — the position became unmanaged and unprotected. | `[R]` | Fixed. Closure requires an exchange-confirmed zero size. | `test_b2_exit_fill.py` |
| **B2a** | *A defect in my own B2 fix.* `_fetch_position_size` read `rows[0]` without matching `positionIdx`; in hedge mode it could read the opposite side. | `[C]` | Fixed. Explicit idx match; non-match returns `None` (unknown), never `0.0`. | `test_b2a_position_idx.py` |
| **B11** | Unrealised PnL counted twice. `live_equity = vault_bal + unrealized_pnl` where `vault_bal` came from `totalEquity`, which already contains it. The figure driving the 15% force-flatten was mis-scaled. | `[C]` | Fixed. One convention, stated in `src/equity.py`, used everywhere. | `test_b11_equity.py` |
| **NEW-1..5** | *Defects in the paper broker I wrote.* `__getattr__` delegated to the live executor: the engine read `9999.99` from the live account while paper held `999.45`; `adjust_leverage` reached the live account; a $100 account opened a $1M position and went to −450. | `[R]` | Fixed. `PaperIsolationError` on anything outside a read-only allow-list; margin solvency check added. | `test_paper_isolation.py` |
| **D1** | *A defect I introduced.* `NameError` — `now_sec` referenced five times, never bound, inside the `ACTIVE_MONITORING` exit loop. Would have broken position management on the first iteration for every open position. | `[C]` | Fixed. One clock reading per iteration. | `ruff F821` in CI |

## State and accounting integrity

| ID | Defect | Class | Status | Test |
|---|---|---|---|---|
| **B12** | `UNKNOWN` settlement treated as a loss, feeding the Kelly sizer and the model labels. | `[C]` | Fixed. `is_correct` is `None` when unresolved; excluded from every statistic. | `test_p1_ledger_integrity.py` |
| **B15/B16** | Ledger migration and outcome counting. | `[C]` | Fixed. Idempotent local migration. | `test_p1_ledger_integrity.py` |
| **B17** | Whitener state did not travel with the RLS weights across a walk-forward fold, so the test fold applied frozen weights to features on a different scale. **Every OOS Sharpe produced this way was invalid — including the one that selected `params.json`.** | `[C]` | Fixed. Whitener mean, covariance and ZCA now exported and restored. | `test_backtest_integrity.py` (state transfer + freeze) |
| **B18** | Only one of four covariance matrices was restored from persisted state. | `[C]` | Fixed; `load_state` hardened to treat persisted state as untrusted. | `test_failure_injection.py` |
| **C6b** | **SQLite does not store NaN — it writes NULL.** Every read site does `float(x or 0.0)`, so a NaN PnL became a *breakeven trade* in the win rate, the expectancy, the Kelly update and the model labels. And `net_pnl > 0` is False for NaN, so it was *also* filed as a loss. | `[R]` | Fixed at the write site (recorded as `UNKNOWN`); backstopped by reconciliation check C6b. | `test_ledger_reconciliation.py` |
| **C9** | No check that win count by outcome label agrees with win count by PnL sign. | `[C]` | Added as a CRITICAL reconciliation check. | `test_ledger_reconciliation.py` |

## Signal and data quality

| ID | Defect | Class | Status | Test |
|---|---|---|---|---|
| **B3** | The traded basket could be swapped without re-subscribing the feed, so signals were computed on symbols with no live data. | `[C]` | Fixed. | `test_b3_universe_subscription.py` |
| **B4** | Directional probability was neither carried nor calibrated through to the exit logic. | `[C]` | Fixed. `src/probability.py` — `ProbabilityEstimate`, with 0.50 indifference fallback. | `test_invariants_property.py` |
| **B5/B6** | The ATR timeframe was not actually subscribed, and Bybit pushes the **forming** candle — signals were computed on incomplete bars. | `[C]` | Fixed. `confirm` flag required. | `test_b5_b6_candles.py` |
| **B19/B20** | Freshness was enforced on the entry path only. The exit loop read whatever snapshot was in memory, so a frozen feed drove the CAMB trailing stop — the failure mode that most endangers an open position. | `[C]` | Fixed. Applies to both paths; stale data suspends software exits while exchange stops remain. | `test_b19_b20_freshness.py` |
| **B22** | `volume24h` is a rolling cumulative; Z-scoring it produced a meaningless statistic. | `[C]` | Fixed. | — |
| **B26** | Base-asset matching was substring-based. | `[C]` | Fixed. Exact match. | — |

## Risk and health machinery

| ID | Defect | Class | Status | Test |
|---|---|---|---|---|
| **B7** | Mode was inferred in several places rather than resolved once. | `[C]` | Fixed. `src/runtime_config.py`. | `test_runtime_config.py` |
| **B8** | Entry health and position-management health were conflated, so an error flood in a signal module could stop position management. | `[C]` | Fixed. Separate predicates; `TRANSIENT` never degrades health. | `test_b8_breaker_separation.py` |
| **B9/B10** | Capital reservations could leak; TTL was derived from the wrong timeout. | `[C]` | Fixed. | `test_b9_b10_reservations.py` |
| **B21** | Skip rather than inflate, on the sizing path. | `[C]` | Fixed. | `test_b1_notional.py` |
| **B25/B30/B32** | Rejection-code handling, including 10001 (qty out of bounds). | `[C]` | Fixed. Handled by name. | `test_failure_injection.py` |
| **Equity zero/negative** | Found by **property testing**: `is_usable()` required `equity > 0.0`, so a wiped account read as UNKNOWN, callers fell back to a stale cached balance, and the drawdown breaker never saw a 100% loss. | `[R]` | Fixed. Finite-only; zero and negative are real readings. | `test_b11_equity.py` |
| **Corrupt state blocks startup** | Found by **failure injection**: `load_state` raised on a corrupt cache and is called during symbol initialisation without a guard — one bad entry prevented startup. | `[R]` | Fixed. | `test_failure_injection.py` |

## Measurement and research integrity

| ID | Defect | Class | Status | Test |
|---|---|---|---|---|
| **B27** | `np.mean([])` is `nan`, and `nan` is truthy, so `or 0.0` did not guard it — a regime with no trades emitted `nan` into the metrics. | `[C]` | Fixed. | `test_backtest_integrity.py` |
| **B28** | Sharpe and Sortino annualised with 252 while Calmar used 365 — the ratios were on different clocks and could not be compared. | `[C]` | Fixed. One convention, asserted as a property. | `test_backtest_integrity.py` |
| **B29** | Monte Carlo block-bootstrapped **with replacement** from the realised trade set and reported P(sum > 0). With a positive sample mean that is ≈1 by construction. **It measured nothing.** | `[C]` | Replaced with sequence-risk and execution-cost-risk tests. | `test_b17_b27_b28_b29_backtest.py` |
| **B33** | Slippage was added to PnL with the wrong sign convention. | `[C]` | Fixed. | — |
| **B34** | Seven declared dependencies were never imported — `pybit`, `openai`, `groq`, `aiosqlite`, `asyncpg`, `pycryptodome`, `httpx`. Supply-chain surface for no benefit. (`pybit`, the official Bybit SDK, was declared while the code hand-rolls `/v5` directly.) | `[C]` | Removed. | — |
| **Non-reproducible research** | `backtest.py` re-fetched klines on every run, so the window moved with wall-clock time and no result could be reproduced. | `[C]` | Fixed. `src/research/` — content-addressed datasets, recorded experiments. | `test_research_infra.py` |

## Security and configuration

| ID | Defect | Class | Status | Test |
|---|---|---|---|---|
| **D2** | `/metrics` published `equity` and `wallet_balance` on an unauthenticated endpoint bound to `0.0.0.0`. Anyone with the URL could read the account balance; the counters leak position activity and timing. | `[R]` | Fixed. Token-gated, `hmac.compare_digest`, **closed when unconfigured**. | `test_health_endpoint_exposure.py` |
| **D3** | 28 environment variables governed the system — including the drawdown kill switch and whether real orders are placed — and none were documented. | `[C]` | Fixed. `.env.example` + a drift test in both directions. | `test_env_documentation.py` |
| **Telegram HTML** | Symbol interpolated unescaped into a `parse_mode=HTML` message; a computed-but-unused `safe_reasoning` made the escaping look deliberate. | `[C]` | Fixed. | — |

---

## Open

| ID | Item | Why it is still open |
|---|---|---|
| **B14** | Directional attribution detail. | Partially addressed; full treatment needs live fill data. |
| **B23/B24** | Minor logic items carried from Phase 1. | Low capital risk; not reached in this pass. |
| **IOC status** | Whether a zero-fill IOC returns `Cancelled` or `Rejected`. | Two Bybit doc pages disagree; no live test was possible. **Safety does not depend on it** — closure is gated on a position-size query, not on order status. Resolve on testnet. |
| **mypy** | Not run. | Most call sites are unannotated; a first pass yields thousands of findings and few useful ones. Worth doing as its own project. |
| **CVE scan** | Not run meaningfully. | Would reflect this container's resolved versions, not the deployed ones. Run against `requirements.lock` where it deploys. |

## Three defects were introduced by this audit

B2a, NEW-1..5 and D1 were all created by fixes made during the audit, and all
three were caught by a *different* technique than the one that motivated the
change: a paper-broker review caught the paper broker, static analysis caught
the fix for a freshness bug, and property testing caught an equity guard that
example-based tests had passed.

This is the argument for the staged deployment process, and the reason nothing
in this branch should reach live without a testnet stage.
