# Changelog

## [phase2/apex-overhaul] — 2026-09-27 — V12 decision system

The owner's 13-point V12 plan, implemented and wired into the live engine.
Full write-up with every number and every caveat: `reports/v12/V12_REPORT.md`.

### Added
* `src/v12/` — multi-horizon forecaster (1m–4h, prequential, overlap-aware),
  regime classifier, cost model calibrated to live receipts, expected value of
  the actual exit policy after all costs, volatility stops with hard bounds,
  edge-and-volatility sizing, hierarchical learning of realised edge per
  regime, trade journal with post-exit price paths, walk-forward exit optimiser
  (recommend-only), strategy health monitor, guardian kill-switch hierarchy
  L1–L5 with correlated-cluster and gross exposure caps, paper->live promotion
  gate, and a walk-forward backtester sharing the live pipeline and exit engine.
* `V12_MODE=enforce|shadow|off` (default enforce). LIVE entries require an
  approved promotion record.
* Scripts: `run_v12_validation.py`, `evaluate_promotion.py`,
  `analyze_exits.py`, `approve_candidate.py`.
* Hourly expectancy-first health report and a decision card on every ticket.

### Fixed
* Exit engine measured R against a 1.5% floor even when the exchange stop was
  tighter; with V12 setting the stop, 1R is now that stop.
* API transport health (not business rejections) now feeds the guardian.

### Measured (synthetic, walk-forward TEST segments, 6 seeds each)
* No-edge markets (noise; fat-tailed clustered noise; mean-reverting): 180,000
  decisions, **0 trades**.
* Trend markets: +61 to +168 bps/trade after costs (t 2.9 to 15.6), holding up
  under 2x costs + 10 s latency. Synthetic — real-market profitability unknown.

---

## [phase2/apex-overhaul] — 2026-09-27 — live-session fixes

Driven by the owner's live Telegram log: 27 trades, 66.7% winners, average loss
3.60x the average win, profit factor 0.56. Full write-up:
`reports/live/LIVE_SESSION_DIAGNOSIS.md`.

### Changed — exit policy (the cause of the loss)
* New default `ExitPolicyConfig`: breakeven+costs only after +1R; trail 1R
  behind the peak from +1.5R; 22%/28% giveback exits removed; order-flow exits
  only from +1.5R; take-profit 3R on the engine AND the exchange bracket; the
  180-min time stop no longer kills runners. `LEGACY_EXIT_CONFIG` reproduces
  the old ladder exactly, for A/B.
* Measured with the live engine over identical entries on fresh seeds: with a
  moderate trend present, legacy -6.7 bps/trade -> new +24.6 bps (paired
  +31.2, t=13.0); with no edge both lose (difference not significant).

### Fixed
* Closed-PnL fees read a non-existent `execFee` field -> 0.0 on 22/27 receipts.
  Now `openFee + closeFee`.
* Fallback settlement charged only the closing fee. Now both legs.
* Entry ticket: edge was hard-coded 0.0; "Sizing Risk" was intended exposure.
  Now shows the honest 60-second edge vs cost, filled exposure, loss-if-stopped.

### Added
* `core/edge_gate.py` — after-cost edge gate learning from settled trades;
  `EDGE_GATE_MODE` off|shadow|enforce (default shadow).
* `research/exit_lab.py` — drives the live exit function over simulated paths.
* 55 tests (518 total).

### Corrected
* A "true win rate 48.1%" I first reported was wrong: Bybit's closedPnl is
  already net of both fees and funding. Correct figure: 66.7%.

---

## [phase2/apex-overhaul] — 2026-09-17

Full audit, repair and hardening pass over a live Bybit trading system.
Base commit `1c8ac82`. **`main` is unmodified.**

Priority order throughout: Correctness → Risk Control → Robustness → Execution
Quality → Signal Quality → Profitability → Efficiency. Nothing below was done
to make a backtest look better; one optimisation was measured, proven
bit-identical and then rejected on those grounds.

---

### Security

* **Live account balance was readable by anyone with the URL.** The health
  server binds `0.0.0.0` (the host requires it) and `/metrics` published
  `METRICS.snapshot()` unauthenticated — which carries the `equity` and
  `wallet_balance` gauges. Now token-gated with `hmac.compare_digest` and
  **closed when `HEALTH_TOKEN` is unset**, so forgetting to configure it exposes
  nothing. Plain `/health` liveness still works for uptime monitors.
* HTML escaping applied to the symbol in the Telegram entry ticket.
* Secrets scan: clean. `.env.example` is asserted to carry no values.

### Fixed — capital risk

* **B1** Sub-minimum quantity rounded **up**, turning a reproduced $500 order
  into $6,500. Refused outright, with a notional-deviation guard behind it.
* **B2** Position marked `CLOSED` on an order acknowledgement while the exchange
  still held 10 units. Closure now requires an exchange-confirmed zero size.
* **B2a** *(defect in the B2 fix)* Hedge mode could read the opposite side.
  `positionIdx` matched explicitly; a non-match is `None`, never `0.0`.
* **B11** Unrealised PnL counted twice in the figure driving the 15%
  force-flatten. One convention now, stated in `src/equity.py`.
* **NEW-1..5** *(defects in the paper broker written during this audit)*
  `__getattr__` delegated to the live executor — the engine read the live
  balance while paper held its own, and `adjust_leverage` reached the live
  account. Now `PaperIsolationError` outside a read-only allow-list, plus an
  initial-margin solvency check.
* **D1** *(defect introduced during this audit)* `NameError` in the position
  exit loop — `now_sec` referenced five times, never bound. Would have broken
  position management on the first iteration for every open position. Found by
  `ruff`, not by the test suite.

### Fixed — state and accounting

* **Non-finite PnL reaching storage.** SQLite does not store NaN; it writes
  NULL, and every read site coerces NULL to `0.0`. A NaN PnL therefore became a
  *breakeven trade* in the win rate, the expectancy, the Kelly update and the
  model labels — and since `net_pnl > 0` is False for NaN, it was also filed as
  a loss. Now recorded as `UNKNOWN` at the write site and excluded from
  statistics.
* **Equity of exactly zero read as "unknown".** Found by property testing:
  callers fell back to a stale cached balance and the drawdown breaker never saw
  a 100% loss. Negative equity had the same problem. Zero and negative are real
  readings; only non-finite means "no reading".
* **A corrupt model-state cache prevented startup.** Found by failure injection:
  `load_state` raised, and it is called during symbol initialisation without a
  guard. Persisted state is now treated as untrusted input.
* **B12** `UNKNOWN` settlement is no longer scored as a loss.
* **B17** The whitener now travels with the RLS weights across a walk-forward
  fold. Without it the test fold applied frozen weights to differently-scaled
  features — **every OOS Sharpe produced that way was invalid, including the one
  that selected `params.json`.**
* **B18** All four covariance matrices restored, not one.

### Fixed — measurement integrity

* **B29** The Monte Carlo block-bootstrapped **with replacement** from the
  realised trade set and reported P(sum > 0), which is ≈1 by construction for
  any positive sample mean. It measured nothing. Replaced with sequence-risk and
  execution-cost-risk tests.
* **B28** Sharpe/Sortino annualised on 252 while Calmar used 365 — different
  clocks, non-comparable ratios. One convention, asserted as a property.
* **B27** `np.mean([])` is `nan` and `nan` is truthy, so `or 0.0` did not guard
  it; a regime with no trades emitted `nan` into the metrics.
* `summarize()` now publishes `periods_per_year`, so a multiple-testing haircut
  can be expressed in the same units as the annualised Sharpe. Without it the
  deflation is ~50x too small and the gate is decorative. No existing metric
  changed.

### Added — research infrastructure

* `src/research/dataset.py` — content-addressed OHLCV, a local kline cache, and
  synthetic generators that declare their ground truth. Replaces re-downloading
  klines on every run, which made every result irreproducible and required
  exchange egress to do any research at all.
* `src/research/experiment.py` — records commit SHA, dirty-tree flag, dataset
  hash, parameters, model version, cost model, results and validation verdict.
  An omitted cost model does **not** mean free trading. Failed experiments are
  recorded, not discarded. `compare()` blocks apples-to-oranges A/B.
* `src/research/validate.py` — gates defined before results are seen. **A gate
  with no evidence reads as FAILED**, never as skipped.
* `scripts/fetch_klines.py`, `scripts/run_experiment.py`,
  `scripts/reconcile_ledger.py`.

### Added — tests

463 total (457 fast, 6 slow instrument tests).

* **Property-based invariants** (`hypothesis`) — the category that found real
  bugs rather than confirming existing behaviour.
* **Failure injection** — timeouts, partitions, malformed payloads, duplicate
  fills, rate limits, exchange disagreement, database failure, corrupt state.
* **Instrument integrity** — causality by truncation, determinism, fee
  monotonicity, state transfer across the split, frozen weights staying frozen,
  and the null/sensitivity pair. Run in CI; skipping them permanently would void
  every research number.
* **Ledger reconciliation** — ten checks, each with a planted corruption.
* **Configuration drift** — fails if code and `.env.example` disagree in either
  direction.

### Added — documentation

Nine documents in `docs/` and five reports in `reports/`, including an explicit
statement of what could not be established and why.

### Changed

* `.env.example` documents all 28 environment variables, which were previously
  documented nowhere — including the drawdown kill switch and the switch
  deciding whether real orders are placed.
* NumPy 1.25 deprecation fixed in both RLS implementations (~154,000 warnings
  per run). Verified under `-W error::DeprecationWarning`.
* 32 unused imports removed; 2 dead-code sites removed.
* **B34** Seven declared-but-never-imported dependencies removed.

### Not changed, deliberately

* **`main`.** All work is on `phase2/apex-overhaul`.
* **Strategy logic, model parameters, thresholds, TP/SL, leverage and risk
  percentages.** None were tuned. Tuning them without real data would be
  fitting to synthetic series.
* **A 1.2x optimisation of the hottest function** (34% of backtest runtime),
  proven bit-identical over 400 randomised inputs. Rejected: efficiency is the
  lowest-priority objective and a 7% research speedup does not justify touching
  signal-path code.

### Measured

Over 30 seeds x 20,000 bars per generator, pooled across 6,908 individual trades:

* **The instrument is trustworthy.** No look-ahead (causality by truncation),
  costs actually applied, fitted state crosses the split, frozen weights stay
  frozen, and no profit manufactured in noise (-0.001578/trade, t = -6.11).
* **The signal is real.** It separates a planted momentum regime from noise at
  t = +4.12, worth about 13.1 bps per trade.
* **The economics do not work.** A round trip costs 19.0 bps, so on a dataset
  built to favour this strategy, after-cost expectancy is NEGATIVE
  (-0.000273/trade over 5,183 trades).

A correction is recorded in `reports/instrument_validation.md`: an earlier
12-seed result appeared to show the opposite. It averaged per-seed means, which
over-weights low-trade-count seeds. Both of its headline figures were wrong, and
both were wrong in the system's favour. The mistake is left in the report
alongside the correction, because it is the clearest available demonstration of
why the sample-size gate exists.

### Still unknown

Real-market profitability. No exchange data was reachable from the audit
environment. See `reports/instrument_validation.md` and
`docs/KNOWN_LIMITATIONS.md`.

---

## [main] — base `1c8ac82`

395 commits, 2026-06-16 to 2026-09-11. Unmodified by this work.
