# V12 — from "predict 60 seconds" to "trade only when the edge pays for itself"

Branch `phase2/apex-overhaul`. `main` unmodified. All numbers below are from
synthetic markets with a declared ground truth; **no real-market profitability
has been demonstrated**, because no exchange data is reachable from the build
environment. What is demonstrated is narrower and still important: the new
decision system does what it was designed to do when the truth is known.

## What changed, item by item (the owner's 13-point plan)

| # | Plan item | Where | Status |
|---|---|---|---|
| 1 | Multi-horizon prediction 1m/5m/15m/1h/4h | `v12/horizons.py` | Built. One online model + calibrator per horizon, scored prequentially (predict, then learn when the outcome arrives). Overlapping labels are down-weighted to their independent share, so a 4-hour label sampled every 10 s counts as ~1 outcome per 4 h, not 1,440. |
| 2 | Entry on expected value after costs | `v12/edge.py`, `v12/pipeline.py` | Built. EV is simulated for the actual exit policy (1R stop, breakeven+costs at 1R, trail from 1.5R, 3R target), minus fees, spread, volatility-scaled slippage and funding. NO TRADE unless net >= 5 bps. |
| 3 | Volatility-aware stop, hard bounds | `v12/risk.py` | Built. 1.5 sigma x sqrt(horizon), >= 4x round-trip cost, clamped to 0.4%–3%. The exit engine now treats that stop as 1R. |
| 4 | Size by edge AND volatility, hard cap | `v12/risk.py` | Built. Unmeasured edge is trusted at 50%; once 30 similar trades exist, measured edge replaces the model's. Never above 2.5% risk or 25% notional; never rounded up to an exchange minimum. |
| 5 | Regime classifier + per-regime performance | `v12/regime.py`, `v12/learning.py` | Built. Trend up/down, range, mean reversion, breakout, chaotic, x high/normal/low vol. Pure noise classifies as RANGE 88% of the time (was 23% false mean-reversion before tuning). Per-regime results use Bonferroni-corrected significance. |
| 6 | Trade-quality score card | `TradeDecision.render()` | Built. Every entry ticket carries it. |
| 7 | Post-trade learning loop | `v12/journal.py` | Built. Separate SQLite journal: prediction next to outcome, plus rejected decisions (sampled) with their later outcome, plus each trade's price path until 240 min after exit. Rebuilt on restart. |
| 8 | MAE/MFE-designed exits | `v12/exit_optimizer.py`, `scripts/analyze_exits.py` | Built. Walk-forward TRAIN -> TEST over recorded paths; recommends only with a Bonferroni-adjusted paired t on TEST and no worse tail. Writes an UNAPPROVED candidate. |
| 9 | Strategy health monitor with HALT | `v12/health.py` | Built. HALT on reliably negative expectancy or edge below cost; wired to guardian L2. |
| 10 | Kill-switch hierarchy L1–L5, fail closed | `v12/guardian.py` | Built. See `docs/RISK_CONTROLS.md`. Adds a correlated-cluster cap (e.g. DOGE+SHIB+WIF long) that did not exist before. |
| 11 | Exchange position is the truth | `main.py` reconciliation | Wired. Any disagreement blocks new entries until adoption/release resolves it. |
| 12 | Realistic walk-forward backtest | `v12/backtest.py` | Built. Same pipeline and same live exit engine as the bot; fees both legs, spread both legs, vol-scaled slippage, latency, funding, gap-through stops, lot rounding down, participation-capped partial fills, pessimistic intrabar order. |
| 13 | Expectancy-first dashboard | `health.render()`, hourly Telegram | Built. Win rate is printed last on purpose. |
| — | "Don't let learning rewrite live parameters" | `approve_candidate.py`, `evaluate_promotion.py` | Enforced mechanically: candidates are written unapproved and ignored unless approved by a named operator AND pointed to by `EXIT_POLICY_FILE`; LIVE entries require an approved promotion record. |

## Validation (synthetic, walk-forward, TEST segments only)

`scripts/run_v12_validation.py`: 6 seeds x 10,000 minutes (10-second ticks) per
condition; the first half of every run is learning, only the second half is
scored. Conditions and seeds were fixed before any result was seen; every run is
reported, including seeds that produced nothing.

| condition (ground truth) | decisions | TEST trades | expectancy after costs | t | PF | max DD |
|---|---:|---:|---:|---:|---:|---:|
| Pure noise (no edge) | 60,000 | **0** | — | — | — | 0 |
| Noise with fat tails + vol clustering (no edge) | 60,000 | **0** | — | — | — | 0 |
| Mean-reverting, no trend | 60,000 | **0** | — | — | — | 0 |
| Regime switching: trend 1/3 of the time | 52,527 | 48 | +61.2 bps (+0.69R) | 2.90 | 2.83 | 1.3% |
| Weak trend (drift 0.15 sigma) | 57,227 | 20 | +135.5 bps (+1.64R) | 4.42 | 7.66 | 0.6% |
| Trend (drift 0.30 sigma) | 31,837 | 168 | +166.8 bps (+1.86R) | 15.62 | 13.82 | 1.2% |
| Trend, **2x costs + 10 s latency** | 53,276 | 39 | +167.7 bps (+1.22R) | 4.49 | 5.76 | 0.7% |

Raw output: `reports/v12/validation.md` and `reports/v12/part2/validation.md`
(JSON beside each, with the pooled dashboards and per-seed results).

### What this shows

* **It refuses to trade when there is nothing to trade.** 180,000 decisions
  across three no-edge markets — including the fat-tailed, volatility-clustered
  one that is the classic source of false trend signals — produced zero
  entries. The legacy entry logic, on the same kind of noise, traded and lost
  (-15.8 bps/trade, t = -6.11; `reports/instrument_validation.md`). This is the
  owner's "losses should not be a word" translated into the only form that is
  achievable: do not take trades that do not pay for their costs.
* **When a real edge exists it is captured after costs**, and the capture
  survives doubled costs plus 10-second latency.

### What this does NOT show — read before believing the table

1. **These are synthetic markets.** A planted persistent drift is a cleaner
   trend than any real market offers. Real-market profitability is unknown.
2. **The model under-predicts heavily** (predicted +10 to +16 bps, realised +61
   to +168). Two reasons, both by design: calibration shrinks probabilities
   toward 50%, and the EV model assumes the drift lasts only for the forecast
   horizon, while in these markets it persisted for hours. The practical
   consequence is that V12 is **timid**: in the weak-trend market it traded in
   only 1 of 6 seeds; with regime switching, 3 of 6. It will miss real
   opportunities. That is the chosen side of the trade-off — a missed trade
   costs nothing, a bad one costs money — but expect long stretches of NO TRADE.
3. **Results concentrate in some seeds** (e.g. regime switching: two seeds made
   all of it). Small samples; treat t-statistics below ~3 as suggestive.
4. **Mean reversion is refused, not exploited.** V12 does not currently trade
   reversal edges; a mean-reverting market simply produces no trades.
5. **Paper trading is the real test.** The live forecaster learns from the
   market it is given; whether real Bybit perps contain an edge that clears
   ~17 bps of round-trip cost at these horizons is exactly what 100+ paper
   trades must answer. The promotion gate will say NOT ELIGIBLE if they do not,
   and that answer should be accepted.

## Other defects found and fixed while building V12

* **Exit engine R-unit mismatch.** The engine latched `initial_risk_dist` with a
  1.5% floor even when the exchange stop was tighter, so "1R", the breakeven
  trigger and the 3R target were measured against a stop that did not exist.
  Now, when V12 sets the stop, 1R is that stop (`risk_dist_from_v12`). Legacy
  behaviour unchanged when V12 is off.
* **EV simulation 27 ms per call** (90% of backtest time; would have stalled the
  live loop): replaced by a per-cell z-curve simulated once on common random
  numbers and interpolated. 9 µs per call after warm-up; quantisation floors
  sigma/stop so the error is pessimistic; measured optimistic error <= 0.0094R,
  inside the 0.01R conservative margin.
* **Overlapping-cluster blind spot** in my own first guardian draft: WIF sits in
  both the Solana and meme clusters, and first-match lookup let DOGE+SHIB+WIF
  long reach 74% of equity. Now checked against every cluster a symbol is in.
* **Journal method shadowed by an attribute** (`path`) — would have raised on
  the first exit analysis. Found by test.
* **Tick path could block on a decision** computing a fresh EV curve (~0.5 s).
  Ticks now never wait: a tick that arrives while a decision holds the lock is
  not sampled (the forecaster samples every 10 s anyway).
* **API health**: business rejections (e.g. "leverage not modified") are not
  counted as infrastructure errors; only transport failures, rate limits and
  exchange system errors are, so L5 does not trip on normal operation.

## Performance

`observe` 133 µs per tick; `decide` 0.7 ms median, 4 ms p99, up to ~0.5 s the
first time a new volatility cell is seen (runs in a worker thread).

## What happens when you run it

* `V12_MODE=enforce` (default) in PAPER: expect **roughly 2.5 hours of NO TRADE
  after a cold start** while forecasts and the regime classifier measure
  themselves — less with the automatic warm start from 5 hours of 1-minute
  klines. Then expect most decisions to be NO TRADE with the reason on the card.
* In LIVE, every new entry is refused until an approved promotion record exists.
  This is the owner's own rule made mechanical. `ALLOW_UNPROMOTED_LIVE=true`
  overrides it; I recommend against it.
* Hourly Telegram report: strategy health, top rejection reasons, guardian state.

## Tests

70+ new tests (`tests/test_v12_core.py`, `tests/test_v12_safety.py`,
`tests/test_v12_integration.py`) covering every property above, including the
negative ones: no edge on a driftless forecast, noise-only multiple testing,
fail-closed guardian on each infrastructure fault, unapproved candidates never
loaded, LIVE refused without promotion, restart restores learning.
