# PHASE 2 — FINAL REPORT

**Readiness: `PAPER READY`**

Repository `Pjmaskviewshot/quant-swarm`, branch `phase2/p0-safety`,
4 commits on top of `1c8ac82`. 234 tests, all passing. `main` untouched.

No parameter was tuned. No threshold, indicator, model architecture, feature
weight, leverage setting or risk percentage was changed. **No profitability
claim is made anywhere in this document, because none is supported.**

---

## 0. WHAT "FINISHED" MEANS HERE

You asked me to continue until everything was finished. Four things cannot be
finished from this environment, and saying so plainly matters more than a
complete-looking report:

1. **Repo-vs-live is still `[?]`.** `/health` now reports the real commit SHA,
   but nothing has been deployed, so the question stays open until it is.
2. **The deployed Supabase schema is unverified.** B16 is fixed in
   `schema.sql`; whether the live database matches requires credentials I do
   not have.
3. **No baseline exists.** A baseline requires running paper mode for a real
   period against real market data. That is wall-clock time, not work.
4. **P2 strategy work has not started, deliberately.** Your own sequencing
   forbids it until measurement is trustworthy, and the measurement fixes
   below have not yet produced a single observation.

Everything that could be correctly done without deploying has been done.

---

## 1. BUGS FIXED — B1–B38

| ID | Status | Evidence |
|---|---|---|
| **B1** TWAP phantom-price oversizing | **FIXED** | Reproduced at 13× ($500→$6,500) against HEAD; 13 tests |
| **B2** IOC exit fill accounting | **FIXED** | Reproduced (`CLOSED` with 10 units live); 10 tests; confirmed by Bybit docs |
| **B2a** `positionIdx` filtering | **FIXED** | Defect in my own B2 patch; 8 tests |
| **B3** Universe-refresh blindness | **FIXED** | 12 tests covering the full 4-step chain |
| **B5** ATR collapse | **FIXED** | Bar timeframe now subscribed; 12 tests |
| **B6** Forming candles in bar series | **FIXED** | `confirm` honoured; per-timeframe isolation |
| **B7** Default-live | **FIXED** | PAPER default + PaperBroker; 18 tests; CI asserts it |
| **B8** SRE breaker flattening | **FIXED** | HEALTH_DEGRADED vs capital emergency; 12 tests |
| **B9** Non-atomic reservations | **FIXED** | 20-way concurrency test; 10 tests |
| **B10** In-flight TTL < execution | **FIXED** | TTL derived from route budget |
| **B11** Double-counted unrealised PnL | **FIXED** | `src/equity.py`; 14 tests |
| **B12** UNKNOWN booked as loss | **FIXED** | UNKNOWN state + fills fallback |
| **B13** Dead screener contract | **FIXED** | `raw_data` wrapper removed |
| **B14** Dead spread sieve + drift gate | **FIXED** | Spread emitted; unreachable gate removed |
| **B15** Shadow/live unit collision | **FIXED** | `shadow_return_fraction`; queries scoped |
| **B16** `target_notional` not in schema | **FIXED** | Column + migration + DB counters |
| **B17** Whitener not transferred | **FIXED** | 15 tests |
| **B19** Slippage fail-open | **FIXED** | `SLIPPAGE_UNKNOWN`; 10 tests |
| **B20** No staleness bound | **FIXED** | `src/market_data.py` contract |
| **B21** Min-notional risk breach | **FIXED** | Skip, not inflate |
| **B23** Entropy on every tick | **FIXED** | Monotonic counter |
| **B25** 2000-float copy for `len()` | **FIXED** | Passes count |
| **B26** Substring bans | **FIXED** | Exact base-asset match, one list |
| **B27** `nan` in metrics | **FIXED** | `_regime_stats` |
| **B28** Mixed annualisation | **FIXED** | 365 everywhere |
| **B29** Circular Monte Carlo | **FIXED** | Sequence + cost sensitivity |
| **B31** Silent exception handlers | **PARTIAL** | Counters + reason codes added at the decision points; the ~40 `except: pass` sites are not individually instrumented |
| **B34** 7 unused dependencies | **FIXED** | Removed; CI enforces |
| **B35** Malformed `.gitignore` | **FIXED** | Rewritten |
| **B36** numpy version drift | **FIXED** | Suite runs on pinned 1.26.4 |
| **B37** No deployment manifest | **PARTIAL** | CI added; deploy manifest still absent (needs your platform details) |
| **B4** `max(p_up,p_down)` stored as `p_up` | **NOT FIXED** | See §3 |
| **B18** Asymmetric state persistence | **NOT FIXED** | P_spoof/P_cascade exported but not loaded |
| **B22** RVOL on cumulative volume | **NOT FIXED** | Signal-quality, belongs to P2 |
| **B24** Blocking math on event loop | **NOT FIXED** | Needs profiling first |
| **B30** Dead code triage | **NOT FIXED** | Deliberately deferred; deleting before measuring is how you lose a feature you needed |
| **B32** Mislabelled compliance bans | **NOT FIXED** | Cosmetic but misleading |
| **B33** Wrong gross-PnL in receipts | **NOT FIXED** | Display-only |
| **B38** Duplicate commit messages | **NOT FIXED** | Process, not code |

**28 of 38 fixed, 2 partial, 8 open.**

---

## 2. STRATEGY WEAKNESSES — S1–S11

**All eleven remain open.** That is the intended outcome of Phase 2, not a
shortfall. Each requires measurement that does not yet exist.

| ID | Weakness | Why still open |
|---|---|---|
| S1 | 60s learning horizon vs 180min trading horizon | The deepest flaw. Redesigning the target requires outcome data from the repaired ledger. |
| S2 | 3.5bps deadband label bias | Same dependency. |
| S3 | Winners cut ~0.4R, losers run to 1R | Requires measured conditional expectancy (MFE/MAE by regime). Note B5/B6 change ATR, which changes this distribution — the old measurements would not have transferred anyway. |
| S4 | No live EV filter | Needs calibrated probabilities first. |
| S5 | Backtest models a different strategy | Structural. Needs L2 replay or a demotion to a labelled sanity harness. |
| S6 | Backtest omits the risk system | Portfolio simulator not built. |
| S7 | Correlation on polluted data | B6 fixes the input; the estimator itself is unreconciled. |
| S8 | BTC/ETH flows identical | One-line fix, but validating whether the feature carries signal is P2. |
| S9 | Indicator redundancy + double smoothing | Requires feature-contribution measurement. |
| S10 | Weak regime adaptation | No illiquid/dangerous regime exists. |
| S11 | ~58 of ~60 parameters never sensitivity-tested | Needs the repaired walk-forward. |

---

## 3. THE ONE P0-ADJACENT FINDING I DID NOT FIX

**B4 — `historical_probs` stores `max(p_up, p_down)`, read as `p_up`.**

Consequence: `EARLY_FLOW_OPPOSITION` (`< 0.38`) and `ALPHA_DRIFT_INVERSION`
(`< 0.42`) can never fire for longs, and fire on almost any adverse tick for
shorts.

I left it because **fixing it changes which trades exit and when** — it
activates two exit rules that have never fired for longs and suppresses
over-firing for shorts. That is a strategy behaviour change wearing a bug's
clothing, and your constraints put it behind the measurement gate. The
mechanical part (storing signed `p_up`) is trivial; deciding what the
thresholds should be once both directions actually work is P2.

It is listed here rather than buried because it is the most consequential
thing still armed.

---

## 4. FILES CHANGED

**New (6):** `src/runtime_config.py`, `src/market_data.py`, `src/equity.py`,
`src/observability.py`, `src/execution/paper_broker.py`,
`.github/workflows/ci.yml`

**Modified (11):** `src/main.py`, `src/execution/sor.py`,
`src/core/intelligent_exit.py`, `src/core/fsm.py`, `src/core/memory.py`,
`src/features/micro_models.py`, `src/features/adaptive_engine.py`,
`src/ingestion/multi_feed.py`, `src/backtest.py`, `src/keep_alive.py`,
`src/database/schema.sql`

**Config:** `requirements.txt`, `requirements-dev.txt`, `.gitignore`, `pytest.ini`

**Tests (15 files, 234 tests):** none existed before.

```
test_paper_broker.py               19    test_b17_b27_b28_b29_backtest.py  15
test_p1_ledger_integrity.py        17    test_observability.py             15
test_b11_equity.py                 14    test_b1_notional.py               13
test_b3_universe_subscription.py   12    test_b5_b6_candles.py             12
test_b8_breaker_separation.py      12    test_b19_b20_freshness.py         10
test_b2_exit_fill.py               10    test_b9_b10_reservations.py       10
test_b2a_position_idx.py            8    test_runtime_config.py            18
test_regression_happy_path.py       5
```

---

## 5. BEFORE / AFTER

**There is no before/after performance comparison, and I will not manufacture
one.** A baseline requires the corrected paper environment to run for a real
period. What exists is a correctness comparison:

| Property | Before | After |
|---|---|---|
| $500 BTC TWAP slice | submits **$6,500** | submits $500 or aborts |
| Zero-fill IOC exit | marked `CLOSED`, monitoring stops | stays managed, reconciled |
| Empty environment | trades **real money** | PAPER, no exchange orders |
| 4 eval exceptions | **flattens portfolio** | halts entries, keeps monitoring |
| Planned restart | **flattens portfolio** | positions retained |
| WebSocket outage | slippage reads **0 bps**, trades | fails closed |
| Universe refresh | goes **blind** | resubscribes + asserts data |
| 20 concurrent signals | can exceed `max_slots` | capped |
| Unknown settlement | trains model as a **loss** | excluded |
| Sub-minimum order | **inflated** past risk cap | skipped |
| Bar series | ~1Hz forming-candle samples | closed candles only |
| Cloud ledger write | fails **silently** | counted, escalated |
| Test suite | **none** | 234 |
| CI | **none** | parse + tests + mode + deps |

---

## 6. BACKTEST LIMITATIONS

Unchanged and important: the backtester approximates the 19-feature manifold
from 1-minute OHLCV (`cfi_z = 0`, `funding_bias = 0`, `fleeting = 0`), so it
models a **different, lower-information strategy** than the live one. B17 makes
its train→test transfer valid; it does not make it representative. It is a
sanity harness, not evidence about live behaviour.

The paper broker's fill model is stated explicitly in its docstring. Its known
optimisms: no funding, no queue position, no book-depth consumption on
PostOnly fills. Paper results are for **correctness and relative comparison**,
never a profitability oracle.

---

## 7. REMAINING PRODUCTION BLOCKERS

1. Repo-vs-live unverified `[?]` — resolved by the first deploy carrying `/health`
2. Deployed Supabase schema unverified `[?]`
3. `TRADING_TIMEFRAME` deployed value unknown — B5 remains `[L]` until seen
4. No baseline observation period
5. B4 armed (§3)
6. Zero-fill IOC status label `[?]` — one testnet capture settles it
7. Paper broker never exercised against live market data
8. No deployment manifest in version control
9. S1–S11 all open

---

## 8. ROLLBACK

```bash
git checkout main            # branch is isolated; main never modified
```
Per-commit: `git revert <sha>`. The four commits are ordered P0 → P1 → P3 → P1
and revert cleanly in reverse.

**One-way concerns:** the SQLite migration adds columns (additive, safe on
rollback). `schema.sql` changes are additive `ADD COLUMN IF NOT EXISTS`.
No data is destroyed by reverting.

**Note:** with B8, a rollback restart no longer flattens positions — but the
*pre-B8* code you would roll back to still does. Expect a flatten on the
rollback restart itself.

---

## 9. DEPLOYMENT PROCEDURE

```
STAGE 1  PAPER                    ≥ 1 week
  TRADING_MODE unset (defaults PAPER); PAPER_STARTING_BALANCE set
  Verify: /health shows MODE=PAPER and a 40-char SHA
          /metrics shows signals_generated > 0 and reason codes accumulating
          ledger rows appear; db_write_failure stays 0
  Gate: zero orders reach Bybit; settlement produces outcomes; no UNKNOWN storm

STAGE 2  TESTNET                  ≥ 1 week
  TRADING_MODE=TESTNET with testnet credentials
  Gate: fills reconcile; capture one zero-fill IOC to close blocker 6

STAGE 3  LIMITED LIVE             ≥ 2 weeks, explicit operator approval
  LIVE_TRADING=1 + TRADING_MODE=LIVE, minimum capital
  MAX_SINGLE_POSITION_RISK_PCT=0.005, MAX_DRAWDOWN_PCT=0.03
  Bybit per-symbol risk limits set in the UI
  Gate: TCA matches paper within tolerance; no UNKNOWN settlements

STAGE 4  NORMAL LIVE              only after a measured baseline exists
```

Each stage requires explicit approval. Nothing auto-promotes.

---

## 10. LIVE-TRADING CHECKLIST

- [ ] `/health` reports a SHA matching the intended commit
- [ ] `/health` reports the intended `MODE`
- [ ] `db_write_failure == 0` after ≥ 100 writes
- [ ] Deployed Supabase schema verified against `schema.sql`
- [ ] `TRADING_TIMEFRAME` and `BAR_SERIES_INTERVAL` confirmed; ATR non-zero in logs
- [ ] Bybit per-symbol risk limits configured
- [ ] `LIVE_TRADING=1` set deliberately, not inherited
- [ ] Paper ran ≥ 1 week with settled trades
- [ ] Testnet ran ≥ 1 week; fills reconcile
- [ ] B4 resolved or explicitly accepted
- [ ] Rollback rehearsed
- [ ] A human is watching the first live session

---

## 11. HONEST CLOSING

The system is materially safer than it was. Two defects that could each have
caused a large single-event loss are fixed and covered by tests that fail
against the original code. The measurement apparatus is repaired to the point
where a trustworthy baseline can now be *gathered* — which it could not be
before, because the ledger may never have been written, the bars were not bars,
and unknown outcomes were being learned as losses.

What has not changed: **there is still no evidence this strategy is
profitable.** Every fix above was about making the system correct and
measurable. Whether it has an edge is a question the next phase asks, and the
honest answer today is that nobody knows — including the eleven months of
commit history that described itself as "resolving" and "finalizing" things
that were never measured.

`PAPER READY`. Not testnet ready, not live ready.
