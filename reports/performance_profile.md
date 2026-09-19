# Performance Profile

APEX section 16. Profiled with `cProfile` over a 6,000-bar backtest run
(`synthetic:momentum`, seed 1) on the branch `phase2/apex-overhaul`.

**Priority note.** The brief's order is Correctness → Risk Control →
Robustness → Execution Quality → Signal Quality → Profitability → **Efficiency**.
Efficiency is last, and nothing in this document was allowed to change numerics.
One candidate optimisation was measured, proven bit-identical, and then **not
adopted** — the reasoning is below, because a rejected optimisation is a result.

---

## Where the time goes

6,000 bars, 12.2 s total, 11.5 M function calls.

| function | cumulative | share | calls |
|---|---|---|---|
| `run_v40_backtest` (total) | 12.21 s | 100% | 1 |
| `compute_lead_lag_cross_alpha` | 4.12 s | **34%** | 5,899 |
| `BacktestRiemannianRLS.update` | 2.02 s | 17% | 16,064 |
| `np.clip` | 1.09 s | 9% | 144,992 |
| `np.percentile` | 1.06 s | 9% | 5,847 |
| `np.corrcoef` | 1.04 s | 9% | 5,870 |
| `compute_permutation_entropy_dithered` | 0.95 s | 8% | 5,889 |
| `BacktestAdaptiveWhitener.orthogonalize` | 0.74 s | 6% | 5,899 |
| `math.log` | 0.54 s | 4% | **3,462,649** |

The headline number is 3.46 million `math.log` calls — about **576 per bar**.
They come from `compute_lead_lag_cross_alpha`, which rebuilds the entire
rolling log-return series from scratch on every bar in a pure-Python loop:

```python
for i in range(2, len(alt_hist)):
    a_ret = math.log(alt_hist[i] / (alt_hist[i - 1] + 1e-9))
    b_ret = math.log(btc_hist[i - 1] / (btc_hist[i - 2] + 1e-9))
```

Work already done on bars 0..n−1 is discarded and redone at bar n.

---

## The optimisation that was measured and rejected

A vectorised NumPy replacement was written and checked for equivalence over 400
randomised inputs of varying length:

```
trials=400   mismatches > 1e-12: 0   max abs diff: 0.0
orig 0.284s   fast 0.241s   speedup 1.2x
```

**Bit-identical** — maximum absolute difference exactly zero, not merely within
tolerance. And only **1.2x faster**, because at the working window size (30–60
elements) the `deque → ndarray` conversion costs about as much as the loop it
replaces.

1.2x on 34% of runtime is roughly a **7% end-to-end gain** on research runs, in
exchange for touching a function on the signal path. Applying the brief's own
priority order: efficiency is the lowest-ranked objective, correctness the
highest, and a 7% research speedup does not justify modifying code that
computes a trading signal. **Not adopted.**

The version that would actually pay — incremental computation, keeping a
running log-return series and updating only the newest element, which is O(1)
per bar instead of O(n) — is a genuinely larger change with real state-management
risk. It is recorded as a future item, not attempted here.

**Live impact: negligible either way.** This function runs once per closed bar
on a 30–60 element deque. On a 5- or 15-minute timeframe that is one call every
few minutes, against a 50 ms exit-monitoring loop. The cost is a research
throughput problem, not a latency problem.

---

## What this means for research throughput

At roughly **1 second per 500 bars**, a single 20,000-bar backtest takes about
60 seconds. Consequences worth planning around:

* The 12-seed × 3-generator null/sensitivity sweep in
  `reports/instrument_validation.md` took approximately 36 minutes.
* A meaningful parameter sweep — say 27 configurations × 5 folds — is on the
  order of **2 hours** single-threaded.
* The folds and the seeds are independent, so this parallelises almost
  perfectly across cores. Doing that would be worth far more than the 7%
  above, and it changes no numerics at all: it is the recommended first
  optimisation.

---

## Live hot path — not profiled

`cProfile` over a synthetic backtest says nothing about live latency, because
the live path is dominated by things the backtester does not do at all: the
WebSocket feed, REST round-trips to Bybit, `asyncio` scheduling, and the 50 ms
`ACTIVE_MONITORING` sleep.

The instrumentation for measuring it honestly already exists —
`METRICS.latency(...)` in `src/observability.py` records p50/p95/p99 per named
operation, and the counters surface on the (now token-gated) `/metrics`
endpoint. What it needs is a live or testnet session to produce data.

Profiling the live path from a backtest and presenting the result as latency
evidence would be exactly the sort of substitution this audit is meant to
prevent, so it is not done here.
