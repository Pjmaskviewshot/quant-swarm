# FINAL APEX REPORT

**Repository:** `Pjmaskviewshot/quant-swarm`
**Branch:** `phase2/apex-overhaul` (base `1c8ac823`) — **`main` is unmodified**
**Date:** 2026-09-17

---

## Update 2026-09-27 — V12

Entries are now decided by the V12 system (`reports/v12/V12_REPORT.md`):
expected value of the whole trade after all costs, multi-horizon forecasts
measured prequentially, volatility stops, edge-and-volatility sizing, and a
fail-closed kill-switch hierarchy. On synthetic ground truth it took zero trades
in 180,000 no-edge decisions and was profitable after costs where an edge
existed. **LIVE remains NOT READY** — now enforced in code: the guardian refuses
LIVE entries until 100+ paper trades pass `scripts/evaluate_promotion.py` and a
named operator approves. Readiness below is unchanged in substance.

---

## Readiness

| Stage | Verdict |
|---|---|
| **PAPER** | **READY** |
| **TESTNET** | **READY** |
| **LIVE** | **NOT READY** |

### PAPER — READY

The defects that made paper mode untrustworthy were in the paper broker itself,
and they were mine. `__getattr__` fell through to the live executor, so the
engine read the live wallet balance while the paper book held its own figure,
and `adjust_leverage` reached the live account. A $100 paper account opened a
$1M position and went to −450.

Fixed: `PaperIsolationError` on any attribute outside an explicit read-only
allow-list, an initial-margin solvency check, and a property test asserting that
no undeclared attribute name reaches the live executor.

Paper mode is now a valid plumbing test. **Paper PnL is not a profitability
estimate** — funding, queue position, L2 matching, market impact, liquidation,
latency and depth-driven partial fills are all unmodelled, and the broker's own
docstring lists them.

### TESTNET — READY

All capital-risk defects are fixed and tested; mode resolution is centralised
and cannot infer LIVE from the presence of credentials; the exit path no longer
closes a position on an acknowledgement; freshness gates both entry and exit.

Testnet is where the remaining unknowns get resolved, and there are real ones:
the `Cancelled` vs `Rejected` question for a zero-fill IOC is still open because
two Bybit documentation pages disagree, and several defects in this audit were
only observable against a genuine exchange response.

### LIVE — NOT READY

**Not because a defect is outstanding. Because the economics do not work in
simulation, and have never been measured on real data.**

On a synthetic series *constructed to favour this strategy*, after-cost
expectancy is negative over 5,183 trades. The signal is real — it separates
momentum from noise at t = +4.12 — but it is worth ~13 bps per trade against a
19 bps round trip. Real markets do not offer a cleaner momentum regime than a
planted AR(1) with φ = 0.35, no gaps and no adverse selection.

That is not proof the system loses money live. Synthetic series are not
markets, and the trade population a real feed generates could differ. It is,
however, the opposite of encouraging, and it is the single most important thing
in this report.

The audit environment had no access to `api.bybit.com` — the proxy returns
`403 CONNECT`, a policy denial, verified against its own status endpoint — so
every measured number here comes from synthetic series with known ground truth.

Two further reasons, independent of the economics:

* **Three defects in this audit were introduced by the audit itself**, and each
  was caught by a different technique than the one that motivated the change. A
  staged rollout is the only honest response to that.
* **The system trades 0.3–0.6% of bars.** Reaching a statistically meaningful
  100 trades takes two to three weeks of continuous data per symbol. Live is a
  slow and expensive place to discover an edge does not exist.

**What would change this verdict:** run `scripts/fetch_klines.py` and
`scripts/run_experiment.py --split` on real BTCUSDT/ETHUSDT data from a machine
with exchange egress. It takes about ten minutes. Given the synthetic result,
the specific thing to look for is whether the per-trade edge on real data
exceeds 19 bps — not whether the backtest total is positive.

---

## Was profitability demonstrated?

**No — and on synthetic data it was actively refuted.** Stated plainly because
the brief asked for it plainly, and because the answer got worse as the evidence
got better.

What *was* demonstrated is that the instrument measuring profitability is
trustworthy. Having built a trustworthy instrument, the first thing it measured
was that the strategy does not clear its own costs on a dataset built to suit
it.

| Claim | Status |
|---|---|
| The backtester has no look-ahead bias | **Established** (causality by truncation) |
| Costs are actually applied | **Established** (fee monotonicity) |
| Fitted state crosses the train/test split | **Established** |
| Frozen weights stay frozen out of sample | **Established** |
| It finds no edge in pure noise | **Established** (1,725 trades, t = −6.11) |
| Its signal discriminates momentum from noise | **Established** (+13.1 bps/trade, t = +4.12) |
| **It is profitable on a strong planted edge** | **REFUTED** (−0.000273/trade after costs, 5,183 trades) |
| It has an edge on real markets | **UNKNOWN — never tested** |
| It is profitable live | **UNKNOWN — never tested** |

### The result that matters most

Given a deliberately planted AR(1) momentum edge — a gift of a dataset, with no
gaps, no outages, no adverse selection, and a persistent φ = 0.35
autocorrelation no real market sustains — the system's after-cost expectancy
over 5,183 pooled trades is **negative**.

The signal is real: it separates momentum from noise at t = +4.12, worth about
**13.1 bps per trade**. The round trip costs **19.0 bps per trade**. The
information the model extracts is not worth what it costs to act on it, by a
margin of roughly 6 bps.

This does not mean the strategy is worthless. It means the gap between signal
strength and transaction cost is the problem to solve, and it is a specific,
measurable problem rather than a vague one. `reports/instrument_validation.md`
sets out the three plausible routes — trade less often on stronger signals, pay
maker rather than taker fees, or hold for larger expected moves — none of which
were attempted here, because tuning against a synthetic generator is fitting to
the generator.

**A correction.** An earlier draft of this report stated that the system detects
the planted edge with 11/12 seeds positive at p = 0.0032. **That was wrong, and
wrong in the system's favour.** It averaged per-seed means across 1,424 trades,
which over-weights seeds that produced very few trades. The pooled figure over
5,183 trades reverses it. The same weighting error had also made the
random-walk result read +0.000019 instead of −0.001578. Two defensible-looking
methods, one of them right, and the wrong one pointed the flattering way both
times. It is corrected here rather than quietly amended.

---

## What was done

**Repaired.** Eleven capital-risk and state-integrity defects, each with a
regression test named after the failure rather than the function. Full list and
evidence class in `reports/defect_register.md`. The ones with the largest blast
radius:

* **B1** — quantisation turning a reproduced $500 order into $6,500.
* **B2** — a position marked `CLOSED` while the exchange still held 10 units.
* **B11** — unrealised PnL double-counted in the figure driving the 15%
  force-flatten.
* **B17** — the whitener not travelling with the RLS weights across a fold,
  which invalidated **every out-of-sample Sharpe the parameter sweep produced,
  including the one that selected the live parameters**.
* **B29** — a Monte Carlo that bootstrapped with replacement from the realised
  trades and reported P(sum > 0), which is ≈1 by construction. It measured
  nothing and was reported as robustness evidence.

**Hardened.** Paper isolation, non-finite PnL rejected at the write site,
persisted state treated as untrusted, health endpoints failing closed, stale
data gating both entry and exit.

**Tested.** 463 tests, from 0. Three kinds, deliberately: example, property
(`hypothesis`), and failure injection. The property and injection categories
found real bugs rather than confirming existing behaviour.

**Measured.** Six instrument-integrity tests plus a 30-seed null/sensitivity
study. Full method and results in `reports/instrument_validation.md`.

**Documented.** Nine documents, five reports, all 28 environment variables, and
a drift test that fails the build when code and documentation disagree.

---

## What was deliberately NOT done

**Strategy logic, model parameters, thresholds, TP/SL, leverage and risk
percentages were not tuned.** Tuning them against synthetic series would be
fitting to a generator, and tuning them against no data at all would be
guessing. The brief's own priority order puts profitability below correctness
and risk control; there was nothing honest to do here yet.

**A 1.2x optimisation of the hottest function was rejected.** 34% of backtest
runtime is one function making 3.46M `math.log` calls. A vectorised replacement
was written and proven bit-identical over 400 randomised inputs — maximum
absolute difference exactly 0.0 — and then not adopted, because a 7% research
speedup does not justify modifying signal-path code when efficiency is the
lowest-ranked objective. A rejected optimisation is a result
(`reports/performance_profile.md`).

**`mypy` was not run.** Most call sites are unannotated; a first pass yields
thousands of findings and few useful ones. Said plainly rather than reported as
a clean run on a configuration tuned until it passed.

**The live hot path was not profiled.** Profiling a synthetic backtest and
presenting it as latency evidence would be exactly the substitution this audit
exists to prevent. The instrumentation exists (`METRICS.latency`); it needs a
live session.

---

## Things that reflect badly and are recorded anyway

**Three defects were introduced by this audit.** The paper broker's live
delegation, the `positionIdx` filter missing from my own B2 fix, and a
`NameError` in the position exit loop that would have broken position management
on the first iteration for every open position. The last was found by `ruff` in
under a second, and my own tests missed it because they exercised the extracted
pure function rather than its call site.

**A test I wrote was wrong and failed for the wrong reason.** It asserted that a
regime-shift dataset must degrade across its halves, but ran the halves
independently with no state transfer — so it compared two regimes rather than
in-sample against out-of-sample. Replaced, and recorded in
`reports/instrument_validation.md` rather than quietly deleted.

**A naive statistic pointed the flattering way.** Averaging per-seed means gave
+0.000019 per trade on pure noise; pooling all 1,725 trades gave −0.001578 with
t = −6.11. Both were computed; the pooled one is correct; the difference is
small-sample weighting. Had only the first been run, the report would have said
something mildly wrong in the system's favour.

**The test suite ran against newer dependencies than the pins request.** aiohttp
3.14.3 against a pinned 3.9.5, pandas 3.0.2 against 2.2.2, Flask 3.1.3 against
3.0.3, and `supabase` not installed at all — so the PostgREST write path ran
only through its failure branches. The pinned set has not been verified. This is
in the header of `requirements.lock` rather than buried.

---

## Deliverables

| | |
|---|---|
| `APEX_FINAL/` | complete package — source, tests, scripts, docs, reports, configs |
| `APEX_FINAL_<commit>.zip` + `.sha256` | archive with per-file `MANIFEST.sha256` |
| `docs/` × 9 | architecture, risk, config, ops, deployment, testing, research, limitations, history |
| `reports/` × 5 | instrument validation, defect register, QA, profiling, raw sweep data |
| Git bundle + format-patches | delivery path, since push is blocked (below) |

**No credentials, keys, secrets, tokens or database files are included.** The
packaging script scans for them and aborts the build if any are found;
`.env.example` is asserted by test to carry no values.

---

## Delivery

Pushing to `github.com/Pjmaskviewshot/quant-swarm` is blocked by this session's
git proxy:

```
remote: access denied by the git proxy:
Pjmaskviewshot/quant-swarm is not in this session's authorized repository set
```

The proxy's own remedy is to add the repository to the session's sources. Until
then the branch is delivered as a git bundle and as `format-patch` files. To
apply:

```bash
git fetch /path/to/quant-swarm-apex.bundle phase2/apex-overhaul:phase2/apex-overhaul
git checkout phase2/apex-overhaul
git push -u origin phase2/apex-overhaul
```

Review the diff before merging. `main` is untouched, so nothing is at risk
until you choose to merge.

---

## Recommended next steps

1. **Fetch real data and run the experiment harness.** Ten minutes on a machine
   with exchange egress. Look specifically at whether per-trade edge exceeds 19
   bps, not at whether the backtest total is positive.
2. **Attack the cost gap before anything else.** It is the binding constraint,
   and it is measurable: try a higher entry threshold (fewer, stronger signals),
   measure the fill-rate cost of PostOnly routing against the 3.5 bps per side
   it saves, and check whether longer holds earn more per unit of friction. Each
   is a hypothesis the experiment harness can now test and record honestly.
3. **If real data shows the same gap**, that is the more valuable outcome, and it
   cost nothing but ten minutes.
4. **If the gates pass**, run 72 hours of PAPER, then TESTNET to the exit
   criteria in `docs/DEPLOYMENT.md`.
5. **Add a cross-symbol correlation limit before live.** Twelve correlated alts
   at 2.5% risk each is 30% directional exposure that no current control sees.
6. **Parallelise the research loop** across seeds and folds. A 27-configuration
   × 5-fold sweep is about two hours single-threaded, it parallelises almost
   perfectly, and it changes no numerics.

---

## The one-line version

The system is materially safer than it was and its measurement apparatus is now
trustworthy — and the first thing that apparatus measured is that the strategy's
signal, while real, is worth about 13 bps per trade against a 19 bps cost to
trade it.
