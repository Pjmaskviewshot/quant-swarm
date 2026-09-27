# Instrument Validation

**What this is:** evidence that the backtester does not cheat.
**What this is not:** evidence that the strategy makes money.

Those are different claims and this document only supports the first. No real
market data was available in this environment (see *Limits*), so nothing here
says anything about BTCUSDT, about live performance, or about profitability.

Generated on branch `phase2/apex-overhaul`. Reproduce with:

```
pytest tests/test_backtest_integrity.py -m slow -v
python scripts/run_experiment.py --dataset synthetic:random_walk --name null-check
```

---

## Why validate the instrument at all

A backtest number is a measurement. Before treating any measurement as
evidence you have to show the instrument is not manufacturing it. The four
classic ways a trading backtester manufactures profit are look-ahead bias,
optimistic fills, under-modelled costs, and a train/test split that leaks. Each
produces a confident, precise, entirely fictional Sharpe ratio.

The tests below are run against `src/backtest.py` — the engine that actually
produces the research numbers — not a simplified reimplementation, because
validating a different simulator would prove nothing about this one.

---

## 1. Causality — no look-ahead

**Method.** Run the backtester over bars `[0, 6000)`. Run it again over bars
`[0, 8000)`. Every trade that closed inside the first window must appear in the
second with the same bar index, the same direction and the same PnL to within
floating-point equality. If any indicator, label, exit rule or fill assumption
read forward even one bar, the longer run would have had information the
shorter one did not, and the two trade lists would diverge.

This is the strongest look-ahead test available without instrumenting every
feature individually, because it does not require guessing where the leak might
be.

**Result: PASS.** Trade indices, directions and PnL are identical across the
truncation boundary.

`test_no_look_ahead_truncating_the_future_cannot_change_the_past`

## 2. Determinism

Identical input produces identical output. Without this, no comparison between
two runs means anything, because the difference could be noise in the engine
rather than in the strategy.

**Result: PASS.**

## 3. Costs are actually applied

**Method.** Raise taker fee, maker fee and slippage tenfold and re-run the same
data. Expectancy must fall.

This catches a specific and common silent defect: a cost model that is defined,
documented and wired up, but never actually subtracted. Such a backtest reports
the same number whatever you set the fee to, and nobody notices because the
fee is visible in the source.

**Result: PASS.** Expectancy falls when costs rise.

Round-trip friction at the production schedule, stated once so nothing
downstream can assume a smaller number:

| component | bps |
|---|---|
| taker fee in | 5.5 |
| taker fee out | 5.5 |
| slippage in | 4.0 |
| slippage out | 4.0 |
| **round trip** | **19.0** |

Any claimed edge has to clear 19 bps before it is an edge at all. The
`cost_survival` gate requires 2x that, because an edge at 1.1x costs
disappears on one bad fill.

## 4. Fitted state actually crosses the train/test split

**Method.** Fit on the train fold, then run the test fold twice: once cold, once
warm-started from the fitted state with `freeze_weights=True`. The two must
differ.

If they do not, `initial_rls_state` is being ignored and every "out-of-sample
with frozen weights" number the parameter sweep produced was really just a
second cold run — which is the B17 defect, asserted here as a property rather
than by reading the code.

**Result: PASS.** Warm-started and cold runs differ.

## 5. Frozen weights are genuinely frozen

With `freeze_weights=True`, the RLS weight vectors (`w_trend`, `w_range`,
`w_spoof`, `w_cascade`) must be bit-identical before and after the test fold.
If they move, the out-of-sample fold is fitting on its own out-of-sample data.

**Result: PASS.** All four weight vectors unchanged.

---

## 6. Null hypothesis — does it find profit in noise?

This is the gate that governs all the others. On a driftless geometric random
walk there is nothing to find. After costs, expectancy must not be positive. A
positive result here would not mean the strategy is good on noise; it would mean
the measurement is broken, and every other number in this document would be
void.

**Method.** 12 independent seeds, 20,000 one-minute bars each, production cost
model, no parameter tuning of any kind.

| generator | seeds | trades | mean trades/seed | mean expectancy | median | seeds positive | sign-test p |
|---|---|---|---|---|---|---|---|
| random walk (**no edge**) | 12 | 607 | 50.6 | **+0.000019** | −0.000379 | 5 / 12 | 0.81 |
| AR(1) momentum, φ=+0.35 (**known edge**) | 12 | 1,424 | 118.7 | **+0.003396** | +0.002411 | **11 / 12** | **0.0032** |
| AR(1) mean-reversion, φ=−0.35 (**known edge**) | 12 | 964 | 80.3 | −0.000099 | −0.000257 | 5 / 12 | 0.81 |

> **⚠ This table is superseded. Read the pooled section below before drawing any
> conclusion from it.** It averages per-seed means, which over-weights seeds that
> produced very few trades. Both of its headline numbers are wrong, and both are
> wrong in the system's favour. It is left in place because the correction is
> the most instructive result in this document, and deleting the mistake would
> remove the evidence for it.

Read naively, the table says the instrument finds nothing in noise (+0.000019,
5/12 seeds positive) and finds the planted momentum edge (11/12 seeds, p =
0.0032). Both readings collapse under a correctly weighted test.

### Pooled trade-level test — the statistically correct version, and a correction

The table above averages **per-seed means**, which over-weights seeds that
produced very few trades — exactly where sampling noise is largest. Two seeds
with 10 trades each carried as much weight as one with 265.

That weighting was not a rounding detail. It reversed the conclusion.

Repeated over **30 independent seeds × 20,000 bars per generator**, pooling every
individual trade's realised net rather than each seed's mean:

| generator | trades pooled | mean net/trade | sd | t vs zero |
|---|---|---|---|---|
| random walk (**no edge**) | 1,725 | **−0.001578** | 0.010727 | **−6.11** |
| AR(1) momentum, φ=+0.35 (**known edge**) | 5,183 | **−0.000273** | 0.013185 | −1.49 |

**Null: PASS, decisively.** On pure noise the system loses money, with high
significance (t = −6.11, p < 10⁻⁹). That is the correct behaviour: with no edge
to find, a strategy pays the round trip on every trade and bleeds. An instrument
that merely broke even here would still be suspicious; one that profited would
be broken.

**Discrimination: PASS.** The difference between the two generators is
**+0.001305 per trade, t = +4.12**. The system genuinely distinguishes a
momentum regime from noise — that is a real, statistically strong signal, and it
is what the strategy is supposed to do.

**Profitability on a known edge: FAIL.** And this is the finding that matters.
Even given a strong, deliberately planted AR(1) momentum edge, after-cost
expectancy is **negative** (−0.000273/trade, t = −1.49 — not distinguishable from
zero, and certainly not positive).

The arithmetic is blunt:

```
signal's edge over noise     +13.1 bps per trade
round-trip cost (19 bps)     -19.0 bps per trade
                             ─────────────────────
                                -5.9 bps per trade
```

**The cost of trading consumes the entire detected edge, and more.** On a
synthetic series constructed to be maximally favourable to a momentum system —
no gaps, no outages, no adverse selection, a persistent φ=0.35 autocorrelation
that no real market sustains — the strategy still does not clear its own
frictions.

### A correction to an earlier version of this document

An earlier draft of this report, written from the 12-seed table, said:

> **Sensitivity: PASS.** On a planted momentum edge it finds one: 11 of 12 seeds
> positive, p = 0.0032, mean expectancy roughly 180x the noise figure.

**That statement was wrong**, and it was wrong in the system's favour. It came
from averaging per-seed means over 1,424 trades in 12 seeds. The pooled figure
over 5,183 trades in 30 seeds says the opposite.

Both numbers were computed honestly from the same code. The difference is
entirely small-sample weighting — the same effect that made the random-walk
figure read +0.000019 (flattering, wrong) instead of −0.001578 (correct). Two
defensible-looking methods; only one of them right; and the wrong one happened
to point the flattering direction both times.

It is corrected here rather than quietly amended, and it is the most direct
demonstration available of why the `sample_size` gate exists and why the
`n_trials` deflation matters.

### What the corrected picture says about the strategy

Separating the two claims the pooled test makes, because they point in opposite
directions and both are true:

**The signal works.** The system distinguishes a momentum regime from noise at
t = +4.12 over 6,908 pooled trades. The 25-dimensional manifold, the online RLS
and the regime gating are extracting real information. That is not nothing — a
great many trading systems cannot clear this bar at all.

**The economics do not.** The information it extracts is worth about 13 bps per
trade. It pays 19 bps per trade to act on it. The gap is not close, and it is
not a tuning problem in the usual sense — no threshold adjustment turns 13 into
19.

The three ways that gap can close, in rough order of plausibility:

1. **Trade less often, on stronger signals.** The entry gates already pass only
   0.3–0.6% of bars; a higher bar would mean fewer, larger-edge trades and the
   same fixed cost amortised over more expected move. Whether the signal's
   strength is concentrated enough for this to work is an empirical question the
   experiment harness can now answer.
2. **Pay maker fees instead of taker.** 2.0 bps versus 5.5 bps changes the round
   trip from 19 to 12 bps. The router already supports PostOnly; what it does
   not have is a measured estimate of the fill-rate cost of insisting on it.
3. **Hold longer.** Cost is per round trip, so a signal held for a larger
   expected move earns more per unit of friction.

None of these were attempted here. Attempting them against synthetic series
would be fitting to a generator, and the brief's priority order puts
profitability below correctness and risk control for exactly this reason.

**On mean reversion:** the 12-seed table showed the system finding nothing in a
planted mean-reversion edge. Given that the same table's momentum claim did not
survive pooling, that row should be treated as unestablished rather than as a
finding. It was not re-run pooled.

### A test I removed, and why

An earlier version of this suite asserted that a regime-shift dataset
(momentum for the first half, mean reversion for the second) must show degraded
performance in the second half, and treated improvement as evidence of leakage.

It failed: second-half expectancy was +0.004594 against first-half +0.000083.

**The test was wrong, not the code.** It ran the two halves independently, with
no state transfer, so it was not comparing in-sample against out-of-sample at
all — it was comparing "how the system does on momentum data" against "how it
does on mean-reverting data" and calling the difference a leak. The failure was
mine. It has been replaced by sections 4 and 5 above, which test state transfer
directly, and the observation itself is recorded here rather than discarded.

(That observation is a single sample and should not be read as a finding
either — it is recorded because it is what the failing test actually produced.)

---

## 7. Trade frequency

Across every synthetic run the engine produced roughly **50–120 trades per
20,000 bars** — about 0.3–0.6% of bars generate a trade. The entry gates are
extremely selective.

Two consequences for anyone reading a result from this system:

* A short backtest will not produce a statistically meaningful sample. At this
  rate, 100 trades needs on the order of 20,000–30,000 one-minute bars, or two
  to three weeks of continuous data per symbol. The `sample_size` gate rejects
  anything below 100 trades for exactly this reason.
* Several individual seeds produced 10–17 trades and, at that sample size,
  showed strongly positive expectancy on **pure noise** — one random-walk seed
  reported +0.0062 per trade over 10 trades, and another +0.0104. These are the
  numbers that would be quoted if someone ran one short backtest and stopped.
  They are pure sampling noise, and the pooled figure is what shows it.

---

## Summary

| claim | verdict | evidence |
|---|---|---|
| The backtester has no look-ahead | **PASS** | causality by truncation |
| It is deterministic | **PASS** | repeated runs identical |
| Costs are actually applied | **PASS** | 10x fees reduce expectancy |
| Fitted state crosses the split | **PASS** | warm vs cold differ |
| Frozen weights stay frozen | **PASS** | four weight vectors unchanged |
| No profit manufactured in noise | **PASS** | −0.001578/trade, t = −6.11, n = 1,725 |
| The signal discriminates momentum from noise | **PASS** | +0.001305/trade, t = +4.12 |
| **It is profitable on a known edge** | **FAIL** | −0.000273/trade after costs, n = 5,183 |
| It is profitable on real markets | **UNKNOWN** | never tested — no data available |

The instrument is trustworthy. The strategy extracts real information. On
synthetic data constructed to favour it, that information is not worth more than
the cost of acting on it.

---

## Limits — what could not be tested here

This environment has no outbound access to `api.bybit.com`; the proxy returns
`403 CONNECT (policy denial)`, verified against the proxy's own status
endpoint. Package registries are reachable, exchanges are not.

Everything above therefore rests on **synthetic series with known ground
truth**. That is the right tool for validating a measurement apparatus — you
cannot check whether an instrument finds profit in noise without noise whose
properties you control — but it is emphatically not a substitute for real data.

Not established here, and not claimed anywhere in this work:

* any statement about performance on real market data
* any statement about live or paper profitability
* fill realism — synthetic series have no order book, so queue position, depth,
  partial fills from thin liquidity and market impact are all unmodelled
* funding-rate realism — a flat 1.0 bps per 8h is assumed, not observed
* regime coverage — no crash, no liquidity hole, no exchange outage, no
  delisting, no funding spike appears in these generators

The tooling to run all of this on real data exists and is committed
(`scripts/fetch_klines.py`, `scripts/run_experiment.py`, `src/research/`). It
needs a machine with exchange egress and roughly ten minutes. Until that has
been run, the honest summary of this system's profitability is: **unknown**.
