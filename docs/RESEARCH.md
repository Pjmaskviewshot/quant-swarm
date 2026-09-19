# Research Workflow

## The problem this replaced

`backtest.py` re-downloaded klines from `api.bybit.com` on every run. Three
consequences, all corrosive:

1. The window moved with wall-clock time, so the same command gave a different
   answer tomorrow. Nothing was reproducible.
2. Every parameter sweep paid the network cost and the rate limit.
3. It could not run at all without exchange egress.

Fetch and compute are now separate. Fetch once, hash the bytes, run any number
of experiments against exactly those bytes.

## Getting data

```bash
python scripts/fetch_klines.py --symbol BTCUSDT ETHUSDT --days 30
```

Public market-data endpoint; **no API key is used or required**. It writes
`data/klines/<SYMBOL>_<INTERVAL>.json` plus an integrity report counting gaps,
duplicate timestamps and invalid OHLC bars. A backtest over data with a
six-hour hole in it will happily produce a Sharpe ratio, and that Sharpe will be
wrong in a way nobody can see — so the gaps are counted, not silently accepted.

## Running an experiment

```bash
python scripts/run_experiment.py --dataset BTCUSDT:1 --name baseline --split
```

Writes a JSON record to `reports/experiments/` containing the commit SHA, a
dirty-tree flag, the dataset content hash, the bar range, the parameters, the
model version, the full cost model, the results and the validation verdict.

Exit code 0 means every gate passed. Exit code 2 means it did not.

## The synthetic generators

```
synthetic:random_walk      driftless GBM      expected_edge: NONE
synthetic:momentum         AR(1) phi=+0.35    expected_edge: POSITIVE_MOMENTUM
synthetic:mean_reverting   AR(1) phi=-0.35    expected_edge: MEAN_REVERSION
synthetic:regime_shift     momentum then MR   expected_edge: REGIME_DEPENDENT
```

**These validate the instrument, never the strategy.** A generator with a known
edge must be detected; one with no edge must not be. That is a test of the
measuring device. A synthetic result is never evidence about real markets and
must not be quoted as one. See `reports/instrument_validation.md`.

## The validation gates

Defined in `src/research/validate.py`, before results are seen, applied to every
candidate identically.

| gate | requirement | defends against |
|---|---|---|
| `sample_size` | ≥ 100 trades | an expectancy whose standard error exceeds the estimate |
| `costs_applied` | fees, slippage, funding all on | quoting a gross figure as profitability |
| `null_rejection` | no after-cost edge on a random walk | look-ahead, optimistic fills, kind costs |
| `cost_survival` | gross edge ≥ 2x round-trip cost | an edge that dies on one bad fill |
| `oos_decay` | OOS positive, ≤ 1.5x in-sample | fitted noise; a leaking split |
| `multiple_testing` | deflated Sharpe > 0 | reporting the best of a sweep |
| `stability` | ≥ 60% of folds positive | a coincidence with good PR |

**A gate with no evidence is recorded as FAILED with "not supplied"**, never
silently skipped. "We did not check" and "it passed" must not render identically
in a report.

The multiple-testing haircut is expressed in the same units as the reported
Sharpe. This matters: `summarize()` reports an annualised Sharpe, so a haircut
computed per-trade and subtracted from it would be off by
`sqrt(periods_per_year)` — roughly 50x too small — and the gate would be
decorative. `summarize()` publishes `periods_per_year` so the conversion is
exact.

## Comparing two results

```python
from research.experiment import load_all, compare
compare(baseline, candidate)
```

Refuses the comparison and reports blockers when the two used different
datasets (cherry-picked window), different cost models (measured more kindly),
or when the candidate has too few trades or far fewer than the baseline.

## Rules

These are not style preferences. Breaking any of them makes the result a lie.

* **Never change a metric's definition to improve a result.** Change the
  strategy, or accept the number.
* **Never move the test period after seeing the result.**
* **Never disable costs** except to show the gross-vs-net gap explicitly. A
  zero-cost record is flagged `costs_disabled` and the validation gate refuses
  to treat it as evidence of profitability.
* **Never quote a synthetic result as a claim about real markets.**
* **Record failed experiments.** They are saved with their error rather than
  discarded — survivorship bias in the experiment log is as dishonest as
  survivorship bias in the backtest.
* **Never quote a result from a dirty tree.** It is flagged `reproducible:
  false`, because nobody else can re-run it.
