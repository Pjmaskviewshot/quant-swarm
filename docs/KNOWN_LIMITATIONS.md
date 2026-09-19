# Known Limitations

Everything here is known, deliberate and unfixed. It is written down so that
nobody has to rediscover it during a drawdown.

## The signal does not clear its own transaction costs

The most important limitation, and the one everything else is downstream of.

Over 30 seeds × 20,000 bars per generator, pooled across 6,908 individual
trades, the system separates a planted AR(1) momentum regime from pure noise at
**t = +4.12**. The signal is real and statistically strong. It is worth about
**13.1 bps per trade**.

A round trip costs **19.0 bps** (5.5 taker in, 5.5 taker out, 4.0 slippage each
side). So on a synthetic series built to suit this strategy — no gaps, no
outages, no adverse selection, persistent φ = 0.35 autocorrelation — after-cost
expectancy is **negative** (−0.000273/trade over 5,183 trades).

Real markets do not offer a cleaner momentum regime than that. Three plausible
routes to closing the gap are set out in `reports/instrument_validation.md`;
none were attempted, because tuning against a generator is fitting to the
generator.

## Profitability on real markets has not been measured

No real market data was available (`api.bybit.com` returns `403 CONNECT`, a
proxy policy denial). The synthetic result above is suggestive, not conclusive:
the trade population a real feed generates could differ.

**The honest summary of this system's real-market edge is: unknown, with a
negative prior.** The tooling exists and needs about ten minutes on a machine
with exchange egress. The thing to look for is whether per-trade edge exceeds 19
bps — not whether the backtest total is positive.

## It trades rarely

0.3–0.6% of bars produce a trade — roughly 50–120 trades per 20,000 one-minute
bars. Reaching 100 trades takes two to three weeks of continuous data per
symbol. **A short backtest cannot produce a statistically meaningful result on
this system**, and several 10-trade samples showed strongly positive expectancy
on pure noise.

## Not modelled in PAPER

Funding payments, queue position, L2 matching, market impact, liquidation,
latency, depth-driven partial fills. Paper PnL is a plumbing smoke test, not a
profitability estimate.

## Not modelled in the backtester

* Fills are OHLCV approximations. **This is not an execution replay** and must
  never be described as one.
* Funding is a flat 1.0 bps per 8h, not the observed rate.
* Slippage is a fixed 4.0 bps baseline, not depth-derived.
* No exchange outages, delistings, funding spikes or liquidity holes appear in
  any test series.

## Risk controls that do not exist

* No hard per-day loss limit independent of drawdown.
* No maximum concurrent position count independent of per-trade risk.
* **No cross-symbol correlation limit.** Twelve correlated alts can be held
  simultaneously, each individually within its 2.5% risk cap, producing
  concentrated directional exposure that no control sees.
* No automated halt on consecutive losses (only a short cooldown).
* No "flatten and shut down" command. Stopping the process leaves positions
  open under exchange-native stops.

## Single process, single event loop

Every daemon is a coroutine on one loop. A blocking call anywhere stalls
everything, including the 50 ms exit sentry that protects open positions. There
is no supervisor process and no watchdog that restarts a wedged loop.

## Dependency versions

The test suite ran against versions **newer** than `requirements.txt` pins
(aiohttp 3.14.3 vs 3.9.5, pandas 3.0.2 vs 2.2.2, Flask 3.1.3 vs 3.0.3). The
pinned set has not been verified. `supabase` was not installed, so the
PostgREST write path ran only through its failure branches.

See the header of `requirements.lock`.

## Not statically type-checked

`mypy` was not run. Most call sites are unannotated, so a first pass would
produce thousands of findings and few useful ones. Worth doing; not done here,
and not papered over with a configuration tuned until it passed.

## Unresolved question from the audit

Whether a zero-fill IOC order returns `Cancelled` or `Rejected` is marked `[?]`
in `PHASE0_B1_B2_VERIFICATION.md` — two Bybit documentation pages disagree and
no live test was possible. Safety does not depend on the answer, because closure
is gated on a position-size query rather than on order status. Resolve it on
testnet.

## The audit itself introduced defects

Three, all found and fixed, all recorded:

* `NameError` in the exit sentry (`now_sec` never bound) — mine, from the
  B19/B20 fix. Would have broken position management on the first iteration.
* `PaperBroker.__getattr__` delegating to the live executor — paper mode read
  the live balance and called `adjust_leverage` against the live account.
* `_fetch_position_size` reading `rows[0]` without matching `positionIdx` — in
  hedge mode it could read the opposite side.

This is not a confession of unusual carelessness. It is the reason the process
is staged, the reason static analysis runs over the whole tree, and the reason
nothing here should go live without testnet.
