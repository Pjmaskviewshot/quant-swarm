# quant-swarm

An asyncio crypto trading system for Bybit linear perpetuals: a 25-dimensional
microstructure feature manifold, online sparse RLS with Markov regime gating,
Merton-jump Kelly sizing, and a continuous adaptive exit ladder.

---

## Read this first

**Profitability has not been demonstrated, and on synthetic data it was
refuted.** The audit environment had no access to `api.bybit.com`, so every
measured result comes from synthetic series with known ground truth.

Those results establish two things. The signal is real: over 6,908 pooled
trades the system separates a momentum regime from pure noise at t = +4.12,
worth about **13 bps per trade**. And the economics do not work: a round trip
costs **19 bps**, so on a dataset deliberately constructed to favour this
strategy, after-cost expectancy is **negative**.

Real markets do not offer a cleaner momentum regime than a planted AR(1) with
φ = 0.35, no gaps and no adverse selection. That is not proof the system loses
money live — synthetic series are not markets — but the gap between signal
strength and transaction cost is the problem to solve before anything else.

See `reports/instrument_validation.md` for the method and the numbers, and
`docs/KNOWN_LIMITATIONS.md` for what this system cannot do.

**Live trading is an explicit human action.** `TRADING_MODE` defaults to PAPER.
Having API credentials configured does not enable live trading. Nothing in this
repository turns it on automatically.

---

## Quick start

```bash
pip install -r requirements.txt
cp .env.example .env          # then fill it in
TRADING_MODE=PAPER python src/main.py
```

```bash
pytest -m "not slow"          # 457 tests, ~19 s
pytest                        # + 6 instrument tests, ~2 min
```

---

## Layout

```
src/
  main.py               orchestration, daemons, signal gate, exit sentry
  runtime_config.py     mode resolution — the single source of truth
  market_data.py        is_tradeable() — freshness and completeness
  equity.py             wallet / unrealised / equity convention
  probability.py        calibrated directional estimate
  observability.py      counters, gauges, reason codes, latencies
  keep_alive.py         health server (token-gated metrics)
  backtest.py           historical simulation — research only
  features/             25D manifold, whitening, online RLS
  execution/            sizing, routing, paper broker
  core/                 exit ladder, execution FSM, dual ledger
  portfolio/            risk vault, portfolio commander
  research/             datasets, experiment records, validation gates
scripts/
  fetch_klines.py       fetch once, hash, reuse
  run_experiment.py     run + record + validate
  reconcile_ledger.py   ten ledger consistency checks (read-only)
tests/                  463 tests
docs/                   architecture, risk, ops, deployment, limitations
reports/                what was measured, and what could not be
```

## Documentation

| | |
|---|---|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | how the pieces fit, and which boundaries matter |
| [`docs/RISK_CONTROLS.md`](docs/RISK_CONTROLS.md) | every control, what it guards — **and what does not exist** |
| [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md) | the variables where getting it wrong costs money |
| [`docs/OPERATIONS.md`](docs/OPERATIONS.md) | running it, what to watch, common failures |
| [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) | PAPER → TESTNET → LIVE, with exit criteria |
| [`docs/TESTING.md`](docs/TESTING.md) | how the suite is organised and why |
| [`docs/RESEARCH.md`](docs/RESEARCH.md) | reproducible experiments and the validation gates |
| [`docs/KNOWN_LIMITATIONS.md`](docs/KNOWN_LIMITATIONS.md) | what this system cannot do |
| [`docs/AUDIT_HISTORY.md`](docs/AUDIT_HISTORY.md) | what was found and why each fix exists |

## Reports

| | |
|---|---|
| [`reports/instrument_validation.md`](reports/instrument_validation.md) | evidence the backtester does not cheat |
| [`reports/defect_register.md`](reports/defect_register.md) | every defect, its evidence class, its test |
| [`reports/qa_static_analysis.md`](reports/qa_static_analysis.md) | static analysis, security, config QA |
| [`reports/performance_profile.md`](reports/performance_profile.md) | where the time goes, and an optimisation that was rejected |
| [`FINAL_APEX_REPORT.md`](FINAL_APEX_REPORT.md) | readiness assessment |

---

## Design principles this codebase actually follows

**Unknown is not zero.** A failed position query returns `None`, never `0.0`. A
missing equity reading is `None`, never a stale cached number. A non-finite PnL
is recorded as `UNKNOWN` and excluded from statistics, never stored as a
breakeven. Throughout, "we do not know" and "the value is zero" are deliberately
different states, because conflating them is how a system silently trades on a
position it thinks is closed.

**Exchange truth beats local state.** A position closes when a position query
says size is zero — never on an order acknowledgement.

**Fail closed.** Stale data blocks trading. An unconfigured metrics token closes
the endpoint. An unresolved mode is PAPER. A missing validation gate reads as
FAILED, not as skipped.

**Measure the instrument before believing the measurement.** A backtest number
is evidence only after the backtester has been shown not to manufacture it.

## Security

Never commit a filled `.env`. Scope the Bybit API key to **trade only** — no
withdrawal permission — and set an IP allowlist. A secrets scan over this
repository is clean; a test asserts `.env.example` carries no values and that
`.env` is ignored.

## Licence

Private.
