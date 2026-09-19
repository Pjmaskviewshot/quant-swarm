# Testing

```bash
pytest -m "not slow"     # 457 tests, ~19 s — the working loop
pytest                   # + 6 instrument tests, ~2 min — what CI runs
```

## What the suite is organised around

Tests are named after the defect or the invariant, not after the function. A
test called `test_wiped_account_reports_zero_not_unknown` says what breaks if it
fails; `test_estimate_live_equity_3` does not.

| file | what it defends |
|---|---|
| `test_b1_notional.py` | quantisation cannot inflate an order |
| `test_b2_exit_fill.py` | a position closes only on exchange-confirmed zero size |
| `test_b2a_position_idx.py` | hedge mode reads the correct side |
| `test_b3_universe_subscription.py` | the traded universe matches the subscribed feed |
| `test_b5_b6_candles.py` | only closed candles drive signals |
| `test_b8_breaker_separation.py` | entry health and management health are separate |
| `test_b9_b10_reservations.py` | capital reservations cannot leak |
| `test_b11_equity.py` | unrealised PnL is not counted twice |
| `test_b17_b27_b28_b29_backtest.py` | walk-forward state, nan metrics, annualisation, bootstrap |
| `test_b19_b20_freshness.py` | stale data blocks entries and exits |
| `test_paper_isolation.py` | paper cannot touch the live account |
| `test_p1_ledger_integrity.py` | ledger write-path integrity |
| `test_ledger_reconciliation.py` | ten consistency checks, each with a planted corruption |
| `test_env_documentation.py` | config and documentation cannot drift |
| `test_health_endpoint_exposure.py` | the balance is not publicly readable |
| `test_invariants_property.py` | properties over all inputs (hypothesis) |
| `test_failure_injection.py` | timeouts, partitions, malformed payloads, corrupt state |
| `test_backtest_integrity.py` | the instrument itself does not cheat (`-m slow`) |
| `test_research_infra.py` | provenance, cost models, validation gates |
| `test_calibration.py` | is p_up = 0.70 actually 70%? are exits late or early? |

## The three kinds, and why all three

**Example tests** check the cases someone thought of. Necessary, insufficient —
every defect in the original audit was a case nobody thought to write.

**Property tests** (`hypothesis`) assert invariants over all inputs. This is the
category that found real bugs rather than confirming existing behaviour. The
clearest: `EquitySnapshot.is_usable()` required `equity > 0.0`, so an account
wiped to exactly zero read as UNKNOWN, callers fell back to a stale cached
balance, and the drawdown breaker never saw a 100% loss. Pushed further,
negative equity had the same problem. No example test would have used exactly
0.0 as an input.

**Failure injection** asks what happens when things break: API timeouts,
network partitions, malformed responses, duplicate fills, rate limits, exchange
disagreement, database failure, corrupt persisted state. This category found
that a corrupt model-state cache prevented startup, because `load_state` was
called during symbol initialisation without a guard.

## Writing a new test

State the failure in the assertion message. `assert size is None, "a failed
position query must be UNKNOWN, never 0.0"` explains itself at 3am when it goes
red; `assert size is None` does not.

For anything touching money, prefer a property over an example.

## Markers

`slow` — runs the real backtester over thousands of bars, minutes not seconds.
Excluded from the fast loop, **always run in CI**: these are the tests that prove
the instrument does not manufacture profit, and skipping them permanently would
void every research number.
