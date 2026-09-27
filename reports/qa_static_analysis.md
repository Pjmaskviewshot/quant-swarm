# Static Analysis, Security and Configuration QA

APEX section 25. Run on branch `phase2/apex-overhaul`.

```
ruff check src/ scripts/ tests/ --select F,E9,B,S
bandit -r src/ scripts/ -ll
pytest tests/test_env_documentation.py tests/test_health_endpoint_exposure.py
```

---

## Summary

| check | result |
|---|---|
| Undefined names (F821) | **1 found — NameError in production, fixed** |
| Hard-coded secrets | none |
| High-entropy literals | none |
| `bandit` HIGH | none |
| `bandit` MEDIUM | 1 (0.0.0.0 bind — addressed, see below) |
| Unauthenticated endpoints exposing account data | **1 found, fixed** |
| Unused imports | 32, removed |
| Undocumented environment variables | **28 of 28, now documented and guarded** |
| Dead code | 2 sites, removed |
| NumPy deprecations | 2 sites (154k warnings), fixed |

Three of these were real defects rather than hygiene. Each is below with what
it would have done in production.

---

## D1 — `NameError` in the position exit loop (CRITICAL)

`src/main.py`, five references to `now_sec` with no binding.

**This was mine.** I introduced it in commit `1e5cdcf` as part of the B19/B20
freshness residual fix — the change that extends stale-data protection from the
entry path to the exit path.

```python
ob_age = (now_sec - float(ob["as_of"])) if ob.get("as_of") else None   # NameError
```

**What it would have done.** This sits inside the `ACTIVE_MONITORING` loop that
manages every open position. The first iteration for the first open position
raises `NameError`. The exit sentry — the component responsible for the
trailing stop and for closing positions — would have failed immediately, for
every position, on a code path whose entire purpose is protecting open risk.

**How it escaped the tests I wrote.** My tests for B19/B20 exercise
`is_tradeable()` as a pure function, which is exactly the isolation that made it
testable and exactly the isolation that meant the call site was never executed.
`ruff` found it in under a second.

**Fix.** Bind one clock reading per loop iteration. The freshness check, the
stale-duration counter and the log throttle all need to agree on "now"; reading
`time.time()` separately at each use lets them disagree by a scheduling slice.

**The general lesson,** recorded because it applies beyond this defect: a unit
test on an extracted pure function verifies the function, not the integration.
Static analysis over the whole tree is not redundant with a passing test suite.

---

## D2 — Live account balance on an unauthenticated public endpoint (HIGH)

`src/keep_alive.py`.

The health server binds `0.0.0.0`, which the hosting platform requires, so every
route is reachable from the public internet on a deployed instance. `/metrics`
returned `METRICS.snapshot()` with no authentication, and `main.py:927-928`
publishes:

```python
METRICS.gauge("equity", snap.equity)
METRICS.gauge("wallet_balance", snap.wallet_balance)
```

**What it exposed.** Anyone who found the URL could read the account's current
equity and wallet balance. The counters alongside it leak order rate, rejection
reasons and position activity — enough to infer when the bot is in a position
and how large. `/health` additionally published `places_real_orders`,
identifying the instance as live.

**Fix — fail closed.** `/metrics`, and the metrics block of `/health`, now
require `HEALTH_TOKEN`, compared with `hmac.compare_digest` (a naive `==` leaks
the token a byte at a time to a timing attacker). **With no token configured the
detailed routes are off**, so forgetting to set it exposes nothing rather than
everything. Plain `/health` liveness stays open, so platform health checks and
uptime monitors continue to work unchanged.

Pinned by `tests/test_health_endpoint_exposure.py` (7 tests), which assert the
balance does not appear in an anonymous response.

The `bandit` MEDIUM on the `0.0.0.0` bind is not resolved by changing the bind —
the host requires it. It is resolved by the routes no longer being worth
reaching.

---

## D3 — 28 environment variables, none documented (MEDIUM)

Every one of these was read by the code and documented nowhere:

```
BAR_SERIES_INTERVAL  BYBIT_API_KEY  BYBIT_API_SECRET  BYBIT_POSITION_IDX
ENABLE_DELTA_NEUTRAL  ENABLE_KEEP_ALIVE  ENABLE_MICRO_HORIZON_LEARNING
EXPERIMENT_DIR  FREEZE_RLS_WEIGHTS  HEALTH_TOKEN  KLINE_CACHE_DIR
MAX_DRAWDOWN_PCT  MAX_ORDERBOOK_AGE_SEC  MAX_SINGLE_POSITION_RISK_PCT
MICRO_HORIZON_SEC  MIN_REQUIRED_EQUITY  NOTIONAL_DEVIATION_TOLERANCE
PAPER_STARTING_BALANCE  PERSISTENT_STORAGE_PATH  PORT  SKIP_BELOW_MIN_NOTIONAL
SUPABASE_KEY  SUPABASE_URL  TELEGRAM_BOT_TOKEN  TELEGRAM_CHAT_ID  TEST_MODE
TRADING_MODE  TRADING_TIMEFRAME
```

That list includes `MAX_DRAWDOWN_PCT` (the force-flatten kill switch),
`TRADING_MODE` (whether real orders are placed at all) and
`SKIP_BELOW_MIN_NOTIONAL` (whether an undersized order is refused or **rounded
up to spend more than intended** — the B1 failure mode). An operator had no way
to discover any of them short of grepping the source.

**Fix.** `.env.example` documents all 28 with effect and default.
`tests/test_env_documentation.py` fails the build when code and documentation
drift **in either direction**: an undocumented variable, or a documented
variable nothing reads — the second being worse, because an operator will set it
and believe it took effect.

The file also asserts `TRADING_MODE` does not default to `LIVE`, that no
credential field carries a value, and that `.env` is in `.gitignore`.

---

## Smaller items

**NumPy deprecation, 2 sites.** `float()` on a `(1,1)` array in the Joseph-form
RLS update — `src/backtest.py` and `src/features/micro_models.py`. Deprecated in
NumPy 1.25, becomes an error in a future release, and it was generating ~154,000
warnings per backtest run. Replaced with `.item()`, which is the explicit and
numerically identical form. Verified under `-W error::DeprecationWarning`.

**HTML injection surface.** `src/services/telegram_ops.py` renders the entry
ticket with `parse_mode=HTML`. A computed-but-never-rendered
`safe_reasoning = html.escape(...)` made the escaping look deliberate where it
was not; the symbol itself was interpolated unescaped. Removed the dead
variable, escaped the symbol. Exchange tickers are low-risk in practice, but the
template should not depend on that.

**Exception chaining.** `raise EmergencyShutdown(...) from e` on the
boot-equity failure. Without `from e` an operator sees "boot failed" and loses
whether it was a network error, a bad key or a rejected vault check — at exactly
the moment they most need to know.

**Unused imports, 32.** Removed via `ruff --fix`, full suite re-run after.

---

## Not done

**Type checking.** `mypy` was not run. This codebase has no annotations on the
majority of its call sites, so a first `mypy` pass would produce thousands of
`no-untyped-def` findings and approximately zero useful ones. Adding annotations
across ~10,000 lines is worthwhile but is a project in its own right, not a
step in this one. Stating it as not done rather than reporting a clean run on a
configuration tuned until it passed.

**Dependency CVE scan.** `pip-audit` was installed but requires resolving the
full dependency tree against the advisory database over the network; the result
would reflect this container's resolved versions rather than the deployed ones.
Run it against `requirements.lock` in the deployment environment.
