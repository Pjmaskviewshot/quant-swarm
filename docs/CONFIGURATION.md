# Configuration

Every variable the code reads is in `.env.example`, with its effect and default.
`tests/test_env_documentation.py` fails the build if that file and the code
drift apart in either direction.

This page covers the ones where getting it wrong costs money.

## The mode switch

```
TRADING_MODE = PAPER | TESTNET | LIVE
```

Resolved once, in `src/runtime_config.py`. Other modules consult
`mode.places_real_orders` and never re-derive it.

* Absent configuration is **PAPER**.
* Credentials being present does **not** imply LIVE.
* `TEST_MODE` is honoured for backward compatibility; `TRADING_MODE` wins where
  both are set.

## The four that move money

**`MAX_DRAWDOWN_PCT`** (0.15) — fraction of peak **equity** lost before the
portfolio commander force-flattens every position. This is the hardest kill
switch in the system. It acts on the equity reading, so if the wallet poll
stalls, this stops working silently. Set it to a loss you are willing to take
today, not to a number that looks reasonable in the abstract.

**`MAX_SINGLE_POSITION_RISK_PCT`** (0.025) — fraction of equity risked per
position. Note there is **no cap on concurrent positions**, so twelve correlated
alts at 2.5% each is 30% directional exposure that nothing checks.

**`SKIP_BELOW_MIN_NOTIONAL`** (`true`) — what to do with a trade below the
exchange minimum notional.

* `true` — refuse it.
* `false` — round **up** to the minimum, **spending more than intended**. This
  is the B1 failure mode, reproduced turning a $500 order into $6,500.

Keep it `true` unless you specifically want the second behaviour and have
sized for it.

**`NOTIONAL_DEVIATION_TOLERANCE`** (0.25) — rejects an order whose realised
notional exceeds the intention by more than this fraction. The backstop for B1.
Raising it widens the hole.

## Data freshness

**`MAX_ORDERBOOK_AGE_SEC`** (5.0) — a book older than this is not tradeable,
on both the entry and the exit path. Raising it lets the system act on stale
quotes; lowering it makes it more conservative and may block trading on a laggy
feed. Prefer fixing the feed.

## Persistence

**`PERSISTENT_STORAGE_PATH`** (`.`) — where the SQLite ledger and model state
live. On an ephemeral container this **must** point at a mounted volume, or
model state and trade history are lost on every restart, and every restart is a
cold start.

**`SUPABASE_URL` / `SUPABASE_KEY`** — optional cloud ledger. Blank runs
SQLite-only. Note that the PostgREST write path was not exercised in this
audit's environment.

## Health server

**`HEALTH_TOKEN`** — shared secret for `/metrics` and the metrics block of
`/health`, which expose the account's equity and wallet balance. **Unset means
those routes are closed.** Fail-closed is deliberate: forgetting to configure
this exposes nothing. Plain `/health` liveness stays open.

**`ENABLE_KEEP_ALIVE` / `PORT`** — the server binds `0.0.0.0` because hosts
require it, so on a deployed instance its routes face the public internet.

## Research (never touches an account)

**`KLINE_CACHE_DIR`**, **`EXPERIMENT_DIR`** — where cached candles and
experiment records live.

**`FREEZE_RLS_WEIGHTS`** — run the fitted model without adapting. Use for a
genuine out-of-sample evaluation, where continued learning would mean the test
period is fitting on itself.

## Secrets

Never commit a filled `.env` (it is in `.gitignore`, and a test asserts that).
Never put a credential in `.env.example` (a test asserts that too). Scope the
Bybit API key to **trade only** — no withdrawal permission — and set an IP
allowlist.
