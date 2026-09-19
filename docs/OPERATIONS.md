# Operations Runbook

## Starting

```bash
cp .env.example .env        # then fill it in
python src/main.py
```

Mode comes from `TRADING_MODE`. Absent configuration is PAPER. Confirm what you
are actually running before walking away:

```bash
curl -s localhost:8080/health | python3 -m json.tool
```

`mode`, `places_real_orders` and `build_revision` are in the response.
`build_revision` is resolved from git at runtime, so it tells you which commit
is live rather than which version string someone last edited.

## Stopping

`SIGINT` triggers graceful shutdown: the ledger flushes, the WebSocket closes,
open positions are **left open** (exchange-native stops remain in force).

To stop and flatten, lower `MAX_DRAWDOWN_PCT` below current drawdown and let the
portfolio commander exit, or close manually on the exchange. There is no
"flatten and exit" command; that is a gap, and it is listed in
`docs/KNOWN_LIMITATIONS.md`.

**If something is genuinely wrong, revoke the API key on Bybit.** It is the only
stop that does not depend on this process behaving correctly.

## What to watch

```bash
curl -s -H "X-Health-Token: $HEALTH_TOKEN" localhost:8080/metrics
```

| signal | meaning | action |
|---|---|---|
| `reason_codes.STALE_DATA` climbing | feed is lagging or frozen | check WS connectivity; entries are already blocked |
| `counters.order_rejected` climbing | sizing or margin problem | check `retCode` in logs; 110007 is insufficient balance |
| `CLOUD LEDGER DEGRADED` in logs | Supabase writes failing | SQLite still authoritative; trading continues |
| `EXIT_SENTRY STALE MARKET DATA` | exit loop suspended for a symbol | exchange stops still in force; investigate the feed |
| `db_health.db_write_failure` rising | ledger degraded | performance figures will be incomplete |
| equity gauge flat while trades occur | wallet poll failing | drawdown breaker may be blind — **investigate immediately** |

That last row is the one that matters most. The drawdown breaker acts on the
equity reading; if the reading stops updating, the breaker stops working, and
nothing else in the system will tell you.

## Restart recovery

On restart the system:

1. resolves config and mode, and **halts** on a failed vault boot check;
2. cancels any resting orders it finds;
3. reconciles local position state against exchange truth;
4. reloads RLS weights, covariances and the whitener from persisted state.

Persisted model state is treated as **untrusted input**: a corrupt or truncated
cache is ignored with a warning rather than raising. Failure injection found the
opposite behaviour — one corrupt cache entry prevented startup entirely, because
`load_state` was called during symbol initialisation without a guard.

`PERSISTENT_STORAGE_PATH` must point at a mounted volume on an ephemeral host,
or model state and trade history are lost on every restart.

## Ledger health

```bash
python scripts/reconcile_ledger.py --db titan_memory_ledger.db
```

Exit code 2 means a HIGH or CRITICAL inconsistency. The two that matter:

* **C9** — win count by outcome label disagrees with win count by PnL sign. The
  labels the model learns from do not match the money. Stop and investigate;
  every performance figure and every Kelly update downstream is affected.
* **C6b** — a resolved trade with NULL PnL. SQLite stores NaN as NULL, and read
  sites coerce NULL to 0.0, so this appears as a breakeven trade. Find why it
  was NULL before trusting any aggregate.

## Common failures

**`110007 insufficient balance`** — the account cannot cover initial margin.
Check leverage and `MAX_SINGLE_POSITION_RISK_PCT`. In PAPER this is the paper
broker's own margin check, not the exchange.

**`10006 rate limit`** — not treated as success; the order is not assumed filled.

**WebSocket disconnect** — REST BBO fallback engages, but it has no depth, so
`is_tradeable` refuses entries until the WS feed returns. That is intended: a
BBO-only quote cannot support a slippage estimate.

**`PaperIsolationError`** — paper mode tried to reach the live executor. This is
the isolation guard working. Report the attribute name; it means a code path
assumes a live broker.

## Before going live

Work through `docs/DEPLOYMENT.md`. Do not skip the testnet stage — several
defects in this audit were only observable against a real exchange response.
