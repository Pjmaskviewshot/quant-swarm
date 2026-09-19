# Architecture

## Shape of the system

A single asyncio process. There is no message broker and no separate worker
tier: every daemon below is a coroutine on one event loop, sharing state through
plain Python objects. That is the most important fact about this codebase,
because it means a blocking call anywhere stalls everything, including the exit
sentry that protects open positions.

```
                       ┌─────────────────────────────┐
   Bybit WS (public) ──▶│  ingestion/multi_feed.py    │
   Bybit WS (private)──▶│  orderbook + trade + kline  │
                       └──────────────┬──────────────┘
                                      │ snapshots
                       ┌──────────────▼──────────────┐
                       │ features/micro_models.py    │
                       │  25D Volterra manifold      │
                       │  ZCA whitening              │
                       │  sparse RLS (Joseph form)   │
                       │  Markov regime beliefs      │
                       │  Platt calibration          │
                       └──────────────┬──────────────┘
                                      │ ProbabilityEstimate
                       ┌──────────────▼──────────────┐
                       │ main.py  signal gate        │
                       │  freshness / EV / regime    │
                       └──────────────┬──────────────┘
                                      │
              ┌───────────────────────▼───────────────────────┐
              │ portfolio/risk_vault  +  portfolio_commander   │
              │  per-trade risk, drawdown breaker, exposure    │
              └───────────────────────┬───────────────────────┘
                                      │ approved size
                       ┌──────────────▼──────────────┐
                       │ execution/sor.py            │
                       │  quantisation, notional     │
                       │  sanity, slicing, pegging   │
                       └──────┬───────────────┬──────┘
                              │               │
              ┌───────────────▼───┐   ┌───────▼─────────────────┐
              │ BybitExecutor     │   │ PaperBroker             │
              │ (TESTNET / LIVE)  │   │ (PAPER — never delegates│
              └───────────────────┘   │  to the live executor)  │
                                      └─────────────────────────┘
                                      │
                       ┌──────────────▼──────────────┐
                       │ core/intelligent_exit.py    │
                       │  CAMB exit ladder           │
                       │  ExecutionGovernorFSM       │
                       └──────────────┬──────────────┘
                                      │
                       ┌──────────────▼──────────────┐
                       │ core/memory.py              │
                       │  SQLite (WAL) + Supabase     │
                       └─────────────────────────────┘
```

## The daemons

| daemon | period | what it does |
|---|---|---|
| WS ingestion | continuous | orderbook, trades, klines |
| signal evaluation | per closed bar | features → probability → gate |
| `ACTIVE_MONITORING` exit loop | 50 ms per open position | CAMB ladder, trailing stop |
| fast state reconciliation | 10 s | local position state vs exchange truth |
| equity / lifecycle | ~60 s | wallet poll, drawdown, breaker |
| ledger sync worker | batched | SQLite → Supabase |
| health server | on demand | `/health`, `/metrics` (token-gated) |

## Boundaries that matter

**`runtime_config.py` is the single source of truth for mode.** `TradingMode` is
`PAPER | TESTNET | LIVE`. `mode.places_real_orders` is the only thing any other
module should consult. Live is never inferred from the presence of credentials.

**`PaperBroker` does not delegate.** It raises `PaperIsolationError` on any
attribute not in an explicit read-only allow-list, rather than falling through
to the live executor via `__getattr__`. This was a real defect found during the
audit: paper mode was reading the live wallet balance and calling
`adjust_leverage` against the live account.

**Exchange truth beats local state.** `ExecutionGovernorFSM` never marks a
position `CLOSED` on an order acknowledgement. It closes only when a position
query confirms size zero. An unreachable exchange yields `None` (unknown), never
`0.0` (flat) — the distinction that B2 turned on.

**Unknown is a first-class value.** A failed fill report is `UNKNOWN`, not
"unfilled". A missing equity reading is `None`, not zero. A non-finite PnL is
recorded as `UNKNOWN` and excluded from statistics rather than stored as a
breakeven. Throughout this codebase, "we do not know" and "the value is zero"
are deliberately different states.

## Module map

| path | responsibility |
|---|---|
| `src/main.py` | orchestration, daemons, signal gate, exit sentry |
| `src/runtime_config.py` | mode resolution, build revision, config validation |
| `src/market_data.py` | `is_tradeable()` — pure freshness/completeness check |
| `src/equity.py` | wallet/equity/unrealised accounting convention |
| `src/probability.py` | `ProbabilityEstimate` — calibrated directional estimate |
| `src/observability.py` | counters, gauges, reason codes, latency percentiles |
| `src/features/micro_models.py` | the 25D manifold and online learning |
| `src/execution/sor.py` | sizing, quantisation, notional sanity, routing |
| `src/execution/paper_broker.py` | isolated simulated broker |
| `src/core/intelligent_exit.py` | exit ladder and execution FSM |
| `src/core/memory.py` | dual ledger |
| `src/core/fsm.py` | system health state machine |
| `src/portfolio/` | risk vault, portfolio commander |
| `src/backtest.py` | historical simulation (research only) |
| `src/research/` | datasets, experiment records, validation gates |
