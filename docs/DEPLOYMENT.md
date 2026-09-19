# Deployment

Three stages. Each has an exit criterion that must be met before the next.
The whole point of the ordering is that the cheap stage catches what the
expensive stage would have cost you.

---

## Stage 1 — PAPER

```bash
TRADING_MODE=PAPER PAPER_STARTING_BALANCE=1000 python src/main.py
```

**What paper proves:** the signal pipeline runs, positions open and close, the
exit ladder behaves, the ledger records, nothing crashes over a sustained period.

**What paper does NOT prove.** The `PaperBroker` docstring lists this explicitly
and it is repeated here because it is the thing most often assumed away:

| modelled | NOT modelled |
|---|---|
| fees (taker/maker) | funding payments |
| fixed slippage | queue position |
| initial-margin solvency | L2 order matching |
| reduce-only semantics | market impact |
| min-notional / step size | liquidation |
| rejection codes | latency |
| | depth-driven partial fills |

**Paper PnL is not a profitability estimate.** It is a smoke test for plumbing.

**Exit criterion:** 72 hours continuous, no crash, no `PaperIsolationError`, no
`C9`/`C6b` from `reconcile_ledger.py`, and the equity gauge updating throughout.

---

## Stage 2 — TESTNET

```bash
TRADING_MODE=TESTNET BYBIT_API_KEY=... BYBIT_API_SECRET=... python src/main.py
```

Use **testnet** credentials. A mainnet key here places real orders.

**What testnet proves and paper cannot:** real API semantics — actual `retCode`
values, actual `orderStatus` transitions, real rate limits, real WebSocket
disconnects, real `positionIdx` behaviour, real min-notional and tick-size
rejections. Several defects in this audit were only observable against a genuine
exchange response.

**Exit criterion:** at least 50 completed round trips. Zero unexplained
positions. Zero cases of local state disagreeing with exchange truth after
reconciliation. Every rejection code seen is one the code handles by name.

---

## Stage 3 — LIVE

**Live activation is an explicit human action. Nothing in this repository
enables it automatically, and nothing should.**

Before setting `TRADING_MODE=LIVE`:

- [ ] Stages 1 and 2 exit criteria met.
- [ ] `pytest` — all tests, including `-m slow`.
- [ ] `python scripts/reconcile_ledger.py` — exit code 0.
- [ ] Every value in `.env` reviewed against `docs/RISK_CONTROLS.md`.
- [ ] `MAX_DRAWDOWN_PCT` set to a loss you are willing to take, today.
- [ ] `PERSISTENT_STORAGE_PATH` on a mounted volume, not container-local.
- [ ] `HEALTH_TOKEN` set, or accept that `/metrics` stays closed.
- [ ] API key scoped to **trade only** — no withdrawal permission.
- [ ] IP allowlist configured on the key.
- [ ] You can revoke the key in under a minute, and know where.
- [ ] **Starting capital is an amount you can lose entirely.**

Then:

1. Start with the smallest capital the exchange permits.
2. Watch the first ten trades individually. Compare each against the ledger.
3. Run `reconcile_ledger.py` after the first day.
4. Scale only after a week of clean reconciliation.

**Profitability is unknown.** It has not been demonstrated on real data in this
work — see `reports/instrument_validation.md` for exactly what was and was not
established. Deploying live is a decision to find out, at your own expense.

---

## Environment notes

Host must set `PORT`, or set `ENABLE_KEEP_ALIVE=true` to run the health server.

The health server binds `0.0.0.0`; on a hosted platform its routes are public.
`/metrics` and the metrics block of `/health` require `HEALTH_TOKEN` and are
**closed when it is unset**. Plain `/health` liveness stays open for uptime
monitors.

Ephemeral filesystems lose the SQLite ledger and all model state on restart.
Mount a volume and point `PERSISTENT_STORAGE_PATH` at it, or configure Supabase
as the durable ledger.

## Rollback

Revert the deployment to the previous commit and restart. Model state persists
across the rollback; if the revert crosses a state-format change, delete the
persisted state and let the system re-learn from cold rather than loading a
format it does not understand.
