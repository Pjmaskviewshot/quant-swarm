# PHASE 1 — REPOSITORY AUDIT (no code modified)

**Scope:** 13 files supplied (`main.py`, `micro_models.py`, `intelligent_exit.py`, `sor.py`,
`risk_vault.py`, `risk_manager.py`, `memory.py`, `multi_feed.py`, `bybit_v5.py`,
`adaptive_engine.py`, `omni_scanner.py`, `quantum_entry.py`, `sector_oracle.py`,
`delta_neutral.py`, `telegram_ops.py`, `keep_alive.py`, `backtest.py`, `schema.sql`).

**Method:** static reading only. Nothing was executed, no exchange was contacted, no backtest
was run. Every finding below is a code-path claim, tagged with a confidence level:

- **[C]** Confirmed by direct reading of both sides of the interface.
- **[L]** Likely; depends on runtime data shape or env config I cannot see (`.env`,
  `params.json`, deployed Supabase schema, `requirements.txt` — none were supplied).
- **[?]** Needs instrumentation to settle.

**No test suite exists in the supplied files.** There is no `tests/` directory, no
`requirements.txt`, no lint/type config, and no CI. That is itself a top-tier finding.

---

## 1. ARCHITECTURE MAP

```
                          ┌───────────────────────────────────────────┐
                          │  main.py :: DistributedQuantEngine        │
                          │  (orchestrator, 11 daemons, event loop)   │
                          └───────────────────────────────────────────┘
        ingestion                     alpha                       execution / risk
 ┌──────────────────────┐   ┌────────────────────────┐   ┌────────────────────────────┐
 │ multi_feed.py        │   │ micro_models.py        │   │ sor.py  SmartOrderRouter   │
 │  MarketStateMatrix   │   │  ContinuousMicro-      │   │  IOC / MakerPeg / TWAP /   │
 │  · public WS (linear)│   │  structureEngine       │   │  EmergencyMarket           │
 │  · L2 bisect ladders │──▶│  · 19 raw features     │──▶│                            │
 │  · Log-MLOFI z       │   │  · ZCA whitener        │   │ intelligent_exit.py        │
 │  · Stoikov microprice│   │  · 25D Volterra vec    │   │  IntelligentExitEngine     │
 │  · conflation workers│   │  · 4× sparse RLS       │   │  (CAMB barrier) +          │
 └──────────────────────┘   │  · Markov regime gate  │   │  ExecutionGovernorFSM      │
 ┌──────────────────────┐   │  · Merton-Kelly sizer  │   │                            │
 │ bybit_v5.py          │   │  · BOCD / Hurst / OU / │   │ risk_vault.py              │
 │  BybitUnifiedExecutor│   │    Hawkes / CVD / spoof│   │  InstitutionalRiskVault    │
 │  · signed REST       │   └────────────────────────┘   │  · DD tiers, slots, corr,  │
 │  · private WS fills  │   ┌────────────────────────┐   │    heat cap, single-risk   │
 │  · token bucket      │   │ quantum_entry.py       │   └────────────────────────────┘
 │  · universe screener │   │  exec-weight / macro   │   ┌────────────────────────────┐
 └──────────────────────┘   │ adaptive_engine.py     │   │ fsm.py  SystemStateMachine │
 ┌──────────────────────┐   │  MTF momentum / ATR    │   │  tiered breakers, locks    │
 │ omni_scanner.py      │   │ sector_oracle.py       │   └────────────────────────────┘
 │  PCA rotation of     │   │  PC1 sector impulse    │   ┌────────────────────────────┐
 │  the asset basket    │   └────────────────────────┘   │ memory.py  MemoryBank      │
 └──────────────────────┘                                │  SQLite WAL + Supabase     │
 ┌──────────────────────┐   ┌────────────────────────┐   │  shadow ledger, k-NN "DNA" │
 │ delta_neutral.py     │   │ telegram_ops.py        │   └────────────────────────────┘
 │  basis harvester     │   │ keep_alive.py (health) │   ┌────────────────────────────┐
 │  ** NEVER STARTED ** │   └────────────────────────┘   │ backtest.py (standalone)   │
 └──────────────────────┘                                └────────────────────────────┘
```

**Daemon set actually launched** (`run_engine_forever`, 11 tasks):
`state_actor.start`, `run_telegram_worker`, `run_cloud_lease_heartbeat`, `run_dna_prewarmer`,
`stream_manager_loop`, `run_system_heartbeat`, `run_shadow_resolution_daemon`,
`_universe_refresher_loop`, `run_omni_swarm_director`,
`run_fast_state_invariant_reconciliation`, `run_correlation_engine`.

`DeltaNeutralYieldEngine.run_yield_scanner_daemon` is **not** in that list. **[C]** ~850 lines
of basis-harvesting code, its Supabase table, and its unwind logic are dead in production.

---

## 2. STRATEGY FLOW (as built)

```
WS orderbook delta
  → MarketStateMatrix._update_ssot_orderbook   (bisect ladders, microprice, MLOFI z)
  → conflation worker (single-slot mailbox, one per symbol)
  → main.handle_incoming_orderbook_tick        (throttle 0.20 s/symbol)
  → main._eval_gate  ──────────────────────────────────────────────────────────┐
        1. circuit breaker / FSM lock / asset lock                             │
        2. ≥40 ticks of history                                                │
        3. sector impulse (SVD PC1 over tick returns)                          │
        4. ATR → sl_dist_pct = max(atr*2.5/price, 0.015); tp = sl * dynamic_rr │
        5. stat_engine.extract_statistical_state(...)  → p_up, regime, kelly   │
        6. gate:      prob_success ≥ dynamic_gate (conformal floor 0.52–0.65)  │
        7. veto:      15 m momentum tape                                       │
        8. veto:      anti-whipsaw (180 s after a loss, opposite direction)     │
        9. veto:      expected-drift  ← DEAD, can never fire (§4 B14)           │
       10. veto:      spread/ATR friction ← DEAD, spread always 1e-4 (§4 B3)    │
       11. veto:      macro tailwind (shorts only)                              │
       12. gate:      DNA "is_armed" (k-NN shadow ledger) — fail-open           │
       13. size:      Kelly → risk% → notional → corr haircut → 25% ticket cap  │
       14. gate:      portfolio heat headroom                                   │
       15. gate:      risk_vault.evaluate_portfolio_safety                      │
       16. reserve in-flight, set leverage, SOR.execute_alpha_signal            │
       17. spawn _position_lifecycle_daemon (50 ms loop, CAMB exits)            │
```

Signal formation inside `extract_statistical_state`:

```
19 raw features → AsynchronousStateAligner (Laplace EWMA, κ=0.4/s)
                → BoundedAdaptiveWhitener (streaming ZCA, eigendecomp every 20 ticks)
                → 25D vector = 19 whitened + 5 bilinear products + affine bias
                → 4 regime-specific sparse RLS logits (trend/range/spoof/cascade)
                → softmax-gated mixture by Markov beliefs (τ=0.80)
                → Platt temperature scaling: logit = clip(score*0.45, ±1.50)
                → p_up ∈ [0.182, 0.818]
                → adverse-flow veto (MLOFI opposing by >1.75σ without iceberg) → HOLD
```

Online learning: a 60-second replay buffer labels each stored feature vector with
"did price rise / hit virtual TP / hit virtual SL over 60 s", then does one Joseph-form RLS
step per regime, weighted by that regime's Markov belief. A second learning path
(`resolve_trade_outcome`) fires on real trade settlement.

**Signal invalidation** exists only as the exit-side flow sensors; there is no persisted
entry thesis object (`PositionExitState.entry_thesis` is declared and never populated).

---

## 3. RISK / EXECUTION / DATA / STATE FLOWS

### Risk flow
| Layer | Where | Value |
|---|---|---|
| Per-trade risk | `main._eval_gate` | Kelly-scaled, clipped `[0.002, 0.025]` of equity |
| Stop distance | `main._eval_gate` | `max(ATR*2.5/price, 0.015)` + `slippage_gap_buffer ≥ 0.0020` |
| Ticket cap | `main._eval_gate` | 25 % of balance notional |
| Correlation haircut | `risk_vault.calculate_correlation_haircut` | 1.0 → 0.25 between ρ 0.65–0.85 |
| Correlation veto | `risk_vault` | avg ρ > 0.85 → reject |
| Slots | `risk_vault.max_slots` | 5, hardcoded |
| Portfolio heat | `risk_vault` + `_eval_gate` | balance × leverage (2.0) |
| Single-position tail | `risk_vault` | notional × (sl% + 20 bps) ≤ 2.5 % equity |
| Soft freeze | `risk_vault` | systemic DD ≥ 5 % → no new entries |
| Daily loss limit | `risk_vault` | 3.5 % from daily high-water mark |
| Hard stop | `risk_vault` / `main` | systemic DD ≥ 15 % → emergency lock → **flatten all** |
| Per-symbol cooldown | `main._state_settle_trade` | 180 s after a loss |
| SRE breaker | `fsm.record_module_error` | 4 CRITICAL or 25 DEGRADED in 60 s → global lock |
| Liquidation sentry | lifecycle daemon | every ~7 s; force-flatten if liq within max(1.5 %, 2.5 ATR) |

Missing versus your spec: no consecutive-loss size reduction, no volatility-spike size
reduction, no long/short exposure split, no sector/cluster exposure cap (clusters are
hardcoded only as *feature* groupings in `_eval_gate`), no expected-value floor.

### Execution flow
`execute_alpha_signal` → quantize → `CASCADE` ⇒ emergency market; else estimate book slippage
→ veto if > dynamic cap → route to maker peg if > 10/15 bps → TWAP iceberg if size > 5 % of
top-3 depth → flash IOC if book skewed or `TRENDING` → else maker peg. SL/TP are attached
inline on the order **and** re-anchored via `/v5/position/trading-stop` with `MarkPrice`
trigger for SL and `LastPrice` for TP.

### Data flow
Public WS (tickers, orderbook.50, publicTrade, kline×3) → callbacks → per-symbol state.
Private WS (execution, order) → fill futures. REST for balances, positions, closed-PnL,
instruments, fee rates. SQLite WAL local ledger + batched Supabase mirror.

### State management
`GlobalStateActor` is *meant* to be the single writer for `active_positions_map`,
`in_flight_symbols`, `in_flight_notionals`, `exit_states`, `active_contexts`.
In practice `_eval_gate`, `run_fast_state_invariant_reconciliation` and the lifecycle daemon
all mutate those dicts directly as well — three concurrent writers to the "single source of
truth". Reconciliation against the exchange runs every 10 s and adopts orphans.

---

## 4. CRITICAL BUGS

Ranked by expected capital damage. Nothing here has been fixed.

### P0 — can lose money immediately

**B1. TWAP iceberg sizes orders against a phantom price of $100. [C]**
`sor._execute_twap_iceberg` calls `_execute_dynamic_maker_peg(...)` **without**
`depth_snapshot`. In the peg, `best_bid = float(bids[0][0]) if bids else 100.0`, and the very
next statement is
`cleaned_qty = self._apply_dynamic_exchange_limits(qty, best_bid, symbol)`.
The min-notional floor is therefore evaluated at $100/unit. For any instrument priced above
~$100, a slice whose true notional is a few dollars is inflated to
`6.50 / 100 = 0.065` units. On BTC at $100 k that is a **$6,500 order instead of $6.50**
(~1000×). The quote price is later refreshed from `core_engine.orderbook_snapshots`, so the
order is placed at a correct price with a catastrophic quantity. It bypasses `risk_vault`
entirely (the vault approved the *intended* notional). Only exchange margin rejection stands
between this path and a blown account. The TWAP branch triggers whenever
`total_qty > 0.05 × top-3 depth`, i.e. exactly in thin books.

**B2. An unfilled IOC exit is recorded as a completed exit. [C]**
`ExecutionGovernorFSM.manage_execution` treats `retCode == 0` as success and immediately sets
`state.execution_state = "CLOSED"; state.actual_qty = 0.0`. A Limit-IOC with a 15 bps collar
that fills zero also returns `retCode 0`. `cumExecQty` is never read. The lifecycle daemon
then breaks its monitoring loop, so the position is live on the exchange with no software
stop management. Partial fills corrupt `state.actual_qty` the same way (it subtracts the
*requested* qty on scale-out). Partially mitigated by `_state_settle_trade`, which sweeps
residual size with up to 4 market IOCs — but that also silently defeats the 15 bps collar the
module was written to enforce.

**B3. After the first universe refresh the bot goes blind. [C]**
`run_universe_refresher` (every 900 s) replaces `self.asset_basket`, calls
`_prune_dead_symbols()` and `_initialize_symbol_structures(...)` — but never resubscribes the
WebSocket and never sets `stream_restart_event`. Only `run_omni_swarm_director` performs
`hot_swap_socket_stream`, and only for a single pair at a time. Result: the stream stays
subscribed to the *old* basket while `handle_incoming_orderbook_tick` rejects every tick whose
symbol is not in the *new* basket. New symbols receive nothing. The engine can sit with zero
usable market data until a stream restart happens for unrelated reasons.

**B4. `historical_probs` stores `max(p_up, p_down)`, so two exit rules are direction-broken. [C]**
`micro_models` appends `prob = max(p_up, 1-p_up)` (always ≥ 0.5).
`intelligent_exit` reads that value as `current_p_up` and computes
`continuation_prob = current_p_up if is_buy else (1 - current_p_up)`.
Consequence: for **longs** `continuation_prob ≥ 0.5` always, so
`EARLY_FLOW_OPPOSITION` (`< 0.38`) and `ALPHA_DRIFT_INVERSION` (`< 0.42`) can **never** fire.
For **shorts** `continuation_prob ≤ 0.5` always, so both fire on essentially any adverse OFI
reading. Shorts are systematically cut early; longs never get the flow-based exit at all.

**B5. ATR is effectively zero, which silently disables the trailing stop. [C/L]**
`adaptive_engine.get_computed_atr` reads `self.timeframes["5"]`, falling back to `["1"]`,
falling back to `std(self.prices[-14:]) * 1.5`. `main` subscribes klines
`[TRADING_TIMEFRAME (default "15"), "60", "240"]` — **"1" and "5" are never populated**, so
the fallback always runs, and `self.prices` is a deque that receives a close from *every*
timeframe on *every* WS push (see B6). ATR therefore collapses to the standard deviation of
~14 near-identical consecutive ticks.
Downstream: `sl_dist_pct` floors at the hardcoded 1.5 %; the Tier-3 chandelier cushion
`atr × [0.4, 1.8]` collapses to ~0, so the trailing stop degenerates to the friction
breakeven price; the stop-advance hysteresis `target_sl > last_sl + atr*0.15` becomes
"any change", producing continuous amend attempts throttled only by the 1.2 s cooldown.
(**[L]** only because it flips if you set `TRADING_TIMEFRAME=5`.)

**B6. Klines are consumed without checking `confirm` — no candle is ever closed. [C]**
`handle_incoming_kline_update` appends `high/low/close` on every WS kline message. Bybit
pushes the *forming* candle roughly once per second. So every "bar" series in the system is
actually a ~1 Hz sample of partial candles:
- `feature_engines.timeframes["15"]` (maxlen 900) ≈ the last 15 minutes of pushes, not 900 bars.
- `timeframes["60"]` (maxlen 200) ≈ 200 **seconds**; `["240"]` (maxlen 100) ≈ 100 **seconds**.
  So `get_htf_trend_bias`'s "4 H EMA" and "1 H EMA" are sub-two-minute averages.
- `AdaptiveFeatureEngine.prices` mixes 15 m, 1 h and 4 h closes into one deque.
- `screener_memory["prices"]` (used for the **correlation matrix**) and `["highs"]/["lows"]`
  (used for **shadow trade resolution**) are the same pollution.
This is the classic "candle not closed" defect and it contaminates ATR, multi-timeframe
momentum, HTF bias, dynamic R:R, portfolio correlation, and the entire shadow ledger.

**B7. Default mode is LIVE with real keys. [C]**
`TEST_MODE` defaults to `"false"`. There is no separate `LIVE_TRADING=1` confirmation, no
`--paper` flag, no post-deploy safe mode. Also, `TEST_MODE=true` switches the executor to
*testnet* (real orders on testnet) and simultaneously disables `_state_settle_trade`, so
testnet runs record no outcomes and produce no learning data — the "paper" mode is neither
paper nor instrumented.

**B8. Any SRE error flood liquidates the whole book. [C]**
`safe_daemon_wrapper` → `fsm.record_module_error(name)`. `_eval_gate` and
`_position_lifecycle_daemon` are mapped to `CRITICAL`; 4 in 60 s trips
`trigger_global_emergency_lock`, which ends the `run_engine_forever` loop, which returns into
`main()`'s `finally: graceful_shutdown()`, which **market-flattens every open position**.
Four transient exceptions in the evaluation path — a code path that holds no risk — will
close the entire portfolio at market. The same coupling means every deploy/restart flattens
positions.

### P1 — corrupts risk accounting or learning

**B9. Slot cap, heat cap and dedupe are checked outside the reservation lock. [C]**
`risk_vault.evaluate_portfolio_safety` (dedupe, `max_slots`, heat) runs *before* the
`circuit_breaker_lock` block that reserves `in_flight_symbols`. `_eval_gate` runs concurrently
for every symbol. Two or more symbols can pass the slot/heat check against the same snapshot
and both reserve. `max_slots=5` and the leverage ceiling are therefore advisory, not enforced.

**B10. In-flight TTL (45 s) is shorter than the TWAP execution path (≈80 s). [C]**
8 slices × (5 s peg timeout + up to 8 s interval) exceeds 45 s. The reconciler purges the
in-flight lock mid-execution, and a fresh `_eval_gate` can open a second position on the same
symbol before `REGISTER_POSITION` lands. Duplicate-order exposure.

**B11. Unrealized PnL is double-counted in the live drawdown. [C/L]**
`get_wallet_balance_usdt` returns `totalEquity` first, which on Bybit UTA already includes
unrealized PnL. The lifecycle daemon then computes
`live_equity = vault_bal + unrealized_pnl` and derives `drawdown_pct` from it. That figure
feeds `PortfolioCommander`, which issues an **EMERGENCY market exit** at 15 %. The drawdown
driving the hardest kill switch in the system is mis-scaled by one position's unrealized PnL.

**B12. Unknown settlement outcomes are silently booked as zero-PnL losses. [C]**
`_state_settle_trade` polls `/v5/position/closed-pnl` 5 times and accepts only records
younger than 180 s. On miss it keeps `net_pnl = 0.0, real_outcome = "RECONCILED"`. That 0.0 is
then written to the ledger, appended to `recent_pnl_history`, fed to
`MertonJumpKellySizer.update` and to `resolve_trade_outcome` (which labels `y_up` from
`net_pnl > 0` → a loss), and `memory.log_live_execution_result` sets `is_correct = False`.
A missed poll therefore poisons the position sizer, the RLS weights, the win-rate metric and
the DNA ledger, and is indistinguishable from a genuine scratch. There is no fills-based
fallback and no "unknown" status.

**B13. The screener callback contract is broken — two features are permanently zero. [C]**
`multi_feed` calls `self.screener_callback(data)` with the raw Bybit ticker dict.
`main.handle_incoming_basket_screener_update` does `if "raw_data" in data:` — a key that never
exists. Consequences: `screener_metrics[sym]["vol_mult"]` stays at 1.0 forever (it is the
primary axis of the DNA k-NN distance), and `update_funding_metrics` is never called, so
`funding_bias` and `squeeze_risk` (features 16 and 17 of 19) are permanently 0.0, along with
the `w_cascade[20] = Squeeze × MLOFI` interaction term.

**B14. Two entry vetoes are mathematically incapable of firing. [C]**
- *Expected-drift gate:* `expected_drift = raw_score`, and `action == "BUY"` ⟺ `p_up > 0.5`
  ⟺ `raw_score > 0`. The condition `action == "BUY" and expected_drift < -0.0005` is therefore
  unreachable (symmetrically for SELL). Dead filter.
- *Spread/ATR friction sieve:* `spread = ob_payload.get("spread", 0.0001)`, but the
  `MarketStateMatrix` payload has no `"spread"` key (it emits `best_bid`/`best_ask`). Spread
  is always the literal 1e-4 **in price units**, so `spread_bps = 1/price` ≈ 1e-6 bps for BTC.
  The sieve never fires, and the same bogus value is persisted to the ledger as
  `bid_ask_spread`.

**B15. Shadow-ledger PnL is in the wrong units and mixes live rows. [C]**
`memory.resolve_batch_historical_predictions` writes
`net_pnl = (gross_return - 0.0011) * simulated_leverage` — a **return fraction** — into the
same `net_pnl` column that live trades fill with **USDT**. `evaluate_shadow_promotion` and
`compute_latent_dna_edge` then query `WHERE resolved = 1 AND symbol = ?` **without**
`is_shadow`, so the promotion Sharpe and the k-NN win rate are computed over a mixed-unit
population. The `sharpe >= 1.5` promotion gate is not measuring anything meaningful.

**B16. `target_notional` is not in `schema.sql`. [C/L]**
`memory.commit_prediction`'s Supabase payload and the shadow upsert both include
`"target_notional"`. `quantitative_ledger` in `schema.sql` has no such column and the
idempotent migration block does not add it. PostgREST rejects unknown columns, and
`_safe_execute_async` swallows the error at `logger.debug`. **[L]** only because I cannot see
the deployed schema — if it matches the file, *every cloud ledger write has been failing
silently* and the entire cloud forensic dataset is empty.

**B17. Backtest train→test transfer is invalid (whitener state is dropped). [C]**
`parameter_sweep` and the default split train with `freeze_weights=False`, then evaluate with
`initial_rls_state=trained_state` — but that dict carries only `w_*` and `f_*`. The
`BacktestAdaptiveWhitener` (mean vector, covariance, cached ZCA matrix) restarts from
identity/zero on the test slice. Frozen weights are applied to features on a different scale
than the one they were fit on. Every OOS Sharpe the optimizer reports is therefore untrustworthy,
and `params.json` — which the **live engine reads** — is selected on that basis.

**B18. Persisted model state is asymmetric. [C]**
`ContinuousMicrostructureEngine.export_state` writes `P_trending`, `P_ranging`, `P_spoof`,
`P_cascade`; `load_state` restores only `P_trending` and `P_ranging`. The spoof and cascade
covariance matrices silently reset to `I × p_scale` on every restart while their weight
vectors are restored — an inconsistent Kalman state.

**B19. Slippage estimation fails *open*. [C]**
`sor.estimate_orderbook_slippage_bps` returns `0.0` when `depth_snapshot` lacks `bids`/`asks`.
The REST fallback in `multi_feed._active_positions_rest_fallback` writes exactly such a dict
(`best_bid`, `best_ask`, `micro_price`, `timestamp` — no ladders) into
`engine.orderbook_snapshots`. During a WebSocket outage, the slippage firewall passes
everything and `get_sweeping_price` returns `mid × 1.001`. The safety check is disabled
precisely when the market data is worst.

**B20. No staleness bound on market data anywhere. [C]**
Neither `_eval_gate` nor the exit loop checks the age of `orderbook_snapshots[sym]` or
`latest_tick_price`. A silently stalled symbol feed freezes the CAMB stop at its last value
indefinitely; the only backstops are the exchange-native SL and the ~7 s liquidation probe.

**B21. Min-notional rounding silently breaches the risk cap. [C]**
`_apply_dynamic_exchange_limits` *raises* quantity to meet `max($6.50, min_notional × 1.05)`,
and `main` clips `target_notional` to a `6.50` lower bound. A position the risk engine sized
below that floor is executed above it, unreported.

**B22. `omni_scanner` RVOL and return sampling are broken. [C]**
- `market_memory[sym]["vol"]` stores `volume24h` — a rolling cumulative. A z-score over 60
  samples of it measures the *drift of a 24 h window*, not relative volume. `rvol_z ≥ 1.5`
  is not the "volume excitement" gate the docstring claims.
- The 30-minute anti-thrash cooldown `return None, None` executes **before** the ingestion
  block, so for 30 minutes after every swap no returns are recorded at all, breaking the
  60-observation continuity the PCA requires.

### P2 — performance, correctness hygiene

**B23. `len(deque) % 50 == 0` is always true once the deque is full. [C]**
`micro_models.update_trades`: `tick_prices` has `maxlen=2000`; once saturated `len` is
permanently 2000 and `2000 % 50 == 0`. So `kaufman_er` *and*
`compute_permutation_entropy` over 100 log-returns are recomputed on **every trade tick**,
forever, on the event loop. On a busy symbol this is the dominant CPU cost in the process,
and it directly delays the 50 ms exit loops.

**B24. Blocking work on the event loop.** `handle_incoming_trade` is synchronous and runs
Hurst (5 lags), OU (a 200-point regression every 10 ticks), BOCD (40-vector update), CVD
divergence (60-slice std) and the above per trade. `memory.compute_latent_dna_edge` runs a
2000-row SQLite scan plus NumPy inline. `commit_prediction` awaits a SQLite write in the
hot signal path.

**B25. `handle_incoming_orderbook_tick` copies up to 2000 floats per evaluation**
(`tick_prices_copy = list(...)`) and the only downstream use is `len(...) < 40`.
~5 evals/s × N symbols of pure waste.

**B26. Substring matching for bans and anchors.** `any(b in sym for b in BANNED_ASSET_KEYWORDS)`
with entries like `"KO"`, `"ARM"`, `"AMD"`, `"BANK"` will false-positive on unrelated tickers;
`any(m in symbol for m in ["BTC","ETH","SOL"])` treats `BTCDOMUSDT`/`WBTCUSDT` as anchors.
The ban list is duplicated verbatim in **three** files (`main`, `omni_scanner`, `bybit_v5`)
plus a fourth variant in `delta_neutral`.

**B27. `summarize` can emit `nan`.** `float(np.mean([]) or 0.0)` — `np.mean([])` is `nan`,
`nan` is truthy, so `by_regime[...]["win_rate"]` becomes `nan` for empty regimes.

**B28. Backtest metric conventions are inconsistent.** Sharpe/Sortino annualize with
`sqrt(252 × trades_per_day)` (equity convention) while `annualized_return` (feeding Calmar)
uses `× 365`. Crypto trades 365 days. Sharpe and Calmar are not on the same clock.

**B29. Monte Carlo measures nothing.** `summarize` block-bootstraps *with replacement from the
realized trade set* and reports `P(sum > 0)`. With a positive sample mean this is ≈ 1 by
construction; it quantifies neither parameter risk nor sequence risk in any decision-relevant
way.

**B30. Dead code / dead state.** `SmartOrderRouter.calculate_risk_adjusted_notional` (never
called — sizing lives in `main`); `QuantumEntryMatrix.update_mlofi_state` (explicit no-op);
`fsm.update_ai_macro_state` / `get_ai_macro_state` / `sector_macro_cache` /
`TradingState.{CALIBRATING, ABSORPTION_COOLDOWN, SECTOR_MISALIGNMENT}`;
`ProfitProtectionState.{locked_pnl, pnl_velocity, rolling_mlofi_peak}`;
`ExitDecision.log_output`; `PositionExitState.{entry_thesis, thesis_inv_cov}`;
`memory.ANCHOR_ASSETS`; `micro_models.is_model_degraded` (computed, never read);
`state["execution_style"]` (computed, never used — SOR routes independently); the entire
`delta_neutral` module.

**B31. Silent failure surfaces.** `await self.synchronize_exchange_state()` is wrapped in
`try/except: pass` at the call site — a failed startup reconciliation is invisible.
`update_orderbook_pressure` is wrapped in `except Exception: pass`. Roughly 40 `logger.debug`
except-handlers absorb exchange and DB faults with no counter or alert.

**B32. `sor` mislabels parameter errors as compliance bans.** `ret_code in [110126, 10002, 10001]`
→ "Agreement Not Signed … Quarantining for 1 hour". 10002 is a parameter error and 10001 is
qty-out-of-bounds. A formatting bug will blacklist a symbol for an hour under a misleading
log line, hiding the real cause.

**B33. `telegram_ops.format_execution_receipt` reports a meaningless gross PnL.**
`gross = net + fees + (|slip_bps|/10000 × net)` multiplies a slippage fraction by net PnL.
Not a manipulation, but a displayed metric that is simply wrong.

---

## 5. STRATEGY WEAKNESSES (structural, not cosmetic)

**S1. The model is trained on a 60-second horizon and traded on a 180-minute horizon.**
The replay buffer labels `y ∈ {0,1}` from "price up over 60 s" (SL/TP branches essentially
never trigger, since a 1.5 % move in 60 s is rare). That probability then drives the conformal
gate, the Kelly fraction and the direction of a trade with a 1.5 % stop, a ~2.5–3 % target and
a 3-hour horizon. The quantity being estimated is not the quantity being risked. This is the
deepest flaw in the system and no amount of feature engineering fixes it.

**S2. The label is sample-selection biased.** The `< 3.5 bps` deadband *discards* small-move
samples from training but the resulting probability is applied to all states. The model
estimates `P(up | |Δ| > 3.5 bps)` and is used as `P(up)`.

**S3. The exit policy cuts winners at ~0.4 R and lets losers run to 1 R.**
Composing the CAMB tiers and the reversal guards:
- `PREDATOR_REVERSAL_STRIKE`: from an MFE of 0.50 R, a 22 % giveback exits at ≈ 0.39 R.
- `DYNAMIC_PROFIT_RETRACEMENT`: from 0.70 R, a 28 % giveback exits at ≈ 0.50 R.
- Scale-out takes 50 % at 1.30 R; the runner is then held behind a chandelier whose cushion is
  ~0 (B5), i.e. effectively breakeven.
- The 2.0 R take-profit is therefore rarely reached.
Meanwhile Tier 1 compresses the stop to 0.2 × initial risk as soon as MFE ≥ 0.10 R (a ~0.15 %
move), so the *realized* risk per trade is roughly 0.2–0.3 R while position size was computed
for 1.0 R. Two consequences: (a) the bot is chronically under-risked relative to its own
sizing intent, and (b) the stop-out frequency on microstructure noise is very high. The
payoff profile needs a win rate well north of 60 % to be positive after 11 bps round-trip
friction, and nothing in the system establishes that it has one.

**S4. Live has no expected-value floor; the backtest does.**
`backtest.py` refuses a trade unless
`net_ev = p·tp − (1−p)·sl − spread/2 − fee > ev_floor`. `main._eval_gate` has no EV
computation at all — its only economic filter is the friction sieve, which is dead (B14).
The live engine therefore takes trades the backtest would reject, and the backtest's
`MAKER_ONLY` routing branch (maker fees, zero slippage) has no live counterpart at all.
This is a direct optimism bias in every comparison.

**S5. The backtest does not test this strategy.** Live features come from L2 ladders, trade
prints, cancellation flow and BBO micro-dislocation. The backtest *approximates* the same 19
slots from 1-minute OHLCV: `cfi_z = 0.0`, `funding_bias = 0.0`, `fleeting = 0.0`, MLOFI from
candle-body position, Hawkes from candle volume. Combined with B17, no backtest number in this
repository is evidence about live behaviour. It is a different, lower-information strategy that
happens to share a weight-vector shape.

**S6. The backtest also does not test the risk system.** It is single-asset, single-position,
no concurrency, no correlation haircut, no slot cap, no portfolio heat, no daily loss limit,
no drawdown breaker, no compounding. `total_return_on_margin` is a sum of per-trade
fractional returns, and `max_drawdown_on_margin` is a drawdown of that sum — not an equity
curve. It understates drawdown and cannot validate any of §3's risk layers.

**S7. Portfolio correlation control is non-functional.** The haircut and the 0.85 veto read
`risk_vault.correlation_matrix`, built by `run_correlation_engine` from 60 entries of
`screener_memory["prices"]` — which, per B6, is ~60 seconds of duplicated partial-candle
pushes. "Correlated positions" is the risk your spec flags most sharply (§16), and the
mechanism intended to control it is measuring noise. Note also that two *different*
correlation estimates coexist (`risk_vault` EWMA on polluted klines; `SectorEigenOracle` PC1
on asynchronous tick returns) with no reconciliation.

**S8. Cross-asset "macro synergy" is a fiction.** `_eval_gate` passes the *same* `parent_flow`
to both `btc_ofi_z` and `eth_ofi_z`, so `QuantumEntryMatrix`'s documented 65/35 BTC/ETH beta
weighting reduces to `1.0 × parent_flow`. And `parent_flow` is read from
`stream_feed_instance.log_mlofi_z["BTCUSDT"]`, which is only populated if BTCUSDT survived the
dynamic universe screen — it is not pinned as a reference subscription. **[?]** Whether this
is zero in production needs a log check.

**S9. Indicator redundancy is real but not the main problem.** MLOFI z, Hawkes z, CVD z,
`clean_ofi_z` and `micro_elasticity_z` all measure signed order flow on overlapping windows;
`meso_momentum_z` and `ou_divergence_z` are both (price − slow EMA)/σ with different
normalizers and near-opposite priors. The ZCA whitener is the right tool for this and largely
handles it — but whitening a 19-vector whose components are already heavily EWMA-smoothed
(the `AsynchronousStateAligner` applies a second ~2.5 s low-pass on top of each feature's own
EWMA) means the "microstructure" signals are smeared well past the horizon they describe.
Worse, the aligner's smoothing constant depends on the *evaluation cadence* (`dt` since last
call), so feature dynamics change with system load.

**S10. Regime adaptation is nominally present, practically weak.** The 4-state Markov gate is
sound in structure, but: `RegimeHysteresisFilter` needs 5 consecutive samples at ≥60 %
consensus before it will move; `SPOOF` likelihood depends on `fleeting_ratio`, which depends
on L2 depth deltas that are fed only top-5 aggregated volume; `CASCADE` requires
`max_stress > 1.8` and otherwise gets a 0.2 multiplier. The *illiquid/dangerous* regime your
spec asks for does not exist as a regime — it lives only as scattered spread/turnover filters,
two of which are dead (B14).

**S11. Hyper-parameter surface is large and mostly unvalidated.** By my count ≈ 60 magic
numbers govern entry/exit: 0.10 R / `r_crit ∈ [0.22, 0.38]` / 0.70 R / 1.30 R tiers, 22 % and
28 % giveback thresholds, 2.2σ / 1.8σ / 2.8σ / 0.65 flow thresholds, 0.38 / 0.42
continuation probabilities, 90 min / 180 min time exits, `CALIBRATED_GAIN = 0.45`,
`LOGIT_BOUND = 1.50`, `τ = 0.80`, 15 bps collar, 8 bps spread ceiling, 1.5 % SL floor.
The walk-forward optimizer tunes exactly **two** of them (`rr_ratio`, `sl_atr_mult`). The
remaining ~58 were set by hand and never tested for sensitivity. That is the definition of an
unvalidated surface, and it is where overfitting risk actually lives here — not in the RLS.

---

## 6. PERFORMANCE BOTTLENECKS

| Rank | Issue | Cost |
|---|---|---|
| 1 | B23 — entropy + Kaufman ER on every trade tick | dominant CPU; adds latency to exit loops |
| 2 | B24 — Hurst/OU/BOCD/CVD synchronous in `handle_incoming_trade` | event-loop blocking |
| 3 | B25 — 2000-float copy per evaluation for a `len()` | pure waste, ~5/s/symbol |
| 4 | `_amend_trailing_stop` spam from B5 (zero ATR hysteresis) | ~50 REST amends/min/position |
| 5 | `compute_latent_dna_edge` 2000-row scan + NumPy inline on the loop | 300 s cadence, tolerable |
| 6 | `handle_incoming_kline_update` calls `_initialize_symbol_structures` per message | dict churn ~3/s/symbol |
| 7 | `backtest.py --optimize` = 9 configs × 4 folds × 2 passes over ~43 k bars, pure Python, re-downloading klines each run | hours |
| 8 | Two thread pools + 11 daemons + 50 ms position loops on one event loop | contention under load |

---

## 7. SECURITY REVIEW

**Good:** no hardcoded secrets; keys read from environment via `dotenv`; the Telegram token is
scrubbed from error logs (`_sanitize_error`); HMAC signing with a 15 s recv-window; STP
(`smpType=CancelMaker`) on every order; `orderLinkId` idempotency with duplicate resolution;
atomic `os.replace` for the model-state file; a cloud mutex lease to prevent twin leaders.

**Issues:**
1. **B7** — default-live is a safety, not a secrets, problem, but it is the highest-severity
   item in this section.
2. `keep_alive.py` binds `0.0.0.0` with the Werkzeug dev server and publicly exposes version,
   uptime and organization name. Low severity; use a production WSGI server or bind behind the
   platform's health check only.
3. `sgd_state.json` and `titan_memory_ledger.db` are written to `PERSISTENT_STORAGE_PATH`
   (default `"."`) with no permission hardening; the SQLite ledger contains full trade history.
4. No dependency pinning is visible (`requirements.txt` was not supplied), so the supply chain
   is unverifiable. `supabase`, `scipy`, `pandas`, `aiohttp`, `flask`, `numpy`, `requests`,
   `python-dotenv` are imported.
5. The cloud lease is advisory: on any Supabase error it logs a warning and **proceeds**
   (`except Exception: ... falling back to standalone mode`), so a DB outage permits a twin
   instance to trade the same account.

---

## 8. TESTING GAPS

There are **no tests of any kind**. In priority order, the untested surfaces that can lose
money are:

1. `sor._apply_dynamic_exchange_limits` / `_format_qty_str` / `_format_price_str` — quantization
   and min-notional (B1, B21 both live here).
2. `ExecutionGovernorFSM.manage_execution` — fill accounting (B2).
3. `IntelligentExitEngine.evaluate` — the CAMB tier ladder, monotonicity, and the R-multiple
   arithmetic (B4 lives here).
4. `risk_vault.evaluate_portfolio_safety` — every limit, including concurrent callers (B9).
5. `_state_settle_trade` — PnL/slippage attribution and the unknown-outcome path (B12).
6. `memory` — shadow resolution arithmetic and unit consistency (B15).
7. Feature engines — determinism and NaN/zero-division behaviour on degenerate books.
8. Failure injection: API timeout, rejected order, partial fill, WS disconnect, duplicate
   signal, restart recovery, missing SL.
9. Backtest integrity: a synthetic series with a known answer, plus an explicit look-ahead
   assertion.

---

## 9. RECOMMENDED OPTIMIZATION PLAN

Sequenced so that nothing is measured until the measuring apparatus is trustworthy.
Each step is small enough to verify independently, per your §22.

### Stage 0 — make it safe to touch (no strategy change)
- **0.1** Pin dependencies; add `pytest`, `ruff`, `mypy` config and a CI entry point.
- **0.2** Invert the mode default: require an explicit `LIVE_TRADING=1` **and** a non-empty
  `BYBIT_API_KEY` to trade real money; make paper mode a true simulator (no exchange orders)
  that still runs settlement and learning, separate from testnet mode. *(B7)*
- **0.3** Central `config.py` dataclass: single home for every threshold currently duplicated
  across `main` / `sor` / `risk_vault` / `intelligent_exit` / `omni_scanner` / `bybit_v5`,
  loaded from env with validation at boot. One ban-list, one min-notional, one fee schedule,
  one leverage cap. *(B26, config sprawl)*
- **0.4** Golden-path characterization tests for the modules in §8 so later changes are
  provably behaviour-preserving where intended.

### Stage 1 — P0 correctness (highest expected value; strictly before any tuning)
- **1.1** B1 — pass `depth_snapshot` through `_execute_twap_iceberg`; make
  `_apply_dynamic_exchange_limits` **raise** on a non-positive or defaulted reference price
  rather than silently substituting 100.0; add a hard notional sanity assert
  (`|actual − intended| / intended < 0.25`) before every order submission.
- **1.2** B2 — read `cumExecQty` / `orderStatus` after every exit order; only mark `CLOSED`
  on verified zero remaining size; subtract *filled* qty on scale-out; re-verify against
  `/v5/position/list` on ambiguity.
- **1.3** B3 — make `run_universe_refresher` diff old vs new basket and drive
  `hot_swap_socket_stream` (or set `stream_restart_event`) for every change.
- **1.4** B4 — store signed `p_up` (not `max(p_up, p_down)`) in a dedicated field the exit
  engine reads; add a regression test asserting both exit rules fire symmetrically for
  longs and shorts.
- **1.5** B6 — honour the kline `confirm` flag; append only closed candles; subscribe the
  timeframes the code actually reads. Then B5 (ATR) resolves as a consequence — re-verify.
- **1.6** B8 — decouple the SRE breaker from liquidation: an error flood should stop *new
  entries* and page, not flatten. Reserve flatten for the drawdown breaker and explicit kill.
  Also stop `graceful_shutdown` from flattening on a clean restart (distinguish
  SIGTERM-with-handoff from emergency).
- **1.7** B19/B20 — fail *closed*: a missing or stale (> N seconds) book vetoes entry and
  escalates exit handling to the exchange-native stop; add an explicit `as_of` timestamp to
  every snapshot and assert freshness at both use sites.

### Stage 2 — risk-engine integrity
- **2.1** B9/B10 — move the slot/heat/dedupe check *inside* the reservation lock; make the
  `GlobalStateActor` the only writer to position/in-flight state and delete the direct
  mutations; size the in-flight TTL from the actual execution budget of the chosen route.
- **2.2** B11 — define one equity accounting convention (realized wallet vs mark equity),
  document it, and use it consistently in the vault, the heartbeat and the lifecycle daemon.
- **2.3** B12/B33 — add an `UNKNOWN` settlement state that is excluded from the Kelly sizer,
  the RLS labels and the win-rate metric; add a fills-based (`/v5/execution/list`) PnL
  fallback; fix the receipt's gross-PnL formula; capture funding from the transaction log.
- **2.4** Add the controls your spec asks for and the code lacks: consecutive-loss size
  reduction, volatility-spike size reduction, long/short exposure split, cluster exposure cap.
- **2.5** B21 — when the risk-sized notional is below the exchange minimum, **skip the trade**
  instead of rounding up.

### Stage 3 — measurement apparatus (must precede any strategy tuning)
- **3.1** B17 — serialize and restore whitener state across backtest folds; add an assertion
  that test-slice feature moments match training moments within tolerance.
- **3.2** B16/B15 — reconcile `schema.sql` with the actual insert payloads; add `is_shadow` to
  every promotion/DNA query; store shadow PnL in a separate column with explicit units; stop
  swallowing PostgREST errors (count them, alert on a nonzero rate).
- **3.3** B28/B29/S6 — one annualization convention (365); replace the bootstrap with
  something decision-relevant (parameter-perturbation sensitivity and a trade-sequence
  permutation test); build a **portfolio-level** backtest that runs the real `risk_vault`,
  slot caps, correlation haircut, daily loss limit and drawdown breaker over multiple assets
  concurrently on an actual equity curve.
- **3.4** S5 — decide honestly what the backtest is for. Either (a) replay archived L2/trade
  data so the simulated features equal the live features, or (b) demote `backtest.py` to a
  sanity harness and rely on a **shadow/paper forward test** as the primary evidence. Option
  (a) is expensive; option (b) is what I would recommend, with the shadow ledger fixed first.
- **3.5** Fix the shadow simulator's bar alignment (falls out of 1.5) and make paper mode the
  default baseline for "BEFORE".

### Stage 4 — signal and strategy (only once Stages 1–3 are green)
- **4.1** S1 — align the learning horizon with the trading horizon. Concretely: label from the
  actual trade outcome (SL/TP/time, net of fees) at the real horizon, and keep the 60 s head
  as an auxiliary task at most. This is the single highest-value strategy change and it will
  change everything downstream, so it must come after the measurement fixes.
- **4.2** S2 — remove the deadband from the label, or apply the same conditioning at inference.
- **4.3** S3 — rebuild the exit policy around measured conditional expectancy: estimate
  `E[R | MFE, regime, flow]` from the ledger and set giveback thresholds from it rather than
  from hand-picked 22 %/28 % constants. Test the null hypothesis that a plain
  chandelier + fixed target beats the current ladder — with a working ATR, it may.
- **4.4** S4 — add a live EV floor mirroring the backtest: no entry unless
  `p·tp − (1−p)·sl − spread − fees − expected slippage > floor`.
- **4.5** B13/B14/S8 — repair the screener contract, emit `spread` in the payload, pin BTC/ETH
  as reference subscriptions, and feed genuinely distinct BTC and ETH flows. Then re-test
  whether `funding_bias`, `squeeze_risk`, `vol_mult` and the macro terms carry any signal at
  all — several may deserve deletion rather than repair.
- **4.6** S7 — rebuild the correlation matrix on closed bars at a real timeframe; reconcile or
  merge the two correlation estimators.
- **4.7** S11 — extend the walk-forward sweep beyond two parameters: sensitivity-test the exit
  ladder, the logit gain and the conformal floor. Flag any parameter whose performance is a
  spike rather than a plateau.

### Stage 5 — performance
- B23, B24 (move heavy per-tick math off the loop or downsample it), B25, the amend-spam from
  B5, and kline-handler churn. Re-measure loop latency before and after; the target is that
  the 50 ms exit loop actually runs at 50 ms under full basket load.

### Stage 6 — dispose of dead weight
- Decide on `delta_neutral.py`: wire it up with its own margin budget and a portfolio-level
  interaction test, or delete it. Leaving 850 lines of untested execution code in the tree
  that *looks* live is a hazard.
- Remove the B30 dead state, or implement it deliberately.

---

## 10. WHAT IS GOOD (worth preserving)

So the plan is not read as "rewrite everything":

- Exchange-native SL/TP with `MarkPrice` triggers, re-anchored after fill, with a mark-clash
  realignment path. This is the single most valuable safety property in the system.
- `orderLinkId` idempotency plus pre-retry existence checks and duplicate-ID resolution —
  genuinely well done.
- Pure-`Decimal` lot/tick quantization with floor rounding.
- The 10 s exchange-truth reconciliation daemon with orphan adoption.
- Verified emergency flatten (re-polls size, retries 5×, escalates on failure).
- Atomic model-state persistence via `os.replace`.
- Token-bucket rate limiting that computes the delay inside the lock and sleeps outside it.
- WS watchdogs on both public and private streams with a bounded reconnect ceiling.
- Purged-and-embargoed walk-forward CV in `parameter_sweep` (López de Prado) — the right
  methodology, currently undermined only by B17.
- O(1) bisect order-book ladders and single-slot conflation workers — a sound ingestion design.
- Self-trade prevention on every order path.

---

## 11. HONEST BOTTOM LINE

The engineering craft in this repository is high — the execution layer, reconciliation and
persistence are better than most retail systems. But three things are true simultaneously:

1. **Several data pipelines that feed the strategy are silently disconnected or
   mis-sampled** (B5, B6, B13, B14, B3). A large fraction of the 19-feature manifold is
   constant, and the multi-timeframe layer is measuring seconds where it claims hours. The
   system is far simpler in practice than it appears in source.
2. **There is currently no valid evidence that the strategy is profitable.** The backtest
   models a different feature set (S5), transfers weights incorrectly across folds (B17),
   omits the entire risk system (S6), lacks the live EV floor (S4), and the live ledger's
   cloud writes may never have landed (B16). I cannot compute a "BEFORE" baseline from what
   was supplied, and I will not produce one that isn't real.
3. **The exit policy as composed is structurally biased toward small wins and full-size
   losses** (S3), and the learning objective does not match the traded objective (S1).

The order matters: fixing the strategy before fixing the measurement would just be guessing
more expensively.

---

## 12. WHAT I NEED TO PROCEED

To move past Stage 0 without guessing:

1. `requirements.txt` / lockfile, and the real directory layout (`main.py` imports
   `core.*`, `features.*`, `execution.*`, `portfolio.*`, `ingestion.*`, `services.*` —
   I only received flat files).
2. The **deployed** Supabase schema (to settle B16), or permission to query it.
3. A redacted `.env` template — specifically `TRADING_TIMEFRAME`, `MAX_DRAWDOWN_PCT`,
   `MAX_SINGLE_POSITION_RISK_PCT`, `TEST_MODE`, `FREEZE_RLS_WEIGHTS`,
   `ENABLE_MICRO_HORIZON_LEARNING` — several findings above are conditional on these.
4. Any existing `params.json`.
5. Recent production logs (even 30 minutes), which would settle B3, B13, S8 and the B16
   insert-failure rate in one pass.
6. Whether a live account is currently running this code. If so, **B1, B2 and B7 warrant
   halting it before anything else.**

Nothing in the repository has been modified.
