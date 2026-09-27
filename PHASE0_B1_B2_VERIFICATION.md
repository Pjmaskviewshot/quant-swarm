# PHASE 0 — REPOSITORY VERIFICATION GATE + B1/B2 VERIFICATION

**Gate result: repository identity CONFIRMED `[C]`. Live-deployment correspondence UNRESOLVED `[?]`.**

No repository file was modified. `git status --short` is empty; HEAD remains `1c8ac82`.
All verification was performed in disposable copies under `/home/claude/`.

---

## 1. REPOSITORY IDENTITY

| Item | Value | Conf |
|---|---|---|
| Remote | `https://github.com/Pjmaskviewshot/quant-swarm.git` | `[C]` |
| Branch | `main` (only branch; `origin/HEAD -> origin/main`) | `[C]` |
| HEAD | `1c8ac823d89843d34a208b16be84f04bb7e668c3` | `[C]` |
| HEAD message | `fix(quant): resolve RLS weight erosion, CAMB mark clamping, and micro-account sizing` | `[C]` |
| Working tree | clean — no uncommitted changes | `[C]` |
| Commits | 395 | `[C]` |
| History span | 2026-06-16 → 2026-09-11 (author `Pjmaskviewshot`) | `[C]` |
| Entrypoint | `src/main.py` → `if __name__ == "__main__": asyncio.run(main())` | `[C]` |
| Tracked files | 22 | `[C]` |

### Tree

```
.gitignore
params.json                  {"rr_ratio": 2.0, "sl_atr_mult": 2.5, "LEVERAGE_CAP": 2.0}
requirements.txt
src/main.py                  ← entrypoint
src/backtest.py
src/keep_alive.py
src/core/          fsm.py  intelligent_exit.py  memory.py  quantum_entry.py
src/database/      schema.sql
src/execution/     delta_neutral.py  sor.py
src/features/      adaptive_engine.py  micro_models.py  omni_scanner.py
src/ingestion/     multi_feed.py
src/portfolio/     risk_manager.py  risk_vault.py
src/services/      bybit_v5.py  sector_oracle.py  telegram_ops.py
```

**Absent:** `tests/`, `README`, any `.md`, CI config, `Dockerfile`, `Procfile`,
`render.yaml`, `railway.json`, `runtime.txt`, `.github/`. None has *ever* existed
in history (checked with `git log --all -- <path>` for each) `[C]`.

`PHASE1_AUDIT.md` is absent from the repository, as expected — it was produced in
this session and never committed `[C]`.

### Modules used by the entrypoint

`src/main.py` imports resolve to exactly the audited files `[C]`:

```
core.fsm · core.memory · core.quantum_entry · core.intelligent_exit
features.adaptive_engine · features.omni_scanner · features.micro_models
execution.sor · execution.delta_neutral
portfolio.risk_vault · ingestion.multi_feed
services.bybit_v5 · services.telegram_ops · services.sector_oracle
```

So the authoritative files are **`src/execution/sor.py`** and
**`src/core/intelligent_exit.py`**.

---

## 2. CRITICAL IDENTITY TEST

| # | Question | Result | Conf |
|---|---|---|---|
| 1 | Same `sor.py`? | **Yes — byte-identical** | `[C]` |
| 2 | Same `intelligent_exit.py`? | **Yes — byte-identical** | `[C]` |
| 3 | Same package structure? | **Yes**, under `src/` | `[C]` |
| 4 | Does `main.py` import the audited modules? | **Yes**, all 14 | `[C]` |
| 5 | Does history contain the audited code? | **Yes** — version headers match exactly | `[C]` |
| 6 | Is this the repo corresponding to the live bot? | **Unresolved** — see §6 | `[?]` |

### 2.1 Byte-level comparison

All 10 uploaded `.py` files were diffed against their repository counterparts.
Every file showed a differing MD5 but an **identical line count** — the signature
of a line-ending difference. Confirmed:

```
uploaded: Python script, UTF-8, with CRLF line terminators   (1115 CR in sor.py)
repo:     Python script, UTF-8                               (0 CR)
```

After `tr -d '\r'`, **all 10 files diff to zero lines**:

```
memory.py IDENTICAL      backtest.py IDENTICAL     bybit_v5.py IDENTICAL
delta_neutral.py IDENT.  sor.py IDENTICAL          micro_models.py IDENTICAL
risk_vault.py IDENTICAL  intelligent_exit.py ID.   main.py IDENTICAL
multi_feed.py IDENTICAL
```

The uploads were a Windows checkout of this repository. `[C]`

### 2.2 Version-header corroboration

Independent of hashing, the module banners match the Phase 1 audit's descriptions
exactly `[C]`:

```
main.py             V50.1 APEX TITAN: FAULT-TOLERANT BARE-METAL CORE ORCHESTRATOR
sor.py              V50.0 APEX TITAN: DIRECT-DRIVE HIGH-FREQUENCY SMART ORDER ROUTER
intelligent_exit.py V50.0 APEX TITAN: SMART PREDATOR ... MICROSTRUCTURE BARRIER
micro_models.py     V50.0 APEX TITAN: 25D VOLTERRA-RIEMANNIAN MICROSTRUCTURE ENGINE
memory.py           V49.0 APEX TITAN: PURE-ASYNC FORENSIC & TCA MEMORY LEDGER
risk_vault.py       V43.0 INSTITUTIONAL RISK VAULT
multi_feed.py       V40.3 APEX TITAN: HIGH-FREQUENCY ZERO-LATENCY MARKET STATE MATRIX
bybit_v5.py         V44.1 APEX TITAN: TITANIUM API EXECUTOR (BYBIT V5)
backtest.py         V50.0 APEX TITAN: HIGH-FIDELITY NEURAL BACKTESTER
```

### 2.3 Identifier search

| Identifier | Occurrences | Note |
|---|---|---|
| `_execute_twap_iceberg` | 2 | definition + call |
| `_execute_dynamic_maker_peg` | 5 | definition + 4 call sites |
| `manage_execution` | 5 | |
| `Bybit` | 40 | |
| `/v5/position/list` | 13 | |
| `/v5/order/realtime` | 6 | |
| `/v5/order/history` | 2 | |
| `pybit` | **0** | declared in `requirements.txt`, never imported |
| `ExitFillStatus` | **0** | my patch — correctly absent |
| `NOTIONAL_DEVIATION_TOLERANCE` | **0** | my patch — correctly absent |
| `best_bid = 100.0` | **0** | *my search string was wrong* — see below |

**Correction on record:** the literal `best_bid = 100.0` does not occur because
the code is an inline conditional. The actual construct is at
`src/execution/sor.py:818-819`:

```python
best_bid = float(bids[0][0]) if bids else 100.0
best_ask = float(asks[0][0]) if asks else 100.0
```

The vulnerability is present. My initial grep pattern was at fault, not the finding.

### 2.4 Has B1/B2 ever been fixed in history?

```
git log --all -S "depth_snapshot=slice_book"                    → 0 commits
git log --all -S "ExitFillStatus"                               → 0 commits
git log --all -S "cumExecQty" -- src/core/intelligent_exit.py   → 0 commits
```

Neither defect has ever been addressed in 395 commits. `[C]`

### Verdict

**The repository is confirmed to be the codebase that was audited in Phase 1** —
byte-identical modules, matching package structure, matching imports, matching
version banners, and both audited defects present and never fixed. `[C]`

---

## 3. B1 — VERIFIED AGAINST THE REAL REPOSITORY

### 3.1 Original implementation

`src/execution/sor.py:816-830` (`_execute_dynamic_maker_peg`):

```python
bids = depth_snapshot.get("bids", []) if depth_snapshot else []
asks = depth_snapshot.get("asks", []) if depth_snapshot else []
best_bid = float(bids[0][0]) if bids else 100.0      # ← phantom price
best_ask = float(asks[0][0]) if asks else 100.0
...
cleaned_qty = self._apply_dynamic_exchange_limits(qty, best_bid, symbol)
```

`src/execution/sor.py:1010-1013` (`_execute_twap_iceberg`) — the trigger:

```python
success, fill_price, fill_qty = await self._execute_dynamic_maker_peg(
    symbol=symbol, direction=direction, qty=slice_qty,
    sl=None, tp=None, timeout=chunk_timeout, regime=regime
)                                          # ← no depth_snapshot
```

### 3.2 Reproduction — executed against repo HEAD `1c8ac82`

```
REPO HEAD 1c8ac82 — B1 reproduction
  intended qty      : 0.005 => $500.00
  SUBMITTED qty     : 0.065 => $6500.00
  inflation factor  : 13x
  reduceOnly        : NOT SET
```

`reduceOnly` is not set, so this is a **position-opening** order. `[C]`

### 3.3 Exact mechanism

1. TWAP omits `depth_snapshot` → peg receives `None`.
2. `bids`/`asks` are empty → `best_bid = best_ask = 100.0`.
3. `_apply_dynamic_exchange_limits(qty, 100.0, symbol)` computes
   `notional = stepped_qty × 100.0`.
4. That notional falls below the `max($6.50, min_notional × 1.05)` floor.
5. The floor is satisfied by solving `req_tokens = 6.50 / 100.0 = 0.065`.
6. `0.065` units are submitted — sized for a $100 instrument, executed on a
   $100,000 one.

The error scales with `real_price / 100`, so it is worst on the highest-priced
instruments. It bypasses `risk_vault` entirely, because the vault approved the
*intended* notional and never observes the submitted one.

### 3.4 Patch status — **ADAPTATION REQUIRED, NOT CLEAN APPLICATION**

The emergency patch **failed** against the real repository:

```
checking file sor.py
Hunk #1 FAILED at 62 (different line endings).
Hunk #2 FAILED at 124 (different line endings).
... 5 of 6 hunks FAILED
```

Cause: the patch was generated from CRLF uploads; the repository is LF.
This is a packaging defect in my patch, not a semantic conflict. Regenerated
against repo files as **`B1_sor_REPO.patch`**, which dry-runs clean:

```
checking file sor.py          (no errors)
checking file intelligent_exit.py  (no errors)
```

Semantic content verified unchanged across the regeneration
(`diff` of normalised patched file vs. regenerated file → 0 lines). `[C]`

### 3.5 Invariants — all enforced

| Invariant | Status | Enforced by |
|---|---|---|
| Reference price finite and > 0 | ✅ | `_is_usable_price` |
| No phantom/fallback price invented | ✅ | `_resolve_reference_price` |
| TWAP slices receive current depth | ✅ | slice-level engine snapshot refresh |
| Intended-vs-actual notional checked | ✅ | `_check_notional_sanity` |
| Min-notional rounding explicit | ✅ | carve-out, capped at exchange floor |
| Invalid price fails closed | ✅ | returns `0.0`, callers abort |
| Rejected sanity check → no submission | ✅ | verified by test |

No invariant was weakened to make a test pass.

### 3.6 Tests

16 tests. Against pristine repo code: **12 failed**. Against patched repo code:
**16 passed**. Plus 5 regression tests that pass in *both* states, confirming
normal trading semantics are unchanged.

### 3.7 Remaining limitations

- `[?]` The engine-snapshot fallback may serve a **stale** book. It is strictly
  better than a phantom price, but staleness is unbounded until B19/B20 is fixed.
- `[C]` Sub-minimum orders are still rounded *up* to the exchange floor. That is
  **B21** and is Phase 2 work; the correct fix is to skip the trade.
- `[?]` The risk vault still never sees the submitted notional — only the guard
  does. Closing that loop requires a `main.py` change, deliberately out of scope here.

---

## 4. B2 — VERIFIED AGAINST THE REAL REPOSITORY

### 4.1 Original implementation

`src/core/intelligent_exit.py:493-505`:

```python
if isinstance(res, dict) and res.get("retCode") == 0:
    if decision.action in ["EXIT", "CLOSE", "EMERGENCY"] or decision.target_q <= 0.01:
        state.execution_state = "CLOSED"
        state.actual_qty = 0.0
        state.q_retained = 0.0
    elif decision.action == "SCALE_OUT":
        state.actual_qty = float(Decimal(str(current_actual_qty)) - Decimal(qty_str))
    return True
```

`cumExecQty` is never read. Scale-out subtracts the **requested** quantity.

### 4.2 Reproduction — executed against repo HEAD `1c8ac82`

```
REPO HEAD 1c8ac82 — B2 reproduction (zero-fill IOC exit)
  manage_execution returned : True
  state.execution_state     : CLOSED
  state.actual_qty          : 0.0
  ACTUAL size on exchange   : 10.0
  cumExecQty ever read      : False

  => engine believes FLAT; exchange holds 10.0 units. Daemon breaks monitoring loop.
```

`[C]`

### 4.3 Patch status

Same CRLF failure and same resolution — **`B2_intelligent_exit_REPO.patch`**,
dry-runs clean. `CLOSED` is now reachable only via
`_fetch_position_size(...) <= DUST_QTY`. 15 B2 tests pass against patched repo code.

### 4.4 Bybit V5 API assumptions — verified against vendor documentation

This is the section where my emergency-patch fakes were most exposed. Findings:

| Assumption | Documentation says | Impact |
|---|---|---|
| `retCode == 0` ≠ filled | **"The acknowledgement of a place order request indicates that the request was successfully accepted. This request is asynchronous so please use the websocket to confirm the order status."** Create-order returns **only `orderId` and `orderLinkId`**. | **B2 confirmed by the vendor's own docs `[C]`.** This is now stronger evidence than my code reading. |
| `avgPrice` may be empty | *"returns `""` for those orders without avg price"* | `[C]` — `_safe_float` handles it. Confirmed correct. |
| `size` is a string | *"size \| string \| Position size, always positive"* | `[C]` — `_safe_float` handles it. |
| `/v5/order/realtime` finds completed orders | *"primarily retrieves unfilled or partially filled orders"*; after a server restart, *"filled, cancelled, and rejected orders of Unified account should only be queried through order history"* | `[C]` — **validates the realtime→history fallback chain.** A completed IOC may be absent from realtime. |
| Zero-fill IOC status | **UNRESOLVED.** One doc page's summary indicates `Rejected`; the enum page describes `Cancelled` as *"may have an executed qty"* in derivatives. The create-order page does not state it. | **`[?]` — my classifier maps `rejected`→`CANCELLED` and `cancelled`→`UNFILLED`. If reality is the reverse, the two labels swap.** Safety is unaffected: neither status can reach `CLOSED`, because closure is gated on position size, not on status. |
| `PartiallyFilledCanceled` | *"Only spot has this order status"* | `[?]` — not handled by name. Falls through to `filled > 0 → PARTIALLY_FILLED`, which is correct. **Relevant to `delta_neutral.py`, which trades spot.** |
| `/v5/position/list` with a symbol | *"it returns data regardless of having position or not"* | `[L]` — likely a row with `size: "0"` rather than an empty list. Both are handled (`[] → 0.0`; `"0" → 0.0`). |
| `positionIdx` 0 = one-way | Confirmed: `0` one-way, `1` hedge-buy, `2` hedge-sell | `[C]` |

**Assumption explicitly marked unsafe:** `_fetch_position_size` reads `rows[0]`
without filtering on `positionIdx`. `main.py` forces one-way mode, so one row is
expected — but `BYBIT_POSITION_IDX` is environment-configurable and could be set
to `1` or `2`. In hedge mode `rows[0]` could be the *opposite* side, which would
make the reconciliation wrong in the dangerous direction. **This is a defect in my
patch, disclosed rather than hidden**, and is listed as `P0-B2a` in the plan.

### 4.5 Remaining limitations

- `[?]` Status-label mapping for zero-fill IOC (above). Resolve by capturing one
  real rejected/cancelled IOC response in paper or testnet.
- `[C]` Adds one `/v5/position/list` call per exit — latency and rate-limit cost.
- `[C]` Fakes model the API; only paper/testnet traffic can validate them fully.

---

## 5. NEW FINDINGS FROM THE REAL REPOSITORY

Not visible in the flat uploads:

| ID | Finding | Conf |
|---|---|---|
| **B34** | `requirements.txt` declares **7 unused dependencies**: `pybit`, `openai`, `groq`, `aiosqlite`, `asyncpg`, `pycryptodome`, `httpx` — zero imports each. Supply-chain surface for no benefit. `pybit==5.7.0` is notable: the Bybit SDK is declared but the code hand-rolls REST/WS. | `[C]` |
| **B35** | `.gitignore` is malformed — line 3 reads `data/. e n v` (space-separated characters, likely a UTF-16 paste artefact). `.env` on line 1 works, so secrets are ignored, but the third rule matches nothing. | `[C]` |
| **B36** | `numpy==1.26.4` is pinned; my verification ran on numpy 2.4.4. Behavioural differences are possible in edge cases. Tests should run against the pinned version. | `[C]` |
| **B37** | No deployment configuration has **ever** been committed. Deployment is therefore configured outside version control (likely the Render dashboard), so there is no reviewable, reproducible deploy definition and no rollback artefact. | `[C]` |
| **B38** | History shows repeated **duplicate commit messages** (e.g. the same `fix(network): deploy unmanaged websocket read loop...` message across 5+ distinct hashes). Signature of force-push/rebase churn or an unreliable commit workflow. Undermines history as an audit trail. | `[L]` |
| **B16** | **Confirmed against real code.** `target_notional` appears 9× in `src/core/memory.py` and **0×** in `src/database/schema.sql`. | `[C]` repo / `[?]` deployed |
| — | **No secrets in history.** All credentials via `os.getenv`; no hardcoded key patterns; `.env` never committed. Good. | `[C]` |

Environment variables referenced in code (18 total):
`BYBIT_API_KEY`, `BYBIT_API_SECRET`, `BYBIT_POSITION_IDX`, `ENABLE_KEEP_ALIVE`,
`ENABLE_MICRO_HORIZON_LEARNING`, `FREEZE_RLS_WEIGHTS`, `MAX_DRAWDOWN_PCT`,
`MAX_SINGLE_POSITION_RISK_PCT`, `MICRO_HORIZON_SEC`, `MIN_REQUIRED_EQUITY`,
`PERSISTENT_STORAGE_PATH`, `PORT`, `SUPABASE_KEY`, `SUPABASE_URL`,
`TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `TEST_MODE`, `TRADING_TIMEFRAME`.

**There is no `LIVE_TRADING` variable — B7 confirmed against real code `[C]`.**

`TRADING_TIMEFRAME` exists but its deployed value is in the gitignored `.env`.
**B5 (ATR collapse) remains `[L]`** and resolves to `[C]` only if that value is
not `1` or `5`.

---

## 6. DEPLOYMENT STATUS — UNRESOLVED

| Question | Answer | Conf |
|---|---|---|
| Is the repo the audited codebase? | **Yes** | `[C]` |
| Is the repo identical to live? | **Unknown** | `[?]` |
| Is live ahead of repo? | Unknown | `[?]` |
| Is live behind repo? | Unknown | `[?]` |

**Evidence available:** HEAD dated 2026-09-11; today is 2026-09-17 — a six-day
gap. Working tree is clean. No deployment manifest has ever been committed, so
nothing in the repository declares what runs where.

**Evidence absent:** no deploy logs, no running-instance version endpoint reading,
no confirmation of which commit the host has checked out.

**This matters practically.** `keep_alive.py` exposes `/health` returning
`{"version": "V1.0 TITANIUM APEX", ...}` — a hardcoded string, not a commit hash,
so it cannot identify the deployed revision even if queried. **Making the health
endpoint report the actual commit SHA is a cheap, high-value P0 fix** (`P0-NEW-1`),
because without it no future change can be verified as deployed.

Until that is resolved, every statement about the live bot carries `[?]`, and
**B1/B2 must not be described as fixed in production** under any circumstance.

---

## 7. GATE DECISION

| Gate | Result |
|---|---|
| Repository identified as the audited codebase | **PASS** `[C]` |
| B1 reproduced against real code | **PASS** — 13×, $500 → $6,500 `[C]` |
| B2 reproduced against real code | **PASS** — `CLOSED` with 10 units live `[C]` |
| Emergency patch applies cleanly as delivered | **FAIL** — CRLF mismatch; adapted |
| Adapted patch applies and passes | **PASS** — 36/36 on repo code `[C]` |
| Repository == live deployed code | **UNRESOLVED** `[?]` |
| Bybit API assumptions verified | **PARTIAL** — 5 confirmed, 3 open `[?]` |

**Phase 0 repository audit may proceed. Production claims may not.**

Nothing has been applied to the repository. `git status` clean, HEAD `1c8ac82`.
