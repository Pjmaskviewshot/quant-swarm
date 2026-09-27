# Audit History

What was found, in what order, and what changed. Kept because the *reasoning*
behind a fix is what stops it being undone six months later by someone who sees
only the code.

## Phase 1 — repository audit

Static review of 22 modules against a live Bybit trading system. Produced
`PHASE1_AUDIT.md`. Findings were labelled by evidence class: `[C]` confirmed by
reading, `[?]` unresolved, `[R]` reproduced.

## Phase 0 — verification gate

Refused to apply any fix until repository identity was positively established
against the running bot, and until the two headline defects were **reproduced**
rather than asserted.

Both were upgraded from `[C]` to executed reproductions against the real
repository at HEAD:

* **B1** — quantisation rounding a sub-minimum order **up**, turning $500 into
  $6,500 (13x).
* **B2** — a `Filled` order acknowledgement marking a position `CLOSED` while
  the exchange still held 10 units, leaving it unmanaged.

One item stayed `[?]`: whether a zero-fill IOC returns `Cancelled` or
`Rejected`. Two Bybit doc pages disagree and no live test was possible. It is
still open, and safety does not depend on it.

**A patch of mine failed to apply.** Generated from CRLF uploads against an LF
repository: `5 of 6 hunks FAILED (different line endings)`. Regenerated. This is
why the instruction not to apply patches blindly was correct.

## Phase 2 — P0 safety gate

Classified remaining defects by capital risk, then reviewed the paper broker
against twelve verification points — and **retracted a PAPER READY verdict**
after finding defects in the paper broker I had written:

* `__getattr__` delegated to the live executor. Reproduced: the engine read
  `9999.99` from the live account while the paper book held `999.45`;
  `adjust_leverage` reached the live account; a $100 account opened a $1M
  position and went to −450.

## APEX overhaul — this branch

**Defects found by property testing.** `EquitySnapshot.is_usable()` required
`equity > 0.0`, so an account wiped to exactly zero read as UNKNOWN, callers
fell back to a stale cached balance, and the drawdown breaker never saw a 100%
loss. Negative equity had the same problem. No example test would have chosen
exactly 0.0 as an input.

**Defects found by failure injection.** A corrupt persisted model-state cache
raised `ValueError` from `load_state`, which is called during symbol
initialisation without a guard — one bad cache entry prevented startup entirely.

**Defects found by static analysis.** Three, detailed in
`reports/qa_static_analysis.md`, including a `NameError` in the position exit
loop introduced by my own B19/B20 fix, which would have broken position
management on the first iteration for every open position.

**Defect found by reasoning about storage.** SQLite does not store NaN — it
writes NULL. Every read site coerces NULL to 0.0. A NaN PnL therefore became a
**breakeven trade** in the win rate, the expectancy, the Kelly update and the
model labels; and since `net_pnl > 0` is False for NaN, it was also filed as a
loss. Fixed at the write site and backstopped by reconciliation check C6b.

**A test I wrote that was wrong.** An assertion that a regime-shift dataset must
degrade across its halves. It failed, and the failure was the test's: it ran the
halves independently with no state transfer, so it compared two different
regimes rather than in-sample against out-of-sample. Replaced with direct tests
of state transfer. Recorded in `reports/instrument_validation.md` rather than
quietly deleted.

**An optimisation that was rejected.** A vectorised replacement for the
34%-of-runtime hot function, proven bit-identical over 400 randomised inputs
(max absolute difference exactly 0.0) — and not adopted, because it was only
1.2x faster and efficiency is the lowest-priority objective. See
`reports/performance_profile.md`.

## The pattern worth keeping

Three of the defects in this audit were introduced **by the audit itself**, and
all three were caught by a different technique than the one that motivated the
change: static analysis caught the fix for a freshness bug; a paper-broker
review caught the paper broker; property testing caught an equity guard that
example tests had passed.

No single technique is sufficient. That is the argument for the staged
deployment process in `docs/DEPLOYMENT.md`, and the reason nothing here should
reach live without testnet.
