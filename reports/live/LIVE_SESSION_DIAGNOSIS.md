# Live Session Diagnosis — September 2026

Source: the Telegram log of 27 closed trades on a ~$77 account, supplied by the
owner. Raw data: `session_2026-09_trades.csv`. Every number below is
recomputable from that file and the scripts in `src/research/exit_lab.py`.

---

## What the account actually did

| | reported by the bot | actual |
|---|---|---|
| win rate | 70.4% | **66.7%** |
| average win | — | +0.042 USDT (≈ +0.22% of notional) |
| average loss | — | −0.150 USDT (≈ −0.79% of notional) |
| **loss ÷ win** | — | **3.60×** |
| win rate needed to break even | — | **78.3%** |
| profit factor | — | **0.56** |
| net over 27 trades | — | **−0.60 USDT** |

The owner's own diagnosis was correct: *high win rate, still losing, because
winners are closed early while losers run.* The largest win of the session was
about **+0.5R**. No trade came near the 2R target.

### A correction to my own first analysis

I first reported a "true" win rate of 48.1%. **That was wrong.** I had assumed
the zero fees on 22 receipts meant fees were missing from net PnL. Bybit's P&L
documentation says `closedPnl` already subtracts the opening fee, the closing
fee *and* funding. Net PnL on those 22 trades was correct; only the fee line on
the receipt was wrong. The corrected figure is 66.7%.

---

## Defects found in the live log, and what was done

### 1. Exit ladder capped winners near 0.5R — the cause of the loss — FIXED

With a 1.5% stop (1R), the legacy ladder:

* moved the stop to breakeven at **+0.24R** — a 0.36% move
* closed any trade that gave back **22% of a +0.5R peak** — about 0.1R, **0.16% of
  price**, which is routine one-minute noise on DOGE, NEAR or SUI
* let losers run to the full −1R, and four overshot it (see 3)

Noise closed the winners; losers ran. That produces exactly this account's shape.

**Evidence the exit rule is the cause.** `research/exit_lab.py` drives the
*actual live function* `IntelligentExitEngine.evaluate` over simulated price
paths with a simulated clock. On pure noise, the legacy engine reproduces the
live account: −13.8 bps per trade (live: −12), more winners than losers, losses
~2.4× wins.

**The fix** (`ExitPolicyConfig`, now the default):

| | legacy | new |
|---|---|---|
| stop to breakeven | +0.24R | **+1.0R**, and at breakeven *plus real costs* |
| trailing starts | +0.70R (ATR cushion) | **+1.5R**, 1R behind the peak |
| 22% / 28% giveback exits | yes | **removed** — the trail does this job |
| order-flow exits allowed from | +0.25R | **+1.5R** (only on trades already locked in profit) |
| take-profit (engine and exchange) | 2R | **3R** |
| 180-min time stop | every trade | only trades that never reached +1R |

**Measured, same entries, fresh seeds, the real engine in both modes:**

| market | engine | win rate | avg win | avg loss | profit factor | per trade |
|---|---|---|---|---|---|---|
| no edge | legacy | 56.1% | 0.29% | −0.69% | 0.54 | −13.8 bps |
| no edge | new | 39.6% | 0.73% | −0.73% | 0.66 | −15.2 bps |
| weak trend | legacy | 57.7% | 0.33% | −0.73% | 0.62 | −11.7 bps |
| weak trend | new | 42.7% | 0.93% | −0.76% | 0.92 | −3.5 bps |
| moderate trend | legacy | 61.0% | 0.39% | −0.78% | 0.78 | **−6.7 bps** |
| moderate trend | new | 47.9% | 1.40% | −0.81% | **1.58** | **+24.6 bps** |
| strong trend | legacy | 69.3% | 0.54% | −0.88% | 1.38 | +10.2 bps |
| strong trend | new | 56.5% | 2.29% | −0.89% | **3.32** | **+90.5 bps** |

Paired differences (new − legacy, identical entries): no edge −1.4 bps (t=−1.0,
not significant); weak +8.2 (t=4.9); moderate +31.2 (t=13.0); strong +80.3 (t=23.0).

Read the moderate row carefully: **with a genuine edge in the market, the legacy
exits still lost money.** The new exits turn the same entries profitable — with a
*lower* win rate. Out-of-sample robustness over 12 further cells
(`exit_policy_robustness.json`): the new policy was never significantly worse,
and was significantly better wherever the edge lasted an hour or more.

**What this does not do.** No exit rule creates edge. On a market with no
drift, every stopping rule has the same expectancy and costs make it negative —
the "no edge" rows confirm it. The new exits stop *throwing away* edge; they
cannot manufacture it.

### 2. The entry model forecasts 60 seconds; trades are held for hours — NOT FIXABLE WITHOUT DATA, NOW VISIBLE

The RLS models are trained on "did price rise over the next 60 seconds"
(`MICRO_HORIZON_SEC=60`). Trades are then held 30–180+ minutes against a 1.5% stop.

At live volatility (ATR ≈ 0.3% on 5-minute bars), a 60-second forecast at
p = 0.60 is worth about **1.7 bps**. A round trip costs about **15 bps**.

The old "Alpha Tensor" formula multiplied the same probability by the full 3%
target — about **30–50× too large** — and the ticket did not even show that: the
value was hard-coded to `0.0`.

**Done:** tickets now show the honest 60-second edge against the cost, and say
"DOES NOT clear costs" when it doesn't. **Expect that on most tickets.** It is the
single most important structural fact about this strategy, and hiding it was
worse than showing it.

**The real fix** is retraining on a horizon that matches the hold (e.g. "did the
trade reach +1R before −1R"). That needs real market data and cannot be done
honestly in this environment.

### 3. Stops overshooting — PARTLY ADDRESSED

After allowing for fees and 5 bps of slippage, two losses went 31–40% past the
stop (NEAR −2.33%, SUI −2.17% against 1.5%); two more went 6–11% past.
Exchange-native stops are placed, so the likely causes are stop-market slippage
on thin alts and stop updates lagging. The branch also fixes **B2** — a filled
exit acknowledgement marking a position closed while the exchange still held
size — which the running code still has. Whether B2 caused these particular
overshoots cannot be proven from the log.

### 4. Fee accounting — FIXED

* The closed-PnL path read `execFee`, **a field that does not exist** in that
  response (it has `openFee` and `closeFee`). 22/27 receipts showed zero fees.
* The fallback path charged **only the closing leg**. All 5 trades settled that
  way recorded exactly one taker fee; one LINK "win" was a loss.

### 5. Misleading ticket fields — FIXED

"Sizing Risk: 25.00%" was *intended* notional ÷ equity, shown even for a $1.25
partial fill (1.6% of equity). Tickets now show filled exposure and the loss if
the stop is hit.

### 6. No check that edge covers costs — ADDED (shadow mode)

`core/edge_gate.py` groups entries by conviction and tracks each group's real
net return from this account's settled trades, starting from a pessimistic prior
(−15 bps: costs paid, no edge). In `shadow` mode (default) it blocks nothing and
logs what it *would* block. Switch to `EDGE_GATE_MODE=enforce` once 100 trades
have settled and the shadow log looks right. If no group is profitable, enforce
mode stops trading — that is the gate working.

---

## The code running live right now has none of this

The live bot runs `main` at `1c8ac82`. Every fix in this report — and every fix
from the whole audit, including B1 and B2 — is on `phase2/apex-overhaul`, which
has not been pushed. Until it is deployed, the live account keeps the capped-winner
exits, the wrong fee figures and the known capital-risk bugs.

## Recommended sequence

1. **Switch live to `TRADING_MODE=PAPER` now.** It is losing ~12 bps per trade on
   unfixed code.
2. Deploy `phase2/apex-overhaul` (bundle + push instructions provided).
3. Run PAPER with `EDGE_GATE_MODE=shadow` for at least 100 settled trades.
4. Check against the old profile: win rate should *fall* (roughly 40–55%),
   average win should rise well above average loss, and profit factor is the
   number that matters. **A lower win rate is the intended result.**
5. Only if profit factor > 1.2 over those 100+ trades: TESTNET, then small LIVE.

## What cannot be promised

Profit cannot be guaranteed and losing trades cannot be eliminated — by this
system or any other. A system that "never loses" does it by never closing
losers, which is how accounts are wiped out. What these changes do is make each
loss bounded at 1R and let each winner be worth several R, so the account can
survive being wrong often and still profit when the signal is right.

Whether the signal is right often enough on real markets is still unmeasured.
This environment has never been able to reach Bybit's market data. PAPER on the
fixed branch is the fastest honest way to find out.
