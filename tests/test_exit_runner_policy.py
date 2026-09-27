"""
Runner exit policy — the fix for the live session's payoff asymmetry.

Live, Sept 2026: 27 trades, 66.7% winners, average loss 3.60x the average win,
break-even win rate 78.3%, profit factor 0.56. The largest win was ~0.5R and no
trade came near the 2R target, because the legacy ladder locked breakeven at
~+0.24R and closed any trade that gave back 22% of a +0.5R peak (~0.1R, one
minute of ordinary noise on the alts traded).

Each test drives IntelligentExitEngine.evaluate directly -- the function the
live loop calls -- with a scripted price path.
"""
import pytest

import core.intelligent_exit as ie
from core.intelligent_exit import (
    IntelligentExitEngine, PositionExitState, EXIT_CONFIG, LEGACY_EXIT_CONFIG,
)

ENTRY = 100.0
RISK = 1.5          # 1R = 1.5% of price: the live stop floor


class Clock:
    def __init__(self, t=1_700_000_000.0):
        self.t = t

    def time(self):
        return self.t

    def __getattr__(self, n):
        import time as _t
        return getattr(_t, n)


def make(is_buy=True, config=None, **ctx_extra):
    clock = Clock()
    state = PositionExitState(position_id="t", entry_time=clock.t, entry_price=ENTRY,
                              exit_side="Sell" if is_buy else "Buy", entry_balance=1000,
                              actual_qty=1.0, base_qty=1.0, last_eval_time=clock.t - 10)
    ctx = {"is_buy": is_buy, "symbol": "T", "atr": 0.3, "initial_risk_dist": RISK,
           "taker_fee_rate": 0.00055, "slippage_buffer_pct": 0.0004,
           "baseline_vol_pct": 0.005, "dynamic_rr_ratio": 2.0,
           "max_drawdown_pct": 0.15, "drawdown_pct": 0.0, **ctx_extra}
    if config is not None:
        ctx["exit_config"] = config
    return clock, state, ctx


def tick(clock, state, ctx, price, dt=10.0):
    clock.t += dt
    ctx["last_ob"] = {"best_bid": price, "best_ask": price}
    ctx["latest_tick_price"] = price
    ctx["mark_price"] = price
    saved, ie.time = ie.time, clock
    try:
        return IntelligentExitEngine.evaluate(ctx, state)
    finally:
        ie.time = saved


def r(x, is_buy=True):
    """Price at x R from entry."""
    return ENTRY + x * RISK if is_buy else ENTRY - x * RISK


def walk(clock, state, ctx, path_r, is_buy=True):
    last = None
    for x in path_r:
        last = tick(clock, state, ctx, r(x, is_buy))
        if last.action == "EXIT":
            return last
    return last


# ================== the defect, and the fix ===============================

def test_the_legacy_ladder_closes_a_winner_on_a_noise_sized_pullback():
    """
    Pins the live defect so it stays documented: +0.6R, then a 0.14R dip
    (0.2% of price -- routine noise) closes the trade under the legacy ladder.
    """
    clock, state, ctx = make(config=LEGACY_EXIT_CONFIG)
    d = walk(clock, state, ctx, [0.2, 0.4, 0.6, 0.46])
    assert d.action == "EXIT"
    assert "PREDATOR_REVERSAL_STRIKE" in d.reason


def test_the_runner_policy_rides_through_the_same_pullback():
    clock, state, ctx = make()
    d = walk(clock, state, ctx, [0.2, 0.4, 0.6, 0.46])
    assert d.action == "HOLD", d.reason


def test_breakeven_is_not_locked_before_one_r():
    """Legacy locked breakeven at ~+0.24R; a 0.36% move against a 1.5% stop."""
    clock, state, ctx = make()
    d = walk(clock, state, ctx, [0.3, 0.6, 0.9])
    assert d.action == "HOLD"
    assert state.profit_state.locked_sl == pytest.approx(r(-1.0))


def test_at_one_r_the_stop_moves_to_breakeven_plus_real_costs():
    clock, state, ctx = make()
    walk(clock, state, ctx, [0.5, 1.0])
    cost = ENTRY * (2 * 0.00055 + 0.0004)
    assert state.profit_state.locked_sl == pytest.approx(ENTRY + cost)
    assert state.profit_state.locked_sl > ENTRY, "a 'breakeven' that loses the fees is not breakeven"


def test_from_one_and_a_half_r_the_stop_trails_one_r_behind_the_peak():
    clock, state, ctx = make()
    walk(clock, state, ctx, [0.5, 1.0, 1.5, 2.2])
    assert state.profit_state.locked_sl == pytest.approx(r(1.2))


def test_a_trailed_runner_exits_in_profit_on_reversal():
    clock, state, ctx = make()
    d = walk(clock, state, ctx, [0.5, 1.0, 1.5, 2.2, 1.9, 1.5, 1.1])
    assert d.action == "EXIT"
    assert "RUNNER_TRAIL_STOP" in d.reason


def test_take_profit_is_at_least_three_r_even_when_dynamic_rr_says_two():
    clock, state, ctx = make(dynamic_rr_ratio=2.0)
    d = walk(clock, state, ctx, [0.5, 1.0, 1.5, 2.1])
    assert d.action == "HOLD", "2R must no longer close the trade"
    assert d.dynamic_tp_price == pytest.approx(r(3.0))
    clock, state, ctx = make(dynamic_rr_ratio=2.0)
    d = walk(clock, state, ctx, [0.5, 1.0, 1.5, 2.0, 2.5, 3.0])
    assert d.action == "EXIT" and "DYNAMIC_TP_REACHED" in d.reason


def test_a_loser_is_stopped_at_minus_one_r():
    clock, state, ctx = make()
    d = walk(clock, state, ctx, [-0.3, -0.7, -1.0])
    assert d.action == "EXIT" and "CAMB_STOP_BREACH" in d.reason


def test_the_stop_never_loosens():
    clock, state, ctx = make()
    walk(clock, state, ctx, [1.0, 1.6, 2.4])
    peak_stop = state.profit_state.locked_sl
    tick(clock, state, ctx, r(2.0))
    assert state.profit_state.locked_sl >= peak_stop


def test_shorts_are_the_mirror_image():
    clock, state, ctx = make(is_buy=False)
    d = walk(clock, state, ctx, [0.2, 0.4, 0.6, 0.46], is_buy=False)
    assert d.action == "HOLD"
    walk(clock, state, ctx, [1.0, 1.5, 2.2], is_buy=False)
    assert state.profit_state.locked_sl == pytest.approx(r(1.2, is_buy=False))


# ================== time exits =============================================

def test_the_time_stop_no_longer_kills_a_working_runner():
    """Legacy closed ANY trade at 180 minutes, including a +2R runner."""
    clock, state, ctx = make()
    walk(clock, state, ctx, [0.5, 1.0, 1.6, 2.0])
    clock.t = state.entry_time + 200 * 60
    d = tick(clock, state, ctx, r(2.0), dt=0)
    assert d.action == "HOLD", d.reason


def test_the_time_stop_still_retires_a_trade_that_never_worked():
    clock, state, ctx = make()
    walk(clock, state, ctx, [0.2, 0.4, 0.1])
    clock.t = state.entry_time + 181 * 60
    d = tick(clock, state, ctx, r(0.1), dt=0)
    assert d.action == "EXIT" and "HORIZON_EXHAUSTION" in d.reason


def test_a_trade_still_losing_after_ninety_minutes_is_scratched():
    clock, state, ctx = make()
    walk(clock, state, ctx, [-0.2, -0.5])
    clock.t = state.entry_time + 91 * 60
    d = tick(clock, state, ctx, r(-0.5), dt=0)
    assert d.action == "EXIT" and "ADVERSE_STAGNATION_SCRATCH" in d.reason


# ================== flow exits can't cut noise-sized winners ================

class _Reversal:
    """A stat engine screaming 'reversal' on every tick."""
    clean_ofi_z = -3.0
    marked_hawkes_z = 3.5
    changepoint_prob = 0.9
    kinetic_tensor = type("K", (), {"accel_z": -2.0})()

    def __init__(self):
        from probability import make_estimate
        self.latest_probability = make_estimate(0.2, horizon_seconds=60)


def test_flow_signals_cannot_close_a_trade_below_the_protected_zone():
    """Legacy acted on these from +0.25R -- noise-sized gains."""
    clock, state, ctx = make(stat_engine=_Reversal())
    d = walk(clock, state, ctx, [0.3, 0.6, 1.0, 1.3])
    assert d.action == "HOLD"


def test_flow_signals_may_close_a_trade_already_locked_in_profit():
    clock, state, ctx = make(stat_engine=_Reversal())
    d = walk(clock, state, ctx, [0.5, 1.0, 1.6])
    assert d.action == "EXIT" and "FLOW_REVERSAL_AFTER_PROFIT" in d.reason


# ================== safety paths preserved =================================

def test_portfolio_drawdown_emergency_still_overrides():
    clock, state, ctx = make(drawdown_pct=0.20)
    d = tick(clock, state, ctx, r(0.5))
    assert d.action == "EMERGENCY"


def test_anomalous_prices_are_still_rejected():
    clock, state, ctx = make()
    d = tick(clock, state, ctx, r(40.0))
    assert d.action == "HOLD" and d.reason == "ANOMALOUS_R_REJECTED"


def test_the_exchange_stop_is_kept_clear_of_the_market():
    clock, state, ctx = make()
    d = walk(clock, state, ctx, [0.5, 1.0, 1.6, 2.0])
    assert d.exchange_ts_price < r(2.0) - 0.0019 * r(2.0)


def test_the_default_is_the_runner_and_legacy_is_opt_in():
    assert EXIT_CONFIG.legacy is False
    assert LEGACY_EXIT_CONFIG.legacy is True
    assert EXIT_CONFIG.be_trigger_r >= 1.0
    assert EXIT_CONFIG.min_reward_r >= 3.0


# ================== the measured claim, re-checked =========================

def test_on_pure_noise_neither_policy_manufactures_profit():
    """
    No exit rule can create edge. If this ever shows a profit on a driftless
    path, the lab -- not the strategy -- is broken.
    """
    from research.exit_lab import drifting_path, momentum_entries, run, summarise, LivePolicy
    for cfg in (LEGACY_EXIT_CONFIG, EXIT_CONFIG):
        trades = []
        for seed in range(3):
            path = drifting_path(20000, seed=900 + seed, sigma_per_min=0.0007)
            trades += run(path, LivePolicy(cfg), momentum_entries(path, sigma_per_min=0.0007))
        assert summarise(trades)["expectancy_bps"] < 0.0


def test_the_legacy_ladder_on_noise_reproduces_the_live_signature():
    """More winners than losers, and losses much larger than wins."""
    from research.exit_lab import drifting_path, momentum_entries, run, summarise, LivePolicy
    trades = []
    for seed in range(4):
        path = drifting_path(20000, seed=910 + seed, sigma_per_min=0.0007)
        trades += run(path, LivePolicy(LEGACY_EXIT_CONFIG), momentum_entries(path, sigma_per_min=0.0007))
    s = summarise(trades)
    assert s["win_rate"] > 0.50
    assert s["loss_to_win"] > 1.5


@pytest.mark.slow
def test_with_a_persistent_trend_the_runner_keeps_edge_the_legacy_ladder_discards():
    """
    The headline measurement, as a regression test: identical entries, paired
    comparison, fresh seeds. Runner must beat legacy with t > 3.
    """
    import math, statistics as st
    from research.exit_lab import drifting_path, momentum_entries, run, LivePolicy
    L, R = [], []
    for seed in range(10):
        path = drifting_path(30000, seed=7000 + seed, sigma_per_min=0.0007,
                             drift_strength=0.10, drift_halflife_min=120)
        ent = momentum_entries(path, sigma_per_min=0.0007)
        L += [t.pnl_pct for t in run(path, LivePolicy(LEGACY_EXIT_CONFIG), ent)]
        R += [t.pnl_pct for t in run(path, LivePolicy(EXIT_CONFIG), ent)]
    d = [a - b for a, b in zip(R, L)]
    t = st.mean(d) / (st.stdev(d) / math.sqrt(len(d)))
    assert st.mean(R) > 0 > st.mean(L)
    assert t > 3.0, f"paired t = {t:.2f}"
