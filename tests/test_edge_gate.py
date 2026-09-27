"""
After-cost edge gate.

Live, Sept 2026: nothing checked that an entry's edge covered its ~15 bps
round-trip cost. The ticket's "Alpha Tensor" was hard-coded to 0.0, and its
formula multiplied a 60-second direction probability by the full 3% target.
"""
import sqlite3

import pytest

from core.edge_gate import EdgeGate, EdgeGateConfig, horizon_edge_bps


def gate(mode="enforce", min_trades=10, **kw):
    return EdgeGate(EdgeGateConfig(mode=mode, min_trades_to_enforce=min_trades, **kw))


# ---- unproven is unprofitable ---------------------------------------------

def test_with_no_history_every_band_is_presumed_to_lose():
    g = gate()
    d = g.decide(0.12)
    assert d.would_block and d.expectancy_bps == pytest.approx(-15.0)


def test_shadow_mode_never_blocks_but_counts():
    g = gate(mode="shadow")
    for _ in range(5):
        assert g.decide(0.1).allowed
    assert g.would_block_count == 5 and g.blocked_count == 0


def test_off_mode_does_nothing():
    g = gate(mode="off")
    d = g.decide(0.1)
    assert d.allowed and not d.would_block and g.would_block_count == 0


def test_enforce_waits_for_enough_evidence_before_blocking():
    g = gate(min_trades=50)
    for _ in range(20):
        g.record(0.1, -0.002)
    d = g.decide(0.1)
    assert d.allowed and d.would_block and "only 20/50" in d.reason


def test_enforce_blocks_a_band_that_loses_after_costs():
    g = gate(min_trades=10)
    for _ in range(40):
        g.record(0.07, -0.0012)
    d = g.decide(0.07)
    assert not d.allowed and "BLOCKED" in d.reason
    assert g.blocked_count == 1


def test_a_band_must_overcome_the_pessimistic_prior():
    """A handful of lucky wins is not evidence."""
    g = gate(min_trades=1)
    for _ in range(3):
        g.record(0.12, +0.004)       # +40 bps each, only three trades
    assert g.decide(0.12).would_block, "3 wins must not outweigh a 30-trade prior"
    for _ in range(60):
        g.record(0.12, +0.002)
    assert g.decide(0.12).allowed and not g.decide(0.12).would_block


def test_bands_are_judged_independently():
    g = gate(min_trades=10)
    for _ in range(80):
        g.record(0.03, -0.002)       # weak conviction loses
        g.record(0.18, +0.003)       # strong conviction pays
    assert not g.decide(0.03).allowed
    assert g.decide(0.18).allowed


def test_unknown_outcomes_are_refused_not_guessed():
    g = gate()
    assert not g.record(None, 0.01)
    assert not g.record(0.1, None)
    assert not g.record(0.1, float("nan"))
    assert not g.record(float("inf"), 0.01)
    assert g.n_total == 0


def test_conviction_is_clamped_into_a_band():
    g = gate()
    assert g.band_index(-1.0) == 0
    assert g.band_index(0.9) == len(g.cfg.band_edges) - 2


def test_reset_clears_history():
    g = gate()
    g.record(0.1, 0.01)
    g.reset()
    assert g.n_total == 0


def test_mode_comes_from_the_environment_and_bad_values_fail_safe(monkeypatch):
    monkeypatch.setenv("EDGE_GATE_MODE", "enforce")
    monkeypatch.setenv("EDGE_GATE_MIN_TRADES", "250")
    c = EdgeGateConfig.from_env()
    assert c.mode == "enforce" and c.min_trades_to_enforce == 250
    monkeypatch.setenv("EDGE_GATE_MODE", "yolo")
    assert EdgeGateConfig.from_env().mode == "shadow"


def test_snapshot_is_reportable():
    g = gate()
    g.record(0.1, -0.001)
    s = g.snapshot()
    assert s["settled_trades"] == 1 and s["mode"] == "enforce"
    assert all("expectancy_bps" in b for b in s["bands"].values())


# ---- rebuilding from the ledger --------------------------------------------

def _ledger():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("""CREATE TABLE quantitative_ledger (signal_id TEXT, predicted_probability REAL,
                 net_pnl REAL, target_notional REAL, resolved BOOLEAN, is_shadow BOOLEAN,
                 settlement_status TEXT)""")
    rows = [("a", 0.62, 0.05, 20.0, 1, 0, "RESOLVED"),
            ("b", 0.58, -0.10, 20.0, 1, 0, "RESOLVED"),
            ("shadow", 0.60, 5.0, 20.0, 1, 1, "RESOLVED"),
            ("open", 0.60, 0.0, 20.0, 0, 0, "PENDING"),
            ("unknown", 0.60, 0.0, 20.0, 1, 0, "UNKNOWN"),
            ("noprob", None, 0.05, 20.0, 1, 0, "RESOLVED")]
    c.executemany("INSERT INTO quantitative_ledger VALUES (?,?,?,?,?,?,?)", rows)
    return list(c.execute("SELECT * FROM quantitative_ledger"))


def test_ledger_rebuild_uses_only_real_settled_trades_with_a_probability():
    g = gate()
    assert g.load_from_ledger_rows(_ledger()) == 2


# ---- what a 60-second forecast is actually worth ---------------------------

def test_a_coin_flip_is_worth_nothing():
    assert horizon_edge_bps(0.5, True, 0.003) == pytest.approx(0.0)


def test_buy_and_sell_are_mirror_images():
    assert horizon_edge_bps(0.6, True, 0.003) == pytest.approx(horizon_edge_bps(0.4, False, 0.003))
    assert horizon_edge_bps(0.6, False, 0.003) < 0


def test_a_60_second_edge_does_not_cover_a_round_trip_at_live_volatility():
    """
    The number behind the finding. ATR ~0.3% on 5-minute bars, p = 0.65 -- a
    confident forecast -- is worth a few bps. A round trip costs ~15.
    """
    edge = horizon_edge_bps(0.65, True, 0.003, horizon_sec=60, bar_minutes=5)
    assert 0 < edge < 5.0
    assert edge < (2 * 0.00055 + 0.0004) * 1e4


def test_the_old_alpha_tensor_formula_overstated_edge_by_an_order_of_magnitude():
    p, tp_dist = 0.60, 0.03
    old_alpha = (2 * p - 1) * tp_dist * 1e4        # micro_models' formula
    honest = horizon_edge_bps(p, True, 0.003)
    assert old_alpha / honest > 20


def test_longer_horizons_are_worth_more():
    assert horizon_edge_bps(0.6, True, 0.003, horizon_sec=3600) > horizon_edge_bps(0.6, True, 0.003, horizon_sec=60)
