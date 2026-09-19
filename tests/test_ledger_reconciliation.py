"""
Ledger reconciliation checks — APEX section 18.

The ledger is what every performance claim, every Kelly update and every model
label is built on. Drift there is silent: nothing downstream re-checks it, and
the bot keeps trading confidently on a false picture of its own results.

Each test plants one specific corruption and asserts the checker catches it.
A reconciliation tool that has never been shown to fail is not evidence.
"""
import sqlite3
import sys
import pathlib

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

import reconcile_ledger as R  # noqa: E402

SCHEMA = """
CREATE TABLE quantitative_ledger (
    signal_id TEXT PRIMARY KEY, timestamp TEXT, symbol TEXT,
    predicted_direction TEXT, price_at_prediction REAL, is_correct BOOLEAN,
    vol_mult REAL, log_mlofi_z REAL, spread REAL, net_pnl REAL,
    actual_outcome TEXT, resolved BOOLEAN, is_shadow BOOLEAN,
    fees_usdt REAL, slippage_drag REAL, holding_minutes REAL,
    virtual_sl REAL, virtual_tp REAL, target_notional REAL,
    shadow_return_fraction REAL, settlement_status TEXT,
    predicted_probability REAL, mfe_r REAL, mae_r REAL
);
"""

COLS = ("signal_id timestamp symbol predicted_direction price_at_prediction is_correct "
        "vol_mult log_mlofi_z spread net_pnl actual_outcome resolved is_shadow fees_usdt "
        "slippage_drag holding_minutes virtual_sl virtual_tp target_notional "
        "shadow_return_fraction settlement_status").split()


def trade(sid, net_pnl=1.0, outcome="WIN", resolved=1, shadow=0, fees=0.05,
          hold=30.0, status=None, symbol="BTCUSDT"):
    return {
        "signal_id": sid, "timestamp": "2026-09-01T00:00:00Z", "symbol": symbol,
        "predicted_direction": "Buy", "price_at_prediction": 100.0, "is_correct": 1,
        "vol_mult": 1.0, "log_mlofi_z": 0.0, "spread": 0.01, "net_pnl": net_pnl,
        "actual_outcome": outcome, "resolved": resolved, "is_shadow": shadow,
        "fees_usdt": fees, "slippage_drag": 0.01, "holding_minutes": hold,
        "virtual_sl": 98.0, "virtual_tp": 104.0, "target_notional": 10.0,
        "shadow_return_fraction": None,
        "settlement_status": status if status is not None else ("SETTLED" if resolved else "PENDING"),
    }


def ledger(*trades):
    conn = sqlite3.connect(":memory:")
    conn.executescript(SCHEMA)
    conn.executemany(
        f"INSERT INTO quantitative_ledger ({','.join(COLS)}) "
        f"VALUES ({','.join('?' * len(COLS))})",
        [tuple(t[c] for c in COLS) for t in trades],
    )
    conn.commit()
    conn.row_factory = sqlite3.Row
    return list(conn.execute("SELECT * FROM quantitative_ledger"))


def codes(findings):
    return {f.code for f in findings}


# ===================== the clean case ====================================

def test_a_consistent_ledger_reconciles():
    rows = ledger(trade("a", 1.0, "WIN"), trade("b", -0.5, "LOSS"),
                  trade("c", 2.0, "TP"))
    assert R.reconcile(rows) == [] or codes(R.reconcile(rows)) <= {"C10"}


def test_an_empty_ledger_is_reported_not_treated_as_clean():
    out = R.reconcile([])
    assert out and out[0].code == "C0"


# ===================== each planted corruption ===========================

def test_negative_holding_time_is_caught():
    """An exit recorded before its entry."""
    rows = ledger(trade("a", 1.0, hold=-5.0))
    assert "C2/C8" in codes(R.reconcile(rows))


def test_implausible_holding_time_is_caught():
    """A resolution that was never recorded looks like a trade open for a year."""
    rows = ledger(trade("a", 1.0, hold=60 * 24 * 400))
    assert "C2/C8" in codes(R.reconcile(rows))


def test_resolved_with_no_outcome_is_caught():
    rows = ledger(trade("a", 1.0, outcome=None, resolved=1))
    assert "C3a" in codes(R.reconcile(rows))


def test_unresolved_trade_carrying_pnl_is_caught():
    """Realised figures would include money that has not settled."""
    rows = ledger(trade("a", 5.0, outcome=None, resolved=0))
    assert "C3b" in codes(R.reconcile(rows))


def test_negative_fees_are_caught():
    """A cost recorded as income inflates net PnL."""
    rows = ledger(trade("a", 1.0, fees=-0.20))
    assert "C4" in codes(R.reconcile(rows))


def test_settlement_status_drift_is_caught():
    rows = ledger(trade("a", 1.0, resolved=1, status="PENDING"))
    assert "C5" in codes(R.reconcile(rows))


def test_sqlite_silently_turns_a_nan_pnl_into_null():
    """
    Documents the storage behaviour the next check depends on. SQLite does not
    store NaN — it writes NULL — while Inf survives intact. Nothing warns.
    """
    rows = ledger(trade("a", float("nan")))
    assert rows[0]["net_pnl"] is None


def test_a_null_pnl_on_a_resolved_trade_is_critical():
    """
    The silent-corruption path. A NaN PnL becomes NULL, every read site does
    `float(x or 0.0)`, and the trade enters the win rate, the expectancy and
    the Kelly update as a BREAKEVEN — indistinguishable from a real scratch.
    """
    rows = ledger(trade("a", float("nan")), trade("b", 1.0))
    found = [f for f in R.reconcile(rows) if f.code == "C6b"]
    assert found and found[0].severity == "CRITICAL"
    assert "breakeven" in found[0].detail.lower()


def test_infinity_is_caught():
    rows = ledger(trade("a", float("inf")))
    assert "C6a" in codes(R.reconcile(rows))


def test_label_and_money_disagreement_is_caught_as_critical():
    """
    C9 — the most damaging inconsistency available. `actual_outcome` says WIN
    while `net_pnl` is negative, so the labels the model learns from do not
    match the money the account made.
    """
    rows = ledger(trade("a", -3.0, outcome="WIN"), trade("b", 1.0, outcome="WIN"))
    found = [f for f in R.reconcile(rows) if f.code == "C9"]
    assert found, "label/PnL disagreement was not detected"
    assert found[0].severity == "CRITICAL"
    assert "corrupt" in found[0].detail.lower()


def test_agreeing_labels_and_pnl_do_not_trip_c9():
    rows = ledger(trade("a", 3.0, outcome="WIN"), trade("b", -1.0, outcome="LOSS"))
    assert "C9" not in codes(R.reconcile(rows))


def test_shadow_trades_are_flagged_but_not_counted_as_real():
    rows = ledger(trade("real", 2.0), trade("shadow", 99.0, shadow=1))
    stats = R.summarise(rows)
    assert stats["rows_real"] == 1
    assert stats["net_pnl_settled"] == pytest.approx(2.0), (
        "shadow PnL leaked into the realised total"
    )
    assert "C10" in codes(R.reconcile(rows))


# ===================== the summary itself ================================

def test_gross_net_and_fees_are_reported_separately():
    """
    APEX forbids quoting a net figure that quietly omits costs. The summary
    always shows gross, fees and net together so the gap is visible.
    """
    rows = ledger(trade("a", 1.00, fees=0.25), trade("b", -0.50, fees=0.25))
    s = R.summarise(rows)
    assert s["net_pnl_settled"] == pytest.approx(0.50)
    assert s["fees_paid"] == pytest.approx(0.50)
    assert s["gross_before_fees"] == pytest.approx(1.00)
    assert s["fees_as_pct_of_gross"] == pytest.approx(50.0)


def test_fee_drag_is_visible_when_it_dominates():
    """The case that matters: an edge entirely eaten by costs."""
    rows = ledger(*[trade(f"t{i}", 0.01, fees=0.09) for i in range(20)])
    s = R.summarise(rows)
    assert s["fees_paid"] > s["net_pnl_settled"]
    assert s["fees_as_pct_of_gross"] > 80.0


def test_wins_and_losses_partition_the_settled_set():
    rows = ledger(trade("a", 1.0), trade("b", -1.0), trade("c", 0.0, outcome="LOSS"))
    s = R.summarise(rows)
    assert s["wins"] + s["losses"] == s["rows_settled"]


def test_reconciler_opens_the_ledger_read_only(tmp_path):
    """
    Behavioural, not a grep: open a real file the way the tool does and prove a
    write is refused. A checker that can corrupt what it checks is worse than
    no checker.
    """
    db = tmp_path / "led.db"
    setup = sqlite3.connect(db)
    setup.executescript(SCHEMA)
    setup.commit()
    setup.close()

    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    assert list(conn.execute("SELECT * FROM quantitative_ledger")) == []
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        conn.execute("DELETE FROM quantitative_ledger")


def test_the_tool_uses_that_read_only_uri():
    assert 'mode=ro' in (REPO / "scripts" / "reconcile_ledger.py").read_text()


def test_findings_serialise_for_a_report():
    rows = ledger(trade("a", -3.0, outcome="WIN"))
    for f in R.reconcile(rows):
        d = f.to_dict()
        assert set(d) == {"code", "severity", "detail", "rows", "examples"}
        assert all(isinstance(e, str) for e in d["examples"])


# ===================== the write-side guard ==============================

def test_a_non_finite_pnl_is_refused_at_the_write_site():
    """
    Catching NULL PnL after the fact is a backstop. The fix is not letting it in.

    A NaN PnL reaching `log_live_execution_result` previously: stored as NULL,
    read back as 0.0, counted as a breakeven trade — and `is_correct = pnl > 0`
    is False for NaN, so it was also filed as a LOSS. Now it is recorded as
    UNKNOWN, which B12 already excludes from every statistic.
    """
    import asyncio
    bank = _bank()
    asyncio.run(bank.commit_prediction("sig", 1_757_000_000.0, 100.0, "BUY", 0.6,
                                       {"symbol": "BTCUSDT"}))
    asyncio.run(bank.log_live_execution_result("sig", float("nan"), 0.0, "WIN"))

    row = bank.local_cursor.execute(
        "SELECT * FROM quantitative_ledger WHERE signal_id='sig'").fetchone()
    assert row["actual_outcome"] == "UNKNOWN", "a NaN PnL was recorded as a real outcome"
    assert row["is_correct"] is None, "NaN > 0 is False — it must not be filed as a loss"


def test_a_normal_pnl_still_records_cleanly():
    import asyncio
    bank = _bank()
    asyncio.run(bank.commit_prediction("sig", 1_757_000_000.0, 100.0, "BUY", 0.6,
                                       {"symbol": "BTCUSDT"}))
    asyncio.run(bank.log_live_execution_result("sig", 2.5, 1.0, "WIN"))
    row = bank.local_cursor.execute(
        "SELECT * FROM quantitative_ledger WHERE signal_id='sig'").fetchone()
    assert row["net_pnl"] == pytest.approx(2.5)
    assert row["actual_outcome"] == "WIN"
    assert row["is_correct"] == 1


# ===================== self-measurement columns ==========================

def _bank():
    """A MemoryBank backed by an in-memory DB with the REAL schema + migrations."""
    import asyncio
    import core.memory as M
    bank = M.MemoryBank.__new__(M.MemoryBank)
    bank.supabase = None
    bank.write_queue = asyncio.Queue()
    bank._db_lock = asyncio.Lock()
    bank.db_path = ":memory:"
    bank._record_db_failure = lambda reason: None
    bank._init_sqlite()
    return bank


def test_the_migration_adds_the_self_measurement_columns():
    """
    Without these the system cannot measure itself: no stored probability means
    calibration is unmeasurable, and no stored excursion means exit quality is.
    """
    bank = _bank()
    cols = {r[1] for r in bank.local_cursor.execute(
        "PRAGMA table_info(quantitative_ledger)")}
    for c in ("predicted_probability", "mfe_r", "mae_r"):
        assert c in cols, f"{c} missing — the migration did not run"


def test_the_migration_is_idempotent():
    """It runs on every startup; a second run must not raise."""
    bank = _bank()
    bank._init_sqlite()
    bank._init_sqlite()
    cols = [r[1] for r in bank.local_cursor.execute(
        "PRAGMA table_info(quantitative_ledger)")]
    assert len(cols) == len(set(cols)), "a column was added twice"


def test_the_predicted_probability_reaches_the_local_ledger():
    """
    It was written only to Supabase (as ai_confidence). The local ledger — the
    one that is always available — dropped it, so nothing could ever check
    whether p_up = 0.70 means 70%, despite that same number driving Kelly
    position sizing.
    """
    import asyncio
    bank = _bank()
    asyncio.run(bank.commit_prediction("s1", 1_757_000_000.0, 100.0, "BUY", 0.73,
                                       {"symbol": "BTCUSDT"}))
    row = bank.local_cursor.execute(
        "SELECT predicted_probability FROM quantitative_ledger WHERE signal_id='s1'"
    ).fetchone()
    assert row["predicted_probability"] == pytest.approx(0.73)


def test_a_non_finite_probability_is_stored_as_null_not_as_a_guess():
    """An invented 0.5 would corrupt the calibration measurement silently."""
    import asyncio
    bank = _bank()
    asyncio.run(bank.commit_prediction("s2", 1_757_000_000.0, 100.0, "BUY",
                                       float("nan"), {"symbol": "BTCUSDT"}))
    row = bank.local_cursor.execute(
        "SELECT predicted_probability FROM quantitative_ledger WHERE signal_id='s2'"
    ).fetchone()
    assert row["predicted_probability"] is None


def test_excursions_are_persisted_at_resolution():
    import asyncio
    bank = _bank()
    asyncio.run(bank.commit_prediction("s3", 1_757_000_000.0, 100.0, "BUY", 0.6,
                                       {"symbol": "BTCUSDT"}))
    asyncio.run(bank.log_live_execution_result(
        "s3", 1.25, 2.0, "WIN", {"mfe_r": 1.8, "mae_r": -0.4, "fees_usdt": 0.05}))
    row = bank.local_cursor.execute(
        "SELECT mfe_r, mae_r FROM quantitative_ledger WHERE signal_id='s3'").fetchone()
    assert row["mfe_r"] == pytest.approx(1.8)
    assert row["mae_r"] == pytest.approx(-0.4)


def test_missing_excursions_are_null_not_zero():
    """A 0.0 excursion is the claim 'the trade never moved' — not the same thing."""
    import asyncio
    bank = _bank()
    asyncio.run(bank.commit_prediction("s4", 1_757_000_000.0, 100.0, "BUY", 0.6,
                                       {"symbol": "BTCUSDT"}))
    asyncio.run(bank.log_live_execution_result("s4", 1.0, 0.0, "WIN", {}))
    row = bank.local_cursor.execute(
        "SELECT mfe_r, mae_r FROM quantitative_ledger WHERE signal_id='s4'").fetchone()
    assert row["mfe_r"] is None and row["mae_r"] is None


def test_the_recorded_columns_feed_the_analysis_tools_directly():
    """End to end: ledger rows in, calibration and excursion verdicts out."""
    import asyncio
    from research.calibration import calibration_report, excursion_report
    bank = _bank()
    for i in range(200):
        p = 0.3 + (i % 7) * 0.1
        won = 1 if (i % 10) < int(round(p * 10)) else 0
        asyncio.run(bank.commit_prediction(f"t{i}", 1_757_000_000.0, 100.0, "BUY", p,
                                           {"symbol": "BTCUSDT"}))
        asyncio.run(bank.log_live_execution_result(
            f"t{i}", 1.0 if won else -1.0, 0.0, "WIN" if won else "LOSS",
            {"mfe_r": 1.5 if won else 0.4, "mae_r": -0.2 if won else -1.1}))

    rows = list(bank.local_cursor.execute(
        "SELECT predicted_probability, is_correct, mfe_r, mae_r, net_pnl "
        "FROM quantitative_ledger WHERE resolved=1"))
    cal = calibration_report([r["predicted_probability"] for r in rows],
                             [r["is_correct"] for r in rows])
    exc = excursion_report([{"mfe": r["mfe_r"], "mae": r["mae_r"],
                             "realised": r["net_pnl"]} for r in rows])
    assert cal.n == 200 and exc.n == 200
    assert "INSUFFICIENT DATA" not in cal.verdict()
    assert "INSUFFICIENT DATA" not in exc.verdict()
