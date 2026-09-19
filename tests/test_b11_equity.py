"""B11 — unrealised PnL was counted twice.

`get_wallet_balance_usdt()` prefers Bybit `totalEquity`, which ALREADY includes
unrealised PnL. The lifecycle daemon then computed
`live_equity = vault_bal + unrealized_pnl` and derived drawdown from it — and
that drawdown drives PortfolioCommander's EMERGENCY MARKET EXIT at 15%.
"""
import pathlib
import pytest

from equity import (
    EquitySnapshot, parse_wallet_response, estimate_live_equity, compute_drawdown,
)

REPO = pathlib.Path(__file__).resolve().parents[1]
MAIN = (REPO / "src" / "main.py").read_text()


def wallet(equity, wallet_balance):
    return {"retCode": 0, "result": {"list": [{
        "totalEquity": str(equity),
        "totalWalletBalance": str(wallet_balance),
        "totalMarginBalance": str(equity),
    }]}}


# --- parsing ---------------------------------------------------------------

def test_parses_both_figures_separately():
    snap = parse_wallet_response(wallet(1100.0, 1000.0), now=0.0)
    assert snap.equity == 1100.0
    assert snap.wallet_balance == 1000.0
    assert snap.unrealised == pytest.approx(100.0)


def test_unparseable_payload_returns_none_not_a_guess():
    assert parse_wallet_response({"retCode": 10001}, 0.0) is None
    assert parse_wallet_response({}, 0.0) is None
    assert parse_wallet_response({"retCode": 0, "result": {"list": []}}, 0.0) is None


def test_missing_wallet_balance_falls_back_without_double_counting():
    payload = {"retCode": 0, "result": {"list": [{"totalEquity": "1000"}]}}
    snap = parse_wallet_response(payload, 0.0)
    assert snap.wallet_balance == snap.equity
    assert snap.unrealised == 0.0, (
        "with no realised figure, unrealised must read as zero rather than be invented"
    )


def test_coin_level_fallback():
    payload = {"retCode": 0, "result": {"list": [{
        "coin": [{"coin": "USDT", "equity": "500", "walletBalance": "480"}]
    }]}}
    snap = parse_wallet_response(payload, 0.0)
    assert snap.equity == 500.0 and snap.wallet_balance == 480.0


# --- the double-count itself ----------------------------------------------

def test_live_equity_builds_from_wallet_balance_not_equity():
    """The headline B11 case."""
    snap = EquitySnapshot(wallet_balance=1000.0, equity=1100.0, as_of=0.0)
    live = estimate_live_equity(snap, {"BTCUSDT": 100.0})
    assert live == pytest.approx(1100.0), (
        f"B11: expected 1100 (wallet 1000 + unrealised 100), got {live}. "
        f"Starting from equity would have produced 1200 -- unrealised counted twice."
    )


def test_double_counting_would_have_inflated_equity():
    """Demonstrates the magnitude of the original error."""
    snap = EquitySnapshot(wallet_balance=1000.0, equity=1100.0, as_of=0.0)
    correct = estimate_live_equity(snap, {"BTCUSDT": 100.0})
    old_buggy = snap.equity + 100.0
    assert old_buggy - correct == pytest.approx(100.0)


def test_live_equity_sums_all_positions():
    snap = EquitySnapshot(wallet_balance=1000.0, equity=1000.0, as_of=0.0)
    live = estimate_live_equity(snap, {"A": 50.0, "B": -20.0, "C": 10.0})
    assert live == pytest.approx(1040.0)


def test_live_equity_handles_losses():
    snap = EquitySnapshot(wallet_balance=1000.0, equity=950.0, as_of=0.0)
    assert estimate_live_equity(snap, {"BTCUSDT": -50.0}) == pytest.approx(950.0)


def test_no_snapshot_returns_none_not_zero():
    assert estimate_live_equity(None, {"BTCUSDT": 10.0}) is None


def test_nonfinite_snapshot_returns_none():
    bad = EquitySnapshot(wallet_balance=float("nan"), equity=float("nan"), as_of=0.0)
    assert estimate_live_equity(bad, {}) is None


def test_wiped_account_reports_zero_not_unknown():
    """
    Found by property testing: equity of exactly 0.0 is a WIPED account, not a
    missing reading. Reporting it as unknown made callers fall back to a stale
    balance, so the drawdown breaker never saw the loss.
    """
    wiped = EquitySnapshot(wallet_balance=1.0, equity=0.0, as_of=0.0)
    assert estimate_live_equity(wiped, {"SYM": -1.0}) == pytest.approx(0.0)
    assert compute_drawdown(1000.0, 0.0) == pytest.approx(1.0)


def test_negative_equity_is_reported_not_hidden():
    """Pushed further by property testing: a blown account is real data."""
    blown = EquitySnapshot(wallet_balance=10.0, equity=-90.0, as_of=0.0)
    assert estimate_live_equity(blown, {"SYM": -100.0}) == pytest.approx(-90.0)


# --- drawdown --------------------------------------------------------------

@pytest.mark.parametrize("peak,current,expected", [
    (1000.0, 900.0, 0.10),
    (1000.0, 1000.0, 0.0),
    (1000.0, 1200.0, 0.0),     # above peak is not a drawdown
    (0.0, 500.0, 0.0),         # no peak yet
])
def test_drawdown_maths(peak, current, expected):
    assert compute_drawdown(peak, current) == pytest.approx(expected)


def test_drawdown_uses_equity_basis_in_main():
    assert "compute_drawdown(baseline_bal, live_equity)" in MAIN
    assert "live_equity = vault_bal + unrealized_pnl" not in MAIN, (
        "B11: the double-counting expression is still present"
    )


def test_main_tracks_unrealised_per_symbol():
    assert "_unrealised_by_symbol" in MAIN
    assert "_refresh_equity_snapshot" in MAIN


def test_heartbeat_uses_paired_snapshot():
    assert "snap = await self._refresh_equity_snapshot()" in MAIN
