"""
Fee accounting defects found in the live Telegram log, Sept 2026.

  * /v5/position/closed-pnl has no `execFee` field -- it has openFee and
    closeFee. Reading execFee returned 0.0: 22 of 27 receipts showed zero fees.
    closedPnl itself is net of both fees and funding (Bybit P&L docs), so net
    PnL was right; the fee figure and anything built on it were not.
  * The execution-list fallback charged only the CLOSING fee. All 5 trades
    settled that way recorded exactly one taker leg.
"""
import asyncio
import pathlib

import pytest

import main as M

REPO = pathlib.Path(__file__).resolve().parents[1]
MAIN = (REPO / "src" / "main.py").read_text()


def test_closed_pnl_fees_come_from_open_and_close_fee():
    assert 'valid_close.get("openFee")' in MAIN and 'valid_close.get("closeFee")' in MAIN
    assert 'valid_close.get("execFee"' not in MAIN, "closed-pnl has no execFee field"


@pytest.mark.parametrize("v,exp", [("0.0103", 0.0103), (None, 0.0), ("", 0.0),
                                   ("nan", 0.0), ("inf", 0.0), ("abc", 0.0), (0.5, 0.5)])
def test_exchange_numeric_parsing(v, exp):
    assert M._finite_float(v) == pytest.approx(exp)


class _Exec:
    def __init__(self, rows):
        self.rows = rows

    async def safe_call(self, method, endpoint, **kw):
        return {"retCode": 0, "result": {"list": self.rows}}


def test_fallback_settlement_charges_both_fee_legs():
    """The exact shape of the live LINK trade: $18.68 notional, 0.0103 per leg."""
    opened_ms = 1_700_000_000_000.0
    rows = [
        {"execTime": str(opened_ms - 1500), "side": "Buy", "execQty": "1.5",
         "execPrice": "12.452", "execFee": "0.0103"},                     # entry fill
        {"execTime": str(opened_ms + 600_000), "side": "Sell", "execQty": "1.5",
         "execPrice": "12.465", "execFee": "0.0103"},                     # exit fill
    ]
    fake = type("E", (), {"executor": _Exec(rows)})()
    ctx = {"daemon_start_time": opened_ms / 1000.0, "actual_entry": 12.452, "is_buy": True}
    net, fees, exit_px = asyncio.run(M.DistributedQuantEngine._reconstruct_pnl_from_fills(fake, "LINKUSDT", ctx))
    gross = (12.465 - 12.452) * 1.5
    assert fees == pytest.approx(0.0206), "both legs must be charged"
    assert net == pytest.approx(gross - 0.0206)
    assert net < 0, "this 'win' was really a loss once the opening fee is counted"


def test_fallback_does_not_count_an_earlier_unrelated_trade():
    opened_ms = 1_700_000_000_000.0
    rows = [
        {"execTime": str(opened_ms - 3_600_000), "side": "Buy", "execQty": "9",
         "execPrice": "10", "execFee": "5.0"},                           # an hour earlier
        {"execTime": str(opened_ms - 1000), "side": "Buy", "execQty": "1",
         "execPrice": "10", "execFee": "0.01"},
        {"execTime": str(opened_ms + 60_000), "side": "Sell", "execQty": "1",
         "execPrice": "10.1", "execFee": "0.01"},
    ]
    fake = type("E", (), {"executor": _Exec(rows)})()
    ctx = {"daemon_start_time": opened_ms / 1000.0, "actual_entry": 10.0, "is_buy": True}
    net, fees, _ = asyncio.run(M.DistributedQuantEngine._reconstruct_pnl_from_fills(fake, "X", ctx))
    assert fees == pytest.approx(0.02)


# ---- receipts and tickets ----------------------------------------------------

@pytest.fixture
def tg():
    from services.telegram_ops import AsyncTelegramReporter
    return AsyncTelegramReporter("", "")


def test_receipt_gross_is_net_plus_fees(tg):
    msg = tg.format_execution_receipt("LINKUSDT", 0.0157, 1.0, 0.0206, 29.0, True)
    assert "+0.0363" in msg and "-0.0206" in msg


def test_ticket_shows_filled_exposure_not_intended(tg):
    """The $1.25 LINK partial fill on a $77 account read 'Sizing Risk: 25.00%'."""
    msg = tg.format_entry_ticket("LINKUSDT", "BUY", 12.514, 0.1, 1.2, 0.25, "TREND",
                                 {"virtual_sl": 12.514 * 0.985}, equity=77.0, cost_bps=15.0)
    assert "1.6% of equity" in msg
    assert "25.00%" not in msg
    assert "Loss if stopped" in msg and "0.02% of equity" in msg


def test_ticket_says_plainly_when_edge_does_not_clear_costs(tg):
    msg = tg.format_entry_ticket("DOGEUSDT", "BUY", 0.088, 215, 1.7, 0.25, "TREND",
                                 {"virtual_sl": 0.088 * 0.985}, equity=77.0, cost_bps=15.0)
    assert "DOES NOT clear costs" in msg


def test_ticket_edge_is_no_longer_hard_coded():
    assert "actual_qty_filled, 0.0, (target_notional / current_bal)" not in MAIN
    assert "horizon_edge_bps(" in MAIN


def test_ticket_is_html_safe(tg):
    msg = tg.format_entry_ticket("<b>X</b>", "BUY", 1.0, 1.0, 1.0, 0.1, "TREND",
                                 {}, equity=10.0, cost_bps=15.0, gate_note="<script>")
    assert "<script>" not in msg and "&lt;script&gt;" in msg
