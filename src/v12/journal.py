"""
V12.4a — TRADE JOURNAL: every trade becomes training data.

A self-contained SQLite journal, separate from the legacy ledger, so its schema
can carry everything the learning loop needs without migrating a live table:

    trades      one row per trade: what the model PREDICTED at entry (horizon,
                probability, expected edge, cost, regime, volatility, spread,
                quality score, full decision card as JSON) next to what
                HAPPENED (fill prices, net return after fees and funding,
                fees, slippage, MFE, MAE, holding time, exit reason, R).
    decisions   rejected decisions too (sampled), so we can later ask "what
                did the trades we refused do?" -- without that, a gate can
                never be shown to be adding value.
    paths       the price path of each trade from entry to 240 minutes AFTER
                exit. MFE/MAE exit design needs to know what happened after
                we left, otherwise every exit rule looks optimal by construction.

Unknown values are stored as NULL, never 0 (a NaN fee is not a free trade).
Writes never raise into the trading loop: a journal failure is logged and
counted, and trading continues.
"""
from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

log = logging.getLogger("v12.journal")

POST_EXIT_TRACK_SEC = 240 * 60
PATH_SAMPLE_SEC = 10

_SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    trade_id TEXT PRIMARY KEY,
    trading_mode TEXT, symbol TEXT, direction TEXT,
    entry_ts REAL, exit_ts REAL, entry_price REAL, exit_price REAL,
    qty REAL, notional REAL, stop_pct REAL,
    model_horizon_sec INTEGER, p_cal REAL, signal_score REAL, agreement REAL,
    expected_edge_bps REAL, cost_bps REAL, regime TEXT, volatility TEXT,
    sigma_1m REAL, spread_bps REAL, volume_24h REAL, quality_score REAL,
    net_pnl REAL, net_return_bps REAL, fees REAL, funding REAL, slippage_bps REAL,
    mfe_r REAL, mae_r REAL, r_multiple REAL, hold_min REAL, exit_reason TEXT,
    decision_json TEXT, status TEXT DEFAULT 'OPEN'
);
CREATE INDEX IF NOT EXISTS trades_exit ON trades(exit_ts);
CREATE TABLE IF NOT EXISTS decisions (
    ts REAL, symbol TEXT, direction TEXT, decision TEXT, reason TEXT,
    horizon_sec INTEGER, net_edge_bps REAL, cost_bps REAL, regime TEXT,
    price REAL, stop_pct REAL, outcome_bps REAL, decision_json TEXT
);
CREATE INDEX IF NOT EXISTS decisions_ts ON decisions(ts);
CREATE TABLE IF NOT EXISTS paths (
    trade_id TEXT, ts REAL, price REAL, phase TEXT,
    PRIMARY KEY (trade_id, ts)
);
"""


def _num(v: Any) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


@dataclass
class _Tracker:
    trade_id: str
    symbol: str
    entry_ts: float
    exit_ts: Optional[float] = None
    last_ts: float = 0.0


class TradeJournal:
    def __init__(self, path: Optional[str] = None, mode: str = "PAPER",
                 decision_sample_every_sec: float = 60.0):
        base = os.getenv("PERSISTENT_STORAGE_PATH", ".")
        self.path = path or os.path.join(base, "v12_journal.db")
        self.mode = mode.upper()
        self.failures = 0
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._trackers: Dict[str, _Tracker] = {}
        self._last_decision: Dict[Tuple[str, str], float] = {}
        self.decision_sample_every_sec = decision_sample_every_sec
        for tid, sym, ets, xts in self._conn.execute(
                "SELECT trade_id, symbol, entry_ts, exit_ts FROM trades "
                "WHERE status='OPEN' OR (exit_ts IS NOT NULL AND exit_ts > ?)",
                (time.time() - POST_EXIT_TRACK_SEC,)):
            self._trackers[tid] = _Tracker(tid, sym, ets or 0.0, xts)

    def _exec(self, sql: str, args: Iterable[Any] = ()) -> bool:
        try:
            with self._lock:
                self._conn.execute(sql, tuple(args))
                self._conn.commit()
            return True
        except sqlite3.Error as exc:   # never break the trading loop
            self.failures += 1
            log.warning("journal write failed: %s", exc)
            return False

    # ------------------------------------------------------------------ writes
    def open_trade(self, trade_id: str, decision: Any, entry_ts: float, entry_price: float,
                   qty: float, volume_24h: Optional[float] = None) -> bool:
        d = decision.as_dict() if hasattr(decision, "as_dict") else dict(decision)
        ok = self._exec(
            "INSERT OR REPLACE INTO trades (trade_id, trading_mode, symbol, direction, entry_ts, "
            "entry_price, qty, notional, stop_pct, model_horizon_sec, p_cal, signal_score, agreement, "
            "expected_edge_bps, cost_bps, regime, volatility, sigma_1m, spread_bps, volume_24h, "
            "quality_score, decision_json, status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'OPEN')",
            (trade_id, self.mode, d.get("symbol"), d.get("direction"), _num(entry_ts), _num(entry_price),
             _num(qty), _num((qty or 0) * (entry_price or 0)), _num(d.get("stop_pct")),
             d.get("horizon_sec"), _num(d.get("p_cal")), _num(d.get("signal_score")),
             _num(d.get("agreement")), _num(d.get("net_edge_bps")), _num(d.get("cost_bps")),
             d.get("regime"), d.get("volatility"), _num(d.get("sigma_1m")), _num(d.get("spread_bps")),
             _num(volume_24h), _num(d.get("quality_score")), json.dumps(d, default=str)))
        if ok:
            self._trackers[trade_id] = _Tracker(trade_id, str(d.get("symbol")), float(entry_ts))
        return ok

    def close_trade(self, trade_id: str, exit_ts: float, exit_price: float, net_pnl: Optional[float],
                    fees: Optional[float], exit_reason: str, funding: Optional[float] = None,
                    slippage_bps: Optional[float] = None, mfe_r: Optional[float] = None,
                    mae_r: Optional[float] = None) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT entry_ts, entry_price, qty, stop_pct, direction FROM trades WHERE trade_id=?",
            (trade_id,)).fetchone()
        if row is None:
            return None
        ets, ep, qty, stop, direction = row
        notional = (ep or 0) * (qty or 0)
        net = _num(net_pnl)
        net_bps = net / notional * 1e4 if net is not None and notional > 0 else None
        r = net_bps / (stop * 1e4) if net_bps is not None and stop else None
        status = "CLOSED" if net is not None else "UNKNOWN"
        self._exec(
            "UPDATE trades SET exit_ts=?, exit_price=?, net_pnl=?, net_return_bps=?, fees=?, funding=?, "
            "slippage_bps=?, mfe_r=?, mae_r=?, r_multiple=?, hold_min=?, exit_reason=?, status=? "
            "WHERE trade_id=?",
            (_num(exit_ts), _num(exit_price), net, net_bps, _num(fees), _num(funding), _num(slippage_bps),
             _num(mfe_r), _num(mae_r), r, (exit_ts - ets) / 60.0 if ets else None, exit_reason, status,
             trade_id))
        tr = self._trackers.get(trade_id)
        if tr:
            tr.exit_ts = exit_ts
        return {"net_return_bps": net_bps, "r_multiple": r, "status": status}

    def record_decision(self, decision: Any, price: float, force: bool = False) -> bool:
        """Journal a decision. Rejections are sampled (one per symbol/direction per
        interval) -- trades are always written."""
        d = decision.as_dict() if hasattr(decision, "as_dict") else dict(decision)
        key = (str(d.get("symbol")), str(d.get("direction")))
        now = _num(d.get("ts")) or time.time()
        if not force and d.get("decision") != "TRADE":
            if now - self._last_decision.get(key, -1e18) < self.decision_sample_every_sec:
                return False
        self._last_decision[key] = now
        reasons = d.get("reasons") or []
        return self._exec(
            "INSERT INTO decisions (ts, symbol, direction, decision, reason, horizon_sec, net_edge_bps, "
            "cost_bps, regime, price, stop_pct, decision_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (now, key[0], key[1], d.get("decision"), reasons[0].split("(")[0].strip() if reasons else "",
             d.get("horizon_sec"), _num(d.get("net_edge_bps")), _num(d.get("cost_bps")),
             d.get("regime"), _num(price), _num(d.get("stop_pct")), json.dumps(d, default=str)))

    def on_price(self, symbol: str, ts: float, price: float) -> None:
        """Feed every tick; records trade paths (in-trade and 240 min post-exit)."""
        if not self._trackers:
            return
        done = []
        for tid, tr in self._trackers.items():
            if tr.symbol != symbol or ts - tr.last_ts < PATH_SAMPLE_SEC or ts < tr.entry_ts:
                continue
            if tr.exit_ts is not None and ts - tr.exit_ts > POST_EXIT_TRACK_SEC:
                done.append(tid)
                continue
            tr.last_ts = ts
            phase = "IN" if tr.exit_ts is None or ts <= tr.exit_ts else "POST"
            self._exec("INSERT OR IGNORE INTO paths VALUES (?,?,?,?)", (tid, ts, _num(price), phase))
        for tid in done:
            self._trackers.pop(tid, None)

    def resolve_decisions(self, symbol: str, now: float, price: float) -> int:
        """Label past decisions (incl. rejected ones) with the move over their horizon,
        signed by direction, in bps before costs. Lets us measure what gates refused."""
        rows = self._conn.execute(
            "SELECT rowid, ts, direction, horizon_sec, price FROM decisions WHERE symbol=? AND "
            "outcome_bps IS NULL AND horizon_sec IS NOT NULL AND ts + horizon_sec <= ? LIMIT 500",
            (symbol, now)).fetchall()
        n = 0
        for rowid, ts, direction, h, p0 in rows:
            if not p0:
                continue
            if now - (ts + h) > 120:   # we missed the resolution moment; do not label with a later price
                self._exec("UPDATE decisions SET outcome_bps=NULL, horizon_sec=NULL WHERE rowid=?", (rowid,))
                continue
            mv = (price - p0) / p0 * 1e4 * (1 if direction == "BUY" else -1)
            n += self._exec("UPDATE decisions SET outcome_bps=? WHERE rowid=?", (mv, rowid))
        return n

    # ------------------------------------------------------------------ reads
    def closed_trades(self, mode: Optional[str] = None, since: Optional[float] = None) -> List[Dict[str, Any]]:
        q = "SELECT * FROM trades WHERE status='CLOSED'"
        args: List[Any] = []
        if mode:
            q += " AND trading_mode=?"
            args.append(mode.upper())
        if since:
            q += " AND exit_ts>=?"
            args.append(since)
        q += " ORDER BY exit_ts"
        cur = self._conn.execute(q, args)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def trade_path(self, trade_id: str) -> List[Tuple[float, float, str]]:
        return list(self._conn.execute(
            "SELECT ts, price, phase FROM paths WHERE trade_id=? ORDER BY ts", (trade_id,)))

    def decision_counts(self, since: float = 0.0) -> Dict[str, int]:
        return {r[0]: r[1] for r in self._conn.execute(
            "SELECT reason, COUNT(*) FROM decisions WHERE ts>=? AND decision='NO_TRADE' "
            "GROUP BY reason ORDER BY 2 DESC LIMIT 20", (since,))}

    def close(self) -> None:
        with self._lock:
            self._conn.close()
