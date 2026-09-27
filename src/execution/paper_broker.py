"""
PAPER TRADING BROKER — simulated exchange for PAPER mode
--------------------------------------------------------------------------------
Audit finding B7: the previous "TEST_MODE" was testnet, and it additionally
disabled `_state_settle_trade`, so it recorded no outcomes and produced no
learning data. There was therefore no way to gather forward evidence without
risking capital.

This wraps the real executor and intercepts every state-changing endpoint,
simulating an account in memory. Read-only PUBLIC market endpoints are passed
through to the real exchange, so the strategy sees genuine live market data
while placing no real orders.

================================ FILL MODEL =================================
Stated explicitly, because an unstated fill model is how paper results become
dishonest:

  Limit + IOC       fills in full at the limit price if the limit is at least as
                    aggressive as the reference price, otherwise fills NOTHING
                    (status Cancelled, cumExecQty 0). This mirrors a real IOC and
                    deliberately reproduces the zero-fill case behind B2.
  Limit + PostOnly  rests. Fills only when a later mark update crosses it.
  Market            fills in full at reference price adjusted by
                    `slippage_bps` (default 4 bps, adverse).
  reduceOnly        cannot increase or flip a position; clamped to open size.

  Fees              taker on market/IOC, maker on PostOnly fills.
  Margin            initial-margin solvency IS enforced (notional / leverage
                    against equity); orders that would exceed it are rejected
                    with 110007, as the live exchange would.

MODELLED:  fills, fees, slippage, reduceOnly semantics, exchange-native
           SL/TP triggering, position averaging, realised PnL, execution
           records, initial margin, order rejection codes, leverage book.

NOT MODELLED -- and not approximated, so results must never be read as if
they were:
  Funding           absent entirely. Paper PnL is optimistic for held
                    positions by roughly the funding cost.
  Queue position    a PostOnly order fills in full the moment price touches it.
                    Real resting orders queue behind existing size and often
                    do not fill at all. This is the largest single optimism.
  True L2 matching  book depth is never consumed; size does not walk the book.
  Market impact     absent. Slippage is a fixed 4 bps, independent of size.
  Liquidation       no maintenance margin, no ADL, no liquidation engine.
  Latency           order submission and fill are instantaneous.
  Partial fills     only via reduceOnly clamping, never from insufficient depth.
  Maintenance margin / tiered risk limits / cross-vs-isolated differences.

These limits are real and must be carried into any report that cites paper
results. They make paper an instrument for CORRECTNESS testing and relative
comparison -- not a profitability oracle.
=============================================================================
"""

from __future__ import annotations

import time
import uuid
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("QUANT_CORE.PAPER")

PUBLIC_PASSTHROUGH_PREFIXES = ("/v5/market/",)


class PaperIsolationError(RuntimeError):
    """
    Raised when PAPER mode would have reached a live, account-touching
    operation. Never caught internally: a paper run that silently touches
    production is worse than a crash.
    """


@dataclass
class PaperPosition:
    symbol: str
    side: str            # "Buy" | "Sell"
    size: float = 0.0
    avg_price: float = 0.0
    position_idx: int = 0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    leverage: float = 1.0
    opened_at: float = field(default_factory=time.time)

    @property
    def is_long(self) -> bool:
        return self.side == "Buy"

    def unrealised(self, mark: float) -> float:
        if self.size <= 0.0 or mark <= 0.0:
            return 0.0
        d = (mark - self.avg_price) if self.is_long else (self.avg_price - mark)
        return d * self.size


class PaperBroker:
    """In-memory exchange simulator with a deliberately explicit fill model."""

    def __init__(
        self,
        real_executor: Any,
        starting_balance: float = 1000.0,
        taker_fee: float = 0.00055,
        maker_fee: float = 0.00020,
        slippage_bps: float = 4.0,
        max_leverage: float = 2.0,
    ):
        self._real = real_executor
        self.balance = float(starting_balance)
        self.starting_balance = float(starting_balance)
        self.taker_fee = taker_fee
        self.maker_fee = maker_fee
        self.slippage_bps = slippage_bps

        self.positions: Dict[str, PaperPosition] = {}
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.resting: Dict[str, Dict[str, Any]] = {}
        self.closed_pnl: List[Dict[str, Any]] = []
        self.marks: Dict[str, float] = {}

        self.leverage: Dict[str, float] = {}
        self.executions: List[Dict[str, Any]] = []      # AUDIT B12 paper support
        self.max_leverage: float = float(max_leverage)
        self.reject_next: Optional[Tuple[int, str]] = None   # AUDIT NEW-5 injection

        self.fills_count = 0
        self.rejected_count = 0
        self.rejections_by_code: Dict[int, int] = {}
        self.total_fees = 0.0

    # -- price plumbing ----------------------------------------------------

    def update_mark(self, symbol: str, price: float) -> None:
        """Called from the engine's tick path. Drives stops and resting fills."""
        if price and price > 0.0:
            self.marks[symbol] = float(price)
            self._check_resting(symbol, float(price))
            self._check_stops(symbol, float(price))

    def _reference_price(self, symbol: str, fallback: float = 0.0) -> float:
        return self.marks.get(symbol, fallback)

    # -- position maths ----------------------------------------------------

    def _apply_fill(self, symbol: str, side: str, qty: float, price: float,
                    fee_rate: float, reduce_only: bool, position_idx: int) -> float:
        pos = self.positions.get(symbol)
        fee = qty * price * fee_rate
        self.total_fees += fee
        self.balance -= fee
        self.fills_count += 1

        # AUDIT B12: execution records are the settlement fallback evidence.
        self.executions.insert(0, {
            "symbol": symbol, "side": side,
            "execQty": str(qty), "execPrice": str(price), "execFee": str(fee),
            "execTime": str(int(time.time() * 1000)),
        })
        del self.executions[256:]

        if pos is None or pos.size <= 0.0:
            if reduce_only:
                return 0.0
            self.positions[symbol] = PaperPosition(
                symbol=symbol, side=side, size=qty, avg_price=price,
                position_idx=position_idx,
            )
            return qty

        if pos.side == side:
            if reduce_only:
                return 0.0
            notional = pos.avg_price * pos.size + price * qty
            pos.size += qty
            pos.avg_price = notional / pos.size
            return qty

        # Opposite side -> reduce
        closing = min(qty, pos.size)
        pnl = ((price - pos.avg_price) if pos.is_long else (pos.avg_price - price)) * closing
        self.balance += pnl
        pos.size -= closing

        self.closed_pnl.insert(0, {
            "symbol": symbol,
            "closedPnl": str(pnl - fee),
            "avgExitPrice": str(price),
            "avgEntryPrice": str(pos.avg_price),
            "execFee": str(fee),
            "qty": str(closing),
            "side": side,
            "updatedTime": str(int(time.time() * 1000)),
        })
        del self.closed_pnl[64:]

        if pos.size <= 1e-12:
            self.positions.pop(symbol, None)
        return closing

    def _check_stops(self, symbol: str, price: float) -> None:
        pos = self.positions.get(symbol)
        if not pos or pos.size <= 0.0:
            return
        hit = None
        if pos.is_long:
            if pos.stop_loss > 0.0 and price <= pos.stop_loss:
                hit = pos.stop_loss
            elif pos.take_profit > 0.0 and price >= pos.take_profit:
                hit = pos.take_profit
        else:
            if pos.stop_loss > 0.0 and price >= pos.stop_loss:
                hit = pos.stop_loss
            elif pos.take_profit > 0.0 and price <= pos.take_profit:
                hit = pos.take_profit
        if hit is not None:
            closing_side = "Sell" if pos.is_long else "Buy"
            logger.info(f"[PAPER] Exchange-native stop triggered on {symbol} @ {hit:.6f}")
            self._apply_fill(symbol, closing_side, pos.size, hit,
                             self.taker_fee, True, pos.position_idx)

    def _check_resting(self, symbol: str, price: float) -> None:
        for oid, o in list(self.resting.items()):
            if o["symbol"] != symbol:
                continue
            limit = o["price"]
            crossed = (o["side"] == "Buy" and price <= limit) or \
                      (o["side"] == "Sell" and price >= limit)
            if not crossed:
                continue
            filled = self._apply_fill(symbol, o["side"], o["qty"], limit,
                                      self.maker_fee, o["reduce_only"], o["position_idx"])
            rec = self.orders[oid]
            rec["cumExecQty"] = str(filled)
            rec["avgPrice"] = str(limit) if filled > 0 else ""
            rec["orderStatus"] = "Filled" if filled >= o["qty"] - 1e-12 else "PartiallyFilledCanceled"
            self.resting.pop(oid, None)

    # -- endpoint dispatch -------------------------------------------------

    async def safe_call(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        if endpoint.startswith(PUBLIC_PASSTHROUGH_PREFIXES):
            return await self._real.safe_call(method, endpoint, **kwargs)

        handler = {
            "/v5/order/create": self._create_order,
            "/v5/order/cancel": self._cancel_order,
            "/v5/order/cancel-all": self._cancel_all,
            "/v5/order/realtime": self._query_order,
            "/v5/order/history": self._query_order,
            "/v5/position/list": self._position_list,
            "/v5/position/trading-stop": self._trading_stop,
            "/v5/position/closed-pnl": self._closed_pnl,
            "/v5/account/wallet-balance": self._wallet_balance,
            "/v5/position/set-leverage": self._ok,
            "/v5/position/switch-mode": self._ok,
            "/v5/account/fee-rate": self._fee_rate,
            "/v5/execution/list": self._execution_list,
        }.get(endpoint)

        if handler is None:
            return {"retCode": 0, "retMsg": "PAPER_NOOP", "result": {"list": []}}
        return handler(**kwargs)

    def _reject(self, code: int, msg: str) -> Dict[str, Any]:
        """AUDIT NEW-5: every rejection is counted and carries a real Bybit code."""
        self.rejected_count += 1
        self.rejections_by_code[code] = self.rejections_by_code.get(code, 0) + 1
        logger.info(f"[PAPER] Order rejected ({code}): {msg}")
        return {"retCode": code, "retMsg": msg, "result": {}}

    def _check_margin(self, symbol: str, side: str, qty: float, price: float) -> Tuple[bool, str]:
        """
        AUDIT NEW-4: initial-margin solvency check.

        Deliberately simple and conservative: required margin is notional over
        the symbol's configured leverage, and total required margin across all
        positions must not exceed equity. It does NOT model maintenance margin,
        tiered risk limits, cross/isolated differences or liquidation mechanics
        — those are documented as unmodelled rather than approximated.
        """
        lev = max(1.0, self.leverage.get(symbol, self.max_leverage))
        incoming_notional = qty * price

        existing = 0.0
        for s, p in self.positions.items():
            p_lev = max(1.0, self.leverage.get(s, self.max_leverage))
            existing += (p.size * p.avg_price) / p_lev

        required = existing + (incoming_notional / lev)
        equity = self.equity()

        if required > equity:
            return False, (
                f"insufficient margin: need ${required:,.2f} at {lev:g}x, "
                f"equity ${equity:,.2f}"
            )
        return True, "OK"

    def _ok(self, **kw):
        return {"retCode": 0, "retMsg": "OK", "result": {}}

    def _execution_list(self, **kw):
        """
        AUDIT B12 (paper): the settlement fills-fallback queries this endpoint.
        Without a handler it returned the default empty noop, so the fallback
        could never succeed in PAPER — meaning paper could not exercise the very
        recovery path B12 added.
        """
        sym = kw.get("symbol")
        rows = [e for e in self.executions if not sym or e["symbol"] == sym]
        rows.sort(key=lambda r: float(r["execTime"]), reverse=True)
        return {"retCode": 0, "result": {"list": rows[: int(kw.get("limit", 50))]}}

    def _fee_rate(self, **kw):
        return {"retCode": 0, "result": {"list": [
            {"takerFeeRate": str(self.taker_fee), "makerFeeRate": str(self.maker_fee)}
        ]}}

    def _create_order(self, **kw):
        symbol = kw.get("symbol", "")
        side = kw.get("side", "Buy")
        order_type = kw.get("orderType", "Market")
        tif = kw.get("timeInForce", "IOC")
        reduce_only = bool(kw.get("reduceOnly", False))
        position_idx = int(kw.get("positionIdx", 0) or 0)
        oid = f"PAPER-{uuid.uuid4().hex[:16]}"

        # AUDIT NEW-5: injectable rejection so failure paths are testable.
        if self.reject_next is not None:
            code, msg = self.reject_next
            self.reject_next = None
            return self._reject(code, msg)

        try:
            qty = float(kw.get("qty", 0.0))
        except (TypeError, ValueError):
            qty = 0.0
        if qty <= 0.0:
            return self._reject(10001, "qty invalid")

        ref = self._reference_price(symbol)
        if ref <= 0.0:
            try:
                ref = float(kw.get("price", 0.0) or 0.0)
            except (TypeError, ValueError):
                ref = 0.0
        if ref <= 0.0:
            return self._reject(10001, "no reference price in paper broker")

        if reduce_only:
            pos = self.positions.get(symbol)
            open_size = pos.size if pos else 0.0
            qty = min(qty, open_size)
            if qty <= 0.0:
                self._record(oid, symbol, side, 0.0, "Cancelled", "")
                return {"retCode": 0, "retMsg": "OK", "result": {"orderId": oid}}
        else:
            # AUDIT NEW-4: solvency. A $100 account previously opened a $1M
            # position and went to -450. The live exchange rejects that with
            # 110007, so paper accepted trades live would refuse and could not
            # validate sizing or leverage behaviour.
            ok, why = self._check_margin(symbol, side, qty, ref)
            if not ok:
                return self._reject(110007, why)

        if order_type == "Market":
            slip = self.slippage_bps / 10000.0
            px = ref * (1.0 + slip) if side == "Buy" else ref * (1.0 - slip)
            filled = self._apply_fill(symbol, side, qty, px, self.taker_fee, reduce_only, position_idx)
            self._record(oid, symbol, side, filled, "Filled" if filled > 0 else "Cancelled",
                         str(px) if filled > 0 else "")
        elif tif == "PostOnly":
            limit = float(kw.get("price", ref))
            self.resting[oid] = {"symbol": symbol, "side": side, "qty": qty, "price": limit,
                                 "reduce_only": reduce_only, "position_idx": position_idx}
            self._record(oid, symbol, side, 0.0, "New", "")
        else:  # Limit IOC / FOK
            limit = float(kw.get("price", ref))
            marketable = (side == "Buy" and limit >= ref) or (side == "Sell" and limit <= ref)
            if marketable:
                filled = self._apply_fill(symbol, side, qty, limit, self.taker_fee,
                                          reduce_only, position_idx)
                self._record(oid, symbol, side, filled, "Filled" if filled > 0 else "Cancelled",
                             str(limit) if filled > 0 else "")
            else:
                # Reproduces the real zero-fill IOC that B2 mishandled.
                self._record(oid, symbol, side, 0.0, "Cancelled", "")

        self._apply_inline_brackets(symbol, kw)
        return {"retCode": 0, "retMsg": "OK", "result": {"orderId": oid,
                                                         "orderLinkId": kw.get("orderLinkId", "")}}

    def _apply_inline_brackets(self, symbol: str, kw: Dict[str, Any]) -> None:
        pos = self.positions.get(symbol)
        if not pos:
            return
        for key, attr in (("stopLoss", "stop_loss"), ("takeProfit", "take_profit")):
            if kw.get(key):
                try:
                    setattr(pos, attr, float(kw[key]))
                except (TypeError, ValueError):
                    pass

    def _record(self, oid, symbol, side, filled, status, avg):
        rec = {"orderId": oid, "symbol": symbol, "side": side,
               "cumExecQty": str(filled), "avgPrice": avg, "orderStatus": status}
        self.orders[oid] = rec
        return rec

    def _cancel_order(self, **kw):
        oid = kw.get("orderId", "")
        self.resting.pop(oid, None)
        if oid in self.orders and self.orders[oid]["orderStatus"] == "New":
            self.orders[oid]["orderStatus"] = "Cancelled"
        return {"retCode": 0, "result": {}}

    def _cancel_all(self, **kw):
        sym = kw.get("symbol")
        for oid, o in list(self.resting.items()):
            if sym is None or o["symbol"] == sym:
                self.resting.pop(oid, None)
                self.orders[oid]["orderStatus"] = "Cancelled"
        return {"retCode": 0, "result": {}}

    def _query_order(self, **kw):
        oid = kw.get("orderId", "")
        rec = self.orders.get(oid)
        return {"retCode": 0, "result": {"list": [rec] if rec else []}}

    def _position_list(self, **kw):
        sym = kw.get("symbol")
        rows = []
        for s, p in self.positions.items():
            if sym and s != sym:
                continue
            if p.size <= 0.0:
                continue
            mark = self.marks.get(s, p.avg_price)
            rows.append({
                "symbol": s, "side": p.side, "size": str(p.size),
                "avgPrice": str(p.avg_price), "markPrice": str(mark),
                "positionIdx": p.position_idx, "leverage": str(p.leverage),
                "liqPrice": "0",
                "positionValue": str(p.size * p.avg_price),
                "unrealisedPnl": str(p.unrealised(mark)),
            })
        return {"retCode": 0, "result": {"list": rows}}

    def _trading_stop(self, **kw):
        pos = self.positions.get(kw.get("symbol", ""))
        if not pos:
            return {"retCode": 110001, "retMsg": "position not found", "result": {}}
        for key, attr in (("stopLoss", "stop_loss"), ("takeProfit", "take_profit")):
            if kw.get(key):
                try:
                    setattr(pos, attr, float(kw[key]))
                except (TypeError, ValueError):
                    pass
        return {"retCode": 0, "result": {}}

    def _closed_pnl(self, **kw):
        sym = kw.get("symbol")
        rows = [r for r in self.closed_pnl if not sym or r["symbol"] == sym]
        return {"retCode": 0, "result": {"list": rows[: int(kw.get("limit", 50))]}}

    def _wallet_balance(self, **kw):
        equity = self.equity()
        return {"retCode": 0, "result": {"list": [{
            "totalEquity": str(equity),
            "totalWalletBalance": str(self.balance),
            "totalMarginBalance": str(equity),
            "coin": [{"coin": "USDT", "equity": str(equity), "walletBalance": str(self.balance)}],
        }]}}

    # =====================================================================
    # ISOLATION BOUNDARY  (AUDIT NEW-1 / NEW-2 / NEW-3)
    # =====================================================================
    # The original implementation delegated any undefined attribute to the
    # real executor. Interception therefore covered ONLY `safe_call`, and the
    # engine's direct method calls escaped to the live exchange:
    #
    #   get_wallet_balance_usdt()  -> returned the REAL account balance, so the
    #                                 engine sized, set watermarks and computed
    #                                 drawdown from an account paper cannot move.
    #   adjust_leverage()          -> issued a real authenticated
    #                                 POST /v5/position/set-leverage.
    #   connect_ws()               -> opened an authenticated private WebSocket.
    #
    # Delegation is now an explicit allow-list of READ-ONLY, PUBLIC operations.
    # Anything else raises rather than silently reaching production.

    _PUBLIC_READONLY_PASSTHROUGH = frozenset({
        "get_top_volatile_assets",   # public /v5/market/tickers screen
        "calibrate_server_time",     # public /v5/market/time
        "rest_base_url",
        "testnet",
        "temporary_symbol_bans",
    })

    def __getattr__(self, item: str):
        if item.startswith("_"):
            raise AttributeError(item)
        if item in self._PUBLIC_READONLY_PASSTHROUGH:
            return getattr(self._real, item)
        raise PaperIsolationError(
            f"PaperBroker refuses to delegate '{item}' to the live executor. "
            f"In PAPER mode every account-touching operation must be simulated. "
            f"If this is a genuine read-only PUBLIC call, add it to "
            f"_PUBLIC_READONLY_PASSTHROUGH; otherwise implement it on PaperBroker."
        )

    # --- simulated replacements for the escaped methods -------------------

    async def get_wallet_balance_usdt(self) -> float:
        """AUDIT NEW-1: the PAPER equity, never the live account."""
        return self.equity()

    def equity(self) -> float:
        return self.balance + sum(
            p.unrealised(self.marks.get(s, p.avg_price)) for s, p in self.positions.items()
        )

    async def adjust_leverage(self, symbol: str, target_leverage: int) -> bool:
        """AUDIT NEW-2: record it; never touch the live account."""
        lev = max(1.0, float(target_leverage))
        self.leverage[symbol] = lev
        pos = self.positions.get(symbol)
        if pos:
            pos.leverage = lev
        return True

    async def connect_ws(self) -> None:
        """AUDIT NEW-3: no authenticated private WebSocket from PAPER."""
        logger.info("[PAPER] Private WebSocket suppressed — PAPER holds no exchange session.")

    async def await_ws_execution_report(self, order_id: str, timeout: float = 0.25):
        """Paper fills resolve synchronously; the REST path already has them."""
        return self.orders.get(order_id)

    async def get_fee_rates(self, symbol: str = "BTCUSDT") -> Dict[str, float]:
        return {"taker": self.taker_fee, "maker": self.maker_fee}

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        logger.info(f"[PAPER] Session closed. {self.stats()}")

    def stats(self) -> Dict[str, Any]:
        return {
            "mode": "PAPER",
            "balance": round(self.balance, 6),
            "starting_balance": self.starting_balance,
            "open_positions": len(self.positions),
            "fills": self.fills_count,
            "rejected": self.rejected_count,
            "total_fees": round(self.total_fees, 6),
            "closed_trades": len(self.closed_pnl),
        }
