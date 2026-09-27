"""Shared fakes for the emergency-patch test suite.

These stand in for the Bybit executor and the core engine. They record every
outbound order payload so tests can assert on what would actually hit the
exchange, without any network access.
"""
import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional


BTC_LIMITS = {
    "min_qty": Decimal("0.001"),
    "qty_step": Decimal("0.001"),
    "tick_size": Decimal("0.1"),
    "min_notional": Decimal("5.0"),
}

ALT_LIMITS = {
    "min_qty": Decimal("1"),
    "qty_step": Decimal("1"),
    "tick_size": Decimal("0.0001"),
    "min_notional": Decimal("5.0"),
}


class FakeExecutor:
    """Records order payloads. Never fills unless told to."""

    def __init__(self, fill_plan: Optional[Dict[str, Any]] = None):
        self.calls: List[tuple] = []
        self.created_orders: List[Dict[str, Any]] = []
        # fill_plan controls what /v5/order/realtime reports back
        self.fill_plan = fill_plan or {
            "orderStatus": "New",
            "cumExecQty": "0",
            "avgPrice": "",
        }
        self.position_size = 0.0
        self.position_side = "Buy"
        self.position_avg = 0.0

    async def safe_call(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        self.calls.append((method, endpoint, kwargs))

        if endpoint == "/v5/order/create":
            self.created_orders.append(dict(kwargs))
            return {"retCode": 0, "retMsg": "OK", "result": {"orderId": "OID-1"}}

        if endpoint == "/v5/order/realtime":
            return {"retCode": 0, "result": {"list": [dict(self.fill_plan)]}}

        if endpoint == "/v5/order/history":
            return {"retCode": 0, "result": {"list": [dict(self.fill_plan)]}}

        if endpoint == "/v5/order/cancel":
            return {"retCode": 0, "result": {}}

        if endpoint == "/v5/position/list":
            if self.position_size <= 0.0:
                return {"retCode": 0, "result": {"list": []}}
            return {
                "retCode": 0,
                "result": {
                    "list": [{
                        "size": str(self.position_size),
                        "side": self.position_side,
                        "avgPrice": str(self.position_avg),
                        "markPrice": str(self.position_avg),
                        "liqPrice": "0",
                    }]
                },
            }

        if endpoint == "/v5/position/trading-stop":
            return {"retCode": 0, "result": {}}

        return {"retCode": 0, "result": {"list": []}}

    # --- helpers for assertions -------------------------------------------
    def submitted_quantities(self) -> List[float]:
        return [float(o["qty"]) for o in self.created_orders if "qty" in o]

    def submitted_notional(self, price: float) -> List[float]:
        return [q * price for q in self.submitted_quantities()]


class FakeCoreEngine:
    """Minimal stand-in for DistributedQuantEngine."""

    def __init__(self, orderbook_snapshots: Optional[Dict[str, dict]] = None):
        self.orderbook_snapshots = orderbook_snapshots or {}
        self.circuit_breakers: Dict[str, float] = {}
        self.stat_engines: Dict[str, Any] = {}
        self.active_positions_map: Dict[str, str] = {}
        self.global_state_cache = {"current_vault_balance": 1000.0}


def book(mid: float, depth: float = 50.0, levels: int = 5) -> Dict[str, list]:
    """Build a symmetric synthetic order book around `mid`."""
    tick = mid * 0.0001
    bids = [[mid - tick * (i + 1), depth] for i in range(levels)]
    asks = [[mid + tick * (i + 1), depth] for i in range(levels)]
    return {"bids": bids, "asks": asks}


def run(coro):
    return asyncio.run(coro)
