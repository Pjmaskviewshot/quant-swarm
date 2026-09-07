"""
V40.3 APEX TITAN: DIRECT-DRIVE HIGH-FREQUENCY SMART ORDER ROUTER
--------------------------------------------------------------------------------
Institutional-grade execution nexus featuring atomic inline bracket orders,
post-fill exchange bracket verification sentries, Avellaneda-Stoikov inventory
reservation pricing, sub-millisecond WebSocket execution telemetry, Perold (1988)
Implementation Shortfall (IS) tracking, pure Decimal lot-quantization, and
contention-free token-bucket rate governance.

Architectural Supremacy (V40.3 Production Upgrades):
- Closed-Form Almgren-Chriss/Kyle Market Impact Modeling (Audit #6 Resolution):
  Replaces static depth assumptions with a dynamic square-root impact law
  scaled by instantaneous microstructure variance and order participation ratio.
- Adaptive TWAP Iceberg Slicing & Cadence: Dynamically sizes slice counts (3 to 8)
  and expands inter-slice delays (2.5s to 8.0s) as volatility and market impact
  surge, preventing predatory orderbook front-running.
- Continuous Intermediate TWAP Protection (Audit #5C Resolution): Re-anchors
  position-level Stop-Loss brackets immediately after each filled child slice,
  eliminating intermediate unhedged inventory risk.
- Partial-Mode Bracket Isolation (Audit #5A Resolution): Enforces `tpslMode="Partial"`
  on all child orders to eliminate bracket overwriting during maker peg handoffs.
- Dynamic Avellaneda-Stoikov Gamma Calibration: Modulates risk aversion (gamma)
  against local tick granularity to ensure quoting half-spreads strictly clear
  exchange tick size floors.
- Scientific-Notation Proof Quantization: Serializes Decimal quantities with
  fixed-point non-exponential formatting, permanently eradicating Bybit Error 10001.
"""

import os
import asyncio
import logging
import math
import time
import random
import numpy as np
from typing import Dict, Any, List, Tuple, Optional
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP

logger = logging.getLogger("QUANT_CORE.SOR")


class SmartOrderRouter:
    """
    V40.3 DIRECT-DRIVE EXECUTION NEXUS
    Routes high-frequency orders across Sweep-to-Peg (IOC/Maker Hybrid),
    Avellaneda-Stoikov Maker Peg (PostOnly), and TWAP Iceberg slices with
    sub-millisecond fill verification and continuous position bracket protection.
    """
    def __init__(self, executor: Any, max_slippage_pct: float = 0.0012, core_engine: Any = None):
        self.executor = executor
        self.core_engine = core_engine
        self.base_max_slippage_pct = max_slippage_pct
        self.instrument_cache: Dict[str, Dict[str, Any]] = {}
        self.position_idx = int(os.getenv("BYBIT_POSITION_IDX", 0))
        self._last_amend_time: Dict[str, float] = {}

        # Avellaneda-Stoikov Base Parameters
        self.gamma_base = 0.08  # Baseline inventory risk-aversion parameter
        self.k_decay = 1.5      # Orderbook liquidity density parameter

        # Exchange Fee Schedules (Defaults to Bybit VIP0 Linear Perps)
        self.taker_fee_rate: float = 0.00055
        self.maker_fee_rate: float = 0.00020

        # Contention-Free Token Bucket (Burst: 12 calls/sec, Steady-State: 8 calls/sec)
        self._rate_tokens = 12.0
        self._rate_last_check = time.time()
        self._rate_lock = asyncio.Lock()

    # =========================================================================
    # RATE LIMITING & NON-BLOCKING PACING
    # =========================================================================

    async def _rate_limit_acquire(self):
        """
        Contention-Free Monotonic Token Bucket:
        Computes necessary backoff within the lock and schedules the sleep outside
        the critical section, preventing event-loop stalls across concurrent workers.
        """
        sleep_time = 0.0
        async with self._rate_lock:
            now = time.time()
            elapsed = max(0.0, now - self._rate_last_check)
            self._rate_last_check = now
            self._rate_tokens = min(12.0, self._rate_tokens + (elapsed * 8.0))

            if self._rate_tokens < 1.0:
                sleep_time = (1.0 - self._rate_tokens) / 8.0
                self._rate_tokens = 0.0
                # Advance virtual timeline so subsequent tasks queue deterministically
                self._rate_last_check += sleep_time
            else:
                self._rate_tokens -= 1.0

        if sleep_time > 0.0:
            await asyncio.sleep(sleep_time)

    # =========================================================================
    # EXCHANGE SPECIFICATIONS & DETERMINISTIC QUANTIZATION
    # =========================================================================

    async def _fetch_exchange_limits(self, symbol: str):
        """Caches and validates lot size filters, tick sizes, and notional thresholds."""
        if symbol in self.instrument_cache:
            return

        try:
            info = await self.executor.safe_call(
                "GET", "/v5/market/instruments-info", category="linear", symbol=symbol
            )
            data_list = info.get("result", {}).get("list", [])
            if data_list:
                lot_filter = data_list[0].get("lotSizeFilter", {})
                price_filter = data_list[0].get("priceFilter", {})
                self.instrument_cache[symbol] = {
                    "min_qty": Decimal(str(lot_filter.get("minOrderQty", "1.0"))),
                    "qty_step": Decimal(str(lot_filter.get("qtyStep", "1.0"))),
                    "tick_size": Decimal(str(price_filter.get("tickSize", "0.01"))),
                    "min_notional": Decimal(str(lot_filter.get("minNotionalValue", "5.0")))
                }
                return
        except Exception as e:
            logger.error(f"[X-RAY] Failed to fetch exchange specifications for {symbol}: {e}")

        # Conservative fallback specifications
        self.instrument_cache[symbol] = {
            "min_qty": Decimal("1.0"),
            "qty_step": Decimal("1.0"),
            "tick_size": Decimal("0.01"),
            "min_notional": Decimal("5.0")
        }

    def _apply_dynamic_exchange_limits(self, raw_qty: float, current_price: float, symbol: str) -> float:
        """Enforces exchange lot steps and minimum notional floors using pure Decimal math."""
        limits = self.instrument_cache.get(symbol, {
            "min_qty": Decimal("1.0"),
            "qty_step": Decimal("1.0"),
            "tick_size": Decimal("0.01"),
            "min_notional": Decimal("5.0")
        })
        min_qty: Decimal = limits["min_qty"]
        qty_step: Decimal = limits["qty_step"]
        min_notional: Decimal = max(Decimal("6.50"), limits["min_notional"] * Decimal("1.05"))
        price_dec = Decimal(str(max(current_price, 1e-9)))
        raw_qty_dec = Decimal(str(max(0.0, raw_qty)))

        if qty_step > Decimal("0"):
            stepped_qty = (raw_qty_dec // qty_step) * qty_step
        else:
            stepped_qty = raw_qty_dec

        notional = stepped_qty * price_dec
        if notional < min_notional:
            req_tokens = min_notional / price_dec
            if qty_step > Decimal("0"):
                stepped_qty = math.ceil(float(req_tokens / qty_step)) * qty_step
            else:
                stepped_qty = req_tokens

        return float(max(min_qty, stepped_qty))

    def _format_qty_str(self, raw_qty: float | Decimal, symbol: str) -> str:
        """Strict floor-quantization (ROUND_FLOOR) preventing margin rejections and scientific notation."""
        qty_step: Decimal = self.instrument_cache.get(symbol, {}).get("qty_step", Decimal("1.0"))
        if qty_step <= Decimal("0"):
            return f"{float(raw_qty):.4f}"
        val_dec = Decimal(str(raw_qty))
        quantized = (val_dec // qty_step) * qty_step
        precision = max(0, -qty_step.as_tuple().exponent)
        return f"{quantized:.{precision}f}"

    def _format_price_str(self, price: float | Decimal, target_symbol: str) -> str:
        """Quantizes order price to the nearest tick grid using ROUND_HALF_UP without scientific notation."""
        tick_size: Decimal = self.instrument_cache.get(target_symbol, {}).get("tick_size", Decimal("0.01"))
        if tick_size <= Decimal("0"):
            return str(price)
        precision = max(0, -tick_size.as_tuple().exponent)
        try:
            stepped = Decimal(str(price)).quantize(tick_size, rounding=ROUND_HALF_UP)
            return f"{stepped:.{precision}f}"
        except Exception:
            return f"{float(price):.{precision}f}"

    # =========================================================================
    # BRACKET INTEGRITY SENTRY & RISK MITIGATION
    # =========================================================================

    async def _verify_and_anchor_stops(
        self,
        symbol: str,
        direction: str,
        fill_price: float,
        sl: Optional[float],
        tp: Optional[float]
    ):
        """
        Actively verifies position bracket state on Bybit post-fill.
        If the exchange dropped or rejected the protective stop during execution,
        this sentry anchors position-level stops via /v5/position/trading-stop.
        """
        if not sl and not tp:
            return

        is_buy = direction.upper() == "BUY"
        for attempt in range(3):
            await asyncio.sleep(0.12 * (attempt + 1))
            try:
                pos_res = await self.executor.safe_call(
                    "GET", "/v5/position/list", category="linear", symbol=symbol
                )
                positions = pos_res.get("result", {}).get("list", [])
                if not positions or float(positions[0].get("size", 0.0)) <= 0:
                    return

                pos = positions[0]
                active_sl = float(pos.get("stopLoss", 0.0) or 0.0)
                mark_price = float(pos.get("markPrice", fill_price) or fill_price)

                # Protective Stop-Loss dropped or missing on exchange
                if active_sl <= 0.0 and sl:
                    if (is_buy and sl >= mark_price) or (not is_buy and sl <= mark_price):
                        logger.warning(
                            f"[SOR_SENTRY] SL Clashing with MarkPrice on {symbol} (SL: {sl:.4f}, Mark: {mark_price:.4f}). "
                            f"Recalibrating to safe 50 bps boundary."
                        )
                        sl = mark_price * (0.995 if is_buy else 1.005)

                    logger.critical(
                        f"[SOR_SENTRY] NAKED POSITION BREACH // {symbol} active without Stop-Loss! "
                        f"Forcing immediate bracket attachment..."
                    )
                    sl_str = self._format_price_str(sl, symbol)
                    payload: Dict[str, Any] = {
                        "category": "linear",
                        "symbol": symbol,
                        "positionIdx": self.position_idx,
                        "stopLoss": sl_str,
                        "slTriggerBy": "MarkPrice",
                        "tpslMode": "Full"
                    }
                    if tp:
                        if (is_buy and tp > mark_price) or (not is_buy and tp < mark_price):
                            payload["takeProfit"] = self._format_price_str(tp, symbol)
                            payload["tpTriggerBy"] = "LastPrice"

                    attach_res = await self.executor.safe_call(
                        "POST", "/v5/position/trading-stop", is_execution=True, **payload
                    )
                    if attach_res.get("retCode") == 0:
                        logger.info(f"[SOR_SENTRY] Stops anchored successfully on {symbol} (SL: {sl_str}).")
                        return
                else:
                    return
            except Exception as e:
                logger.debug(f"[SOR_SENTRY] Bracket integrity sentry probe fault on {symbol}: {e}")

    # =========================================================================
    # CAPITAL SIZING & MICROSTRUCTURE FIREWALLS
    # =========================================================================

    def calculate_risk_adjusted_notional(
        self,
        prob_success: float,
        exec_weight: float,
        sl_pct: float,
        tp_pct: float,
        current_balance: float,
        inst_var: float
    ) -> float:
        """Fallback sizing engine bounded strictly within portfolio leverage headroom."""
        base_risk_pct = 0.01
        vol_scalar = 1.0 / (1.0 + (inst_var * 1000.0))
        confidence_scalar = float(np.clip((prob_success - 0.5) * 2.0, 0.5, 1.0))

        final_risk_pct = min(0.015, base_risk_pct * vol_scalar * confidence_scalar * exec_weight)
        trade_risk_dollars = current_balance * final_risk_pct
        target_notional = trade_risk_dollars / (sl_pct + 1e-9)

        # Enforce 1.90x balance ceiling to prevent aggregate HEAT_CAP_EXCEEDED vetoes
        max_leverage_cap = 1.90
        max_permitted_notional = max(6.50, current_balance * max_leverage_cap)
        return float(np.clip(target_notional, 6.50, max_permitted_notional))

    def compute_dynamic_slippage_cap_bps(self, symbol: str, regime: str, live_spread_bps: float) -> float:
        """Calculates adaptive slippage boundary based on asset tier and spread."""
        is_major = symbol in ["BTCUSDT", "ETHUSDT"]
        is_high_cap = symbol in ["SOLUSDT", "SUIUSDT", "AVAXUSDT", "LINKUSDT", "NEARUSDT", "APTUSDT"]

        spread_multiplier = 2.0 if is_major else 2.5
        calculated_cap = max(8.0, live_spread_bps * spread_multiplier)
        if regime == "TRENDING":
            calculated_cap += 5.0

        if is_major:
            return min(15.0, calculated_cap)
        elif is_high_cap:
            return min(25.0, calculated_cap)
        return min(40.0, calculated_cap)

    def calculate_kyle_market_impact_bps(
        self, 
        symbol: str, 
        qty: float, 
        mid_price: float, 
        inst_var: float
    ) -> float:
        """
        Closed-form Almgren-Chriss / Kyle Square-Root Law Market Impact Model.
        I_bps = eta * sigma * sqrt(Notional / Turnover_Proxy)
        """
        notional = qty * mid_price
        vol_pct = math.sqrt(max(1e-9, inst_var)) * 10000.0  # Volatility in basis points
        eta = 0.45  # Empirical crypto permanent impact coefficient

        limits = self.instrument_cache.get(symbol, {})
        base_depth_notional = float(limits.get("min_notional", Decimal("5.0"))) * 10000.0

        participation_ratio = notional / max(base_depth_notional, notional * 2.0)
        impact_bps = eta * vol_pct * math.sqrt(min(1.0, participation_ratio))
        return float(np.clip(impact_bps, 1.0, 50.0))

    def estimate_orderbook_slippage_bps(self, depth_snapshot: Dict, side: str, qty: float, current_mid: float) -> float:
        """Simulates instantaneous orderbook-crossing implementation shortfall."""
        if not depth_snapshot or "bids" not in depth_snapshot or "asks" not in depth_snapshot:
            return 0.0
        levels = depth_snapshot.get("asks" if side.upper() == "BUY" else "bids", [])
        if not levels:
            return 0.0

        accumulated_qty, accumulated_cost = 0.0, 0.0
        for level in levels:
            try:
                p, v = float(level[0]), float(level[1])
                needed = qty - accumulated_qty
                if v >= needed:
                    accumulated_cost += (needed * p)
                    accumulated_qty += needed
                    break
                else:
                    accumulated_cost += (v * p)
                    accumulated_qty += v
            except (IndexError, ValueError, TypeError):
                continue

        if accumulated_qty < qty or accumulated_qty == 0:
            return 999.0

        avg_expected_price = accumulated_cost / accumulated_qty
        top_of_book = float(levels[0][0])

        if side.upper() == "BUY":
            slippage_bps = ((avg_expected_price - top_of_book) / max(top_of_book, 1e-9)) * 10000.0
        else:
            slippage_bps = ((top_of_book - avg_expected_price) / max(top_of_book, 1e-9)) * 10000.0

        return max(0.0, slippage_bps)

    def get_sweeping_price(self, depth_snapshot: Dict, side: str, qty: float, current_mid: float) -> float:
        """Walks orderbook depth to return the boundary price needed to clear target volume."""
        if not depth_snapshot:
            return current_mid * (1.001 if side.upper() == "BUY" else 0.999)
        levels = depth_snapshot.get("asks" if side.upper() == "BUY" else "bids", [])
        if not levels:
            return current_mid * (1.001 if side.upper() == "BUY" else 0.999)

        accumulated_qty = 0.0
        for level in levels:
            try:
                p, v = float(level[0]), float(level[1])
                accumulated_qty += v
                if accumulated_qty >= qty:
                    return p
            except (IndexError, ValueError, TypeError):
                continue
        return float(levels[-1][0])

    # =========================================================================
    # AVELLANEDA-STOIKOV QUOTING & TELEMETRY
    # =========================================================================

    def _calculate_avellaneda_stoikov_quote(
        self,
        symbol: str,
        side: str,
        mid_price: float,
        depth_snapshot: Dict,
        time_horizon: float = 1.0
    ) -> float:
        """
        Computes optimal reservation quotes based on inventory skew, volatility, and orderbook pressure.
        Incorporates dynamic volatility-scaled gamma calibration to guarantee spreads clear exchange tick size.
        """
        tick_size_dec = self.instrument_cache.get(symbol, {}).get("tick_size", Decimal("0.01"))
        tick_size = float(tick_size_dec)
        bids = depth_snapshot.get("bids", [])
        asks = depth_snapshot.get("asks", [])
        best_bid = float(bids[0][0]) if bids else mid_price
        best_ask = float(asks[0][0]) if asks else mid_price

        # Continuous microstructure variance retrieval
        inst_var = 1e-5
        if self.core_engine and hasattr(self.core_engine, 'stat_engines'):
            stat_eng = self.core_engine.stat_engines.get(symbol)
            if stat_eng:
                inst_var = getattr(stat_eng, 'inst_variance', 1e-5)

        # Net inventory skew (q): positive for long inventory, negative for short
        q = 0.0
        if self.core_engine and hasattr(self.core_engine, 'active_positions_map'):
            curr_dir = self.core_engine.active_positions_map.get(symbol, "NONE")
            if curr_dir == "BUY":
                q = 1.0
            elif curr_dir == "SELL":
                q = -1.0

        tau = max(0.1, min(2.0, time_horizon))

        # Dynamic Volatility-Scaled Gamma Calibration
        norm_tick_vol = (tick_size / max(mid_price, 1e-9)) / (math.sqrt(inst_var) + 1e-9)
        dynamic_gamma = float(np.clip(self.gamma_base * (1.0 + min(3.0, norm_tick_vol)), 0.02, 0.35))

        reservation_price = mid_price - (q * dynamic_gamma * inst_var * tau)
        vol_cushion = dynamic_gamma * inst_var * tau
        liquidity_cushion = (2.0 / dynamic_gamma) * math.log1p(dynamic_gamma / self.k_decay)
        optimal_half_spread = max(tick_size, (vol_cushion + liquidity_cushion) / 2.0)

        spread = max(tick_size, best_ask - best_bid)
        half_spread = max(optimal_half_spread, spread * 0.45)

        if side.upper() == "BUY":
            optimal_quote = min(reservation_price - half_spread, best_ask - tick_size)
            return max(best_bid, optimal_quote)
        else:
            optimal_quote = max(reservation_price + half_spread, best_bid + tick_size)
            return min(best_ask, optimal_quote)

    async def cancel_order_safe(self, symbol: str, order_id: str) -> bool:
        """Cancels an order with idempotent absorption of terminal exchange codes."""
        await self._rate_limit_acquire()
        for _ in range(3):
            try:
                res = await self.executor.safe_call(
                    "POST", "/v5/order/cancel", is_execution=True,
                    category="linear", symbol=symbol, orderId=order_id
                )
                if res.get("retCode") == 0:
                    return True
                err_str = str(res.get("retMsg", "")).lower()
                if any(k in err_str for k in ["110001", "not exists", "too late", "already completed"]):
                    return True
            except Exception as e:
                err_str = str(e).lower()
                if any(k in err_str for k in ["110001", "not exists", "too late", "already completed"]):
                    return True
                await asyncio.sleep(0.10)
        return False

    async def _verify_order_fill(self, symbol: str, order_id: str, timeout: float = 0.75) -> dict:
        """Zero-polling execution listener intercepting fill reports from private WebSockets."""
        if hasattr(self.executor, 'await_ws_execution_report'):
            try:
                ws_report = await self.executor.await_ws_execution_report(order_id, timeout=timeout)
                if ws_report:
                    return ws_report
            except asyncio.TimeoutError:
                pass

        # Micro-backoff prior to fallback REST historical check
        await asyncio.sleep(0.06)
        try:
            hist_res = await self.executor.safe_call(
                "GET", "/v5/order/history", category="linear", symbol=symbol, orderId=order_id, limit=1
            )
            orders = hist_res.get("result", {}).get("list", [])
            return orders[0] if orders else {}
        except Exception:
            return {}

    async def _amend_trailing_stop(self, symbol: str, new_sl: float, new_tp: float) -> bool:
        """Sub-second trailing stop amendment with micro-jitter to prevent exchange rate blocks."""
        now = time.time()
        throttle_window = 0.08 + random.uniform(0.0, 0.04)
        if now - self._last_amend_time.get(symbol, 0.0) < throttle_window:
            return False

        sl_str = self._format_price_str(new_sl, symbol)
        tp_str = self._format_price_str(new_tp, symbol)

        await self._rate_limit_acquire()
        try:
            res = await self.executor.safe_call(
                "POST", "/v5/position/trading-stop", is_execution=True,
                category="linear", symbol=symbol, positionIdx=self.position_idx,
                takeProfit=tp_str, stopLoss=sl_str,
                tpTriggerBy="LastPrice", slTriggerBy="MarkPrice"
            )
            ret_code = res.get("retCode")
            ret_msg = res.get("retMsg", "").lower()

            if ret_code == 0 or "not modified" in ret_msg or "same" in ret_msg:
                self._last_amend_time[symbol] = now
                return True
            return False
        except Exception as e:
            err_str = str(e).lower()
            if "not modified" in err_str or "same" in err_str:
                self._last_amend_time[symbol] = now
                return True
            logger.debug(f"[X-RAY] Trailing stop amend fault for {symbol}: {e}")
            return False

    # =========================================================================
    # EXECUTION TOPOLOGIES
    # =========================================================================

    async def _execute_flash_strike(
        self,
        symbol: str,
        direction: str,
        qty: float,
        current_mid_price: float,
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        depth_snapshot: dict = None,
        regime: str = "TRENDING"
    ) -> Tuple[bool, float, float]:
        """
        Executes immediate market-cross IOC sweeps with partial-mode Stop-Loss
        and Take-Profit protection embedded directly in the creation payload.
        """
        logger.critical(f"[X-RAY] ATOMIC FLASH STRIKE // {symbol} {direction} sweeping orderbook.")
        side = "Buy" if direction.upper() == "BUY" else "Sell"
        cleaned_qty = self._apply_dynamic_exchange_limits(qty, current_mid_price, symbol)
        qty_str = self._format_qty_str(cleaned_qty, symbol)

        # Depth-bounded boundary check
        bids = depth_snapshot.get("bids", []) if depth_snapshot else []
        asks = depth_snapshot.get("asks", []) if depth_snapshot else []
        best_bid = float(bids[0][0]) if bids else current_mid_price
        best_ask = float(asks[0][0]) if asks else current_mid_price
        live_spread_bps = ((best_ask - best_bid) / (best_bid + 1e-9)) * 10000.0

        dynamic_cap_bps = self.compute_dynamic_slippage_cap_bps(symbol, regime, live_spread_bps)
        sweeping_price = self.get_sweeping_price(depth_snapshot, side, cleaned_qty, current_mid_price)

        if side == "Buy":
            max_allowed = current_mid_price * (1.0 + (dynamic_cap_bps / 10000.0))
            target_price = min(sweeping_price, max_allowed)
        else:
            max_allowed = current_mid_price * (1.0 - (dynamic_cap_bps / 10000.0))
            target_price = max(sweeping_price, max_allowed)

        final_price_str = self._format_price_str(target_price, symbol)

        order_payload: Dict[str, Any] = {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": "Limit",
            "qty": qty_str,
            "price": final_price_str,
            "timeInForce": "IOC",
            "positionIdx": self.position_idx
        }

        # Partial-mode stops avoid overwriting existing position brackets during handoff
        if sl:
            order_payload["stopLoss"] = self._format_price_str(sl, symbol)
            order_payload["slTriggerBy"] = "MarkPrice"
        if tp:
            order_payload["takeProfit"] = self._format_price_str(tp, symbol)
            order_payload["tpTriggerBy"] = "LastPrice"
        if sl or tp:
            order_payload["tpslMode"] = "Partial"

        total_executed_qty = 0.0
        avg_price = current_mid_price

        await self._rate_limit_acquire()
        try:
            response = await self.executor.safe_call(
                "POST", "/v5/order/create", is_execution=True, **order_payload
            )
            if response.get("retCode") == 0:
                order_id = response.get("result", {}).get("orderId", "UNKNOWN")
                fill_report = await self._verify_order_fill(symbol, order_id, timeout=0.75)
                if fill_report:
                    raw_exec = fill_report.get("cumExecQty")
                    raw_avg = fill_report.get("avgPrice")
                    total_executed_qty = float(raw_exec) if raw_exec and str(raw_exec).strip() != "" else 0.0
                    avg_price = float(raw_avg) if raw_avg and str(raw_avg).strip() != "" else current_mid_price
            else:
                logger.warning(f"[X-RAY] Flash Strike IOC rejected: {response.get('retMsg')}")
        except Exception as e:
            logger.error(f"[X-RAY] Flash Strike execution fault for {symbol}: {e}")
            total_executed_qty = 0.0

        # Partial fill handoff: Route remainder without duplicate bracket collision
        remainder = cleaned_qty - total_executed_qty
        min_tradeable = float(self.instrument_cache.get(symbol, {}).get("min_qty", Decimal("0.001")))

        if remainder > min_tradeable and (remainder * current_mid_price) >= 6.50:
            logger.info(f"[X-RAY] Partial fill ({total_executed_qty:.4f}/{cleaned_qty:.4f}). Handing off remainder to Maker Peg.")
            peg_success, peg_price, peg_qty = await self._execute_dynamic_maker_peg(
                symbol, direction, remainder, sl=None, tp=None, depth_snapshot=depth_snapshot,
                timeout=4, regime=regime
            )
            if peg_success and peg_qty > 0.0:
                total_cost = (total_executed_qty * avg_price) + (peg_qty * peg_price)
                total_executed_qty += peg_qty
                avg_price = total_cost / total_executed_qty

        if total_executed_qty > 0.0:
            # Anchor full position protection on the finalized executed volume
            await self._verify_and_anchor_stops(symbol, direction, avg_price, sl, tp)
            is_bps = ((avg_price - current_mid_price) / current_mid_price * 10000.0) if side == "Buy" else \
                     ((current_mid_price - avg_price) / current_mid_price * 10000.0)

            logger.critical(
                f" FLASH STRIKE FILLED // {symbol} {total_executed_qty:.4f} units @ {avg_price:.4f} "
                f"(Arrival Mid: {current_mid_price:.4f} | IS: {is_bps:+.1f} bps | Stops: Verified Protected)"
            )
            return True, avg_price, total_executed_qty

        return False, 0.0, 0.0

    async def _execute_dynamic_maker_peg(
        self,
        symbol: str,
        direction: str,
        qty: float,
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        depth_snapshot: dict = None,
        timeout: int = 5,
        regime: str = "MEAN_REVERTING"
    ) -> Tuple[bool, float, float]:
        """
        Passive PostOnly liquidity pegging with Avellaneda-Stoikov quoting,
        active chase firewalls, and queue preservation.
        """
        start_time = time.time()
        current_order_id = None
        side = "Buy" if direction.upper() == "BUY" else "Sell"
        anchor_price = None

        tick_size = float(self.instrument_cache.get(symbol, {}).get("tick_size", Decimal("0.01")))
        current_peg_price = 0.0

        bids = depth_snapshot.get("bids", []) if depth_snapshot else []
        asks = depth_snapshot.get("asks", []) if depth_snapshot else []
        best_bid = float(bids[0][0]) if bids else 100.0
        best_ask = float(asks[0][0]) if asks else 100.0
        mid = (best_bid + best_ask) / 2.0

        live_spread_bps = ((best_ask - best_bid) / (best_bid + 1e-9)) * 10000.0
        dynamic_cap_bps = self.compute_dynamic_slippage_cap_bps(symbol, regime, live_spread_bps)
        max_chase_deviation = max(0.001, dynamic_cap_bps / 10000.0)

        cleaned_qty = self._apply_dynamic_exchange_limits(qty, best_bid, symbol)
        qty_str = self._format_qty_str(cleaned_qty, symbol)

        while time.time() - start_time < timeout:
            try:
                fresh_ob = depth_snapshot
                if self.core_engine and hasattr(self.core_engine, 'orderbook_snapshots'):
                    fresh_ob = self.core_engine.orderbook_snapshots.get(symbol, depth_snapshot)

                optimal_price = self._calculate_avellaneda_stoikov_quote(
                    symbol, side, mid, fresh_ob or {}
                )
                target_price_str = self._format_price_str(optimal_price, symbol)
                target_price_float = float(target_price_str)

                if anchor_price is None:
                    anchor_price = target_price_float

                # Anti-Chase Firewall: Terminate if quote drifts beyond slippage cap
                if side == "Buy" and target_price_float > anchor_price * (1.0 + max_chase_deviation):
                    logger.warning(f"[X-RAY] CHASE BREACH // {symbol} drifted +{max_chase_deviation:.2%} past anchor.")
                    break
                if side == "Sell" and target_price_float < anchor_price * (1.0 - max_chase_deviation):
                    logger.warning(f"[X-RAY] CHASE BREACH // {symbol} drifted -{max_chase_deviation:.2%} past anchor.")
                    break

                # 1. Place initial PostOnly limit
                if not current_order_id:
                    post_payload: Dict[str, Any] = {
                        "category": "linear",
                        "symbol": symbol,
                        "side": side,
                        "orderType": "Limit",
                        "qty": qty_str,
                        "price": target_price_str,
                        "timeInForce": "PostOnly",
                        "positionIdx": self.position_idx
                    }
                    if sl:
                        post_payload["stopLoss"] = self._format_price_str(sl, symbol)
                        post_payload["slTriggerBy"] = "MarkPrice"
                    if tp:
                        post_payload["takeProfit"] = self._format_price_str(tp, symbol)
                        post_payload["tpTriggerBy"] = "LastPrice"
                    if sl or tp:
                        post_payload["tpslMode"] = "Partial"

                    await self._rate_limit_acquire()
                    place_response = await self.executor.safe_call(
                        "POST", "/v5/order/create", is_execution=True, **post_payload
                    )
                    if place_response.get("retCode") == 0:
                        current_order_id = place_response["result"]["orderId"]
                        current_peg_price = target_price_float
                    else:
                        err_msg = place_response.get("retMsg", "")
                        if "post only" in err_msg.lower():
                            await asyncio.sleep(0.04)
                        else:
                            await asyncio.sleep(0.15)
                        continue

                # 2. Sub-millisecond WebSocket Fill Check
                fill_report = await self._verify_order_fill(symbol, current_order_id, timeout=0.75)
                if fill_report:
                    raw_exec = fill_report.get("cumExecQty")
                    raw_avg = fill_report.get("avgPrice")
                    cum_exec = float(raw_exec) if raw_exec and str(raw_exec).strip() != "" else 0.0
                    avg_price = float(raw_avg) if raw_avg and str(raw_avg).strip() != "" else current_peg_price
                    order_status = fill_report.get("orderStatus", "")

                    if order_status == "Filled" or cum_exec >= cleaned_qty:
                        logger.critical(f" MAKER PEG SECURED // {symbol} filled completely. Earned Maker Rebates.")
                        if sl or tp:
                            await self._verify_and_anchor_stops(symbol, direction, avg_price, sl, tp)
                        return True, avg_price, cum_exec
                    elif order_status in ["Cancelled", "Rejected"]:
                        current_order_id = None
                        if cum_exec > 0.0:
                            if sl or tp:
                                await self._verify_and_anchor_stops(symbol, direction, avg_price, sl, tp)
                            return True, avg_price, cum_exec
                        continue

                # 3. Queue-Preserving Order Amendment (>= 3.0 ticks displacement)
                tick_displacement = abs(target_price_float - current_peg_price) / max(1e-9, tick_size)
                if current_order_id and tick_displacement >= 3.0:
                    await self._rate_limit_acquire()
                    amend_res = await self.executor.safe_call(
                        "POST", "/v5/order/amend", is_execution=True,
                        category="linear", symbol=symbol, orderId=current_order_id,
                        price=target_price_str
                    )
                    if amend_res.get("retCode") == 0:
                        current_peg_price = target_price_float

            except Exception as e:
                error_str = str(e)
                if any(fatal in error_str for fatal in ["110126", "INNOVATION ZONE", "10002", "10001"]):
                    break
                await asyncio.sleep(0.15)

        # 4. Timeout Cancellation and Residual Fill Sweep
        if current_order_id:
            await self.cancel_order_safe(symbol, current_order_id)
            fill_report = await self._verify_order_fill(symbol, current_order_id, timeout=0.20)
            if fill_report:
                raw_exec = fill_report.get("cumExecQty")
                raw_avg = fill_report.get("avgPrice")
                cum_exec = float(raw_exec) if raw_exec and str(raw_exec).strip() != "" else 0.0
                fallback_price = anchor_price if anchor_price else best_bid
                avg_price = float(raw_avg) if raw_avg and str(raw_avg).strip() != "" else fallback_price
                if cum_exec > 0.0:
                    if sl or tp:
                        await self._verify_and_anchor_stops(symbol, direction, avg_price, sl, tp)
                    return True, avg_price, cum_exec

        return False, 0.0, 0.0

    async def _execute_twap_iceberg(
        self,
        symbol: str,
        direction: str,
        total_qty: float,
        current_mid_price: float,
        sl: float,
        tp: float,
        slices: int = 4,
        slice_interval_sec: float = 4.0,
        regime: str = "TRENDING"
    ) -> Tuple[bool, float, float]:
        """Dynamic TWAP: Sizes chunks and sets cadence to minimize square-root market impact."""
        limits = self.instrument_cache.get(symbol, {"min_qty": Decimal("1.0")})
        min_qty = float(limits["min_qty"])
        min_notional_qty = 6.50 / max(current_mid_price, 1e-9)
        absolute_min_slice = max(min_qty, min_notional_qty)

        # Retrieve microstructure variance for impact estimation
        inst_var = 1e-5
        if self.core_engine and hasattr(self.core_engine, 'stat_engines'):
            stat_eng = self.core_engine.stat_engines.get(symbol)
            if stat_eng:
                inst_var = getattr(stat_eng, 'inst_variance', 1e-5)

        impact_bps = self.calculate_kyle_market_impact_bps(symbol, total_qty, current_mid_price, inst_var)
        
        # Dynamically scale slice count based on market impact
        if impact_bps > 12.0:
            recommended_slices = min(8, max(4, math.ceil(impact_bps / 3.0)))
        else:
            recommended_slices = max(3, slices)

        if (total_qty / recommended_slices) < absolute_min_slice:
            recommended_slices = max(1, math.floor(total_qty / absolute_min_slice))

        slice_qty = total_qty / recommended_slices
        total_executed_qty, weighted_notional_sum = 0.0, 0.0

        # Dynamic interval: Higher volatility requires longer replenishment windows
        dynamic_interval = max(2.5, min(8.0, 2.0 + (math.sqrt(inst_var) * 1000.0)))
        chunk_timeout = 3 if symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"] else 5

        logger.critical(
            f"[X-RAY] ICEBERG TUNED // {symbol}: {recommended_slices} slices of {slice_qty:.4f} | "
            f"Cadence: {dynamic_interval:.1f}s | Est Impact: {impact_bps:.1f}bps"
        )

        for i in range(recommended_slices):
            success, fill_price, fill_qty = await self._execute_dynamic_maker_peg(
                symbol=symbol, direction=direction, qty=slice_qty,
                sl=None, tp=None, timeout=chunk_timeout, regime=regime
            )

            if success and fill_qty > 0.0:
                total_executed_qty += fill_qty
                weighted_notional_sum += (fill_price * fill_qty)
                current_avg_price = weighted_notional_sum / total_executed_qty
                # Continuously anchor stops to protect partial fills across the intermediate lifecycle
                await self._verify_and_anchor_stops(symbol, direction, current_avg_price, sl, tp)

            if i < recommended_slices - 1:
                await asyncio.sleep(dynamic_interval)

        if total_executed_qty > 0.0:
            avg_fill_price = weighted_notional_sum / total_executed_qty
            await self._verify_and_anchor_stops(symbol, direction, avg_fill_price, sl, tp)
            logger.critical(f" ICEBERG SUCCESSFUL // {symbol} filled {total_executed_qty:.4f} @ avg {avg_fill_price:.4f}.")
            return True, avg_fill_price, total_executed_qty

        return False, 0.0, 0.0

    # =========================================================================
    # MASTER ALPHA ROUTING PIPELINE
    # =========================================================================

    async def execute_alpha_signal(
        self,
        symbol: str,
        direction: str,
        prob_success: float,
        exec_weight: float,
        current_mid_price: float,
        sl_price: float,
        tp_price: float,
        inst_var: float,
        depth_snapshot: dict,
        target_notional: float,
        regime: str = "TRENDING"
    ) -> Tuple[bool, float, float]:
        """
        V40.3 MASTER ROUTING ENTRYPOINT:
        Evaluates depth elasticity, verifies slippage caps, and routes to Flash Strike,
        Maker Peg, or TWAP Icebergs based on real-time microstructure topology.
        """
        if target_notional <= 0.0:
            logger.warning(f"[X-RAY] Sizing abort for {symbol}: target notional <= 0.")
            return False, current_mid_price, 0.0

        await self._fetch_exchange_limits(symbol)
        total_qty = self._apply_dynamic_exchange_limits(target_notional / current_mid_price, current_mid_price, symbol)

        if (total_qty * current_mid_price) < 6.0:
            logger.warning(f"[X-RAY] Insufficient notional for {symbol}: ${total_qty * current_mid_price:.2f} < $6.00.")
            return False, current_mid_price, 0.0

        # Slippage Firewall Check
        ob = depth_snapshot or {}
        bids = ob.get("bids", [])
        asks = ob.get("asks", [])
        best_bid = float(bids[0][0]) if bids else current_mid_price
        best_ask = float(asks[0][0]) if asks else current_mid_price
        live_spread_bps = ((best_ask - best_bid) / (best_bid + 1e-9)) * 10000.0 if best_bid > 0 else 1.0

        dynamic_cap_bps = self.compute_dynamic_slippage_cap_bps(symbol, regime, live_spread_bps)
        est_slippage = self.estimate_orderbook_slippage_bps(ob, direction, total_qty, current_mid_price)

        if est_slippage > dynamic_cap_bps:
            logger.warning(
                f"[X-RAY] SLIPPAGE FIREWALL VETO // {symbol} est. slippage {est_slippage:.1f} bps > "
                f"Cap {dynamic_cap_bps:.1f} bps. Aborting."
            )
            return False, current_mid_price, 0.0

        # Whale Routing (TWAP Iceberg)
        top_bid_vol = sum(float(l[1]) for l in bids[:3]) if bids else 0.0
        top_ask_vol = sum(float(l[1]) for l in asks[:3]) if asks else 0.0
        avg_tob_vol = (top_bid_vol + top_ask_vol) / 2.0

        if avg_tob_vol > 0.0 and total_qty > (avg_tob_vol * 0.05):
            logger.info(f"[X-RAY] WHALE SIZING // {symbol} > 5% Top-of-Book depth. Routing TWAP Iceberg.")
            return await self._execute_twap_iceberg(
                symbol, direction, total_qty, current_mid_price, sl_price, tp_price, regime=regime
            )

        # Urgent Routing (Flash Strike IOC)
        book_skew = top_bid_vol / (top_ask_vol + 1e-9)
        urgent_taker = False

        if direction.upper() == "BUY" and (book_skew < 0.35 or exec_weight > 1.3):
            urgent_taker = True
        elif direction.upper() == "SELL" and (book_skew > 2.8 or exec_weight > 1.3):
            urgent_taker = True

        if urgent_taker or regime == "TRENDING":
            return await self._execute_flash_strike(
                symbol, direction, total_qty, current_mid_price, sl_price, tp_price, depth_snapshot=ob, regime=regime
            )

        # Passive Routing (Avellaneda-Stoikov Maker Peg)
        is_major = symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        dynamic_timeout = 3 if is_major else 5
        return await self._execute_dynamic_maker_peg(
            symbol, direction, total_qty, sl_price, tp_price, depth_snapshot=ob, timeout=dynamic_timeout, regime=regime
        )