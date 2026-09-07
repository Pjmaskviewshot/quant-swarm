"""
V39.2 APEX TITAN: ATOMIC DUAL-LEG BASIS & YIELD HARVESTER
------------------------------------------------------------------------
Ultra-low latency delta-neutral basis cash-and-carry execution engine.
Sweeps idle margin into high-rate funding arbitrage with full multiplier 
normalization, cross-instrument lot step harmonization, and atomic rollback.

Architectural Supremacy (V39.2 Production Fixes):
- Multi-Candidate Waterfall Scan: Eradicates the single-candidate greedy trap 
  where unborrowable or zero-collateral tokens (e.g. KOUSDT) locked out the 
  entire scanner. Iteratively tests top-ranking funding pairs until securing a viable node.
- Micro-Account Capital Scaling: Dynamically scales idle capital requirements 
  and allocation thresholds for sub-$100 bankrolls, enabling accounts with 
  $15–$50 equity to deploy into $6.50 minimum notionals without capital exhaustion.
- Maker-Taker Hybrid Routing: Eliminates dual-market-order taker fee bleed by 
  posting passive PostOnly limit orders on the Spot leg first, firing the 
  Perpetual Short IOC hedge only upon verified Spot fill.
- Strict Single-Epoch Hurdle: Enforces epochs_to_breakeven <= 1 (<= 8 hours) to 
  prevent fee drag entrapment when altcoin funding rates rapidly mean-revert.
- Dynamic UTA Collateral Haircut Guard: Enforces minimum 70% collateral valuation 
  ratio on base assets to prevent account liquidation via collateral discounting.
- Fee-Compensated Unwind: Queries exact post-fill base coin balance before spot sale 
  to eradicate Bybit errors 170131/10001 (insufficient spot balance).
"""

import re
import math
import uuid
import asyncio
import logging
import time
import numpy as np
from collections import deque
from typing import Dict, Any, List, Tuple, Optional
from decimal import Decimal, ROUND_FLOOR

logger = logging.getLogger("QUANT_CORE.DELTA_NEUTRAL")


class DeltaNeutralYieldEngine:
    """
    V39.2 APEX TITAN BASIS ENGINE
    Captures perpetual funding rate premiums via synchronized Spot Long / Perp Short
    atomic pairing with zero residual directional exposure.
    """
    def __init__(self, core_engine):
        self.core = core_engine

        # Minimum funding rate to initiate cash-and-carry (0.12% per 8h = ~131.4% APY)
        self.entry_funding_threshold = 0.0012

        # Unwind threshold: Exit when funding decays below 0.015% per 8h (~16.4% APY)
        self.exit_funding_threshold = 0.00015

        # Minimum UTA collateral ratio required for spot asset (70%)
        self.min_collateral_ratio = 0.70

        self.active_hedges: Dict[str, dict] = {}
        self.basis_history: Dict[str, deque] = {}
        self.instrument_cache: Dict[str, dict] = {}
        self.collateral_ratio_cache: Dict[str, Tuple[float, float]] = {}

        # Default Bybit VIP0 Linear & Spot Fee Schedules
        self.maker_fee_rate = 0.00020
        self.taker_fee_rate = 0.00055

    # =========================================================================
    # CONTRACT MULTIPLIER & INSTRUMENT RESOLUTION
    # =========================================================================

    def _resolve_contract_scale(self, linear_symbol: str) -> Tuple[str, str, float]:
        """
        Parses Bybit Linear multiplier prefixes (e.g., 1000PEPEUSDT -> PEPEUSDT, 1000.0x).
        Returns: (spot_symbol, base_asset, contract_multiplier)
        """
        match = re.match(r"^(\d+)?([A-Z0-9]+)USDT$", linear_symbol.upper())
        if not match:
            base = linear_symbol[:-4] if linear_symbol.endswith("USDT") else linear_symbol
            return f"{base}USDT", base, 1.0

        multiplier_str, base_asset = match.groups()
        multiplier = float(multiplier_str) if multiplier_str else 1.0
        return f"{base_asset}USDT", base_asset, multiplier

    async def _fetch_instrument_specs(self, symbol: str, category: str) -> Optional[Dict[str, Any]]:
        """Caches and validates lot size filters and tick specifications."""
        cache_key = f"{category}_{symbol}"
        if cache_key in self.instrument_cache:
            return self.instrument_cache[cache_key]

        try:
            res = await self.core.executor.safe_call(
                "GET", "/v5/market/instruments-info", category=category, symbol=symbol
            )
            data_list = res.get("result", {}).get("list", [])
            if not data_list:
                return None

            info = data_list[0]
            lot_filter = info.get("lotSizeFilter", {})
            price_filter = info.get("priceFilter", {})

            if category == "spot":
                specs = {
                    "base_precision": float(lot_filter.get("basePrecision", 0.0001)),
                    "min_order_qty": float(lot_filter.get("minOrderQty", 0.0001)),
                    "min_order_amt": float(lot_filter.get("minOrderAmt", 5.0)),
                    "tick_size": float(price_filter.get("tickSize", 0.01))
                }
            else:
                specs = {
                    "qty_step": float(lot_filter.get("qtyStep", 0.001)),
                    "min_order_qty": float(lot_filter.get("minOrderQty", 0.001)),
                    "min_notional": float(lot_filter.get("minNotionalValue", 5.0)),
                    "tick_size": float(price_filter.get("tickSize", 0.01))
                }

            self.instrument_cache[cache_key] = specs
            return specs
        except Exception as e:
            logger.debug(f"[YIELD_SENTRY] Spec fetch failed for {category} {symbol}: {e}")
            return None

    async def _fetch_collateral_ratio(self, base_asset: str) -> float:
        """
        Queries Bybit UTA collateral ratio for the underlying base asset.
        Caches results for 1 hour to prevent REST rate limit exhaustion.
        """
        now = time.time()
        if base_asset in self.collateral_ratio_cache:
            cached_time, ratio = self.collateral_ratio_cache[base_asset]
            if now - cached_time < 3600.0:
                return ratio

        try:
            res = await self.core.executor.safe_call(
                "GET", "/v5/account/collateral-info", currency=base_asset
            )
            data_list = res.get("result", {}).get("list", [])
            if data_list:
                ratio = float(data_list[0].get("collateralRatio", 0.0) or 0.0)
                self.collateral_ratio_cache[base_asset] = (now, ratio)
                return ratio
        except Exception as e:
            logger.debug(f"[YIELD_SENTRY] Collateral ratio probe failed for {base_asset}: {e}")

        # Conservative fallback: Assume unrated assets carry zero collateral value
        return 0.0

    def _quantize_value(self, value: float, step: float) -> str:
        """Strict floor-quantization using Decimal arithmetic."""
        if step <= 0:
            return f"{value:.4f}"
        step_dec = Decimal(str(step))
        val_dec = Decimal(str(value))
        quantized = (val_dec // step_dec) * step_dec
        precision = max(0, abs(int(round(math.log10(step))))) if step < 1 else 0
        return f"{quantized:.{precision}f}"

    async def _calculate_harmonized_quantities(
        self,
        linear_symbol: str,
        spot_symbol: str,
        multiplier: float,
        target_notional: float,
        spot_price: float
    ) -> Optional[Tuple[str, str, float, float]]:
        """
        Calculates exact synchronized Spot and Linear quantities.
        Prevents exchange error 10001 and guarantees zero unhedged contract remainder.
        """
        spot_specs = await self._fetch_instrument_specs(spot_symbol, "spot")
        perp_specs = await self._fetch_instrument_specs(linear_symbol, "linear")

        if not spot_specs or not perp_specs:
            return None

        spot_step = spot_specs["base_precision"]
        perp_step = perp_specs["qty_step"]

        # Calculate raw perp contracts required
        raw_base_tokens = target_notional / max(spot_price, 1e-9)
        raw_perp_contracts = raw_base_tokens / multiplier

        # Synchronize contract step floor
        perp_contracts = math.floor(raw_perp_contracts / perp_step) * perp_step
        if perp_contracts < perp_specs["min_order_qty"]:
            return None

        # Spot base tokens strictly equal to: perp_contracts * multiplier
        required_spot_tokens = perp_contracts * multiplier
        spot_tokens = math.floor(required_spot_tokens / spot_step) * spot_step

        # Backward validation check for multiplier parity
        final_perp_contracts = spot_tokens / multiplier
        if final_perp_contracts < perp_specs["min_order_qty"]:
            return None

        spot_notional = spot_tokens * spot_price
        if spot_notional < max(spot_specs["min_order_amt"], 5.0):
            return None

        spot_qty_str = self._quantize_value(spot_tokens, spot_step)
        perp_qty_str = self._quantize_value(final_perp_contracts, perp_step)

        return spot_qty_str, perp_qty_str, float(spot_tokens), float(final_perp_contracts)

    # =========================================================================
    # ECONOMIC VIABILITY & BASIS DISLOCATION SENTRY
    # =========================================================================

    def _check_basis_dislocation(self, spot_price: float, perp_price: float, symbol: str) -> bool:
        """Statistical guard: Aborts if Perp-Spot premium decouples beyond 2.5 sigma."""
        if symbol not in self.basis_history:
            self.basis_history[symbol] = deque(maxlen=200)

        current_basis_bps = ((perp_price - spot_price) / spot_price) * 10000.0
        self.basis_history[symbol].append(current_basis_bps)

        if len(self.basis_history[symbol]) < 30:
            return False

        arr = np.array(self.basis_history[symbol])
        basis_z = abs(current_basis_bps - np.mean(arr)) / (np.std(arr) + 1e-9)

        if basis_z > 2.5:
            logger.warning(
                f"[YIELD_SENTRY] Structural Basis Dislocation on {symbol} "
                f"(Z: {basis_z:.2f} | Current: {current_basis_bps:.1f} bps). Entry Vetoed."
            )
            return True
        return False

    async def _calculate_execution_drag(self, perp_symbol: str, spot_symbol: str) -> Tuple[float, float, float]:
        """
        Calculates realistic Maker-Taker implementation drag:
        - Spot Entry: Maker Rebate/Fee (0.02%)
        - Perp Entry: Taker Fee (0.055%)
        - Spot/Perp Unwind: Conservative Taker (0.055% x 2)
        """
        try:
            spot_task = self.core.executor.safe_call("GET", "/v5/market/tickers", category="spot", symbol=spot_symbol)
            perp_task = self.core.executor.safe_call("GET", "/v5/market/tickers", category="linear", symbol=perp_symbol)

            results = await asyncio.gather(spot_task, perp_task, return_exceptions=True)
            if isinstance(results[0], Exception) or isinstance(results[1], Exception):
                return 999.0, 0.0, 0.0

            spot_list = results[0].get("result", {}).get("list", [])
            perp_list = results[1].get("result", {}).get("list", [])

            if not spot_list or not perp_list:
                return 999.0, 0.0, 0.0

            spot_bid = float(spot_list[0].get("bid1Price", 0.0) or 0.0)
            perp_bid = float(perp_list[0].get("bid1Price", 0.0) or 0.0)

            if spot_bid <= 0.0 or perp_bid <= 0.0:
                return 999.0, 0.0, 0.0

            basis_cost_pct = max(0.0, (spot_bid - perp_bid) / spot_bid)
            fee_drag_pct = self.maker_fee_rate + (self.taker_fee_rate * 3.0)
            slippage_buffer_pct = 0.0006

            total_drag_bps = (basis_cost_pct + fee_drag_pct + slippage_buffer_pct) * 10000.0
            return total_drag_bps, spot_bid, perp_bid
        except Exception as e:
            logger.debug(f"[X-RAY] Drag calculation fault for {perp_symbol}: {e}")
            return 999.0, 0.0, 0.0

    def _validate_arbitrage_viability(
        self,
        funding_rate: float,
        drag_bps: float,
        min_net_epochs: int = 3
    ) -> Tuple[bool, str, int, float]:
        """
        Production Economic Hurdle Validator:
        Ensures execution friction is recouped within 1 epoch (8 hours) and net APY >= 25.0%.
        """
        funding_bps_per_epoch = funding_rate * 10000.0
        if funding_bps_per_epoch <= 0.0:
            return False, "NEGATIVE_OR_ZERO_FUNDING", 999, 0.0

        epochs_to_breakeven = math.ceil(drag_bps / max(1e-4, funding_bps_per_epoch))
        if epochs_to_breakeven > 1:
            return (
                False,
                f"EXCESSIVE_DRAG ({drag_bps:.1f} bps requires {epochs_to_breakeven} epochs > 1 limit)",
                epochs_to_breakeven,
                0.0
            )

        net_epoch_yield_bps = funding_bps_per_epoch - (drag_bps / float(min_net_epochs))
        projected_net_apy = (net_epoch_yield_bps / 10000.0) * 3.0 * 365.0 * 100.0

        if projected_net_apy < 25.0:
            return (
                False,
                f"SUB_HURDLE_APY ({projected_net_apy:.1f}% < 25.0% min)",
                epochs_to_breakeven,
                projected_net_apy
            )

        return True, "PASSED_ECONOMIC_HURDLES", epochs_to_breakeven, projected_net_apy

    async def _fetch_free_spot_balance(self, base_asset: str) -> float:
        """Queries true available Spot coin balance for fee-compensated unwinding."""
        try:
            res = await self.core.executor.safe_call(
                "GET", "/v5/account/wallet-balance", accountType="UNIFIED", coin=base_asset
            )
            data_list = res.get("result", {}).get("list", [])
            if data_list:
                coins = data_list[0].get("coin", [])
                for c in coins:
                    if c.get("coin") == base_asset:
                        return float(c.get("availableToWithdraw", c.get("walletBalance", 0.0)) or 0.0)
        except Exception as e:
            logger.debug(f"[YIELD] Failed querying spot balance for {base_asset}: {e}")
        return 0.0

    # =========================================================================
    # CORE EXECUTION & LIFECYCLE
    # =========================================================================

    async def run_yield_scanner_daemon(self):
        """Continuously scans the global universe and rotates capital into basis yield."""
        logger.info("DELTA-NEUTRAL BASIS ENGINE ONLINE: Scanning for Altcoin Funding Yield.")

        while True:
            await asyncio.sleep(180)  # 3-minute scan cycle

            if not self.core.fsm.can_execute_trades:
                continue

            try:
                tickers_res = await self.core.executor.safe_call("GET", "/v5/market/tickers", category="linear")
                if not isinstance(tickers_res, dict) or tickers_res.get("retCode") != 0:
                    continue

                ticker_list = tickers_res.get("result", {}).get("list", [])

                # 1. Evaluate active hedges for yield decay or basis dislocation
                await self._evaluate_active_hedges(ticker_list)

                # 2. Extract and rank all candidates exceeding funding threshold
                candidates = []
                for t in ticker_list:
                    symbol = t.get("symbol", "")
                    if not symbol.endswith("USDT") or "BTC" in symbol or "ETH" in symbol:
                        continue

                    funding_rate = float(t.get("fundingRate", 0.0) or 0.0)
                    if funding_rate >= self.entry_funding_threshold:
                        if symbol not in self.active_hedges and symbol not in self.core.active_positions_map:
                            candidates.append((symbol, funding_rate))

                # Sort descending by funding rate to prioritize highest yields
                candidates.sort(key=lambda x: x[1], reverse=True)

                # 3. Multi-Candidate Waterfall: Probe top candidates in order of yield
                for target_asset, best_funding in candidates[:15]:
                    _, base_asset, _ = self._resolve_contract_scale(target_asset)
                    collateral_ratio = await self._fetch_collateral_ratio(base_asset)
                    if collateral_ratio < self.min_collateral_ratio:
                        logger.warning(
                            f"[YIELD] Skip {target_asset}: Collateral ratio {collateral_ratio:.0%} < "
                            f"{self.min_collateral_ratio:.0%} min required (UTA Discounting Risk)."
                        )
                        continue

                    logger.info(
                        f"[YIELD] Candidate validated: {target_asset} "
                        f"(Funding: {best_funding*10000:.1f} bps, Collateral: {collateral_ratio:.0%}). Attempting entry..."
                    )
                    
                    hedge_secured = await self.execute_atomic_cash_and_carry_hedge(target_asset, best_funding)
                    if hedge_secured:
                        logger.info(f"[YIELD] Basis hedge successfully secured on {target_asset}.")
                        break

            except Exception as e:
                logger.error(f"[X-RAY] Yield Scanner daemon cycle error: {e}", exc_info=False)

    async def _evaluate_active_hedges(self, current_tickers: List[Dict[str, Any]]):
        """Monitors active hedges and triggers unwinds when funding decays."""
        symbols_to_unwind = []
        for active_symbol, hedge_meta in list(self.active_hedges.items()):
            ticker_data = next((t for t in current_tickers if t.get("symbol") == active_symbol), None)
            if not ticker_data:
                continue

            current_funding = float(ticker_data.get("fundingRate", 0.0) or 0.0)
            holding_hours = (time.time() - hedge_meta["timestamp"]) / 3600.0

            if current_funding <= self.exit_funding_threshold:
                logger.warning(
                    f"[X-RAY] YIELD DECAY // {active_symbol} funding dropped to "
                    f"{current_funding * 10000:.1f} bps (Held: {holding_hours:.1f}h). Triggering Unwind."
                )
                symbols_to_unwind.append(active_symbol)

        for sym in symbols_to_unwind:
            await self.unwind_cash_and_carry_hedge(sym)

    async def execute_atomic_cash_and_carry_hedge(self, symbol: str, funding_rate: float) -> bool:
        """
        Executes Maker-Taker Hybrid cash-and-carry routing:
        1. Validates collateral ratio, execution drag, and economic hurdles.
        2. Posts passive Spot Buy order via PostOnly at the best bid.
        3. Waits up to 4 seconds for complete or partial fill.
        4. If unfilled, cancels cleanly with ZERO naked directional exposure.
        5. If filled, executes matching Linear Perpetual Short via Market IOC.
        Returns: True if hedge was deployed successfully, False otherwise.
        """
        spot_symbol, base_asset, multiplier = self._resolve_contract_scale(symbol)

        # 1. UTA Collateral Valuation Guard
        collateral_ratio = await self._fetch_collateral_ratio(base_asset)
        if collateral_ratio < self.min_collateral_ratio:
            return False

        # 2. Execution Drag and Pricing Verification
        drag_bps, spot_bid_price, perp_bid_price = await self._calculate_execution_drag(symbol, spot_symbol)
        if spot_bid_price <= 0.0 or perp_bid_price <= 0.0:
            return False

        if self._check_basis_dislocation(spot_bid_price, perp_bid_price, symbol):
            return False

        # 3. Production Economic Hurdle Test
        viable, reason, epochs_to_be, expected_apy = self._validate_arbitrage_viability(
            funding_rate, drag_bps, min_net_epochs=3
        )
        if not viable:
            logger.info(f"[YIELD] Arbitrage Vetoed for {symbol}: {reason}")
            return False

        total_bal = await self.core.executor.get_wallet_balance_usdt()

        # Calculate active directional margin usage
        active_margin = 0.0
        for s in self.core.active_positions_map.keys():
            try:
                pos_res = await self.core.executor.safe_call("GET", "/v5/position/list", category="linear", symbol=s)
                pos_list = pos_res.get("result", {}).get("list", [])
                if pos_list:
                    active_margin += float(pos_list[0].get("positionValue", 0.0)) / float(pos_list[0].get("leverage", 1.0))
            except Exception:
                pass

        idle_capital = total_bal - active_margin

        # Micro-Account Adaptive Sizing:
        # For accounts < $100, lower required idle floor to $8.00 and allocate up to 45% of balance ($6.50 min)
        min_idle_capital = 8.0 if total_bal < 100.0 else 30.0
        if idle_capital < min_idle_capital:
            logger.debug(f"[YIELD] Insufficient idle capital (${idle_capital:.2f} < ${min_idle_capital:.2f}) for {symbol}.")
            return False

        if total_bal < 100.0:
            yield_capital = min(idle_capital * 0.85, max(7.0, total_bal * 0.45))
            min_yield_threshold = 6.50
        else:
            yield_capital = min(idle_capital * 0.90, total_bal * 0.20)
            min_yield_threshold = 15.0

        if yield_capital < min_yield_threshold:
            return False

        calc_result = await self._calculate_harmonized_quantities(
            symbol, spot_symbol, multiplier, yield_capital, spot_bid_price
        )
        if not calc_result:
            return False

        spot_qty_str, perp_qty_str, spot_units, perp_contracts = calc_result
        spot_specs = await self._fetch_instrument_specs(spot_symbol, "spot")
        perp_specs = await self._fetch_instrument_specs(symbol, "linear")
        if not spot_specs or not perp_specs:
            return False

        spot_price_str = self._quantize_value(spot_bid_price, spot_specs["tick_size"])

        logger.info(
            f"[X-RAY] Initiating Maker-Taker Basis Pairing: "
            f"Spot PostOnly Buy {spot_qty_str} {spot_symbol} @ {spot_price_str} "
            f"(Target Perp: {perp_qty_str} {symbol} | Net APY: {expected_apy:.1f}%)"
        )

        # 4. Phase 1: Submit Passive Spot Maker Leg
        spot_create_res = await self.core.executor.safe_call(
            "POST", "/v5/order/create", is_execution=True,
            category="spot", symbol=spot_symbol, side="Buy",
            orderType="Limit", price=spot_price_str, qty=spot_qty_str,
            timeInForce="PostOnly"
        )

        if spot_create_res.get("retCode") != 0:
            logger.info(f"[YIELD] Spot PostOnly rejected or crossed spread: {spot_create_res.get('retMsg')}")
            return False

        spot_order_id = spot_create_res.get("result", {}).get("orderId")
        if not spot_order_id:
            return False

        # 5. Phase 2: Await Spot Fill (Up to 4.0s timeout)
        filled_spot_qty = 0.0
        avg_spot_fill_price = spot_bid_price
        for _ in range(20):
            await asyncio.sleep(0.20)
            order_info = await self.core.executor.safe_call(
                "GET", "/v5/order/realtime", category="spot", symbol=spot_symbol, orderId=spot_order_id
            )
            order_list = order_info.get("result", {}).get("list", [])
            if order_list:
                o_data = order_list[0]
                status = o_data.get("orderStatus")
                cum_qty = float(o_data.get("cumExecQty", 0.0) or 0.0)
                if status == "Filled" or cum_qty >= spot_units:
                    filled_spot_qty = cum_qty
                    avg_spot_fill_price = float(o_data.get("avgPrice", spot_bid_price) or spot_bid_price)
                    break
                elif status in ["Cancelled", "Rejected"]:
                    filled_spot_qty = cum_qty
                    break

        # Cancel remaining resting spot order if not completely filled
        if filled_spot_qty < spot_units:
            await self.core.executor.safe_call(
                "POST", "/v5/order/cancel", is_execution=True,
                category="spot", symbol=spot_symbol, orderId=spot_order_id
            )
            await asyncio.sleep(0.15)
            post_cancel_info = await self.core.executor.safe_call(
                "GET", "/v5/order/realtime", category="spot", symbol=spot_symbol, orderId=spot_order_id
            )
            post_list = post_cancel_info.get("result", {}).get("list", [])
            if post_list:
                filled_spot_qty = float(post_list[0].get("cumExecQty", filled_spot_qty) or filled_spot_qty)

        # If zero spot was filled, abort cleanly with zero directional risk
        if filled_spot_qty <= 0.0:
            logger.info(f"[YIELD] Spot PostOnly timed out without fills on {spot_symbol}. Clean abort.")
            return False

        # 6. Phase 3: Execute Matching Linear Perpetual Short Leg
        raw_matched_contracts = (filled_spot_qty / multiplier)
        perp_step = perp_specs["qty_step"]
        matched_perp_contracts = math.floor(raw_matched_contracts / perp_step) * perp_step
        matched_perp_qty_str = self._quantize_value(matched_perp_contracts, perp_step)

        if matched_perp_contracts < perp_specs["min_order_qty"]:
            logger.warning(f"[YIELD] Partial spot fill ({filled_spot_qty}) below perp min order. Liquidating spot...")
            await self._fetch_free_spot_balance(base_asset)
            sell_qty = self._quantize_value(filled_spot_qty, spot_specs["base_precision"])
            await self.core.executor.safe_call(
                "POST", "/v5/order/create", is_execution=True,
                category="spot", symbol=spot_symbol, side="Sell",
                orderType="Market", qty=sell_qty
            )
            return False

        perp_res = await self.core.executor.safe_call(
            "POST", "/v5/order/create", is_execution=True,
            category="linear", symbol=symbol, side="Sell",
            orderType="Market", qty=matched_perp_qty_str,
            positionIdx=self.core.sor.position_idx, timeInForce="IOC"
        )

        perp_success = isinstance(perp_res, dict) and perp_res.get("retCode") == 0

        # Successful Dual-Leg Basis Capture
        if perp_success:
            actual_perp_price = perp_bid_price
            perp_fill_data = perp_res.get("result", {})
            if "orderId" in perp_fill_data:
                await asyncio.sleep(0.15)
                p_info = await self.core.executor.safe_call(
                    "GET", "/v5/order/realtime", category="linear", symbol=symbol, orderId=perp_fill_data["orderId"]
                )
                p_list = p_info.get("result", {}).get("list", [])
                if p_list:
                    actual_perp_price = float(p_list[0].get("avgPrice", perp_bid_price) or perp_bid_price)

            hedge_id = str(uuid.uuid4())
            hedge_data = {
                "hedge_id": hedge_id,
                "spot_symbol": spot_symbol,
                "base_asset": base_asset,
                "spot_qty_str": self._quantize_value(filled_spot_qty, spot_specs["base_precision"]),
                "perp_qty_str": matched_perp_qty_str,
                "spot_units": filled_spot_qty,
                "perp_contracts": matched_perp_contracts,
                "multiplier": multiplier,
                "entry_spot_price": avg_spot_fill_price,
                "entry_perp_price": actual_perp_price,
                "funding_rate_entry": funding_rate,
                "entry_drag_bps": drag_bps,
                "expected_apy_pct": expected_apy,
                "projected_breakeven_epochs": epochs_to_be,
                "allocated_capital_usdt": filled_spot_qty * avg_spot_fill_price,
                "timestamp": time.time()
            }
            self.active_hedges[symbol] = hedge_data

            if hasattr(self.core, 'memory') and self.core.memory and self.core.memory.write_queue:
                payload = {
                    "hedge_id": hedge_id,
                    "symbol": symbol,
                    "spot_symbol": spot_symbol,
                    "contract_multiplier": multiplier,
                    "allocated_capital_usdt": filled_spot_qty * avg_spot_fill_price,
                    "spot_units": filled_spot_qty,
                    "perp_contracts": matched_perp_contracts,
                    "entry_spot_price": avg_spot_fill_price,
                    "entry_perp_price": actual_perp_price,
                    "funding_rate_entry": funding_rate,
                    "expected_apy_pct": expected_apy,
                    "projected_breakeven_epochs": epochs_to_be,
                    "entry_drag_bps": drag_bps,
                    "status": "ACTIVE"
                }
                self.core.memory.write_queue.put_nowait(("INSERT", "delta_neutral_ledger", payload, None, None))

            msg = (
                f"<b>MAKER-TAKER BASIS LOCK SECURED</b>\n"
                f"Perp Asset: <code>{symbol}</code>\n"
                f"Spot Pair: <code>{spot_symbol}</code>\n"
                f"Allocated: <code>${filled_spot_qty * avg_spot_fill_price:.2f}</code>\n"
                f"Collateral Ratio: <code>{collateral_ratio:.0%}</code>\n"
                f"Target Net APY: <code>~{expected_apy:.1f}%</code>\n"
                f"Break-Even Horizon: <code>{epochs_to_be} Epoch</code>"
            )
            await self.core._safe_telegram_dispatch(msg, is_html=True)
            logger.info(f"Basis hedge secured: {symbol} Short / {spot_symbol} Long.")
            return True

        # Phase 4: Atomic Emergency Rollback if Perpetual Leg Fails
        logger.critical(f"[YIELD] PERP HEDGE FAILED FOR {symbol}. Rolling back naked Spot Long immediately...")
        await asyncio.sleep(0.3)
        free_spot = await self._fetch_free_spot_balance(base_asset)
        sell_qty = self._quantize_value(free_spot, spot_specs["base_precision"]) if free_spot > 0 else spot_qty_str

        await self.core.executor.safe_call(
            "POST", "/v5/order/create", is_execution=True,
            category="spot", symbol=spot_symbol, side="Sell",
            orderType="Market", qty=sell_qty
        )
        return False

    async def unwind_cash_and_carry_hedge(self, symbol: str):
        """Unwinds both legs simultaneously and cleans up position records with spot fee compensation."""
        if symbol not in self.active_hedges:
            return

        data = self.active_hedges[symbol]
        spot_symbol = data["spot_symbol"]
        base_asset = data["base_asset"]
        perp_qty_str = data["perp_qty_str"]
        hedge_id = data.get("hedge_id")

        logger.critical(f"[YIELD] UNWINDING BASIS HEDGE // {symbol}. Closing Perp Short, Selling Spot Long.")

        free_spot = await self._fetch_free_spot_balance(base_asset)
        spot_specs = await self._fetch_instrument_specs(spot_symbol, "spot")
        if spot_specs and free_spot > 0.0:
            actual_spot_qty_str = self._quantize_value(free_spot, spot_specs["base_precision"])
        else:
            actual_spot_qty_str = data["spot_qty_str"]

        perp_task = self.core.executor.safe_call(
            "POST", "/v5/order/create", is_execution=True,
            category="linear", symbol=symbol, side="Buy",
            orderType="Market", qty=perp_qty_str,
            reduceOnly=True, positionIdx=self.core.sor.position_idx, timeInForce="IOC"
        )
        spot_task = self.core.executor.safe_call(
            "POST", "/v5/order/create", is_execution=True,
            category="spot", symbol=spot_symbol, side="Sell",
            orderType="Market", qty=actual_spot_qty_str
        )

        try:
            results = await asyncio.wait_for(
                asyncio.gather(perp_task, spot_task, return_exceptions=True),
                timeout=5.0
            )
            perp_res, spot_res = results[0], results[1]
        except asyncio.TimeoutError:
            logger.critical(f"[YIELD] UNWIND TIMEOUT (5.0s) ON {symbol}!")
            perp_res = {"retCode": -999}
            spot_res = {"retCode": -999}

        perp_ok = isinstance(perp_res, dict) and perp_res.get("retCode") == 0
        spot_ok = isinstance(spot_res, dict) and spot_res.get("retCode") == 0

        if perp_ok and spot_ok:
            del self.active_hedges[symbol]
            duration_days = (time.time() - data["timestamp"]) / 86400.0

            if hasattr(self.core, 'memory') and self.core.memory and self.core.memory.write_queue and hedge_id:
                update_payload = {
                    "status": "CLOSED",
                    "holding_hours": round(duration_days * 24.0, 2),
                    "close_timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                }
                self.core.memory.write_queue.put_nowait(("UPDATE", "delta_neutral_ledger", update_payload, "hedge_id", hedge_id))

            msg = (
                f"<b>DELTA-NEUTRAL HEDGE CLOSED</b>\n"
                f"Asset: <code>{symbol}</code>\n"
                f"Holding Time: <code>{duration_days:.2f} Days</code>\n"
                f"Reason: Rate Reversion"
            )
            await self.core._safe_telegram_dispatch(msg, is_html=True)
            logger.info(f"Hedge closed cleanly for {symbol}.")
        else:
            self.active_hedges[symbol]["status"] = "UNWIND_DESYNC_REQUIRES_MANUAL_AUDIT"
            logger.critical(
                f"[YIELD] UNWIND ASYMMETRY DETECTED // Perp OK: {perp_ok}, Spot OK: {spot_ok}. "
                f"Retained in memory for audit."
            )