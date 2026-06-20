import asyncio
import random
import logging
import time
from typing import Dict, Any
from services.bybit_v5 import BybitUnifiedExecutor

logger = logging.getLogger("QUANT_CORE.SOR")

class SmartOrderRouter:
    def __init__(self, executor: BybitUnifiedExecutor, max_slippage_pct: float = 0.0015):
        self.executor = executor
        self.max_slippage_pct = max_slippage_pct

    async def execute_iceberg_block(
        self, 
        symbol: str, 
        direction: str, 
        total_qty: float, 
        current_mid_price: float,
        stop_loss: float = None,
        take_profit: float = None,
        **kwargs
    ) -> bool:
        """
        Deconstructs massive order footprints into localized randomized slices.
        Contextually routes tranches as a single block if value approaches exchange floor thresholds.
        """
        # ====================================================================
        # 🛡️ SPREAD GUARD: PREVENT SLIPPAGE ON HOLLOW ORDER BOOKS
        # ====================================================================
        depth_snapshot = kwargs.get("depth_snapshot", {})
        if depth_snapshot and "bids" in depth_snapshot and "asks" in depth_snapshot:
            try:
                best_bid = float(depth_snapshot["bids"][0][0])
                best_ask = float(depth_snapshot["asks"][0][0])
                live_spread = (best_ask - best_bid) / best_bid
                
                # Halt instantly if structural order book friction exceeds our safety cap
                if live_spread > self.max_slippage_pct:
                    logger.warning(
                        f"❌ EXECUTION BLOCK BY SPREAD RADAR // {symbol} "
                        f"Spread too wide: {live_spread:.4%} (Max Cap: {self.max_slippage_pct:.4%}). Capital protected."
                    )
                    return False
            except (IndexError, ValueError, TypeError) as e:
                logger.debug(f"Depth profile parsing skipped or incomplete for {symbol}: {e}")

        logger.info(f"SOR INITIALIZED // Target: {symbol} | Direction: {direction} | Block Size: {total_qty}")
        
        allocated_qty = 0.0

        def _format_lot_size(qty: float, target_symbol: str) -> str:
            """Formats the quantity to comply with strict exchange lot sizes to prevent API bans."""
            if target_symbol.startswith("BTC") or target_symbol.startswith("ETH"):
                return f"{qty:.3f}"
            elif target_symbol.startswith("AVAX") or target_symbol.startswith("NEAR") or target_symbol.startswith("SOL") or target_symbol.startswith("WLD"):
                return f"{qty:.1f}"
            elif target_symbol.startswith("XLM") or target_symbol.startswith("ONDO") or target_symbol.startswith("ESPORTS") or target_symbol.startswith("XRP"):
                return f"{qty:.1f}"
            else:
                return f"{qty:.1f}"

        def _format_price_precision(price: float, target_symbol: str) -> float:
            """Dynamically rounds target boundary prices to prevent decimal step rejections."""
            if target_symbol.startswith("BTC") or target_symbol.startswith("ETH"):
                return round(price, 2)
            elif target_symbol.startswith("SOL") or target_symbol.startswith("AVAX"):
                return round(price, 3)
            
            if price < 1.0:
                return round(price, 4)
            return round(price, 2)

        # Institutional slicing loop execution sequence
        while allocated_qty < total_qty:
            remaining_qty = total_qty - allocated_qty
            remaining_notional = remaining_qty * current_mid_price
            
            # 🛡️ MICRO-ACCOUNT GROUNDING GATEWAY
            # If remaining dollar value is close to or under Bybit's floor limit ($5.00),
            # override the iceberg slicing completely and deploy 100% of the block as a single tranche.
            if remaining_notional <= 6.0:
                tranche_pct = 1.0
                logger.info(f"📦 NOTIONAL FLOOR PROTECTION // Order value (${remaining_notional:.2f} USDT) close to exchange minimums. Slicing bypassed for safe block routing.")
            else:
                tranche_pct = random.uniform(0.10, 0.25)
            
            raw_tranche_qty = min(total_qty * tranche_pct, remaining_qty)
            
            formatted_qty_str = _format_lot_size(raw_tranche_qty, symbol)
            tranche_qty = float(formatted_qty_str)

            if tranche_qty <= 0.0:
                formatted_qty_str = _format_lot_size(remaining_qty, symbol)
                tranche_qty = float(formatted_qty_str)
                if tranche_qty <= 0.0:
                    break

            try:
                # Format safety brackets contextually utilizing caller engine variables directly
                final_tp = _format_price_precision(take_profit, symbol) if take_profit else 0.0
                final_sl = _format_price_precision(stop_loss, symbol) if stop_loss else 0.0

                # Dispatch execution slice downstream
                order_id = await self.executor.dispatch_market_order(
                    symbol=symbol,
                    direction=direction,
                    qty=tranche_qty,
                    tp=final_tp,
                    sl=final_sl
                )

                if order_id:
                    allocated_qty += tranche_qty
                    logger.info(f"TRANCHE FILLED // Qty: {tranche_qty} | Progress: {allocated_qty / total_qty:.2%}")
                else:
                    logger.error("Tranche routing rejected at exchange interface. Aborting loop waterfall.")
                    return True if allocated_qty > 0 else False

            except Exception as e:
                logger.error(f"Critical execution failure during SOR slicing routine: {e}")
                return True if allocated_qty > 0 else False

            # Mask architectural order patterns with microsecond delays
            if allocated_qty < total_qty:
                await asyncio.sleep(random.uniform(0.15, 0.65))

        if allocated_qty > 0:
            logger.critical(f"SOR BLOCK EXECUTION COMPLETION SUCCESSFUL // Final Size: {allocated_qty} {symbol}")
            return True
            
        return False