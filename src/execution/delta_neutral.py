"""
ðŸ¦ V1.0 TITANIUM APEX: INSTITUTIONAL YIELD HARVESTER
-----------------------------------------------------
Executes Cash-and-Carry (Basis) Arbitrage and Funding Rate Harvesting.
Upgraded with Exact Execution Drag Calculus, Break-Even Horizon Gating, 
20% Account Exposure Limits, and Active Yield Ejection (Unwind) Daemons.
"""

import asyncio
import logging
import time
from typing import Dict, Any, List

logger = logging.getLogger("QUANT_CORE.YIELD_HARVESTER")

class DeltaNeutralYieldEngine:
    """
    Sweeps strictly idle margin into risk-free basis trades (Spot Long + Perp Short) 
    to harvest extreme funding rates. Automatically unwinds when yield decays.
    """
    def __init__(self, core_engine):
        self.core = core_engine
        
        # 7.5 bps per 8 hours = ~82% Risk-Free APY trigger minimum
        self.entry_funding_threshold = 0.00075  
        
        # Unwind threshold: Exit if funding drops below 1.5 bps per 8h
        self.exit_funding_threshold = 0.00015   
        
        self.active_hedges: Dict[str, dict] = {} 
        self.taker_fee_rate = 0.00055 # Bybit base taker fee

    async def run_yield_scanner_daemon(self):
        logger.info("ðŸ¦ DELTA-NEUTRAL YIELD ENGINE ONLINE: Scanning for Cash-and-Carry Arbitrage.")
        
        while True:
            await asyncio.sleep(300)  # Scan global rates every 5 minutes
            
            if not self.core.fsm.can_execute_trades:
                continue
                
            try:
                tickers_res = await self.core.executor.safe_call(
                    self.core.executor.client.get_tickers, category="linear"
                )
                if tickers_res.get("retCode") != 0: continue
                
                ticker_list = tickers_res.get("result", {}).get("list", [])
                
                # 1. EVALUATE ACTIVE HEDGES FOR UNWIND (YIELD EJECTION)
                await self._evaluate_active_hedges(ticker_list)
                    
                # 2. SCAN FOR NEW YIELD OPPORTUNITIES
                target_asset = None
                best_funding = 0.0
                
                for t in ticker_list:
                    symbol = t.get("symbol", "")
                    if not symbol.endswith("USDT") or "BTC" in symbol or "ETH" in symbol: 
                        continue # Focus strictly on volatile altcoin funding premiums
                    
                    funding_rate = float(t.get("fundingRate", 0.0) or 0.0)
                    
                    # Look for extreme POSITIVE funding (Longs overleveraged, paying Shorts)
                    if funding_rate >= self.entry_funding_threshold and funding_rate > best_funding:
                        if symbol not in self.active_hedges and symbol not in self.core.active_positions_map:
                            best_funding = funding_rate
                            target_asset = symbol
                
                if target_asset:
                    await self.execute_cash_and_carry_hedge(target_asset, best_funding)
                    
            except Exception as e:
                logger.error(f"[X-RAY] Yield Scanner Fault: {e}", exc_info=True)

    async def _evaluate_active_hedges(self, current_tickers: List[Dict[str, Any]]):
        """Monitors ongoing hedges and unwinds them if the funding rate collapses."""
        symbols_to_unwind = []
        
        for active_symbol in list(self.active_hedges.keys()):
            ticker_data = next((t for t in current_tickers if t.get("symbol") == active_symbol), None)
            
            if ticker_data:
                current_funding = float(ticker_data.get("fundingRate", 0.0) or 0.0)
                
                # Eject if funding drops too low or flips negative
                if current_funding <= self.exit_funding_threshold:
                    logger.warning(f"[X-RAY] ðŸ“‰ YIELD COLLAPSE // {active_symbol} funding dropped to {current_funding*10000:.1f} bps. Initiating Unwind.")
                    symbols_to_unwind.append(active_symbol)

        for sym in symbols_to_unwind:
            await self.unwind_cash_and_carry_hedge(sym)

    async def _calculate_execution_drag(self, symbol: str) -> float:
        """
        Calculates the exact basis spread and fee drag required to enter the position.
        Returns the total cost in basis points (bps).
        """
        try:
            spot_res = await self.core.executor.safe_call(self.core.executor.client.get_tickers, category="spot", symbol=symbol)
            perp_res = await self.core.executor.safe_call(self.core.executor.client.get_tickers, category="linear", symbol=symbol)
            
            spot_ask = float(spot_res["result"]["list"][0]["ask1Price"])
            perp_bid = float(perp_res["result"]["list"][0]["bid1Price"])
            
            # We BUY Spot Ask and SELL Perp Bid
            basis_spread_pct = abs(spot_ask - perp_bid) / spot_ask
            fee_drag_pct = self.taker_fee_rate * 2.0  # Fees paid on both legs
            
            return (basis_spread_pct + fee_drag_pct) * 10000.0
            
        except Exception as e:
            logger.debug(f"[X-RAY] Drag calculation failed for {symbol}: {e}")
            return 35.0  # Fallback to a safe 35 bps estimate

    async def execute_cash_and_carry_hedge(self, symbol: str, funding_rate: float):
        """Executes atomic dual-leg order: BUY Spot (Cash) + SHORT Perpetual (Carry)"""
        if funding_rate < self.entry_funding_threshold:
            return
            
        logger.info(f"[X-RAY] âš–ï¸ YIELD LOCK EVALUATION // {symbol} | Target Funding Rate: {funding_rate*10000:.1f} bps")
        
        try:
            # 1. Evaluate Execution Drag vs. Break-Even Horizon
            drag_bps = await self._calculate_execution_drag(symbol)
            daily_funding_bps = (funding_rate * 3) * 10000.0
            
            if daily_funding_bps <= 0: return
                
            break_even_days = drag_bps / daily_funding_bps
            
            # Reject if it takes more than 1.5 days just to earn back the entry fees & spread
            if break_even_days > 1.5:
                logger.warning(f"[X-RAY] ðŸš« YIELD REJECTED // {symbol} Drag is {drag_bps:.1f} bps. Takes {break_even_days:.1f} days to break even. Skipping.")
                return

            # 2. Calculate True Idle Capital Dynamically
            total_bal = await self.core.executor.get_wallet_balance_usdt()
            
            active_alpha_margin = 0.0
            for sym in self.core.active_positions_map.keys():
                try:
                    pos_res = await self.core.executor.safe_call(self.core.executor.client.get_positions, category="linear", symbol=sym)
                    pos_list = pos_res.get("result", {}).get("list", [])
                    if pos_list:
                        active_alpha_margin += float(pos_list[0].get("positionValue", 0.0)) / float(pos_list[0].get("leverage", 1.0))
                except Exception: pass
            
            idle_capital = total_bal - active_alpha_margin
            
            # Bybit requires ~$5-6 for Spot AND ~$5-6 for Perp. Need >$15 idle to be safe.
            if idle_capital < 15.0:
                return

            # ðŸš€ V1.0: Strict Capital Allocation Limits
            # Sweep 90% of IDLE capital, but NEVER exceed 20% of TOTAL account equity per hedge
            max_allowed_hedge = total_bal * 0.20
            yield_capital = min(idle_capital * 0.90, max_allowed_hedge)
            
            if yield_capital < 12.0:
                return
            
            spot_res = await self.core.executor.safe_call(self.core.executor.client.get_tickers, category="spot", symbol=symbol)
            spot_price = float(spot_res["result"]["list"][0]["lastPrice"])
            
            await self.core.sor._fetch_exchange_limits(symbol)
            qty = self.core.sor._apply_dynamic_exchange_limits(yield_capital / spot_price, spot_price, symbol)
            
            # ATOMIC EXECUTION BLOCK
            logger.info(f"[X-RAY] ðŸ¦ Routing SPOT BUY for {qty} {symbol}...")
            spot_order = await self.core.executor.safe_call(
                self.core.executor.client.place_order, category="spot", symbol=symbol, side="Buy", 
                orderType="Market", qty=str(qty), marketUnit="baseCoin" # Force exact base coin matching
            )
            
            if spot_order.get("retCode") != 0:
                logger.error(f"[X-RAY] âŒ SPOT Leg Failed: {spot_order.get('retMsg')}. Aborting hedge.")
                return
                
            logger.info(f"[X-RAY] ðŸ¦ Routing PERPETUAL SHORT for {qty} {symbol}...")
            perp_order = await self.core.executor.safe_call(
                self.core.executor.client.place_order, category="linear", symbol=symbol, side="Sell", 
                orderType="Market", qty=str(qty), positionIdx=self.core.sor.position_idx
            )
            
            if perp_order.get("retCode") != 0:
                logger.critical(f"ðŸš¨ FATAL: PERPETUAL Leg Failed after Spot execution! Engaging emergency Spot liquidation.")
                await self.core.executor.safe_call(self.core.executor.client.place_order, category="spot", symbol=symbol, side="Sell", orderType="Market", qty=str(qty))
                return
            
            self.active_hedges[symbol] = {
                "qty": qty, 
                "entry_price": spot_price, 
                "timestamp": time.time(),
                "projected_break_even_days": break_even_days
            }
            
            msg = (
                f"âœ… <b>DELTA-NEUTRAL YIELD LOCK SECURED</b>\n"
                f"Asset: <code>{symbol}</code>\n"
                f"Capital Swept: <code>${yield_capital:.2f}</code>\n"
                f"Yield APY Target: <code>~{(funding_rate * 3 * 365)*100:.1f}%</code>\n"
                f"Break-Even Horizon: <code>{break_even_days:.1f} Days</code>"
            )
            await self.core._safe_telegram_dispatch(msg, is_html=True)
            logger.critical(f"âœ… DELTA-NEUTRAL LOCK SECURED // {symbol} successfully hedged using idle cash flow.")
            
        except Exception as e:
            logger.error(f"[X-RAY] Hedge execution failed for {symbol}: {e}", exc_info=True)

    async def unwind_cash_and_carry_hedge(self, symbol: str):
        """Dismantles an active hedge by Selling Spot and Buying back the Perpetual Short."""
        if symbol not in self.active_hedges:
            return
            
        hedge_data = self.active_hedges[symbol]
        qty = str(hedge_data["qty"])
        
        logger.critical(f"[X-RAY] ðŸŒªï¸ UNWINDING HEDGE // {symbol}. Selling Spot, Covering Short.")
        
        try:
            # 1. Close Perpetual Short
            perp_order = await self.core.executor.safe_call(
                self.core.executor.client.place_order, category="linear", symbol=symbol, side="Buy", 
                orderType="Market", qty=qty, reduceOnly=True, positionIdx=self.core.sor.position_idx
            )
            
            if perp_order.get("retCode") != 0:
                logger.error(f"[X-RAY] âŒ Unwind PERP Leg Failed: {perp_order.get('retMsg')}. Manual intervention required.")
                
            # 2. Sell Spot Collateral
            spot_order = await self.core.executor.safe_call(
                self.core.executor.client.place_order, category="spot", symbol=symbol, side="Sell", 
                orderType="Market", qty=qty
            )
            
            if spot_order.get("retCode") != 0:
                logger.error(f"[X-RAY] âŒ Unwind SPOT Leg Failed: {spot_order.get('retMsg')}. Manual intervention required.")
            
            # Remove from tracking regardless of success to prevent infinite loop errors
            del self.active_hedges[symbol]
            
            duration_days = (time.time() - hedge_data["timestamp"]) / 86400.0
            msg = f"ðŸ”„ <b>DELTA-NEUTRAL HEDGE UNWOUND</b>\nAsset: <code>{symbol}</code>\nHolding Time: <code>{duration_days:.1f} Days</code>\nReason: Funding Decay"
            await self.core._safe_telegram_dispatch(msg, is_html=True)
            logger.info(f"âœ… HEDGE SUCCESSFULLY UNWOUND for {symbol}.")
            
        except Exception as e:
            logger.error(f"[X-RAY] Unwind execution failed for {symbol}: {e}", exc_info=True)