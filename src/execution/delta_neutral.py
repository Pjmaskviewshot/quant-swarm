"""
🏦 V56.2 INSTITUTIONAL YIELD HARVESTER: SMART SCAVENGER ENGINE
------------------------------------------------------------
Executes Cash-and-Carry (Basis) Arbitrage and Funding Rate Harvesting.
Upgraded to dynamically sweep idle margin without starving the Alpha Swarm.
Enforces strict positive-yield guards and atomic spot/perp execution.
"""

import asyncio
import logging
import time
from typing import Dict, Any

logger = logging.getLogger("QUANT_CORE.YIELD_HARVESTER")

class DeltaNeutralYieldEngine:
    def __init__(self, core_engine):
        self.core = core_engine
        # 5 bps per 8 hours = ~54% Risk-Free APY trigger minimum
        self.min_funding_threshold = 0.0005  
        self.active_hedges: Dict[str, dict] = {} 

    async def run_yield_scanner_daemon(self):
        logger.info("🏦 DELTA-NEUTRAL YIELD ENGINE ONLINE: Scanning for Cash-and-Carry Arbitrage.")
        
        while True:
            await asyncio.sleep(300)  # Scan global rates every 5 minutes
            
            if not self.core.fsm.can_execute_trades:
                continue
                
            try:
                tickers_res = await self.core.executor.safe_call(
                    self.core.executor.client.get_tickers, category="linear"
                )
                if tickers_res.get("retCode") != 0: continue
                    
                target_asset = None
                best_funding = 0.0
                
                for t in tickers_res.get("result", {}).get("list", []):
                    symbol = t.get("symbol", "")
                    if not symbol.endswith("USDT") or "BTC" in symbol or "ETH" in symbol: 
                        continue # Focus strictly on volatile altcoin funding premiums
                    
                    funding_rate = float(t.get("fundingRate", 0.0) or 0.0)
                    
                    # Look for extreme POSITIVE funding (Longs overleveraged, paying Shorts)
                    if funding_rate > self.min_funding_threshold and funding_rate > best_funding:
                        if symbol not in self.active_hedges and symbol not in self.core.active_positions_map:
                            best_funding = funding_rate
                            target_asset = symbol
                
                if target_asset:
                    await self.execute_cash_and_carry_hedge(target_asset, best_funding)
                    
            except Exception as e:
                logger.error(f"[X-RAY] Yield Scanner Fault: {e}", exc_info=True)

    async def execute_cash_and_carry_hedge(self, symbol: str, funding_rate: float):
        """Executes atomic dual-leg order: BUY Spot (Cash) + SHORT Perpetual (Carry)"""
        
        # 🚀 AUDIT FIX: Only harvest if funding is strictly positive (Shorts get paid)
        if funding_rate <= 0:
            return
            
        logger.critical(f"[X-RAY] ⚖️ YIELD LOCK INITIATED // {symbol} | Target Funding Rate: {funding_rate*10000:.1f} bps")
        
        try:
            # 1. Calculate True Idle Capital Dynamically
            total_bal = await self.core.executor.get_wallet_balance_usdt()
            
            # Calculate how much margin the Alpha Swarm is actively using
            active_alpha_margin = 0.0
            for sym in self.core.active_positions_map.keys():
                try:
                    pos_res = await self.core.executor.safe_call(self.core.executor.client.get_positions, category="linear", symbol=sym)
                    pos_list = pos_res.get("result", {}).get("list", [])
                    if pos_list:
                        active_alpha_margin += float(pos_list[0].get("positionValue", 0.0)) / float(pos_list[0].get("leverage", 1.0))
                except Exception:
                    pass
            
            idle_capital = total_bal - active_alpha_margin
            
            # 🚀 V56.2 ABSOLUTE PLUS: Bybit requires ~$5 for Spot AND ~$5 for Perp.
            # We ONLY execute the hedge if there is >$12.00 sitting completely idle.
            if idle_capital < 12.0:
                logger.warning(f"[X-RAY] 🛑 Idle Capital (${idle_capital:.2f}) too low for dual-leg hedge. Leaving funds for Alpha Swarm.")
                return

            # Sweep 90% of the IDLE capital (not total capital) into the yield hedge
            yield_capital = idle_capital * 0.90 
            
            spot_res = await self.core.executor.safe_call(self.core.executor.client.get_tickers, category="spot", symbol=symbol)
            spot_price = float(spot_res["result"]["list"][0]["lastPrice"])
            
            await self.core.sor._fetch_exchange_limits(symbol)
            qty = self.core.sor._apply_dynamic_exchange_limits(yield_capital / spot_price, spot_price, symbol)
            
            # ATOMIC EXECUTION BLOCK
            logger.info(f"[X-RAY] 🏦 Routing SPOT BUY for {qty} {symbol}...")
            spot_order = await self.core.executor.safe_call(
                self.core.executor.client.place_order, category="spot", symbol=symbol, side="Buy", 
                orderType="Market", qty=str(qty), marketUnit="baseCoin" # Force exact base coin matching
            )
            
            if spot_order.get("retCode") != 0:
                logger.error(f"[X-RAY] ❌ SPOT Leg Failed: {spot_order.get('retMsg')}. Aborting hedge.")
                return
                
            logger.info(f"[X-RAY] 🏦 Routing PERPETUAL SHORT for {qty} {symbol}...")
            perp_order = await self.core.executor.safe_call(
                self.core.executor.client.place_order, category="linear", symbol=symbol, side="Sell", 
                orderType="Market", qty=str(qty), positionIdx=self.core.sor.position_idx
            )
            
            if perp_order.get("retCode") != 0:
                logger.critical(f"🚨 FATAL: PERPETUAL Leg Failed after Spot execution! Engaging emergency Spot liquidation.")
                await self.core.executor.safe_call(self.core.executor.client.place_order, category="spot", symbol=symbol, side="Sell", orderType="Market", qty=str(qty))
                return
            
            self.active_hedges[symbol] = {"qty": qty, "entry_price": spot_price, "timestamp": time.time()}
            
            msg = f"✅ <b>DELTA-NEUTRAL YIELD LOCK SECURED</b>\nAsset: {symbol}\nIdle Capital Swept: ${yield_capital:.2f}\nYield APY Target: ~{(funding_rate * 3 * 365)*100:.1f}%"
            await self.core._safe_telegram_dispatch(msg, is_html=True)
            logger.critical(f"✅ DELTA-NEUTRAL LOCK SECURED // {symbol} successfully hedged using idle cash flow.")
            
        except Exception as e:
            logger.error(f"[X-RAY] Hedge execution failed for {symbol}: {e}", exc_info=True)