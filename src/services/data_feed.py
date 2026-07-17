import asyncio
import aiohttp
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger("QUANT_CORE.DATA_FEED")

class AsynchronousDataFeed:
    def __init__(self, finnhub_key: str):
        self.finnhub_key = finnhub_key
        self.bybit_base_url = "https://api.bybit.com/v5/market/kline"
        self.finnhub_url = "https://finnhub.io/api/v1/news"

    async def fetch_market_snapshot(
        self, 
        symbol: str, 
        interval: str, 
        session: Optional[aiohttp.ClientSession] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Pools external pricing pipelines concurrently. Supports session injection 
        to leverage persistent TCP connection pooling across swarm nodes.
        """
        if session and not session.closed:
            return await self._execute_snapshot(session, symbol, interval)
        
        # Fallback context manager if no master session is injected
        async with aiohttp.ClientSession() as ephemeral_session:
            return await self._execute_snapshot(ephemeral_session, symbol, interval)

    async def _execute_snapshot(self, session: aiohttp.ClientSession, symbol: str, interval: str) -> Optional[Dict[str, Any]]:
        """Internal execution core for compiling market and narrative arrays."""
        try:
            # 🛑 P1-5 FIX: Only request a limit of "1" candle instead of "30".
            # The orchestrator only calls this function to refresh the news cache and 
            # extract the current price. Fetching 30 candles wasted API rate limits.
            market_task = self._get_bybit_klines(session, symbol, interval, limit="1")
            news_task = self._get_finnhub_news(session)
            
            klines, news_headlines = await asyncio.gather(market_task, news_task, return_exceptions=True)
            
            news_context = news_headlines if not isinstance(news_headlines, Exception) else "Macro narrative feed unavailable."
            
            # CIRCUIT BREAKER: Force quick-fail on empty data arrays to protect API limits
            if isinstance(klines, Exception) or not klines:
                logger.warning(f"Market data absent for {symbol}. Dropping asset from current cycle to protect API limits.")
                return None
                
            current_price = float(klines[0][4])
            
            return {
                "symbol": symbol,
                "current_price": current_price,
                "raw_klines": klines,
                "news_context": news_context
            }
        except Exception as e:
            logger.error(f"Systemic ingestion pipeline fault: {e}")
            return None

    async def _get_bybit_klines(self, session: aiohttp.ClientSession, symbol: str, interval: str, limit: str = "1") -> List[List[str]]:
        """Fetches historical data to build the baseline, with strict error fallbacks."""
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": limit  # Applied the optimized limit parameter
        }
        
        for attempt in range(3):
            try:
                async with session.get(self.bybit_base_url, params=params, timeout=10.0) as response:
                    if response.status in [429, 403]:
                        logger.warning(f"Bybit HTTP Rate Limit ({response.status}) on {symbol}. Heavy backoff initiated...")
                        await asyncio.sleep(5)
                        continue
                        
                    response.raise_for_status()
                    payload = await response.json()
                    
                    if payload.get("retCode") == 0:
                        data_list = payload.get("result", {}).get("list", [])
                        if len(data_list) > 0:
                            return data_list
                        else:
                            # CIRCUIT BREAKER: Immediate break on empty historical arrays
                            logger.warning(f"Bybit returned an empty structural array for {symbol}. Circuit breaker triggered.")
                            return []
                    else:
                        ret_code = payload.get("retCode")
                        if ret_code in [10006, 10002]:
                            logger.error(f"Bybit API JSON Rate Limit ({ret_code}) hit for {symbol}. Forcing thread sleep.")
                            await asyncio.sleep(5)
                            return []
                            
                        logger.warning(f"Bybit payload error {ret_code} for {symbol}. Retrying...")
                        
            except Exception as e:
                logger.error(f"Failed to fetch klines for {symbol} on attempt {attempt + 1}: {e}")
            
            await asyncio.sleep(2)
            
        logger.critical(f"All historical data retries exhausted for {symbol}. Returning empty to force cooldown.")
        return []

    async def _get_finnhub_news(self, session: aiohttp.ClientSession) -> str:
        """Fetches macro narrative context from Finnhub."""
        params = {
            "category": "crypto",
            "token": self.finnhub_key
        }
        for attempt in range(2):
            try:
                async with session.get(self.finnhub_url, params=params, timeout=5.0) as response:
                    if response.status != 200:
                        return "Macro narrative feed unavailable."
                    data = await response.json()
                    headlines = [item.get("headline", "") for item in data[:4] if item.get("headline")]
                    return " | ".join(headlines)
            except Exception:
                await asyncio.sleep(1)
        return "Macro narrative feed unavailable."