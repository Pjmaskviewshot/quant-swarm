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

    async def fetch_market_snapshot(self, symbol: str, interval: str) -> Optional[Dict[str, Any]]:
        """Concurrently pools external pricing pipelines and narrative flows with safe fallbacks."""
        async with aiohttp.ClientSession() as session:
            try:
                # Dispatches concurrent network requests to minimize blocking
                market_task = self._get_bybit_klines(session, symbol, interval)
                news_task = self._get_finnhub_news(session)
                
                klines, news_headlines = await asyncio.gather(market_task, news_task, return_exceptions=True)
                
                # Safely unpack the news even if the klines task failed
                news_context = news_headlines if not isinstance(news_headlines, Exception) else "Macro narrative feed unavailable."
                
                # If Bybit failed to return candles, gracefully return an empty structural snapshot
                if isinstance(klines, Exception) or not klines:
                    logger.error(f"Failed to fetch market data array for {symbol}. Returning neutral baseline.")
                    return {
                        "symbol": symbol,
                        "current_price": 0.0,
                        "raw_klines": [],
                        "news_context": news_context
                    }
                    
                current_price = float(klines[0][4])  # Latest structural close candle boundary
                
                return {
                    "symbol": symbol,
                    "current_price": current_price,
                    "raw_klines": klines,
                    "news_context": news_context
                }
            except Exception as e:
                logger.error(f"Systemic ingestion pipeline fault: {e}")
                return None

    async def _get_bybit_klines(self, session: aiohttp.ClientSession, symbol: str, interval: str) -> List[List[str]]:
        """Fetches historical data to build the baseline, with strict error fallbacks."""
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": "30"
        }
        
        for attempt in range(3): # Try 3 times before giving up
            try:
                async with session.get(self.bybit_base_url, params=params, timeout=10.0) as response:
                    # Handle Bybit rate limits gracefully
                    if response.status == 429:
                        logger.warning(f"Bybit Rate Limit (429) on {symbol}. Retrying...")
                        await asyncio.sleep(2)
                        continue
                        
                    response.raise_for_status()
                    payload = await response.json()
                    
                    # Safely check if 'result' and 'list' actually exist in the dictionary
                    if payload.get("retCode") == 0 and "result" in payload and "list" in payload["result"]:
                        return payload["result"]["list"]
                    else:
                        logger.warning(f"Bybit returned an empty/malformed historical array for {symbol}. Retrying...")
                        
            except Exception as e:
                logger.error(f"Failed to fetch klines for {symbol} on attempt {attempt + 1}: {e}")
            
            await asyncio.sleep(2) # Wait 2 seconds before retrying
            
        # 🔴 CRITICAL FALLBACK: If Bybit completely fails, return a safe, flat baseline
        logger.critical(f"All historical data retries exhausted for {symbol}. Injecting neutral baseline.")
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
                    # Extract and compress the 4 latest structural headlines
                    headlines = [item.get("headline", "") for item in data[:4] if item.get("headline")]
                    return " | ".join(headlines)
            except Exception:
                await asyncio.sleep(1)
        return "Macro narrative feed unavailable."