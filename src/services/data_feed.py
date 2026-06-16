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
        """Concurrently pools external pricing pipelines and narrative flows."""
        async with aiohttp.ClientSession() as session:
            try:
                # Dispatches concurrent network requests to minimize blocking
                market_task = self._get_bybit_klines(session, symbol, interval)
                news_task = self._get_finnhub_news(session)
                
                klines, news_headlines = await asyncio.gather(market_task, news_task, return_exceptions=True)
                
                if isinstance(klines, Exception) or not klines:
                    logger.error(f"Failed to fetch market data array: {klines}")
                    return None
                    
                current_price = float(klines[0][4])  # Latest structural close candle boundary
                
                return {
                    "symbol": symbol,
                    "current_price": current_price,
                    "raw_klines": klines,
                    "news_context": news_headlines if not isinstance(news_headlines, Exception) else ""
                }
            except Exception as e:
                logger.error(f"Systemic ingestion pipeline fault: {e}")
                return None

    async def _get_bybit_klines(self, session: aiohttp.ClientSession, symbol: str, interval: str) -> List[List[str]]:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": "30"
        }
        async with session.get(self.bybit_base_url, params=params, timeout=5.0) as response:
            response.raise_for_status()
            payload = await response.json()
            return payload["result"]["list"]

    async def _get_finnhub_news(self, session: aiohttp.ClientSession) -> str:
        params = {
            "category": "crypto",
            "token": self.finnhub_key
        }
        async with session.get(self.finnhub_url, params=params, timeout=5.0) as response:
            if response.status != 200:
                return "Macro narrative feed unavailable."
            data = await response.json()
            # Extract and compress the 4 latest structural headlines
            headlines = [item.get("headline", "") for item in data[:4] if item.get("headline")]
            return " | ".join(headlines)