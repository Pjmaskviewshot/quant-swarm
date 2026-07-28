"""
💎 V50.0 QUANTUM SWARM: ASYNCHRONOUS DATA FEED & NARRATIVE ENGINE
-----------------------------------------------------------------
Features In-Memory News TTL Caching, Malformed Candle Array Guards,
Rate-Limit Exponential Backoff Protocols, and X-Ray Diagnostic Telemetry.
"""

import time
import asyncio
import aiohttp
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger("QUANT_CORE.DATA_FEED")

class AsynchronousDataFeed:
    """
    🚀 V50.0 ASYNCHRONOUS DATA FEED & NARRATIVE ENGINE
    Pools historical kline snapshots and macro news narratives concurrently with 
    strict connection pooling, TTL caching, and X-Ray diagnostics.
    """
    def __init__(self, finnhub_key: str, news_ttl_seconds: float = 300.0):
        self.finnhub_key = finnhub_key or ""
        self.bybit_base_url = "https://api.bybit.com/v5/market/kline"
        self.finnhub_url = "https://finnhub.io/api/v1/news"
        
        # In-Memory News TTL Cache (Prevents API rate-limit exhaustion across swarm cycles)
        self._cached_news: str = "Macro narrative feed initializing."
        self._last_news_fetch: float = 0.0
        self._news_ttl: float = news_ttl_seconds

    async def fetch_market_snapshot(
        self, 
        symbol: str, 
        interval: str, 
        session: Optional[aiohttp.ClientSession] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Pools pricing and narrative data concurrently. Supports session injection 
        to leverage persistent TCP connection pooling across multi-node swarms.
        """
        if session and not session.closed:
            return await self._execute_snapshot(session, symbol, interval)
        
        # Fallback context manager if no master session is injected
        async with aiohttp.ClientSession() as ephemeral_session:
            return await self._execute_snapshot(ephemeral_session, symbol, interval)

    async def _execute_snapshot(self, session: aiohttp.ClientSession, symbol: str, interval: str) -> Optional[Dict[str, Any]]:
        """Internal execution core for compiling market and narrative arrays."""
        try:
            # 1 candle fetch for current price check
            market_task = self._get_bybit_klines(session, symbol, interval, limit="1")
            news_task = self._get_finnhub_news(session)
            
            klines, news_headlines = await asyncio.gather(market_task, news_task, return_exceptions=True)
            news_context = news_headlines if isinstance(news_headlines, str) else self._cached_news
            
            # CIRCUIT BREAKER: Quick-fail on empty data arrays to protect downstream processes
            if isinstance(klines, Exception) or not klines:
                logger.warning(f"[X-RAY] ⚠️ Market data absent for {symbol}. Dropping asset from current snapshot cycle.")
                return None
            
            # Safe Parsing Guard against Malformed Candle Arrays
            try:
                current_price = float(klines[0][4])
                if current_price <= 0:
                    raise ValueError("Non-positive price returned.")
            except (IndexError, ValueError, TypeError) as parse_err:
                logger.error(f"[X-RAY] ❌ Malformed candle array structure for {symbol}: {parse_err}")
                return None
            
            return {
                "symbol": symbol,
                "current_price": current_price,
                "raw_klines": klines,
                "news_context": news_context
            }
        except Exception as e:
            logger.error(f"[X-RAY] Systemic ingestion pipeline fault on {symbol}: {e}")
            return None

    async def _get_bybit_klines(self, session: aiohttp.ClientSession, symbol: str, interval: str, limit: str = "1") -> List[List[str]]:
        """Fetches historical data to build baseline candles with strict exponential backoff fallbacks."""
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        
        for attempt in range(3):
            try:
                async with session.get(self.bybit_base_url, params=params, timeout=10.0) as response:
                    if response.status in [429, 403]:
                        logger.warning(f"[X-RAY] ⚠️ Bybit HTTP Rate Limit ({response.status}) on {symbol}. Backing off...")
                        await asyncio.sleep(3.0 * (attempt + 1))
                        continue
                        
                    response.raise_for_status()
                    payload = await response.json()
                    
                    if payload.get("retCode") == 0:
                        data_list = payload.get("result", {}).get("list", [])
                        if data_list:
                            return data_list
                        else:
                            logger.warning(f"[X-RAY] ⚠️ Bybit returned empty kline array for {symbol}.")
                            return []
                    else:
                        ret_code = payload.get("retCode")
                        if ret_code in [10006, 10002]:
                            logger.error(f"[X-RAY] ❌ Bybit API Rate Limit JSON Code ({ret_code}) hit for {symbol}.")
                            await asyncio.sleep(5.0)
                            return []
                            
                        logger.warning(f"[X-RAY] Bybit payload error {ret_code} for {symbol}. Retrying...")
                        
            except Exception as e:
                logger.error(f"[X-RAY] Failed to fetch klines for {symbol} on attempt {attempt + 1}: {e}")
            
            await asyncio.sleep(1.5 * (attempt + 1))
            
        logger.error(f"[X-RAY] ❌ All historical data retries exhausted for {symbol}.")
        return []

    async def _get_finnhub_news(self, session: aiohttp.ClientSession) -> str:
        """
        Fetches macro narrative context from Finnhub.
        Shared TTL Cache prevents concurrent swarm nodes from burning Finnhub API limits.
        """
        now = time.time()
        # Return cached news if TTL is unexpired
        if now - self._last_news_fetch < self._news_ttl and self._cached_news:
            return self._cached_news

        if not self.finnhub_key:
            return "Macro narrative feed disabled (no API key)."

        params = {
            "category": "crypto",
            "token": self.finnhub_key
        }
        
        for attempt in range(2):
            try:
                async with session.get(self.finnhub_url, params=params, timeout=5.0) as response:
                    if response.status != 200:
                        return self._cached_news
                    
                    data = await response.json()
                    if isinstance(data, list) and data:
                        headlines = [
                            str(item.get("headline", "")).replace("\n", " ").strip() 
                            for item in data[:4] 
                            if item.get("headline")
                        ]
                        if headlines:
                            self._cached_news = " | ".join(headlines)
                            self._last_news_fetch = now
                            logger.debug(f"[X-RAY] 📰 Macro News Cache Refreshed: {self._cached_news[:80]}...")
                            return self._cached_news
            except Exception as e:
                logger.debug(f"[X-RAY] Finnhub news fetch attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(1.0)
                
        return self._cached_news