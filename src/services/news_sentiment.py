import os
import re
import json
import time
import logging
import asyncio
from typing import Dict, List
from services.ai_router import ResilientAIRouter

logger = logging.getLogger("QUANT_CORE.NEWS_SENTIMENT")

class MacroNewsSentimentAnalyzer:
    """
    🚀 V39.0 APEX: MACRO NEWS SENTIMENT ANALYZER
    Processes live financial news headlines, translates unstructured text into 
    quantitative sentiment tensors [-1.0 to 1.0], and caches them for the math engine.
    """
    def __init__(self, router_override: ResilientAIRouter = None):
        if router_override:
            self.router = router_override
        else:
            nv_keys = [os.getenv("NVIDIA_API_KEY_1", ""), os.getenv("NVIDIA_API_KEY_2", "")]
            deepseek_key = os.getenv("DEEPSEEK_API_KEY", "")
            self.router = ResilientAIRouter(nv_keys=nv_keys, deepseek_key=deepseek_key)
            
        self.sentiment_cache: Dict[str, Dict[str, float]] = {}

    async def analyze_news_batch(self, symbol: str, headlines: List[str]) -> float:
        """
        Evaluates a batch of recent news headlines and returns a numeric sentiment score.
        """
        if not headlines:
            return 0.0

        context = "\n".join([f"- {h}" for h in headlines[:15]])
        prompt = (
            f"Analyze the following recent news headlines for the cryptocurrency asset {symbol}. "
            "Determine the immediate short-term market sentiment. "
            "Return ONLY a valid JSON object with no markdown formatting.\n"
            "Format: {\"sentiment_score\": float between -1.0 (extreme bearish/panic) and 1.0 (extreme bullish/euphoria), \"summary\": \"1 concise sentence explaining why\"}"
        )

        try:
            raw_response = await self.router.execute_inference(
                messages=[{"role": "user", "content": f"{prompt}\n\nHeadlines:\n{context}"}],
                require_json=True,
                timeout=15.0
            )
            
            # Extract JSON cleanly to prevent parser crashes
            match = re.search(r'\{.*\}', raw_response, re.DOTALL)
            clean_payload = match.group(0) if match else raw_response
            
            data = json.loads(clean_payload)
            score = float(data.get("sentiment_score", 0.0))
            score = max(-1.0, min(1.0, score))
            
            logger.info(f"📰 {symbol} NEWS SENTIMENT: {score:.2f} | {data.get('summary', '')}")
            
            self.sentiment_cache[symbol] = {
                "score": score,
                "timestamp": time.time()
            }
            return score
            
        except Exception as e:
            logger.error(f"News sentiment analysis failed for {symbol}: {e}")
            return 0.0

    def get_latest_sentiment(self, symbol: str) -> float:
        """Returns the cached sentiment score if it's less than 1 hour old, else returns 0.0 (neutral)."""
        cache = self.sentiment_cache.get(symbol)
        if cache and (time.time() - cache["timestamp"] < 3600):
            return cache["score"]
        return 0.0