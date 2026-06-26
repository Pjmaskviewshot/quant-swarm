import json
import logging
import httpx
from typing import Dict, Any, List
from openai import AsyncOpenAI

logger = logging.getLogger("QUANT_CORE.AI_ROUTER")

class ResilientAIRouter:
    def __init__(self, nv_keys: List[str], deepseek_key: str):
        # ⚠️ NVIDIA COMPLETELY BYPASSED
        # nv_keys is kept in the parameters so main.py doesn't crash, but it is ignored.
        self.deepseek_key = deepseek_key
        
        # 🛡️ HARDENED NETWORK CONFIGURATION FOR CLOUD DEPLOYMENT
        custom_http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(45.0, connect=15.0),
            http2=False,  # CRITICAL: Disabling HTTP/2 prevents shared-cloud connection drops
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
        )
        
        # Instantiate Native DeepSeek Async Client directly with custom HTTP client
        self.client = AsyncOpenAI(
            base_url="https://api.deepseek.com/v1",
            api_key=self.deepseek_key,
            max_retries=5,  # Automatically retry 5 times on network blips before failing
            http_client=custom_http_client
        )
        logger.info("✅ Native DeepSeek V4 Router initialized (Hardened Network Mode Active)")

    def _clean_json_output(self, raw_text: str) -> str:
        """Strips markdown formatting if the LLM wraps the JSON in code blocks."""
        if "```json" in raw_text:
            return raw_text.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_text:
            return raw_text.split("```")[1].split("```")[0].strip()
        return raw_text.strip()

    async def extract_market_verdict(self, batched_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Orchestrates structured intent extraction via DeepSeek V4.
        Implements Local Pre-Filtering and Context Caching to maximize API runway.
        """
        
        # ---------------------------------------------------------
        # 🛑 1. CONDITIONAL PRE-FILTER (Local Math Check)
        # ---------------------------------------------------------
        # Check if the market is ranging across all assets in the batch.
        # If every single asset has an absolute Z-score < 2.0, skip the AI entirely.
        is_market_active = False
        for ticker, data in batched_payload.items():
            # 🚀 BUG FIX: Corrected key mapping to match the payload from main.py
            z_score = data.get("volatility_z_score", 0.0)
            if abs(z_score) >= 2.0:
                is_market_active = True
                break
        
        if not is_market_active:
            logger.info("💤 Local Pre-Filter: Matrix is flat (|Z| < 2.0). Skipping DeepSeek API to save costs.")
            # Automatically default all assets to HOLD locally
            return {symbol: {"direction": "HOLD", "confidence": 0.0} for symbol in batched_payload.keys()}

        logger.info("🚨 Local Pre-Filter: Structural Anomaly Detected. Waking up DeepSeek Cloud...")

        # ---------------------------------------------------------
        # 🛑 2. STATIC SYSTEM PREFIX (Cache Target)
        # ---------------------------------------------------------
        # This block MUST remain perfectly static. DeepSeek caches exact prefixes.
        # By separating this from the live data, we slash token costs by up to 90%.
        system_instruction = (
            "You are an elite institutional algorithmic execution core operating a Dual-Gate MIEG+TFI architecture.\n"
            "Your objective is to analyze order book imbalances (Z-OBI), tape flow exhaustion, and regime profiles.\n\n"
            "EXECUTION RULES:\n"
            "1. If Z-score is strictly between -2.0 and 2.0, output HOLD.\n"
            "2. If Z-score <= -2.40 and tape confirms exhaustion, output BUY.\n"
            "3. If Z-score >= 2.40 and tape confirms exhaustion, output SELL.\n"
            "4. For edge cases between 2.0 and 2.40, evaluate structural volume.\n\n"
            "You MUST return ONLY a raw, valid JSON dictionary mapping the asset ticker symbol to its verdict. "
            "Do not include markdown formatting tags, wrapping blocks, or prose.\n"
            "Format exactly like this example:\n"
            "{\n"
            "  \"BTCUSDT\": {\"direction\": \"BUY\", \"confidence\": 0.85},\n"
            "  \"ETHUSDT\": {\"direction\": \"HOLD\", \"confidence\": 0.00}\n"
            "}\n"
            "Valid directions are strictly: BUY, SELL, HOLD. Every asset in the payload must have an entry in the response."
        )

        # ---------------------------------------------------------
        # 🟢 3. DYNAMIC SUFFIX (Live Market Data)
        # ---------------------------------------------------------
        # This changes every cycle. We keep it at the absolute bottom of the payload.
        prompt = f"LIVE MARKET DATA BATCH:\n{json.dumps(batched_payload, indent=2)}"

        messages = [
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": prompt}
        ]

        try:
            response = await self.client.chat.completions.create(
                model="deepseek-chat",
                messages=messages,
                temperature=0.0,  # CRITICAL: 0.0 for deterministic JSON stability
                top_p=0.95,
                max_tokens=2048,
                response_format={"type": "json_object"}
            )
            
            raw_content = response.choices[0].message.content
            cleaned_content = self._clean_json_output(raw_content)
            return json.loads(cleaned_content)
            
        except Exception as e:
            logger.critical(f"Systemic AI Network Blackout. DeepSeek failed: {e}")
            # Dynamic Fallback: Safely construct a "HOLD" dictionary for every coin
            return {symbol: {"direction": "HOLD", "confidence": 0.0} for symbol in batched_payload.keys()}