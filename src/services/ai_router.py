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
        Orchestrates structured intent extraction directly via DeepSeek V4.
        Processes a BATCHED payload of multiple assets in a single ultra-fast API call.
        """
        prompt = f"""
        Execute quantitative matrix analysis on the following integrated streaming data parameters for multiple assets:
        {json.dumps(batched_payload, indent=2)}

        Determine market structural execution matrix direction for the upcoming execution cycle window for EACH asset.
        """
        
        system_instruction = (
            "You are an institutional algorithmic execution core. You will receive a JSON payload containing live market metrics for a batch of multiple assets. "
            "Evaluate the macro regime for EACH asset independently based on its specific data. "
            "You MUST return ONLY a raw, valid JSON dictionary mapping the asset ticker symbol to its verdict. Do not include markdown formatting tags, wrapping blocks, or prose. "
            "Format exactly like this example:\n"
            "{\n"
            "  \"BTCUSDT\": {\"direction\": \"BUY\", \"confidence\": 0.85},\n"
            "  \"ETHUSDT\": {\"direction\": \"HOLD\", \"confidence\": 0.00}\n"
            "}\n"
            "Valid directions are strictly: BUY, SELL, HOLD. Every asset in the payload must have an entry in the response."
        )

        messages = [
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": prompt}
        ]

        try:
            # Direct routing to native DeepSeek for lowest possible latency
            response = await self.client.chat.completions.create(
                model="deepseek-chat",
                messages=messages,
                temperature=0.1,  # CRITICAL: Kept low for deterministic JSON stability
                top_p=0.95,
                max_tokens=2048,
                response_format={"type": "json_object"}
            )
            
            raw_content = response.choices[0].message.content
            cleaned_content = self._clean_json_output(raw_content)
            return json.loads(cleaned_content)
            
        except Exception as e:
            logger.critical(f"Systemic AI Network Blackout. DeepSeek failed: {e}")
            # Dynamic Fallback: Safely construct a "HOLD" dictionary for every coin in the batch request
            return {symbol: {"direction": "HOLD", "confidence": 0.0} for symbol in batched_payload.keys()}