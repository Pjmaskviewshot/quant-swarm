import os
import time
import json
import logging
import httpx
from typing import Dict, Any, List
from openai import AsyncOpenAI

logger = logging.getLogger("QUANT_CORE.AI_ROUTER")

class ResilientAIRouter:
    def __init__(self, nv_keys: List[str], deepseek_key: str):
        self.providers = []
        
        # 🛡️ HARDENED NETWORK CONFIGURATION FOR CLOUD DEPLOYMENT
        # Lowered timeout to 12.0 so the Cascade Router can failover rapidly.
        self.custom_http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(12.0, connect=5.0),
            http2=False,  # CRITICAL: Disabling HTTP/2 prevents shared-cloud connection drops
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
        )

        # 1. GROQ: The Speed King (Primary for HFT). ~800 tokens/sec. FREE.
        groq_key = os.getenv("GROQ_API_KEY")
        if groq_key:
            self.providers.append({
                "name": "GROQ_LLAMA_3_1_8B",
                "client": AsyncOpenAI(base_url="https://api.groq.com/openai/v1", api_key=groq_key, http_client=self.custom_http_client, max_retries=0),
                "model": "llama-3.1-8b-instant",
                "cooldown_until": 0.0
            })
            self.providers.append({
                "name": "GROQ_LLAMA_3_3_70B",
                "client": AsyncOpenAI(base_url="https://api.groq.com/openai/v1", api_key=groq_key, http_client=self.custom_http_client, max_retries=0),
                "model": "llama-3.3-70b-versatile",
                "cooldown_until": 0.0
            })

        # 2. NVIDIA NIM: Running DeepSeek V4-Flash (The Upgraded Institutional Failover)
        for i, key in enumerate(nv_keys):
            if key:
                self.providers.append({
                    "name": f"NVIDIA_NIM_DEEPSEEK_{i+1}",
                    "client": AsyncOpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=key, http_client=self.custom_http_client, max_retries=0),
                    "model": "deepseek-ai/deepseek-v4-flash",
                    "cooldown_until": 0.0
                })

        # 3. DEEPSEEK NATIVE: The Paid Last Resort
        if deepseek_key:
            self.providers.append({
                "name": "DEEPSEEK_V4_NATIVE",
                "client": AsyncOpenAI(base_url="https://api.deepseek.com/v1", api_key=deepseek_key, http_client=self.custom_http_client, max_retries=0),
                "model": "deepseek-chat",
                "cooldown_until": 0.0
            })

        if not self.providers:
            logger.critical("❌ NO AI PROVIDERS CONFIGURED. THE ROUTER IS BLIND.")
        else:
            logger.info(f"✅ Cascade Matrix initialized with {len(self.providers)} failover nodes (Hardened Network Mode Active)")

    def _get_next_healthy_provider(self):
        """Scans the matrix sequentially and retrieves the first API key that is not on a blackout cooldown."""
        current_time = time.time()
        for p in self.providers:
            if current_time >= p["cooldown_until"]:
                return p
        return None

    def _clean_json_output(self, raw_text: str) -> str:
        """Strips markdown formatting if the LLM wraps the JSON in code blocks."""
        if "```json" in raw_text:
            return raw_text.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_text:
            return raw_text.split("```")[1].split("```")[0].strip()
        return raw_text.strip()

    async def extract_market_verdict(self, batched_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Orchestrates structured intent extraction via Cascade Matrix.
        Implements Local Pre-Filtering and Context Caching to maximize API runway.
        """
        
        # 🚀 BUG FIX: Safely extract the ASSET_MATRIX to avoid parsing the GLOBAL_MACRO_NEWS string
        assets_data = batched_payload.get("ASSET_MATRIX", batched_payload)
        
        # ---------------------------------------------------------
        # 🛑 1. CONDITIONAL PRE-FILTER (Local Math Check)
        # ---------------------------------------------------------
        is_market_active = False
        for ticker, data in assets_data.items():
            # Ensure we are only looking at valid dictionary objects, not text strings
            if isinstance(data, dict):
                z_score = data.get("volatility_z_score", 0.0)
                if abs(z_score) >= 2.0:
                    is_market_active = True
                    break
        
        if not is_market_active:
            logger.info("💤 Local Pre-Filter: Matrix is flat (|Z| < 2.0). Skipping AI matrix to save rate limits.")
            return {symbol: {"direction": "HOLD", "confidence": 0.0} for symbol in assets_data.keys() if isinstance(assets_data[symbol], dict)}

        logger.info("🚨 Local Pre-Filter: Structural Anomaly Detected. Waking up AI Cascade Matrix...")

        # ---------------------------------------------------------
        # 🛑 2. STATIC SYSTEM PREFIX (Cache Target)
        # ---------------------------------------------------------
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
        prompt = f"LIVE MARKET DATA BATCH:\n{json.dumps(batched_payload, indent=2)}"

        messages = [
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": prompt}
        ]

        # ---------------------------------------------------------
        # 🔄 4. CASCADE FAILOVER LOOP
        # ---------------------------------------------------------
        for attempt in range(len(self.providers)):
            provider = self._get_next_healthy_provider()
            
            if not provider:
                logger.error("⚠️ ALL AI NODES ON COOLDOWN. Returning emergency HOLD matrix.")
                return {symbol: {"direction": "HOLD", "confidence": 0.0} for symbol in assets_data.keys() if isinstance(assets_data[symbol], dict)}

            logger.info(f"🧠 Routing inference to {provider['name']} [{provider['model']}]...")
            
            try:
                response = await provider["client"].chat.completions.create(
                    model=provider["model"],
                    messages=messages,
                    temperature=0.0,
                    top_p=0.95,
                    max_tokens=2048,
                    response_format={"type": "json_object"}
                )
                
                raw_content = response.choices[0].message.content
                cleaned_content = self._clean_json_output(raw_content)
                return json.loads(cleaned_content)
                
            except Exception as e:
                error_str = str(e).lower()
                logger.warning(f"⚠️ {provider['name']} Failed: {e}")
                
                # Dynamic Penalty Box Logic
                if "rate limit" in error_str or "429" in error_str or "413" in error_str:
                    penalty = 30.0
                elif "timeout" in error_str:
                    penalty = 15.0
                elif "connection" in error_str or "network" in error_str:
                    penalty = 20.0
                else:
                    penalty = 30.0
                    
                provider["cooldown_until"] = time.time() + penalty
                logger.info(f"🔄 Rotating to next provider. {provider['name']} placed in penalty box for {penalty}s.")

        logger.critical("🛑 Systemic AI Network Blackout. All Cascade providers failed.")
        return {symbol: {"direction": "HOLD", "confidence": 0.0} for symbol in assets_data.keys() if isinstance(assets_data[symbol], dict)}