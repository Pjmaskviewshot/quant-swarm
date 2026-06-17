import json
import logging
from typing import Dict, Any, List
from openai import AsyncOpenAI

logger = logging.getLogger("QUANT_CORE.AI_ROUTER")

class ResilientAIRouter:
    def __init__(self, nv_keys: List[str], deepseek_key: str):
        # Filter empty keys
        self.nv_keys = [k for k in nv_keys if k]
        self.deepseek_key = deepseek_key
        self.active_key_index = 0
        
        # Instantiate Async Clients
        self.nvidia_base_url = "https://integrate.api.nvidia.com/v1"
        self.deepseek_base_url = "https://api.deepseek.com/v1"

    def _get_nvidia_client(self) -> AsyncOpenAI:
        """Dynamically loads the active NVIDIA key to allow for hot-swapping."""
        return AsyncOpenAI(
            base_url=self.nvidia_base_url,
            api_key=self.nv_keys[self.active_key_index]
        )

    def _rotate_nvidia_key(self):
        """Swaps to the backup key if the primary hits a rate limit."""
        if len(self.nv_keys) > 1:
            self.active_key_index = (self.active_key_index + 1) % len(self.nv_keys)
            logger.warning(f"Rotated to backup NVIDIA API Key index: {self.active_key_index}")

    async def extract_market_verdict(self, batched_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Orchestrates structured intent extraction via DeepSeek V4 Pro.
        Processes a BATCHED payload of multiple assets in a single API call to bypass rate limits.
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

        # Strategy 1: Attempt NVIDIA NIM Cluster (DeepSeek V4 Pro)
        for attempt in range(len(self.nv_keys)):
            try:
                client = self._get_nvidia_client()
                response = await client.chat.completions.create(
                    model="deepseek-ai/deepseek-v4-pro",
                    messages=messages,
                    temperature=0.1,  # CRITICAL: Kept low for deterministic JSON stability
                    top_p=0.95,
                    max_tokens=2048,  # Increased to handle the larger response size of a 15-asset JSON dict
                    response_format={"type": "json_object"},
                    extra_body={"chat_template_kwargs": {"thinking": False}} # Disabled chain-of-thought for latency speed
                )
                
                content = response.choices[0].message.content
                return json.loads(content)
                
            except Exception as e:
                logger.error(f"NVIDIA Inference failure on key index {self.active_key_index}: {e}")
                self._rotate_nvidia_key()

        # Strategy 2: Absolute Cascade Fallback to Native DeepSeek Servers
        logger.critical("NVIDIA NIM Cluster fully exhausted. Routing batched query to native DeepSeek API...")
        try:
            backup_client = AsyncOpenAI(
                base_url=self.deepseek_base_url,
                api_key=self.deepseek_key
            )
            
            response = await backup_client.chat.completions.create(
                model="deepseek-chat", # Standard DeepSeek API model name
                messages=messages,
                temperature=0.1,
                top_p=0.95,
                max_tokens=2048, # Increased
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            return json.loads(content)
            
        except Exception as e:
            logger.critical(f"Systemic AI Network Blackout. DeepSeek failed: {e}")
            # Dynamic Fallback: Safely construct a "HOLD" dictionary for every coin in the original batch request
            return {symbol: {"direction": "HOLD", "confidence": 0.0} for symbol in batched_payload.keys()}