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

    async def extract_market_verdict(self, market_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Orchestrates structured intent extraction via DeepSeek V4 Pro.
        Forces the LLM to act strictly as a deterministic quant engine.
        """
        prompt = f"""
        Execute quantitative matrix analysis on the following integrated streaming data parameters:
        {json.dumps(market_payload, indent=2)}

        Determine market structural execution matrix direction for the upcoming execution cycle window.
        """
        
        system_instruction = (
            "You are an institutional algorithmic execution core. Analyze market context and return "
            "ONLY a verified JSON object. Do not include markdown formatting tags, wrapping blocks, or prose. "
            "Format: {\"direction\": \"BUY\"|\"SELL\"|\"HOLD\", \"confidence\": float from 0.0 to 1.0}"
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
                    max_tokens=1024,
                    response_format={"type": "json_object"},
                    extra_body={"chat_template_kwargs": {"thinking": False}} # Disabled chain-of-thought for latency speed
                )
                
                content = response.choices[0].message.content
                return json.loads(content)
                
            except Exception as e:
                logger.error(f"NVIDIA Inference failure on key index {self.active_key_index}: {e}")
                self._rotate_nvidia_key()

        # Strategy 2: Absolute Cascade Fallback to Native DeepSeek Servers
        logger.critical("NVIDIA NIM Cluster fully exhausted. Routing query to native DeepSeek API...")
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
                max_tokens=1024,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            return json.loads(content)
            
        except Exception as e:
            logger.critical(f"Systemic AI Network Blackout. DeepSeek failed: {e}")
            return {"direction": "HOLD", "confidence": 0.0}