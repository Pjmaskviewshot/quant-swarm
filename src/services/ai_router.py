import os
import re
import json
import time
import asyncio
import logging
import httpx
from typing import Dict, Any, List
from openai import AsyncOpenAI

logger = logging.getLogger("QUANT_CORE.AI_ROUTER")

class ResilientAIRouter:
    def __init__(self, nv_keys: List[str], deepseek_key: str):
        self.providers = []
        self.current_provider = "INITIALIZING" 
        
        # 🛡️ HARDENED NETWORK CONFIGURATION FOR CLOUD DEPLOYMENT
        self.custom_http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            http2=False,  # CRITICAL: Disabling HTTP/2 prevents shared-cloud connection drops
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
        )

        # 1. GROQ: The Speed King (Primary for HFT). ~800 tokens/sec. FREE.
        groq_key = os.getenv("GROQ_API_KEY")
        if groq_key:
            self.providers.append({
                "name": "GROQ_LLAMA_3_3_70B",
                "client": AsyncOpenAI(base_url="https://api.groq.com/openai/v1", api_key=groq_key, http_client=self.custom_http_client, max_retries=0),
                "model": "llama-3.3-70b-versatile",
                "cooldown_until": 0.0,
                "json_mode": True,
                "params": {"temperature": 0.1, "top_p": 0.95, "max_tokens": 2048}
            })
            self.providers.append({
                "name": "GROQ_LLAMA_3_1_8B",
                "client": AsyncOpenAI(base_url="https://api.groq.com/openai/v1", api_key=groq_key, http_client=self.custom_http_client, max_retries=0),
                "model": "llama-3.1-8b-instant",
                "cooldown_until": 0.0,
                "json_mode": True,
                "params": {"temperature": 0.1, "top_p": 0.95, "max_tokens": 2048}
            })

        # 2. NVIDIA NIM: Running Moonshot Kimi K2.6 (Institutional Heavy-Lifter)
        for i, key in enumerate(nv_keys):
            if key:
                self.providers.append({
                    "name": f"NVIDIA_NIM_KIMI_{i+1}",
                    "client": AsyncOpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=key, http_client=self.custom_http_client, max_retries=0),
                    "model": "moonshotai/kimi-k2.6",
                    "cooldown_until": 0.0,
                    "json_mode": False, # 🚀 Bypassed to prevent 400 Bad Request errors on Kimi
                    "params": {"temperature": 0.1, "top_p": 0.10, "max_tokens": 8192}
                })

        # 3. DEEPSEEK NATIVE: The Paid Last Resort
        if deepseek_key:
            self.providers.append({
                "name": "DEEPSEEK_NATIVE",
                "client": AsyncOpenAI(base_url="https://api.deepseek.com/v1", api_key=deepseek_key, http_client=self.custom_http_client, max_retries=0),
                "model": "deepseek-chat", 
                "cooldown_until": 0.0,
                "json_mode": True,
                "params": {"temperature": 0.1, "top_p": 0.95, "max_tokens": 2048}
            })

        if not self.providers:
            logger.critical("❌ NO AI PROVIDERS CONFIGURED. THE ROUTER IS BLIND.")
        else:
            logger.info(f"✅ Universal Cascade Matrix initialized with {len(self.providers)} failover nodes.")

    def _sanitize_error(self, error_str: str) -> str:
        """🚀 SECURITY FIX: Redacts any string that looks like an API key to prevent log leakage."""
        return re.sub(r'(gsk_[a-zA-Z0-9]{20,}|sk-[a-zA-Z0-9]{20,}|nvapi-[a-zA-Z0-9-_]{20,})', '[REDACTED_API_KEY]', error_str)

    def _get_next_healthy_provider(self):
        """Scans the matrix and skips nodes currently serving time in the penalty box."""
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

    async def execute_inference(self, messages: List[Dict[str, str]], require_json: bool = False, timeout: float = 12.0) -> str:
        """
        🚀 V6 APEX: Universal Inference Engine
        Replaces the obsolete V5 batched-dictionary system. Now serves as the indestructible 
        backend for the Adversarial Debate Matrix (Predator/Skeptic/Judge).
        """
        MAX_ATTEMPTS = 10 
        
        for attempt in range(MAX_ATTEMPTS):
            provider = self._get_next_healthy_provider()
            
            if not provider:
                logger.warning(f"⏳ Tactical Pause: All cascade nodes cooling down. Retrying in 5s (Attempt {attempt+1}/{MAX_ATTEMPTS})...")
                await asyncio.sleep(5)
                continue

            self.current_provider = provider['name'] 
            
            try:
                kwargs = {
                    "model": provider["model"],
                    "messages": messages,
                    "temperature": provider.get("params", {}).get("temperature", 0.1),
                    "top_p": provider.get("params", {}).get("top_p", 0.95),
                    "max_tokens": provider.get("params", {}).get("max_tokens", 2048),
                }
                
                # Apply OpenAI Strict JSON structure only if the model explicitly supports it AND it's requested by the Agent
                if require_json and provider.get("json_mode", False):
                    kwargs["response_format"] = {"type": "json_object"}

                response = await asyncio.wait_for(
                    provider["client"].chat.completions.create(**kwargs),
                    timeout=timeout
                )
                
                raw_content = response.choices[0].message.content
                
                if require_json and not provider.get("json_mode", False):
                    # Clean markdown manually for models (like Kimi) that don't support hardware JSON mode
                    return self._clean_json_output(raw_content)
                    
                return raw_content
                
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ {provider['name']} Timed Out after {timeout}s. Penalizing node.")
                provider["cooldown_until"] = time.time() + 15.0
                
            except Exception as e:
                error_str = self._sanitize_error(str(e).lower())
                logger.warning(f"⚠️ {provider['name']} Inference Failed: {error_str}")
                
                # Dynamic Penalty Box Logic based on API failure types
                if "rate limit" in error_str or "429" in error_str or "413" in error_str:
                    penalty = 30.0
                elif "connection" in error_str or "network" in error_str:
                    penalty = 20.0
                else:
                    penalty = 30.0
                    
                provider["cooldown_until"] = time.time() + penalty
                logger.info(f"🔄 Rotating matrix. {provider['name']} placed in penalty box for {penalty}s.")

        logger.critical("🛑 Systemic AI Network Blackout. Exhausted all cascade retries.")
        return "NODE_FAULT"