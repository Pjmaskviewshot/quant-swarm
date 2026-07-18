import os
import re
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
        
        # 🛡️ HARDENED NETWORK CONFIGURATION (Increased limits to prevent Event Loop Choke)
        self.custom_http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            http2=False,  
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50) 
        )

        # 1. GROQ: The Speed King 
        groq_key = os.getenv("GROQ_API_KEY")
        if groq_key:
            self.providers.append({
                "name": "GROQ_LLAMA_3_3_70B",
                "client": AsyncOpenAI(base_url="https://api.groq.com/openai/v1", api_key=groq_key, http_client=self.custom_http_client, max_retries=0),
                "model": "llama-3.3-70b-versatile",
                "cooldown_until": 0.0,
                "last_used": 0.0,
                "json_mode": True,
                "params": {"temperature": 0.1, "top_p": 0.95, "max_tokens": 2048}
            })
            self.providers.append({
                "name": "GROQ_LLAMA_3_1_8B",
                "client": AsyncOpenAI(base_url="https://api.groq.com/openai/v1", api_key=groq_key, http_client=self.custom_http_client, max_retries=0),
                "model": "llama-3.1-8b-instant",
                "cooldown_until": 0.0,
                "last_used": 0.0,
                "json_mode": True,
                "params": {"temperature": 0.1, "top_p": 0.95, "max_tokens": 2048}
            })

        # 2. NVIDIA NIM: 🚀 APEX UPGRADE -> DeepSeek V4 Flash with Reasoning Engine
        for i, key in enumerate(nv_keys):
            if key:
                self.providers.append({
                    "name": f"NVIDIA_NIM_DEEPSEEK_FLASH_{i+1}",
                    "client": AsyncOpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=key, http_client=self.custom_http_client, max_retries=0),
                    "model": "deepseek-ai/deepseek-v4-flash",
                    "cooldown_until": 0.0,
                    "last_used": 0.0,
                    "json_mode": False, # DeepSeek reasoning works best when extracting JSON from markdown fences
                    "params": {
                        "temperature": 1.0, 
                        "top_p": 0.95, 
                        "max_tokens": 8192, 
                        "extra_body": {"chat_template_kwargs": {"thinking": True, "reasoning_effort": "high"}}
                    }
                })

        # 3. DEEPSEEK NATIVE: The Paid Last Resort
        if deepseek_key:
            self.providers.append({
                "name": "DEEPSEEK_V4_FLASH",
                "client": AsyncOpenAI(base_url="https://api.deepseek.com/v1", api_key=deepseek_key, http_client=self.custom_http_client, max_retries=0),
                "model": "deepseek-v4-flash", 
                "cooldown_until": 0.0,
                "last_used": 0.0,
                "json_mode": False,
                "params": {
                    "temperature": 1.0, 
                    "top_p": 0.95, 
                    "max_tokens": 8192
                }
            })

        if not self.providers:
            logger.critical("❌ NO AI PROVIDERS CONFIGURED. THE ROUTER IS BLIND.")
        else:
            logger.info(f"✅ Universal Cascade Matrix initialized with {len(self.providers)} failover nodes.")

    def _sanitize_error(self, error_str: str) -> str:
        return re.sub(r'(gsk_[a-zA-Z0-9]{20,}|sk-[a-zA-Z0-9]{20,}|nvapi-[a-zA-Z0-9-_]{20,})', '[REDACTED_API_KEY]', error_str)

    def _get_next_healthy_provider(self):
        """
        🚀 APEX UPGRADE: Least-Recently-Used (LRU) Round-Robin Routing.
        Prevents Node Starvation. 
        """
        current_time = time.time()
        healthy_providers = [p for p in self.providers if current_time >= p["cooldown_until"]]
        
        if healthy_providers:
            healthy_providers.sort(key=lambda x: x.get("last_used", 0.0))
            selected = healthy_providers[0]
            selected["last_used"] = current_time
            return selected
            
        return None

    def _clean_json_output(self, raw_text: str) -> str:
        if "```json" in raw_text:
            return raw_text.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_text:
            return raw_text.split("```")[1].split("```")[0].strip()
        return raw_text.strip()

    async def execute_inference(self, messages: List[Dict[str, str]], require_json: bool = False, timeout: float = 15.0) -> str:
        # Reduced max attempts to 4 to fail fast and trigger mathematical fallbacks
        MAX_ATTEMPTS = 4 
        
        for attempt in range(MAX_ATTEMPTS):
            provider = self._get_next_healthy_provider()
            
            if not provider:
                logger.warning(f"⏳ Tactical Pause: All cascade nodes in penalty box. Retrying in 5s (Attempt {attempt+1}/{MAX_ATTEMPTS})...")
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
                
                # 🚀 APEX UPGRADE: Inject NVIDIA Extra Body params for DeepSeek Reasoning
                if "extra_body" in provider.get("params", {}):
                    kwargs["extra_body"] = provider["params"]["extra_body"]
                
                if require_json and provider.get("json_mode", False):
                    kwargs["response_format"] = {"type": "json_object"}

                response = await asyncio.wait_for(
                    provider["client"].chat.completions.create(**kwargs),
                    timeout=timeout
                )
                
                msg_obj = response.choices[0].message
                
                # 🚀 APEX UPGRADE: Capture DeepSeek's internal reasoning chain (CoT)
                reasoning = getattr(msg_obj, "reasoning", None) or getattr(msg_obj, "reasoning_content", None)
                if reasoning:
                    logger.debug(f"🧠 {provider['name']} Deep-Thought Logic: {reasoning[:150]}...")
                
                raw_content = msg_obj.content
                
                if require_json and not provider.get("json_mode", False):
                    return self._clean_json_output(raw_content)
                    
                return raw_content
                
            except asyncio.TimeoutError:
                # Harsh 60-second penalty for Timeouts to prevent Death Loops
                logger.warning(f"⚠️ {provider['name']} Timed Out after {timeout}s. Penalizing node for 60s.")
                provider["cooldown_until"] = time.time() + 60.0
                
            except Exception as e:
                error_str = self._sanitize_error(str(e).lower())
                logger.warning(f"⚠️ {provider['name']} Inference Failed: {error_str}")
                
                # Dynamic Penalty Box Logic
                if "rate limit" in error_str or "429" in error_str or "413" in error_str:
                    penalty = 60.0
                elif "connection" in error_str or "network" in error_str:
                    penalty = 20.0
                else:
                    penalty = 45.0
                    
                provider["cooldown_until"] = time.time() + penalty
                logger.info(f"🔄 Rotating matrix. {provider['name']} placed in penalty box for {penalty}s.")

        logger.critical("🛑 Systemic AI Network Blackout. Exhausted all cascade retries.")
        return "NODE_FAULT"