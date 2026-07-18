import os
import json
import time
import asyncio
import logging
from typing import Dict, Any
from groq import AsyncGroq

logger = logging.getLogger("QUANT_CORE.ADVERSARIAL_AI")

class AdversarialDebateMatrix:
    def __init__(self):
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            logger.critical("⚠️ Configuration Fault: GROQ_API_KEY missing.")
            
        self.client = AsyncGroq(api_key=api_key)
        self.model = "llama-3.3-70b-versatile"
        self.debate_history = [] # Temporal Memory Loopback

    def _calibrate_confidence(self, vpin_data: dict, judge_confidence: float, dna_stats: dict) -> float:
        """
        Dynamically calibrates AI confidence against hard mathematical realities (KNN + VPIN Z-Scores).
        """
        calibrated = judge_confidence
        vpin_z = vpin_data.get("vpin_z_score", 0.0)
        dna_win_rate = dna_stats.get("cluster_win_rate", 0.50)

        # 1. Structural Penalties
        if abs(vpin_z) < 1.8:
            calibrated *= 0.8  # Penalize weak anomaly signals
        if dna_win_rate < 0.52:
            calibrated *= 0.7  # Penalize if historically similar setups lost money

        # 2. Absorption Penalty (If volume pushes but price refuses to move)
        if vpin_data.get("is_absorption_anomaly"):
            calibrated *= 0.6  # Heavy penalty: High risk of a whale trap

        # 3. Structural Boosts
        if abs(vpin_z) >= 3.0 and dna_win_rate >= 0.60:
            calibrated = min(1.0, calibrated * 1.25)

        return round(calibrated, 4)

    async def _query_agent(self, role_prompt: str, data_context: str, timeout: float = 12.0, require_json: bool = False) -> str:
        """
        🚀 APEX UPGRADE: Hardware-Level JSON Enforcement
        Uses Groq's native response_format to guarantee pure JSON outputs for the Judge,
        eliminating the need for fragile regex stripping.
        """
        messages = [
            {"role": "system", "content": role_prompt},
            {"role": "user", "content": data_context}
        ]
        
        kwargs = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.1,  # Ultra-low temperature for strict logical deduction
            "max_tokens": 400
        }
        
        if require_json:
            kwargs["response_format"] = {"type": "json_object"}

        try:
            response = await asyncio.wait_for(
                self.client.chat.completions.create(**kwargs),
                timeout=timeout
            )
            return response.choices[0].message.content
        except asyncio.TimeoutError:
            logger.error("⏳ AI Agent Query Timed Out.")
            return "TIMEOUT"
        except Exception as e:
            logger.error(f"❌ AI Agent API Error: {e}")
            return "NODE_FAULT"

    def _get_temporal_memory(self, symbol: str) -> str:
        """Retrieves the last 3 decisions for this asset to prevent AI logic oscillation."""
        recent = [d for d in self.debate_history if d['symbol'] == symbol][-3:]
        if not recent:
            return "No recent rulings for this asset."
            
        memory_str = "\n".join([f"- {int(time.time() - d['timestamp'])}s ago: {d['action']} (VPIN: {d['vpin']:.2f})" for d in recent])
        return memory_str

    async def execute_debate_cycle(self, symbol: str, vpin_data: dict, dna_stats: dict, macro_context: str) -> Dict[str, Any]:
        """
        Runs full multi-agent debate layered with KNN neighborhood data, Systemic Context, 
        and the new VPIN Whale Absorption Metrics.
        """
        vpin_score = vpin_data.get('vpin_score', 0)
        vpin_z = vpin_data.get('vpin_z_score', 0)
        directional_bias = vpin_data.get('directional_bias', 0)
        suggested_dir = vpin_data.get('suggested_direction', 'HOLD')
        price = vpin_data.get('current_price', 0)
        
        # 🚀 APEX UPGRADE: Extracting new Footprint metrics
        is_absorption = vpin_data.get('is_absorption_anomaly', False)
        avg_trade_size = vpin_data.get('avg_trade_size', 0)
        absorption_str = "CRITICAL ALERT: Hidden Whale Limit Wall absorbing all volume." if is_absorption else "Normal Flow"

        # Compile consolidated data context sheet
        context = (
            f"--- MICROSTRUCTURE DATA ---\n"
            f"Asset Node: {symbol} | Price: {price}\n"
            f"VPIN Score: {vpin_score} | Anomaly Z-Score: {vpin_z} SD\n"
            f"Directional Imbalance: {directional_bias} ({suggested_dir} Pressure)\n"
            f"Absorption State: {absorption_str}\n"
            f"Avg Trade Size (Footprint): {avg_trade_size}\n"
            f"--- STRUCTURAL NEIGHBORHOOD HISTORY (KNN) ---\n"
            f"Database Matched Profiles: {dna_stats.get('matched_samples', 0)}\n"
            f"Historical Match Win-Rate: {dna_stats.get('cluster_win_rate', 0.50):.2%}\n"
            f"--- SYSTEMIC CONTEXT ---\n"
            f"{macro_context}\n"
            f"--- TEMPORAL MEMORY (Past Rulings) ---\n"
            f"{self._get_temporal_memory(symbol)}"
        )

        # Phase 1: Predator
        predator_prompt = (
            "You are the Predator. Analyze the VPIN toxicity metrics, institutional footprint (trade size), "
            "and the KNN historical win rate. Build a concise, highly aggressive 3-sentence trading thesis "
            "explaining why market microstructure proves informed flow is on our side. Demand execution."
        )
        predator_thesis = await self._query_agent(predator_prompt, context)
        
        if predator_thesis in ["NODE_FAULT", "TIMEOUT"]:
            return self._execute_deterministic_fallback("Predator Defect", vpin_data)

        # Phase 2: Skeptic
        skeptic_prompt = (
            f"You are the Skeptic. The Predator just claimed: '{predator_thesis}'. "
            "Your objective is to destroy this argument. Focus heavily on the 'Absorption State' and KNN win rates. "
            "If an Absorption Anomaly is active, argue that the Predator is walking into a whale's iceberg trap. Be brutal. 3 sentences max."
        )
        skeptic_critique = await self._query_agent(skeptic_prompt, context)
        
        if skeptic_critique in ["NODE_FAULT", "TIMEOUT"]:
            return self._execute_deterministic_fallback("Skeptic Defect", vpin_data)

        # Phase 3: Judge (Now armed with Native JSON format)
        judge_prompt = (
            "You are the Chief Investment Officer. Weigh the Predator's structural expansion thesis against the "
            "Skeptic's risk-quarantine critique. Factor in your Temporal Memory (Past Rulings) to maintain logical consistency. "
            "Output strictly a JSON object. Ensure the output is valid JSON.\n"
            "Format required: {\"action\": \"BUY\" | \"SELL\" | \"HOLD\", \"confidence\": float between 0.0 and 1.0, \"reasoning\": \"1 sentence summary\"}"
        )
        debate_log = f"Debate Layer for {symbol}:\n\nPredator:\n{predator_thesis}\n\nSkeptic:\n{skeptic_critique}"
        
        raw_verdict = await self._query_agent(judge_prompt, debate_log, require_json=True)

        try:
            # No regex stripping required. Groq guarantees pure JSON.
            verdict_json = json.loads(raw_verdict)
            
            # Calibrate confidence using raw metrics layer
            raw_conf = float(verdict_json.get("confidence", 0.50))
            calibrated_conf = self._calibrate_confidence(vpin_data, raw_conf, dna_stats)
            verdict_json["confidence"] = calibrated_conf
            
            # Record tracking entry for memory context logs (Keep last 100 total)
            self.debate_history.append({
                "timestamp": time.time(), "symbol": symbol, "action": verdict_json.get("action"), "vpin": vpin_score
            })
            if len(self.debate_history) > 100:
                self.debate_history.pop(0)
            
            return verdict_json
            
        except json.JSONDecodeError as json_err:
            logger.error(f"❌ JSON Matrix Fault: {json_err} | Raw payload: {raw_verdict}")
            return self._execute_deterministic_fallback("Parsing Failure", vpin_data)
        except Exception as e:
            logger.error(f"❌ Judge Evaluation Fault: {e}")
            return self._execute_deterministic_fallback("Judge Exception", vpin_data)

    def _execute_deterministic_fallback(self, fail_reason: str, vpin_data: dict) -> Dict[str, Any]:
        """Processes clear non-LLM decision matrix fallback rules."""
        logger.warning(f"⚠️ Circuit Breaker Triggered: {fail_reason}. Initiating math fallback protocol.")
        vpin_z = vpin_data.get("vpin_z_score", 0.0)
        directional_bias = vpin_data.get("directional_bias", 0.0)

        # If API dies, only trade on extremely obvious systemic imbalances
        if abs(vpin_z) >= 2.5 and abs(directional_bias) >= 0.08 and not vpin_data.get("is_absorption_anomaly"):
            action = "BUY" if directional_bias > 0 else "SELL"
            confidence = min(0.65, abs(vpin_z) / 4.5)
            return {
                "action": action,
                "confidence": round(confidence, 4),
                "reasoning": f"VPIN High-Imbalance Fallback (Circuit: {fail_reason})"
            }
            
        return {"action": "HOLD", "confidence": 0.0, "reasoning": f"Fallback Engine: No absolute edge under {fail_reason} state."}