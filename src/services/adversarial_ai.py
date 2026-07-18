import os
import re
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
            logger.critical("激活 Configuration Fault: GROQ_API_KEY missing.")
        self.client = AsyncGroq(api_key=api_key)
        self.model = "llama-3.3-70b-versatile"
        self.debate_history = [] # In-memory track for temporal context

    def _calibrate_confidence(self, vpin_data: dict, judge_confidence: float, dna_stats: dict) -> float:
        """
        🚀 REVIEW ENHANCEMENT 1: Dynamic Confidence Calibration
        Cross-references Judge output with raw VPIN metrics and KNN historical data.
        """
        calibrated = judge_confidence
        vpin_z = vpin_data.get("vpin_z_score", 0.0)
        dna_win_rate = dna_stats.get("cluster_win_rate", 0.50)

        # Penalty Matrix
        if abs(vpin_z) < 1.8:
            calibrated *= 0.8  # Penalize low anomaly signals
        if dna_win_rate < 0.52:
            calibrated *= 0.7  # Heavy structural penalty if neighborhood data is weak

        # Boost Matrix
        if abs(vpin_z) >= 3.0 and dna_win_rate >= 0.60:
            calibrated = min(1.0, calibrated * 1.25)

        return round(calibrated, 4)

    async def _query_agent(self, role_prompt: str, data_context: str, timeout: float = 12.0) -> str:
        messages = [
            {"role": "system", "content": role_prompt},
            {"role": "user", "content": data_context}
        ]
        try:
            response = await asyncio.wait_for(
                self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=0.1,
                    max_tokens=400
                ),
                timeout=timeout
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Agent prompt error: {e}")
            return "NODE_FAULT"

    async def execute_debate_cycle(self, symbol: str, vpin_data: dict, dna_stats: dict, macro_context: str) -> Dict[str, Any]:
        """
        Runs full multi-agent debate layered with KNN neighborhood data and systemic context.
        """
        vpin_score = vpin_data.get('vpin_score', 0)
        vpin_z = vpin_data.get('vpin_z_score', 0)
        directional_bias = vpin_data.get('directional_bias', 0)
        suggested_dir = vpin_data.get('suggested_direction', 'HOLD')
        price = vpin_data.get('current_price', 0)

        # Compile consolidated data context sheet
        context = (
            f"--- MICROSTRUCTURE DATA ---\n"
            f"Asset Node: {symbol} | Price: {price}\n"
            f"VPIN Score: {vpin_score} | Anomaly Z-Score: {vpin_z} SD\n"
            f"Directional Imbalance: {directional_bias} ({suggested_dir} Pressure)\n"
            f"--- STRUCTURAL NEIGHBORHOOD HISTORY (KNN) ---\n"
            f"Database Matched Profiles: {dna_stats.get('matched_samples', 0)}\n"
            f"Historical Match Win-Rate: {dna_stats.get('cluster_win_rate', 0.50):.2%}\n"
            f"--- SYSTEMIC SYSTEM CONTEXT ---\n"
            f"{macro_context}"
        )

        # Phase 1: Predator
        predator_prompt = (
            "You are the Predator. Analyze the VPIN toxicity metrics and the KNN historical neighborhood win rate. "
            "Build a concise, highly aggressive 3-sentence trading thesis explaining why market microstructure proves "
            "informed institutional flow is on our side. Demand execution."
        )
        predator_thesis = await self._query_agent(predator_prompt, context)
        
        # 🛡️ REVIEW ENHANCEMENT 4: Hard Deterministic Fallback Circuit Breaker
        if predator_thesis in ["NODE_FAULT", "TIMEOUT"]:
            return self._execute_deterministic_fallback("Predator Defect", vpin_data)

        # Phase 2: Skeptic
        skeptic_prompt = (
            f"You are the Skeptic. The Predator just claimed: '{predator_thesis}'. "
            "Your sole objective is to logically destroy this argument. Use the systemic market context and spread fields "
            "to prove why this signal is likely a toxic liquidity trap or high-frequency spoofing wall. 3 sentences max."
        )
        skeptic_critique = await self._query_agent(skeptic_prompt, context)
        if skeptic_critique in ["NODE_FAULT", "TIMEOUT"]:
            return self._execute_deterministic_fallback("Skeptic Defect", vpin_data)

        # Phase 3: Judge
        judge_prompt = (
            "You are the Chief Investment Officer. Weigh the Predator's structural expansion thesis against the "
            "Skeptic's risk-quarantine critique. Evaluate the KNN win-rate base baseline. "
            "Output strictly a PURE JSON string. Do not include markdown codeblocks or backticks.\n"
            "Format: {\"action\": \"BUY\" | \"SELL\" | \"HOLD\", \"confidence\": float, \"reasoning\": \"string\"}"
        )
        debate_log = f"Debate Layer:\nPredator: {predator_thesis}\n\nSkeptic: {skeptic_critique}"
        
        raw_verdict = await self._query_agent(judge_prompt, debate_log)
        clean_verdict = re.sub(r'```json|```', '', raw_verdict).strip()

        try:
            verdict_json = json.loads(clean_verdict)
            
            # Calibrate confidence using raw metrics layer
            raw_conf = float(verdict_json.get("confidence", 0.50))
            calibrated_conf = self._calibrate_confidence(vpin_data, raw_conf, dna_stats)
            verdict_json["confidence"] = calibrated_conf
            
            # Record tracking entry for memory context logs
            self.debate_history.append({
                "timestamp": time.time(), "symbol": symbol, "action": verdict_json.get("action"), "vpin": vpin_score
            })
            
            return verdict_json
        except Exception as json_err:
            logger.error(f"Judge JSON mutation error: {json_err} | Raw payload: {raw_verdict}")
            return self._execute_deterministic_fallback("Parsing Failure", vpin_data)

    def _execute_deterministic_fallback(self, fail_reason: str, vpin_data: dict) -> Dict[str, Any]:
        """Processes clear non-LLM decision matrix fallback rules."""
        logger.warning(f"⚠️ Circuit Breaker Triggered: {fail_reason}. Initiating math fallback protocol.")
        vpin_z = vpin_data.get("vpin_z_score", 0.0)
        directional_bias = vpin_data.get("directional_bias", 0.0)

        if abs(vpin_z) >= 2.5 and abs(directional_bias) >= 0.08:
            action = "BUY" if directional_bias > 0 else "SELL"
            confidence = min(0.65, abs(vpin_z) / 4.5)
            return {
                "action": action,
                "confidence": round(confidence, 4),
                "reasoning": f"VPIN High-Imbalance Fallback (Circuit: {fail_reason})"
            }
        return {"action": "HOLD", "confidence": 0.0, "reasoning": f"Fallback Engine: No absolute edge under {fail_reason} state."}