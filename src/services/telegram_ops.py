import os
import re
import asyncio
import aiohttp
import logging
from typing import Optional

logger = logging.getLogger("QUANT_CORE.TELEGRAM")

class AsyncTelegramReporter:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}/sendMessage"

    def _sanitize_error(self, error_msg: str) -> str:
        """
        🛑 P2-5 FIX: SECRETS HYGIENE
        Scrubs the Telegram bot token from log outputs to prevent aiohttp 
        exceptions from leaking credentials into plain-text server logs.
        """
        if not self.token:
            return error_msg
        return str(error_msg).replace(self.token, "********")

    def _strip_html(self, text: str) -> str:
        """
        🚀 V5 UPGRADE: Robust regex stripper to completely sanitize payloads
        if Telegram rejects our HTML formatting.
        """
        cleaner = re.compile('<.*?>')
        return re.sub(cleaner, '', text)

    async def log_message(self, text: str, alert_level: str = "INFO", max_retries: int = 3):
        """Fires fully compiled markdown alerts downstream with Exponential Backoff."""
        if not self.token or not self.chat_id:
            logger.warning("Telegram configuration unpopulated. Aborting reporting pipeline step.")
            return

        emojis = {"INFO": "ℹ️", "SUCCESS": "🟢", "WARNING": "⚠️", "CRITICAL": "🚨"}
        prefix = emojis.get(alert_level.upper(), "🤖")
        
        payload = {
            "chat_id": self.chat_id,
            "text": f"{prefix} *[SYSTEM ALERT]*\n\n{text}",
            "parse_mode": "Markdown"
        }

        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(self.base_url, json=payload, timeout=5.0) as response:
                        if response.status == 200:
                            return  # Success! Exit the loop.
                            
                        raw_err = await response.text()
                        
                        # If Telegram rejects the Markdown format (HTTP 400), strip it and retry instantly
                        if response.status == 400 and "parse" in raw_err.lower():
                            logger.warning("Telegram rejected Markdown. Falling back to plain text.")
                            payload["parse_mode"] = ""
                            continue
                            
                        logger.error(self._sanitize_error(f"Telegram remote rejection: {raw_err}"))
                        
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(self._sanitize_error(f"❌ Telegram API permanently unreachable: {e}"))
                else:
                    sleep_time = 2 ** attempt
                    logger.warning(self._sanitize_error(f"⚠️ Telegram network fault: {e}. Retrying in {sleep_time}s..."))
                    await asyncio.sleep(sleep_time)

    async def send_html_report(self, html_text: str, max_retries: int = 3):
        """Dispatches HTML payloads to Telegram with auto-retry and plain-text fallback."""
        if not self.token or not self.chat_id:
            logger.warning("Telegram configuration unpopulated. Aborting HTML report dispatch.")
            return

        payload = {
            "chat_id": self.chat_id,
            "text": html_text,
            "parse_mode": "HTML"
        }
        
        for attempt in range(max_retries):
            try:
                # Using 10s timeout for the heavier HTML payload
                async with aiohttp.ClientSession() as session:
                    async with session.post(self.base_url, json=payload, timeout=10.0) as response:
                        if response.status == 200:
                            return  # Success! Exit the loop.
                            
                        raw_err = await response.text()
                        
                        # 🛑 HTML FALLBACK PATCH: Catch formatting rejections
                        if response.status == 400 and "parse" in raw_err.lower():
                            logger.warning(self._sanitize_error(f"Telegram rejected HTML format. Stripping tags and retrying. Error: {raw_err}"))
                            
                            # Instantly convert the payload to clean plain-text
                            payload["text"] = self._strip_html(html_text)
                            payload["parse_mode"] = "" 
                            continue # Try again immediately with the cleaned text
                            
                        logger.error(self._sanitize_error(f"Telegram API rejected payload: {raw_err}"))
                        
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(self._sanitize_error(f"❌ Failed to establish connection to Telegram API: {e}"))
                else:
                    sleep_time = 2 ** attempt
                    logger.warning(self._sanitize_error(f"⚠️ Telegram network drop. Retrying in {sleep_time}s..."))
                    await asyncio.sleep(sleep_time)