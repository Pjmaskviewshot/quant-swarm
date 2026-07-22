import os
import re
import asyncio
import aiohttp
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger("QUANT_CORE.TELEGRAM")

class AsyncTelegramReporter:
    """
    🚀 V26.0 APEX: ASYNCHRONOUS TELEGRAM REPORTER
    Upgraded with persistent TCP ClientSession connection pooling (eliminates session-churn memory leaks),
    dynamic HTTP 429 `retry_after` backoff handling, HTML tag stripping fallbacks, and token scrubbing.
    """
    def __init__(self, token: str, chat_id: str):
        self.token = token or ""
        self.chat_id = chat_id or ""
        self.base_url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazy initialization of persistent aiohttp session for high-throughput connection pooling."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10.0)
            )
        return self._session

    async def close(self):
        """
        🚀 V26 UPGRADE: Resource Cleanup
        Gracefully closes persistent HTTP session during main daemon teardown.
        """
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("🔌 Telegram Reporter HTTP session gracefully closed.")

    def _sanitize_error(self, error_msg: str) -> str:
        """
        🛡️ SECRETS HYGIENE
        Scrubs token from plain-text server logs to prevent credential leakage.
        """
        if not self.token:
            return str(error_msg)
        return str(error_msg).replace(self.token, "********")

    def _strip_html(self, text: str) -> str:
        """Sanitizes payloads by stripping HTML tags if Telegram rejects formatting."""
        cleaner = re.compile(r'<.*?>')
        return re.sub(cleaner, '', text)

    async def _dispatch_payload(self, payload: Dict[str, Any], max_retries: int = 3) -> bool:
        """
        Core request worker with dynamic HTTP 429 backoff support and token protection.
        """
        if not self.token or not self.chat_id:
            logger.warning("Telegram credentials unpopulated. Skipping dispatch.")
            return False

        session = await self._get_session()

        for attempt in range(max_retries):
            try:
                async with session.post(self.base_url, json=payload) as response:
                    if response.status == 200:
                        return True

                    raw_err = await response.text()

                    # ⚡ V26 UPGRADE: Dynamic HTTP 429 Rate-Limit Handling
                    if response.status == 429:
                        try:
                            err_json = await response.json()
                            retry_after = float(err_json.get("parameters", {}).get("retry_after", 2.0))
                        except Exception:
                            retry_after = 2.0
                        logger.warning(f"⚠️ Telegram Rate Limit hit. Backing off for {retry_after:.1f}s...")
                        await asyncio.sleep(retry_after)
                        continue

                    # Fallback for parse errors (HTTP 400 Bad Request)
                    if response.status == 400 and "parse" in raw_err.lower():
                        logger.warning("Telegram rejected formatting. Falling back to plain text.")
                        payload["text"] = self._strip_html(payload.get("text", ""))
                        payload["parse_mode"] = ""
                        continue

                    logger.error(self._sanitize_error(f"Telegram remote rejection (HTTP {response.status}): {raw_err}"))

            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(self._sanitize_error(f"❌ Telegram API permanently unreachable: {e}"))
                else:
                    sleep_time = 2.0 ** attempt
                    logger.warning(self._sanitize_error(f"⚠️ Telegram network fault: {e}. Retrying in {sleep_time}s..."))
                    await asyncio.sleep(sleep_time)

        return False

    async def log_message(self, text: str, alert_level: str = "INFO", max_retries: int = 3):
        """Fires markdown-formatted alert downstream."""
        emojis = {"INFO": "ℹ️", "SUCCESS": "🟢", "WARNING": "⚠️", "CRITICAL": "🚨"}
        prefix = emojis.get(str(alert_level).upper(), "🤖")

        payload = {
            "chat_id": self.chat_id,
            "text": f"{prefix} *[SYSTEM ALERT]*\n\n{text}",
            "parse_mode": "Markdown"
        }
        await self._dispatch_payload(payload, max_retries=max_retries)

    async def send_html_report(self, html_text: str, max_retries: int = 3):
        """Dispatches HTML payloads to Telegram with auto-retry and plain-text fallback."""
        payload = {
            "chat_id": self.chat_id,
            "text": html_text,
            "parse_mode": "HTML"
        }
        await self._dispatch_payload(payload, max_retries=max_retries)