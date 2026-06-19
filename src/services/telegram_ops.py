import aiohttp
import logging

logger = logging.getLogger("QUANT_CORE.TELEGRAM")

class AsyncTelegramReporter:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}/sendMessage"

    async def log_message(self, text: str, alert_level: str = "INFO"):
        """Fires fully compiled markdown alerts downstream to your mobile instance."""
        if not self.token or not self.chat_id:
            logger.warning("Telegram configuration unpopulated. Aborting reporting pipeline step.")
            return

        emojis = {"INFO": "ℹ️", "SUCCESS": "🟢", "WARNING": "⚠️", "CRITICAL": "🚨"}
        prefix = emojis.get(alert_level.upper(), "🤖")
        
        formatted_payload = {
            "chat_id": self.chat_id,
            "text": f"{prefix} *[SYSTEM ALERT]*\n\n{text}",
            "parse_mode": "Markdown"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.base_url, json=formatted_payload, timeout=4.0) as response:
                    if response.status != 200:
                        raw_err = await response.text()
                        logger.error(f"Telegram remote rejection payload received: {raw_err}")
        except Exception as e:
            logger.error(f"Unable to cleanly resolve connection context to Telegram API infrastructure: {e}")

    async def send_html_report(self, html_text: str):
        """Dispatches an explicitly formatted HTML payload to Telegram (used for hourly metrics)."""
        if not self.token or not self.chat_id:
            logger.warning("Telegram configuration unpopulated. Aborting HTML report dispatch.")
            return

        payload = {
            "chat_id": self.chat_id,
            "text": html_text,
            "parse_mode": "HTML"
        }
        
        try:
            # Using a slightly longer timeout (10s) for the heavier HTML payload
            async with aiohttp.ClientSession() as session:
                async with session.post(self.base_url, json=payload, timeout=10.0) as response:
                    if response.status != 200:
                        raw_err = await response.text()
                        logger.error(f"Telegram API rejected HTML payload: {raw_err}")
        except Exception as e:
            logger.error(f"Failed to establish connection to Telegram API for HTML report: {e}")