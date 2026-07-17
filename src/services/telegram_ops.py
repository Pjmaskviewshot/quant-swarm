import aiohttp
import logging

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
        return error_msg.replace(self.token, "********")

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
                        logger.error(self._sanitize_error(f"Telegram remote rejection payload received: {raw_err}"))
        except Exception as e:
            logger.error(self._sanitize_error(f"Unable to cleanly resolve connection context to Telegram API infrastructure: {e}"))

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
                        
                        # 🛑 HTML FALLBACK PATCH: Catch formatting rejections
                        if response.status == 400:
                            logger.warning(self._sanitize_error(f"Telegram rejected HTML format. Falling back to plain text. Error: {raw_err}"))
                            
                            # Strip standard HTML tags from the message
                            clean_msg = html_text.replace("<b>", "").replace("</b>", "")\
                                                 .replace("<code>", "").replace("</code>", "")\
                                                 .replace("<i>", "").replace("</i>", "")
                            
                            fallback_payload = {
                                "chat_id": self.chat_id,
                                "text": clean_msg,
                                "parse_mode": ""  # Send as raw text
                            }
                            
                            async with session.post(self.base_url, json=fallback_payload, timeout=10.0) as fb_response:
                                if fb_response.status != 200:
                                    fb_err = await fb_response.text()
                                    logger.error(self._sanitize_error(f"Telegram also rejected plain-text fallback: {fb_err}"))
                        else:
                            logger.error(self._sanitize_error(f"Telegram API rejected HTML payload: {raw_err}"))
                            
        except Exception as e:
            logger.error(self._sanitize_error(f"Failed to establish connection to Telegram API for HTML report: {e}"))