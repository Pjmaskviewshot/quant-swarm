"""
🚀 V50.0 QUANTUM SWARM: TELEGRAM MISSION CONTROL
------------------------------------------------
Upgraded with persistent TCP connection pooling, dynamic HTTP 429 backoff,
and institutional-grade Forensic X-Ray HTML formatters.
"""

import os
import re
import asyncio
import aiohttp
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger("QUANT_CORE.TELEGRAM")

class AsyncTelegramReporter:
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
        """Gracefully closes persistent HTTP session during main daemon teardown."""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("🔌 Telegram Reporter HTTP session gracefully closed.")

    def _sanitize_error(self, error_msg: str) -> str:
        """Scrubs token from plain-text server logs to prevent credential leakage."""
        if not self.token:
            return str(error_msg)
        return str(error_msg).replace(self.token, "********")

    def _strip_html(self, text: str) -> str:
        """Sanitizes payloads by stripping HTML tags if Telegram rejects formatting."""
        cleaner = re.compile(r'<.*?>')
        return re.sub(cleaner, '', text)

    async def _dispatch_payload(self, payload: Dict[str, Any], max_retries: int = 3) -> bool:
        """Core request worker with dynamic HTTP 429 backoff support and token protection."""
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

                    # Dynamic HTTP 429 Rate-Limit Handling
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

    # ====================================================================
    # 🚀 V50.0 APEX: X-RAY FORENSIC FORMATTERS
    # ====================================================================

    def format_entry_ticket(self, symbol: str, direction: str, price: float, size: float, edge_bps: float, risk_pct: float, regime: str, features: Dict[str, Any]) -> str:
        """Formats the Deep-Dive Entry Ticket with X-Ray Diagnostics."""
        
        # Extract X-Ray Metrics
        notional_value = price * size
        sl_price = features.get("virtual_sl", price)
        sl_pct = (abs(price - sl_price) / price) if price > 0 else 0.0
        spread_bps = features.get("bid_ask_spread", 0.0) * 10000.0
        
        micro_status = "STABLE"
        mlofi_z = features.get('adaptive_obi_z', 0.0)
        if mlofi_z > 2.0: micro_status = "TOXIC BUY PRESSURE"
        elif mlofi_z < -2.0: micro_status = "TOXIC SELL PRESSURE"
        elif "DARK_POOL" in features.get('reasoning', ''): micro_status = "ICEBERG ABSORPTION"
        elif "MAKER_ONLY" in features.get('reasoning', ''): micro_status = "WIDE SPREAD (MAKER PEG)"

        return (
            f"🎯 <b>X-RAY DISPATCH // {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"• Action: <b>{direction}</b>\n"
            f"• Fill Price: <code>{price:.5f}</code>\n"
            f"• Position: <code>{size:.4f} units (${notional_value:.2f})</code>\n"
            f"• Sizing Risk: <code>{risk_pct:.2%} Equity</code>\n\n"
            f"🔬 <b>X-RAY DIAGNOSTICS:</b>\n"
            f"• HMM Regime: <code>{regime}</code>\n"
            f"• Net Edge (EV): <code>{edge_bps:.1f} bps</code>\n"
            f"• Est. Spread: <code>{spread_bps:.1f} bps</code>\n"
            f"• Stop Loss: <code>{sl_pct:.2%}</code>\n"
            f"• Depth Radar: <code>{micro_status}</code>"
        )

    def format_execution_receipt(self, symbol: str, net_pnl: float, slippage_bps: float, fees: float, duration_mins: float, is_win: bool) -> str:
        """Formats the Post-Trade Autopsy Receipt upon position closure."""
        gross_pnl = net_pnl + fees + (abs(slippage_bps)/10000 * net_pnl)
        outcome_emoji = "🟢 WIN" if is_win else "🔴 LOSS"
        
        return (
            f"🔬 <b>POST-TRADE AUTOPSY // {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"• Outcome: <b>{outcome_emoji}</b>\n"
            f"• Net PnL: <code>{net_pnl:+.4f} USDT</code>\n\n"
            f"📊 <b>EXECUTION METRICS:</b>\n"
            f"• Time in Market: <code>{duration_mins:.1f} mins</code>\n"
            f"• Gross PnL: <code>{gross_pnl:+.4f} USDT</code>\n"
            f"• Maker/Taker Fees: <code>-{fees:.4f} USDT</code>\n"
            f"• Slippage Drag: <code>{slippage_bps:.1f} bps</code>"
        )

    def format_mission_control_dashboard(self, uptime: float, live_count: int, shadow_count: int, balance: float, session_pnl: float, drawdown: float, dd_bar: str, execution_stats: Dict[str, Any]) -> str:
        """Formats the 10-Minute Mission Control Heartbeat for V50.0."""
        win_rate = execution_stats.get('win_rate', 0.0)
        trades = execution_stats.get('trade_count', 0)
        avg_slip = execution_stats.get('avg_slippage_bps', 0.0)
        
        tox_radar = "SAFE"
        if avg_slip > 5.0: tox_radar = "ELEVATED SLIPPAGE"
        if drawdown > 0.10: tox_radar = "SYSTEMIC DRAWDOWN"
        
        return (
            f"💎 <b>QUANTUM SWARM (V50.0 APEX)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱️ <b>Uptime:</b> <code>{uptime:.2f} Hours</code>\n"
            f"🛰️ <b>Swarm Status:</b> <code>[{live_count} Live | {shadow_count} Shadow]</code>\n\n"
            f"💵 <b>FINANCIAL VAULT</b>\n"
            f"• Total Liquidity: <code>{balance:.4f} USDT</code>\n"
            f"• Session Return:  <code>{session_pnl:+.4f} USDT</code>\n"
            f"• Peak Drawdown:   <code>{drawdown:.2%}</code>\n"
            f"• Risk Buffer:     <code>[{dd_bar}]</code>\n\n"
            f"🔬 <b>TODAY's EXECUTION METRICS</b>\n"
            f"• Trades Settled: <code>{trades}</code>\n"
            f"• Live Win Rate: <code>{win_rate:.1%}</code>\n"
            f"• Avg Slippage: <code>{avg_slip:.1f} bps</code>\n"
            f"• Toxicity Radar: <code>{tox_radar}</code>"
        )