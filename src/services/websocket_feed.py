import asyncio
import aiohttp
import json
import logging
from typing import Callable, Coroutine, Any

logger = logging.getLogger("QUANT_CORE.WEBSOCKET")

class BybitWebSocketClient:
    def __init__(self, symbol: str, callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]):
        self.symbol = symbol.upper()
        self.callback = callback
        self.ws_url = "wss://stream.bybit.com/v5/public/linear"
        self.is_running = False

    async def monitor_stream_pipeline(self):
        """Maintains persistent automated network subscription reconnect loops indefinitely."""
        self.is_running = True
        subscription_payload = {
            "op": "subscribe",
            "args": [f"orderbook.50.{self.symbol}"]
        }

        while self.is_running:
            try:
                logger.info(f"Establishing primary market data streaming socket interface to: {self.ws_url}")
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ws_url, heartbeat=20.0) as ws:
                        # Dispatch authentication/subscription payloads
                        await ws.send_str(json.dumps(subscription_payload))
                        logger.info(f"Subscription matrix registered for topic: orderbook.50.{self.symbol}")

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                # Filter heartbeat responses or operation ack envelopes
                                if "data" in data:
                                    await self.callback(data["data"])
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                logger.warning("WebSocket transport pipeline connection torn down. Reconnecting...")
                                break
            except Exception as e:
                logger.error(f"Connection exception caught inside WebSocket transport layer: {e}")
                
            # Cool down connection matrix before initiating a clean hot-reboot cycle
            await asyncio.sleep(5)

    def terminate_stream(self):
        self.is_running = False