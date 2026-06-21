import asyncio
import aiohttp
import json
import time
import logging
from typing import Dict, Any, Callable, Coroutine, List

logger = logging.getLogger("QUANT_CORE.MULTI_FEED")

class HighVelocityMultiFeed:
    def __init__(
        self, 
        basket: List[str], 
        intervals: List[str], 
        orderbook_callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]], 
        screener_callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]],
        kline_callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]
    ):
        # Format the basket matrix natively
        self.basket = [symbol.upper() for symbol in basket]
        self.intervals = intervals
        
        # Thread Callbacks
        self.orderbook_callback = orderbook_callback
        self.screener_callback = screener_callback
        self.kline_callback = kline_callback
        
        self.ws_url = "wss://stream.bybit.com/v5/public/linear"
        self.is_running = False
        
        # 🛡️ UPGRADE: Timestamp tracker for the Resiliency Watchdog
        self.last_msg_timestamp = time.time()

    async def initialize_multiplexed_stream(self):
        """Spawns concurrent asynchronous subscription worker processes for the entire asset basket."""
        self.is_running = True
        
        # Build institutional subscription map configurations dynamically
        args_payload = []
        for symbol in self.basket:
            args_payload.append(f"tickers.{symbol}")       # Lightweight Screener Feed
            args_payload.append(f"orderbook.50.{symbol}")  # Heavy Microstructure Feed
            for interval in self.intervals:
                args_payload.append(f"kline.{interval}.{symbol}") # Multi-Timeframe Momentum Feed

        subscription_request = {
            "op": "subscribe",
            "args": args_payload
        }

        while self.is_running:
            try:
                logger.info(f"Opening high-speed multiplexed socket interface channel at: {self.ws_url}")
                async with aiohttp.ClientSession() as session:
                    # 🛡️ UPGRADE: We keep the basic aiohttp heartbeat, but we will add an explicit Application Watchdog
                    async with session.ws_connect(self.ws_url, heartbeat=20.0) as ws:
                        
                        # 🚀 PHASE 4 ADVANCEMENT: THE RESILIENCY WATCHDOG
                        # This internal task guarantees the socket never turns into a "Zombie Connection".
                        async def connection_watchdog():
                            while not ws.closed and self.is_running:
                                await asyncio.sleep(20)
                                try:
                                    # Ping the Bybit server explicitly
                                    await ws.send_json({"req_id": str(int(time.time())), "op": "ping"})
                                    
                                    # Check if the connection has flatlined silently
                                    if time.time() - self.last_msg_timestamp > 45.0:
                                        logger.error("🚨 WATCHDOG TRIGGERED: Silent flatline detected (No data for >45s). Severing zombie connection.")
                                        await ws.close()
                                        break
                                except Exception as e:
                                    logger.debug(f"Watchdog ping failed dynamically: {e}")
                                    break
                                    
                        watchdog_task = asyncio.create_task(connection_watchdog())

                        await ws.send_str(json.dumps(subscription_request))
                        logger.info(f"Successfully multiplexed topics for tracking matrix: {self.basket}")
                        
                        self.last_msg_timestamp = time.time()

                        async for msg in ws:
                            # 🛡️ UPGRADE: Reset the watchdog timer every time ANY data arrives
                            self.last_msg_timestamp = time.time()
                            
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                payload = json.loads(msg.data)
                                
                                # Intercept and ignore Application Pongs
                                if payload.get("op") == "pong" or payload.get("ret_msg") == "pong":
                                    continue
                                    
                                topic: str = payload.get("topic", "")
                                data = payload.get("data")

                                if not data:
                                    continue

                                # Route incoming bytes instantly to the correct processing channel thread
                                if topic.startswith("tickers"):
                                    await self.screener_callback(data)
                                elif topic.startswith("orderbook"):
                                    await self.orderbook_callback(data)
                                elif topic.startswith("kline"):
                                    # Normalize candle metadata envelope structure for the orchestrator
                                    await self.kline_callback({
                                        "interval": topic.split(".")[1],
                                        "symbol": topic.split(".")[2],
                                        "candle_data": data[0]
                                    })
                                    
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                logger.warning("Multiplexed WebSocket transport socket severed. Initializing recovery link.")
                                break
                                
                        # Clean up the watchdog when the socket loop drops
                        watchdog_task.cancel()
                        
            except Exception as e:
                logger.error(f"Critical connection failure caught in multiplex ingestion loop: {e}")
                
            # Cool down connection matrix before initiating a clean hot-reboot cycle
            await asyncio.sleep(4)

    def terminate_all_feeds(self):
        """Performs structural teardown actions across active streaming context pipelines."""
        self.is_running = False
        logger.warning("Terminating multiplexed ingestion pipelines cleanly.")