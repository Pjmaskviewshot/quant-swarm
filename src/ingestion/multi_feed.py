import asyncio
import aiohttp
import json
import time
import logging
from typing import Dict, Any, Callable, Coroutine, List

logger = logging.getLogger("QUANT_CORE.MULTI_FEED")

class HighVelocityMultiFeed:
    """
    🚀 V20.2 GENESIS: PRODUCTION INGESTION LAYER
    A pure, high-speed multiplexed data pipe. 
    Strict chronological awaits implemented to prevent L2/L3 state race conditions.
    """
    def __init__(
        self, 
        basket: List[str], 
        intervals: List[str], 
        orderbook_callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]], 
        screener_callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]],
        kline_callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]],
        trade_callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]] = None, 
        engine_reference: Any = None 
    ):
        self.basket = [symbol.upper() for symbol in basket]
        self.intervals = intervals
        
        self.orderbook_callback = orderbook_callback
        self.screener_callback = screener_callback
        self.kline_callback = kline_callback
        self.trade_callback = trade_callback 
        self.engine_reference = engine_reference
        
        self.ws_url = "wss://stream.bybit.com/v5/public/linear"
        self.is_running = False
        self.last_msg_timestamp = time.time()
        self.orderbook_sequences: Dict[str, int] = {}
        
        self._active_tasks = set()

    def track_task(self, coro: Coroutine):
        """Safely tracks fire-and-forget daemon tasks to prevent GC mid-flight."""
        task = asyncio.create_task(coro)
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)
        return task

    async def initialize_multiplexed_stream(self):
        """Spawns concurrent asynchronous subscription worker processes for the entire asset basket."""
        self.is_running = True
        
        args_payload = []
        for symbol in self.basket:
            args_payload.append(f"tickers.{symbol}")      
            args_payload.append(f"orderbook.50.{symbol}")  
            args_payload.append(f"publicTrade.{symbol}")   
            for interval in self.intervals:
                args_payload.append(f"kline.{interval}.{symbol}") 

        subscription_request = {
            "op": "subscribe",
            "args": args_payload
        }

        # 🚀 Exponential Backoff Reconnect Guard to prevent connection storms
        reconnect_delay = 1.0
        max_reconnect_delay = 30.0

        while self.is_running:
            watchdog_task = None
            try:
                logger.info(f"Opening high-speed multiplexed socket interface channel at: {self.ws_url}")
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ws_url, heartbeat=20.0) as ws:
                        
                        # Connection successful: Reset exponential backoff delay
                        reconnect_delay = 1.0
                        
                        async def connection_watchdog():
                            try:
                                while not ws.closed and self.is_running:
                                    await asyncio.sleep(20)
                                    try:
                                        await ws.send_json({"req_id": str(int(time.time())), "op": "ping"})
                                        if time.time() - self.last_msg_timestamp > 45.0:
                                            logger.error("🚨 WATCHDOG TRIGGERED: Silent flatline detected (No data for >45s). Severing zombie connection.")
                                            await ws.close()
                                            break
                                    except Exception as e:
                                        logger.debug(f"Watchdog ping failed dynamically: {e}")
                                        break
                            except asyncio.CancelledError:
                                # Normal behavior when the socket drops and we cancel the watchdog
                                pass
                                    
                        watchdog_task = self.track_task(connection_watchdog())

                        await ws.send_str(json.dumps(subscription_request))
                        logger.info(f"Successfully multiplexed topics for tracking matrix: {self.basket}")
                        
                        self.last_msg_timestamp = time.time()

                        async for msg in ws:
                            self.last_msg_timestamp = time.time()
                            
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                payload = json.loads(msg.data)
                                
                                if payload.get("op") == "ping" or payload.get("ret_msg") == "pong":
                                    continue
                                    
                                topic: str = payload.get("topic", "")
                                data = payload.get("data")

                                if not data:
                                    continue

                                # 🚀 Route incoming bytes instantly to the correct processing channel
                                if topic.startswith("tickers"):
                                    await self.screener_callback(data)
                                    
                                elif topic.startswith("orderbook"):
                                    symbol = data.get("s")
                                    u_sequence = data.get("u")
                                    msg_type = payload.get("type", "delta")
                                    
                                    if msg_type == "snapshot":
                                        self.orderbook_sequences[symbol] = u_sequence
                                    elif msg_type == "delta":
                                        last_u = self.orderbook_sequences.get(symbol)
                                        # 🚀 V20.2 FIX: Changed <= to < to survive Bybit's duplicate heartbeat sequence numbers
                                        if last_u is not None and u_sequence < last_u:
                                            logger.critical(f"⚠️ SEQUENCE ANOMALY // {symbol} Orderbook dropped a packet (Got u:{u_sequence} < Last:{last_u}). Forcing clean disconnect recovery.")
                                            await ws.close()
                                            break
                                        self.orderbook_sequences[symbol] = u_sequence

                                    await self.orderbook_callback({
                                        "s": symbol, "b": data.get("b", []), "a": data.get("a", []), "u": u_sequence, "type": msg_type
                                    })
                                    
                                elif topic.startswith("kline"):
                                    await self.kline_callback({
                                        "interval": topic.split(".")[1], "symbol": topic.split(".")[2], "candle_data": data[0]
                                    })
                                    
                                # ⚡ PURE HFT PIPELINE: Raw tick feeding directly to V20.2 Core
                                elif topic.startswith("publicTrade"):
                                    symbol = topic.split(".")[-1]
                                    
                                    for tick in data:
                                        if self.trade_callback:
                                            tick_payload = {
                                                "symbol": symbol,
                                                "price": float(tick.get("p", 0.0)),
                                                "size": float(tick.get("v", 0.0)),
                                                "side": tick.get("S", "Buy"),
                                                "timestamp": float(tick.get("T", time.time() * 1000))
                                            }
                                            # 🚀 V20.2 FIX: Await sequentially to preserve strict chronological order.
                                            # No async tasks. Prevents lethal race conditions in state updates.
                                            await self.trade_callback(tick_payload)
                                            
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                                
                        if watchdog_task and not watchdog_task.done():
                            watchdog_task.cancel()
                        
            except Exception as e:
                logger.error(f"Critical connection failure caught in multiplex ingestion loop: {e}", exc_info=True)
                
            if not self.is_running:
                break
                
            logger.warning(f"⚠️ Ingestion link down. Reconnecting via backoff protocol in {reconnect_delay:.2f}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(max_reconnect_delay, reconnect_delay * 1.5)

    def terminate_all_feeds(self):
        """Performs structural teardown actions across active streaming context pipelines."""
        self.is_running = False
        logger.warning("Terminating multiplexed ingestion pipelines cleanly.")
        
        # Clean up any lingering background tasks
        for task in list(self._active_tasks):
            if not task.done():
                task.cancel()