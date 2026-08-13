"""
ðŸŒŒ V1.0 TITANIUM APEX: DECOUPLED L2 INGESTION LAYER
----------------------------------------------------
Features absolute sequence gap intolerance with Seamless REST Bridging,
Subscription Chunking (10-arg limit compliance), Pure JSON Pings,
C-Optimized Float Pre-Parsing, Institutional Load Shedding, 
and X-Ray Diagnostic Telemetry for High-Frequency Swarm execution.
"""

import asyncio
import aiohttp
import time
import logging
from typing import Dict, Any, Callable, Coroutine, List

# Attempt to load ultra-fast JSON parsers to reduce CPU deserialization drag, fallback to standard
try:
    import ujson as json
except ImportError:
    try:
        import orjson as json
    except ImportError:
        import json

logger = logging.getLogger("QUANT_CORE.MULTI_FEED")

class HighVelocityMultiFeed:
    """
    ðŸš€ V1.0 TITANIUM APEX: DECOUPLED INGESTION LAYER
    Maintains ultra-low latency WebSocket connections, decoupling raw ingestion
    from downstream processing via a high-capacity asynchronous FIFO queue.
    Upgraded with C-level list comprehensions for Orderbook deserialization.
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
        
        self.active_ws = None  # Reference to the active WebSocket for hot-swapping
        self._active_tasks = set()
        
        # High-Capacity Decoupling Queue (Prevents network backpressure)
        self.ingestion_queue = asyncio.Queue(maxsize=50000)

    def track_task(self, coro: Coroutine):
        """Safely tracks fire-and-forget daemon tasks to prevent GC mid-flight."""
        task = asyncio.create_task(coro)
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)
        return task

    async def _data_consumer_worker(self):
        """
        ðŸš€ DEDICATED CONSUMER
        Pulls from the high-speed FIFO queue and executes callbacks sequentially.
        Maintains strict chronological L2/L3 ordering without blocking network socket reading.
        """
        logger.info("[X-RAY] âš¡ V1.0 Decoupled Data Consumer Worker ONLINE.")
        while self.is_running:
            try:
                payload_type, payload_data = await self.ingestion_queue.get()
                
                # Ordered by highest frequency to optimize branching
                if payload_type == "orderbook":
                    await self.orderbook_callback(payload_data)
                elif payload_type == "trade" and self.trade_callback:
                    await self.trade_callback(payload_data)
                elif payload_type == "tickers":
                    await self.screener_callback(payload_data)
                elif payload_type == "kline":
                    await self.kline_callback(payload_data)
                    
                self.ingestion_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[X-RAY] âŒ Consumer worker failed to process payload: {e}", exc_info=True)

    async def hot_swap_socket_stream(self, drop_symbol: str, add_symbol: str):
        """
        ðŸš€ V1.0 OMNI-SWARM DYNAMIC HOT-SWAPPING
        Pushes chunked subscribe/unsubscribe JSON commands over the active WebSocket
        without needing to tear down the entire multiplexer connection.
        """
        if not self.active_ws or self.active_ws.closed:
            return

        unsub_args = [
            f"tickers.{drop_symbol}", 
            f"orderbook.50.{drop_symbol}", 
            f"publicTrade.{drop_symbol}"
        ] + [f"kline.{i}.{drop_symbol}" for i in self.intervals]

        sub_args = [
            f"tickers.{add_symbol}", 
            f"orderbook.50.{add_symbol}", 
            f"publicTrade.{add_symbol}"
        ] + [f"kline.{i}.{add_symbol}" for i in self.intervals]

        try:
            # Bybit Limit: Max 10 args per payload. Chunking required.
            for i in range(0, len(unsub_args), 10):
                await self.active_ws.send_json({"op": "unsubscribe", "args": unsub_args[i:i+10]})
            for i in range(0, len(sub_args), 10):
                await self.active_ws.send_json({"op": "subscribe", "args": sub_args[i:i+10]})
            
            # Clean up local sequence memory for dropped coin
            self.orderbook_sequences.pop(drop_symbol, None)
            
            logger.info(f"[X-RAY] ðŸ”„ Socket Hot-Swap Complete: Dropped {drop_symbol}, Added {add_symbol}")
        except Exception as e:
            logger.error(f"[X-RAY] âŒ Hot-swap socket injection failed: {e}")

    async def _resync_isolated_symbol(self, symbol: str):
        """
        ðŸš€ V1.0 ISOLATED SNAPSHOT RESYNC (Seamless REST Bridging)
        Immediately fetches a REST snapshot and injects it into the high-speed queue 
        when sequence gaps occur, preventing downstream MLOFI corruption.
        """
        if not self.active_ws or self.active_ws.closed:
            return
            
        logger.warning(f"[X-RAY] ðŸ”„ Requesting isolated snapshot resync for {symbol}...")
        self.orderbook_sequences.pop(symbol, None)
        
        try:
            # 1. Seamless REST Bridge
            if self.engine_reference and hasattr(self.engine_reference, "executor"):
                try:
                    rest_ob = await self.engine_reference.executor.safe_call(
                        self.engine_reference.executor.client.get_orderbook, 
                        category="linear", symbol=symbol
                    )
                    data = rest_ob.get("result", {})
                    if data and "b" in data and "a" in data:
                        parsed_bids = self._fast_float_parse_book(data.get("b", []))
                        parsed_asks = self._fast_float_parse_book(data.get("a", []))
                        
                        # Inject REST snapshot directly into the fast-queue safely
                        try:
                            self.ingestion_queue.put_nowait(("orderbook", {
                                "s": symbol, "b": parsed_bids, "a": parsed_asks, 
                                "u": int(data.get("u", 0)), "type": "snapshot", 
                                "ts": int(data.get("ts", time.time() * 1000))
                            }))
                        except asyncio.QueueFull:
                            logger.debug("[X-RAY] âš ï¸ Ingestion queue full during REST resync. Load shedding snapshot.")

                        # Re-anchor sequence state to the fresh REST snapshot
                        self.orderbook_sequences[symbol] = int(data.get("u", 0))
                except Exception as e:
                    logger.debug(f"[X-RAY] REST bridge failed during resync for {symbol}: {e}")

            # 2. Cycle WS without artificial sleep to repair the delta stream behind the scenes
            await self.active_ws.send_json({"op": "unsubscribe", "args": [f"orderbook.50.{symbol}"]})
            await self.active_ws.send_json({"op": "subscribe", "args": [f"orderbook.50.{symbol}"]})
        except Exception as e:
            logger.error(f"[X-RAY] âŒ Isolated resync request failed for {symbol}: {e}")

    def _fast_float_parse_book(self, levels: list) -> list:
        """
        ðŸš€ V1.0 UPGRADE: Pre-parses string lists into float arrays using 
        C-optimized list comprehensions to save downstream CPU cycles.
        """
        try:
            return [[float(lvl[0]), float(lvl[1])] for lvl in levels]
        except (IndexError, ValueError):
            # Fallback for malformed exchange payloads
            parsed = []
            for lvl in levels:
                try:
                    parsed.append([float(lvl[0]), float(lvl[1])])
                except (IndexError, ValueError):
                    pass
            return parsed

    async def initialize_multiplexed_stream(self):
        """Spawns concurrent asynchronous subscription worker processes."""
        self.is_running = True
        
        consumer_task = self.track_task(self._data_consumer_worker())
        
        args_payload = []
        for symbol in self.basket:
            args_payload.append(f"tickers.{symbol}")      
            args_payload.append(f"orderbook.50.{symbol}")  
            args_payload.append(f"publicTrade.{symbol}")   
            for interval in self.intervals:
                args_payload.append(f"kline.{interval}.{symbol}") 

        reconnect_delay = 1.0
        max_reconnect_delay = 30.0

        while self.is_running:
            watchdog_task = None
            self.orderbook_sequences.clear()
            
            try:
                logger.info(f"[X-RAY] ðŸ“¡ Opening high-speed multiplexed socket interface channel at: {self.ws_url}")
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ws_url) as ws:
                        
                        self.active_ws = ws
                        reconnect_delay = 1.0
                        self.last_msg_timestamp = time.time()
                        
                        async def connection_watchdog():
                            try:
                                while not ws.closed and self.is_running:
                                    await asyncio.sleep(20) # Bybit requires a ping exactly every 20s
                                    try:
                                        await ws.send_json({"req_id": str(int(time.time())), "op": "ping"})
                                        if time.time() - self.last_msg_timestamp > 45.0:
                                            logger.error("[X-RAY] ðŸš¨ WATCHDOG TRIGGERED: Silent flatline detected (No data for >45s). Severing zombie connection.")
                                            await ws.close()
                                            break
                                    except Exception as e:
                                        logger.debug(f"[X-RAY] Watchdog ping failed dynamically: {e}")
                                        break
                            except asyncio.CancelledError:
                                pass
                                    
                        watchdog_task = self.track_task(connection_watchdog())

                        # Bybit Limit: Max 10 args per request. We must chunk the payload.
                        chunk_size = 10
                        for i in range(0, len(args_payload), chunk_size):
                            chunk = args_payload[i:i + chunk_size]
                            await ws.send_json({"op": "subscribe", "args": chunk})
                            await asyncio.sleep(0.05) # Prevent connection flooding
                            
                        logger.info(f"[X-RAY] âœ… Successfully multiplexed topics (chunked) for tracking matrix: {len(self.basket)} nodes.")

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

                                try:
                                    # Route incoming bytes to FIFO queue in O(1) time
                                    # Ordered by highest frequency updates first
                                    if topic.startswith("orderbook"):
                                        symbol = data.get("s")
                                        u_sequence = data.get("u")
                                        msg_type = payload.get("type", "delta")
                                        
                                        if msg_type == "snapshot":
                                            self.orderbook_sequences[symbol] = u_sequence
                                        elif msg_type == "delta":
                                            last_seq = self.orderbook_sequences.get(symbol)
                                            prev_seq = data.get("pu")  
                                            
                                            # ZERO SEQUENCE GAP TOLERANCE
                                            if last_seq is not None and prev_seq is not None:
                                                if prev_seq != last_seq:
                                                    logger.critical(f"[X-RAY] âŒ SEVERE SEQUENCE BREAK // {symbol} (Gap | PrevSeq:{prev_seq} != Stored:{last_seq}). Initiating seamless REST resync.")
                                                    self.track_task(self._resync_isolated_symbol(symbol))
                                                    continue # Skip processing this broken delta
                                                    
                                            self.orderbook_sequences[symbol] = u_sequence
                                            
                                        parsed_bids = self._fast_float_parse_book(data.get("b", []))
                                        parsed_asks = self._fast_float_parse_book(data.get("a", []))

                                        try:
                                            self.ingestion_queue.put_nowait(("orderbook", {
                                                "s": symbol, "b": parsed_bids, "a": parsed_asks, "u": u_sequence, "type": msg_type, "ts": payload.get("ts", time.time()*1000)
                                            }))
                                        except asyncio.QueueFull:
                                            logger.debug("[X-RAY] âš ï¸ Ingestion queue full. Shedding orderbook tick.")
                                            
                                    elif topic.startswith("publicTrade"):
                                        symbol = topic.split(".")[-1]
                                        for tick in data:
                                            tick_payload = {
                                                "symbol": symbol,
                                                "price": float(tick.get("p", 0.0)),
                                                "size": float(tick.get("v", 0.0)),
                                                "side": tick.get("S", "Buy"),
                                                "timestamp": float(tick.get("T", time.time() * 1000))
                                            }
                                            try:
                                                self.ingestion_queue.put_nowait(("trade", tick_payload))
                                            except asyncio.QueueFull:
                                                logger.debug("[X-RAY] âš ï¸ Ingestion queue full. Shedding trade tick.")
                                                
                                    elif topic.startswith("tickers"):
                                        try:
                                            self.ingestion_queue.put_nowait(("tickers", data))
                                        except asyncio.QueueFull:
                                            logger.debug("[X-RAY] âš ï¸ Ingestion queue full. Shedding tickers tick.")
                                            
                                    elif topic.startswith("kline"):
                                        try:
                                            self.ingestion_queue.put_nowait(("kline", {
                                                "interval": topic.split(".")[1], "symbol": topic.split(".")[2], "candle_data": data[0]
                                            }))
                                        except asyncio.QueueFull:
                                            logger.debug("[X-RAY] âš ï¸ Ingestion queue full. Shedding kline tick.")

                                except Exception as e:
                                    logger.error(f"[X-RAY] ðŸš¨ OVERLOAD OR ROUTING ERROR: {e}")
                                            
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                                
                        if watchdog_task and not watchdog_task.done():
                            watchdog_task.cancel()
                        
            except Exception as e:
                logger.error(f"[X-RAY] âŒ Critical connection failure caught in multiplex ingestion loop: {e}", exc_info=True)
                
            if not self.is_running:
                break
                
            self.active_ws = None
            logger.warning(f"[X-RAY] âš ï¸ Ingestion link down. Reconnecting via backoff protocol in {reconnect_delay:.2f}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(max_reconnect_delay, reconnect_delay * 1.5)

        if consumer_task and not consumer_task.done():
            consumer_task.cancel()

    def terminate_all_feeds(self):
        """Performs structural teardown actions across active streaming context pipelines."""
        self.is_running = False
        logger.warning("[X-RAY] ðŸ›‘ Terminating multiplexed ingestion pipelines cleanly.")
        
        for task in list(self._active_tasks):
            if not task.done():
                task.cancel()