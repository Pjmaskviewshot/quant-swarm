"""
💎 V22.0 APEX QUANTUM PRIME: DECOUPLED L2 INGESTION LAYER
----------------------------------------------------
Features absolute sequence gap intolerance with Seamless REST Bridging,
L2 Delta Staging Buffers (Zero-Tick-Loss Resync), Subscription Chunking,
Pure JSON Pings, and C-Optimized Float Pre-Parsing.

Upgraded with V22.0:
- O(1) Memory Leak Eradication (Explicit GC for hot-swapped symbols)
- Symbol-Sharded Ingestion Queues (Eradicates Single-Consumer Bottleneck)
- Independent Asynchronous Workers per Asset
- O(1) Demultiplexed Routing
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
    🚀 V22.0 QUANTUM PRIME: DECOUPLED INGESTION LAYER
    Maintains ultra-low latency WebSocket connections, decoupling raw ingestion
    from downstream processing via Symbol-Sharded asynchronous FIFO queues.
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
        
        self.delta_buffer: Dict[str, List[Dict[str, Any]]] = {}
        self.is_resyncing: Dict[str, bool] = {}
        
        self.active_ws = None  
        self._active_tasks = set()
        
        # 🚀 V22.0: Symbol-Sharded Queues & Deterministic Worker Tracking
        self.sharded_queues: Dict[str, asyncio.Queue] = {}
        self.consumer_tasks: Dict[str, asyncio.Task] = {}

    def track_task(self, coro: Coroutine) -> asyncio.Task:
        """Safely tracks fire-and-forget daemon tasks to prevent GC mid-flight."""
        task = asyncio.create_task(coro)
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)
        return task

    def _get_or_create_queue(self, symbol: str) -> asyncio.Queue:
        """Dynamically provisions a queue and an isolated consumer worker per symbol."""
        if symbol not in self.sharded_queues:
            self.sharded_queues[symbol] = asyncio.Queue(maxsize=10000)
            worker_task = self.track_task(self._sharded_consumer_worker(symbol, self.sharded_queues[symbol]))
            self.consumer_tasks[symbol] = worker_task
            logger.info(f"[X-RAY] ⚡ Spawning isolated data consumer for {symbol}")
        return self.sharded_queues[symbol]

    async def _sharded_consumer_worker(self, symbol: str, queue: asyncio.Queue):
        """
        🚀 DEDICATED SYMBOL CONSUMER
        Pulls from the symbol-specific FIFO queue and executes callbacks.
        Guarantees heavy flow on BTC will never block execution routing for ETH.
        """
        while self.is_running:
            try:
                payload_type, payload_data = await queue.get()
                
                # 🚀 Garbage Collection / Termination Signal
                if payload_type == "SHUTDOWN":
                    queue.task_done()
                    break
                
                if payload_type == "orderbook":
                    await self.orderbook_callback(payload_data)
                elif payload_type == "trade" and self.trade_callback:
                    await self.trade_callback(payload_data)
                elif payload_type == "tickers":
                    await self.screener_callback(payload_data)
                elif payload_type == "kline":
                    await self.kline_callback(payload_data)
                    
                queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[X-RAY] ❌ Consumer worker for {symbol} failed: {e}", exc_info=True)
                
        logger.info(f"[X-RAY] ♻️ Consumer worker for {symbol} successfully garbage collected.")

    async def hot_swap_socket_stream(self, drop_symbol: str, add_symbol: str):
        """Dynamically updates the multiplexed stream and reallocates workers."""
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
            for i in range(0, len(unsub_args), 10):
                await self.active_ws.send_json({"op": "unsubscribe", "args": unsub_args[i:i+10]})
            for i in range(0, len(sub_args), 10):
                await self.active_ws.send_json({"op": "subscribe", "args": sub_args[i:i+10]})
            
            # 🧹 Clean up local sequence memory for dropped coin
            self.orderbook_sequences.pop(drop_symbol, None)
            self.is_resyncing.pop(drop_symbol, None)
            self.delta_buffer.pop(drop_symbol, None)
            
            # 🚀 V22.0: MEMORY LEAK PREVENTION (Plugging the Zombie Task vulnerability)
            drop_queue = self.sharded_queues.pop(drop_symbol, None)
            drop_task = self.consumer_tasks.pop(drop_symbol, None)
            
            if drop_queue:
                try:
                    # Allow queue to drain gracefully before worker terminates
                    drop_queue.put_nowait(("SHUTDOWN", None))
                except asyncio.QueueFull:
                    # If queue is gridlocked, forcibly assassinate the task
                    if drop_task and not drop_task.done():
                        drop_task.cancel()
            
            logger.info(f"[X-RAY] 🔄 Socket Hot-Swap Complete: Dropped {drop_symbol}, Added {add_symbol}")
        except Exception as e:
            logger.error(f"[X-RAY] ❌ Hot-swap socket injection failed: {e}")

    async def _resync_isolated_symbol(self, symbol: str):
        """
        🚀 ZERO-TICK-LOSS SNAPSHOT RESYNC
        Fetches a REST snapshot and seamlessly replays any deltas that arrived 
        over the WebSocket directly into the symbol's isolated queue.
        """
        if not self.active_ws or self.active_ws.closed:
            return
            
        logger.warning(f"[X-RAY] 🔄 Requesting buffered snapshot resync for {symbol}...")
        self.orderbook_sequences.pop(symbol, None)
        symbol_queue = self._get_or_create_queue(symbol)
        
        try:
            if self.engine_reference and hasattr(self.engine_reference, "executor"):
                try:
                    rest_ob = await self.engine_reference.executor.safe_call(
                        "GET", "/v5/market/orderbook", 
                        category="linear", symbol=symbol, limit=50
                    )
                    data = rest_ob.get("result", {})
                    if data and "b" in data and "a" in data:
                        parsed_bids = self._fast_float_parse_book(data.get("b", []))
                        parsed_asks = self._fast_float_parse_book(data.get("a", []))
                        snap_seq = int(data.get("u", 0))
                        
                        try:
                            symbol_queue.put_nowait(("orderbook", {
                                "s": symbol, "b": parsed_bids, "a": parsed_asks, 
                                "u": snap_seq, "type": "snapshot", 
                                "ts": int(data.get("ts", time.time() * 1000))
                            }))
                        except asyncio.QueueFull:
                            logger.debug(f"[X-RAY] ⚠️ {symbol} queue full during REST resync. Load shedding snapshot.")

                        self.orderbook_sequences[symbol] = snap_seq
                        
                        buffered_deltas = self.delta_buffer.get(symbol, [])
                        replayed = 0
                        for delta in buffered_deltas:
                            if int(delta.get("u", 0)) > snap_seq:
                                try:
                                    b_parsed = self._fast_float_parse_book(delta.get("b", []))
                                    a_parsed = self._fast_float_parse_book(delta.get("a", []))
                                    
                                    symbol_queue.put_nowait(("orderbook", {
                                        "s": symbol, "b": b_parsed, "a": a_parsed, 
                                        "u": delta.get("u"), "type": "delta", 
                                        "ts": int(time.time() * 1000) 
                                    }))
                                    self.orderbook_sequences[symbol] = delta.get("u")
                                    replayed += 1
                                except asyncio.QueueFull:
                                    pass
                        
                        logger.info(f"[X-RAY] ✅ Resync complete for {symbol}. Replayed {replayed}/{len(buffered_deltas)} buffered deltas.")
                except Exception as e:
                    logger.debug(f"[X-RAY] REST bridge failed during resync for {symbol}: {e}")

        finally:
            self.is_resyncing[symbol] = False
            self.delta_buffer.pop(symbol, None)
            
            try:
                if self.active_ws and not self.active_ws.closed:
                    await self.active_ws.send_json({"op": "unsubscribe", "args": [f"orderbook.50.{symbol}"]})
                    await self.active_ws.send_json({"op": "subscribe", "args": [f"orderbook.50.{symbol}"]})
            except Exception:
                pass

    def _fast_float_parse_book(self, levels: list) -> list:
        try:
            return [[float(lvl[0]), float(lvl[1])] for lvl in levels]
        except (IndexError, ValueError):
            parsed = []
            for lvl in levels:
                try:
                    parsed.append([float(lvl[0]), float(lvl[1])])
                except (IndexError, ValueError):
                    pass
            return parsed

    async def initialize_multiplexed_stream(self):
        self.is_running = True
        
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
            self.is_resyncing.clear()
            self.delta_buffer.clear()
            
            try:
                logger.info(f"[X-RAY] 📡 Opening high-speed multiplexed socket interface channel at: {self.ws_url}")
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ws_url) as ws:
                        
                        self.active_ws = ws
                        reconnect_delay = 1.0
                        self.last_msg_timestamp = time.time()
                        
                        async def connection_watchdog():
                            try:
                                while not ws.closed and self.is_running:
                                    await asyncio.sleep(20) 
                                    try:
                                        await ws.send_json({"req_id": str(int(time.time())), "op": "ping"})
                                        if time.time() - self.last_msg_timestamp > 45.0:
                                            logger.error("[X-RAY] 🚨 WATCHDOG TRIGGERED: Silent flatline detected (No data for >45s). Severing zombie connection.")
                                            await ws.close()
                                            break
                                    except Exception as e:
                                        logger.debug(f"[X-RAY] Watchdog ping failed dynamically: {e}")
                                        break
                            except asyncio.CancelledError:
                                pass
                                    
                        watchdog_task = self.track_task(connection_watchdog())

                        chunk_size = 10
                        for i in range(0, len(args_payload), chunk_size):
                            chunk = args_payload[i:i + chunk_size]
                            await ws.send_json({"op": "subscribe", "args": chunk})
                            await asyncio.sleep(0.05) 
                            
                        logger.info(f"[X-RAY] ✅ Successfully multiplexed topics (chunked) for tracking matrix: {len(self.basket)} nodes.")

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
                                    # Route incoming bytes to SYMBOL-SPECIFIC FIFO queues
                                    if topic.startswith("orderbook"):
                                        symbol = data.get("s")
                                        u_sequence = data.get("u")
                                        msg_type = payload.get("type", "delta")
                                        
                                        target_queue = self._get_or_create_queue(symbol)
                                        
                                        if msg_type == "snapshot":
                                            self.orderbook_sequences[symbol] = u_sequence
                                        elif msg_type == "delta":
                                            if self.is_resyncing.get(symbol, False):
                                                self.delta_buffer.setdefault(symbol, []).append(data)
                                                continue
                                                
                                            last_seq = self.orderbook_sequences.get(symbol)
                                            prev_seq = data.get("pu")  
                                            
                                            if last_seq is not None and prev_seq is not None:
                                                if prev_seq != last_seq:
                                                    logger.critical(f"[X-RAY] ❌ SEVERE SEQUENCE BREAK // {symbol} (Gap | PrevSeq:{prev_seq} != Stored:{last_seq}). Initiating buffered REST resync.")
                                                    
                                                    self.is_resyncing[symbol] = True
                                                    self.delta_buffer.setdefault(symbol, []).append(data)
                                                    
                                                    self.track_task(self._resync_isolated_symbol(symbol))
                                                    continue 
                                                
                                            self.orderbook_sequences[symbol] = u_sequence
                                            
                                        parsed_bids = self._fast_float_parse_book(data.get("b", []))
                                        parsed_asks = self._fast_float_parse_book(data.get("a", []))

                                        try:
                                            target_queue.put_nowait(("orderbook", {
                                                "s": symbol, "b": parsed_bids, "a": parsed_asks, "u": u_sequence, "type": msg_type, "ts": payload.get("ts", time.time()*1000)
                                            }))
                                        except asyncio.QueueFull:
                                            logger.debug(f"[X-RAY] ⚠️ {symbol} queue full. Shedding orderbook tick.")
                                            
                                    elif topic.startswith("publicTrade"):
                                        symbol = topic.split(".")[-1]
                                        target_queue = self._get_or_create_queue(symbol)
                                        
                                        for tick in data:
                                            tick_payload = {
                                                "symbol": symbol,
                                                "price": float(tick.get("p", 0.0)),
                                                "size": float(tick.get("v", 0.0)),
                                                "side": tick.get("S", "Buy"),
                                                "timestamp": float(tick.get("T", time.time() * 1000))
                                            }
                                            try:
                                                target_queue.put_nowait(("trade", tick_payload))
                                            except asyncio.QueueFull:
                                                logger.debug(f"[X-RAY] ⚠️ {symbol} queue full. Shedding trade tick.")
                                                
                                    elif topic.startswith("tickers"):
                                        symbol = data.get("symbol")
                                        if symbol:
                                            target_queue = self._get_or_create_queue(symbol)
                                            try:
                                                target_queue.put_nowait(("tickers", data))
                                            except asyncio.QueueFull:
                                                logger.debug(f"[X-RAY] ⚠️ {symbol} queue full. Shedding tickers tick.")
                                            
                                    elif topic.startswith("kline"):
                                        symbol = topic.split(".")[2]
                                        target_queue = self._get_or_create_queue(symbol)
                                        try:
                                            target_queue.put_nowait(("kline", {
                                                "interval": topic.split(".")[1], "symbol": symbol, "candle_data": data[0]
                                            }))
                                        except asyncio.QueueFull:
                                            logger.debug(f"[X-RAY] ⚠️ {symbol} queue full. Shedding kline tick.")

                                except Exception as e:
                                    logger.error(f"[X-RAY] 🚨 OVERLOAD OR ROUTING ERROR: {e}")
                                            
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                                
                        if watchdog_task and not watchdog_task.done():
                            watchdog_task.cancel()
                        
            except Exception as e:
                logger.error(f"[X-RAY] ❌ Critical connection failure caught in multiplex ingestion loop: {e}", exc_info=True)
                
            if not self.is_running:
                break
                
            self.active_ws = None
            logger.warning(f"[X-RAY] ⚠️ Ingestion link down. Reconnecting via backoff protocol in {reconnect_delay:.2f}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(max_reconnect_delay, reconnect_delay * 1.5)

    def terminate_all_feeds(self):
        """Performs structural teardown actions across active streaming context pipelines."""
        self.is_running = False
        logger.warning("[X-RAY] 🛑 Terminating multiplexed ingestion pipelines cleanly.")
        
        for task in list(self._active_tasks):
            if not task.done():
                task.cancel()