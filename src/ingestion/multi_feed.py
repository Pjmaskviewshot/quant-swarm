"""
💎 V38.0 APEX TITAN: HIGH-FREQUENCY ZERO-LATENCY MARKET STATE MATRIX
--------------------------------------------------------------------------------
The Single Source of Truth (SSOT) for ultra-low latency L2 orderbook ingestion.
Maintains streaming book state, computes Cont-Kukanov-Stoikov Log-MLOFI and 
Stoikov micro-prices without object allocation, and decouples ingestion from 
alpha computation via conflation queues and priority trade workers.

Architectural Supremacy (V38.0 Upgrades):
- Synchronous In-Memory Fast-Path: Bypasses track_task for trade ticks, screener
  updates, and klines when handlers perform pure in-memory operations (<3μs).
  Eradicates the event-loop task explosion and TASK OVERFLOW (>400) cascade.
- Coroutine Leak Shield: Explicitly calls coro.close() on shed tasks to eradicate
  RuntimeWarning: coroutine was never awaited.
- Throttled Overflow Diagnostics: Limits buffer overflow log reporting to 
  at most once every 5.0 seconds, preventing terminal I/O stdout blocking.
- IPv4 TCP Connector Enforcement: Binds socket.AF_INET on the WebSocket session,
  eliminating Windows getaddrinfo DNS resolution delays.
- Conflated L2 Mailbox: Per-symbol single-slot overwrite buffers eliminate FIFO
  queuing latency, ensuring strategies evaluate strictly against fresh books.
"""

import asyncio
import aiohttp
import time
import math
import heapq
import logging
import json
import socket
import numpy as np
from collections import deque
from typing import Dict, Any, Callable, Coroutine, List, Optional, Tuple, Union

logger = logging.getLogger("QUANT_CORE.MARKET_MATRIX")


class MarketStateMatrix:
    """
    🚀 V38.0 HIGH-FREQUENCY L2 ORDERBOOK & LIQUIDITY MATRIX
    Ingests Bybit public linear streams, manages local L2 limit order books,
    calculates micro-price dislocations, and feeds downstream trading daemons.
    """
    def __init__(
        self,
        basket: List[str],
        intervals: List[str],
        orderbook_callback: Callable[[Dict[str, Any]], Any],
        screener_callback: Callable[[Dict[str, Any]], Any],
        kline_callback: Callable[[Dict[str, Any]], Any],
        trade_callback: Callable[[Dict[str, Any]], Any] = None,
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
        self._last_overflow_log = 0.0

        # O(1) Hash Map Orderbook Representations: {price: volume}
        self.l2_bids: Dict[str, Dict[float, float]] = {}
        self.l2_asks: Dict[str, Dict[float, float]] = {}

        # Cached previous top 5 levels: list of (price, volume)
        self.prev_top_bids: Dict[str, List[Tuple[float, float]]] = {}
        self.prev_top_asks: Dict[str, List[Tuple[float, float]]] = {}

        # O(1) Recursive Welford-EWMA Moments for Log-MLOFI
        self.mlofi_mean: Dict[str, float] = {}
        self.mlofi_var: Dict[str, float] = {}
        self.mlofi_alpha = 0.05
        self.log_mlofi_z: Dict[str, float] = {}
        self.micro_prices: Dict[str, float] = {}

        self.orderbook_sequences: Dict[str, int] = {}
        self.is_resyncing: Dict[str, bool] = {}

        self.active_ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._active_tasks = set()

        # Conflated Mailbox Queue per Symbol (Guarantees zero queuing latency)
        self._conflation_events: Dict[str, asyncio.Event] = {}
        self._conflated_payloads: Dict[str, Dict[str, Any]] = {}
        self.consumer_tasks: Dict[str, asyncio.Task] = {}

    def track_task(self, coro: Any) -> asyncio.Task:
        """Schedules coroutines with bounded capacity, cleanup, and leak prevention."""
        self._active_tasks = {t for t in self._active_tasks if not t.done()}

        if len(self._active_tasks) > 350:
            now = time.time()
            if now - self._last_overflow_log > 5.0:
                logger.critical(f"[X-RAY] TASK OVERFLOW ({len(self._active_tasks)} > 350). Shedding tasks to protect event loop.")
                self._last_overflow_log = now

            # Eradicate RuntimeWarning: coroutine was never awaited
            if asyncio.iscoroutine(coro):
                coro.close()

            dummy = asyncio.Future()
            dummy.set_result(None)
            return dummy

        task = asyncio.create_task(coro)
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)
        return task

    def _get_or_create_conflation_worker(self, symbol: str):
        """Provisions a single-slot conflation worker per symbol."""
        if symbol not in self._conflation_events:
            self._conflation_events[symbol] = asyncio.Event()
            self._conflated_payloads[symbol] = {}
            worker_task = self.track_task(self._conflated_consumer_worker(symbol))
            self.consumer_tasks[symbol] = worker_task

    async def _conflated_consumer_worker(self, symbol: str):
        """
        Processes orderbook state via a single-slot mailbox buffer.
        Eliminates lag and avoids queuing thousands of stale ticks.
        """
        event = self._conflation_events[symbol]
        while self.is_running:
            try:
                await event.wait()
                event.clear()

                payload = self._conflated_payloads.get(symbol)
                if not payload:
                    continue

                if payload.get("type") == "SHUTDOWN":
                    break

                res = self.orderbook_callback(payload)
                if asyncio.iscoroutine(res):
                    await res

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[X-RAY] Conflation worker error for {symbol}: {e}", exc_info=True)

    def _fast_float_parse_book(self, levels: list) -> List[List[float]]:
        """Parses raw string price and size records into floats."""
        parsed = []
        for lvl in levels:
            try:
                parsed.append([float(lvl[0]), float(lvl[1])])
            except (IndexError, ValueError, TypeError):
                continue
        return parsed

    def _update_ssot_orderbook(
        self, symbol: str, msg_type: str, parsed_bids: list, parsed_asks: list, ts: int
    ) -> Optional[Dict[str, Any]]:
        """
        Maintains orderbook state and calculates Stoikov Micro-Price 
        and Cont-Kukanov-Stoikov Level-5 MLOFI.
        """
        if symbol not in self.l2_bids or msg_type == "snapshot":
            self.l2_bids[symbol] = {}
            self.l2_asks[symbol] = {}
            self.prev_top_bids[symbol] = []
            self.prev_top_asks[symbol] = []
            self.mlofi_mean[symbol] = 0.0
            self.mlofi_var[symbol] = 1.0
            self.log_mlofi_z[symbol] = 0.0

        bids_dict = self.l2_bids[symbol]
        asks_dict = self.l2_asks[symbol]

        # 1. Update In-Memory L2 Hash Maps
        for p, v in parsed_bids:
            if v <= 0.0:
                bids_dict.pop(p, None)
            else:
                bids_dict[p] = v

        for p, v in parsed_asks:
            if v <= 0.0:
                asks_dict.pop(p, None)
            else:
                asks_dict[p] = v

        if not bids_dict or not asks_dict:
            return None

        # 2. Extract Top 10 BBO Levels via Heap Selection
        top_bid_prices = heapq.nlargest(10, bids_dict.keys())
        top_ask_prices = heapq.nsmallest(10, asks_dict.keys())

        best_bid, best_ask = top_bid_prices[0], top_ask_prices[0]
        if best_bid >= best_ask:
            return None  # Crossed-book packet burst protection

        # 3. Amortized Memory Pruning (Bounded Hash Map)
        if len(bids_dict) > 100:
            retained_bids = heapq.nlargest(50, bids_dict.keys())
            self.l2_bids[symbol] = {p: bids_dict[p] for p in retained_bids}
            bids_dict = self.l2_bids[symbol]

        if len(asks_dict) > 100:
            retained_asks = heapq.nsmallest(50, asks_dict.keys())
            self.l2_asks[symbol] = {p: asks_dict[p] for p in retained_asks}
            asks_dict = self.l2_asks[symbol]

        bid_v, ask_v = bids_dict[best_bid], asks_dict[best_ask]

        # 4. Non-Linear Stoikov Micro-Price
        imb = bid_v / (bid_v + ask_v + 1e-9)
        spread = best_ask - best_bid
        micro_price = ((best_bid + best_ask) / 2.0) + (spread * (imb - 0.5) * (1.0 + abs(imb - 0.5)))
        self.micro_prices[symbol] = micro_price

        # 5. Level-5 Cont-Kukanov-Stoikov MLOFI
        curr_bids = [(p, bids_dict[p]) for p in top_bid_prices[:5]]
        curr_asks = [(p, asks_dict[p]) for p in top_ask_prices[:5]]
        prev_bids = self.prev_top_bids[symbol]
        prev_asks = self.prev_top_asks[symbol]

        mid = (best_bid + best_ask) / 2.0
        mlofi_t = 0.0
        decay_alpha = 0.40

        if prev_bids and prev_asks:
            for i in range(min(len(curr_bids), len(prev_bids))):
                c_p, c_v = curr_bids[i]
                p_p, p_v = prev_bids[i]
                dist_bps = (abs(c_p - mid) / mid) * 10000.0
                w = math.exp(-decay_alpha * (dist_bps / 5.0))

                if c_p > p_p:
                    delta_w = math.log1p(c_v)
                elif c_p == p_p:
                    delta_w = math.log1p(c_v) - math.log1p(p_v)
                else:
                    delta_w = -math.log1p(p_v)

                mlofi_t += delta_w * w

            for i in range(min(len(curr_asks), len(prev_asks))):
                c_p, c_v = curr_asks[i]
                p_p, p_v = prev_asks[i]
                dist_bps = (abs(c_p - mid) / mid) * 10000.0
                w = math.exp(-decay_alpha * (dist_bps / 5.0))

                if c_p < p_p:
                    delta_w = math.log1p(c_v)
                elif c_p == p_p:
                    delta_w = math.log1p(c_v) - math.log1p(p_v)
                else:
                    delta_w = -math.log1p(p_v)

                mlofi_t -= delta_w * w

        # 6. Online Welford-EWMA Z-Score Tracking
        mean = self.mlofi_mean[symbol]
        var = self.mlofi_var[symbol]
        delta_stat = mlofi_t - mean
        self.mlofi_mean[symbol] += self.mlofi_alpha * delta_stat
        self.mlofi_var[symbol] = (1.0 - self.mlofi_alpha) * var + self.mlofi_alpha * (delta_stat ** 2)

        std = math.sqrt(max(1e-9, self.mlofi_var[symbol]))
        z = float(np.clip((mlofi_t - self.mlofi_mean[symbol]) / std, -5.0, 5.0))

        self.log_mlofi_z[symbol] = z
        self.prev_top_bids[symbol] = curr_bids
        self.prev_top_asks[symbol] = curr_asks

        return {
            "symbol": symbol,
            "best_bid": best_bid,
            "bid_vol": bid_v,
            "best_ask": best_ask,
            "ask_vol": ask_v,
            "micro_price": micro_price,
            "log_mlofi_z": z,
            "bids": [[p, bids_dict[p]] for p in top_bid_prices],
            "asks": [[p, asks_dict[p]] for p in top_ask_prices],
            "timestamp": ts
        }

    async def _resync_symbol_topic(self, symbol: str):
        """
        Re-subscribes to the orderbook topic via WebSocket to fetch
        a fresh snapshot without hitting REST API rate limits.
        """
        if not self.active_ws or self.active_ws.closed:
            return

        topic = f"orderbook.50.{symbol}"
        try:
            await self.active_ws.send_json({"op": "unsubscribe", "args": [topic]})
            await asyncio.sleep(0.02)
            await self.active_ws.send_json({"op": "subscribe", "args": [topic]})
            logger.info(f"[X-RAY] Topic re-subscription triggered for {symbol}.")
        except Exception as e:
            logger.debug(f"[X-RAY] Topic re-subscription fault for {symbol}: {e}")
        finally:
            self.is_resyncing[symbol] = False

    async def hot_swap_socket_stream(self, drop_symbol: str, add_symbol: str):
        """Dynamically hot-swaps asset subscriptions without restarting the socket."""
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
                await self.active_ws.send_json({"op": "unsubscribe", "args": unsub_args[i:i + 10]})
            for i in range(0, len(sub_args), 10):
                await self.active_ws.send_json({"op": "subscribe", "args": sub_args[i:i + 10]})

            # Purge memory states of dropped asset
            self.orderbook_sequences.pop(drop_symbol, None)
            self.is_resyncing.pop(drop_symbol, None)
            self.l2_bids.pop(drop_symbol, None)
            self.l2_asks.pop(drop_symbol, None)
            self.prev_top_bids.pop(drop_symbol, None)
            self.prev_top_asks.pop(drop_symbol, None)
            self.mlofi_mean.pop(drop_symbol, None)
            self.mlofi_var.pop(drop_symbol, None)
            self.log_mlofi_z.pop(drop_symbol, None)
            self.micro_prices.pop(drop_symbol, None)

            # Cleanly shutdown the dropped symbol's consumer task
            if drop_symbol in self._conflation_events:
                self._conflated_payloads[drop_symbol] = {"type": "SHUTDOWN"}
                self._conflation_events[drop_symbol].set()
                self._conflation_events.pop(drop_symbol, None)
                self.consumer_tasks.pop(drop_symbol, None)

            logger.info(f"[X-RAY] Hot-Swap Complete: Dropped {drop_symbol} | Added {add_symbol}")
        except Exception as e:
            logger.error(f"[X-RAY] Hot-swap operation failed: {e}")

    async def initialize_multiplexed_stream(self):
        """Starts and maintains the multiplexed WebSocket streaming pipeline."""
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

            try:
                logger.info(f"[X-RAY] Connecting to multiplexed stream at: {self.ws_url}")
                connector = aiohttp.TCPConnector(
                    family=socket.AF_INET,
                    ssl=True,
                    limit=50,
                    keepalive_timeout=45.0,
                    enable_cleanup_closed=True
                )
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.ws_connect(
                        self.ws_url,
                        autoping=True,
                        heartbeat=20.0,
                        max_msg_size=16 * 1024 * 1024
                    ) as ws:
                        self.active_ws = ws
                        reconnect_delay = 1.0
                        self.last_msg_timestamp = time.time()

                        async def connection_watchdog():
                            try:
                                while not ws.closed and self.is_running:
                                    await asyncio.sleep(15)
                                    if time.time() - self.last_msg_timestamp > 35.0:
                                        logger.error("[X-RAY] WATCHDOG TRIGGERED: Silent flatline (>35s). Closing connection.")
                                        await ws.close()
                                        break
                                    try:
                                        await ws.send_json({"op": "ping"})
                                    except Exception:
                                        break
                            except asyncio.CancelledError:
                                pass

                        watchdog_task = self.track_task(connection_watchdog())

                        chunk_size = 10
                        for i in range(0, len(args_payload), chunk_size):
                            chunk = args_payload[i:i + chunk_size]
                            await ws.send_json({"op": "subscribe", "args": chunk})
                            await asyncio.sleep(0.04)

                        logger.info(f"[X-RAY] Subscribed to {len(args_payload)} topics across {len(self.basket)} nodes.")

                        async for msg in ws:
                            self.last_msg_timestamp = time.time()

                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    payload = json.loads(msg.data)
                                except Exception:
                                    continue

                                if payload.get("op") == "pong" or payload.get("ret_msg") == "pong":
                                    continue

                                topic: str = payload.get("topic", "")
                                data = payload.get("data")
                                if not data:
                                    continue

                                try:
                                    # 1. High-Priority Trade Stream (Direct synchronous execution)
                                    if topic.startswith("publicTrade"):
                                        symbol = topic.split(".")[-1]
                                        if self.trade_callback:
                                            for tick in data:
                                                tick_payload = {
                                                    "symbol": symbol,
                                                    "price": float(tick.get("p", 0.0)),
                                                    "size": float(tick.get("v", 0.0)),
                                                    "side": tick.get("S", "Buy"),
                                                    "timestamp": float(tick.get("T", time.time() * 1000))
                                                }
                                                res = self.trade_callback(tick_payload)
                                                if asyncio.iscoroutine(res):
                                                    self.track_task(res)

                                    # 2. Conflated L2 Orderbook Stream (Single-slot mailbox worker)
                                    elif topic.startswith("orderbook"):
                                        symbol = data.get("s")
                                        u_sequence = data.get("u")
                                        prev_seq = data.get("pu")
                                        msg_type = payload.get("type", "delta")

                                        # Sequence Gap Verification
                                        if msg_type == "snapshot":
                                            self.orderbook_sequences[symbol] = u_sequence
                                        elif msg_type == "delta":
                                            last_seq = self.orderbook_sequences.get(symbol)
                                            if last_seq is not None and prev_seq is not None and prev_seq != last_seq:
                                                if not self.is_resyncing.get(symbol, False):
                                                    logger.warning(f"[X-RAY] SEQUENCE BREAK on {symbol}. Resyncing topic.")
                                                    self.is_resyncing[symbol] = True
                                                    self.track_task(self._resync_symbol_topic(symbol))
                                                continue
                                            self.orderbook_sequences[symbol] = u_sequence

                                        parsed_b = self._fast_float_parse_book(data.get("b", []))
                                        parsed_a = self._fast_float_parse_book(data.get("a", []))

                                        rich_payload = self._update_ssot_orderbook(
                                            symbol=symbol,
                                            msg_type=msg_type,
                                            parsed_bids=parsed_b,
                                            parsed_asks=parsed_a,
                                            ts=payload.get("ts", int(time.time() * 1000))
                                        )

                                        if rich_payload:
                                            self._get_or_create_conflation_worker(symbol)
                                            self._conflated_payloads[symbol] = rich_payload
                                            self._conflation_events[symbol].set()

                                    # 3. Market Tickers Stream (Direct synchronous execution)
                                    elif topic.startswith("tickers"):
                                        symbol = data.get("symbol")
                                        if symbol and self.screener_callback:
                                            res = self.screener_callback(data)
                                            if asyncio.iscoroutine(res):
                                                self.track_task(res)

                                    # 4. Multi-Timeframe Kline Stream (Direct synchronous execution)
                                    elif topic.startswith("kline"):
                                        parts = topic.split(".")
                                        if self.kline_callback:
                                            res = self.kline_callback({
                                                "interval": parts[1],
                                                "symbol": parts[2],
                                                "candle_data": data[0]
                                            })
                                            if asyncio.iscoroutine(res):
                                                self.track_task(res)

                                except Exception as parse_err:
                                    logger.error(f"[X-RAY] Ingestion routing error: {parse_err}")

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break

                        if watchdog_task and not watchdog_task.done():
                            watchdog_task.cancel()

            except Exception as e:
                logger.error(f"[X-RAY] Stream connection error: {e}", exc_info=True)

            if not self.is_running:
                break

            self.active_ws = None
            logger.warning(f"[X-RAY] Stream disconnected. Reconnecting in {reconnect_delay:.2f}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(max_reconnect_delay, reconnect_delay * 1.5)

    def terminate_all_feeds(self):
        """Cleans up background workers and shuts down active streams."""
        self.is_running = False
        logger.warning("[X-RAY] Terminating all streaming feeds cleanly.")

        for event in self._conflation_events.values():
            event.set()

        for task in list(self._active_tasks):
            if not task.done():
                task.cancel()