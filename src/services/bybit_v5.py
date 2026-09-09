"""
APEX TITAN: TITANIUM API EXECUTOR (BYBIT V5)
--------------------------------------------------------
Cloud-resilient, zero-latency unified Bybit V5 exchange execution connector.

Production Hardening & Compliance Upgrades (V43.0 Audit Remediations):
- Expanded Recv-Window Tolerance (Error 10002 Remediation): Broadened `X-BAPI-RECV-WINDOW` 
  headers from 5000ms to 15000ms across all REST requests and signatures. Eradicates 
  timestamp drift errors caused by Render cloud container clock skew.
- Self-Trade Prevention (STP) Enforcement: Automatically injects `smpType="CancelMaker"`
  into order creation payloads to eliminate self-matching against resting inventory
  or delta-neutral cash-and-carry hedges.
- Non-Throwing Compliance Quarantine (Audit 110126 Resolution): Catches Bybit error 
  110126 (Agreement Not Signed / Innovation Zone) and applies a 1-hour quarantine ban
  without throwing unhandled exceptions that destabilize asyncio task runners.
- Monotonic Contention-Free Token Bucket: Computes rate pacing delays inside a minimal
  critical lock and sleeps outside the lock, eradicating lock-contention latency.
- WebSocket Watchdog Heartbeat: Pairs active 20-second pings with a 45-second message 
  inactivity watchdog, forcibly resetting stale WebSocket feeds before silent disconnection.
- Synchronized Asset Exclusion Matrix: Blocks pre-market, TradFi synthetics, and 
  innovation-zone tokens before network calls to prevent compliance infractions.
- Idempotent Order Retry & Reconciliation: Verifies order state via `orderLinkId` 
  before network retries and recovers true `orderId` on RetCode 110008.
"""

import time
import math
import uuid
import hmac
import hashlib
import asyncio
import logging
import json
import urllib.parse
import socket
from typing import Dict, Any, List, Optional

import aiohttp

logger = logging.getLogger("QUANT_CORE.BYBIT")


class BybitRetCode:
    SUCCESS = 0
    PARAMETER_ERROR = 10002          
    SYSTEM_MAINTENANCE = 10004       
    RATE_LIMIT_REACHED = 10006       
    QTY_OUT_OF_BOUNDS = 10001        
    SERVICE_UNAVAILABLE = 10016      
    ORDER_NOT_EXISTS = 110001        
    DUPLICATE_ORDER_LINK_ID = 110008 
    INSUFFICIENT_BALANCE = 110007    
    RISK_LIMIT_EXCEEDED = 110013     
    LEVERAGE_NOT_MODIFIED = 110025   
    LEVERAGE_NOT_MODIFIED_2 = 110043 
    AGREEMENT_NOT_SIGNED = 110126    


class TokenBucketRateLimiter:
    """
    Contention-Free Monotonic Token Bucket:
    Calculates rate-limit backoff inside a locked critical section and executes 
    the delay outside the lock to prevent event-loop stalls across concurrent workers.
    """
    def __init__(self, capacity: int = 12, fill_rate: float = 6.0):
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.fill_rate = float(fill_rate) 
        self.last_fill_time = time.time()
        self.lock = asyncio.Lock()

    async def acquire(self):
        sleep_time = 0.0
        async with self.lock:
            now = time.time()
            elapsed = max(0.0, now - self.last_fill_time)
            self.last_fill_time = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)

            if self.tokens < 1.0:
                sleep_time = (1.0 - self.tokens) / self.fill_rate
                self.tokens = 0.0
                self.last_fill_time += sleep_time
            else:
                self.tokens -= 1.0

        if sleep_time > 0.0:
            await asyncio.sleep(sleep_time)


class BybitUnifiedExecutor:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        self.testnet = testnet
        
        if self.testnet:
            self.rest_routes = ["https://api-testnet.bybit.com"]
            self.ws_private_url = "wss://stream-testnet.bybit.com/v5/private"
        else:
            self.rest_routes = [
                "https://api.bybit.com",
                "https://api.bytick.com"
            ]
            self.ws_private_url = "wss://stream.bybit.com/v5/private"

        self.current_route_idx = 0
        self.rest_base_url = self.rest_routes[self.current_route_idx]
        
        self.data_rate_limiter = TokenBucketRateLimiter(capacity=12, fill_rate=6.0)
        self.execution_rate_limiter = TokenBucketRateLimiter(capacity=15, fill_rate=10.0)
        
        self.session: Optional[aiohttp.ClientSession] = None
        self._server_time_offset_ms: int = 0
        self._leverage_cache: Dict[str, int] = {}
        self._fee_cache: Dict[str, Dict[str, float]] = {}
        self.temporary_symbol_bans: Dict[str, float] = {}
        self._last_known_equity: float = 0.0

        self._ws_connection: Optional[aiohttp.ClientWebSocketResponse] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_ping_task: Optional[asyncio.Task] = None
        self._clock_sync_task: Optional[asyncio.Task] = None  
        self._order_waiters: Dict[str, List[asyncio.Future]] = {}
        self._execution_cache: Dict[str, Dict[str, Any]] = {}
        self._last_ws_msg_time: float = time.time()
        
        self._waiter_lock = asyncio.Lock()
        self._is_terminating = False
        
        logger.info(f"Initialized Async Bybit V5 Unified Executor (Testnet: {self.testnet})")

    async def initialize(self):
        if not self.session or self.session.closed:
            connector = aiohttp.TCPConnector(
                family=socket.AF_INET,
                limit=100, 
                keepalive_timeout=45.0, 
                ttl_dns_cache=300, 
                enable_cleanup_closed=True,
                ssl=True
            )
            self.session = aiohttp.ClientSession(
                connector=connector, 
                timeout=aiohttp.ClientTimeout(total=15.0, connect=6.0)
            )
        await self.calibrate_server_time()
        
        if not self._clock_sync_task or self._clock_sync_task.done():
            self._clock_sync_task = asyncio.create_task(self._continuous_clock_sync_loop())

    def _rotate_dns_route(self):
        if len(self.rest_routes) > 1:
            self.current_route_idx = (self.current_route_idx + 1) % len(self.rest_routes)
            self.rest_base_url = self.rest_routes[self.current_route_idx]
            logger.critical(f"[X-RAY] NETWORK ROUTING SHIFT: Active Gateway -> {self.rest_base_url}")

    async def calibrate_server_time(self) -> int:
        try:
            start_local = int(time.time() * 1000)
            async with self.session.get(f"{self.rest_base_url}/v5/market/time") as resp:
                raw_text = await resp.text()
                data = json.loads(raw_text)
            end_local = int(time.time() * 1000)

            if data.get("retCode") == 0:
                server_time = int(data["result"]["timeNano"]) // 1_000_000
                latency = max(0, (end_local - start_local) // 2)
                self._server_time_offset_ms = server_time - (end_local - latency)
                logger.info(f"[X-RAY] Clock Recalibrated via {self.rest_base_url}. Offset: {self._server_time_offset_ms}ms (Latency: {latency * 2}ms)")
                return self._server_time_offset_ms
        except Exception as e:
            logger.warning(f"Clock calibration fault on {self.rest_base_url}: {e}. Retaining prior offset.")
        return self._server_time_offset_ms

    async def _continuous_clock_sync_loop(self):
        while not self._is_terminating:
            try:
                await asyncio.sleep(900) 
                await self.calibrate_server_time()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Continuous clock sync iteration bypassed: {e}")

    def _generate_signature(self, timestamp: str, payload: str) -> str:
        # Expanded recv_window inclusion (15000ms) matches header expectation
        param_str = f"{timestamp}{self.api_key}15000{payload}"
        return hmac.new(self.api_secret.encode("utf-8"), param_str.encode("utf-8"), hashlib.sha256).hexdigest()

    async def _query_order_by_link_id(self, category: str, symbol: str, order_link_id: str) -> Optional[Dict[str, Any]]:
        """Queries order details using client order link ID to reconcile network drops."""
        try:
            timestamp = str(int(time.time() * 1000) + self._server_time_offset_ms)
            params = {"category": category, "symbol": symbol, "orderLinkId": order_link_id}
            query_str = urllib.parse.urlencode(params)
            sig = self._generate_signature(timestamp, query_str)
            headers = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-SIGN": sig,
                "X-BAPI-RECV-WINDOW": "15000",  # Expanded window to absorb container clock drift
                "Content-Type": "application/json"
            }
            url = f"{self.rest_base_url}/v5/order/realtime?{query_str}"
            async with self.session.get(url, headers=headers) as resp:
                data = await resp.json()
                if data.get("retCode") == 0:
                    orders = data.get("result", {}).get("list", [])
                    if orders:
                        return orders[0]
        except Exception as e:
            logger.debug(f"[X-RAY] Order inquiry by orderLinkId failed: {e}")
        return None

    async def _safe_api_call(self, method: str, endpoint: str, is_execution: bool = False, **kwargs) -> Any:
        if not self.session or self.session.closed:
            await self.initialize()
        if is_execution:
            await self.execution_rate_limiter.acquire()
        else:
            await self.data_rate_limiter.acquire()

        is_order_create = (endpoint == "/v5/order/create" and method == "POST")
        if is_order_create:
            if "orderLinkId" not in kwargs or not kwargs["orderLinkId"]:
                kwargs["orderLinkId"] = f"APEX_{uuid.uuid4().hex[:16]}"
            if "smpType" not in kwargs:
                kwargs["smpType"] = "CancelMaker"

        clean_kwargs = {k: v for k, v in kwargs.items() if v is not None}
        order_link_id = clean_kwargs.get("orderLinkId")
        category = clean_kwargs.get("category", "linear")
        symbol = clean_kwargs.get("symbol", "")

        for attempt in range(3):
            if attempt > 0 and is_order_create and order_link_id and symbol:
                logger.warning(f"[X-RAY] Verifying order existence on Bybit before retry: {order_link_id}")
                existing_order = await self._query_order_by_link_id(category, symbol, order_link_id)
                if existing_order:
                    logger.info(f"[X-RAY] Order already established on exchange: {existing_order.get('orderId')}")
                    return {
                        "retCode": 0,
                        "retMsg": "OK_ORDER_RECOVERED",
                        "result": {
                            "orderId": existing_order.get("orderId"),
                            "orderLinkId": order_link_id,
                            "cumExecQty": existing_order.get("cumExecQty", "0"),
                            "avgPrice": existing_order.get("avgPrice", "0")
                        }
                    }

            try:
                timestamp = str(int(time.time() * 1000) + self._server_time_offset_ms)
                payload = ""

                if method == "GET":
                    if clean_kwargs:
                        sorted_kwargs = dict(sorted({k: (str(v).lower() if isinstance(v, bool) else v) for k, v in clean_kwargs.items()}.items()))
                        payload = urllib.parse.urlencode(sorted_kwargs)
                        endpoint_full = f"{endpoint}?{payload}"
                    else:
                        endpoint_full = endpoint
                else:
                    payload = json.dumps(clean_kwargs, separators=(',', ':')) if clean_kwargs else ""
                    endpoint_full = endpoint

                signature = self._generate_signature(timestamp, payload)
                headers = {
                    "X-BAPI-API-KEY": self.api_key,
                    "X-BAPI-TIMESTAMP": timestamp,
                    "X-BAPI-SIGN": signature,
                    "X-BAPI-RECV-WINDOW": "15000",  # Expanded window prevents Error 10002 drift rejections
                    "Content-Type": "application/json"
                }

                url = f"{self.rest_base_url}{endpoint_full}"
                async with self.session.request(method, url, headers=headers, data=payload if method == "POST" else None) as resp:
                    limit_status = resp.headers.get("X-Bapi-Limit-Status")
                    if limit_status:
                        try:
                            remaining = int(limit_status)
                            if remaining <= 4:
                                logger.warning(f"[X-RAY] BYBIT LIMITER PACING: {remaining} requests remaining. Smoothing loop.")
                                await asyncio.sleep(0.25)
                        except ValueError:
                            pass

                    raw_text = await resp.text()
                    try:
                        response = json.loads(raw_text)
                    except json.JSONDecodeError:
                        logger.warning(f"[X-RAY] Upstream Gateway Non-JSON ({resp.status}). Retrying...")
                        response = {"retCode": -999, "retMsg": f"HTTP_{resp.status}_GATEWAY_BURST"}

                ret_code = response.get("retCode", -1)
                
                if ret_code == BybitRetCode.DUPLICATE_ORDER_LINK_ID and is_order_create and order_link_id:
                    existing_order = await self._query_order_by_link_id(category, symbol, order_link_id)
                    real_id = existing_order.get("orderId", order_link_id) if existing_order else order_link_id
                    return {
                        "retCode": 0,
                        "retMsg": "OK_DUPLICATE_RESOLVED",
                        "result": {
                            "orderId": real_id,
                            "orderLinkId": order_link_id,
                            "cumExecQty": existing_order.get("cumExecQty", "0") if existing_order else "0",
                            "avgPrice": existing_order.get("avgPrice", "0") if existing_order else "0"
                        }
                    }

                if ret_code == BybitRetCode.AGREEMENT_NOT_SIGNED:
                    symbol_banned = clean_kwargs.get("symbol", "UNKNOWN")
                    if symbol_banned != "UNKNOWN":
                        self.temporary_symbol_bans[symbol_banned] = time.time() + 3600.0
                        logger.error(f"[COMPLIANCE] Agreement Not Signed (110126) for {symbol_banned}. Symbol quarantined for 1 hour.")
                    return response

                if ret_code == BybitRetCode.PARAMETER_ERROR and "timestamp" in response.get("retMsg", "").lower():
                    logger.warning("[X-RAY] Timestamp Drift (Error 10002). Forcing NTP calibration...")
                    await self.calibrate_server_time()
                    await asyncio.sleep(0.15)
                    continue

                if ret_code in [BybitRetCode.RATE_LIMIT_REACHED, BybitRetCode.SERVICE_UNAVAILABLE]: 
                    logger.warning(f"[X-RAY] Exchange Overload (Code: {ret_code}). Backing off...")
                    await asyncio.sleep(1.0)
                    continue
                
                return response
                
            except (asyncio.TimeoutError, aiohttp.ClientOSError, aiohttp.ServerDisconnectedError, aiohttp.ClientConnectorError) as e:
                logger.warning(f"[X-RAY] NETWORK FAULT on {endpoint}: {type(e).__name__} ({e}). Retrying ({attempt+1}/3)...")
                if attempt == 1:
                    self._rotate_dns_route()
                if attempt == 2:
                    return {"retCode": -999, "retMsg": f"Fatal Network Dropout to {self.rest_base_url}{endpoint} after 3 attempts."}
                await asyncio.sleep(0.5 * (attempt + 1))

            except Exception as e:
                if attempt == 2:
                    logger.error(f"[X-RAY] API call critically failed: {e}")
                    return {"retCode": -999, "retMsg": str(e)}
                await asyncio.sleep(0.3)

        return {"retCode": -999, "retMsg": "REQUEST_EXHAUSTED"}

    async def safe_call(self, method: str, endpoint: str, **kwargs) -> Any:
        is_exec = kwargs.pop("is_execution", False)
        return await self._safe_api_call(method.upper(), endpoint, is_execution=is_exec, **kwargs)

    async def get_fee_rates(self, symbol: str = "BTCUSDT") -> Dict[str, float]:
        """Fetches dynamic account fee schedules with 1-hour rolling cache."""
        if symbol in self._fee_cache:
            cache_entry = self._fee_cache[symbol]
            if time.time() - cache_entry.get("_ts", 0) < 3600.0:
                return {
                    "taker": cache_entry["taker"],
                    "maker": cache_entry["maker"]
                }

        try:
            res = await self.safe_call("GET", "/v5/account/fee-rate", category="linear", symbol=symbol)
            if res.get("retCode") == 0:
                data_list = res.get("result", {}).get("list", [])
                if data_list:
                    item = data_list[0]
                    taker_fee = float(item.get("takerFeeRate", 0.00055))
                    maker_fee = float(item.get("makerFeeRate", 0.00020))
                    self._fee_cache[symbol] = {
                        "taker": taker_fee,
                        "maker": maker_fee,
                        "_ts": time.time()
                    }
                    return {"taker": taker_fee, "maker": maker_fee}
        except Exception as e:
            logger.debug(f"[X-RAY] Fee rate query fault for {symbol}: {e}")

        return {"taker": 0.00055, "maker": 0.00020}

    async def connect_ws(self):
        if not self.session or self.session.closed:
            await self.initialize()
        self._is_terminating = False
        self._ws_task = asyncio.create_task(self._ws_lifecycle_loop())
        logger.info("Bybit Private WebSocket Stream Connected. Telemetry Armed.")

    async def _ws_ping_loop(self, ws: aiohttp.ClientWebSocketResponse):
        """Active background heartbeat loop dispatching pings every 20 seconds."""
        while not self._is_terminating and not ws.closed:
            try:
                await asyncio.sleep(20.0)
                if not ws.closed:
                    await ws.send_json({"req_id": str(int(time.time())), "op": "ping"})
                
                if time.time() - self._last_ws_msg_time > 45.0:
                    logger.warning("[X-RAY] WS Watchdog Timeout (>45s inactivity). Forcing reconnect.")
                    await ws.close()
                    break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[X-RAY] WS Heartbeat ping fault: {e}")
                break

    async def _ws_lifecycle_loop(self):
        backoff = 1.0
        while not self._is_terminating:
            try:
                async with self.session.ws_connect(
                    self.ws_private_url, 
                    autoping=False, 
                    max_msg_size=16 * 1024 * 1024
                ) as ws:
                    self._ws_connection = ws
                    self._last_ws_msg_time = time.time()
                    backoff = 1.0

                    expires = int(time.time() * 1000) + self._server_time_offset_ms + 10000
                    signature = hmac.new(
                        self.api_secret.encode("utf-8"), 
                        f"GET/realtime{expires}".encode("utf-8"), 
                        hashlib.sha256
                    ).hexdigest()

                    await ws.send_json({"op": "auth", "args": [self.api_key, expires, signature]})
                    
                    auth_msg = await asyncio.wait_for(ws.receive(), timeout=10.0)
                    if auth_msg.type != aiohttp.WSMsgType.TEXT:
                        logger.critical("WS Auth Failed: Non-text response received from Bybit.")
                        await asyncio.sleep(3.0)
                        continue

                    try:
                        auth_resp = json.loads(auth_msg.data)
                    except Exception:
                        auth_resp = {}

                    if not auth_resp.get("success"):
                        logger.critical(f"WS Auth Rejected: {auth_resp.get('ret_msg')}")
                        await asyncio.sleep(4.0)
                        continue

                    await ws.send_json({"op": "subscribe", "args": ["execution", "order"]})

                    if self._ws_ping_task and not self._ws_ping_task.done():
                        self._ws_ping_task.cancel()
                    self._ws_ping_task = asyncio.create_task(self._ws_ping_loop(ws))

                    while not self._is_terminating:
                        msg = await ws.receive()
                        self._last_ws_msg_time = time.time()
                            
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = json.loads(msg.data)
                            except Exception:
                                continue

                            if payload.get("op") == "pong" or payload.get("ret_msg") == "pong":
                                continue
                            await self._on_ws_message(payload)

                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logger.warning("[X-RAY] WS Connection closed by exchange.")
                            break
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[X-RAY] WS Transport reset ({e}). Reconnecting in {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(20.0, backoff * 1.5)
            finally:
                if self._ws_ping_task and not self._ws_ping_task.done():
                    self._ws_ping_task.cancel()

    async def _on_ws_message(self, message: dict):
        topic = message.get("topic", "")
        data = message.get("data", [])

        if topic == "order":
            for order in data:
                order_id = order.get("orderId")
                order_link_id = order.get("orderLinkId")
                status = order.get("orderStatus")
                
                if order_id:
                    self._execution_cache[order_id] = order
                if order_link_id:
                    self._execution_cache[order_link_id] = order
                    
                if len(self._execution_cache) > 2000:
                    prune_keys = list(self._execution_cache.keys())[:500]
                    for k in prune_keys:
                        self._execution_cache.pop(k, None)

                if status in ["Filled", "PartiallyFilled", "Cancelled", "Rejected"]:
                    if order_id:
                        await self._resolve_ws_future(order_id, order)
                    if order_link_id:
                        await self._resolve_ws_future(order_link_id, order)
                        
        elif topic == "execution":
            for exec_report in data:
                order_id = exec_report.get("orderId")
                order_link_id = exec_report.get("orderLinkId")
                
                if order_id:
                    synthetic_event = {
                        "orderId": order_id, 
                        "orderLinkId": order_link_id,
                        "cumExecQty": exec_report.get("execQty"), 
                        "avgPrice": exec_report.get("execPrice"), 
                        "orderStatus": "PartiallyFilled", 
                        "isWsExecution": True
                    }
                    self._execution_cache[order_id] = synthetic_event
                    await self._resolve_ws_future(order_id, synthetic_event)
                    if order_link_id:
                        self._execution_cache[order_link_id] = synthetic_event
                        await self._resolve_ws_future(order_link_id, synthetic_event)

    async def _resolve_ws_future(self, key_id: str, data: dict):
        async with self._waiter_lock:
            waiters = self._order_waiters.pop(key_id, [])
            for fut in waiters:
                if not fut.done():
                    fut.set_result(data)

    async def await_ws_execution_report(self, order_id: str, timeout: float = 0.25) -> Optional[Dict[str, Any]]:
        async with self._waiter_lock:
            cached = self._execution_cache.get(order_id)
            if cached and cached.get("orderStatus") in ["Filled", "Cancelled", "Rejected"]:
                return cached

            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            self._order_waiters.setdefault(order_id, []).append(fut)

        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            async with self._waiter_lock:
                if order_id in self._order_waiters and fut in self._order_waiters[order_id]:
                    self._order_waiters[order_id].remove(fut)

    async def get_wallet_balance_usdt(self) -> float:
        """
        Extracts verified Bybit Unified Trading Account (UTA) or Classic Contract equity.
        Returns exact numeric balance; never returns hardcoded fake capital.
        """
        for acc_type in ["UNIFIED", "CONTRACT"]:
            try:
                response = await self._safe_api_call("GET", "/v5/account/wallet-balance", accountType=acc_type)
                if response.get("retCode") == 0:
                    accounts = response.get("result", {}).get("list", [])
                    if not accounts:
                        continue
                    
                    acc = accounts[0]
                    
                    total_equity = acc.get("totalEquity")
                    if total_equity not in (None, "", "0"):
                        val = float(total_equity)
                        if val > 0:
                            self._last_known_equity = val
                            logger.info(f"[X-RAY] Verified Bybit {acc_type} Total Equity: ${val:.2f} USDT")
                            return val

                    total_wallet = acc.get("totalWalletBalance")
                    if total_wallet not in (None, "", "0"):
                        val = float(total_wallet)
                        if val > 0:
                            self._last_known_equity = val
                            logger.info(f"[X-RAY] Verified Bybit {acc_type} Total Wallet Balance: ${val:.2f} USDT")
                            return val

                    total_margin = acc.get("totalMarginBalance")
                    if total_margin not in (None, "", "0"):
                        val = float(total_margin)
                        if val > 0:
                            self._last_known_equity = val
                            logger.info(f"[X-RAY] Verified Bybit {acc_type} Margin Balance: ${val:.2f} USDT")
                            return val

                    for coin_info in acc.get("coin", []):
                        if coin_info.get("coin") == "USDT":
                            eq = coin_info.get("equity") or coin_info.get("walletBalance")
                            if eq not in (None, "", "0"):
                                val = float(eq)
                                if val > 0:
                                    self._last_known_equity = val
                                    logger.info(f"[X-RAY] Verified USDT Coin Balance: ${val:.2f}")
                                    return val

            except Exception as e:
                logger.debug(f"[X-RAY] Balance verification failed on {acc_type}: {e}")

        if self._last_known_equity > 0:
            logger.warning(f"[X-RAY] Active balance fetch unfulfilled. Serving last verified equity: ${self._last_known_equity:.2f}")
            return self._last_known_equity

        return 0.0

    async def adjust_leverage(self, symbol: str, target_leverage: int) -> bool:
        """Sets contract leverage safely with cache invalidation and error code normalization."""
        try:
            if self._leverage_cache.get(symbol) == target_leverage:
                return True
                
            pos_info = await self._safe_api_call("GET", "/v5/position/list", category="linear", symbol=symbol)
            positions = pos_info.get("result", {}).get("list", [])
            if positions:
                current_leverage = int(float(positions[0].get("leverage", 1)))
                self._leverage_cache[symbol] = current_leverage
                if current_leverage == target_leverage:
                    return True

            res = await self._safe_api_call(
                "POST", "/v5/position/set-leverage", is_execution=True, 
                category="linear", symbol=symbol, buyLeverage=str(target_leverage), sellLeverage=str(target_leverage)
            )
            ret_code = res.get("retCode", -1)
            ret_msg = str(res.get("retMsg", "")).lower()

            if ret_code in (0, BybitRetCode.LEVERAGE_NOT_MODIFIED, BybitRetCode.LEVERAGE_NOT_MODIFIED_2) or "not modified" in ret_msg:
                self._leverage_cache[symbol] = target_leverage
                return True
                
            if ret_code == BybitRetCode.RISK_LIMIT_EXCEEDED or "risk limit" in ret_msg:
                info = await self._safe_api_call("GET", "/v5/market/instruments-info", category="linear", symbol=symbol)
                max_allowed = int(float(info["result"]["list"][0]["leverageFilter"]["maxLeverage"]))
                await self._safe_api_call(
                    "POST", "/v5/position/set-leverage", is_execution=True,
                    category="linear", symbol=symbol, buyLeverage=str(max_allowed), sellLeverage=str(max_allowed)
                )
                self._leverage_cache[symbol] = max_allowed
                return True

            return False
            
        except Exception as e:
            err_str = str(e).lower()
            if "not modified" in err_str or "110043" in err_str or "110025" in err_str:
                self._leverage_cache[symbol] = target_leverage
                return True
            return False

    async def get_top_volatile_assets(self, limit: int = 16, min_turnover: float = 15_000_000.0) -> List[str]:
        """Scans liquid perps while filtering out synthetic TradFi, commodities, and quarantined tokens."""
        banned_keywords = [
            "AAPL", "TSLA", "NVDA", "AMZN", "MSFT", "GOOG", "META", "SOXL",
            "SPCX", "SKHY", "SNDK", "BANK", "MUUSDT", "BEAT", "MSTR", "ESPUSDT",
            "DEXE", "PUMP", "EUL", "XAU", "XAG", "USDC", "CLUSDT", "SSPCUSDT",
            "KO", "HANMI", "LRCX", "PURR", "MUU", "XIAOMI", "INTW", "CLANKER",
            "AAOI", "COIN", "PLTR", "ARM", "BABA", "NIO", "AMD", "WTIUSDT", "BRENTUSDT"
        ]
        try:
            response = await self._safe_api_call("GET", "/v5/market/tickers", category="linear")
            tickers = response.get("result", {}).get("list", [])
            valid_assets = []
            for t in tickers:
                symbol = t.get("symbol", "")
                if not symbol.endswith("USDT") or any(b in symbol for b in banned_keywords):
                    continue
                if symbol.startswith(("PRE-", "INNO-", "TEST-")):
                    continue
                if symbol in self.temporary_symbol_bans:
                    if time.time() < self.temporary_symbol_bans[symbol]:
                        continue
                    else:
                        del self.temporary_symbol_bans[symbol]
                    
                turnover = float(t.get("turnover24h", 0.0) or 0.0)
                bid = float(t.get("bid1Price", 0.0) or 0.0)
                ask = float(t.get("ask1Price", 0.0) or 0.0)
                
                if bid <= 0 or ask <= bid or turnover < min_turnover:
                    continue
                
                high = float(t.get("highPrice24h", ask))
                low = float(t.get("lowPrice24h", bid))
                if low <= 0:
                    continue
                
                volatility_bps = ((high - low) / low) * 10000.0
                if volatility_bps < 180.0:
                    continue
                
                dynamic_spread_cap_bps = max(5.0, volatility_bps * 0.020)
                live_spread_bps = ((ask - bid) / bid) * 10000.0
                if live_spread_bps > dynamic_spread_cap_bps:
                    continue 
                
                bid_size = float(t.get("bid1Size", 0.0) or 0.0)
                ask_size = float(t.get("ask1Size", 0.0) or 0.0)
                top_depth_usd = min(bid * bid_size, ask * ask_size)
                if top_depth_usd < 200.0:
                    continue

                valid_assets.append({
                    "symbol": symbol, 
                    "spread_bps": live_spread_bps, 
                    "vol_bps": volatility_bps, 
                    "turnover": turnover
                })
                
            valid_assets.sort(key=lambda x: (x["vol_bps"] * math.log1p(x["turnover"])), reverse=True)
            top_symbols = [asset["symbol"] for asset in valid_assets[:limit]]
            logger.info(f"[X-RAY] RADAR DISCOVERED {len(top_symbols)} QUALIFIED LIQUID NODES.")
            return top_symbols if top_symbols else ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        except Exception as e:
            logger.error(f"[X-RAY] Failed to fetch global market tickers: {e}")
            return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    async def close(self):
        logger.info("Halting Bybit Exchange Connector...")
        self._is_terminating = True
        if hasattr(self, '_clock_sync_task') and self._clock_sync_task:
            self._clock_sync_task.cancel()
        if hasattr(self, '_ws_ping_task') and self._ws_ping_task:
            self._ws_ping_task.cancel()
        if self._ws_task:
            self._ws_task.cancel()
        if self._ws_connection and not self._ws_connection.closed:
            await self._ws_connection.close()
        if self.session and not self.session.closed:
            await self.session.close()