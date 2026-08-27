"""
💎 V22.0 APEX QUANTUM PRIME: UNIFIED API EXECUTOR
--------------------------------------------------------
Features Token-Bucket Rate Limiting, Pure Asyncio I/O, 
Smart Leverage Caching, and True Unified Equity Parsing.

Upgraded with V22.0:
- Eradicated ThreadPoolExecutor (Pure aiohttp REST & WS)
- Zero-Latency Private WebSocket Execution Tracking
- Thread-Safe Asyncio Future Bridging
- Innovation Zone Cooldowns (ErrCode 110126)
- Volatility-Adjusted Spread Coefficient (VASC) 
- Top-of-Book Depth Shields
"""

import time
import math
import hmac
import hashlib
import asyncio
import logging
import json
import urllib.parse
from typing import Dict, Any, List, Optional

import aiohttp

logger = logging.getLogger("QUANT_CORE.BYBIT")

class BybitRetCode:
    """
    🚀 V22.0 BYBIT RETURN CODES
    Structured integer mapping to eliminate fragile string-matching on API errors.
    """
    SUCCESS = 0
    PARAMETER_ERROR = 10002          # Invalid request parameter
    SYSTEM_MAINTENANCE = 10004       # Server maintenance window
    RATE_LIMIT_REACHED = 10006       # Too many requests
    QTY_OUT_OF_BOUNDS = 10001        # Invalid parameter / quantity step error
    SERVICE_UNAVAILABLE = 10016      # Service temporary error
    ORDER_NOT_EXISTS = 110001        # Order does not exist or too late to cancel
    INSUFFICIENT_BALANCE = 110007    # Abundant/insufficient balance
    RISK_LIMIT_EXCEEDED = 110013     # Requested leverage exceeds symbol's max risk tier limit
    LEVERAGE_NOT_MODIFIED = 110025   # Position mode or leverage already set
    LEVERAGE_NOT_MODIFIED_2 = 110043 # Set leverage not modified
    AGREEMENT_NOT_SIGNED = 110126    # Innovation Zone UI agreement required


class TokenBucketRateLimiter:
    """
    🚀 TOKEN-BUCKET RATE LIMITER
    Throttles outbound API calls to strictly respect Bybit's private endpoint limits.
    """
    def __init__(self, capacity: int = 10, fill_rate: float = 5.0):
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.fill_rate = float(fill_rate) 
        self.last_fill_time = time.time()
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                now = time.time()
                elapsed = now - self.last_fill_time
                self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)
                self.last_fill_time = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
            await asyncio.sleep(0.02) 


class BybitUnifiedExecutor:
    """
    🚀 V22.0 PURE ASYNC UNIFIED API EXECUTOR
    Native aiohttp wrapper for Bybit V5 with automated rate-limiting, leverage caching,
    VASC ticker filtering, and Priority Execution Lanes.
    """
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        self.testnet = testnet
        
        # Network Routing
        self.rest_base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
        self.ws_private_url = "wss://stream-testnet.bybit.com/v5/private" if testnet else "wss://stream.bybit.com/v5/private"
        
        # Priority lanes to ensure orders fire even if data polling is saturated
        self.data_rate_limiter = TokenBucketRateLimiter(capacity=20, fill_rate=10.0)
        self.execution_rate_limiter = TokenBucketRateLimiter(capacity=30, fill_rate=15.0)
        
        self.session: Optional[aiohttp.ClientSession] = None
        self._server_time_offset_ms: int = 0
        self._leverage_cache: Dict[str, int] = {}
        self.temporary_symbol_bans: Dict[str, float] = {}

        # WebSocket Integration State (Zero-Latency Fill Tracking)
        self._ws_connection: Optional[aiohttp.ClientWebSocketResponse] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._order_waiters: Dict[str, List[asyncio.Future]] = {}
        self._execution_cache: Dict[str, Dict[str, Any]] = {}
        self._waiter_lock = asyncio.Lock()
        self._is_terminating = False
        
        logger.info(f"Initialized Pure-Async Bybit V5 Executor (Testnet: {self.testnet})")

    # =========================================================================
    # 1. CORE REST ENGINE & CLOCK SYNCHRONIZATION
    # =========================================================================

    async def initialize(self):
        """Bootstraps the HTTP session and calibrates the clock."""
        if not self.session:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10.0))
        await self.calibrate_server_time()

    async def calibrate_server_time(self) -> int:
        """Computes exact network latency and clock drift to prevent 10002 errors."""
        try:
            start_local = int(time.time() * 1000)
            async with self.session.get(f"{self.rest_base_url}/v5/market/time") as resp:
                data = await resp.json()
            end_local = int(time.time() * 1000)

            if data.get("retCode") == 0:
                server_time = int(data["result"]["timeNano"]) // 1_000_000
                latency = (end_local - start_local) // 2
                self._server_time_offset_ms = server_time - (end_local - latency)
                logger.info(f"Clock Calibrated. Offset: {self._server_time_offset_ms}ms (Latency: {latency * 2}ms)")
                return self._server_time_offset_ms
        except Exception as e:
            logger.warning(f"Clock calibration failed: {e}. Defaulting to host time.")
        return 0

    def _generate_signature(self, timestamp: str, payload: str) -> str:
        param_str = f"{timestamp}{self.api_key}5000{payload}"
        return hmac.new(
            self.api_secret.encode("utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

    async def _safe_api_call(self, method: str, endpoint: str, is_execution: bool = False, **kwargs) -> Any:
        """
        🛡️ UNIFIED API GATEWAY
        Enforces rate-limiting, thread-free async I/O, and token/secret scrubbing.
        """
        if not self.session:
            await self.initialize()

        if is_execution:
            await self.execution_rate_limiter.acquire()
        else:
            await self.data_rate_limiter.acquire()

        for attempt in range(3):
            try:
                timestamp = str(int(time.time() * 1000) + self._server_time_offset_ms)
                payload = ""

                if method == "GET":
                    if kwargs:
                        payload = urllib.parse.urlencode(kwargs)
                        endpoint_full = f"{endpoint}?{payload}"
                    else:
                        endpoint_full = endpoint
                else:
                    payload = json.dumps(kwargs) if kwargs else ""
                    endpoint_full = endpoint

                signature = self._generate_signature(timestamp, payload)
                headers = {
                    "X-BAPI-API-KEY": self.api_key,
                    "X-BAPI-TIMESTAMP": timestamp,
                    "X-BAPI-SIGN": signature,
                    "X-BAPI-RECV-WINDOW": "5000",
                    "Content-Type": "application/json"
                }

                url = f"{self.rest_base_url}{endpoint_full}"
                async with self.session.request(method, url, headers=headers, data=payload if method == "POST" else None) as resp:
                    response = await resp.json()

                ret_code = response.get("retCode", -1)
                
                # Innovation Zone Block (Temporary Cooldown)
                if ret_code == BybitRetCode.AGREEMENT_NOT_SIGNED:
                    symbol_banned = kwargs.get("symbol", "UNKNOWN")
                    if symbol_banned != "UNKNOWN":
                        self.temporary_symbol_bans[symbol_banned] = time.time() + 900.0 
                    error_msg = f"[X-RAY] 🚫 110126 INNOVATION ZONE: {symbol_banned} requires UI agreement. 15m cooldown applied."
                    logger.warning(error_msg)
                    raise ValueError(error_msg)

                if ret_code == BybitRetCode.PARAMETER_ERROR:
                    error_msg = f"[X-RAY] ❌ 10002 Parameter Fault: {response.get('retMsg', 'Unknown')}. Failing fast."
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                if ret_code in [BybitRetCode.RATE_LIMIT_REACHED, BybitRetCode.SERVICE_UNAVAILABLE]: 
                    logger.warning(f"[X-RAY] ⚠️ Bybit System Load/Rate Limit (Code: {ret_code}). Backing off...")
                    await asyncio.sleep(2.0)
                    continue
                
                return response
                
            except asyncio.TimeoutError:
                logger.warning(f"[X-RAY] ⚠️ API Call to {endpoint} timed out. Retrying ({attempt+1}/3)...")
                await asyncio.sleep(1.0)
                if attempt == 2:
                    raise Exception(f"API Call Timeout for {endpoint} after 3 attempts.")
                continue

            except Exception as e:
                error_str = str(e)
                if self.api_key and self.api_key in error_str:
                    error_str = error_str.replace(self.api_key, "********")
                if self.api_secret and self.api_secret in error_str:
                    error_str = error_str.replace(self.api_secret, "********")
                
                if attempt == 2:
                    logger.error(f"[X-RAY] ❌ Bybit API call failed after 3 attempts: {error_str}")
                    raise Exception(error_str)
                await asyncio.sleep(1.0)

    # =========================================================================
    # 2. PRIVATE WEBSOCKET INTEGRATION
    # =========================================================================

    async def connect_ws(self):
        """Initializes the private WebSocket stream inside the main event loop."""
        if not self.session:
            await self.initialize()
        self._is_terminating = False
        self._ws_task = asyncio.create_task(self._ws_lifecycle_loop())
        logger.info("📡 Bybit Private WebSocket Stream Connected. Zero-Latency Tracking Armed.")

    async def _ws_lifecycle_loop(self):
        backoff = 1.0
        while not self._is_terminating:
            try:
                async with self.session.ws_connect(self.ws_private_url, autoping=True, heartbeat=20.0) as ws:
                    self._ws_connection = ws
                    backoff = 1.0

                    expires = int(time.time() * 1000) + self._server_time_offset_ms + 10000
                    signature = hmac.new(
                        self.api_secret.encode("utf-8"),
                        f"GET/realtime{expires}".encode("utf-8"),
                        hashlib.sha256
                    ).hexdigest()

                    await ws.send_json({"op": "auth", "args": [self.api_key, expires, signature]})
                    auth_resp = await ws.receive_json()

                    if not auth_resp.get("success"):
                        logger.critical(f"WS Auth Failed: {auth_resp.get('ret_msg')}")
                        await asyncio.sleep(5.0)
                        continue

                    await ws.send_json({"op": "subscribe", "args": ["execution", "order"]})

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._on_ws_message(json.loads(msg.data))
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"WebSocket disconnected ({e}). Reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 1.5)

    async def _on_ws_message(self, message: dict):
        topic = message.get("topic", "")
        data = message.get("data", [])

        if topic == "order":
            for order in data:
                order_id = order.get("orderId")
                status = order.get("orderStatus")
                if order_id:
                    self._execution_cache[order_id] = order
                    if status in ["Filled", "PartiallyFilled", "Cancelled", "Rejected"]:
                        await self._resolve_ws_future(order_id, order)
        elif topic == "execution":
            for exec_report in data:
                order_id = exec_report.get("orderId")
                if order_id:
                    synthetic_event = {
                        "orderId": order_id,
                        "cumExecQty": exec_report.get("execQty"),
                        "avgPrice": exec_report.get("execPrice"),
                        "orderStatus": "PartiallyFilled",
                        "isWsExecution": True
                    }
                    await self._resolve_ws_future(order_id, synthetic_event)

    async def _resolve_ws_future(self, order_id: str, data: dict):
        async with self._waiter_lock:
            waiters = self._order_waiters.pop(order_id, [])
            for fut in waiters:
                if not fut.done():
                    fut.set_result(data)

    async def await_ws_execution_report(self, order_id: str, timeout: float = 0.2) -> Optional[Dict[str, Any]]:
        """O(1) Zero-Latency Execution Tracker."""
        cached = self._execution_cache.get(order_id)
        if cached and cached.get("orderStatus") in ["Filled", "Cancelled", "Rejected"]:
            return cached

        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        async with self._waiter_lock:
            self._order_waiters.setdefault(order_id, []).append(fut)

        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            async with self._waiter_lock:
                if order_id in self._order_waiters and fut in self._order_waiters[order_id]:
                    self._order_waiters[order_id].remove(fut)

    # =========================================================================
    # 3. HIGH-LEVEL TRADING ABSTRACTIONS
    # =========================================================================

    async def get_wallet_balance_usdt(self) -> float:
        """Calculates absolute Total Equity (Cash + Unrealized PnL)."""
        try:
            response = await self._safe_api_call("GET", "/v5/account/wallet-balance", accountType="UNIFIED", coin="USDT")
            if response.get("retCode") == 0:
                accounts = response.get("result", {}).get("list", [])
                if accounts:
                    true_equity = accounts[0].get("totalEquity") 
                    if true_equity is not None:
                        return float(true_equity)
                    for coin_info in accounts[0].get("coin", []):
                        if coin_info.get("coin") == "USDT":
                            return float(coin_info.get("equity", 0.0))
            return 0.0
        except Exception as e:
            logger.error(f"[X-RAY] Failed to fetch true wallet balance: {e}")
            return 0.0

    async def adjust_leverage(self, symbol: str, target_leverage: int) -> bool:
        """Smart Leverage Caching with Error Guards."""
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
            
            self._leverage_cache[symbol] = target_leverage
            logger.info(f"[X-RAY] ⚙️ AUTO-SCALED LEVERAGE: {symbol} is now set to {target_leverage}x")
            return True
            
        except Exception as e:
            error_msg = str(e)
            if "110043" in error_msg or "110025" in error_msg or "not modified" in error_msg.lower():
                self._leverage_cache[symbol] = target_leverage
                return True
                
            if "110013" in error_msg or "Risk limit exceeded" in error_msg:
                try:
                    info = await self._safe_api_call("GET", "/v5/market/instruments-info", category="linear", symbol=symbol)
                    max_allowed = int(float(info["result"]["list"][0]["leverageFilter"]["maxLeverage"]))
                    logger.warning(f"[X-RAY] ⚠️ Risk Cap hit for {symbol}. Auto-clamping leverage from {target_leverage}x to {max_allowed}x.")
                    
                    await self._safe_api_call(
                        "POST", "/v5/position/set-leverage", is_execution=True,
                        category="linear", symbol=symbol, buyLeverage=str(max_allowed), sellLeverage=str(max_allowed)
                    )
                    self._leverage_cache[symbol] = max_allowed
                    return True
                except Exception:
                    logger.error(f"[X-RAY] Leverage auto-clamping failed for {symbol}.")
                    return False
                    
            logger.error(f"[X-RAY] ❌ Failed to synchronize leverage matrix for {symbol}: {error_msg}")
            return False

    async def get_top_volatile_assets(self, limit: int = 16, min_turnover: float = 15_000_000.0) -> List[str]:
        """
        🚀 TRUE DYNAMIC OMNI-SCANNER (VASC + DEPTH SHIELD)
        """
        banned_keywords = ["SOXL", "SPCX", "SKHY", "SNDK", "BANK", "MUUSDT", "BEAT", "MSTR", "ESPUSDT", "DEXE", "PUMP", "EUL", "XAU", "XAG", "USDC"]
        
        try:
            response = await self._safe_api_call("GET", "/v5/market/tickers", category="linear")
            tickers = response.get("result", {}).get("list", [])
            valid_assets = []
            
            for t in tickers:
                symbol = t.get("symbol", "")
                
                if not symbol.endswith("USDT"): continue
                if any(b in symbol for b in banned_keywords): continue
                
                if symbol in self.temporary_symbol_bans:
                    if time.time() < self.temporary_symbol_bans[symbol]: continue
                    else: del self.temporary_symbol_bans[symbol]
                    
                turnover = float(t.get("turnover24h", 0.0) or 0.0)
                bid = float(t.get("bid1Price", 0.0) or 0.0)
                ask = float(t.get("ask1Price", 0.0) or 0.0)
                
                if bid <= 0 or ask <= bid or turnover < min_turnover: continue
                
                high = float(t.get("highPrice24h", ask))
                low = float(t.get("lowPrice24h", bid))
                if low <= 0: continue
                
                volatility_bps = ((high - low) / low) * 10000.0
                if volatility_bps < 200.0: continue
                
                # 🧬 THE VASC MATH (Volatility-Adjusted Spread Cap)
                dynamic_spread_cap_bps = max(5.0, volatility_bps * 0.015)
                live_spread_bps = ((ask - bid) / bid) * 10000.0
                
                if live_spread_bps > dynamic_spread_cap_bps: continue 
                
                # 🛡️ TOP-OF-BOOK DEPTH FLOOR (Hollow Book Shield)
                bid_size = float(t.get("bid1Size", 0.0) or 0.0)
                ask_size = float(t.get("ask1Size", 0.0) or 0.0)
                top_depth_usd = min(bid * bid_size, ask * ask_size)
                
                if top_depth_usd < 250.0: continue

                valid_assets.append({
                    "symbol": symbol,
                    "spread_bps": live_spread_bps,
                    "vol_bps": volatility_bps,
                    "turnover": turnover,
                    "top_depth_usd": top_depth_usd
                })
                
            valid_assets.sort(key=lambda x: (x["vol_bps"] * math.log1p(x["turnover"])), reverse=True)
            top_symbols = [asset["symbol"] for asset in valid_assets[:limit]]
            
            logger.info(f"[X-RAY] 📡 NEURAL VASC RADAR DISCOVERED {len(top_symbols)} QUALIFIED LIQUID NODES.")
            return top_symbols if top_symbols else ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
            
        except Exception as e:
            logger.error(f"[X-RAY] ❌ Failed to fetch global market tickers: {e}")
            return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    async def close(self):
        """Gracefully halts active socket streams and requests."""
        logger.info("Halting Bybit Exchange Connector...")
        self._is_terminating = True
        
        if self._ws_task:
            self._ws_task.cancel()
        if self._ws_connection and not self._ws_connection.closed:
            await self._ws_connection.close()
        if self.session and not self.session.closed:
            await self.session.close()