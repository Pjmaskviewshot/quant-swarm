"""
💎 V61.4 APEX NEURAL: PARALLELIZED UNIFIED API EXECUTOR
--------------------------------------------------------
Features Token-Bucket Rate Limiting, Thread-Isolated Dispatch, Smart Leverage Caching,
True Unified Equity Parsing, and Dynamic L2 Depth Ticker Discovery.
Upgraded with V61.4 Innovation Zone Auto-Banning (ErrCode 110126), 
Volatility-Adjusted Spread Coefficient (VASC), and Top-of-Book Depth Shields.
"""

import os
import time
import math
import asyncio
import logging
import functools
import concurrent.futures
from typing import Dict, Any, List, Optional
from pybit.unified_trading import HTTP

logger = logging.getLogger("QUANT_CORE.EXECUTION")

class BybitRetCode:
    """
    🚀 V61.4 BYBIT RETURN CODES
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
    AGREEMENT_NOT_SIGNED = 110126    # 🚀 V61.4: Innovation Zone UI agreement required


class TokenBucketRateLimiter:
    """
    🚀 TOKEN-BUCKET RATE LIMITER
    Throttles outbound API calls to strictly respect Bybit's private endpoint limits
    and prevent HTTP 429 rate-limit bans.
    """
    def __init__(self, capacity: int = 10, fill_rate: float = 5.0):
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.fill_rate = float(fill_rate)  # Tokens regenerated per second
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
            await asyncio.sleep(0.02)  # Tightened sleep for lower latency acquisition


class BybitUnifiedExecutor:
    """
    🚀 V61.4 PARALLELIZED UNIFIED API EXECUTOR
    Thread-isolated wrapper for Pybit V5 with automated rate-limiting, leverage caching,
    secrets scrubbing, Hyperion ticker filtering, and Priority Execution Lanes.
    """
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, max_workers: int = 12):
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        
        self.client = HTTP(
            testnet=testnet,
            api_key=api_key,
            api_secret=api_secret
        )
        
        # Expanded Execution Capacity for High-Frequency Chandelier Stops
        self.data_rate_limiter = TokenBucketRateLimiter(capacity=20, fill_rate=10.0)
        self.execution_rate_limiter = TokenBucketRateLimiter(capacity=30, fill_rate=15.0)
        
        self._api_thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, 
            thread_name_prefix="BybitIsolator"
        )
        self._leverage_cache: Dict[str, int] = {}
        self._banned_innovation_zones = set() # 🚀 V61.4 Dynamic Blacklist

    async def _safe_api_call(self, func, *args, **kwargs) -> Any:
        """
        🛡️ UNIFIED API GATEWAY
        Enforces rate-limiting, thread isolation, automatic retries on server load,
        fail-fast on parameter faults (10002), and token/secret scrubbing.
        """
        # Segment traffic: Priority express lane for crucial position management commands
        func_name = getattr(func, '__name__', '').lower()
        is_execution = any(word in func_name for word in ["place", "cancel", "amend", "set_trading_stop", "set_leverage"])
        
        if is_execution:
            await self.execution_rate_limiter.acquire()
        else:
            await self.data_rate_limiter.acquire()

        loop = asyncio.get_running_loop()
        bound_func = functools.partial(func, *args, **kwargs)
        
        for attempt in range(3):
            try:
                response = await loop.run_in_executor(self._api_thread_pool, bound_func)
                ret_code = response.get("retCode") if isinstance(response, dict) else 0
                
                # 🚀 V61.4 FAIL FAST: Innovation Zone Block
                if ret_code == BybitRetCode.AGREEMENT_NOT_SIGNED:
                    symbol_banned = kwargs.get("symbol", "UNKNOWN")
                    if symbol_banned != "UNKNOWN":
                        self._banned_innovation_zones.add(symbol_banned)
                    error_msg = f"[X-RAY] 🚫 110126 INNOVATION ZONE BLOCK: {symbol_banned} requires manual UI agreement. Banning from matrix."
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Fail Fast on Parameter Error
                if ret_code == BybitRetCode.PARAMETER_ERROR:
                    error_msg = f"[X-RAY] ❌ 10002 Parameter Fault: {response.get('retMsg', 'Unknown')}. Failing fast."
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Backoff on server load or rate limits
                if ret_code in [BybitRetCode.RATE_LIMIT_REACHED, BybitRetCode.SERVICE_UNAVAILABLE]: 
                    logger.warning(f"[X-RAY] ⚠️ Bybit System Load/Rate Limit (Code: {ret_code}). Backing off...")
                    await asyncio.sleep(2.0)
                    continue
                
                return response
                
            except Exception as e:
                error_str = str(e)
                # Scrub API credentials from output logs
                if self.api_key and self.api_key in error_str:
                    error_str = error_str.replace(self.api_key, "********")
                if self.api_secret and self.api_secret in error_str:
                    error_str = error_str.replace(self.api_secret, "********")
                
                # Catch raw exception strings for Innovation Zone blocks
                if "110126" in error_str:
                    symbol_banned = kwargs.get("symbol", "UNKNOWN")
                    if symbol_banned != "UNKNOWN":
                        self._banned_innovation_zones.add(symbol_banned)
                    raise ValueError(f"[X-RAY] 🚫 110126 INNOVATION ZONE BLOCK: {symbol_banned} banned from matrix.")

                if "10002 Parameter Fault" in error_str:
                    raise ValueError(error_str)
                    
                if attempt == 2:
                    logger.error(f"[X-RAY] ❌ Bybit API call failed after 3 attempts: {error_str}")
                    raise Exception(error_str)
                await asyncio.sleep(1.0)

    async def safe_call(self, func, *args, **kwargs) -> Any:
        """Public async gateway for executing raw pybit methods safely."""
        return await self._safe_api_call(func, *args, **kwargs)

    async def get_wallet_balance_usdt(self) -> float:
        """
        🚀 True Intelligent Equity Parsing.
        Calculates absolute Total Equity (Cash + Unrealized PnL), completely ignoring 
        Initial Margin locks. Prevents the "Margin Illusion" from triggering false drawdowns.
        """
        try:
            # Primary: Try Unified Trading Account
            response = await self._safe_api_call(
                self.client.get_wallet_balance, 
                accountType="UNIFIED"
            )
            
            if response.get("retCode") == 0:
                accounts = response.get("result", {}).get("list", [])
                if accounts:
                    true_equity = accounts[0].get("totalEquity") 
                    if true_equity is not None:
                        return float(true_equity)
                    
                    for coin_info in accounts[0].get("coin", []):
                        if coin_info.get("coin") == "USDT":
                            return float(coin_info.get("equity", 0.0))

            # Fallback: Try Standard Contract Account
            response_contract = await self._safe_api_call(
                self.client.get_wallet_balance, 
                accountType="CONTRACT",
                coin="USDT"
            )
            
            if response_contract.get("retCode") == 0:
                accounts = response_contract.get("result", {}).get("list", [])
                if accounts:
                    for coin_info in accounts[0].get("coin", []):
                        if coin_info.get("coin") == "USDT":
                            return float(coin_info.get("equity", coin_info.get("walletBalance", 0.0)))

            return 0.0
        except Exception as e:
            logger.error(f"[X-RAY] Failed to fetch true wallet balance: {e}")
            return 0.0

    async def adjust_leverage(self, symbol: str, target_leverage: int) -> bool:
        """
        🚀 Smart Leverage Caching with Error Guards.
        Only sends API updates when leverage differs from current exchange state.
        Uses structured integer return codes.
        """
        try:
            if self._leverage_cache.get(symbol) == target_leverage:
                return True
                
            pos_info = await self._safe_api_call(
                self.client.get_positions,
                category="linear",
                symbol=symbol
            )
            
            positions = pos_info.get("result", {}).get("list", [])
            if positions:
                current_leverage = int(float(positions[0].get("leverage", 1)))
                self._leverage_cache[symbol] = current_leverage
                if current_leverage == target_leverage:
                    return True

            # Push state update to Bybit
            res = await self._safe_api_call(
                self.client.set_leverage,
                category="linear",
                symbol=symbol,
                buyLeverage=str(target_leverage),
                sellLeverage=str(target_leverage)
            )
            
            self._leverage_cache[symbol] = target_leverage
            logger.info(f"[X-RAY] ⚙️ AUTO-SCALED LEVERAGE: {symbol} is now set to {target_leverage}x")
            return True
            
        except Exception as e:
            error_msg = str(e)
            ret_code = getattr(e, "ret_code", None) or getattr(e, "code", None)
            
            # Leverage not modified (already at target value)
            if ret_code == BybitRetCode.LEVERAGE_NOT_MODIFIED_2 or ret_code == BybitRetCode.LEVERAGE_NOT_MODIFIED or "110043" in error_msg or "not modified" in error_msg.lower():
                self._leverage_cache[symbol] = target_leverage
                return True
                
            # Requested leverage exceeds symbol's max risk tier limit
            if ret_code == BybitRetCode.RISK_LIMIT_EXCEEDED or "110013" in error_msg:
                try:
                    info = await self._safe_api_call(
                        self.client.get_instruments_info,
                        category="linear",
                        symbol=symbol
                    )
                    max_allowed = int(float(info["result"]["list"][0]["leverageFilter"]["maxLeverage"]))
                    logger.warning(f"[X-RAY] ⚠️ Risk Cap hit for {symbol}. Auto-clamping leverage from {target_leverage}x to {max_allowed}x.")
                    
                    await self._safe_api_call(
                        self.client.set_leverage,
                        category="linear",
                        symbol=symbol,
                        buyLeverage=str(max_allowed),
                        sellLeverage=str(max_allowed)
                    )
                    self._leverage_cache[symbol] = max_allowed
                    return True
                except Exception:
                    logger.error(f"[X-RAY] Leverage auto-clamping failed for {symbol}.")
                    return False
                    
            logger.error(f"[X-RAY] ❌ Failed to synchronize leverage matrix for {symbol}: {error_msg}")
            return False

    async def get_top_volatile_assets(self, limit: int = 6, min_turnover: float = 15_000_000.0) -> List[str]:
        """
        🚀 V61.4 TRUE DYNAMIC OMNI-SCANNER (VASC + DEPTH SHIELD)
        Calculates Volatility-Adjusted Spread Coefficient AND enforces a minimum
        Top-of-Book Depth Floor ($250 USD) to reject paper-thin, sweep-prone coins.
        Automatically filters out Innovation Zone blocks.
        """
        banned_keywords = ["SOXL", "SPCX", "SKHY", "SNDK", "BANK", "MUUSDT", "BEAT", "MSTR", "ESPUSDT", "DEXE", "PUMP", "EUL", "XAU", "XAG", "USDC"]
        
        try:
            response = await self._safe_api_call(
                self.client.get_tickers,
                category="linear"
            )
            
            tickers = response.get("result", {}).get("list", [])
            valid_assets = []
            
            for t in tickers:
                symbol = t.get("symbol", "")
                
                # 1. Broad Universe & Innovation Zone Filters
                if not symbol.endswith("USDT"): continue
                if any(b in symbol for b in banned_keywords): continue
                if symbol in self._banned_innovation_zones: continue # 🚀 V61.4 Innovation Zone Ban
                    
                turnover = float(t.get("turnover24h", 0.0) or 0.0)
                bid = float(t.get("bid1Price", 0.0) or 0.0)
                ask = float(t.get("ask1Price", 0.0) or 0.0)
                
                if bid <= 0 or ask <= bid or turnover < min_turnover: continue
                
                # 2. Extract True 24H Volatility
                high = float(t.get("highPrice24h", ask))
                low = float(t.get("lowPrice24h", bid))
                
                if low <= 0: continue
                volatility_bps = ((high - low) / low) * 10000.0
                
                # 3. Dead Market Filter (Requires > 2.0% daily variance to justify trading)
                if volatility_bps < 200.0: continue
                
                # 4. 🧬 THE VASC MATH (Volatility-Adjusted Spread Cap)
                dynamic_spread_cap_bps = max(5.0, volatility_bps * 0.015)
                
                # Calculate live spread
                live_spread_bps = ((ask - bid) / bid) * 10000.0
                
                # If the spread eats too much of the asset's daily range, reject it.
                if live_spread_bps > dynamic_spread_cap_bps: 
                    continue 
                
                # 5. 🛡️ V61.3 TOP-OF-BOOK DEPTH FLOOR (Hollow Book Shield)
                bid_size = float(t.get("bid1Size", 0.0) or 0.0)
                ask_size = float(t.get("ask1Size", 0.0) or 0.0)
                top_depth_usd = min(bid * bid_size, ask * ask_size)
                
                # If top level holds less than $250 USD, the coin is a hollow trap. Reject.
                if top_depth_usd < 250.0:
                    continue

                # 6. Asset Approval
                valid_assets.append({
                    "symbol": symbol,
                    "spread_bps": live_spread_bps,
                    "vol_bps": volatility_bps,
                    "turnover": turnover,
                    "top_depth_usd": top_depth_usd
                })
                
            # Rank dynamically by high-volatility momentum, weighted by turnover to prevent thin flash-crashes
            valid_assets.sort(key=lambda x: (x["vol_bps"] * math.log1p(x["turnover"])), reverse=True)
            top_symbols = [asset["symbol"] for asset in valid_assets[:limit]]
            
            logger.info(f"[X-RAY] 📡 NEURAL VASC RADAR DISCOVERED {len(top_symbols)} QUALIFIED LIQUID NODES.")
            return top_symbols if top_symbols else ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
            
        except Exception as e:
            logger.error(f"[X-RAY] ❌ Failed to fetch global market tickers: {e}")
            # Failsafe fallback 
            return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]