"""
CENTRAL RUNTIME CONFIGURATION & BUILD IDENTITY
--------------------------------------------------------------------------------
Single source of truth for operating mode, build revision, and the safety
thresholds that were previously scattered as literals across main.py, sor.py,
risk_vault.py, intelligent_exit.py, omni_scanner.py and bybit_v5.py.

Addresses audit findings:
  B7   — default mode was LIVE; there was no LIVE_TRADING gate at all.
  B26  — the banned-asset list was duplicated across four modules.
  NEW-1— the deployed revision was unidentifiable (/health returned a literal).
  B19/B20 — market-data freshness limits had no configuration home.
  B21  — min-notional behaviour was implicit.

Design rule: this module NEVER guesses. An ambiguous or unsafe configuration
raises at import/boot rather than resolving to something permissive.
"""

from __future__ import annotations

import os
import subprocess
import logging
from dataclasses import dataclass
from enum import Enum
from typing import FrozenSet, Optional

logger = logging.getLogger("QUANT_CORE.CONFIG")


class ConfigurationError(RuntimeError):
    """Raised when configuration is unsafe or ambiguous. Never caught to a default."""


class TradingMode(str, Enum):
    PAPER = "PAPER"        # No exchange orders. Full lifecycle simulated.
    TESTNET = "TESTNET"    # Real orders against Bybit testnet.
    LIVE = "LIVE"          # Real capital.

    @property
    def places_real_orders(self) -> bool:
        return self is not TradingMode.PAPER

    @property
    def uses_testnet_endpoints(self) -> bool:
        return self is TradingMode.TESTNET


# --- B26: the single banned-asset list ------------------------------------
# Matching is EXACT on the normalised base asset, not substring: the previous
# substring form banned any symbol merely CONTAINING "KO", "ARM", "AMD", "BANK".
BANNED_BASE_ASSETS: FrozenSet[str] = frozenset({
    # TradFi equities / synthetics
    "AAPL", "TSLA", "NVDA", "AMZN", "MSFT", "GOOG", "META", "SOXL", "SPCX",
    "SKHY", "SNDK", "BANK", "BEAT", "MSTR", "KO", "HANMI", "LRCX", "XIAOMI",
    "INTW", "AAOI", "COIN", "PLTR", "ARM", "BABA", "NIO", "AMD",
    # Commodities
    "XAU", "XAG", "WTI", "BRENT",
    # Stables / settlement
    "USDC",
    # Illiquid or restricted
    "DEXE", "PUMP", "EUL", "PURR", "MUU", "CLANKER", "CL", "SSPC", "ESP",
})

BANNED_SYMBOL_PREFIXES = ("PRE-", "INNO-", "TEST-")


def normalise_base_asset(symbol: str) -> str:
    """
    'BTCUSDT' -> 'BTC'; '1000PEPEUSDT' -> 'PEPE'.
    Used so bans match the asset, not an accidental substring.
    """
    s = (symbol or "").upper().strip()
    if s.endswith("USDT"):
        s = s[:-4]
    i = 0
    while i < len(s) and s[i].isdigit():
        i += 1
    return s[i:] if i < len(s) else s


def is_banned_symbol(symbol: str) -> bool:
    s = (symbol or "").upper().strip()
    if any(s.startswith(p) for p in BANNED_SYMBOL_PREFIXES):
        return True
    return normalise_base_asset(s) in BANNED_BASE_ASSETS


# --- NEW-1: build identity -------------------------------------------------

def resolve_build_revision() -> str:
    """
    The commit this process is running. Checked in order:
      1. explicit BUILD_SHA
      2. platform-injected vars (Render / Railway / GitHub Actions)
      3. local git
      4. 'unknown'
    Returning 'unknown' is itself actionable: it means the deploy cannot be
    verified against the repository.
    """
    for var in ("BUILD_SHA", "RENDER_GIT_COMMIT", "RAILWAY_GIT_COMMIT_SHA", "GITHUB_SHA"):
        val = os.getenv(var, "").strip()
        if val:
            return val
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except Exception:
        pass
    return "unknown"


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        logger.warning(f"[CONFIG] {name} unparseable; using default {default}")
        return default


@dataclass(frozen=True)
class RuntimeConfig:
    mode: TradingMode
    build_revision: str

    # B1
    notional_deviation_tolerance: float = 0.25
    # B21 — skip rather than inflate a sub-minimum order
    skip_below_min_notional: bool = True
    # B19/B20 — market-data freshness
    max_orderbook_age_sec: float = 5.0
    max_tick_age_sec: float = 10.0
    max_execution_data_age_sec: float = 30.0
    # B10 — reservation TTL derived from the slowest route
    execution_budget_sec: float = 120.0

    @property
    def is_live(self) -> bool:
        return self.mode is TradingMode.LIVE

    @classmethod
    def from_env(cls) -> "RuntimeConfig":
        """
        B7 resolution. Default is PAPER. LIVE is reachable only by explicitly
        setting LIVE_TRADING=1 AND supplying credentials.
        """
        live_requested = _env_bool("LIVE_TRADING", False)
        explicit = os.getenv("TRADING_MODE", "").strip().upper()
        legacy_test_mode = os.getenv("TEST_MODE")

        has_creds = bool(
            os.getenv("BYBIT_API_KEY", "").strip()
            and os.getenv("BYBIT_API_SECRET", "").strip()
        )

        if explicit:
            if explicit not in TradingMode.__members__:
                raise ConfigurationError(
                    f"TRADING_MODE={explicit!r} is not one of "
                    f"{sorted(TradingMode.__members__)}. Refusing to boot."
                )
            mode = TradingMode[explicit]
            if mode is TradingMode.LIVE and not live_requested:
                raise ConfigurationError(
                    "TRADING_MODE=LIVE requires LIVE_TRADING=1. Refusing to boot."
                )
        elif live_requested:
            mode = TradingMode.LIVE
        elif legacy_test_mode is not None and legacy_test_mode.strip().lower() in ("1", "true", "yes", "on"):
            # Backwards compatibility: the old TEST_MODE=true meant testnet.
            logger.warning(
                "[CONFIG] Legacy TEST_MODE detected. Interpreting as TRADING_MODE=TESTNET. "
                "Please migrate to TRADING_MODE."
            )
            mode = TradingMode.TESTNET
        else:
            mode = TradingMode.PAPER

        if mode is TradingMode.LIVE and not has_creds:
            raise ConfigurationError(
                "LIVE mode requested but BYBIT_API_KEY/BYBIT_API_SECRET are not set. "
                "Refusing to boot."
            )

        return cls(
            mode=mode,
            build_revision=resolve_build_revision(),
            notional_deviation_tolerance=_env_float("NOTIONAL_DEVIATION_TOLERANCE", 0.25),
            skip_below_min_notional=_env_bool("SKIP_BELOW_MIN_NOTIONAL", True),
            max_orderbook_age_sec=_env_float("MAX_ORDERBOOK_AGE_SEC", 5.0),
            max_tick_age_sec=_env_float("MAX_TICK_AGE_SEC", 10.0),
            max_execution_data_age_sec=_env_float("MAX_EXECUTION_DATA_AGE_SEC", 30.0),
            execution_budget_sec=_env_float("EXECUTION_BUDGET_SEC", 120.0),
        )

    def banner(self) -> str:
        rev = self.build_revision[:12] if self.build_revision != "unknown" else "UNKNOWN"
        lines = [
            "=" * 66,
            f"  MODE={self.mode.value}",
            f"  BUILD={rev}",
            f"  REAL ORDERS: {'YES' if self.mode.places_real_orders else 'NO'}",
        ]
        if self.mode is TradingMode.LIVE:
            lines.append("  *** REAL CAPITAL AT RISK ***")
        if self.build_revision == "unknown":
            lines.append("  WARNING: build revision unknown - deploy is unverifiable")
        lines.append("=" * 66)
        return "\n".join(lines)


_ACTIVE: Optional[RuntimeConfig] = None


def get_config() -> RuntimeConfig:
    global _ACTIVE
    if _ACTIVE is None:
        _ACTIVE = RuntimeConfig.from_env()
    return _ACTIVE


def reset_config_for_tests() -> None:
    global _ACTIVE
    _ACTIVE = None
