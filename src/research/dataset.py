"""
DATASET LAYER — real klines from a local cache, or synthetic series with known
ground truth.

Why a cache rather than live fetches: `backtest.py` re-downloaded klines from
api.bybit.com on every run, so results were not reproducible (the window moved),
every experiment paid the network cost, and the whole research loop was
unusable anywhere without exchange egress.

Datasets are addressed by a content hash, so an experiment can record exactly
which bytes produced a result.

SYNTHETIC SERIES exist to validate the MEASUREMENT APPARATUS, never the
strategy. A generator with a known edge must be detected; a generator with no
edge must NOT be. That is a test of the instrument.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import pathlib
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

DEFAULT_CACHE = pathlib.Path(
    os.getenv("KLINE_CACHE_DIR", pathlib.Path(__file__).resolve().parents[2] / "data" / "klines")
)


@dataclass
class Dataset:
    """A named, hashable OHLCV series."""
    name: str
    symbol: str
    interval: str
    candles: List[Dict[str, float]]
    source: str                      # "cache" | "synthetic"
    meta: Dict[str, Any] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.candles)

    @property
    def start_ts(self) -> int:
        return int(self.candles[0]["ts"]) if self.candles else 0

    @property
    def end_ts(self) -> int:
        return int(self.candles[-1]["ts"]) if self.candles else 0

    def content_hash(self) -> str:
        h = hashlib.sha256()
        for c in self.candles:
            h.update(f"{c['ts']}{c['open']}{c['high']}{c['low']}{c['close']}{c['volume']}".encode())
        return h.hexdigest()[:16]

    def describe(self) -> Dict[str, Any]:
        return {
            "name": self.name, "symbol": self.symbol, "interval": self.interval,
            "bars": len(self.candles), "source": self.source,
            "start_ts": self.start_ts, "end_ts": self.end_ts,
            "content_hash": self.content_hash(), **self.meta,
        }

    def split(self, train_frac: float = 0.6, embargo_bars: int = 240
              ) -> Tuple["Dataset", "Dataset"]:
        """
        Chronological split with an EMBARGO gap.

        The gap matters: a trade opened near the end of training can still be
        open at the start of test, so without it the two sets overlap in
        outcome space and the "out-of-sample" result is contaminated.
        """
        n = len(self.candles)
        cut = int(n * train_frac)
        train = self.candles[:cut]
        test = self.candles[cut + embargo_bars:]
        return (
            Dataset(f"{self.name}:train", self.symbol, self.interval, train, self.source,
                    {**self.meta, "split": "train", "embargo_bars": embargo_bars}),
            Dataset(f"{self.name}:test", self.symbol, self.interval, test, self.source,
                    {**self.meta, "split": "test", "embargo_bars": embargo_bars}),
        )


# ----------------------------------------------------------------------------
# Cache
# ----------------------------------------------------------------------------

def cache_path(symbol: str, interval: str, cache_dir: Optional[pathlib.Path] = None) -> pathlib.Path:
    d = pathlib.Path(cache_dir or DEFAULT_CACHE)
    return d / f"{symbol.upper()}_{interval}.json"


def save_to_cache(symbol: str, interval: str, candles: List[Dict[str, float]],
                  cache_dir: Optional[pathlib.Path] = None) -> pathlib.Path:
    p = cache_path(symbol, interval, cache_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({
        "symbol": symbol.upper(), "interval": interval,
        "bars": len(candles), "candles": candles,
    }))
    return p


def load_from_cache(symbol: str, interval: str = "1",
                    cache_dir: Optional[pathlib.Path] = None) -> Dataset:
    p = cache_path(symbol, interval, cache_dir)
    if not p.exists():
        raise FileNotFoundError(
            f"No cached klines at {p}.\n"
            f"This environment has no exchange egress, so data must be fetched where it does:\n"
            f"    python scripts/fetch_klines.py --symbol {symbol} --interval {interval} --days 30\n"
            f"then copy the resulting file into {p.parent}."
        )
    raw = json.loads(p.read_text())
    return Dataset(
        name=f"{symbol.upper()}_{interval}", symbol=raw["symbol"], interval=raw["interval"],
        candles=raw["candles"], source="cache",
    )


def available_datasets(cache_dir: Optional[pathlib.Path] = None) -> List[str]:
    d = pathlib.Path(cache_dir or DEFAULT_CACHE)
    return sorted(p.stem for p in d.glob("*.json")) if d.exists() else []


# ----------------------------------------------------------------------------
# Synthetic generators — for validating the INSTRUMENT
# ----------------------------------------------------------------------------

def _ohlc_from_path(prices: np.ndarray, start_ts: int, interval_ms: int,
                    vol_scale: float, rng: np.random.Generator) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    prev = float(prices[0])
    for i, close in enumerate(prices):
        close = float(close)
        hi = max(prev, close) * (1.0 + abs(rng.normal(0, 0.0004)))
        lo = min(prev, close) * (1.0 - abs(rng.normal(0, 0.0004)))
        out.append({
            "ts": start_ts + i * interval_ms,
            "open": prev, "high": hi, "low": lo, "close": close,
            "volume": float(max(1.0, rng.lognormal(math.log(vol_scale), 0.5))),
        })
        prev = close
    return out


def synthetic_random_walk(n: int = 20_000, seed: int = 7, start: float = 100.0,
                          sigma: float = 0.0008) -> Dataset:
    """
    A driftless geometric random walk. **There is no edge here.**

    Any strategy that reports a positive expectancy on this data, after costs,
    is measuring an artefact — look-ahead, survivorship, or a cost model that
    is too kind. This is the instrument's null hypothesis.
    """
    rng = np.random.default_rng(seed)
    steps = rng.normal(0.0, sigma, n)
    prices = start * np.exp(np.cumsum(steps))
    return Dataset(
        name=f"synthetic_random_walk_s{seed}", symbol="SYNTHUSDT", interval="1",
        candles=_ohlc_from_path(prices, 1_700_000_000_000, 60_000, 1000.0, rng),
        source="synthetic",
        meta={"generator": "random_walk", "seed": seed, "sigma": sigma,
              "expected_edge": "NONE"},
    )


def synthetic_momentum(n: int = 20_000, seed: int = 11, start: float = 100.0,
                       sigma: float = 0.0008, phi: float = 0.35) -> Dataset:
    """
    An AR(1) series with POSITIVE autocorrelation — a genuine, detectable
    momentum edge. A correct instrument must find it.

    phi is the persistence: r_t = phi * r_{t-1} + noise.
    """
    rng = np.random.default_rng(seed)
    r = np.zeros(n)
    eps = rng.normal(0.0, sigma, n)
    for i in range(1, n):
        r[i] = phi * r[i - 1] + eps[i]
    prices = start * np.exp(np.cumsum(r))
    return Dataset(
        name=f"synthetic_momentum_s{seed}_phi{phi}", symbol="SYNTHUSDT", interval="1",
        candles=_ohlc_from_path(prices, 1_700_000_000_000, 60_000, 1000.0, rng),
        source="synthetic",
        meta={"generator": "ar1_momentum", "seed": seed, "phi": phi,
              "expected_edge": "POSITIVE_MOMENTUM"},
    )


def synthetic_mean_reverting(n: int = 20_000, seed: int = 13, start: float = 100.0,
                             sigma: float = 0.0008, phi: float = -0.35) -> Dataset:
    """AR(1) with NEGATIVE autocorrelation — a detectable mean-reversion edge."""
    ds = synthetic_momentum(n=n, seed=seed, start=start, sigma=sigma, phi=phi)
    ds.name = f"synthetic_mean_revert_s{seed}_phi{phi}"
    ds.meta.update({"generator": "ar1_mean_reversion", "expected_edge": "MEAN_REVERSION"})
    return ds


def synthetic_regime_shift(n: int = 20_000, seed: int = 17) -> Dataset:
    """
    Momentum for the first half, mean reversion for the second.

    A strategy fitted on the first half and validated on the second SHOULD
    degrade. If it does not, the validation is not actually out-of-sample.
    """
    half = n // 2
    a = synthetic_momentum(n=half, seed=seed, phi=0.35)
    last = a.candles[-1]["close"]
    b = synthetic_mean_reverting(n=n - half, seed=seed + 1, start=last, phi=-0.35)
    shift = a.candles[-1]["ts"] + 60_000
    for i, c in enumerate(b.candles):
        c["ts"] = shift + i * 60_000
    return Dataset(
        name=f"synthetic_regime_shift_s{seed}", symbol="SYNTHUSDT", interval="1",
        candles=a.candles + b.candles, source="synthetic",
        meta={"generator": "regime_shift", "seed": seed, "shift_at_bar": half,
              "expected_edge": "REGIME_DEPENDENT"},
    )


SYNTHETIC_GENERATORS = {
    "random_walk": synthetic_random_walk,
    "momentum": synthetic_momentum,
    "mean_reverting": synthetic_mean_reverting,
    "regime_shift": synthetic_regime_shift,
}


def load_dataset(spec: str) -> Dataset:
    """
    Resolve a dataset spec.

        "synthetic:momentum"      -> generated, known ground truth
        "synthetic:momentum:42"   -> with an explicit seed
        "BTCUSDT:1"               -> from the local kline cache
    """
    if spec.startswith("synthetic:"):
        parts = spec.split(":")
        gen = parts[1]
        if gen not in SYNTHETIC_GENERATORS:
            raise ValueError(f"unknown generator {gen!r}; have {sorted(SYNTHETIC_GENERATORS)}")
        kwargs = {"seed": int(parts[2])} if len(parts) > 2 else {}
        return SYNTHETIC_GENERATORS[gen](**kwargs)

    symbol, _, interval = spec.partition(":")
    return load_from_cache(symbol, interval or "1")
