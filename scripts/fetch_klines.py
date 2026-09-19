#!/usr/bin/env python3
"""
Fetch Bybit 1-minute klines into the local research cache.

WHY THIS EXISTS SEPARATELY FROM THE BACKTESTER
----------------------------------------------
`backtest.py` re-downloaded klines from api.bybit.com on every single run. Three
consequences, all of them corrosive to research:

  1. The window moved with wall-clock time, so the same command produced a
     different answer tomorrow. Nothing was reproducible.
  2. Every parameter sweep paid the network cost and the rate limit.
  3. It could not run at all in an environment without exchange egress.

Separating fetch from compute fixes all three: fetch once, hash the bytes, then
run any number of experiments against exactly those bytes.

USAGE
-----
    python scripts/fetch_klines.py --symbol BTCUSDT --days 30
    python scripts/fetch_klines.py --symbol BTCUSDT ETHUSDT SOLUSDT --days 60

Requires outbound access to api.bybit.com. This is a PUBLIC market-data
endpoint: no API key is used, sent, or required. Nothing here touches an
account.

Output lands in data/klines/<SYMBOL>_<INTERVAL>.json (override with
KLINE_CACHE_DIR).
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time
from typing import Dict, List

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from research.dataset import save_to_cache  # noqa: E402

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"
MAX_PER_PAGE = 1000


def fetch(symbol: str, interval: str, days: int, verbose: bool = True) -> List[Dict[str, float]]:
    import requests                       # imported late: not needed to run experiments

    per_day = {"1": 1440, "3": 480, "5": 288, "15": 96, "60": 24}.get(interval, 1440)
    target = days * per_day
    end = int(time.time() * 1000)
    out: List[Dict[str, float]] = []
    seen = set()

    while len(out) < target:
        try:
            resp = requests.get(
                BYBIT_KLINE_URL,
                params={"category": "linear", "symbol": symbol,
                        "interval": interval, "limit": MAX_PER_PAGE, "end": end},
                timeout=20,
            )
        except Exception as exc:                        # noqa: BLE001
            raise SystemExit(
                f"\nCould not reach api.bybit.com: {exc}\n"
                f"Run this script from a machine or network with exchange egress, "
                f"then copy data/klines/ across."
            ) from exc

        payload = resp.json()
        if payload.get("retCode") != 0:
            raise SystemExit(f"Bybit error {payload.get('retCode')}: {payload.get('retMsg')}")

        batch = payload.get("result", {}).get("list", [])
        if not batch:
            break

        for k in batch:
            ts = int(k[0])
            if ts in seen:
                continue
            seen.add(ts)
            out.append({"ts": ts, "open": float(k[1]), "high": float(k[2]),
                        "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])})

        oldest = min(int(k[0]) for k in batch)
        if oldest >= end:
            break                                        # no progress; stop rather than spin
        end = oldest - 1
        if verbose:
            print(f"  {symbol}: {len(out)}/{target} bars", end="\r", flush=True)
        time.sleep(0.15)                                 # courtesy rate limit

    out.sort(key=lambda c: c["ts"])
    return out[-target:]


def check_integrity(candles: List[Dict[str, float]], interval: str) -> Dict[str, object]:
    """
    Report gaps and anomalies rather than silently accepting them.

    A backtest run over data with a six-hour hole in it will happily produce a
    Sharpe ratio, and that Sharpe will be wrong in a way nobody can see.
    """
    step_ms = int(interval) * 60_000 if interval.isdigit() else 60_000
    gaps, dupes, bad = [], 0, 0
    for a, b in zip(candles, candles[1:]):
        d = b["ts"] - a["ts"]
        if d == 0:
            dupes += 1
        elif d > step_ms:
            gaps.append({"after_ts": a["ts"], "missing_bars": d // step_ms - 1})
    for c in candles:
        if not (c["low"] <= c["open"] <= c["high"] and c["low"] <= c["close"] <= c["high"]):
            bad += 1
        elif c["low"] <= 0 or c["high"] <= 0:
            bad += 1
    return {
        "bars": len(candles),
        "gaps": len(gaps),
        "missing_bars": sum(int(g["missing_bars"]) for g in gaps),
        "duplicate_timestamps": dupes,
        "invalid_ohlc": bad,
        "largest_gaps": sorted(gaps, key=lambda g: -g["missing_bars"])[:5],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--symbol", nargs="+", required=True)
    ap.add_argument("--interval", default="1")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--cache-dir", default=None)
    args = ap.parse_args()

    cache_dir = pathlib.Path(args.cache_dir) if args.cache_dir else None
    rc = 0
    for symbol in args.symbol:
        symbol = symbol.upper()
        print(f"\nFetching {symbol} interval={args.interval} days={args.days}")
        candles = fetch(symbol, args.interval, args.days)
        if not candles:
            print(f"  no data returned for {symbol}")
            rc = 1
            continue

        report = check_integrity(candles, args.interval)
        path = save_to_cache(symbol, args.interval, candles, cache_dir)
        (path.parent / f"{symbol}_{args.interval}.integrity.json").write_text(
            json.dumps(report, indent=2))

        print(f"  saved {report['bars']} bars -> {path}")
        print(f"  gaps={report['gaps']} missing_bars={report['missing_bars']} "
              f"dupes={report['duplicate_timestamps']} invalid_ohlc={report['invalid_ohlc']}")
        if report["missing_bars"]:
            print("  NOTE: gaps present. Any result on this data inherits them; "
                  "the integrity report is saved alongside.")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
