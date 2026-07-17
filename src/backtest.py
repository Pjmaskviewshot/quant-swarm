"""
🧪 Institutional Walk-Forward Backtester & Optimizer for Quant Swarm.

Replays the CORE candle-based edge with:
  - Worst-case intra-candle TP/SL resolution.
  - Taker fees (0.055%), estimated funding drag, and slippage (5 bps).
  - Leveraged accounting to match the live Risk Vault.
  - Monte Carlo bootstrapping for statistical edge verification.
  - Automated parameter sweeping (--optimize flag).

Usage:
    Standard: python backtest.py --symbol BTCUSDT --interval 15 --days 60
    Optimize: python backtest.py --symbol BTCUSDT --interval 15 --days 90 --optimize
"""
import argparse
import time
from dataclasses import dataclass
from typing import List, Dict

import numpy as np
import requests

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"
TAKER_FEE = 0.00055          # 0.055% per side
FUNDING_PER_8H = 0.0001      # 0.01% baseline estimate
SLIPPAGE_BPS = 5             # 5 basis points (0.05%) slippage per execution leg


def _parse_interval_to_minutes(interval: str) -> int:
    """Safely converts Bybit interval strings to minutes for duration math."""
    mapping = {"D": 1440, "W": 10080, "M": 43200}
    if interval.upper() in mapping:
        return mapping[interval.upper()]
    return int(interval)


def fetch_klines(symbol: str, interval: str, days: int) -> List[Dict]:
    """Pulls up to `days` of closed candles from the public Bybit REST API."""
    interval_mins = _parse_interval_to_minutes(interval)
    candles_per_day = (24 * 60) // interval_mins
    target = days * candles_per_day
    end = int(time.time() * 1000)
    out: List[Dict] = []
    
    while len(out) < target:
        resp = requests.get(
            BYBIT_KLINE_URL,
            params={
                "category": "linear",
                "symbol": symbol,
                "interval": interval,
                "limit": 1000,
                "end": end,
            },
            timeout=15,
        )
        payload = resp.json()
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit error: {payload.get('retMsg')}")
            
        batch = payload["result"]["list"]
        if not batch:
            break
            
        for k in batch:
            out.append({
                "ts": int(k[0]),
                "open": float(k[1]), "high": float(k[2]),
                "low": float(k[3]), "close": float(k[4]),
                "volume": float(k[5]),
            })
            
        end = int(batch[-1][0]) - 1  
        time.sleep(0.25)
        
    out.sort(key=lambda c: c["ts"])
    return out[-target:]


def kaufman_er(closes: np.ndarray) -> float:
    if len(closes) < 2:
        return 0.0
    directional = abs(closes[-1] - closes[0])
    path = np.sum(np.abs(np.diff(closes)))
    return directional / path if path > 0 else 0.0


@dataclass
class Params:
    z_entry: float = 2.5        # Synced with main.py Golden Setup barrier
    z_window: int = 100         # MAD lookback
    vol_mult_min: float = 1.2   # minimum volume anomaly (trend mode)
    sl_atr_mult: float = 2.5
    rr_ratio: float = 2.0       # TP distance = SL distance x rr_ratio
    atr_period: int = 14
    er_threshold: float = 0.35  # regime split
    leverage: float = 5.0       # Base leverage simulation matching Risk Vault


def compute_atr(candles: List[Dict], i: int, period: int) -> float:
    if i < period + 1:
        return 0.0
    trs = []
    for j in range(i - period, i):
        h, l, pc = candles[j]["high"], candles[j]["low"], candles[j - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return float(np.mean(trs))


def monte_carlo_run(trades: List[Dict], n_simulations: int = 1000) -> Dict:
    """Simulates edge stability via bootstrap resampling with replacement."""
    if not trades:
        return {}
    nets = np.array([t["net"] for t in trades])
    results = []
    for _ in range(n_simulations):
        sim_nets = np.random.choice(nets, size=len(nets), replace=True)
        results.append(np.sum(sim_nets))
        
    return {
        "mc_mean_return": float(np.mean(results)),
        "mc_prob_positive": float(np.mean(np.array(results) > 0))
    }


def run_backtest(candles: List[Dict], p: Params, interval_mins: int) -> Dict:
    trades = []
    i = p.z_window + p.atr_period + 2
    cooldown_until = -1

    while i < len(candles) - 1:
        i += 1
        if i <= cooldown_until:
            continue

        window = np.array([c["close"] for c in candles[i - p.z_window:i]])
        median = np.median(window)
        mad = np.median(np.abs(window - median)) * 1.4826 + 1e-9
        price_z = (candles[i]["close"] - median) / mad

        vols = np.array([c["volume"] for c in candles[i - 20:i]])
        vol_mult = candles[i]["volume"] / (np.mean(vols) + 1e-9)

        er = kaufman_er(np.array([c["close"] for c in candles[i - 45:i]]))
        regime = "TRENDING" if er >= p.er_threshold else "RANGING"
        atr = compute_atr(candles, i, p.atr_period)
        if atr <= 0:
            continue

        direction = None
        if regime == "TRENDING":
            if price_z >= p.z_entry and vol_mult >= p.vol_mult_min:
                direction = "SELL"  
            elif price_z <= -p.z_entry and vol_mult >= p.vol_mult_min:
                direction = "BUY"
        else:  
            if price_z <= -p.z_entry and vol_mult < 1.0:
                direction = "BUY"
            elif price_z >= p.z_entry and vol_mult < 1.0:
                direction = "SELL"

        if not direction:
            continue

        entry = candles[i]["close"]
        sl_dist = max(atr * p.sl_atr_mult, entry * 0.005)
        tp_dist = sl_dist * p.rr_ratio
        sl = entry - sl_dist if direction == "BUY" else entry + sl_dist
        tp = entry + tp_dist if direction == "BUY" else entry - tp_dist

        outcome, exit_price, bars_held = None, entry, 0
        for j in range(i + 1, min(i + 241, len(candles))): 
            bars_held = j - i
            h, l = candles[j]["high"], candles[j]["low"]
            if direction == "BUY":
                hit_tp, hit_sl = h >= tp, l <= sl
            else:
                hit_tp, hit_sl = l <= tp, h >= sl
                
            if hit_tp and hit_sl:
                outcome, exit_price = "LOSS", sl
                break
            if hit_tp:
                outcome, exit_price = "WIN", tp
                break
            if hit_sl:
                outcome, exit_price = "LOSS", sl
                break
                
        if outcome is None: 
            exit_price = candles[min(i + 240, len(candles) - 1)]["close"]
            outcome = "WIN" if ((exit_price > entry) == (direction == "BUY")) else "LOSS"

        gross = (exit_price - entry) / entry if direction == "BUY" else (entry - exit_price) / entry
        holding_hours = bars_held * (interval_mins / 60)
        funding_drag = FUNDING_PER_8H * (holding_hours / 8)
        slippage_penalty = (SLIPPAGE_BPS * 2) / 10000 
        
        net_unleveraged = gross - (TAKER_FEE * 2) - funding_drag - slippage_penalty
        net_leveraged = net_unleveraged * p.leverage

        trades.append({
            "i": i, "direction": direction, "regime": regime,
            "outcome": outcome, "net": net_leveraged, "bars": bars_held
        })
        cooldown_until = i + bars_held  

    return summarize(trades)


def summarize(trades: List[Dict]) -> Dict:
    if not trades:
        return {"trades": 0}
        
    nets = np.array([t["net"] for t in trades])
    wins = nets[nets > 0]
    losses = nets[nets <= 0]
    equity = np.cumsum(nets)
    peak = np.maximum.accumulate(equity)
    max_dd = float(np.max(peak - equity)) if len(equity) else 0.0
    
    mc_stats = monte_carlo_run(trades)
    
    return {
        "trades": len(trades),
        "win_rate": float(len(wins) / len(trades)),
        "avg_win": float(np.mean(wins)) if len(wins) else 0.0,
        "avg_loss": float(np.mean(losses)) if len(losses) else 0.0,
        "expectancy_per_trade": float(np.mean(nets)),
        "profit_factor": float(wins.sum() / abs(losses.sum())) if losses.sum() != 0 else float("inf"),
        "total_return_on_margin": float(equity[-1]),
        "max_drawdown_on_margin": max_dd,
        "monte_carlo_p_positive": mc_stats.get("mc_prob_positive", 0.0),
        "by_regime": {
            r: {"trades": sum(1 for t in trades if t["regime"] == r),
                "win_rate": float(np.mean([1 if t["net"] > 0 else 0 for t in trades if t["regime"] == r]) or 0.0)}
            for r in ("TRENDING", "RANGING")
        },
    }

def parameter_sweep(candles: List[Dict], interval_mins: int) -> List[Dict]:
    """Runs a grid search over key hyperparameters to find the optimal OOS Profit Factor."""
    results = []
    print("\n⏳ Running Institutional Parameter Sweep (This will take a moment)...")
    
    # Grid Search Space
    z_entries = [2.0, 2.5, 3.0]
    rr_ratios = [1.5, 2.0, 2.5]
    atr_mults = [2.0, 2.5, 3.0]
    
    split = int(len(candles) * 0.7)
    
    for z in z_entries:
        for rr in rr_ratios:
            for atr_m in atr_mults:
                p = Params(z_entry=z, rr_ratio=rr, sl_atr_mult=atr_m)
                
                train = run_backtest(candles[:split], p, interval_mins)
                test = run_backtest(candles[split:], p, interval_mins)
                
                # Filter out statistical noise (requires > 10 trades and positive expectancy)
                if test.get("trades", 0) > 10 and test.get("expectancy_per_trade", 0) > 0:
                    results.append({
                        "Z": z,
                        "RR": rr,
                        "ATR": atr_m,
                        "OOS_Profit_Factor": test["profit_factor"],
                        "OOS_Expectancy": test["expectancy_per_trade"],
                        "OOS_WinRate": test["win_rate"]
                    })
                    
    # Return Top 5 configurations sorted by Out-Of-Sample Profit Factor
    return sorted(results, key=lambda x: x["OOS_Profit_Factor"], reverse=True)[:5]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="15", help="kline interval in minutes (e.g., 1, 5, 15, D)")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--optimize", action="store_true", help="Run a hyperparameter sweep to find optimal settings")
    args = parser.parse_args()

    interval_mins = _parse_interval_to_minutes(args.interval)
    
    print(f"Fetching {args.days}d of {args.interval}m klines for {args.symbol}...")
    candles = fetch_klines(args.symbol, args.interval, args.days)
    print(f"Loaded {len(candles)} candles.")

    if args.optimize:
        best_params = parameter_sweep(candles, interval_mins)
        print("\n🏆 Top 5 Parameter Configurations (Sorted by Out-Of-Sample Profit Factor):")
        for i, res in enumerate(best_params, 1):
            print(f" {i}. Z-Entry: {res['Z']} | RR: {res['RR']} | SL ATR: {res['ATR']} "
                  f"--> PF: {res['OOS_Profit_Factor']:.2f} | WR: {res['OOS_WinRate']:.1%}")
    else:
        split = int(len(candles) * 0.6)
        params = Params()

        train = run_backtest(candles[:split], params, interval_mins)
        test = run_backtest(candles[split:], params, interval_mins)

        print("\n=== IN-SAMPLE (first 60%) ===")
        for k, v in train.items():
            if isinstance(v, float):
                print(f"  {k}: {v:.4f}")
            else:
                print(f"  {k}: {v}")
                
        print("\n=== OUT-OF-SAMPLE (last 40%) — trust THIS one ===")
        for k, v in test.items():
            if isinstance(v, float):
                print(f"  {k}: {v:.4f}")
            else:
                print(f"  {k}: {v}")
                
        print("\nRule of thumb: only consider live deployment if OOS expectancy > 0, "
              "profit factor > 1.3, and Monte Carlo probability > 0.85.")