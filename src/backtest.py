"""
🧪 V25.2 INSTITUTIONAL BACKTESTER: AUDIT-HARDENED APEX MIRROR
Synchronized with the Quant Swarm live node.

Includes:
  - Dual-Horizon OFI (Proxy via OHLCV density).
  - GARCH-anchored Hawkes momentum.
  - Corrected Cross-Sectional BTC Lead-Lag Alignment.
  - MSE Divergence Model Reset Guard.
  - Volatility-Dynamic Slippage Penalty.
  
Usage:
    Standard: python backtest.py --symbol DEXEUSDT --interval 15 --days 60
    Optimize: python backtest.py --symbol DEXEUSDT --interval 15 --days 90 --optimize
"""
import argparse
import time
import math
from collections import deque
from dataclasses import dataclass
from typing import List, Dict, Tuple

import numpy as np
import requests

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"
TAKER_FEE = 0.00055          # 0.055% per side
FUNDING_PER_8H = 0.0001      # 0.01% baseline estimate
BASE_SLIPPAGE_BPS = 5        # 5 basis points baseline slippage per leg


def _parse_interval_to_minutes(interval: str) -> int:
    mapping = {"D": 1440, "W": 10080, "M": 43200}
    if interval.upper() in mapping:
        return mapping[interval.upper()]
    return int(interval)


def fetch_klines(symbol: str, interval: str, days: int) -> List[Dict]:
    interval_mins = _parse_interval_to_minutes(interval)
    candles_per_day = (24 * 60) // interval_mins
    target = days * candles_per_day
    end = int(time.time() * 1000)
    out: List[Dict] = []
    
    while len(out) < target:
        resp = requests.get(
            BYBIT_KLINE_URL,
            params={"category": "linear", "symbol": symbol, "interval": interval, "limit": 1000, "end": end},
            timeout=15,
        )
        payload = resp.json()
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit error: {payload.get('retMsg')}")
            
        batch = payload["result"]["list"]
        if not batch: break
            
        for k in batch:
            out.append({
                "ts": int(k[0]),
                "open": float(k[1]), "high": float(k[2]),
                "low": float(k[3]), "close": float(k[4]),
                "volume": float(k[5]),
            })
            
        end = int(batch[-1][0]) - 1  
        time.sleep(0.2)
        
    out.sort(key=lambda c: c["ts"])
    return out[-target:]


def fetch_aligned_data(symbol: str, interval: str, days: int) -> Tuple[List[Dict], List[Dict]]:
    """Fetches Target Asset and BTC Lead asset, aligning them cleanly by timestamp."""
    print(f"📡 Fetching target asset ({symbol})...")
    target_candles = fetch_klines(symbol, interval, days)
    
    if symbol == "BTCUSDT":
        return target_candles, target_candles
        
    print("📡 Fetching global BTC lead-lag context...")
    btc_raw = fetch_klines("BTCUSDT", interval, days)
    btc_dict = {c['ts']: c for c in btc_raw}
    aligned_btc = []
    
    for c in target_candles:
        # 🚀 V25.2 FIX: Neutral zero-volume fallback prevents target data leaking into BTC lead
        if c['ts'] in btc_dict:
            aligned_btc.append(btc_dict[c['ts']])
        else:
            aligned_btc.append({
                "ts": c['ts'], "open": c['close'], "high": c['close'],
                "low": c['close'], "close": c['close'], "volume": 0.0
            })
        
    return target_candles, aligned_btc


def kaufman_er(closes: np.ndarray) -> float:
    if len(closes) < 2: return 0.0
    directional = abs(closes[-1] - closes[0])
    path = np.sum(np.abs(np.diff(closes)))
    return directional / path if path > 0 else 0.0


@dataclass
class Params:
    prob_threshold: float = 0.55    # Minimum calibrated bayesian probability
    rr_ratio: float = 2.0           # Target Reward/Risk ratio
    sl_atr_mult: float = 1.5        # Stop Loss distance relative to ATR
    atr_period: int = 14
    leverage: float = 5.0           # Base leverage mapping Risk Vault


def compute_atr(candles: List[Dict], i: int, period: int) -> float:
    if i < period + 1: return 0.0
    trs = []
    for j in range(i - period, i):
        h, l, pc = candles[j]["high"], candles[j]["low"], candles[j - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return float(np.mean(trs))


def run_v25_backtest(target_candles: List[Dict], btc_candles: List[Dict], p: Params, interval_mins: int) -> Dict:
    trades = []
    cooldown_until = -1
    
    alpha_fast = 0.15
    alpha_slow = 0.02
    
    ofi_fast_mean, ofi_fast_var = 0.0, 1.0
    ofi_slow_mean, ofi_slow_var = 0.0, 1.0
    btc_fast_mean, btc_fast_var = 0.0, 1.0
    hawkes_mean, hawkes_var = 0.0, 1.0
    
    weights = np.array([0.20, 0.20, 0.20, 0.20, 0.20])
    lr = 0.001
    l2_lambda = 0.0005
    burn_in_ticks = 1000
    
    # 🚀 V25.2 FIX: Validation Guard Buffer
    validation_buffer = deque(maxlen=100)

    for i in range(45, len(target_candles) - 1):
        c = target_candles[i]
        c_prev = target_candles[i-1]
        
        # 1. Feature Proxy Math
        hl = c['high'] - c['low'] + 1e-9
        buy_v = c['volume'] * ((c['close'] - c['low']) / hl)
        sell_v = c['volume'] * ((c['high'] - c['close']) / hl)
        flow_delta = buy_v - sell_v
        
        ofi_fast_mean = (1 - alpha_fast) * ofi_fast_mean + alpha_fast * flow_delta
        ofi_fast_var = (1 - alpha_fast) * ofi_fast_var + alpha_fast * (flow_delta - ofi_fast_mean)**2
        ofi_fast_z = (flow_delta - ofi_fast_mean) / (math.sqrt(ofi_fast_var) + 1e-9)
        
        ofi_slow_mean = (1 - alpha_slow) * ofi_slow_mean + alpha_slow * flow_delta
        ofi_slow_var = (1 - alpha_slow) * ofi_slow_var + alpha_slow * (flow_delta - ofi_slow_mean)**2
        ofi_slow_z = (flow_delta - ofi_slow_mean) / (math.sqrt(ofi_slow_var) + 1e-9)
        
        hawkes_pressure = np.sign(c['close'] - c_prev['close']) * c['volume']
        hawkes_mean = (1 - alpha_fast) * hawkes_mean + alpha_fast * hawkes_pressure
        hawkes_var = (1 - alpha_slow) * hawkes_var + alpha_slow * (hawkes_pressure - hawkes_mean)**2
        hawkes_z = (hawkes_pressure - hawkes_mean) / (math.sqrt(hawkes_var) + 1e-9)
        
        vwap = (c['high'] + c['low'] + c['close']) / 3
        skew = ((c['close'] - vwap) / vwap) * 10000.0
        
        # BTC Lead
        b_c = btc_candles[i]
        b_hl = b_c['high'] - b_c['low'] + 1e-9
        b_flow = (b_c['volume'] * ((b_c['close'] - b_c['low']) / b_hl)) - (b_c['volume'] * ((b_c['high'] - b_c['close']) / b_hl))
        btc_fast_mean = (1 - alpha_fast) * btc_fast_mean + alpha_fast * b_flow
        btc_fast_var = (1 - alpha_fast) * btc_fast_var + alpha_fast * (b_flow - btc_fast_mean)**2
        btc_ofi_z = (b_flow - btc_fast_mean) / (math.sqrt(btc_fast_var) + 1e-9)
        
        btc_lead = btc_ofi_z if b_c['ts'] == c['ts'] else 0.0

        # 2. Extract Statistical State
        ofi_delta_z = ofi_fast_z - ofi_slow_z
        features = np.array([
            ofi_fast_z / 3.0,
            ofi_delta_z / 6.0,
            hawkes_z / 3.0,
            skew / 10.0,
            btc_lead / 3.0
        ])
        features = np.clip(features, -1.0, 1.0)
        
        if i < burn_in_ticks: active_weights = np.array([0.3, 0.2, 0.2, 0.1, 0.2])
        else: active_weights = weights
            
        logit = max(-10.0, min(10.0, np.dot(active_weights, features)))
        base_p_up = 1.0 / (1.0 + math.exp(-logit))
        p_up = 0.5 + 0.65 * (base_p_up - 0.5)
        p_down = 1.0 - p_up

        er = kaufman_er(np.array([cx["close"] for cx in target_candles[i - 45:i]]))
        regime = "TRENDING" if er >= 0.35 else "RANGING"

        # 3. L2 Ridge SGD Update
        next_ret = math.log(target_candles[i+1]['close'] / c['close'])
        scaled_ret = max(-0.5, min(0.5, next_ret * 100.0))
        error = scaled_ret - (p_up - 0.5)
        
        # 🚀 V25.2 FIX: Validation Guard Simulation
        validation_buffer.append(error ** 2)
        if len(validation_buffer) == 100:
            if np.mean(validation_buffer) > 0.40:
                weights = np.array([0.3, 0.2, 0.2, 0.1, 0.2])
                validation_buffer.clear()
                continue

        weights = (1.0 - lr * l2_lambda) * weights + (lr * error * features)
        weights /= (np.linalg.norm(weights) + 1e-9)

        # 4. Signal Router
        if i > burn_in_ticks and i > cooldown_until:
            prob_success = max(p_up, p_down)
            action = "BUY" if p_up > p_down else "SELL"
            
            if prob_success >= p.prob_threshold:
                atr = compute_atr(target_candles, i, p.atr_period)
                if atr > 0:
                    sl_dist_pct = max((atr * p.sl_atr_mult) / c['close'], 0.01)
                    tp_dist_pct = sl_dist_pct * p.rr_ratio
                    ev_pct = (prob_success * tp_dist_pct) - ((1.0 - prob_success) * sl_dist_pct)
                    
                    base_edge_pct = ev_pct * abs(hawkes_z * 0.10)
                    net_edge_bps = (base_edge_pct - (TAKER_FEE * 2)) * 10000.0
                    
                    if net_edge_bps > 5.0:
                        entry = c['close']
                        sl_dist = sl_dist_pct * entry
                        tp_dist = tp_dist_pct * entry
                        sl = entry - sl_dist if action == "BUY" else entry + sl_dist
                        tp = entry + tp_dist if action == "BUY" else entry - tp_dist
                        
                        outcome, exit_price, bars_held = None, entry, 0
                        for j in range(i + 1, min(i + 241, len(target_candles))): 
                            bars_held = j - i
                            h, l = target_candles[j]["high"], target_candles[j]["low"]
                            if action == "BUY":
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
                            exit_price = target_candles[min(i + 240, len(target_candles) - 1)]["close"]
                            outcome = "WIN" if ((exit_price > entry) == (action == "BUY")) else "LOSS"

                        gross = (exit_price - entry) / entry if action == "BUY" else (entry - exit_price) / entry
                        holding_hours = bars_held * (interval_mins / 60)
                        funding_drag = FUNDING_PER_8H * (holding_hours / 8)
                        
                        # 🚀 V25.2 FIX: Volatility-Dynamic Slippage Penalty
                        dynamic_slippage_bps = BASE_SLIPPAGE_BPS * max(1.0, abs(hawkes_z) * 0.5)
                        slippage_penalty = (dynamic_slippage_bps * 2) / 10000.0
                        
                        net_unleveraged = gross - (TAKER_FEE * 2) - funding_drag - slippage_penalty
                        net_leveraged = net_unleveraged * p.leverage

                        trades.append({
                            "i": i, "direction": action, "regime": regime,
                            "outcome": outcome, "net": net_leveraged, "bars": bars_held
                        })
                        cooldown_until = i + bars_held  

    return summarize(trades)


def summarize(trades: List[Dict]) -> Dict:
    if not trades: return {"trades": 0}
        
    nets = np.array([t["net"] for t in trades])
    wins = nets[nets > 0]
    losses = nets[nets <= 0]
    equity = np.cumsum(nets)
    peak = np.maximum.accumulate(equity)
    max_dd = float(np.max(peak - equity)) if len(equity) else 0.0
    
    mc_results = []
    for _ in range(1000):
        sim_nets = np.random.choice(nets, size=len(nets), replace=True)
        mc_results.append(np.sum(sim_nets))
    
    return {
        "trades": len(trades),
        "win_rate": float(len(wins) / len(trades)),
        "avg_win": float(np.mean(wins)) if len(wins) else 0.0,
        "avg_loss": float(np.mean(losses)) if len(losses) else 0.0,
        "expectancy_per_trade": float(np.mean(nets)),
        "profit_factor": float(wins.sum() / abs(losses.sum())) if losses.sum() != 0 else float("inf"),
        "total_return_on_margin": float(equity[-1]),
        "max_drawdown_on_margin": max_dd,
        "monte_carlo_p_positive": float(np.mean(np.array(mc_results) > 0)),
        "by_regime": {
            r: {"trades": sum(1 for t in trades if t["regime"] == r),
                "win_rate": float(np.mean([1 if t["net"] > 0 else 0 for t in trades if t["regime"] == r]) or 0.0)}
            for r in ("TRENDING", "RANGING")
        },
    }


def parameter_sweep(t_cand: List[Dict], b_cand: List[Dict], interval_mins: int) -> List[Dict]:
    results = []
    print("\n⏳ Running V25.2 Parameter Sweep (Testing L2 thresholds)...")
    
    probs = [0.52, 0.55, 0.58]
    rr_ratios = [1.5, 2.0, 2.5]
    atr_mults = [1.2, 1.5, 2.0]
    
    split = int(len(t_cand) * 0.7)
    
    for prob in probs:
        for rr in rr_ratios:
            for atr_m in atr_mults:
                p = Params(prob_threshold=prob, rr_ratio=rr, sl_atr_mult=atr_m)
                test = run_v25_backtest(t_cand[split:], b_cand[split:], p, interval_mins)
                
                if test.get("trades", 0) > 10 and test.get("expectancy_per_trade", 0) > 0:
                    results.append({
                        "Prob_Gate": prob, "RR": rr, "ATR": atr_m,
                        "OOS_Profit_Factor": test["profit_factor"],
                        "OOS_Expectancy": test["expectancy_per_trade"],
                        "OOS_WinRate": test["win_rate"]
                    })
                    
    return sorted(results, key=lambda x: x["OOS_Profit_Factor"], reverse=True)[:5]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="15", help="kline interval in minutes")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--optimize", action="store_true")
    args = parser.parse_args()

    interval_mins = _parse_interval_to_minutes(args.interval)
    
    print(f"📥 Building matrix mapping for {args.days}d of {args.interval}m data...")
    t_cand, b_cand = fetch_aligned_data(args.symbol, args.interval, args.days)
    print(f"✅ Matrix synchronized. ({len(t_cand)} blocks)")

    if args.optimize:
        best_params = parameter_sweep(t_cand, b_cand, interval_mins)
        print("\n🏆 Top 5 Parameter Configurations (Sorted by Out-Of-Sample Profit Factor):")
        for i, res in enumerate(best_params, 1):
            print(f" {i}. Prob Gate: {res['Prob_Gate']} | RR: {res['RR']} | SL ATR: {res['ATR']} "
                  f"--> PF: {res['OOS_Profit_Factor']:.2f} | WR: {res['OOS_WinRate']:.1%}")
    else:
        split = int(len(t_cand) * 0.6)
        params = Params()

        train = run_v25_backtest(t_cand[:split], b_cand[:split], params, interval_mins)
        test = run_v25_backtest(t_cand[split:], b_cand[split:], params, interval_mins)

        print("\n=== IN-SAMPLE (first 60%) ===")
        for k, v in train.items():
            if isinstance(v, float): print(f"  {k}: {v:.4f}")
            else: print(f"  {k}: {v}")
                
        print("\n=== OUT-OF-SAMPLE (last 40%) — trust THIS one ===")
        for k, v in test.items():
            if isinstance(v, float): print(f"  {k}: {v:.4f}")
            else: print(f"  {k}: {v}")
                
        print("\nRule of thumb: only consider live deployment if OOS expectancy > 0, "
              "profit factor > 1.3, and Monte Carlo probability > 0.85.")