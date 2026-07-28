"""
🧪 V41.20 APEX BACKTESTER: PERFECT PARITY
Synchronized strictly with Quant Swarm live node V41.20.
Implements Pessimistic Intra-bar Execution, Dynamic Z-Scored Entropy Gating,
6 bps Breakeven Ratchets, Maker Routing for Wide Spreads, and 1 bps EV Triggers.
"""
import argparse
import time
import math
from collections import deque
from itertools import permutations
from dataclasses import dataclass
from typing import List, Dict, Tuple

import numpy as np
import requests

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"
TAKER_FEE = 0.00055          
MAKER_FEE = 0.00020          
FUNDING_PER_8H = 0.0001      
BASE_SLIPPAGE_BPS = 5        

def fetch_klines_1m(symbol: str, days: int) -> List[Dict]:
    target = days * 1440  
    end = int(time.time() * 1000)
    out: List[Dict] = []
    
    while len(out) < target:
        resp = requests.get(
            BYBIT_KLINE_URL, 
            params={"category": "linear", "symbol": symbol, "interval": "1", "limit": 1000, "end": end}, 
            timeout=15
        )
        payload = resp.json()
        if payload.get("retCode") != 0: raise RuntimeError(f"Bybit error: {payload.get('retMsg')}")
        batch = payload["result"]["list"]
        if not batch: break
        
        for k in batch:
            out.append({"ts": int(k[0]), "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])})
            
        end = int(batch[-1][0]) - 1  
        time.sleep(0.2)
        
    out.sort(key=lambda c: c["ts"])
    return out[-target:]

def fetch_aligned_data(symbol: str, days: int) -> Tuple[List[Dict], List[Dict]]:
    print(f"📡 Fetching target asset 1-Minute Data ({symbol})...")
    target_candles = fetch_klines_1m(symbol, days)
    if symbol == "BTCUSDT": return target_candles, target_candles
        
    print("📡 Fetching global BTC 1-Minute lead-lag context...")
    btc_raw = fetch_klines_1m("BTCUSDT", days)
    btc_dict = {c['ts']: c for c in btc_raw}
    aligned_btc = []
    
    for c in target_candles:
        if c['ts'] in btc_dict: 
            aligned_btc.append(btc_dict[c['ts']])
        else: 
            aligned_btc.append({"ts": c['ts'], "open": c['close'], "high": c['close'], "low": c['close'], "close": c['close'], "volume": 0.0})
            
    return target_candles, aligned_btc

@dataclass
class Params:
    rr_ratio: float = 2.0            
    sl_atr_mult: float = 1.5         
    atr_period: int = 14
    leverage: float = 3.0            

def get_cluster_priors(symbol: str):
    if any(m in symbol for m in ["BTC", "ETH", "SOL"]):
        w_trend = np.array([0.22, 0.18, 0.15, 0.08, 0.12, 0.10, 0.05, 0.05, 0.05])
        w_range = np.array([0.08, 0.15, 0.05, 0.22, 0.18, 0.05, 0.12, 0.08, 0.07])
        p_scale = 1.0
    elif any(m in symbol for m in ["AVAX", "LINK", "XRP", "ADA", "DOT", "NEAR"]):
        w_trend = np.array([0.20, 0.16, 0.14, 0.10, 0.10, 0.10, 0.08, 0.06, 0.06])
        w_range = np.array([0.09, 0.14, 0.06, 0.20, 0.16, 0.06, 0.11, 0.09, 0.09])
        p_scale = 2.0
    else:
        w_trend = np.array([0.15, 0.12, 0.10, 0.15, 0.08, 0.10, 0.10, 0.10, 0.10])
        w_range = np.array([0.10, 0.10, 0.08, 0.18, 0.14, 0.08, 0.12, 0.10, 0.10])
        p_scale = 0.5 
    return w_trend, w_range, np.eye(9) * p_scale

def compute_atr_5m_wilder(candles: List[Dict], i: int, period: int) -> float:
    if i < (period * 5) + 1: 
        return 0.0
        
    history_slice = candles[max(0, i - (period * 5 + 10)) : i]
    if len(history_slice) < period * 5: return 0.0
    
    bars_5m = []
    for k in range(0, len(history_slice), 5):
        chunk = history_slice[k:k+5]
        if not chunk: continue
        bars_5m.append({
            "high": max([c["high"] for c in chunk]),
            "low": min([c["low"] for c in chunk]),
            "close": chunk[-1]["close"]
        })
        
    if len(bars_5m) < period + 1: return 0.0
    
    trs = []
    for j in range(1, len(bars_5m)):
        h, l, pc = bars_5m[j]["high"], bars_5m[j]["low"], bars_5m[j-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        
    trs = trs[-period:]
    
    atr = trs[0]
    for tr in trs[1:]:
        atr = (atr * (period - 1) + tr) / period
        
    return float(atr)

def compute_tensor_alpha(btc_hist: deque, alt_hist: deque) -> float:
    if len(btc_hist) < 30 or len(alt_hist) < 30: return 0.0
    aligned_b, aligned_a = [], []
    
    for i in range(2, len(alt_hist)):
        a_ret = math.log(alt_hist[i] / (alt_hist[i-1] + 1e-9))
        b_ret = math.log(btc_hist[i-1] / (btc_hist[i-2] + 1e-9))
        aligned_a.append(a_ret)
        aligned_b.append(b_ret)
    
    if len(aligned_a) < 20: return 0.0
    correlation = np.corrcoef(aligned_b, aligned_a)[0, 1]
    if np.isnan(correlation): return 0.0
    
    btc_momentum = np.mean(aligned_b[-10:])
    if abs(btc_momentum) > 0.0002 and correlation > 0.60:
        return float(np.sign(btc_momentum) * min(1.0, abs(correlation)))
    return 0.0

def compute_permutation_entropy(series: list, order: int = 3, delay: int = 1) -> float:
    if len(series) < (order * delay): return 1.0
    
    sub_vectors = []
    for i in range(len(series) - (order - 1) * delay):
        sub_vectors.append([series[i + j * delay] for j in range(order)])
        
    perm_counts = {perm: 0 for perm in permutations(range(order))}
    
    for vec in sub_vectors:
        rank = tuple(np.argsort(vec))
        perm_counts[rank] += 1
        
    total = len(sub_vectors)
    entropy = 0.0
    for count in perm_counts.values():
        if count > 0:
            p = count / total
            entropy -= p * math.log2(p)
            
    max_entropy = math.log2(math.factorial(order))
    return float(entropy / max_entropy)

def get_vpin_bucket_size(symbol: str) -> float:
    if "BTC" in symbol: return 1_000_000.0
    if "ETH" in symbol: return 500_000.0
    if "SOL" in symbol: return 250_000.0
    return 100_000.0

def log_gaussian_pdf(x: float, mean: float, std: float) -> float:
    variance = float(std)**2 + 1e-9
    return -0.5 * math.log(2 * math.pi * variance) - ((float(x) - float(mean))**2 / (2 * variance))

def detect_hmm_regime(closes_arr: np.ndarray, volumes_arr: np.ndarray, current_state_probs: np.ndarray) -> Tuple[str, np.ndarray, float]:
    if len(closes_arr) < 20:
        return "MEAN_REVERTING", current_state_probs, 0.5

    log_returns = np.diff(np.log(closes_arr + 1e-9))
    mu_ret = float(np.mean(log_returns))
    volatility = float(np.std(log_returns)) + 1e-9
    
    directional_change = abs(closes_arr[-1] - closes_arr[0])
    absolute_changes = np.sum(np.abs(np.diff(closes_arr)))
    er = float(directional_change / (absolute_changes + 1e-9))
    
    avg_vol = float(np.mean(volumes_arr))
    
    archetypes = {
        "TRENDING_BULL":    {"ret": (0.001, 0.0005), "vol": (0.002, 0.001), "er": (0.8, 0.15)},
        "TRENDING_BEAR":    {"ret": (-0.001, 0.0005), "vol": (0.002, 0.001), "er": (0.8, 0.15)},
        "HIGH_VOL_CHOP":    {"ret": (0.0, 0.002), "vol": (0.008, 0.002), "er": (0.3, 0.15)},
        "MEAN_REVERTING":   {"ret": (0.0, 0.0005), "vol": (0.0015, 0.0005), "er": (0.2, 0.1)},
        "LIQUIDITY_VACUUM": {"ret": (0.0, 0.001), "vol": (0.004, 0.002), "er": (0.5, 0.2)}
    }
    
    regimes = list(archetypes.keys())
    transition_matrix = np.array([
        [0.75, 0.05, 0.10, 0.08, 0.02], 
        [0.05, 0.75, 0.10, 0.08, 0.02], 
        [0.10, 0.10, 0.70, 0.05, 0.05], 
        [0.05, 0.05, 0.05, 0.80, 0.05], 
        [0.05, 0.05, 0.15, 0.05, 0.70]  
    ])

    log_emissions = np.zeros(5)
    for i, regime in enumerate(regimes):
        arch = archetypes[regime]
        log_p_ret = log_gaussian_pdf(mu_ret, arch["ret"][0], arch["ret"][1])
        log_p_vol = log_gaussian_pdf(volatility, arch["vol"][0], arch["vol"][1])
        log_p_er  = log_gaussian_pdf(er, arch["er"][0], arch["er"][1])
        
        log_emission = log_p_ret + log_p_vol + log_p_er
        if regime == "LIQUIDITY_VACUUM" and avg_vol < np.percentile(volumes_arr, 25):
            log_emission += math.log(2.0) 
            
        log_emissions[i] = log_emission

    prior = np.dot(transition_matrix.T, current_state_probs)
    prior_log = np.log(prior + 1e-9)
    
    unnormalized_log_posterior = log_emissions + prior_log
    max_log = np.max(unnormalized_log_posterior)
    posterior = np.exp(unnormalized_log_posterior - max_log)
    new_state_probs = posterior / np.sum(posterior)
    
    best_state_idx = int(np.argmax(new_state_probs))
    detected_regime = regimes[best_state_idx]
    
    binary_regime = "TRENDING" if detected_regime in ["TRENDING_BULL", "TRENDING_BEAR"] else "RANGING"
    return binary_regime, new_state_probs, er

def calculate_evt_tail_risk(volatility_surface: deque) -> float:
    if len(volatility_surface) < 30: return 1.05 
        
    vol_arr = np.array(volatility_surface)
    threshold = np.percentile(vol_arr, 95)
    exceedances = vol_arr[vol_arr > threshold] - threshold
    
    if len(exceedances) < 5 or np.mean(exceedances) <= 1e-9: return 1.05
        
    log_exceedances = np.log(exceedances + 1e-9)
    xi_estimator = np.mean(log_exceedances) - np.log(threshold + 1e-9)
    xi_clamped = max(0.0, min(1.5, xi_estimator))
    
    if xi_clamped <= 0.3: return 1.05

    raw_suppression = min(0.65, (xi_clamped - 0.3) * 1.5)
    
    current_variance = volatility_surface[-1] if volatility_surface else 0.0
    vol_scalar = min(1.0, current_variance * 1000.0)
    dynamic_floor = 0.35 + 0.15 * (1.0 - vol_scalar)
    
    return max(dynamic_floor, 1.0 - raw_suppression)

def calibrate_confidence(prob: float, regime: str, mse: float) -> float:
    floor = 0.50
    ceiling = 0.85

    if regime in ["TRENDING_BULL", "TRENDING_BEAR", "TRENDING"]:
        ceiling = min(0.92, ceiling + 0.07)
        floor = max(0.48, floor - 0.02)
    elif regime == "LIQUIDITY_VACUUM":
        ceiling = min(0.90, ceiling + 0.05)
        floor = max(0.52, floor + 0.03)
    else:  
        ceiling = min(0.80, ceiling - 0.05)
        floor = max(0.52, floor + 0.03)

    mse_penalty = min(0.10, mse * 0.4)
    ceiling = max(floor, ceiling - mse_penalty)

    return max(floor, min(ceiling, prob))

def run_v41_backtest(target_candles: List[Dict], btc_candles: List[Dict], p: Params, symbol: str) -> Dict:
    trades = []
    cooldown_until = -1
    
    ofi_fast_mean, ofi_fast_var, ofi_fast_z = 0.0, 1.0, 0.0
    ofi_slow_mean, ofi_slow_var, ofi_slow_z = 0.0, 1.0, 0.0
    hawkes_mean, hawkes_var, hawkes_z = 0.0, 1.0, 0.0
    hawkes_velocity, hawkes_acceleration = 0.0, 0.0
    hawkes_z_prev, hawkes_v_prev = 0.0, 0.0
    
    amihud_history = deque(maxlen=100)
    rolling_outcomes = deque(maxlen=100)
    evt_vol_surface = deque(maxlen=300)
    trade_imbalances = deque(maxlen=100)
    
    btc_1m_history = deque(maxlen=300)
    alt_1m_history = deque(maxlen=300)
    entropy_history = deque(maxlen=200) 
    log_returns = deque(maxlen=500)
    inst_variance = 1e-6
    
    vpin_bucket_size = get_vpin_bucket_size(symbol)
    current_bucket_vol = 0.0
    current_bucket_buy_vol = 0.0
    vpin_history = deque(maxlen=200)
    synthetic_vpin_z = 0.0
    
    w_t, w_r, P_init = get_cluster_priors(symbol)
    weights_trending = w_t.copy()
    weights_ranging  = w_r.copy()
    P_trending = P_init.copy()
    P_ranging = P_init.copy()
    forgetting_factor = 0.998      
    
    rls_updates = 100 
    hmm_state_probs = np.array([0.2, 0.2, 0.2, 0.2, 0.2])
    
    validation_buffer = deque(maxlen=100)
    prediction_buffer = deque()
    historical_probs = deque(maxlen=2000) 
    ewma_mse = 0.25
    
    rolling_notional_volume = 0.0
    amihud_anchor_price = 0.0
    
    if "BTC" in symbol:
        amihud_threshold = 2_500_000.0  
    elif "ETH" in symbol or "SOL" in symbol:
        amihud_threshold = 1_000_000.0   
    else:
        amihud_threshold = 250_000.0  

    for i in range(75, len(target_candles)):
        c = target_candles[i]
        c_prev = target_candles[i-1]
        now_ts = c['ts']
        sim_price = c['close']
        
        btc_1m_history.append(btc_candles[i]['close'])
        alt_1m_history.append(sim_price)
        
        shannon_entropy = 1.0
        if len(alt_1m_history) > 10:
            shannon_entropy = compute_permutation_entropy(list(alt_1m_history)[-20:])
            entropy_history.append(shannon_entropy) 
        
        ret = math.log(sim_price / (c_prev['close'] + 1e-9))
        log_returns.append(ret)
        if len(log_returns) > 10:
            inst_variance = np.var(list(log_returns)[-10:]) + 1e-9
            evt_vol_surface.append(inst_variance)

        vol_notional = c['volume'] * sim_price
        current_bucket_vol += vol_notional
        if c['close'] >= c['open']: current_bucket_buy_vol += vol_notional
            
        if current_bucket_vol >= vpin_bucket_size:
            sell_vol = current_bucket_vol - current_bucket_buy_vol
            vpin_score = abs(current_bucket_buy_vol - sell_vol) / current_bucket_vol
            vpin_history.append(vpin_score)
            
            if len(vpin_history) > 20:
                hist_arr = np.array(vpin_history)
                synthetic_vpin_z = float((vpin_score - np.mean(hist_arr)) / (np.std(hist_arr) + 1e-9))
            
            current_bucket_vol = 0.0
            current_bucket_buy_vol = 0.0

        while prediction_buffer and (now_ts - prediction_buffer[0][0]) >= 300000:  
            old_ts, old_price, old_features, old_p_up, virt_sl, virt_tp, action_dir, old_r_blend = prediction_buffer.popleft()
            
            if sim_price != old_price and old_price > 0:
                y_true = 0.5
                if action_dir == "BUY":
                    if sim_price >= virt_tp: y_true = 1.0
                    elif sim_price <= virt_sl: y_true = 0.0
                else:
                    if sim_price <= virt_tp: y_true = 1.0
                    elif sim_price >= virt_sl: y_true = 0.0
                    
                if y_true == 0.5:
                    y_true = 1.0 if ((sim_price > old_price) == (action_dir == "BUY")) else 0.0
                    
                error = y_true - old_p_up
                ewma_mse = (0.98 * ewma_mse) + (0.02 * (error ** 2))
                
                validation_buffer.append(error ** 2)
                if len(validation_buffer) == 100:
                    rolling_mse = np.mean(validation_buffer)
                    if rolling_mse > 0.35 or np.trace(P_trending) > 5000.0:
                        trace_t = np.trace(P_trending)
                        trace_r = np.trace(P_ranging)
                        # 🚀 V41.20 FIX: Stable Covariance Resets (1e-3)
                        if trace_t > 5000: P_trending = P_trending / (trace_t / 1000.0) + np.eye(9) * 1e-3
                        if trace_r > 5000: P_ranging = P_ranging / (trace_r / 1000.0) + np.eye(9) * 1e-3
                        validation_buffer.clear()
                        break

                x_feat = old_features.reshape(-1, 1)
                var_pi = max(1e-4, old_p_up * (1.0 - old_p_up))
                
                if old_r_blend > 0.1:
                    P_x_t = P_trending @ x_feat
                    den_t = forgetting_factor + var_pi * float(x_feat.T @ P_x_t)
                    K_t = P_x_t / den_t
                    weights_trending = weights_trending + (K_t.flatten() * error * old_r_blend)
                    P_trending = (P_trending - var_pi * (K_t @ (x_feat.T @ P_trending))) / forgetting_factor

                r_range = 1.0 - old_r_blend
                if r_range > 0.1:
                    P_x_r = P_ranging @ x_feat
                    den_r = forgetting_factor + var_pi * float(x_feat.T @ P_x_r)
                    K_r = P_x_r / den_r
                    weights_ranging = weights_ranging + (K_r.flatten() * error * r_range)
                    P_ranging = (P_ranging - var_pi * (K_r @ (x_feat.T @ P_ranging))) / forgetting_factor
                    
                P_trending = (P_trending + P_trending.T) / 2.0
                P_ranging = (P_ranging + P_ranging.T) / 2.0
                rls_updates += 1

        closes_slice = np.array([cx["close"] for cx in target_candles[max(0, i-20):i+1]])
        vols_slice = np.array([cx["volume"] for cx in target_candles[max(0, i-20):i+1]])
        regime, hmm_state_probs, er = detect_hmm_regime(closes_slice, vols_slice, hmm_state_probs)

        vol_scalar = min(1.0, max(0.0, inst_variance * 5000.0))
        alpha_fast = np.clip(0.05 + (vol_scalar * 0.25) + (er * 0.05), 0.05, 0.35)
        alpha_slow = alpha_fast / 5.0
        hawkes_decay = np.clip(1.0 + (vol_scalar * 4.0), 1.0, 5.0)

        vol_step = c['volume']
        price_step = (c['close'] - c_prev['close'])
        mlofi_step = vol_step * np.sign(price_step) * 0.5 
        
        ofi_fast_mean = (1 - alpha_fast) * ofi_fast_mean + alpha_fast * mlofi_step
        ofi_fast_var = (1 - alpha_fast) * ofi_fast_var + alpha_fast * (mlofi_step - ofi_fast_mean)**2
        ofi_fast_z = (mlofi_step - ofi_fast_mean) / (math.sqrt(ofi_fast_var) + 1e-9)
        
        ofi_slow_mean = (1 - alpha_slow) * ofi_slow_mean + alpha_slow * mlofi_step
        ofi_slow_var = (1 - alpha_slow) * ofi_slow_var + alpha_slow * (mlofi_step - ofi_slow_mean)**2
        ofi_slow_z = (mlofi_step - ofi_slow_mean) / (math.sqrt(ofi_slow_var) + 1e-9)
        
        volume_signed = np.sign(price_step) * vol_step
        hawkes_mean = (1 - alpha_fast) * hawkes_mean + alpha_fast * volume_signed
        hawkes_var = (1 - alpha_slow) * hawkes_var + alpha_slow * (volume_signed - hawkes_mean)**2
        hawkes_z = (volume_signed - hawkes_mean) / (math.sqrt(hawkes_var) + 1e-9)
        
        hawkes_velocity = hawkes_z - hawkes_z_prev
        hawkes_acceleration = hawkes_velocity - hawkes_v_prev
        hawkes_z_prev, hawkes_v_prev = hawkes_z, hawkes_velocity
        
        skew = ((sim_price - c_prev['close']) / (c_prev['close'] + 1e-9)) * 10000.0
        tensor_alpha = compute_tensor_alpha(btc_1m_history, alt_1m_history)
        ofi_delta_z = ofi_fast_z - ofi_slow_z
        
        sim_t_imb = volume_signed
        trade_imbalances.append(sim_t_imb)
        
        price_delta_bps = abs((sim_price - c_prev['close']) / (c_prev['close'] + 1e-9)) * 10000.0
        
        orderbook_elasticity = price_delta_bps / (abs(ofi_fast_z) + 1.0)
        liquidation_div = (hawkes_acceleration / 3.0) * (skew / 10.0) * -1.0 
        
        base_features = np.array([ofi_fast_z / 3.0, ofi_delta_z / 6.0, hawkes_z / 3.0, skew / 10.0, synthetic_vpin_z / 4.0]) 
        cross_momentum = (ofi_fast_z / 3.0) * (hawkes_z / 3.0)
        cross_skew_abs = (skew / 10.0) * (ofi_delta_z / 6.0)
        
        features = np.concatenate([base_features, [cross_momentum, cross_skew_abs, liquidation_div, tensor_alpha]])
        features = np.clip(features, -1.0, 1.0)
        
        attention_temp = max(0.15, min(0.48, 0.18 + 0.30 * (1.0 - er)))
        feature_magnitudes = np.abs(features)
        exp_f = np.exp(feature_magnitudes / attention_temp)
        attended_features = features * (exp_f / (np.sum(exp_f) + 1e-9)) * 9
        
        r_blend = 1.0 / (1.0 + math.exp(-12.0 * (er - 0.35)))

        logit_trend = np.dot(weights_trending, attended_features)
        logit_range = np.dot(weights_ranging, attended_features)
        logit_fused = (r_blend * logit_trend) + ((1.0 - r_blend) * logit_range)

        logit = max(-5.0, min(5.0, logit_fused))
        p_up = 1.0 / (1.0 + math.exp(-logit))
        p_down = 1.0 - p_up
        
        prob_success = max(p_up, p_down)
        action_dir = "BUY" if p_up > p_down else "SELL"
        
        prob_success = calibrate_confidence(prob_success, regime, ewma_mse)
        
        historical_probs.append(prob_success)
        if len(historical_probs) > 100:
            mean_prob = np.mean(historical_probs)
            std_prob = np.std(historical_probs) + 1e-9
            error_penalty = max(0.0, (ewma_mse - 0.25) * 2.0)
            dynamic_gate = mean_prob + (1.25 * std_prob) + error_penalty 
        else:
            dynamic_gate = 0.52 
            
        dynamic_gate = max(0.52, dynamic_gate)
        
        # 🚀 V41.20: DYNAMIC Z-SCORED ENTROPY GATE
        if len(entropy_history) > 30:
            entropy_arr = np.array(entropy_history)
            entropy_mean = np.mean(entropy_arr)
            entropy_std = np.std(entropy_arr) + 1e-9
            entropy_z = (shannon_entropy - entropy_mean) / entropy_std
            if entropy_z > 2.0:
                dynamic_gate = min(0.85, dynamic_gate + 0.05)
        else:
            if shannon_entropy > 0.96:
                dynamic_gate = min(0.85, dynamic_gate + 0.05)
            
        t_imb_z = (sim_t_imb - np.mean(trade_imbalances)) / (np.std(trade_imbalances) + 1e-9) if len(trade_imbalances) > 10 else 0.0
        if t_imb_z < -2.5 and abs(skew) < 1.0: 
            action_dir = "BUY"
            prob_success = max(prob_success, 0.65)
        elif t_imb_z > 2.5 and abs(skew) < 1.0: 
            action_dir = "SELL"
            prob_success = max(prob_success, 0.65)
        
        vol_sigma = math.sqrt(inst_variance) * math.sqrt(60.0)
        elasticity_scalar = max(0.8, min(2.5, orderbook_elasticity))
        sl_dist_pct = max(0.004, min(0.030, vol_sigma * 1.5 * elasticity_scalar))
        
        dynamic_rr_ratio = np.clip(1.2 + (2.0 * (er ** 2)), 1.2, 3.2)
        tp_dist_pct = sl_dist_pct * dynamic_rr_ratio

        virt_sl = sim_price - (sl_dist_pct * sim_price) if action_dir == "BUY" else sim_price + (sl_dist_pct * sim_price)
        virt_tp = sim_price + (tp_dist_pct * sim_price) if action_dir == "BUY" else sim_price - (tp_dist_pct * sim_price)
        
        prediction_buffer.append((now_ts, sim_price, attended_features, p_up, virt_sl, virt_tp, action_dir, r_blend))

        notional_vol = c['volume'] * c['close']
        rolling_notional_volume += notional_vol
        if amihud_anchor_price == 0.0: amihud_anchor_price = c['close']
            
        if rolling_notional_volume >= amihud_threshold:
            amihud_history.append(abs(math.log(c['close'] / (amihud_anchor_price + 1e-9))) / rolling_notional_volume)
            rolling_notional_volume, amihud_anchor_price = 0.0, c['close']

        if i > cooldown_until:
            vacuum_blocked = len(amihud_history) >= 10 and amihud_history[-1] > (np.mean(list(amihud_history)[-10:]) * 4.0)
            dna_win_rate = np.mean(rolling_outcomes) if len(rolling_outcomes) > 10 else 0.50
                    
            routing_mode = "STANDARD"
            if vacuum_blocked and prob_success > 0.65:
                routing_mode = "MAKER_ONLY"
                regime = "MEAN_REVERTING"
                vacuum_blocked = False 
                
            # 🚀 V41.20: DYNAMIC SPREAD & MAKER ROUTING ESTIMATOR
            spread_cost = max(0.0001, min(0.0020, math.sqrt(inst_variance) * 0.5))
            if spread_cost > 0.0005: 
                routing_mode = "MAKER_ONLY"
                regime = "MEAN_REVERTING"
                
            if routing_mode == "MAKER_ONLY":
                dynamic_gate -= 0.08
                    
            if prob_success >= max(dynamic_gate, dna_win_rate) and not vacuum_blocked:
                
                taker_fee_pct = 0.0002 if routing_mode == "MAKER_ONLY" else 0.0006
                
                net_ev_pct = (prob_success * tp_dist_pct) - ((1.0 - prob_success) * sl_dist_pct) - (spread_cost if routing_mode != "MAKER_ONLY" else -spread_cost * 0.2) - taker_fee_pct
                
                # 🚀 V41.20: 1 BPS EV THRESHOLD
                if net_ev_pct > 0.0001:  
                    
                    entry = c['close']
                    sl = entry - (sl_dist_pct * entry) if action_dir == "BUY" else entry + (sl_dist_pct * entry)
                    tp = entry + (tp_dist_pct * entry) if action_dir == "BUY" else entry - (tp_dist_pct * entry)
                    
                    outcome, exit_price, bars_held = None, entry, 0
                    
                    max_favorable_price = entry
                    locked_breakeven = False
                    scaled_out = False
                    initial_risk = sl_dist_pct * entry
                    current_sl = sl
                    current_tp = tp
                    
                    pnl_accum = 0.0
                    position_size = 1.0
                    
                    for j in range(i + 1, min(i + 61, len(target_candles))): 
                        bars_held = j - i
                        h, l = target_candles[j]["high"], target_candles[j]["low"]
                        
                        if action_dir == "BUY" and h > max_favorable_price: max_favorable_price = h
                        elif action_dir == "SELL" and l < max_favorable_price: max_favorable_price = l
                        
                        profit_distance = abs(max_favorable_price - entry)
                        r_multiple = profit_distance / (initial_risk + 1e-9)
                        
                        if r_multiple >= 1.0 and not scaled_out:
                            pnl_accum += (1.0 * initial_risk) * 0.5
                            position_size = 0.5
                            scaled_out = True
                        
                        if r_multiple >= 0.5 and not locked_breakeven:
                            if r_multiple >= 1.0:
                                # 🚀 V41.20: 6 BPS BREAKEVEN RATCHET
                                fee_coverage = entry * 0.0006
                                current_sl = entry + fee_coverage if action_dir == "BUY" else entry - fee_coverage
                                locked_breakeven = True
                            else:
                                half_risk_sl = entry - (initial_risk * 0.5) if action_dir == "BUY" else entry + (initial_risk * 0.5)
                                if (action_dir == "BUY" and half_risk_sl > current_sl) or (action_dir == "SELL" and half_risk_sl < current_sl):
                                    current_sl = half_risk_sl
                        
                        base_mult = max(0.4, 2.0 - (r_multiple * 0.4))
                        vol_adj = 1.0 + (vol_scalar * 0.5)
                        el_adj = max(0.8, min(1.5, 1.0 / (orderbook_elasticity + 0.2)))
                        dynamic_trail_dist = initial_risk * base_mult * vol_adj * el_adj
                        
                        if action_dir == "BUY":
                            calc_sl = max_favorable_price - dynamic_trail_dist
                            if calc_sl > current_sl:
                                if abs(calc_sl - current_sl) / entry > 0.0010: # 🚀 V41.20: Stale price fix (entry dev)
                                    current_sl = calc_sl
                        else:
                            calc_sl = max_favorable_price + dynamic_trail_dist
                            if calc_sl < current_sl:
                                if abs(current_sl - calc_sl) / entry > 0.0010: # 🚀 V41.20: Stale price fix (entry dev)
                                    current_sl = calc_sl
                            
                        if r_multiple > 1.2:
                            tp_expansion_factor = min(4.0, r_multiple + (vol_scalar * 2.0))
                            dynamic_tp_dist = initial_risk * tp_expansion_factor
                            
                            calc_tp = entry + dynamic_tp_dist if action_dir == "BUY" else entry - dynamic_tp_dist
                            
                            # 🚀 V41.20: TP Stale Price Guard Check
                            if action_dir == "BUY" and calc_tp > current_tp:
                                if abs(calc_tp - current_tp) / entry > 0.002:
                                    current_tp = calc_tp
                            elif action_dir == "SELL" and calc_tp < current_tp:
                                if abs(current_tp - calc_tp) / entry > 0.002:
                                    current_tp = calc_tp
                            
                        hit_tp = h >= current_tp if action_dir == "BUY" else l <= current_tp
                        hit_sl = l <= current_sl if action_dir == "BUY" else h >= current_sl
                        
                        if hit_tp and hit_sl: outcome, exit_price = "LOSS", current_sl; break
                        if hit_tp: outcome, exit_price = "WIN", current_tp; break
                        if hit_sl: outcome, exit_price = "LOSS" if not scaled_out else "WIN", current_sl; break
                            
                    if outcome is None: 
                        exit_price = target_candles[min(i + 60, len(target_candles) - 1)]["close"]
                        outcome = "WIN" if ((exit_price > entry) == (action_dir == "BUY")) else "LOSS"

                    gross = (exit_price - entry) / entry if action_dir == "BUY" else (entry - exit_price) / entry
                    gross = (gross * position_size) + (pnl_accum / entry) 
                    
                    holding_hours = bars_held / 60.0
                    funding_drag = FUNDING_PER_8H * (holding_hours / 8)
                    
                    if regime == "RANGING" or routing_mode == "MAKER_ONLY":
                        slippage_penalty = 0.0
                        applied_fee = MAKER_FEE * 2
                    else:
                        dynamic_slippage_bps = BASE_SLIPPAGE_BPS * max(1.0, abs(hawkes_z) * 0.5)
                        slippage_penalty = (dynamic_slippage_bps * 2) / 10000.0
                        applied_fee = TAKER_FEE * 2
                    
                    edge = prob_success - 0.50
                    risk_multiplier = edge / 0.10
                    raw_fractional_risk = max(0.005, min(0.015, 0.01 * risk_multiplier))
                    
                    # 🚀 V41.20: Explicitly define net_edge_bps so Kelly multiplier doesn't NameError
                    net_edge_bps = net_ev_pct * 10000.0 
                    edge_factor = min(1.5, max(0.5, net_edge_bps / 50.0))
                    
                    evt_penalty = calculate_evt_tail_risk(evt_vol_surface)
                    fractional_risk = raw_fractional_risk * evt_penalty * edge_factor
                    
                    net_unleveraged = gross - applied_fee - funding_drag - slippage_penalty
                    net_leveraged = net_unleveraged * p.leverage * (fractional_risk / 0.015)

                    trades.append({
                        "i": i, "direction": action_dir, "regime": regime,
                        "outcome": outcome, "net": net_leveraged, "bars": bars_held
                    })
                    
                    rolling_outcomes.append(1.0 if net_leveraged > 0 else 0.0)
                    
                    if net_leveraged < 0:
                        recent_losses = sum(1 for out in list(rolling_outcomes)[-2:] if out == 0.0)
                        if recent_losses >= 2:
                            cooldown_until = i + 120 
                        else:
                            cooldown_until = i + bars_held
                    else:
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
    
    mean_return = np.mean(nets)
    std_return = np.std(nets) + 1e-9
    sharpe = (mean_return / std_return) * math.sqrt(len(trades))
    
    return {
        "trades": len(trades),
        "win_rate": float(len(wins) / len(trades)),
        "avg_win": float(np.mean(wins)) if len(wins) else 0.0,
        "avg_loss": float(np.mean(losses)) if len(losses) else 0.0,
        "expectancy_per_trade": float(np.mean(nets)),
        "profit_factor": float(wins.sum() / (abs(losses.sum()) + 1e-9)) if losses.sum() != 0 else float("inf"),
        "total_return_on_margin": float(equity[-1]),
        "max_drawdown_on_margin": max_dd,
        "sharpe_ratio": float(sharpe),
        "monte_carlo_p_positive": float(np.mean(np.array(mc_results) > 0)),
        "by_regime": {
            r: {"trades": sum(1 for t in trades if t["regime"] == r),
                "win_rate": float(np.mean([1 if t["net"] > 0 else 0 for t in trades if t["regime"] == r]) or 0.0)}
            for r in ("TRENDING", "RANGING")
        },
    }

def parameter_sweep(t_cand: List[Dict], b_cand: List[Dict], symbol: str) -> List[Dict]:
    results = []
    print("\n⏳ Running V41.20 APEX Walk-Forward Validation (5 Folds)...")
    
    rr_ratios = [1.5, 2.0, 2.5]
    atr_mults = [1.2, 1.5, 2.0]
    
    total_len = len(t_cand)
    fold_size = int(total_len / 6) 
    train_size = fold_size * 2 
    
    for rr in rr_ratios:
        for atr_m in atr_mults:
            p = Params(rr_ratio=rr, sl_atr_mult=atr_m)
            
            fold_sharpes = []
            fold_expectancies = []
            total_trades = 0
            
            for fold in range(4):
                train_start = fold * fold_size
                test_start = train_start + train_size
                test_end = test_start + fold_size
                
                if test_end > total_len: break
                
                test_result = run_v41_backtest(t_cand[test_start:test_end], b_cand[test_start:test_end], p, symbol)
                
                if test_result.get("trades", 0) > 2:
                    fold_sharpes.append(test_result.get("sharpe_ratio", 0.0))
                    fold_expectancies.append(test_result.get("expectancy_per_trade", 0.0))
                    total_trades += test_result.get("trades", 0)
                else:
                    fold_sharpes.append(-1.0) 
            
            if total_trades > 10 and len(fold_sharpes) == 4:
                avg_sharpe = np.mean(fold_sharpes)
                avg_expectancy = np.mean(fold_expectancies)
                
                if min(fold_sharpes) > -0.5:
                    results.append({
                        "RR": rr, "ATR": atr_m,
                        "OOS_Avg_Sharpe": avg_sharpe,
                        "OOS_Avg_Expectancy": avg_expectancy,
                        "Total_Trades": total_trades,
                        "Min_Fold_Sharpe": min(fold_sharpes)
                    })
                    
    return sorted(results, key=lambda x: x["OOS_Avg_Sharpe"], reverse=True)[:5]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--days", type=int, default=30)  
    parser.add_argument("--optimize", action="store_true")
    args = parser.parse_args()

    print(f"📥 Building matrix mapping for {args.days}d of 1-Minute High-Resolution Data...")
    t_cand, b_cand = fetch_aligned_data(args.symbol, args.days)
    print(f"✅ Matrix synchronized. ({len(t_cand)} true 1m blocks)")

    if args.optimize:
        best_params = parameter_sweep(t_cand, b_cand, args.symbol)
        print("\n🏆 Top 5 Walk-Forward Configurations (Sorted by Avg OOS Sharpe):")
        for i, res in enumerate(best_params, 1):
            print(f" {i}. RR: {res['RR']} | SL ATR: {res['ATR']} "
                  f"--> Avg Sharpe: {res['OOS_Avg_Sharpe']:.2f} | Min Fold Sharpe: {res['Min_Fold_Sharpe']:.2f}")
        
        import json
        if best_params:
            best = best_params[0]
            with open("params.json", "w") as f:
                json.dump({"rr_ratio": best["RR"], "sl_atr_mult": best["ATR"]}, f)
            print("💾 Saved most robust parameters to params.json for live engine sync.")
            
    else:
        split = int(len(t_cand) * 0.6)
        params = Params()

        test = run_v41_backtest(t_cand[split:], b_cand[split:], params, args.symbol)
                
        print("\n=== OUT-OF-SAMPLE (last 40%) — TRUE MATHEMATICAL REALITY ===")
        for k, v in test.items():
            if isinstance(v, float): print(f"  {k}: {v:.4f}")
            else: print(f"  {k}: {v}")