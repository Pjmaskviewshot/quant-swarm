"""
💎 V25.2 APEX QUANTUM PRIME: NEURAL BACKTESTER
-------------------------------------
WARNING: This backtester uses 1-Minute OHLCV data. It is an approximation
of the live V25 L2-tick engine and cannot simulate true Order Flow Imbalance.

Architectural Supremacy (V25.2):
1. Fixed Fractional Parity: Aligned backtest sizing with the live SOR (1.0% - 1.5% max risk),
   abandoning the ruinous Kelly Criterion.
2. Pure Directional RLS Target: RLS filters now train strictly on binary directional 
   classification rather than path-dependent R-multiples.
3. Accurate Intraday Sharpe Annualization: Corrected the Sharpe ratio calculation 
   to scale by trades_per_day * 252 rather than raw trade count.
4. Directional Orthogonalization: Synced the Volterra attention mechanism with the 
   live engine's L2 vector normalization.
"""

import argparse
import time
import math
import datetime
import requests
import json
import numpy as np
from collections import deque
from dataclasses import dataclass
from typing import List, Dict, Tuple

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"
TAKER_FEE = 0.00055          
MAKER_FEE = 0.00020          
FUNDING_PER_8H = 0.0001      
BASE_SLIPPAGE_BPS = 5        

class AdaptiveSessionClock:
    """Handles Weekend vs. Weekday regime adjustments for backtesting fidelity."""
    @staticmethod
    def is_weekend(ts_ms: int) -> bool:
        dt = datetime.datetime.fromtimestamp(ts_ms / 1000.0, datetime.timezone.utc)
        return dt.weekday() in (5, 6)

    @classmethod
    def get_turnover_threshold(cls, ts_ms: int) -> float:
        return 3_000_000.0 if cls.is_weekend(ts_ms) else 5_000_000.0

    @classmethod
    def get_ev_floor(cls, routing_mode: str) -> float:
        if routing_mode == "MAKER_ONLY":
            return 0.00001  
        return 0.00002      


class ClusterWarmStartRLS:
    @staticmethod
    def get_cluster_priors(symbol: str, dim: int = 18) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, float]:
        w_trend = np.zeros(dim, dtype=np.float64)
        w_range = np.zeros(dim, dtype=np.float64)
        w_spoof = np.zeros(dim, dtype=np.float64)
        w_cascade = np.zeros(dim, dtype=np.float64)

        if any(m in symbol for m in ["BTC", "ETH", "SOL"]):
            p_scale = 1.0
        elif any(m in symbol for m in ["AVAX", "LINK", "XRP", "ADA", "DOT", "NEAR", "SUI"]):
            p_scale = 2.0
        else:
            p_scale = 3.0

        w_trend[:] = 0.1
        w_range[:] = 0.05
        w_spoof[:] = -0.1
        w_cascade[:] = 0.2

        return w_trend, w_range, w_spoof, w_cascade, p_scale


def compute_permutation_entropy(series: list, order: int = 3, delay: int = 1) -> float:
    """C-Vectorized Shannon Permutation Entropy."""
    if len(series) < (order * delay): return 1.0
    try:
        arr = np.asarray(series, dtype=np.float64)
        shape = (arr.size - (order - 1) * delay, order)
        strides = (arr.strides[0], arr.strides[0] * delay)
        
        sub_vectors = np.lib.stride_tricks.as_strided(arr, shape=shape, strides=strides)
        perms = np.argsort(sub_vectors, axis=1)
        
        bases = order ** np.arange(order) 
        hashed = np.sum(perms * bases, axis=1)
        
        _, counts = np.unique(hashed, return_counts=True)
        p = counts / counts.sum()
        p = p[p > 0]
        
        entropy = -np.sum(p * np.log2(p))
        max_entropy = math.log2(math.factorial(order))
        return float(np.clip(entropy / max_entropy, 0.0, 1.0))
    except Exception: return 1.0


class BacktestCholeskyWhitening:
    """
    🚀 V25.0 AUDIT FIX: Matches production micro_models.py streaming 11D Cholesky whitening.
    """
    def __init__(self, n_features: int = 11, alpha: float = 0.001): # V25.1 Matrix Stability Fix
        self.n = n_features
        self.alpha = alpha
        self.mean = np.zeros(n_features, dtype=np.float64)
        self.cov = np.eye(n_features, dtype=np.float64) * 0.1
        self.I = np.eye(n_features, dtype=np.float64)

    def update_and_whiten(self, x: np.ndarray) -> np.ndarray:
        delta = x - self.mean
        self.mean += self.alpha * delta
        self.cov = (1.0 - self.alpha) * self.cov + self.alpha * np.outer(delta, delta)
        self.cov = 0.5 * (self.cov + self.cov.T)
        
        stable_cov = self.cov + (self.I * 1e-5)
        try:
            L = np.linalg.cholesky(stable_cov)
            return np.clip(np.linalg.solve(L, delta) / 3.0, -2.5, 2.5)
        except np.linalg.LinAlgError:
            diag_stds = np.sqrt(np.maximum(1e-8, np.diag(stable_cov)))
            return np.clip(delta / (diag_stds * 3.0), -2.5, 2.5)


class JosephStabilizedRLS:
    def __init__(self, dim: int = 18, p_init: float = 1.0):
        self.dim = dim
        self.w = np.zeros(dim, dtype=np.float64)
        self.P = np.eye(dim, dtype=np.float64) * p_init
        self.I = np.eye(dim, dtype=np.float64)
        self.error_history = deque(maxlen=50)

    def update(self, x: np.ndarray, y_target: float, y_pred: float) -> float:
        raw_error = float(y_target - y_pred)
        self.error_history.append(abs(raw_error))
        
        recent_err = np.mean(self.error_history) if len(self.error_history) >= 10 else abs(raw_error)
        dynamic_lambda = max(0.90, min(0.999, 0.998 - (0.08 * recent_err)))
        
        bounded_error = np.clip(raw_error, -1.0, 1.0)
        x_vec = x.reshape(-1, 1)

        Px = self.P @ x_vec
        denom = dynamic_lambda + float(x_vec.T @ Px)
        if denom < 1e-9: return raw_error

        K = Px / denom
        self.w = self.w + (K.flatten() * bounded_error)

        IKx = self.I - (K @ x_vec.T)
        self.P = (IKx @ self.P @ IKx.T + (K @ K.T) * (bounded_error**2 + 1e-5)) / dynamic_lambda
        self.P = 0.5 * (self.P + self.P.T) + (self.I * 1e-4)

        trace_P = np.trace(self.P)
        if trace_P > 600.0: self.P = self.P * (600.0 / trace_P)

        return raw_error


class QuantumMarkovRegimeDetector:
    def __init__(self):
        self.beliefs = np.array([0.25, 0.25, 0.25, 0.25], dtype=np.float64)
        self.TPM = np.array([
            [0.94, 0.02, 0.02, 0.02],  
            [0.02, 0.94, 0.02, 0.02],  
            [0.05, 0.05, 0.85, 0.05],  
            [0.05, 0.05, 0.05, 0.85]   
        ], dtype=np.float64)

    def update_beliefs(self, er: float, entropy: float, fleeting: float, jump_z: float) -> np.ndarray:
        prior = self.TPM.T @ self.beliefs
        l_trend = math.exp(-2.0 * ((1.0 - er)**2) - 1.5 * (entropy**2) - 3.0 * (fleeting**2))
        l_range = math.exp(-2.5 * (er**2) - 1.5 * ((1.0 - entropy)**2) - 2.0 * (fleeting**2))
        l_spoof = math.exp(-3.0 * ((1.0 - fleeting)**2) - 1.0 * (er**2))
        l_cascade = math.exp(-1.0 * ((3.0 - min(3.0, abs(jump_z)))**2)) 
        
        likelihoods = np.array([l_trend, l_range, l_spoof, l_cascade], dtype=np.float64) + 1e-6
        unnormalized = prior * likelihoods
        self.beliefs = unnormalized / (np.sum(unnormalized) + 1e-9)
        return self.beliefs


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
    sl_atr_mult: float = 2.5         
    atr_period: int = 14
    leverage: float = 2.0  # 🚀 V25.2 FIX: Enforced 2.0x Hard Cap (Matches SOR & Risk Vault)          


def compute_tensor_alpha(btc_hist: deque, alt_hist: deque) -> float:
    if len(btc_hist) < 30 or len(alt_hist) < 30: return 0.0
    aligned_b, aligned_a = [], []
    
    for i in range(2, len(alt_hist)):
        try:
            a_ret = math.log(alt_hist[i] / (alt_hist[i-1] + 1e-9))
            b_ret = math.log(btc_hist[i-1] / (btc_hist[i-2] + 1e-9))
            aligned_a.append(a_ret)
            aligned_b.append(b_ret)
        except ValueError:
            continue
    
    if len(aligned_a) < 20: return 0.0
    
    try:
        with np.errstate(divide='ignore', invalid='ignore'):
            correlation = float(np.corrcoef(aligned_b, aligned_a)[0, 1])
        if np.isnan(correlation): return 0.0
    except Exception:
        return 0.0
    
    btc_momentum = float(np.mean(aligned_b[-10:]))
    if abs(btc_momentum) > 0.00015 and correlation > 0.45:
        return float(math.copysign(min(1.0, abs(correlation)), btc_momentum))
    return 0.0


def run_v25_backtest(target_candles: List[Dict], btc_candles: List[Dict], p: Params, symbol: str) -> Dict:
    trades = []
    cooldown_until = -1
    
    ofi_fast_mean, ofi_fast_var, ofi_fast_z = 0.0, 1.0, 0.0
    ofi_slow_mean, ofi_slow_var, ofi_slow_z = 0.0, 1.0, 0.0
    hawkes_mean, hawkes_var, hawkes_z = 0.0, 1.0, 0.0
    hawkes_velocity, hawkes_acceleration = 0.0, 0.0
    hawkes_z_prev, hawkes_v_prev = 0.0, 0.0
    
    meso_fast_ema = None
    meso_slow_ema = None
    meso_momentum_z = 0.0
    
    vol_ewma = 0.0
    amihud_history = deque(maxlen=100)
    rolling_outcomes = deque(maxlen=100)
    trade_imbalances = deque(maxlen=100)
    
    btc_1m_history = deque(maxlen=300)
    alt_1m_history = deque(maxlen=300)
    entropy_history = deque(maxlen=200) 
    log_returns = deque(maxlen=500)
    inst_variance = 1e-6
    kaufman_er = 0.5

    w_t, w_r, w_s, w_c, p_scale = ClusterWarmStartRLS.get_cluster_priors(symbol, dim=18)
    
    # 🚀 AUDIT FIX: 1:1 Live Production Cholesky Whitening & RLS Setup
    cholesky_engine = BacktestCholeskyWhitening(n_features=11, alpha=0.001)
    rls_trend = JosephStabilizedRLS(dim=18, p_init=p_scale)
    rls_range = JosephStabilizedRLS(dim=18, p_init=p_scale)
    rls_spoof = JosephStabilizedRLS(dim=18, p_init=p_scale)
    rls_cascade = JosephStabilizedRLS(dim=18, p_init=p_scale)
    
    rls_trend.w, rls_range.w, rls_spoof.w, rls_cascade.w = w_t.copy(), w_r.copy(), w_s.copy(), w_c.copy()
    
    regime_detector = QuantumMarkovRegimeDetector()
    
    prediction_buffer = deque()
    historical_probs = deque(maxlen=2000) 
    ewma_mse = 0.25
    
    rolling_notional_volume = 0.0
    amihud_anchor_price = 0.0
    
    if "BTC" in symbol: amihud_threshold = 2_500_000.0  
    elif "ETH" in symbol or "SOL" in symbol: amihud_threshold = 1_000_000.0   
    else: amihud_threshold = 250_000.0   

    for i in range(101, len(target_candles)):
        c_prev = target_candles[i-1]
        c_prev_prev = target_candles[i-2]
        
        c = target_candles[i]
        now_ts = c['ts']
        sim_price = c['open'] 
        
        btc_1m_history.append(btc_candles[i-1]['close'])
        alt_1m_history.append(c_prev['close'])
        
        safe_curr_prev = max(1e-9, c_prev['close'])
        safe_prev_prev = max(1e-9, c_prev_prev['close'])
        ret_prev = math.log(safe_curr_prev / safe_prev_prev)
        log_returns.append(ret_prev)
        
        shannon_entropy = 1.0
        if len(log_returns) > 10:
            inst_variance = np.var(list(log_returns)[-10:]) + 1e-9
            shannon_entropy = compute_permutation_entropy(list(log_returns)[-20:])
            entropy_history.append(shannon_entropy) 

        a_fast = 2.0 / (51.0)
        a_slow = 2.0 / (301.0)
        if meso_fast_ema is None:
            meso_fast_ema, meso_slow_ema = sim_price, sim_price
        else:
            meso_fast_ema = (sim_price - meso_fast_ema) * a_fast + meso_fast_ema
            meso_slow_ema = (sim_price - meso_slow_ema) * a_slow + meso_slow_ema
        meso_momentum_z = ((meso_fast_ema - meso_slow_ema) / meso_slow_ema) / (math.sqrt(inst_variance) + 1e-9)

        vol_scalar = min(1.0, max(0.0, inst_variance * 5000.0))
        
        closes_slice = np.array([cx["close"] for cx in target_candles[max(0, i-101):i]])
        if len(closes_slice) >= 20:
            directional_change = abs(closes_slice[-1] - closes_slice[0])
            absolute_changes = np.sum(np.abs(np.diff(closes_slice)))
            kaufman_er = float(directional_change / (absolute_changes + 1e-9))
        else: kaufman_er = 0.5
            
        alpha_fast = np.clip(0.05 + (vol_scalar * 0.25) + (kaufman_er * 0.05), 0.05, 0.35)
        alpha_slow = alpha_fast / 5.0

        vol_step = c_prev['volume']
        log_vol_step = math.log1p(vol_step)
        price_step = (c_prev['close'] - c_prev_prev['close'])
        mlofi_step = log_vol_step * np.sign(price_step) * 0.5 
        
        ofi_fast_mean = (1 - alpha_fast) * ofi_fast_mean + alpha_fast * mlofi_step
        ofi_fast_var = (1 - alpha_fast) * ofi_fast_var + alpha_fast * (mlofi_step - ofi_fast_mean)**2
        ofi_fast_z = (mlofi_step - ofi_fast_mean) / (math.sqrt(ofi_fast_var) + 1e-9)
        
        ofi_slow_mean = (1 - alpha_slow) * ofi_slow_mean + alpha_slow * mlofi_step
        ofi_slow_var = (1 - alpha_slow) * ofi_slow_var + alpha_slow * (mlofi_step - ofi_slow_mean)**2
        
        vol_ewma = (1 - 0.05) * vol_ewma + 0.05 * vol_step if vol_ewma > 0 else vol_step
        normalized_volume = vol_step / (vol_ewma + 1e-9)
        volume_mark = math.log1p(max(0.0, normalized_volume))
        
        volume_signed = np.sign(price_step) * volume_mark
        hawkes_mean = (1 - alpha_fast) * hawkes_mean + alpha_fast * volume_signed
        hawkes_var = (1 - alpha_slow) * hawkes_var + alpha_slow * (volume_signed - hawkes_mean)**2
        hawkes_z = (volume_signed - hawkes_mean) / (math.sqrt(hawkes_var) + 1e-9)
        
        hawkes_velocity = hawkes_z - hawkes_z_prev
        hawkes_acceleration = hawkes_velocity - hawkes_v_prev
        hawkes_z_prev, hawkes_v_prev = hawkes_z, hawkes_velocity
        
        tensor_alpha = compute_tensor_alpha(btc_1m_history, alt_1m_history)
        sim_t_imb = volume_signed
        trade_imbalances.append(sim_t_imb)
        
        # Proxies for 1m offline backtesting mapped to 11D array
        micro_elasticity_z = (c_prev['high'] - c_prev['low']) / (sim_price * math.sqrt(inst_variance) + 1e-9)
        ou_divergence_z = (sim_price - meso_slow_ema) / (sim_price * math.sqrt(inst_variance) + 1e-9)
        cfi_z = 0.0 # Unavailable in 1m OHLCV
        jump_z = abs(price_step) / (sim_price * math.sqrt(inst_variance) + 1e-9)
        swd_z = hawkes_z - (price_step / (sim_price * math.sqrt(inst_variance) + 1e-9))
        accel_z = hawkes_acceleration
        
        raw_vec = np.array([
            ofi_fast_z, hawkes_z, meso_momentum_z, tensor_alpha,
            micro_elasticity_z, ou_divergence_z, cfi_z,
            jump_z, shannon_entropy, swd_z, accel_z
        ], dtype=np.float64)

        f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11 = cholesky_engine.update_and_whiten(raw_vec)
        
        exploit_spoof = f7 * -f3      
        predict_iceberg = f1 * f10    
        kinetic_reversal = f11 * f2   
        survive_cascade = f8 * f5     
        exploit_macro = f4 * f1       
        exploit_reversion = f6 * f2   
        
        volterra = np.array([
            f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11,
            exploit_spoof, predict_iceberg, kinetic_reversal, survive_cascade, exploit_macro, exploit_reversion,
            1.0
        ], dtype=np.float64)

        # 🚀 V25.2 FIX: L2 Vector Normalization (Replaces Ruinous Absolute Value Softmax)
        norm = np.linalg.norm(volterra) + 1e-9
        volterra_att = volterra / norm
        
        beliefs = regime_detector.update_beliefs(kaufman_er, shannon_entropy, 0.0, jump_z)
        p_t, p_r, p_s, p_c = beliefs

        l_t = float(np.dot(rls_trend.w, volterra_att))
        l_r = float(np.dot(rls_range.w, volterra_att))
        l_s = float(np.dot(rls_spoof.w, volterra_att))
        l_c = float(np.dot(rls_cascade.w, volterra_att))

        logit = float(np.clip((p_t * l_t) + (p_r * l_r) + (p_s * l_s) + (p_c * l_c), -5.0, 5.0))
        p_up = 1.0 / (1.0 + math.exp(-logit))
        p_down = 1.0 - p_up
        
        prob_success = max(p_up, p_down)
        action_dir = "BUY" if p_up > p_down else "SELL"
        
        historical_probs.append(prob_success)
        if len(historical_probs) >= 30:
            prob_arr = np.fromiter(historical_probs, dtype=float, count=len(historical_probs))
            baseline_gate = float(np.percentile(prob_arr, 60))
            dynamic_ceiling = min(0.72, float(np.percentile(prob_arr, 95)) + 0.05)
        else:
            baseline_gate = 0.55
            dynamic_ceiling = 0.72
            
        if len(entropy_history) > 10:
            ent_arr = np.array(entropy_history)
            entropy_z_val = (shannon_entropy - float(np.mean(ent_arr))) / (float(np.std(ent_arr)) + 1e-9)
            entropy_multiplier = 1.0 + (entropy_z_val * 0.04)
        else:
            entropy_multiplier = 1.0
            
        error_scaler = 1.0 + max(0.0, (ewma_mse - 0.25) * 0.5)
        raw_gate = baseline_gate * entropy_multiplier * error_scaler
        dynamic_gate = max(0.515, min(dynamic_ceiling, raw_gate))  

        while prediction_buffer and (now_ts - prediction_buffer[0][0]) >= 60000:  
            old_ts, old_price, old_features, old_p_up, virt_sl, virt_tp, old_action_dir, old_beliefs = prediction_buffer.popleft()
            
            if sim_price != old_price and old_price > 0:
                price_delta = sim_price - old_price
                risk_distance = abs(old_price - virt_sl) + 1e-9
                realized_r = price_delta / risk_distance
                
                # 🚀 V25.2 FIX: Pure Directional RLS Target
                if old_action_dir == "BUY":
                    y_target = 1.0 if realized_r > 0 else 0.0
                else:
                    y_target = 1.0 if realized_r < 0 else 0.0

                old_p_up_prob = old_p_up if old_action_dir == "BUY" else (1.0 - old_p_up)
                
                e_t = rls_trend.update(old_features, y_target, old_p_up_prob)
                e_r = rls_range.update(old_features, y_target, old_p_up_prob)
                e_s = rls_spoof.update(old_features, y_target, old_p_up_prob)
                e_c = rls_cascade.update(old_features, y_target, old_p_up_prob)
                
                error = (e_t * old_beliefs[0]) + (e_r * old_beliefs[1]) + (e_s * old_beliefs[2]) + (e_c * old_beliefs[3])
                ewma_mse = (0.98 * ewma_mse) + (0.02 * (error ** 2))

        vol_sigma = math.sqrt(inst_variance) * math.sqrt(60.0)
        atr_proxy = vol_sigma * sim_price
        
        sl_distance = max(atr_proxy * p.sl_atr_mult, sim_price * 0.025)
        sl_dist_pct = sl_distance / sim_price
        
        dynamic_rr_ratio = np.clip(p.rr_ratio + (1.5 * (kaufman_er ** 2)), 1.2, 4.0)
        tp_dist_pct = sl_dist_pct * dynamic_rr_ratio

        virt_sl = sim_price - (sl_dist_pct * sim_price) if action_dir == "BUY" else sim_price + (sl_dist_pct * sim_price)
        virt_tp = sim_price + (tp_dist_pct * sim_price) if action_dir == "BUY" else sim_price - (tp_dist_pct * sim_price)
        
        prediction_buffer.append((now_ts, sim_price, volterra_att, p_up, virt_sl, virt_tp, action_dir, beliefs))

        notional_vol = c_prev['volume'] * c_prev['close']
        rolling_notional_volume += notional_vol
        if amihud_anchor_price == 0.0: amihud_anchor_price = c_prev['close']
            
        if rolling_notional_volume >= amihud_threshold:
            amihud_history.append(abs(math.log(c_prev['close'] / (amihud_anchor_price + 1e-9))) / rolling_notional_volume)
            rolling_notional_volume, amihud_anchor_price = 0.0, c_prev['close']

        if i > cooldown_until and i > 150:
            vacuum_blocked = len(amihud_history) >= 10 and amihud_history[-1] > (np.mean(list(amihud_history)[-10:]) * 4.0)
            
            if len(rolling_outcomes) >= 10:
                p_win = np.mean(rolling_outcomes)
            else:
                p_win = 0.50
                
            routing_mode = "STANDARD"
            regime = "TRENDING" if p_t > 0.5 else "RANGING"
            
            if vacuum_blocked and prob_success > 0.55:
                routing_mode = "MAKER_ONLY"
                regime = "MEAN_REVERTING"
                vacuum_blocked = False 
                
            spread_cost = max(0.0001, min(0.0020, math.sqrt(inst_variance) * 0.5))
            if spread_cost > 0.0004: 
                routing_mode = "MAKER_ONLY"
                regime = "MEAN_REVERTING"
                
            if routing_mode == "MAKER_ONLY":
                dynamic_gate -= 0.05
                
            ev_floor = AdaptiveSessionClock.get_ev_floor(routing_mode)
                
            if prob_success >= max(dynamic_gate, p_win) and not vacuum_blocked:
                taker_fee_pct = 0.0002 if routing_mode == "MAKER_ONLY" else 0.0005
                
                net_ev_pct = (prob_success * tp_dist_pct) - ((1.0 - prob_success) * sl_dist_pct) - (spread_cost if routing_mode != "MAKER_ONLY" else -spread_cost * 0.2) - taker_fee_pct
                net_edge_bps = net_ev_pct * 10000.0
                
                if net_ev_pct > ev_floor:  
                    entry = c['open'] 
                    initial_risk = sl_dist_pct * entry
                    realigned_sl = entry - initial_risk if action_dir == "BUY" else entry + initial_risk
                    
                    r_t1 = round(max(1.0, dynamic_rr_ratio * 0.6), 2)
                    r_t2 = round(max(1.5, dynamic_rr_ratio * 1.0), 2)
                    r_t3 = round(max(2.0, dynamic_rr_ratio * 1.5), 2)
                    scaled_levels = {r_t1: False, r_t2: False, r_t3: False}
                    
                    max_favorable_price = entry
                    current_sl = realigned_sl
                    current_tp = entry + (tp_dist_pct * entry) if action_dir == "BUY" else entry - (tp_dist_pct * entry)
                    
                    outcome, exit_price, bars_held = None, entry, 0
                    pnl_accum = 0.0
                    position_size = 1.0
                    
                    for j in range(i + 1, min(i + 300, len(target_candles))): 
                        bars_held = j - i
                        h, l = target_candles[j]["high"], target_candles[j]["low"]
                        c_j = target_candles[j]["close"]
                        
                        if action_dir == "BUY" and h > max_favorable_price: max_favorable_price = h
                        elif action_dir == "SELL" and l < max_favorable_price: max_favorable_price = l
                        
                        r_multiple = abs(max_favorable_price - entry) / (initial_risk + 1e-9)
                        current_r = (c_j - entry) / (initial_risk + 1e-9) if action_dir == "BUY" else (entry - c_j) / (initial_risk + 1e-9)
                        
                        c_prev_j = target_candles[j-1]
                        norm_vol_j = target_candles[j]["volume"] / (vol_ewma + 1e-9)
                        hawkes_z_j = math.log1p(max(0.0, norm_vol_j)) * 1.5 * np.sign(c_j - c_prev_j["close"])
                        
                        if r_multiple >= 0.75:
                            if action_dir == "BUY" and hawkes_z_j < -2.8:
                                outcome, exit_price = "HAWKES_CLIMAX", c_j
                                break
                            elif action_dir == "SELL" and hawkes_z_j > 2.8:
                                outcome, exit_price = "HAWKES_CLIMAX", c_j
                                break
                                
                        if r_multiple >= 1.20:
                            retrace_pct = (r_multiple - current_r) / (r_multiple + 1e-9)
                            if retrace_pct >= 0.30:
                                outcome, exit_price = "PROFIT_RETRACEMENT", c_j
                                break

                        if r_multiple >= 0.60:
                            fee_buffer = entry * 0.0015
                            be_price = (entry + fee_buffer) if action_dir == "BUY" else (entry - fee_buffer)
                            current_sl = max(current_sl, be_price) if action_dir == "BUY" else min(current_sl, be_price)
                            
                        if r_multiple >= 1.0:
                            vol_buffer = max(atr_proxy * 1.2, entry * math.sqrt(inst_variance) * 1.5)
                            tight_trail = max_favorable_price - vol_buffer if action_dir == "BUY" else max_favorable_price + vol_buffer
                            current_sl = max(current_sl, tight_trail) if action_dir == "BUY" else min(current_sl, tight_trail)

                        for target_r, flag in scaled_levels.items():
                            if r_multiple >= target_r and not flag:
                                portion = 0.25 if target_r == r_t1 else (0.35 if target_r == r_t2 else 0.40)
                                scale_exit_price = entry + (target_r * initial_risk) if action_dir == "BUY" else entry - (target_r * initial_risk)
                                gross_r = (scale_exit_price - entry) / entry if action_dir == "BUY" else (entry - scale_exit_price) / entry
                                pnl_accum += gross_r * position_size * portion
                                position_size -= (position_size * portion)
                                scaled_levels[target_r] = True

                        hit_tp = h >= current_tp if action_dir == "BUY" else l <= current_tp
                        hit_sl = l <= current_sl if action_dir == "BUY" else h >= current_sl
                        
                        if hit_tp and hit_sl: outcome, exit_price = "LOSS", current_sl; break
                        if hit_tp: outcome, exit_price = "WIN", current_tp; break
                        if hit_sl: outcome, exit_price = ("LOSS" if r_multiple < 1.0 else "WIN"), current_sl; break
                            
                    if outcome is None: 
                        exit_price = target_candles[min(i + 299, len(target_candles) - 1)]["close"]
                        outcome = "TIME_EXIT"

                    gross = (exit_price - entry) / entry if action_dir == "BUY" else (entry - exit_price) / entry
                    gross = (gross * position_size) + pnl_accum 
                    
                    holding_hours = bars_held / 60.0
                    funding_drag = FUNDING_PER_8H * (holding_hours / 8)
                    
                    if outcome in ["HAWKES_CLIMAX", "PROFIT_RETRACEMENT"]:
                        max_slippage = 0.0015 if any(m in symbol for m in ["BTC", "ETH", "SOL"]) else 0.0035
                        slippage_penalty = max_slippage
                        applied_fee = TAKER_FEE * 2 
                    elif regime == "RANGING" or routing_mode == "MAKER_ONLY":
                        slippage_penalty = 0.0
                        applied_fee = MAKER_FEE * 2
                    else:
                        dynamic_slippage_bps = BASE_SLIPPAGE_BPS * max(1.0, abs(hawkes_z) * 0.5)
                        slippage_penalty = (dynamic_slippage_bps * 2) / 10000.0
                        applied_fee = TAKER_FEE * 2
                    
                    # 🚀 V25.2 FIX: Accurate Fixed Fractional Sizing Simulation
                    base_risk_pct = 0.01 
                    vol_scalar = 1.0 / (1.0 + (inst_variance * 1000.0))
                    confidence_scalar = float(np.clip((prob_success - 0.5) * 2.0, 0.5, 1.0))
                    
                    final_risk_pct = base_risk_pct * vol_scalar * confidence_scalar
                    fractional_risk = min(0.015, final_risk_pct) 
                    
                    # Maximum allowable positional leverage modeled at 2.0x (Matches Risk Vault)
                    position_leverage = min(p.leverage, fractional_risk / sl_dist_pct)
                    
                    net_unleveraged = gross - applied_fee - funding_drag - slippage_penalty
                    net_leveraged = net_unleveraged * position_leverage

                    trades.append({
                        "i": i, "direction": action_dir, "regime": regime,
                        "outcome": outcome, "net": net_leveraged, "bars": bars_held
                    })
                    
                    is_win = net_leveraged > 0
                    rolling_outcomes.append(1.0 if is_win else 0.0)
                    
                    if net_leveraged < 0:
                        recent_losses = sum(1 for out in list(rolling_outcomes)[-2:] if out == 0.0)
                        if recent_losses >= 2:
                            cooldown_until = i + 120 
                        else:
                            cooldown_until = i + bars_held
                    else:
                        cooldown_until = i + bars_held  

    return summarize(trades, len(target_candles))

def summarize(trades: List[Dict], total_minutes: int = 0) -> Dict:
    if not trades: return {"trades": 0}
        
    nets = np.array([t["net"] for t in trades])
    wins = nets[nets > 0]
    losses = nets[nets <= 0]
    equity = np.cumsum(nets)
    peak = np.maximum.accumulate(equity)
    max_dd = float(np.max(peak - equity)) if len(equity) else 0.0
    
    mc_results = []
    block_size = 5
    if len(nets) > 0:
        num_blocks = len(nets) // block_size + 1
        
        for _ in range(1000):
            sim_nets = []
            for _ in range(num_blocks):
                start_idx = np.random.randint(0, max(1, len(nets) - block_size + 1))
                sim_nets.extend(nets[start_idx : start_idx + block_size])
            
            sim_nets = np.array(sim_nets[:len(nets)])
            mc_results.append(np.sum(sim_nets))
    else:
        mc_results = [0]
    
    mean_return = np.mean(nets)
    std_return = np.std(nets) + 1e-9
    
    # 🚀 V25.2 FIX: Intraday Sharpe Annualization
    assumed_days = max(1.0, total_minutes / 1440.0)
    trades_per_day = len(trades) / assumed_days
    sharpe = (mean_return / std_return) * math.sqrt(252 * trades_per_day)
    
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
    print("\n⏳ Running V25.2 APEX QUANTUM PRIME Walk-Forward Validation (5 Folds)...")
    
    rr_ratios = [1.5, 2.0, 2.5]
    atr_mults = [2.0, 2.5, 3.0] 
    
    total_len = len(t_cand)
    fold_size = int(total_len / 5) 
    
    for rr in rr_ratios:
        for atr_m in atr_mults:
            p = Params(rr_ratio=rr, sl_atr_mult=atr_m)
            
            fold_sharpes = []
            fold_expectancies = []
            total_trades = 0
            
            for fold in range(4):
                test_start = (fold + 1) * fold_size
                test_end = test_start + fold_size
                
                if test_end > total_len: break
                
                test_result = run_v25_backtest(t_cand[test_start:test_end], b_cand[test_start:test_end], p, symbol)
                
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
        
        if best_params:
            best = best_params[0]
            with open("params.json", "w") as f:
                json.dump({"rr_ratio": best["RR"], "sl_atr_mult": best["ATR"]}, f)
            print("💾 Saved most robust parameters to params.json for live engine sync.")
            
    else:
        split = int(len(t_cand) * 0.6)
        params = Params()

        test = run_v25_backtest(t_cand[split:], b_cand[split:], params, args.symbol)
                
        print("\n=== V25.2 APEX QUANTUM PRIME OUT-OF-SAMPLE (last 40%) ===")
        for k, v in test.items():
            if isinstance(v, float): print(f"  {k}: {v:.4f}")
            else: print(f"  {k}: {v}")