"""
V42.0 APEX TITAN: HIGH-FIDELITY NEURAL BACKTESTER (25D FULL MANIFOLD)
--------------------------------------------------------------------------------
Institutional-grade historical simulation engine replicating the V42.0
25D Volterra-Riemannian Manifold, exact Joseph-stabilized RLS, Bayesian-prior
Merton Jump Kelly allocation, and CAMB (Continuous Adaptive Microstructure Barrier).
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
from typing import List, Dict, Tuple, Optional, Any

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"
TAKER_FEE = 0.00055          # 5.5 bps
MAKER_FEE = 0.00020          # 2.0 bps
FUNDING_PER_8H = 0.0001      # 1.0 bps per epoch
BASE_SLIPPAGE_BPS = 4.0      # Baseline market impact


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
            return 0.000015
        return 0.000030


class ClusterWarmStartRLS:
    """Provides prior weight matrices anchored to asset volatility tiers across the 25D manifold."""
    @staticmethod
    def get_cluster_priors(symbol: str, dim: int = 25) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, float]:
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

        # Feature Index Legend (25D Full Volterra Manifold):
        # 0: MLOFI_Z, 1: Hawkes_Z, 2: Meso_Momentum_Z, 3: Sector_Impulse,
        # 4: Micro_Elasticity, 5: OU_Divergence, 6: CFI_Z, 7: Jump_Z, 8: Shannon_Entropy,
        # 9: SWD_Z, 10: Accel_Z, 11: Hurst_Dev, 12: P_Bid_Deplete, 13: CVD_Z, 14: Div_Z,
        # 15: Eco_Alpha, 16: Funding_Bias, 17: Squeeze_Risk, 18: Micro_Dislocation,
        # 19: Hurst x Hawkes, 20: Squeeze x MLOFI, 21: Eco x MLOFI, 22: CVD x Momentum,
        # 23: OU x Hawkes, 24: Affine Bias

        # 1. TREND REGIME: Flow alignment, Hawkes intensity, momentum, and CVD
        w_trend[0] = 0.55   # MLOFI
        w_trend[1] = 0.45   # Hawkes cascade
        w_trend[2] = 0.60   # Meso momentum
        w_trend[3] = 0.40   # Sector impulse
        w_trend[10] = 0.30  # Hawkes acceleration
        w_trend[13] = 0.40  # CVD Z
        w_trend[14] = 0.35  # CVD Divergence
        w_trend[19] = 0.25  # Hurst x Hawkes
        w_trend[22] = 0.30  # CVD x Momentum
        w_trend[24] = 0.05  # Intercept bias

        # 2. RANGE REGIME: Fades price stretch and dislocation; flow neutralized
        w_range[0] = 0.00   # Flow neutralized
        w_range[5] = -0.70  # Strong OU reversion
        w_range[12] = 0.40  # Bid depletion
        w_range[18] = -0.50 # Fade micro-dislocation
        w_range[23] = -0.35 # OU x Hawkes
        w_range[24] = 0.00

        # 3. SPOOF REGIME: Defensive against sweeps; prioritizes iceberg absorption
        w_spoof[0] = -0.60  # Toxic flow rejection
        w_spoof[6] = -0.75  # Fleeting order imbalance (CFI)
        w_spoof[9] = 0.50   # Iceberg absorption (SWD)
        w_spoof[18] = -0.40
        w_spoof[24] = 0.00

        # 4. CASCADE REGIME: Directional execution on liquidation cascades
        w_cascade[0] = 0.70 # Order flow push
        w_cascade[1] = 0.85 # Extreme Hawkes surge
        w_cascade[7] = 0.50 # Volatility Jump Z
        w_cascade[10] = 0.60
        w_cascade[13] = 0.65
        w_cascade[20] = 0.45
        w_cascade[24] = 0.00

        return w_trend, w_range, w_spoof, w_cascade, p_scale


def compute_permutation_entropy_dithered(series: list, order: int = 3, delay: int = 1) -> float:
    """Shannon Permutation Entropy with micro-dither to eliminate flatline tie-rank collapse."""
    if len(series) < (order * delay):
        return 1.0
    try:
        arr = np.asarray(series, dtype=np.float64)
        tie_breaker = np.sin(np.arange(len(arr))) * 1e-11
        arr_jittered = arr + tie_breaker

        shape = (arr_jittered.size - (order - 1) * delay, order)
        strides = (arr_jittered.strides[0], arr_jittered.strides[0] * delay)
        sub_vectors = np.lib.stride_tricks.as_strided(arr_jittered, shape=shape, strides=strides)
        perms = np.argsort(sub_vectors, axis=1)

        bases = order ** np.arange(order)
        hashed = np.sum(perms * bases, axis=1)
        _, counts = np.unique(hashed, return_counts=True)
        p = counts / counts.sum()
        p = p[p > 0]

        entropy = -np.sum(p * np.log2(p))
        max_entropy = math.log2(math.factorial(order))
        return float(np.clip(entropy / max_entropy, 0.0, 1.0))
    except Exception:
        return 1.0


class BacktestHurstEstimator:
    """O(1) Analytical Hurst Exponent Estimator matching live production."""
    def __init__(self, lags: Tuple[int, ...] = (1, 2, 4, 8, 16)):
        self.lags = np.array(lags, dtype=np.float64)
        self.prices = deque(maxlen=int(max(lags)) + 2)
        self.means = {lag: 0.0 for lag in lags}
        self.m2 = {lag: 1e-9 for lag in lags}
        self.counts = {lag: 0 for lag in lags}

        self.x_vals = np.log(self.lags)
        self.x_mean = float(np.mean(self.x_vals))
        self.x_diff = self.x_vals - self.x_mean
        self.ss_x = float(np.sum(self.x_diff ** 2) + 1e-9)
        self.hurst_h = 0.5

    def update(self, price: float) -> float:
        self.prices.append(price)
        if len(self.prices) < int(self.lags[-1]) + 1:
            return 0.5

        variances = []
        for lag in self.lags:
            lag_int = int(lag)
            ret = math.log(max(1e-9, price) / max(1e-9, self.prices[-1 - lag_int]))
            self.counts[lag_int] += 1
            delta = ret - self.means[lag_int]
            self.means[lag_int] += delta / min(100, self.counts[lag_int])
            delta2 = ret - self.means[lag_int]
            self.m2[lag_int] = 0.98 * self.m2[lag_int] + 0.02 * (delta * delta2)
            variances.append(max(1e-12, self.m2[lag_int]))

        y_vals = np.log(variances)
        slope = float(np.sum(self.x_diff * (y_vals - np.mean(y_vals))) / self.ss_x)
        self.hurst_h = float(np.clip(slope / 2.0, 0.01, 0.99))
        return self.hurst_h


class BacktestAdaptiveWhitener:
    """19D Streaming Regularized Whitening Engine with Tikhonov Ridge Stability."""
    def __init__(self, dim: int = 19, base_alpha: float = 0.001):
        self.dim = dim
        self.base_alpha = base_alpha
        self.mean = np.zeros(dim, dtype=np.float64)
        self.cov = np.eye(dim, dtype=np.float64) * 0.1
        self.eye = np.eye(dim, dtype=np.float64)
        self.baseline_var = 1e-6
        self.cached_zca_matrix = np.eye(dim, dtype=np.float64)
        self.ticks_since_eigen = 0
        self.last_eigen_var = 1e-6

    def orthogonalize(self, raw_vec: np.ndarray, inst_variance: float) -> np.ndarray:
        self.baseline_var = 0.99 * self.baseline_var + 0.01 * max(1e-9, inst_variance)
        norm_v = (inst_variance - self.baseline_var) / (self.baseline_var + 1e-9)
        alpha = float(np.clip(self.base_alpha * (1.0 + np.tanh(norm_v)), 0.0005, 0.008))

        delta = raw_vec - self.mean
        self.mean += alpha * delta
        self.cov = (1.0 - alpha) * self.cov + alpha * np.outer(delta, delta)
        self.cov = 0.5 * (self.cov + self.cov.T)

        self.ticks_since_eigen += 1
        var_shift = abs(inst_variance - self.last_eigen_var) / (self.last_eigen_var + 1e-9)

        if self.ticks_since_eigen >= 20 or var_shift > 0.10:
            tr = np.trace(self.cov)
            # Tikhonov Ridge Regularization matching micro_models.py
            tikhonov_ridge = max(1e-4, (tr / self.dim) * 0.01)
            stable_cov = self.cov + (self.eye * tikhonov_ridge)

            try:
                evals, evecs = np.linalg.eigh(stable_cov)
                evals_stabilized = np.maximum(evals, tikhonov_ridge)
                inv_sqrt = 1.0 / np.sqrt(evals_stabilized)
                self.cached_zca_matrix = evecs @ np.diag(inv_sqrt) @ evecs.T
                self.ticks_since_eigen = 0
                self.last_eigen_var = inst_variance
            except Exception:
                diag_stds = np.sqrt(np.maximum(1e-8, np.diag(stable_cov)))
                self.cached_zca_matrix = np.diag(1.0 / (diag_stds + 1e-9))

        # P0 RESOLUTION: Preserves 1-sigma scale without distortive dividers
        whitened = self.cached_zca_matrix @ delta
        return np.clip(whitened, -3.0, 3.0)


class BacktestRiemannianRLS:
    """25D Joseph-Stabilized RLS with Bounded Observation Noise & L1 Sparsity."""
    def __init__(self, dim: int = 25, p_init: float = 1.0, l1_penalty: float = 1e-4):
        self.dim = dim
        self.w = np.zeros(dim, dtype=np.float64)
        self.f_inv = np.eye(dim, dtype=np.float64) * p_init
        self.eye = np.eye(dim, dtype=np.float64)
        self.l1_penalty = l1_penalty
        self.lambda_reg = 0.9995

    def update(self, x: np.ndarray, y_target: float, p_pred: float, weight: float = 1.0) -> float:
        err = float(y_target - p_pred)
        x_vec = x.reshape(-1, 1)

        p_clamped = float(np.clip(p_pred, 0.01, 0.99))
        fisher_var = max(1e-4, p_clamped * (1.0 - p_clamped))

        fx = self.f_inv @ x_vec
        denom = self.lambda_reg + float(x_vec.T @ fx) * fisher_var
        if denom < 1e-9:
            return err

        kalman_gain = (fx * fisher_var) / denom
        w_temp = self.w + (kalman_gain.flatten() * err * weight)

        # Proximal L1 Soft-Thresholding
        self.w = np.sign(w_temp) * np.maximum(np.abs(w_temp) - self.l1_penalty, 0.0)

        # Joseph-form covariance update with bounded observation noise
        i_kx = self.eye - (kalman_gain @ x_vec.T)
        bounded_r = min(1000.0, 1.0 / fisher_var)
        noise_cov = (kalman_gain @ kalman_gain.T) * bounded_r

        self.f_inv = (i_kx @ self.f_inv @ i_kx.T + noise_cov) / self.lambda_reg
        self.f_inv = 0.5 * (self.f_inv + self.f_inv.T)

        np.fill_diagonal(self.f_inv, np.maximum(np.diag(self.f_inv), 1e-5))
        tr = float(np.trace(self.f_inv))
        if tr > 1500.0:
            self.f_inv *= (1500.0 / tr)

        w_norm = float(np.linalg.norm(self.w))
        if w_norm > 50.0:
            self.w *= (50.0 / w_norm)

        return err


class QuantumMarkovRegimeDetector:
    """4-State Hidden Markov Model tracking Trend, Range, Spoof, and Cascade regimes."""
    def __init__(self):
        self.beliefs = np.array([0.25, 0.25, 0.25, 0.25], dtype=np.float64)
        self.tpm = np.array([
            [0.94, 0.02, 0.02, 0.02],
            [0.02, 0.94, 0.02, 0.02],
            [0.05, 0.05, 0.85, 0.05],
            [0.05, 0.05, 0.05, 0.85]
        ], dtype=np.float64)

    def update_beliefs(self, er: float, entropy: float, fleeting: float, jump_z: float) -> np.ndarray:
        prior = self.tpm.T @ self.beliefs
        l_trend = math.exp(-2.0 * ((1.0 - er) ** 2) - 1.5 * (entropy ** 2) - 3.0 * (fleeting ** 2))
        l_range = math.exp(-2.5 * (er ** 2) - 1.5 * ((1.0 - entropy) ** 2) - 2.0 * (fleeting ** 2))
        l_spoof = math.exp(-3.0 * ((1.0 - fleeting) ** 2) - 1.0 * (er ** 2))
        l_cascade = math.exp(-1.0 * ((3.0 - min(3.0, abs(jump_z))) ** 2))

        likelihoods = np.array([l_trend, l_range, l_spoof, l_cascade], dtype=np.float64) + 1e-6
        unnormalized = prior * likelihoods
        self.beliefs = unnormalized / (np.sum(unnormalized) + 1e-9)
        return self.beliefs


class BacktestMertonJumpKelly:
    """Continuous-Time Merton Jump Kelly Sizer matching live micro_models.py specifications."""
    def __init__(self, prior_win_rate: float = 0.54, prior_payoff: float = 1.20, prior_weight: float = 10.0):
        self.prior_w = prior_weight
        self.wins_accum = prior_win_rate * prior_weight
        self.trials_accum = prior_weight
        self.win_return_sum = prior_payoff * (prior_weight * 0.5)
        self.win_return_count = prior_weight * 0.5
        self.loss_return_sum = 1.0 * (prior_weight * 0.5)
        self.loss_return_count = prior_weight * 0.5

        self.win_rate = prior_win_rate
        self.avg_win = prior_payoff
        self.avg_loss = 1.0

    def update(self, net_pnl: float, return_pct: float):
        ret_mag = max(1e-4, abs(return_pct))
        self.trials_accum += 1.0
        if net_pnl > 0:
            self.wins_accum += 1.0
            self.win_return_sum += ret_mag
            self.win_return_count += 1.0
        else:
            self.loss_return_sum += ret_mag
            self.loss_return_count += 1.0

        self.win_rate = self.wins_accum / self.trials_accum
        self.avg_win = self.win_return_sum / self.win_return_count
        self.avg_loss = self.loss_return_sum / self.loss_return_count

    def compute(self, inst_variance: float, hawkes_intensity: float) -> float:
        b = self.avg_win / max(1e-6, self.avg_loss)
        p = self.win_rate
        q = 1.0 - p

        raw_kelly = (b * p - q) / b if b > 0.0 else 0.0
        abs_h = abs(hawkes_intensity)
        jump_penalty = (abs_h * 0.003) + (max(0.0, abs_h - 1.8) ** 2) * 0.015
        variance_dampener = 1.0 / (1.0 + inst_variance * 600.0)
        f_star = (raw_kelly * variance_dampener) - jump_penalty

        if f_star <= 0.001:
            return 0.0
        return float(np.clip(f_star * 0.125, 0.001, 0.0075))  # Exact Eighth-Kelly parity


def fetch_klines_1m(symbol: str, days: int) -> List[Dict]:
    """Fetches high-resolution 1-minute OHLCV candles from Bybit Linear V5."""
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
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit API error: {payload.get('retMsg')}")
        batch = payload.get("result", {}).get("list", [])
        if not batch:
            break

        for k in batch:
            out.append({
                "ts": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5])
            })

        end = int(batch[-1][0]) - 1
        time.sleep(0.15)

    out.sort(key=lambda c: c["ts"])
    return out[-target:]


def fetch_aligned_data(symbol: str, days: int) -> Tuple[List[Dict], List[Dict]]:
    print(f"  Fetching target asset 1-Minute Data ({symbol})...")
    target_candles = fetch_klines_1m(symbol, days)
    if symbol == "BTCUSDT":
        return target_candles, target_candles

    print("  Fetching global BTC lead-lag matrix context...")
    btc_raw = fetch_klines_1m("BTCUSDT", days)
    btc_dict = {c["ts"]: c for c in btc_raw}
    aligned_btc = []

    for c in target_candles:
        if c["ts"] in btc_dict:
            aligned_btc.append(btc_dict[c["ts"]])
        else:
            aligned_btc.append({
                "ts": c["ts"], "open": c["close"], "high": c["close"],
                "low": c["close"], "close": c["close"], "volume": 0.0
            })

    return target_candles, aligned_btc


@dataclass
class Params:
    rr_ratio: float = 2.0
    sl_atr_mult: float = 2.5
    atr_period: int = 14
    leverage: float = 2.0


def compute_lead_lag_cross_alpha(btc_hist: deque, alt_hist: deque) -> float:
    """Rolling log-return cross correlation between BTC and Altcoin."""
    if len(btc_hist) < 30 or len(alt_hist) < 30:
        return 0.0
    aligned_b, aligned_a = [], []

    for i in range(2, len(alt_hist)):
        try:
            a_ret = math.log(alt_hist[i] / (alt_hist[i - 1] + 1e-9))
            b_ret = math.log(btc_hist[i - 1] / (btc_hist[i - 2] + 1e-9))
            aligned_a.append(a_ret)
            aligned_b.append(b_ret)
        except ValueError:
            continue

    if len(aligned_a) < 20:
        return 0.0

    try:
        with np.errstate(divide="ignore", invalid="ignore"):
            correlation = float(np.corrcoef(aligned_b, aligned_a)[0, 1])
        if np.isnan(correlation):
            return 0.0
    except Exception:
        return 0.0

    btc_momentum = float(np.mean(aligned_b[-10:]))
    if abs(btc_momentum) > 0.00015 and correlation > 0.40:
        return float(math.copysign(min(1.0, abs(correlation)), btc_momentum))
    return 0.0


def run_v40_backtest(
    target_candles: List[Dict],
    btc_candles: List[Dict],
    p: Params,
    symbol: str,
    initial_rls_state: Optional[Dict[str, Any]] = None,
    freeze_weights: bool = False
) -> Tuple[Dict, Dict[str, Any]]:
    trades = []
    cooldown_until = -1

    # Order flow and Hawkes intensity state
    hawkes_mean, hawkes_var, hawkes_z = 0.0, 1.0, 0.0
    hawkes_velocity, hawkes_acceleration = 0.0, 0.0
    hawkes_z_prev, hawkes_v_prev = 0.0, 0.0
    mlofi_mean, mlofi_var, mlofi_z = 0.0, 1.0, 0.0

    # Moving equilibrium filters
    meso_fast_ema = None
    meso_slow_ema = None
    meso_momentum_z = 0.0

    # Volume & CVD state
    vol_ewma = 0.0
    cvd_accum = 0.0
    cvd_mean, cvd_var = 0.0, 1.0
    cvd_history = deque(maxlen=60)
    price_history = deque(maxlen=60)

    # Microstructure moments & estimators
    amihud_history = deque(maxlen=100)
    rolling_outcomes = deque(maxlen=100)
    btc_1m_history = deque(maxlen=300)
    alt_1m_history = deque(maxlen=300)
    entropy_history = deque(maxlen=200)
    log_returns = deque(maxlen=500)
    inst_variance = 1e-6
    kaufman_er = 0.5

    hurst_estimator = BacktestHurstEstimator()

    # V42.0 Full 25D RLS and Feature Engines
    w_t, w_r, w_s, w_c, p_scale = ClusterWarmStartRLS.get_cluster_priors(symbol, dim=25)
    whitening_engine = BacktestAdaptiveWhitener(dim=19, base_alpha=0.001)

    rls_trend = BacktestRiemannianRLS(dim=25, p_init=p_scale)
    rls_range = BacktestRiemannianRLS(dim=25, p_init=p_scale)
    rls_spoof = BacktestRiemannianRLS(dim=25, p_init=p_scale)
    rls_cascade = BacktestRiemannianRLS(dim=25, p_init=p_scale)

    if initial_rls_state:
        rls_trend.w = initial_rls_state["w_trend"].copy()
        rls_range.w = initial_rls_state["w_range"].copy()
        rls_spoof.w = initial_rls_state["w_spoof"].copy()
        rls_cascade.w = initial_rls_state["w_cascade"].copy()
        rls_trend.f_inv = initial_rls_state["f_trend"].copy()
        rls_range.f_inv = initial_rls_state["f_range"].copy()
        rls_spoof.f_inv = initial_rls_state["f_spoof"].copy()
        rls_cascade.f_inv = initial_rls_state["f_cascade"].copy()
    else:
        rls_trend.w = w_t.copy()
        rls_range.w = w_r.copy()
        rls_spoof.w = w_s.copy()
        rls_cascade.w = w_c.copy()

    regime_detector = QuantumMarkovRegimeDetector()
    kelly_sizer = BacktestMertonJumpKelly(prior_win_rate=0.54, prior_payoff=1.20, prior_weight=10.0)

    prediction_buffer = deque()
    historical_probs = deque(maxlen=2000)
    calibration_errors = deque(maxlen=300)

    rolling_notional_volume = 0.0
    amihud_anchor_price = 0.0

    if "BTC" in symbol:
        amihud_threshold = 2_500_000.0
    elif "ETH" in symbol or "SOL" in symbol:
        amihud_threshold = 1_000_000.0
    else:
        amihud_threshold = 250_000.0

    for i in range(101, len(target_candles)):
        c_prev = target_candles[i - 1]
        c_prev_prev = target_candles[i - 2]
        c = target_candles[i]
        now_ts = c["ts"]
        sim_price = c["open"]

        btc_1m_history.append(btc_candles[i - 1]["close"])
        alt_1m_history.append(c_prev["close"])

        safe_curr_prev = max(1e-9, c_prev["close"])
        safe_prev_prev = max(1e-9, c_prev_prev["close"])
        ret_prev = math.log(safe_curr_prev / safe_prev_prev)
        log_returns.append(ret_prev)

        hurst_h = hurst_estimator.update(c_prev["close"])

        shannon_entropy = 1.0
        if len(log_returns) > 10:
            inst_variance = np.var(list(log_returns)[-10:]) + 1e-9
            shannon_entropy = compute_permutation_entropy_dithered(list(log_returns)[-20:])
            entropy_history.append(shannon_entropy)

        # Meso-momentum tracking
        a_fast = 2.0 / 51.0
        a_slow = 2.0 / 301.0
        if meso_fast_ema is None:
            meso_fast_ema, meso_slow_ema = sim_price, sim_price
        else:
            meso_fast_ema = (sim_price - meso_fast_ema) * a_fast + meso_fast_ema
            meso_slow_ema = (sim_price - meso_slow_ema) * a_slow + meso_slow_ema
        meso_momentum_z = ((meso_fast_ema - meso_slow_ema) / (meso_slow_ema + 1e-9)) / (math.sqrt(inst_variance) + 1e-9)

        # Kaufman Efficiency Ratio (ER)
        closes_slice = np.array([cx["close"] for cx in target_candles[max(0, i - 101):i]])
        if len(closes_slice) >= 20:
            directional_change = abs(closes_slice[-1] - closes_slice[0])
            absolute_changes = np.sum(np.abs(np.diff(closes_slice)))
            kaufman_er = float(directional_change / (absolute_changes + 1e-9))
        else:
            kaufman_er = 0.5

        # Hawkes Intensity Cascade
        vol_step = c_prev["volume"]
        vol_ewma = (1 - 0.05) * vol_ewma + 0.05 * vol_step if vol_ewma > 0 else vol_step
        norm_vol = vol_step / (vol_ewma + 1e-9)
        price_step = c_prev["close"] - c_prev_prev["close"]

        volume_signed = np.sign(price_step) * math.log1p(max(0.0, norm_vol))
        alpha_fast = np.clip(0.05 + (kaufman_er * 0.15), 0.05, 0.35)
        alpha_slow = alpha_fast / 5.0

        hawkes_mean = (1 - alpha_fast) * hawkes_mean + alpha_fast * volume_signed
        hawkes_var = (1 - alpha_slow) * hawkes_var + alpha_slow * ((volume_signed - hawkes_mean) ** 2)
        hawkes_z = (volume_signed - hawkes_mean) / (math.sqrt(hawkes_var) + 1e-9)

        hawkes_velocity = hawkes_z - hawkes_z_prev
        hawkes_acceleration = hawkes_velocity - hawkes_v_prev
        hawkes_z_prev, hawkes_v_prev = hawkes_z, hawkes_velocity

        # Level-5 MLOFI flow approximation
        candle_range = max(1e-9, c_prev["high"] - c_prev["low"])
        intra_flow_raw = ((c_prev["close"] - c_prev["open"]) + 0.5 * (c_prev["close"] - (c_prev["high"] + c_prev["low"]) * 0.5)) / candle_range
        candle_mlofi_step = intra_flow_raw * math.log1p(max(0.0, norm_vol))
        d_mlofi = candle_mlofi_step - mlofi_mean
        mlofi_mean += 0.05 * d_mlofi
        mlofi_var = (1.0 - 0.05) * mlofi_var + 0.05 * (d_mlofi ** 2)
        mlofi_z = float(np.clip((candle_mlofi_step - mlofi_mean) / (math.sqrt(mlofi_var) + 1e-9), -5.0, 5.0))

        # CVD divergence calculation
        trade_dir = 1.0 if price_step >= 0 else -1.0
        cvd_delta = vol_step * trade_dir
        cvd_accum += cvd_delta
        cvd_history.append(cvd_accum)
        price_history.append(c_prev["close"])

        d_stat = cvd_accum - cvd_mean
        cvd_mean += 0.02 * d_stat
        cvd_var = (1.0 - 0.02) * cvd_var + 0.02 * (d_stat ** 2)
        cvd_z = float(np.clip((cvd_accum - cvd_mean) / (math.sqrt(cvd_var) + 1e-9), -5.0, 5.0))

        div_z = 0.0
        if len(cvd_history) >= 20:
            p_diff = price_history[-1] - price_history[0]
            c_diff = cvd_history[-1] - cvd_history[0]
            if c_diff < 0 and p_diff >= 0:
                div_z = abs(c_diff) / (np.std(list(cvd_history)) + 1e-9)
            elif c_diff > 0 and p_diff <= 0:
                div_z = -abs(c_diff) / (np.std(list(cvd_history)) + 1e-9)

        # 19D Raw Microstructure State Vector
        sector_impulse = compute_lead_lag_cross_alpha(btc_1m_history, alt_1m_history)
        micro_elasticity_z = (c_prev["high"] - c_prev["low"]) / (sim_price * math.sqrt(inst_variance) + 1e-9)
        ou_divergence_z = (sim_price - meso_slow_ema) / (sim_price * math.sqrt(inst_variance) + 1e-9)
        cfi_z = 0.0
        jump_z = abs(price_step) / (sim_price * math.sqrt(inst_variance) + 1e-9)
        swd_z = hawkes_z - (price_step / (sim_price * math.sqrt(inst_variance) + 1e-9))
        accel_z = hawkes_acceleration
        hurst_h_diff = hurst_h - 0.5
        p_bid_deplete = (c_prev["close"] - c_prev["low"]) / candle_range
        ecosystem_alpha = sector_impulse * 0.8
        funding_bias = 0.0
        squeeze_risk = float(np.clip(abs(hawkes_z) * kaufman_er * 0.5, 0.0, 1.0))
        typical_price = (c_prev["high"] + c_prev["low"] + c_prev["close"]) / 3.0
        micro_dislocation_z = (typical_price - sim_price) / (sim_price * math.sqrt(inst_variance) + 1e-9)

        raw_vec_19 = np.array([
            mlofi_z, hawkes_z, meso_momentum_z, sector_impulse,
            micro_elasticity_z, ou_divergence_z, cfi_z, jump_z,
            shannon_entropy, swd_z, accel_z, hurst_h_diff,
            p_bid_deplete, cvd_z, div_z, ecosystem_alpha,
            funding_bias, squeeze_risk, micro_dislocation_z
        ], dtype=np.float64)

        f = whitening_engine.orthogonalize(raw_vec_19, inst_variance)

        # 25D Full Volterra Bilinear Interaction Manifold
        volterra = np.empty(25, dtype=np.float64)
        volterra[:19] = f
        volterra[19] = f[11] * f[1]  # 19: Hurst x Hawkes interaction
        volterra[20] = f[17] * f[0]  # 20: Squeeze Risk x MLOFI
        volterra[21] = f[15] * f[0]  # 21: Macro Spillover x MLOFI
        volterra[22] = f[14] * f[2]  # 22: CVD Divergence x Meso Momentum
        volterra[23] = f[5] * f[1]   # 23: OU Mean Reversion x Hawkes

        # P0 RESOLUTION: RMS Amplitude Scaling matching micro_models.py
        rms_scale = math.sqrt(float(np.mean(volterra[:24] ** 2)) + 1e-9)
        v_att = np.empty(25, dtype=np.float64)
        v_att[:24] = np.clip(volterra[:24] / max(1.0, rms_scale), -3.0, 3.0)
        v_att[24] = 1.0  # Invariant Affine Intercept Bias

        # Bayesian Markov Regime Updates
        beliefs = regime_detector.update_beliefs(kaufman_er, shannon_entropy, 0.0, jump_z)
        p_t, p_r, p_s, p_c = beliefs

        l_t = float(np.dot(rls_trend.w, v_att))
        l_r = float(np.dot(rls_range.w, v_att))
        l_s = float(np.dot(rls_spoof.w, v_att))
        l_c = float(np.dot(rls_cascade.w, v_att))

        tau = 0.80
        exp_weights = np.exp((beliefs - np.max(beliefs)) / tau)
        gate_weights = exp_weights / (np.sum(exp_weights) + 1e-9)

        regime_logits = np.array([l_t, l_r, l_s, l_c], dtype=np.float64)
        raw_score = float(np.dot(gate_weights, regime_logits))

        # Calibrated Logit Gain
        LOGIT_GAIN = 3.5
        logit = float(np.clip(raw_score * LOGIT_GAIN, -5.0, 5.0))
        p_up = 1.0 / (1.0 + math.exp(-logit))
        p_down = 1.0 - p_up

        prob_success = max(p_up, p_down)
        action_dir = "BUY" if p_up > p_down else "SELL"

        # Adverse Order Flow Selection Veto
        has_iceberg_absorption = swd_z > 2.0
        if action_dir == "BUY" and mlofi_z < -1.75 and not has_iceberg_absorption:
            prob_success = 0.50
            action_dir = "HOLD"
        elif action_dir == "SELL" and mlofi_z > 1.75 and not has_iceberg_absorption:
            prob_success = 0.50
            action_dir = "HOLD"

        historical_probs.append(prob_success)

        # Split-Conformal Prediction Coverage Gate (85% Coverage)
        if len(calibration_errors) >= 30:
            q_threshold = float(np.percentile(calibration_errors, 85))
        else:
            q_threshold = 0.08
        dynamic_gate = float(np.clip(0.51 + (q_threshold * 0.25), 0.52, 0.65))

        # Dynamic ATR Volatility Brackets
        vol_sigma = math.sqrt(inst_variance) * math.sqrt(60.0)
        atr_proxy = vol_sigma * sim_price
        sl_distance = max(atr_proxy * p.sl_atr_mult, sim_price * 0.015)
        sl_dist_pct = sl_distance / sim_price

        dynamic_rr_ratio = float(np.clip(p.rr_ratio + (1.5 * (kaufman_er ** 2)), 1.2, 3.5))
        tp_dist_pct = sl_dist_pct * dynamic_rr_ratio

        virt_sl = sim_price - (sl_dist_pct * sim_price) if action_dir == "BUY" else sim_price + (sl_dist_pct * sim_price)
        virt_tp = sim_price + (tp_dist_pct * sim_price) if action_dir == "BUY" else sim_price - (tp_dist_pct * sim_price)

        # Replay Online Learning Buffer with Full Decoupled Updates
        while prediction_buffer and (now_ts - prediction_buffer[0][0]) >= 60000:
            _, old_price, old_features, old_p_up, old_virt_sl, old_virt_tp, old_action_dir, old_beliefs = prediction_buffer.popleft()
            if sim_price != old_price and old_price > 0:
                sl_breached = (old_action_dir == "BUY" and sim_price <= old_virt_sl) or \
                              (old_action_dir == "SELL" and sim_price >= old_virt_sl)
                tp_reached = (old_action_dir == "BUY" and sim_price >= old_virt_tp) or \
                             (old_action_dir == "SELL" and sim_price <= old_virt_tp)

                if sl_breached:
                    y_target = 0.0 if old_p_up > 0.5 else 1.0
                elif tp_reached:
                    y_target = 1.0 if old_p_up > 0.5 else 0.0
                else:
                    y_target = 1.0 if sim_price > old_price else 0.0

                calibration_errors.append(abs(y_target - old_p_up))

                if not freeze_weights:
                    p_tr = 1.0 / (1.0 + math.exp(-float(np.clip(np.dot(rls_trend.w, old_features) * LOGIT_GAIN, -5.0, 5.0))))
                    p_ra = 1.0 / (1.0 + math.exp(-float(np.clip(np.dot(rls_range.w, old_features) * LOGIT_GAIN, -5.0, 5.0))))
                    p_sp = 1.0 / (1.0 + math.exp(-float(np.clip(np.dot(rls_spoof.w, old_features) * LOGIT_GAIN, -5.0, 5.0))))
                    p_ca = 1.0 / (1.0 + math.exp(-float(np.clip(np.dot(rls_cascade.w, old_features) * LOGIT_GAIN, -5.0, 5.0))))

                    rls_trend.update(old_features, y_target, p_tr, weight=old_beliefs[0])
                    rls_range.update(old_features, y_target, p_ra, weight=old_beliefs[1])
                    rls_spoof.update(old_features, y_target, p_sp, weight=old_beliefs[2])
                    rls_cascade.update(old_features, y_target, p_ca, weight=old_beliefs[3])

        prediction_buffer.append((now_ts, sim_price, v_att, p_up, virt_sl, virt_tp, action_dir, beliefs))

        # Amihud Illiquidity Vacuum Filter
        notional_vol = c_prev["volume"] * c_prev["close"]
        rolling_notional_volume += notional_vol
        if amihud_anchor_price == 0.0:
            amihud_anchor_price = c_prev["close"]

        if rolling_notional_volume >= amihud_threshold:
            amihud_history.append(abs(math.log(c_prev["close"] / (amihud_anchor_price + 1e-9))) / rolling_notional_volume)
            rolling_notional_volume, amihud_anchor_price = 0.0, c_prev["close"]

        if i > cooldown_until and i > 150:
            vacuum_blocked = len(amihud_history) >= 10 and amihud_history[-1] > (np.mean(list(amihud_history)[-10:]) * 4.0)
            p_win = np.mean(rolling_outcomes) if len(rolling_outcomes) >= 10 else 0.50

            routing_mode = "STANDARD"
            regime = "TRENDING" if p_t > 0.5 else "RANGING"

            spread_cost = max(0.0001, min(0.0018, math.sqrt(inst_variance) * 0.5))
            if spread_cost > 0.0004 or vacuum_blocked or hurst_h < 0.52:
                routing_mode = "MAKER_ONLY"
                regime = "MEAN_REVERTING"
                dynamic_gate -= 0.03

            ev_floor = AdaptiveSessionClock.get_ev_floor(routing_mode)

            if action_dir != "HOLD" and prob_success >= max(dynamic_gate, p_win):
                fee_rate = MAKER_FEE if routing_mode == "MAKER_ONLY" else TAKER_FEE
                net_ev_pct = (prob_success * tp_dist_pct) - ((1.0 - prob_success) * sl_dist_pct) - (spread_cost * 0.5) - fee_rate

                if net_ev_pct > ev_floor:
                    entry = c["open"]
                    initial_risk = sl_dist_pct * entry
                    realigned_sl = entry - initial_risk if action_dir == "BUY" else entry + initial_risk

                    max_favorable_price = entry
                    current_sl = realigned_sl
                    locked_sl = realigned_sl
                    current_tp = entry + (tp_dist_pct * entry) if action_dir == "BUY" else entry - (tp_dist_pct * entry)

                    outcome, exit_price, bars_held = None, entry, 0
                    pnl_accum = 0.0
                    position_size = 1.0

                    # 4-Point Micro-Trajectory Intra-Bar Simulation with CAMB Parity
                    for j in range(i + 1, min(i + 240, len(target_candles))):
                        bars_held = j - i
                        bar = target_candles[j]
                        o_j, h_j, l_j, c_j = bar["open"], bar["high"], bar["low"], bar["close"]
                        
                        # Evaluate adverse price movement first
                        sub_path = [o_j, l_j, h_j, c_j] if action_dir == "BUY" else [o_j, h_j, l_j, c_j]

                        tick_break = False
                        for tick_p in sub_path:
                            if action_dir == "BUY" and tick_p > max_favorable_price:
                                max_favorable_price = tick_p
                            elif action_dir == "SELL" and tick_p < max_favorable_price:
                                max_favorable_price = tick_p

                            r_multiple = abs(max_favorable_price - entry) / (initial_risk + 1e-9)
                            current_r = (tick_p - entry) / (initial_risk + 1e-9) if action_dir == "BUY" else (entry - tick_p) / (initial_risk + 1e-9)

                            # 1. Scale-out 50% at 1.4R (Parity with IntelligentExitEngine)
                            if r_multiple >= 1.40 and position_size == 1.0:
                                partial_return = (tick_p - entry) / entry if action_dir == "BUY" else (entry - tick_p) / entry
                                pnl_accum += partial_return * 0.5
                                position_size = 0.5

                            # 2. CONTINUOUS ADAPTIVE MICROSTRUCTURE BARRIER (CAMB Parity)
                            h_clamped = min(0.70, max(0.30, hurst_h))
                            r_crit = 0.45 + (h_clamped - 0.30) * 0.75
                            round_trip_friction = (fee_rate * 2.0) + (BASE_SLIPPAGE_BPS / 10000.0)
                            friction_be_price = entry * (1.0 + round_trip_friction) if action_dir == "BUY" else entry * (1.0 - round_trip_friction)

                            if r_multiple < r_crit:
                                # Anti-Choking Phase: Hold initial risk stop
                                calculated_sl = realigned_sl
                            elif r_multiple >= r_crit and r_multiple < 1.0:
                                # Friction-Compensated Breakeven Activation
                                calculated_sl = friction_be_price
                            else:
                                # Asymptotic Parabolic Trailing Chandelier
                                decay_lambda = 0.65
                                alpha_max, alpha_min = 2.2, 0.6
                                cushion_mult = alpha_min + (alpha_max - alpha_min) * math.exp(-decay_lambda * (r_multiple - 1.0))
                                dynamic_cushion = atr_proxy * cushion_mult

                                if action_dir == "BUY":
                                    trail_stop = max_favorable_price - dynamic_cushion
                                    calculated_sl = max(friction_be_price, trail_stop)
                                else:
                                    trail_stop = max_favorable_price + dynamic_cushion
                                    calculated_sl = min(friction_be_price, trail_stop)

                            # Monotonic Ratchet Invariant
                            if action_dir == "BUY":
                                locked_sl = max(locked_sl, calculated_sl)
                            else:
                                locked_sl = min(locked_sl, calculated_sl)

                            current_sl = locked_sl

                            # Physical boundary breaches
                            hit_sl = tick_p <= current_sl if action_dir == "BUY" else tick_p >= current_sl
                            hit_tp = tick_p >= current_tp if action_dir == "BUY" else tick_p <= current_tp

                            if hit_sl and hit_tp:
                                outcome = "WIN" if r_multiple >= r_crit else "LOSS"
                                exit_price = current_sl
                                tick_break = True
                                break
                            elif hit_sl:
                                outcome = "WIN" if r_multiple >= r_crit else "LOSS"
                                exit_price = current_sl
                                tick_break = True
                                break
                            elif hit_tp:
                                outcome, exit_price = "WIN", current_tp
                                tick_break = True
                                break

                        if tick_break:
                            break

                        # Hawkes Volatility Climax Exit
                        bar_vol_norm = bar["volume"] / (vol_ewma + 1e-9)
                        hawkes_burst = math.log1p(max(0.0, bar_vol_norm)) * 1.5 * np.sign(c_j - target_candles[j - 1]["close"])
                        if r_multiple >= 0.70:
                            if (action_dir == "BUY" and hawkes_burst < -2.8) or (action_dir == "SELL" and hawkes_burst > 2.8):
                                outcome, exit_price = "HAWKES_CLIMAX", c_j
                                break

                        # Profit Retracement Locking (Gave back 25% from >= 0.80R peak)
                        if r_multiple >= 0.80:
                            retrace = (r_multiple - current_r) / (r_multiple + 1e-9)
                            if retrace >= 0.25:
                                outcome, exit_price = "PROFIT_RETRACEMENT", c_j
                                break

                    if outcome is None:
                        exit_price = target_candles[min(i + 239, len(target_candles) - 1)]["close"]
                        outcome = "TIME_EXIT"

                    gross = (exit_price - entry) / entry if action_dir == "BUY" else (entry - exit_price) / entry
                    gross = (gross * position_size) + pnl_accum

                    holding_hours = bars_held / 60.0
                    funding_drag = FUNDING_PER_8H * (holding_hours / 8.0)

                    # Power-Law Non-Linear Market Impact Model
                    bar_vol_notional = max(1000.0, c_prev["volume"] * c_prev["close"])
                    impact_bps = BASE_SLIPPAGE_BPS * (1.0 + math.sqrt(max(0.01, (position_size * entry) / bar_vol_notional)) * 1.5)

                    if routing_mode == "MAKER_ONLY":
                        applied_fee = MAKER_FEE * 2
                        slippage_penalty = 0.0
                    else:
                        applied_fee = TAKER_FEE * 2
                        slippage_penalty = (impact_bps * 2.0) / 10000.0

                    # Merton Jump Kelly Sizing (Eighth-Kelly Parity)
                    kelly_f = kelly_sizer.compute(inst_variance, hawkes_z)
                    target_risk_pct = max(0.001, min(0.0075, kelly_f))
                    position_leverage = min(p.leverage, target_risk_pct / max(sl_dist_pct, 1e-4))

                    net_unleveraged = gross - applied_fee - funding_drag - slippage_penalty
                    net_leveraged = net_unleveraged * position_leverage

                    trades.append({
                        "i": i, "direction": action_dir, "regime": regime,
                        "outcome": outcome, "net": net_leveraged, "bars": bars_held
                    })

                    is_win = net_leveraged > 0
                    rolling_outcomes.append(1.0 if is_win else 0.0)
                    kelly_sizer.update(net_leveraged, net_unleveraged)

                    if net_leveraged < 0:
                        recent_losses = sum(1 for out in list(rolling_outcomes)[-2:] if out == 0.0)
                        cooldown_until = i + 120 if recent_losses >= 2 else i + bars_held
                    else:
                        cooldown_until = i + bars_held

    final_rls_state = {
        "w_trend": rls_trend.w.copy(),
        "w_range": rls_range.w.copy(),
        "w_spoof": rls_spoof.w.copy(),
        "w_cascade": rls_cascade.w.copy(),
        "f_trend": rls_trend.f_inv.copy(),
        "f_range": rls_range.f_inv.copy(),
        "f_spoof": rls_spoof.f_inv.copy(),
        "f_cascade": rls_cascade.f_inv.copy()
    }

    return summarize(trades, len(target_candles)), final_rls_state


def summarize(trades: List[Dict], total_minutes: int = 0) -> Dict:
    if not trades:
        return {"trades": 0}

    nets = np.array([t["net"] for t in trades])
    wins = nets[nets > 0]
    losses = nets[nets <= 0]
    equity = np.cumsum(nets)
    peak = np.maximum.accumulate(equity)
    drawdowns = peak - equity
    max_dd = float(np.max(drawdowns)) if len(equity) else 0.0

    # Max Drawdown Duration (in trades)
    dd_duration = 0
    current_dd_duration = 0
    for dd in drawdowns:
        if dd > 0:
            current_dd_duration += 1
            dd_duration = max(dd_duration, current_dd_duration)
        else:
            current_dd_duration = 0

    mc_results = []
    block_size = 5
    if len(nets) > 0:
        num_blocks = len(nets) // block_size + 1
        for _ in range(1000):
            sim_nets = []
            for _ in range(num_blocks):
                start_idx = np.random.randint(0, max(1, len(nets) - block_size + 1))
                sim_nets.extend(nets[start_idx:start_idx + block_size])
            sim_nets = np.array(sim_nets[:len(nets)])
            mc_results.append(np.sum(sim_nets))
    else:
        mc_results = [0]

    mean_return = np.mean(nets)
    std_return = np.std(nets) + 1e-9

    assumed_days = max(1.0, total_minutes / 1440.0)
    trades_per_day = len(trades) / assumed_days
    sharpe = (mean_return / std_return) * math.sqrt(252 * trades_per_day)

    downside_returns = nets[nets < 0]
    downside_std = np.std(downside_returns) + 1e-9 if len(downside_returns) > 0 else 1e-9
    sortino = (mean_return / downside_std) * math.sqrt(252 * trades_per_day)

    # Tail Risk: Conditional Value-at-Risk (CVaR 95%)
    var_95 = float(np.percentile(nets, 5))
    tail_losses = nets[nets <= var_95]
    cvar_95 = float(np.mean(tail_losses)) if len(tail_losses) > 0 else var_95

    # Calmar Ratio
    annualized_return = mean_return * trades_per_day * 365.0
    calmar = (annualized_return / max_dd) if max_dd > 0 else float("inf")

    return {
        "trades": len(trades),
        "win_rate": float(len(wins) / len(trades)),
        "avg_win": float(np.mean(wins)) if len(wins) else 0.0,
        "avg_loss": float(np.mean(losses)) if len(losses) else 0.0,
        "expectancy_per_trade": float(np.mean(nets)),
        "profit_factor": float(wins.sum() / (abs(losses.sum()) + 1e-9)) if losses.sum() != 0 else float("inf"),
        "total_return_on_margin": float(equity[-1]),
        "max_drawdown_on_margin": max_dd,
        "max_drawdown_duration_trades": dd_duration,
        "sharpe_ratio": float(sharpe),
        "sortino_ratio": float(sortino),
        "calmar_ratio": float(calmar),
        "cvar_95": cvar_95,
        "monte_carlo_p_positive": float(np.mean(np.array(mc_results) > 0)),
        "by_regime": {
            r: {
                "trades": sum(1 for t in trades if t["regime"] == r),
                "win_rate": float(np.mean([1 if t["net"] > 0 else 0 for t in trades if t["regime"] == r]) or 0.0)
            }
            for r in ("TRENDING", "RANGING", "MEAN_REVERTING")
        },
    }


def parameter_sweep(t_cand: List[Dict], b_cand: List[Dict], symbol: str) -> List[Dict]:
    """
    Purged & Embargoed Walk-Forward Cross-Validation (López de Prado Methodology).
    Warm-starts and adapts on rolling train folds, purges the maximum trade horizon
    (240 minutes) plus an embargo window (60 minutes) using millisecond timestamps,
    and scores on out-of-sample test splits with strictly frozen weights.
    """
    results = []
    print("\n  Running V42.0 Purged & Embargoed Walk-Forward Cross-Validation (5 Folds)...")

    rr_ratios = [1.8, 2.0, 2.4]
    atr_mults = [2.0, 2.5, 3.0]

    total_len = len(t_cand)
    fold_size = int(total_len / 5)
    purge_ms = 240 * 60 * 1000    # 240 Minutes max holding duration
    embargo_ms = 60 * 60 * 1000   # 60 Minutes autoregressive memory clearance

    for rr in rr_ratios:
        for atr_m in atr_mults:
            p = Params(rr_ratio=rr, sl_atr_mult=atr_m)
            fold_sharpes = []
            fold_expectancies = []
            total_trades = 0

            for fold in range(4):
                train_start = 0
                train_end = (fold + 1) * fold_size
                train_end_ts = t_cand[train_end - 1]["ts"]

                # Enforce timestamp duration clearance for purge + embargo
                test_start_ts = train_end_ts + purge_ms + embargo_ms
                test_start = next((idx for idx, c in enumerate(t_cand) if c["ts"] >= test_start_ts), total_len)
                test_end = min(total_len, (fold + 2) * fold_size)

                if test_start >= test_end:
                    continue

                # 1. Warm-start & train on in-sample fold
                _, trained_state = run_v40_backtest(
                    t_cand[train_start:train_end], b_cand[train_start:train_end], p, symbol,
                    freeze_weights=False
                )

                # 2. Score out-of-sample with purged/embargoed boundary and frozen weights
                test_result, _ = run_v40_backtest(
                    t_cand[test_start:test_end], b_cand[test_start:test_end], p, symbol,
                    initial_rls_state=trained_state,
                    freeze_weights=True  # Zero weight adaptation on OOS folds
                )

                if test_result.get("trades", 0) >= 2:
                    fold_sharpes.append(test_result.get("sharpe_ratio", 0.0))
                    fold_expectancies.append(test_result.get("expectancy_per_trade", 0.0))
                    total_trades += test_result.get("trades", 0)
                else:
                    fold_sharpes.append(-1.0)

            if total_trades > 8 and len(fold_sharpes) >= 3:
                avg_sharpe = float(np.mean(fold_sharpes))
                avg_expectancy = float(np.mean(fold_expectancies))

                if min(fold_sharpes) > -0.5:
                    results.append({
                        "RR": rr, "ATR": atr_m,
                        "OOS_Avg_Sharpe": avg_sharpe,
                        "OOS_Avg_Expectancy": avg_expectancy,
                        "Total_Trades": total_trades,
                        "Min_Fold_Sharpe": float(min(fold_sharpes))
                    })

    return sorted(results, key=lambda x: x["OOS_Avg_Sharpe"], reverse=True)[:5]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--optimize", action="store_true")
    args = parser.parse_args()

    print(f"  Building matrix mapping for {args.days}d of 1-Minute High-Resolution Data...")
    t_cand, b_cand = fetch_aligned_data(args.symbol, args.days)
    print(f"  Matrix synchronized. ({len(t_cand)} true 1m blocks)")

    if args.optimize:
        best_params = parameter_sweep(t_cand, b_cand, args.symbol)
        print("\n  Top Walk-Forward Configurations (Sorted by Avg OOS Sharpe):")
        for idx, res in enumerate(best_params, 1):
            print(f" {idx}. RR: {res['RR']} | SL ATR: {res['ATR']} "
                  f"--> Avg Sharpe: {res['OOS_Avg_Sharpe']:.2f} | Min Fold Sharpe: {res['Min_Fold_Sharpe']:.2f}")

        if best_params:
            best = best_params[0]
            with open("params.json", "w") as f:
                json.dump({"rr_ratio": best["RR"], "sl_atr_mult": best["ATR"]}, f)
            print("  Saved optimal parameters to params.json for live engine synchronization.")

    else:
        split = int(len(t_cand) * 0.6)
        params = Params()
        print("\n  Warm-starting RLS manifold on in-sample 60% training block...")
        _, trained_state = run_v40_backtest(
            t_cand[:split], b_cand[:split], params, args.symbol, freeze_weights=False
        )
        print("  Evaluating out-of-sample performance on final 40% (Weights Frozen)...")
        test, _ = run_v40_backtest(
            t_cand[split:], b_cand[split:], params, args.symbol,
            initial_rls_state=trained_state, freeze_weights=True
        )

        print("\n=== V42.0 APEX TITAN OUT-OF-SAMPLE TEST (Last 40% Frozen) ===")
        for k, v in test.items():
            if isinstance(v, float):
                print(f"  {k}: {v:.4f}")
            else:
                print(f"  {k}: {v}")