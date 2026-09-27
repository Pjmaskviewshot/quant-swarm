"""
V12.1 — MULTI-HORIZON FORECASTER.

THE DEFECT IT REPLACES. The live model was trained on one label: "did price
rise over the next 60 seconds". Trades were then held for 30 minutes to several
hours against a 1.5% stop. At live volatility a confident 60-second forecast
(p = 0.60) is worth ~1.7 bps; a round trip costs ~15. A 60-second model was
steering a multi-hour position.

WHAT THIS DOES. Maintains one online model per horizon — 1m, 5m, 15m, 1h, 4h —
each trained on its OWN label ("did price rise over the next h"), resolved
causally: a label is only learned once h has actually elapsed.

OVERLAPPING LABELS. Sampling every 10 s with a 1-hour horizon means successive
labels share 99.7% of their price path: 7,600 "resolved predictions" are about
21 independent observations. Treating them as 7,600 let one random excursion
masquerade as overwhelming evidence -- on a pure random walk the first version
of this module reported p = 0.32 for the 1-hour horizon. Every label is now
weighted by its independent share (sample interval / horizon) in learning, in
calibration and in scoring, so evidence is counted in independent horizons.

MEASURED, NOT CLAIMED. A model can be confidently wrong. Every raw probability
is recorded at the moment it is produced, and scored against the outcome when
that outcome matures (prequential evaluation — the model never saw the answer).
Downstream code uses the CALIBRATED probability: the empirical hit-rate of past
predictions in the same probability bin, shrunk hard toward 0.5 until evidence
accumulates. A fresh model therefore reports no edge at all, which is correct.

Expected move magnitude per horizon is also measured (EWMA of realised |return|
over h), not assumed from sqrt-time scaling.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Sequence, Tuple

import numpy as np

HORIZONS_SEC: Tuple[int, ...] = (60, 300, 900, 3600, 14400)
HORIZON_NAMES = {60: "1m", 300: "5m", 900: "15m", 3600: "1h", 14400: "4h"}
PRICE_LOOKBACKS_MIN: Tuple[int, ...] = (1, 5, 15, 60, 240)


def _sigmoid(z: float) -> float:
    if z >= 0:
        e = math.exp(-z)
        return 1.0 / (1.0 + e)
    e = math.exp(z)
    return e / (1.0 + e)


class OnlineLogit:
    """Logistic regression with per-coordinate AdaGrad and L2. Starts at p = 0.5."""

    def __init__(self, dim: int, lr: float = 0.05, l2: float = 1e-4):
        self.w = np.zeros(dim)
        self.b = 0.0
        self.g2 = np.full(dim, 1e-8)
        self.gb2 = 1e-8
        self.lr, self.l2 = lr, l2
        self.updates = 0

    def predict(self, x: np.ndarray) -> float:
        return _sigmoid(float(np.dot(self.w, x) + self.b))

    def update(self, x: np.ndarray, y: float, weight: float = 1.0) -> None:
        """
        `weight` scales the STEP, not the gradient: AdaGrad is invariant to a
        constant gradient scale, so weighting the gradient would do nothing.
        """
        err = self.predict(x) - y
        g = err * x + self.l2 * self.w
        self.g2 += g * g
        self.w -= weight * self.lr * g / np.sqrt(self.g2)
        self.gb2 += err * err
        self.b -= weight * self.lr * err / math.sqrt(self.gb2)
        self.updates += 1


class Calibrator:
    """
    Maps a raw probability to the hit-rate actually observed for past
    predictions in the same bin, shrunk toward 0.5 with `strength` pseudo-counts.
    Also tracks Brier skill over a rolling window, for drift detection.
    """

    def __init__(self, n_bins: int = 10, strength: float = 50.0, window: int = 600):
        self.n_bins, self.k = n_bins, strength
        self.n = np.zeros(n_bins)
        self.hits = np.zeros(n_bins)
        self.recent: Deque[Tuple[float, int]] = deque(maxlen=window)
        self.count = 0

    def _bin(self, p: float) -> int:
        return min(self.n_bins - 1, max(0, int(p * self.n_bins)))

    def calibrated(self, p_raw: float) -> float:
        i = self._bin(p_raw)
        return float((self.hits[i] + 0.5 * self.k) / (self.n[i] + self.k))

    def add(self, p_raw: float, y: int, weight: float = 1.0, score: bool = True) -> None:
        """`weight` = independent share of this label; `score` = enter the Brier window."""
        i = self._bin(p_raw)
        self.n[i] += weight
        self.hits[i] += weight * y
        self.count += 1
        if score:
            self.recent.append((self.calibrated(p_raw), int(y)))

    @property
    def resolved(self) -> int:
        """Effective number of INDEPENDENT scored predictions."""
        return int(self.n.sum())

    def brier_skill(self, last: Optional[int] = None) -> float:
        """
        1 - Brier(model)/Brier(base rate). > 0 beats guessing the base rate.
        The window holds ~10 entries per horizon length (strided), so 100
        entries is ~10 independent horizons -- the minimum before this reports
        anything. With fewer, a skill figure is noise dressed as a measurement.
        """
        data = list(self.recent)[-last:] if last else list(self.recent)
        if len(data) < 100:
            return 0.0
        ys = np.array([y for _, y in data], float)
        ps = np.array([p for p, _ in data], float)
        base = ys.mean()
        ref = float(np.mean((base - ys) ** 2))
        if ref <= 1e-12:
            return 0.0
        return 1.0 - float(np.mean((ps - ys) ** 2)) / ref


@dataclass(frozen=True)
class HorizonForecast:
    horizon_sec: int
    p_raw: float
    p_cal: float                 # MEASURED probability of an up-move over the horizon
    resolved: int                # how many past predictions have been scored
    exp_abs_move: float          # measured E|log return| over the horizon (fraction)
    brier_skill: float

    @property
    def name(self) -> str:
        return HORIZON_NAMES.get(self.horizon_sec, f"{self.horizon_sec}s")

    def expected_move(self, is_buy: bool) -> float:
        """Signed expected return in the trade's direction, as a fraction."""
        q = self.p_cal if is_buy else 1.0 - self.p_cal
        return (2.0 * q - 1.0) * self.exp_abs_move


class MultiHorizonForecaster:
    """
    One per symbol. Call observe(now, price, features) as often as you like;
    it samples internally every `sample_every_sec`.
    """

    def __init__(self, horizons: Sequence[int] = HORIZONS_SEC, sample_every_sec: float = 10.0,
                 external_dim: int = 0, lr: float = 0.05, l2: float = 1e-4,
                 calib_strength: float = 50.0):
        self.horizons = tuple(horizons)
        self.dt = float(sample_every_sec)
        self.external_dim = int(external_dim)
        self.dim = len(PRICE_LOOKBACKS_MIN) + 2 + self.external_dim
        self.models = {h: OnlineLogit(self.dim, lr, l2) for h in self.horizons}
        self.calib = {h: Calibrator(strength=calib_strength) for h in self.horizons}
        self.abs_move_sum = {h: 0.0 for h in self.horizons}
        self.abs_move_n_eff = {h: 0.0 for h in self.horizons}
        self._resolved_count = {h: 0 for h in self.horizons}
        max_lb = max(PRICE_LOOKBACKS_MIN) * 60
        self.hist: Deque[Tuple[float, float]] = deque(maxlen=int(max_lb / self.dt) + 8)
        self.pending: Dict[int, Deque[Tuple[float, float, np.ndarray, float]]] = {
            h: deque() for h in self.horizons}
        self.last_sample = -math.inf
        self.last_x: Optional[np.ndarray] = None
        self.var_1m = 0.0            # EWMA of squared 1-minute log returns
        self._slow_cache: Optional[Tuple[float, float]] = None
        self._last_1m: Optional[Tuple[float, float]] = None

    # ------------------------------------------------------------------ features
    def _price_at(self, t: float) -> Optional[float]:
        """
        Most recent sampled price at or before t. Samples are (nearly) evenly
        spaced, so jump straight to the estimated index and step to the exact
        one -- O(1) instead of scanning four hours of history every sample.
        """
        n = len(self.hist)
        if n == 0 or self.hist[0][0] > t:
            return None
        k = int((self.hist[-1][0] - t) / self.dt)
        i = max(0, min(n - 1, n - 1 - k))
        while i < n - 1 and self.hist[i + 1][0] <= t:
            i += 1
        while i > 0 and self.hist[i][0] > t:
            i -= 1
        return self.hist[i][1] if self.hist[i][0] <= t else None

    def sigma_1m(self) -> float:
        return math.sqrt(self.var_1m) if self.var_1m > 0 else 0.0

    def _features(self, now: float, price: float, external: Optional[Sequence[float]]) -> np.ndarray:
        s1 = max(self.sigma_1m(), 1e-5)
        feats: List[float] = []
        for lb in PRICE_LOOKBACKS_MIN:
            past = self._price_at(now - lb * 60)
            r = math.log(price / past) if past and past > 0 else 0.0
            feats.append(float(np.clip(r / (s1 * math.sqrt(lb)), -5, 5)))
        # volatility state relative to its own recent level, and 1m acceleration
        feats.append(float(np.clip(math.log(max(s1, 1e-6) / max(self._slow_sigma(), 1e-6)), -3, 3)))
        feats.append(float(np.clip(feats[0] - feats[1] / math.sqrt(5), -5, 5)))
        if self.external_dim:
            ext = np.zeros(self.external_dim)
            if external is not None:
                e = np.asarray(external, float).ravel()[: self.external_dim]
                ext[: len(e)] = np.clip(np.nan_to_num(e), -5, 5)
            feats.extend(ext.tolist())
        return np.array(feats, float)

    def _slow_sigma(self) -> float:
        """Volatility over the stored history, recomputed at most once a minute."""
        now = self.hist[-1][0] if self.hist else 0.0
        if self._slow_cache is not None and now - self._slow_cache[0] < 60.0:
            return self._slow_cache[1]
        v = self._slow_sigma_compute()
        self._slow_cache = (now, v)
        return v

    def _slow_sigma_compute(self) -> float:
        if len(self.hist) < 20:
            return max(self.sigma_1m(), 1e-5)
        px = np.array([p for _, p in self.hist])
        step = max(1, int(60 / self.dt))
        sub = px[::step]
        if len(sub) < 5:
            return max(self.sigma_1m(), 1e-5)
        r = np.diff(np.log(sub))
        return float(np.std(r)) if len(r) > 1 else max(self.sigma_1m(), 1e-5)

    # ------------------------------------------------------------------ learning
    def observe(self, now: float, price: float, external: Optional[Sequence[float]] = None) -> bool:
        if not (price > 0 and math.isfinite(price)):
            return False
        if now - self.last_sample < self.dt:
            return False
        self.last_sample = now
        if self._last_1m is None:
            self._last_1m = (now, price)
        elif now - self._last_1m[0] >= 60.0:
            r = math.log(price / self._last_1m[1])
            self.var_1m = r * r if self.var_1m == 0 else 0.97 * self.var_1m + 0.03 * r * r
            self._last_1m = (now, price)
        self.hist.append((now, price))

        # resolve matured predictions FIRST (prequential: score, then learn)
        for h in self.horizons:
            q = self.pending[h]
            w = min(1.0, self.dt / h)                    # independent share of one label
            stride = max(1, int(round(h / (10.0 * self.dt))))
            while q and now - q[0][0] >= h:
                t0, p0, x0, praw0 = q.popleft()
                y = 1 if price > p0 else 0
                self._resolved_count[h] += 1
                self.calib[h].add(praw0, y, weight=w,
                                  score=(self._resolved_count[h] % stride == 0))
                self.models[h].update(x0, float(y), weight=w)
                mv = abs(math.log(price / p0))
                self.abs_move_sum[h] += w * mv
                self.abs_move_n_eff[h] += w

        x = self._features(now, price, external)
        self.last_x = x
        for h in self.horizons:
            self.pending[h].append((now, price, x, self.models[h].predict(x)))
        return True

    # ------------------------------------------------------------------ output
    def _fallback_abs_move(self, h: int) -> float:
        return self.sigma_1m() * math.sqrt(h / 60.0) * math.sqrt(2.0 / math.pi)

    def expected_abs_move(self, h: int) -> float:
        """
        sqrt-time scaling from well-sampled 1-minute volatility, blended toward
        the directly measured |return| over h as INDEPENDENT windows accumulate.
        Measured-only was unusable: a 4-hour EWMA over overlapping windows is
        one realisation, and came out smaller than the 1-hour figure.
        """
        scaled = self._fallback_abs_move(h)
        n_eff = self.abs_move_n_eff[h]
        if n_eff <= 0:
            return scaled
        measured = self.abs_move_sum[h] / n_eff
        w = n_eff / (n_eff + 30.0)
        return w * measured + (1 - w) * scaled

    def forecast(self) -> Dict[int, HorizonForecast]:
        out: Dict[int, HorizonForecast] = {}
        for h in self.horizons:
            p_raw = self.models[h].predict(self.last_x) if self.last_x is not None else 0.5
            am = self.expected_abs_move(h)
            out[h] = HorizonForecast(h, p_raw, self.calib[h].calibrated(p_raw),
                                     self.calib[h].resolved, am, self.calib[h].brier_skill())
        return out

    def agreement(self, is_buy: bool, forecasts: Optional[Dict[int, HorizonForecast]] = None,
                  min_resolved: int = 30) -> float:
        """
        0-100. 50 = no view; 100 = every mature horizon measurably agrees with the
        trade; 0 = every mature horizon measurably opposes it. Horizons without
        enough scored predictions contribute nothing.
        """
        fc = forecasts or self.forecast()
        num = den = 0.0
        for h, f in fc.items():
            if f.resolved < min_resolved:
                continue
            q = f.p_cal if is_buy else 1.0 - f.p_cal
            w = math.log(h / 30.0)
            num += w * float(np.clip((2 * q - 1) / 0.10, -1.0, 1.0))
            den += w
        if den == 0:
            return 50.0
        return 50.0 + 50.0 * num / den
