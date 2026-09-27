"""
V12.4b — MFE/MAE ANALYSIS and a WALK-FORWARD EXIT OPTIMISER that only RECOMMENDS.

"Don't assume 1R/1.5R/3R is optimal. Measure it."

Input: journalled trades with their recorded price path from entry until 240
minutes after exit (see journal.py). Each path is converted to R units using
that trade's own stop distance, so trades of different volatility pool.

1. `mfe_mae_report` answers the questions the owner asked:
     * how far do winners typically run (MFE distribution)?
     * how far against us do winners go before working (MAE of winners)?
       -> if winners rarely exceed -0.6R, a 1R stop is paying for nothing
     * how many losers were once in profit (> +1R) before stopping out?
       -> the cost of NOT protecting profit
     * how much of the available move did the exit capture (MFE capture)?
     * what happened after we exited (post-exit continuation)?
       -> if price kept going our way, exits are early

2. `walk_forward_optimise` replays a GRID of runner-policy variants over the
   recorded paths. Trades are split by time into TRAIN (first 70%) and TEST
   (last 30%). The best variant on TRAIN is scored ONCE on TEST and compared to
   the CURRENT policy on the SAME test trades (paired). A recommendation is
   produced only if
       * at least `min_trades` paths exist,
       * the improvement on TEST is positive with paired t >= `min_t`
         (Bonferroni-adjusted for the grid size), and
       * the variant is not worse on the TEST worst decile (tail check).
   Even then the output is a CANDIDATE file with "approved": false. It is never
   loaded by the bot until a human runs scripts/approve_candidate.py, and then
   only if EXIT_POLICY_FILE points at it. Learn -> evaluate -> paper test ->
   approve -> deploy. The optimiser never touches live parameters.

Replay limitation, stated plainly: paths are sampled every 10 s, so intrabar
extremes are missed, and replay assumes our own exit would not have moved the
price. Stop exits are charged at the stop level or worse (never better).
"""
from __future__ import annotations

import itertools
import json
import math
import statistics as st
import time
from dataclasses import asdict, dataclass, replace
from typing import Dict, List, Optional, Sequence, Tuple

from core.intelligent_exit import EXIT_CONFIG, ExitPolicyConfig


@dataclass
class RPath:
    trade_id: str
    exit_ts_index: int          # index in r[] where the real exit happened
    r: List[float]              # price move in R units from entry, sampled
    dt_min: float               # minutes between samples
    cost_r: float               # round-trip costs in R
    realised_r: Optional[float] = None


def to_rpath(trade: Dict, path: Sequence[Tuple[float, float, str]], cost_frac: float = 0.0015) -> Optional[RPath]:
    ep, stop, d = trade.get("entry_price"), trade.get("stop_pct"), trade.get("direction")
    if not ep or not stop or not path or len(path) < 3:
        return None
    sgn = 1.0 if d == "BUY" else -1.0
    r = [sgn * (p - ep) / (ep * stop) for _, p, _ in path]
    exit_idx = next((i for i, (_, _, ph) in enumerate(path) if ph == "POST"), len(path) - 1)
    dt = max(1e-6, (path[-1][0] - path[0][0]) / 60.0 / (len(path) - 1))
    return RPath(str(trade.get("trade_id")), exit_idx, r, dt, cost_frac / stop, trade.get("r_multiple"))


def replay(rp: RPath, cfg: ExitPolicyConfig) -> Tuple[float, float, str]:
    """Runner policy on a recorded R path. Returns (net R, hold minutes, reason)."""
    stop_r, peak = -1.0, 0.0
    for i, x in enumerate(rp.r[1:], start=1):
        peak = max(peak, x)
        if peak >= cfg.be_trigger_r - 1e-9:
            stop_r = max(stop_r, rp.cost_r)
        if peak >= cfg.trail_start_r - 1e-9:
            stop_r = max(stop_r, peak - cfg.trail_distance_r)
        t = i * rp.dt_min
        if x <= stop_r:
            return min(stop_r, x) - rp.cost_r, t, ("STOP" if stop_r < 0 else "TRAIL")
        if x >= cfg.min_reward_r:
            return cfg.min_reward_r - rp.cost_r, t, "TARGET"
        if t >= cfg.stagnation_minutes and peak < cfg.stagnation_r:
            return x - rp.cost_r, t, "STAGNATION"
        if t >= cfg.horizon_minutes and peak < cfg.be_trigger_r:
            return x - rp.cost_r, t, "TIME"
    return rp.r[-1] - rp.cost_r, (len(rp.r) - 1) * rp.dt_min, "END"


def _q(xs: List[float], q: float) -> float:
    s = sorted(xs)
    return s[min(len(s) - 1, max(0, int(q * (len(s) - 1))))] if s else float("nan")


def mfe_mae_report(paths: List[RPath]) -> Dict[str, object]:
    if not paths:
        return {"n": 0}
    rows = []
    for p in paths:
        live = p.r[:p.exit_ts_index + 1]
        post = p.r[p.exit_ts_index:]
        mfe, mae = max(live), min(live)
        real = p.realised_r if p.realised_r is not None else live[-1] - p.cost_r
        rows.append(dict(mfe=mfe, mae=mae, real=real,
                         post_best=max(post) - live[-1] if post else 0.0,
                         post_worst=min(post) - live[-1] if post else 0.0))
    wins = [r for r in rows if r["real"] > 0]
    losses = [r for r in rows if r["real"] <= 0]
    out: Dict[str, object] = {
        "n": len(rows),
        "winners_mfe_median_r": _q([r["mfe"] for r in wins], 0.5) if wins else None,
        "winners_mfe_p75_r": _q([r["mfe"] for r in wins], 0.75) if wins else None,
        "winners_mae_p90_r": _q([r["mae"] for r in wins], 0.10) if wins else None,
        "losers_once_above_1r": (sum(r["mfe"] >= 1.0 for r in losses) / len(losses)) if losses else None,
        "losers_once_above_0_5r": (sum(r["mfe"] >= 0.5 for r in losses) / len(losses)) if losses else None,
        "mfe_capture": (st.mean(r["real"] for r in wins) / st.mean(r["mfe"] for r in wins))
        if wins and st.mean(r["mfe"] for r in wins) > 0 else None,
        "post_exit_best_median_r": _q([r["post_best"] for r in rows], 0.5),
        "post_exit_worst_median_r": _q([r["post_worst"] for r in rows], 0.5),
    }
    notes = []
    wm = out["winners_mae_p90_r"]
    if isinstance(wm, float) and wm > -0.6 and len(wins) >= 30:
        notes.append(f"90% of winners never went below {wm:+.2f}R: the stop may be wider than needed")
    l1 = out["losers_once_above_1r"]
    if isinstance(l1, float) and l1 > 0.15 and len(losses) >= 30:
        notes.append(f"{l1:.0%} of losers were once above +1R: profit protection is too loose")
    pb = out["post_exit_best_median_r"]
    if isinstance(pb, float) and pb > 0.5:
        notes.append(f"median trade moved a further {pb:+.2f}R our way after exit: exits look early")
    out["notes"] = notes
    return out


DEFAULT_GRID = {
    "be_trigger_r": (0.75, 1.0, 1.5),
    "trail_start_r": (1.0, 1.5, 2.0),
    "trail_distance_r": (0.75, 1.0, 1.5),
    "min_reward_r": (2.0, 3.0, 4.0, 99.0),
}


def _grid(base: ExitPolicyConfig, grid: Dict[str, Sequence[float]]) -> List[ExitPolicyConfig]:
    keys = list(grid)
    out = []
    for vals in itertools.product(*(grid[k] for k in keys)):
        kw = dict(zip(keys, vals))
        if kw.get("trail_start_r", base.trail_start_r) < kw.get("be_trigger_r", base.be_trigger_r):
            continue
        out.append(replace(base, **kw))
    return out


def _paired_t(a: List[float], b: List[float]) -> Tuple[float, float]:
    d = [x - y for x, y in zip(a, b)]
    if len(d) < 3:
        return 0.0, 0.0
    m = st.mean(d)
    sd = st.stdev(d)
    return m, (m / (sd / math.sqrt(len(d))) if sd > 0 else 0.0)


def walk_forward_optimise(paths: List[RPath], current: ExitPolicyConfig = EXIT_CONFIG,
                          grid: Optional[Dict[str, Sequence[float]]] = None,
                          train_frac: float = 0.7, min_trades: int = 100,
                          min_t: float = 2.0) -> Dict[str, object]:
    grid = grid or DEFAULT_GRID
    cands = _grid(current, grid)
    n = len(paths)
    res: Dict[str, object] = {"n": n, "grid_size": len(cands), "current": asdict(current),
                              "recommend": False}
    if n < min_trades:
        res["reason"] = f"only {n} recorded trade paths; need {min_trades} before any exit change is considered"
        return res
    k = int(n * train_frac)
    train, test = paths[:k], paths[k:]
    score = lambda cfg, ps: [replay(p, cfg)[0] for p in ps]   # noqa: E731
    best = max(cands, key=lambda c: st.mean(score(c, train)))
    cur_test, best_test = score(current, test), score(best, test)
    diff, t = _paired_t(best_test, cur_test)
    # Bonferroni: the best of N candidates needs a stricter bar
    from v12.learning import _normal_quantile
    t_needed = max(min_t, _normal_quantile(1 - 0.025 / max(1, len(cands))))
    tail_ok = _q(best_test, 0.1) >= _q(cur_test, 0.1) - 0.05
    res.update({
        "train_n": len(train), "test_n": len(test), "best": asdict(best),
        "train_mean_r_best": st.mean(score(best, train)),
        "train_mean_r_current": st.mean(score(current, train)),
        "test_mean_r_best": st.mean(best_test), "test_mean_r_current": st.mean(cur_test),
        "test_paired_diff_r": diff, "test_paired_t": t, "t_needed": t_needed, "tail_ok": tail_ok,
    })
    if best == current:
        res["reason"] = "current policy is already the best on TRAIN"
    elif diff <= 0 or t < t_needed:
        res["reason"] = (f"best TRAIN variant does not significantly beat current on TEST "
                         f"(diff {diff:+.3f}R, t={t:.2f} < {t_needed:.2f})")
    elif not tail_ok:
        res["reason"] = "improvement comes with a worse tail on TEST"
    else:
        res["recommend"] = True
        res["reason"] = f"TEST improvement {diff:+.3f}R/trade, paired t={t:.2f}"
    return res


def write_candidate(result: Dict[str, object], path: str) -> None:
    """Candidates are ALWAYS written unapproved."""
    doc = dict(result)
    doc.update({"kind": "exit_policy", "created_ts": time.time(), "approved": False,
                "approved_by": None, "note": "Not active. Review, paper-test, then run "
                "scripts/approve_candidate.py and set EXIT_POLICY_FILE to deploy."})
    with open(path, "w") as fh:
        json.dump(doc, fh, indent=2, default=str)


def load_approved_exit_config(path: Optional[str]) -> Optional[ExitPolicyConfig]:
    """Returns a config only for an approved candidate file. Anything else -> None."""
    if not path:
        return None
    try:
        with open(path) as fh:
            doc = json.load(fh)
    except (OSError, ValueError):
        return None
    if doc.get("kind") != "exit_policy" or doc.get("approved") is not True or not doc.get("approved_by"):
        return None
    best = doc.get("best") or {}
    fields = {k: v for k, v in best.items() if k in ExitPolicyConfig.__dataclass_fields__}
    try:
        cfg = ExitPolicyConfig(**fields)
    except TypeError:
        return None
    if cfg.legacy or cfg.min_reward_r < 1.0 or cfg.trail_distance_r <= 0:
        return None
    return cfg
