#!/usr/bin/env python3
"""
V12 validation: does the pipeline REFUSE markets with no edge and PROFIT, after
realistic costs, where an edge exists?

Every condition runs the full walk-forward backtester (same decision pipeline,
same live exit engine, fees + spread + vol-scaled slippage + latency + funding
+ gap stops + lot rounding, Guardian and health monitor active). Metrics are
pooled over TEST segments only. Seeds and conditions are fixed here, before
any result is seen, and every run is reported -- nothing is dropped.

    python scripts/run_v12_validation.py [--seeds 6] [--minutes 10000] [--jobs 2]
"""
from __future__ import annotations

import argparse
import json
import math
import pathlib
import sys
import time
from multiprocessing import Pool

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

CONDITIONS = {
    # name: (drift_strength, cost multiplier, latency_sec, generator)
    "NOISE (no edge exists)": (0.0, 1.0, 2.0, "drift"),
    "NOISE, fat tails + volatility clustering (no edge)": (0.0, 1.0, 2.0, "garch"),
    "MEAN-REVERTING (OU, no trend)": (0.0, 1.0, 2.0, "ou"),
    "REGIME SWITCHING (trend 0.30 in 1/3 of time, noise otherwise)": (0.30, 1.0, 2.0, "switch"),
    "WEAK TREND (drift 0.15)": (0.15, 1.0, 2.0, "drift"),
    "TREND (drift 0.30)": (0.30, 1.0, 2.0, "drift"),
    "TREND, 2x COSTS + 10s latency": (0.30, 2.0, 10.0, "drift"),
}


def make_path(gen: str, minutes: int, seed: int, drift: float):
    """Ticks every 10 s, 1-minute sigma 0.07%. Each generator declares its truth."""
    import numpy as np
    from research.exit_lab import drifting_path
    sig = 0.0007
    if gen == "drift":
        return drifting_path(minutes, seed=seed, sigma_per_min=sig, drift_strength=drift, drift_halflife_min=240)
    rng = np.random.default_rng(seed)
    n = minutes * 6
    dt = 1 / 6
    if gen == "garch":
        # GARCH(1,1)-style variance with Student-t(3) shocks: jumps and clustered
        # volatility, zero drift. The classic source of false trend signals.
        var = np.empty(n)
        r = np.empty(n)
        v = (sig ** 2) * dt
        w, a, b = v * 0.02, 0.10, 0.88
        t = rng.standard_t(3, n) / np.sqrt(3.0)
        for i in range(n):
            var[i] = v
            r[i] = np.sqrt(v) * t[i]
            v = w + a * r[i] ** 2 + b * v
        return 100 * np.exp(np.cumsum(r))
    if gen == "ou":
        # log price mean-reverts to 100 with a 60-minute half-life
        k = np.log(2) / 60.0 * dt
        x = 0.0
        out = np.empty(n)
        e = rng.standard_normal(n) * sig * np.sqrt(dt) * 3.0
        for i in range(n):
            x += -k * x + e[i]
            out[i] = x
        return 100 * np.exp(out)
    if gen == "switch":
        # 8-hour blocks; one in three carries a persistent trend of random sign
        base = drifting_path(minutes, seed=seed, sigma_per_min=sig, drift_strength=0.0)
        r = np.diff(np.log(base), prepend=np.log(base[0]))
        block = 8 * 60 * 6
        for s0 in range(0, n, block):
            if rng.random() < 1 / 3:
                r[s0:s0 + block] += rng.choice([-1, 1]) * drift * sig * dt
        return 100 * np.exp(np.cumsum(r))
    raise ValueError(gen)


def one(args):
    name, seed, minutes = args
    from v12.backtest import ExecConfig, WalkForwardBacktest
    from v12.edge import CostModel
    drift, cmult, lat, gen = CONDITIONS[name]
    p = make_path(gen, minutes, 1000 + seed, drift)
    ticks = [(1_700_000_000 + i * 10, float(x)) for i, x in enumerate(p)]
    base = CostModel()
    costs = CostModel(taker_fee=base.taker_fee * cmult, maker_fee=base.maker_fee * cmult,
                      base_slippage_bps=base.base_slippage_bps * cmult,
                      slippage_per_sigma=base.slippage_per_sigma * cmult)
    bt = WalkForwardBacktest(exec_cfg=ExecConfig(latency_sec=lat), costs=costs)
    t0 = time.time()
    r = bt.run_ticks("SIM", ticks, spread_bps=1.0 * cmult)
    test = [t.__dict__ for t in r["trades"] if t.segment == "TEST"]
    return {"condition": name, "seed": seed, "secs": time.time() - t0, "decisions": r["decisions"],
            "trade_decisions": r["trade_decisions"], "rejections": r["rejections"], "test_trades": test,
            "health": r["health"].status}


def pooled(trades):
    from v12.backtest import BTTrade, dashboard
    return dashboard([BTTrade(**t) for t in trades], 1000.0)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=6)
    ap.add_argument("--minutes", type=int, default=10000)
    ap.add_argument("--jobs", type=int, default=2)
    ap.add_argument("--out", default=str(ROOT / "reports" / "v12"))
    ap.add_argument("--only", default="", help="comma-separated substrings of condition names")
    a = ap.parse_args()
    conds = [c for c in CONDITIONS if not a.only or any(k.strip() and k.strip() in c for k in a.only.split(","))]
    jobs = [(c, s, a.minutes) for c in conds for s in range(a.seeds)]
    with Pool(a.jobs) as pool:
        runs = pool.map(one, jobs)
    out = {"config": vars(a), "conditions": {}}
    lines = ["# V12 validation (synthetic, walk-forward, TEST segments pooled)", ""]
    for c in conds:
        rs = [r for r in runs if r["condition"] == c]
        trades = [t for r in rs for t in r["test_trades"]]
        d = pooled(trades)
        per_seed = [(r["seed"], len(r["test_trades"]),
                     sum(t["net_pnl"] for t in r["test_trades"])) for r in rs]
        out["conditions"][c] = {"dashboard": d, "per_seed": per_seed,
                                "decisions": sum(r["decisions"] for r in rs),
                                "trade_decisions": sum(r["trade_decisions"] for r in rs),
                                "health": [r["health"] for r in rs]}
        lines.append(f"## {c}")
        lines.append(f"decisions {out['conditions'][c]['decisions']}, entries "
                     f"{out['conditions'][c]['trade_decisions']}, TEST trades {d.get('n', 0)}")
        if d.get("n"):
            pf = d["profit_factor"]
            lines += [
                f"- Expectancy **{d['expectancy_bps']:+.1f} bps** ({d['expectancy_r']:+.2f}R), t = {d['t_stat']:.2f}",
                f"- Profit factor {'inf' if math.isinf(pf) else f'{pf:.2f}'}; net P&L {d['net_pnl']:+.2f} on 1000 per seed",
                f"- Max drawdown {d['max_drawdown']:.1%}; fees {d['cost_per_trade_bps']:.1f} bps/trade; funding {d['funding_total']:+.3f}",
                f"- MFE captured {'n/a' if d['mfe_captured'] is None else format(d['mfe_captured'], '.0%')}; avg MAE {d['avg_mae_r']:+.2f}R",
                f"- Predicted net edge {d['predicted_bps']:+.1f} bps vs realised {d['expectancy_bps']:+.1f} bps",
                f"- Win rate {d['win_rate']:.0%}; exits {d['exit_reasons']}",
            ]
        lines.append(f"- per seed (seed, trades, net): {per_seed}")
        lines.append("")
    pathlib.Path(a.out).mkdir(parents=True, exist_ok=True)
    (pathlib.Path(a.out) / "validation.json").write_text(json.dumps(out, indent=2, default=str))
    (pathlib.Path(a.out) / "validation.md").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
