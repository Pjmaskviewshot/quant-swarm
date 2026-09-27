"""
V12 WALK-FORWARD BACKTESTER — the same decision pipeline and the same live exit
engine the bot runs, with execution modelled pessimistically.

Honesty rules this module enforces:
  * CAUSAL. Every decision uses only prices at or before its timestamp. The
    forecaster is prequential (it predicts, then learns when the outcome
    arrives), so the whole run is one continuous walk-forward.
  * TRAIN -> TEST. The first `train_frac` of the period is warm-up/learning.
    Headline metrics are reported on the TEST segment only, with TRAIN shown
    beside them. Learned state (forecaster weights, calibration, measured edge
    per regime) carries across the split -- nothing is refit on TEST.
  * COSTS. Taker fee on both legs, half the spread on both legs, volatility-
    scaled slippage, latency (the fill happens `latency_sec` after the
    decision, at that later price), funding charged at every 8-hour boundary
    crossed while holding (longs pay positive funding).
  * STOPS GAP. An exchange stop fills at the first price beyond it, never at
    the stop level itself if the market jumped through. Targets fill at the
    target (they are resting orders) minus taker fee.
  * OHLC bars are expanded to O -> adverse extreme -> favourable extreme -> C
    relative to an open position, so a bar that touched both stop and target
    counts as a stop.
  * EXCHANGE CONSTRAINTS. Quantity is rounded DOWN to the lot step; an order
    below the minimum notional is skipped, never rounded up.
  * PARTIAL FILLS. With bar volume available, an order larger than
    `max_participation` of the bar's traded notional is filled only up to it.
  * RISK. The Guardian's daily-loss and drawdown levels and the health
    monitor's HALT apply during the backtest exactly as live.
"""
from __future__ import annotations

import math
import statistics as st
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from core.intelligent_exit import EXIT_CONFIG, ExitPolicyConfig
from research.exit_lab import LivePolicy
from v12.edge import CostModel
from v12.guardian import Guardian, GuardianConfig
from v12.health import StrategyHealthMonitor
from v12.pipeline import DecisionPipeline, PipelineConfig


@dataclass
class ExecConfig:
    latency_sec: float = 2.0
    qty_step: float = 0.001
    min_notional: float = 5.0
    max_participation: float = 0.02
    funding_rate_8h: float = 0.0001
    decide_every_sec: float = 60.0
    train_frac: float = 0.5
    starting_equity: float = 1000.0
    max_hold_min: float = 24 * 60


@dataclass
class BTTrade:
    symbol: str
    direction: str
    entry_ts: float
    exit_ts: float
    entry_price: float
    exit_price: float
    qty: float
    stop_pct: float
    predicted_bps: float
    cost_model_bps: float
    regime: str
    horizon_sec: int
    net_pnl: float
    net_bps: float
    fees: float
    funding: float
    r: float
    mfe_r: float
    mae_r: float
    hold_min: float
    exit_reason: str
    segment: str
    partial: bool = False


@dataclass
class _Open:
    dec: object
    entry_ts: float
    entry: float
    qty: float
    stop_px: float
    tp_px: float
    risk: float
    fees: float
    policy: LivePolicy
    mfe: float = 0.0
    mae: float = 0.0
    funding: float = 0.0
    partial: bool = False


def _round_down(q: float, step: float) -> float:
    return math.floor(q / step + 1e-9) * step


def bars_to_ticks(bars: Sequence[Tuple[float, float, float, float, float, float]],
                  pessimistic_for: str = "BOTH") -> Tuple[List[Tuple[float, float]], List[float]]:
    """(ts, o, h, l, c, volume) 1-minute bars -> four ticks per bar (15 s apart).

    Which extreme comes first is unknowable from a bar. We place the extreme
    nearer the open first (the usual path) EXCEPT that a bar containing BOTH a
    new high and a new low relative to its open is ordered low-first on odd
    minutes and high-first on even ones, so neither longs nor shorts are
    systematically flattered. Stops and targets touched inside one bar by a
    real position are resolved stop-first by the simulator (see run_ticks)."""
    ticks: List[Tuple[float, float]] = []
    vols: List[float] = []
    for k, (ts, o, h, lo, c, v) in enumerate(bars):
        low_first = (o - lo) < (h - o) if pessimistic_for == "BOTH" else pessimistic_for == "LONG"
        if pessimistic_for == "BOTH" and abs((o - lo) - (h - o)) < 1e-12:
            low_first = k % 2 == 1
        seq = (o, lo, h, c) if low_first else (o, h, lo, c)
        for j, p in enumerate(seq):
            ticks.append((ts + 15 * j, float(p)))
            vols.append(float(v) / 4.0)
    return ticks, vols


class WalkForwardBacktest:
    def __init__(self, pipe_cfg: Optional[PipelineConfig] = None, exec_cfg: Optional[ExecConfig] = None,
                 exit_cfg: ExitPolicyConfig = EXIT_CONFIG, costs: Optional[CostModel] = None,
                 guardian_cfg: Optional[GuardianConfig] = None):
        self.pc = pipe_cfg or PipelineConfig()
        self.xc = exec_cfg or ExecConfig()
        self.exit_cfg = exit_cfg
        self.costs = costs or CostModel()
        self.gcfg = guardian_cfg or GuardianConfig(require_promotion_for_live=False)

    # ------------------------------------------------------------------ core
    def run_ticks(self, symbol: str, ticks: Sequence[Tuple[float, float]], spread_bps: float = 1.0,
                  volume_per_tick: Optional[Sequence[float]] = None) -> Dict[str, object]:
        """ticks: [(ts, price)] in time order (e.g. every 10 s)."""
        xc = self.xc
        pipe = DecisionPipeline(self.pc, costs=self.costs, exit_cfg=self.exit_cfg)
        guard = Guardian(self.gcfg, mode="PAPER")
        health = StrategyHealthMonitor()
        equity = xc.starting_equity
        split_ts = ticks[0][0] + (ticks[-1][0] - ticks[0][0]) * xc.train_frac
        trades: List[BTTrade] = []
        pending: Optional[Tuple[float, object]] = None     # (fill_ts, decision)
        pos: Optional[_Open] = None
        last_decide = -1e18
        n_dec = n_trade_dec = blocked = 0
        reasons: Dict[str, int] = {}
        taker = self.costs.taker_fee

        for i, (ts, px) in enumerate(ticks):
            px = float(px)
            pipe.observe(symbol, ts, px)
            sigma = pipe._fc(symbol).sigma_1m() or 0.0
            slip = (self.costs.base_slippage_bps + self.costs.slippage_per_sigma * sigma * 1e4) / 1e4
            half_spread = spread_bps / 2e4
            guard.set_equity(equity, ts)
            guard.set_reconciliation(True, "backtest", ts)
            guard.set_data_age(symbol, 0.0)

            # ---- manage the open position
            if pos is not None:
                is_buy = pos.dec.direction == "BUY"
                rr = ((px - pos.entry) if is_buy else (pos.entry - px)) / pos.risk
                pos.mfe, pos.mae = max(pos.mfe, rr), min(pos.mae, rr)
                # funding at 8h boundaries
                prev_ts = ticks[i - 1][0]
                if int(prev_ts // 28800) != int(ts // 28800):
                    f = xc.funding_rate_8h * pos.qty * px
                    pos.funding += f if is_buy else -f
                reason, fill = None, None
                hit_stop = px <= pos.stop_px if is_buy else px >= pos.stop_px
                hit_tp = px >= pos.tp_px if is_buy else px <= pos.tp_px
                if hit_stop:        # exchange stop: gap fills at the worse of stop / current
                    fill = min(px, pos.stop_px) if is_buy else max(px, pos.stop_px)
                    reason = "EXCHANGE_STOP"
                elif hit_tp:
                    fill, reason = pos.tp_px, "EXCHANGE_TP"
                else:
                    why = pos.policy.step(px, ts)
                    if why:
                        fill, reason = px, why
                    elif (ts - pos.entry_ts) / 60 >= xc.max_hold_min:
                        fill, reason = px, "MAX_HOLD"
                if reason:
                    fill = fill * (1 - half_spread - slip) if is_buy else fill * (1 + half_spread + slip)
                    if reason == "EXCHANGE_TP":   # resting target: no spread crossing, but taker trigger
                        fill = pos.tp_px
                    exit_fee = taker * pos.qty * fill
                    gross = (fill - pos.entry) * pos.qty if is_buy else (pos.entry - fill) * pos.qty
                    net = gross - pos.fees - exit_fee - pos.funding
                    notional = pos.entry * pos.qty
                    net_bps = net / notional * 1e4
                    seg = "TRAIN" if pos.entry_ts < split_ts else "TEST"
                    t = BTTrade(symbol, pos.dec.direction, pos.entry_ts, ts, pos.entry, fill, pos.qty,
                                pos.dec.stop_pct, pos.dec.net_edge_bps, pos.dec.cost_bps, pos.dec.regime,
                                pos.dec.horizon_sec, net, net_bps, pos.fees + exit_fee, pos.funding,
                                net_bps / (pos.dec.stop_pct * 1e4), pos.mfe, pos.mae,
                                (ts - pos.entry_ts) / 60, reason, seg, pos.partial)
                    trades.append(t)
                    equity += net
                    pipe.record_outcome(pos.dec, net_bps)
                    health.record_trade(net, net_bps, t.r, t.fees / notional * 1e4,
                                        pos.dec.net_edge_bps, t.mfe_r, t.mae_r)
                    guard.record_trade(net)
                    rep = health.report(equity)
                    guard.set_strategy_halt(rep.reasons[0] if rep.status == "HALT" else None)
                    pos = None
                continue

            # ---- fill a pending entry after latency
            if pending is not None and ts >= pending[0]:
                dec = pending[1]
                pending = None
                is_buy = dec.direction == "BUY"
                fill = px * (1 + half_spread + slip) if is_buy else px * (1 - half_spread - slip)
                qty = _round_down(dec.notional / fill, xc.qty_step)
                partial = False
                if volume_per_tick is not None:
                    cap = xc.max_participation * float(volume_per_tick[i]) * px
                    if qty * fill > cap:
                        qty, partial = _round_down(cap / fill, xc.qty_step), True
                if qty * fill < xc.min_notional:
                    blocked += 1
                    continue
                risk = dec.stop_pct * fill
                stop_px = fill - risk if is_buy else fill + risk
                tp_px = fill + self.exit_cfg.min_reward_r * risk if is_buy else fill - self.exit_cfg.min_reward_r * risk
                pol = LivePolicy(self.exit_cfg)
                pol.start(fill, is_buy, sigma * fill * math.sqrt(5), risk, ts)
                pos = _Open(dec, ts, fill, qty, stop_px, tp_px, risk, taker * qty * fill, pol, partial=partial)
                continue

            # ---- decide
            if ts - last_decide < xc.decide_every_sec:
                continue
            last_decide = ts
            dec = pipe.decide(symbol, ts, equity=equity, spread_bps=spread_bps,
                              funding_rate_8h=xc.funding_rate_8h, min_notional=xc.min_notional)
            n_dec += 1
            if not dec.is_trade:
                k = dec.reasons[0].split("(")[0].strip() if dec.reasons else "?"
                reasons[k] = reasons.get(k, 0) + 1
                continue
            g = guard.evaluate_entry(symbol, dec.direction, dec.notional, ts)
            if not g.allowed:
                k = f"GUARDIAN {g.level}"
                reasons[k] = reasons.get(k, 0) + 1
                continue
            n_trade_dec += 1
            pending = (ts + xc.latency_sec, dec)

        return {
            "symbol": symbol, "trades": trades, "decisions": n_dec, "trade_decisions": n_trade_dec,
            "skipped_min_notional": blocked, "rejections": reasons, "final_equity": equity,
            "train": dashboard([t for t in trades if t.segment == "TRAIN"], xc.starting_equity),
            "test": dashboard([t for t in trades if t.segment == "TEST"], xc.starting_equity),
            "health": health.report(equity),
        }


def dashboard(trades: List[BTTrade], starting_equity: float) -> Dict[str, object]:
    """Expectancy first; win rate last."""
    n = len(trades)
    if n == 0:
        return {"n": 0}
    bps = [t.net_bps for t in trades]
    pnl = [t.net_pnl for t in trades]
    gw, gl = sum(p for p in pnl if p > 0), -sum(p for p in pnl if p <= 0)
    eq = peak = starting_equity
    mdd = 0.0
    for p in pnl:
        eq += p
        peak = max(peak, eq)
        mdd = max(mdd, (peak - eq) / peak)
    se = st.stdev(bps) / math.sqrt(n) if n > 1 else float("nan")
    wins = [t for t in trades if t.net_pnl > 0]
    mfe_w = [t.mfe_r for t in wins if t.mfe_r > 0]
    return {
        "n": n,
        "expectancy_bps": st.mean(bps),
        "expectancy_r": st.mean(t.r for t in trades),
        "t_stat": st.mean(bps) / se if se and se > 0 else 0.0,
        "profit_factor": gw / gl if gl > 0 else float("inf"),
        "net_pnl": sum(pnl),
        "max_drawdown": mdd,
        "avg_r": st.mean(t.r for t in trades),
        "cost_per_trade_bps": st.mean(t.fees / (t.entry_price * t.qty) * 1e4 for t in trades),
        "funding_total": sum(t.funding for t in trades),
        "mfe_captured": (st.mean(t.r for t in wins) / st.mean(mfe_w)) if mfe_w else None,
        "avg_mae_r": st.mean(t.mae_r for t in trades),
        "predicted_bps": st.mean(t.predicted_bps for t in trades),
        "avg_hold_min": st.mean(t.hold_min for t in trades),
        "win_rate": len(wins) / n,
        "exit_reasons": {r: sum(1 for t in trades if t.exit_reason == r) for r in {t.exit_reason for t in trades}},
    }


def render_dashboard(d: Dict[str, object], title: str = "") -> str:
    if not d.get("n"):
        return f"{title}: no trades"
    pf = d["profit_factor"]
    return "\n".join([
        title,
        f"  Expectancy       {d['expectancy_bps']:+.1f} bps  ({d['expectancy_r']:+.2f}R)   t={d['t_stat']:.2f}",
        f"  Profit Factor    {'inf' if math.isinf(pf) else f'{pf:.2f}'}",
        f"  Net P&L          {d['net_pnl']:+.2f}",
        f"  Max Drawdown     {d['max_drawdown']:.1%}",
        f"  Avg R            {d['avg_r']:+.2f}",
        f"  Cost/Trade       {d['cost_per_trade_bps']:.1f} bps fees (+ spread/slippage in fills)",
        "  MFE Captured     " + ("n/a" if d["mfe_captured"] is None else f"{d['mfe_captured']:.0%}"),
        f"  Avg MAE          {d['avg_mae_r']:+.2f}R",
        f"  Win Rate         {d['win_rate']:.0%}",
        f"  Trades           {d['n']}  (avg hold {d['avg_hold_min']:.0f} min; predicted {d['predicted_bps']:+.1f} bps)",
    ])
