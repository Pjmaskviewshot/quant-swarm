#!/usr/bin/env python3
"""
Run a recorded, validated backtest experiment.

    # instrument check — must find NO edge
    python scripts/run_experiment.py --dataset synthetic:random_walk --name null-check

    # known-edge check — must find one
    python scripts/run_experiment.py --dataset synthetic:momentum --name momentum-check

    # real data, honest split
    python scripts/run_experiment.py --dataset BTCUSDT:1 --name btc-baseline --split

Every run writes a JSON record under reports/experiments/ carrying the commit,
the dataset content hash, the parameters, the cost model, the results and the
validation verdict — so any number quoted from it can be traced back and re-run.

NOTHING HERE PLACES AN ORDER. It reads cached candles and simulates.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any, Dict

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from research.dataset import Dataset, load_dataset, available_datasets   # noqa: E402
from research.experiment import CostModel, run_experiment       # noqa: E402
from research import validate as V                                       # noqa: E402


def backtest_runner(ds: Dataset, params: Dict[str, Any], cm: CostModel) -> Dict[str, Any]:
    """
    Adapter onto the production backtester.

    It is the SAME engine the strategy work uses — deliberately. Validating a
    separate toy simulator would prove nothing about the thing that actually
    produces the numbers.
    """
    from backtest import run_v40_backtest, Params
    import backtest as bt

    # The backtester holds its frictions as module constants. Bind them to the
    # recorded cost model so the record cannot disagree with the run.
    bt.TAKER_FEE = cm.taker_fee if cm.fees_applied else 0.0
    bt.MAKER_FEE = cm.maker_fee if cm.fees_applied else 0.0
    bt.FUNDING_PER_8H = cm.funding_per_8h if cm.funding_applied else 0.0
    bt.BASE_SLIPPAGE_BPS = cm.base_slippage_bps if cm.slippage_applied else 0.0

    p = Params(**{k: v for k, v in params.items() if k in Params.__dataclass_fields__})
    summary, _state = run_v40_backtest(ds.candles, ds.candles, p, ds.symbol)
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", required=True,
                    help='"synthetic:momentum[:seed]" or "BTCUSDT:1"')
    ap.add_argument("--name", default="experiment")
    ap.add_argument("--hypothesis", default="(none stated)")
    ap.add_argument("--rr-ratio", type=float, default=2.0)
    ap.add_argument("--sl-atr-mult", type=float, default=2.5)
    ap.add_argument("--leverage", type=float, default=2.0)
    ap.add_argument("--split", action="store_true",
                    help="train/test split with embargo; reports OOS decay")
    ap.add_argument("--train-frac", type=float, default=0.6)
    ap.add_argument("--embargo-bars", type=int, default=240)
    ap.add_argument("--null-baseline", default="synthetic:random_walk",
                    help="dataset used for the null-rejection gate; '' to skip")
    ap.add_argument("--n-trials", type=int, default=1,
                    help="how many configurations were searched before this one "
                         "(drives the multiple-testing deflation)")
    ap.add_argument("--no-costs", action="store_true",
                    help="run frictionless — the record is flagged and can never "
                         "be quoted as profitability")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()

    if args.list:
        print("cached:", available_datasets() or "(none)")
        print("synthetic: random_walk, momentum, mean_reverting, regime_shift")
        return 0

    cm = CostModel(fees_applied=not args.no_costs,
                   funding_applied=not args.no_costs,
                   slippage_applied=not args.no_costs,
                   taker_fee=0.0 if args.no_costs else 0.00055,
                   maker_fee=0.0 if args.no_costs else 0.00020,
                   funding_per_8h=0.0 if args.no_costs else 0.0001,
                   base_slippage_bps=0.0 if args.no_costs else 4.0)

    params = {"rr_ratio": args.rr_ratio, "sl_atr_mult": args.sl_atr_mult,
              "leverage": args.leverage}

    ds = load_dataset(args.dataset)
    print(f"dataset: {json.dumps(ds.describe(), default=str)}")
    print(f"costs:   {cm.describe()}")

    is_metric = oos_metric = None
    if args.split:
        train, test = ds.split(args.train_frac, args.embargo_bars)
        print(f"split:   train={len(train)} bars  embargo={args.embargo_bars}  test={len(test)} bars")
        rec_tr = run_experiment(f"{args.name}-train", args.hypothesis, train, params,
                                backtest_runner, cm)
        rec = run_experiment(f"{args.name}-test", args.hypothesis, test, params,
                             backtest_runner, cm)
        is_metric = float(rec_tr.results.get("expectancy_per_trade", 0.0) or 0.0)
        oos_metric = float(rec.results.get("expectancy_per_trade", 0.0) or 0.0)
        print(f"  train: {rec_tr.summary_line()}")
    else:
        rec = run_experiment(args.name, args.hypothesis, ds, params, backtest_runner, cm)

    null_exp = null_n = None
    if args.null_baseline:
        null_ds = load_dataset(args.null_baseline)
        null_rec = run_experiment(f"{args.name}-null", "instrument must find no edge in noise",
                                  null_ds, params, backtest_runner, cm)
        null_exp = float(null_rec.results.get("expectancy_per_trade", 0.0) or 0.0)
        null_n = int(null_rec.results.get("trades", 0) or 0)
        print(f"  null:  {null_rec.summary_line()}")

    report = V.validate_result(
        rec.results, rec.cost_model,
        null_expectancy=null_exp, null_trades=null_n or 0,
        is_metric=is_metric, oos_metric=oos_metric,
        n_trials=args.n_trials,
    )
    rec.validation = report.to_dict()
    path = rec.save()

    print("\n" + rec.summary_line())
    print(report.render())
    print(f"\nrecord: {path}")
    if rec.error:
        print(f"ERROR RECORDED: {rec.error}")
    return 0 if report.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
