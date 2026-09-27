# V12 validation (synthetic, walk-forward, TEST segments pooled)

## NOISE, fat tails + volatility clustering (no edge)
decisions 60000, entries 0, TEST trades 0
- per seed (seed, trades, net): [(0, 0, 0), (1, 0, 0), (2, 0, 0), (3, 0, 0), (4, 0, 0), (5, 0, 0)]

## MEAN-REVERTING (OU, no trend)
decisions 60000, entries 0, TEST trades 0
- per seed (seed, trades, net): [(0, 0, 0), (1, 0, 0), (2, 0, 0), (3, 0, 0), (4, 0, 0), (5, 0, 0)]

## REGIME SWITCHING (trend 0.30 in 1/3 of time, noise otherwise)
decisions 52527, entries 62, TEST trades 48
- Expectancy **+61.2 bps** (+0.69R), t = 2.90
- Profit factor 2.83; net P&L +74.46 on 1000 per seed
- Max drawdown 1.3%; fees 11.0 bps/trade; funding +0.044
- MFE captured 81%; avg MAE -0.50R
- Predicted net edge +10.3 bps vs realised +61.2 bps
- Win rate 56%; exits {'HORIZON_EXHAUSTION': 11, 'RUNNER_TRAIL_STOP': 2, 'ADVERSE_STAGNATION_SCRATCH': 8, 'EXCHANGE_STOP': 10, 'EXCHANGE_TP': 17}
- per seed (seed, trades, net): [(0, 22, 43.73334088434108), (1, 25, 33.19597224072962), (2, 0, 0), (3, 0, 0), (4, 1, -2.4686994106753763), (5, 0, 0)]
