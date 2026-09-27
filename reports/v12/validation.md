# V12 validation (synthetic, walk-forward, TEST segments pooled)

## NOISE (no edge exists)
decisions 60000, entries 0, TEST trades 0
- per seed (seed, trades, net): [(0, 0, 0), (1, 0, 0), (2, 0, 0), (3, 0, 0), (4, 0, 0), (5, 0, 0)]

## WEAK TREND (drift 0.15)
decisions 57227, entries 20, TEST trades 20
- Expectancy **+135.5 bps** (+1.64R), t = 4.42
- Profit factor 7.66; net P&L +69.50 on 1000 per seed
- Max drawdown 0.6%; fees 11.1 bps/trade; funding +0.184
- MFE captured 92%; avg MAE -0.40R
- Predicted net edge +11.8 bps vs realised +135.5 bps
- Win rate 75%; exits {'HORIZON_EXHAUSTION': 3, 'EXCHANGE_TP': 12, 'ADVERSE_STAGNATION_SCRATCH': 3, 'EXCHANGE_STOP': 2}
- per seed (seed, trades, net): [(0, 0, 0), (1, 20, 69.50144976217636), (2, 0, 0), (3, 0, 0), (4, 0, 0), (5, 0, 0)]

## TREND (drift 0.30)
decisions 31837, entries 243, TEST trades 168
- Expectancy **+166.8 bps** (+1.86R), t = 15.62
- Profit factor 13.82; net P&L +782.83 on 1000 per seed
- Max drawdown 1.2%; fees 11.0 bps/trade; funding -0.117
- MFE captured 88%; avg MAE -0.29R
- Predicted net edge +15.5 bps vs realised +166.8 bps
- Win rate 82%; exits {'ADVERSE_STAGNATION_SCRATCH': 13, 'EXCHANGE_STOP': 13, 'HORIZON_EXHAUSTION': 16, 'EXCHANGE_TP': 114, 'RUNNER_TRAIL_STOP': 12}
- per seed (seed, trades, net): [(0, 25, 62.431205433295396), (1, 43, 275.4231774739163), (2, 40, 193.29166250453258), (3, 0, 0), (4, 34, 168.00960408430547), (5, 26, 83.67838240172212)]

## TREND, 2x COSTS + 10s latency
decisions 53276, entries 41, TEST trades 39
- Expectancy **+167.7 bps** (+1.22R), t = 4.49
- Profit factor 5.76; net P&L +146.34 on 1000 per seed
- Max drawdown 0.7%; fees 22.1 bps/trade; funding +0.059
- MFE captured 84%; avg MAE -0.34R
- Predicted net edge +8.7 bps vs realised +167.7 bps
- Win rate 67%; exits {'ADVERSE_STAGNATION_SCRATCH': 7, 'EXCHANGE_STOP': 6, 'HORIZON_EXHAUSTION': 1, 'EXCHANGE_TP': 19, 'RUNNER_TRAIL_STOP': 6}
- per seed (seed, trades, net): [(0, 10, 5.236842156399254), (1, 16, 121.240975051886), (2, 6, 10.668900913141254), (3, 1, 2.464466878085983), (4, 6, 6.733335485962328), (5, 0, 0)]
