"""
V12 decision architecture.

    MARKET DATA -> FEATURES -> MULTI-HORIZON FORECASTS (1m/5m/15m/1h/4h)
                -> REGIME -> EDGE (under the real exit policy, after all costs)
                -> RISK (vol/horizon-aware stop, edge-and-vol sizing)
                -> GUARDIAN (5-level kill switches, reconciliation, promotion)
                -> EXECUTION -> EXIT ENGINE -> JOURNAL -> LEARNING
                                                      (recommends; never auto-deploys)

Every module is pure and deterministic given its inputs, so the SAME code runs
in the live bot and in the walk-forward backtester. A result measured in the
backtest is a result about the code that trades.
"""
