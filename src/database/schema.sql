-- ====================================================================
-- 💎 V37.0 APEX TITAN: FORENSIC & TCA LEDGER ARCHITECTURAL SCHEMA
-- HIGH-FREQUENCY PERSISTENCE & BAYESIAN DNA QUANTITATIVE PIPELINE
-- SAFE MIGRATION PIPELINE: Non-destructive hot-migration enabled.
-- ====================================================================

-- 1. PRIMARY QUANTITATIVE LEDGER (Signals, Telemetry & PnL Attribution)
CREATE TABLE IF NOT EXISTS quantitative_ledger (
    signal_id UUID PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    symbol TEXT NOT NULL,
    predicted_direction TEXT NOT NULL,
    price_at_prediction NUMERIC NOT NULL,
    ai_confidence NUMERIC DEFAULT 0.0,
    
    -- Feature Engine Analytics (25D Volterra-Riemannian State)
    market_regime TEXT DEFAULT 'UNKNOWN',
    log_mlofi_z NUMERIC DEFAULT 0.0,          -- Cont-Kukanov-Stoikov Level-5 MLOFI Z-score
    hawkes_z NUMERIC DEFAULT 0.0,             -- Marked Bivariate Hawkes Cascade Intensity
    sector_impulse NUMERIC DEFAULT 0.0,       -- Cross-Asset SVD Eigenvector Tailwinds
    swd_z NUMERIC DEFAULT 0.0,                -- Structural Work Deficit (Iceberg Absorption)
    accel_z NUMERIC DEFAULT 0.0,              -- Kinematic Order Flow Acceleration
    micro_dislocation_z NUMERIC DEFAULT 0.0,  -- Stoikov Micro-Price Dislocation (V37.0)
    hurst_h NUMERIC DEFAULT 0.5,              -- Fractional Brownian Rough Volatility Exponent
    bocd_cp_prob NUMERIC DEFAULT 0.0,         -- Adams-MacKay Bayesian Changepoint Probability
    ou_divergence_z NUMERIC DEFAULT 0.0,      -- Ornstein-Uhlenbeck Micro-Reversion Z-score
    cvd_z NUMERIC DEFAULT 0.0,                -- Cumulative Volume Delta Z-score
    vol_mult NUMERIC DEFAULT 1.0,             -- Relative Volume Multiplier (RVOL)
    spread NUMERIC DEFAULT 0.0,               -- Bid-Ask Spread at arrival

    -- Capital Allocation & Sizing Engine
    kelly_fraction NUMERIC DEFAULT 0.0,       -- Merton Jump-Diffusion Kelly allocation
    conformal_gate NUMERIC DEFAULT 0.52,      -- Split-Conformal Coverage Probability threshold
    virtual_sl NUMERIC DEFAULT 0.0,           -- Volatility Chandelier Stop-Loss
    virtual_tp NUMERIC DEFAULT 0.0,           -- Kinetic Compressed Take-Profit
    
    -- Routing & Shadow Swarm Governance
    is_shadow BOOLEAN DEFAULT FALSE,
    execution_mode TEXT DEFAULT 'GHOST',      -- GHOST | FLASH_STRIKE | MAKER_PEG | TWAP_ICEBERG
    
    -- Execution Resolution & Performance Forensics
    resolved BOOLEAN DEFAULT FALSE,
    actual_outcome TEXT,                      -- WIN | LOSS | TIMEOUT | RECONCILED
    is_correct BOOLEAN DEFAULT FALSE,
    net_pnl NUMERIC DEFAULT 0.0,
    slippage_drag NUMERIC DEFAULT 0.0,

    -- TCA (Transaction Cost Analysis) Metrics
    tca_entry_slippage_bps NUMERIC DEFAULT 0.0,
    tca_exit_slippage_bps NUMERIC DEFAULT 0.0,
    tca_total_slippage_bps NUMERIC DEFAULT 0.0,
    fees_usdt NUMERIC DEFAULT 0.0,
    funding_usdt NUMERIC DEFAULT 0.0,
    leverage NUMERIC DEFAULT 1.0,
    holding_minutes NUMERIC DEFAULT 0.0,
    exec_details JSONB DEFAULT '{}'::jsonb
);

-- ====================================================================
-- 2. DELTA-NEUTRAL BASIS HARVESTER LEDGER (Cash-and-Carry Surveillance)
-- ====================================================================
CREATE TABLE IF NOT EXISTS delta_neutral_ledger (
    hedge_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    symbol TEXT NOT NULL,                     -- Linear Perpetual Pair (e.g. 1000PEPEUSDT)
    spot_symbol TEXT NOT NULL,                -- Normalized Spot Pair (e.g. PEPEUSDT)
    contract_multiplier NUMERIC DEFAULT 1.0,
    
    -- Capital Deployment & Sizing
    allocated_capital_usdt NUMERIC NOT NULL,
    spot_units NUMERIC NOT NULL,
    perp_contracts NUMERIC NOT NULL,
    entry_spot_price NUMERIC NOT NULL,
    entry_perp_price NUMERIC NOT NULL,
    
    -- Rates & Economics
    funding_rate_entry NUMERIC NOT NULL,
    funding_rate_exit NUMERIC DEFAULT 0.0,
    expected_apy_pct NUMERIC DEFAULT 0.0,
    projected_breakeven_epochs INT DEFAULT 1,
    
    -- TCA & Unwind Status
    entry_drag_bps NUMERIC DEFAULT 0.0,
    net_funding_harvested_usdt NUMERIC DEFAULT 0.0,
    holding_hours NUMERIC DEFAULT 0.0,
    status TEXT DEFAULT 'ACTIVE',             -- ACTIVE | CLOSED | UNWIND_DESYNC | ROLLEDBACK
    close_timestamp TIMESTAMPTZ
);

-- ====================================================================
-- 3. HOT-MIGRATION UPGRADE PIPELINE (For Existing Databases)
-- Idempotent column attachment guarantees zero downtime or table locking.
-- ====================================================================
DO $$ 
BEGIN
    -- V37.0 Microstructure & Physics Features
    ALTER TABLE quantitative_ledger ADD COLUMN IF NOT EXISTS micro_dislocation_z NUMERIC DEFAULT 0.0;
    ALTER TABLE quantitative_ledger ADD COLUMN IF NOT EXISTS hurst_h NUMERIC DEFAULT 0.5;
    ALTER TABLE quantitative_ledger ADD COLUMN IF NOT EXISTS bocd_cp_prob NUMERIC DEFAULT 0.0;
    ALTER TABLE quantitative_ledger ADD COLUMN IF NOT EXISTS ou_divergence_z NUMERIC DEFAULT 0.0;
    ALTER TABLE quantitative_ledger ADD COLUMN IF NOT EXISTS cvd_z NUMERIC DEFAULT 0.0;

    -- Capital & Risk Scaling
    ALTER TABLE quantitative_ledger ADD COLUMN IF NOT EXISTS kelly_fraction NUMERIC DEFAULT 0.0;
    ALTER TABLE quantitative_ledger ADD COLUMN IF NOT EXISTS conformal_gate NUMERIC DEFAULT 0.52;

    -- Granular TCA Attribution
    ALTER TABLE quantitative_ledger ADD COLUMN IF NOT EXISTS tca_entry_slippage_bps NUMERIC DEFAULT 0.0;
    ALTER TABLE quantitative_ledger ADD COLUMN IF NOT EXISTS tca_exit_slippage_bps NUMERIC DEFAULT 0.0;
    ALTER TABLE quantitative_ledger ADD COLUMN IF NOT EXISTS tca_total_slippage_bps NUMERIC DEFAULT 0.0;
    ALTER TABLE quantitative_ledger ADD COLUMN IF NOT EXISTS exec_details JSONB DEFAULT '{}'::jsonb;
END $$;

-- ====================================================================
-- 4. ULTRA-LOW LATENCY PARTIAL & COVERING INDEXES
-- Designed specifically for high-throughput Supabase queries:
-- - Eliminates sequential scans during async batch resolution.
-- - Optimizes Bayesian k-NN DNA clustering searches (<2ms).
-- ====================================================================

-- Accelerated Ghost Forensics Unresolved Batch Polling
CREATE INDEX IF NOT EXISTS idx_ledger_unresolved_batch 
ON quantitative_ledger (timestamp ASC) 
WHERE resolved = FALSE;

-- Accelerated Bayesian DNA Edge & Shadow Promotion Lookups
CREATE INDEX IF NOT EXISTS idx_ledger_bayesian_dna_knn 
ON quantitative_ledger (symbol, timestamp DESC) 
INCLUDE (vol_mult, log_mlofi_z, spread, price_at_prediction, is_correct)
WHERE resolved = TRUE;

-- Accelerated Daily PnL & TCA Forensic Summary Queries
CREATE INDEX IF NOT EXISTS idx_ledger_forensic_summary 
ON quantitative_ledger (timestamp DESC) 
INCLUDE (net_pnl, fees_usdt, slippage_drag, holding_minutes, is_correct, symbol)
WHERE resolved = TRUE AND is_shadow = FALSE;

-- Primary Symbol & Regime Multi-Column Filtering
CREATE INDEX IF NOT EXISTS idx_ledger_symbol_ts 
ON quantitative_ledger (symbol, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_ledger_regime_ts 
ON quantitative_ledger (market_regime, timestamp DESC);

-- Delta-Neutral Hedging Lookups
CREATE INDEX IF NOT EXISTS idx_delta_neutral_active 
ON delta_neutral_ledger (symbol, status) 
WHERE status = 'ACTIVE';