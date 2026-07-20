-- ====================================================================
-- 🌌 V20.2 PRODUCTION-GRADE ARCHITECTURAL SCHEMA
-- SAFE MIGRATION PIPELINE: Nuking live historical data is forbidden.
-- ====================================================================

-- 1. Create the ultimate advanced forensic ledger table safely
CREATE TABLE IF NOT EXISTS quantitative_ledger (
    signal_id UUID PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    symbol TEXT NOT NULL,
    predicted_direction TEXT NOT NULL,
    price_at_prediction NUMERIC NOT NULL,
    ai_confidence NUMERIC DEFAULT 0.0,
    
    -- Feature Engine Analytics (The "Why")
    market_regime TEXT DEFAULT 'UNKNOWN',
    z_obi NUMERIC DEFAULT 0.0,
    vol_mult NUMERIC DEFAULT 1.0,
    spread NUMERIC DEFAULT 0.0,

    -- Dedicated Virtual Brackets (Fixes the PnL Drift)
    virtual_sl NUMERIC DEFAULT 0.0,
    virtual_tp NUMERIC DEFAULT 0.0,
    
    -- Shadow Swarm Flag (Protects FSM Accuracy)
    is_shadow BOOLEAN DEFAULT FALSE,
    
    -- Execution Resolution (The "Outcome")
    resolved BOOLEAN DEFAULT FALSE,
    actual_outcome TEXT,
    is_correct BOOLEAN DEFAULT FALSE,
    net_pnl NUMERIC DEFAULT 0.0,
    slippage_drag NUMERIC DEFAULT 0.0,

    -- True Economics Attribution (Backward Compatible)
    fees_usdt NUMERIC DEFAULT 0.0,        -- Taker/Maker fees actually paid
    funding_usdt NUMERIC DEFAULT 0.0,     -- Perpetual funding paid/received
    leverage NUMERIC DEFAULT 1.0,         -- Leverage used on the live leg
    holding_minutes NUMERIC DEFAULT 0.0,  -- Trade duration for horizon analytics
    execution_mode TEXT DEFAULT 'GHOST'   -- GHOST | MAKER_PEG | FLASH_STRIKE | RECOVERY
);

-- 2. Create high-speed indexes for the reporting engines safely
CREATE INDEX IF NOT EXISTS idx_ledger_resolved ON quantitative_ledger(resolved);
CREATE INDEX IF NOT EXISTS idx_ledger_regime ON quantitative_ledger(market_regime);
CREATE INDEX IF NOT EXISTS idx_ledger_timestamp ON quantitative_ledger(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ledger_is_shadow ON quantitative_ledger(is_shadow);