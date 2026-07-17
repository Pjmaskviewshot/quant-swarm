-- 1. Wipe the old, outdated table (and clear the fake PnL hack data)
DROP TABLE IF EXISTS quantitative_ledger;

-- 2. Create the ultimate advanced forensic ledger table
CREATE TABLE quantitative_ledger (
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

    -- 🚀 NEW: Dedicated Virtual Brackets (Fixes the PnL Hack)
    virtual_sl NUMERIC DEFAULT 0.0,
    virtual_tp NUMERIC DEFAULT 0.0,
    
    -- 🚀 NEW: Shadow Swarm Flag (Protects FSM Accuracy)
    is_shadow BOOLEAN DEFAULT FALSE,
    
    -- Execution Resolution (The "Outcome")
    resolved BOOLEAN DEFAULT FALSE,
    actual_outcome TEXT,
    is_correct BOOLEAN DEFAULT FALSE,
    net_pnl NUMERIC DEFAULT 0.0,
    slippage_drag NUMERIC DEFAULT 0.0,

    -- 🚀 NEW v2: true economics attribution (all optional, backward compatible)
    fees_usdt NUMERIC DEFAULT 0.0,        -- taker/maker fees actually paid
    funding_usdt NUMERIC DEFAULT 0.0,     -- perpetual funding paid/received
    leverage NUMERIC DEFAULT 1.0,         -- leverage used on the live leg
    holding_minutes NUMERIC DEFAULT 0.0,  -- trade duration for expectancy-by-horizon analysis
    execution_mode TEXT DEFAULT 'GHOST'   -- GHOST | MAKER_PEG | FLASH_STRIKE | RECOVERY
);

-- 3. Create high-speed indexes for the Telegram reporting queries
CREATE INDEX idx_ledger_resolved ON quantitative_ledger(resolved);
CREATE INDEX idx_ledger_regime ON quantitative_ledger(market_regime);
CREATE INDEX idx_ledger_timestamp ON quantitative_ledger(timestamp DESC);

-- 🚀 NEW: High-speed index to keep FSM accuracy tracking blazing fast
CREATE INDEX idx_ledger_is_shadow ON quantitative_ledger(is_shadow);