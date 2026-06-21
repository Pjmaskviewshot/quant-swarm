-- 1. Create the advanced forensic ledger table
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
    
    -- Execution Resolution (The "Outcome")
    resolved BOOLEAN DEFAULT FALSE,
    actual_outcome TEXT,
    is_correct BOOLEAN DEFAULT FALSE,
    net_pnl NUMERIC DEFAULT 0.0,
    slippage_drag NUMERIC DEFAULT 0.0
);

-- 2. Create high-speed indexes for the Telegram reporting queries
CREATE INDEX IF NOT EXISTS idx_ledger_resolved ON quantitative_ledger(resolved);
CREATE INDEX IF NOT EXISTS idx_ledger_regime ON quantitative_ledger(market_regime);
CREATE INDEX IF NOT EXISTS idx_ledger_timestamp ON quantitative_ledger(timestamp DESC);