"""P1 — B15/B16/B12 ledger and settlement integrity.

B16: `target_notional` was sent on every insert but absent from the schema, so
     PostgREST rejected the row and the error was swallowed at logger.debug.
B15: shadow PnL (a return FRACTION) shared the net_pnl column with realised
     USDT, and promotion/DNA queries did not filter on is_shadow.
B12: a failed closed-PnL poll left net_pnl=0.0 / is_correct=False, so unknown
     outcomes trained the model as losses.
"""
import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parents[1]
SCHEMA = (REPO / "src" / "database" / "schema.sql").read_text()
MEMORY = (REPO / "src" / "core" / "memory.py").read_text()
MAIN = (REPO / "src" / "main.py").read_text()


# --- B16: schema matches payloads -----------------------------------------

def test_schema_declares_target_notional():
    assert "target_notional" in SCHEMA, (
        "B16: memory.py sends target_notional on every insert; the schema must declare it"
    )


def test_schema_migration_adds_new_columns_idempotently():
    for col in ("target_notional", "shadow_return_fraction", "settlement_status"):
        assert re.search(rf"ADD COLUMN IF NOT EXISTS {col}", SCHEMA), (
            f"{col} missing from the idempotent migration block"
        )


def test_every_insert_payload_key_exists_in_schema():
    """Guards against the exact class of defect B16 represents."""
    block = MEMORY.split("payload = {", 1)[1].split("}", 1)[0]
    keys = set(re.findall(r'"([a-z_]+)":', block))
    ignored = {"exec_details"}
    missing = [k for k in keys - ignored if k not in SCHEMA]
    assert not missing, f"B16: insert payload keys absent from schema: {sorted(missing)}"


def test_db_write_counters_exist():
    for counter in ("db_write_success", "db_write_failure", "db_write_retry"):
        assert counter in MEMORY, f"B16: missing {counter} observability counter"


def test_db_failures_are_not_silently_debug_logged():
    assert "_record_db_failure" in MEMORY
    assert "CLOUD LEDGER DEGRADED" in MEMORY, (
        "a sustained DB failure rate must escalate, not stay at debug level"
    )


# --- B15: shadow / live separation ----------------------------------------

def test_shadow_resolution_writes_fraction_not_net_pnl():
    assert '"shadow_return_fraction": float(net_pnl)' in MEMORY, (
        "B15: shadow outcomes must be written to the fraction column"
    )


def test_promotion_query_filters_shadow_rows():
    promo = MEMORY.split("async def evaluate_shadow_promotion", 1)[1][:1600]
    assert "is_shadow = 1" in promo, (
        "B15: promotion statistics must not span live executions"
    )
    assert "shadow_return_fraction" in promo, (
        "B15: promotion Sharpe must read the shadow-unit column"
    )


def test_dna_query_filters_shadow_rows():
    dna = MEMORY.split("async def compute_latent_dna_edge", 1)[1][:2600]
    assert "is_shadow = 1" in dna, "B15: DNA k-NN must use the shadow population only"


def test_schema_has_separate_shadow_and_live_indexes():
    assert "idx_ledger_dna_knn_shadow" in SCHEMA
    assert "idx_ledger_dna_knn_live" in SCHEMA


# --- B12: UNKNOWN settlement ----------------------------------------------

def test_settlement_defaults_to_unknown_not_reconciled():
    assert 'real_outcome, slippage_bps, fees, exit_price = 0.0, "UNKNOWN"' in MAIN, (
        "B12: an unpolled settlement must start as UNKNOWN, not a zero-PnL scratch"
    )


def test_settlement_resolved_flag_gates_learning():
    assert "settlement_resolved" in MAIN
    assert "if settlement_resolved and ctx.get(\"stat_engine\")" in MAIN, (
        "B12: RLS must not train on an unresolved outcome"
    )


def test_unknown_outcome_skips_pnl_history():
    assert "if settlement_resolved:\n                self.recent_pnl_history.append(net_pnl)" in MAIN


def test_unknown_outcome_skips_quarantine():
    assert "if settlement_resolved and net_pnl < 0:" in MAIN, (
        "B12: an unknown outcome must not be treated as a loss for quarantine"
    )


def test_fills_fallback_exists():
    assert "_reconstruct_pnl_from_fills" in MAIN
    assert "/v5/execution/list" in MAIN, (
        "B12: execution records are the fallback evidence when closed-PnL is unavailable"
    )


def test_is_correct_is_null_for_unknown():
    assert "is_correct = None if is_unknown else (net_pnl > 0)" in MEMORY, (
        "B12: UNKNOWN must be NULL, never False"
    )


def test_forensic_summary_excludes_unknown():
    assert "is_correct IS NOT NULL" in MEMORY, (
        "B12: unresolved rows must not enter the win-rate statistic"
    )


def test_discard_pending_outcome_exists():
    mm = (REPO / "src" / "features" / "micro_models.py").read_text()
    assert "def discard_pending_outcome" in mm, (
        "B12: an unresolved signal must be discardable without training on it"
    )
