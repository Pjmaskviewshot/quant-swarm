"""
Configuration drift guard.

APEX section 25: 28 environment variables governed this system — including the
drawdown kill switch and whether real orders are placed — and none were
documented. Documentation written once rots immediately, so this test fails the
build when code and `.env.example` disagree in either direction:

  * a variable read by the code but absent from the file  -> undocumented knob
  * a variable in the file that no code reads             -> stale instruction,
                                                             which is worse than
                                                             none, because an
                                                             operator will set it
                                                             and believe it took
                                                             effect
"""
import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parents[1]
ENV_EXAMPLE = REPO / ".env.example"

# Names read only inside tests or vendored code, deliberately not operator-facing.
NOT_OPERATOR_FACING: set = set()

_PATTERNS = [
    re.compile(r"os\.getenv\(\s*[\"']([A-Z][A-Z0-9_]*)[\"']"),
    re.compile(r"os\.environ\.get\(\s*[\"']([A-Z][A-Z0-9_]*)[\"']"),
    re.compile(r"os\.environ\[\s*[\"']([A-Z][A-Z0-9_]*)[\"']"),
]


def env_vars_in_code() -> set:
    found = set()
    for d in ("src", "scripts"):
        for p in (REPO / d).rglob("*.py"):
            text = p.read_text(encoding="utf-8", errors="ignore")
            for pat in _PATTERNS:
                found.update(pat.findall(text))
    return found - NOT_OPERATOR_FACING


def env_vars_documented() -> set:
    documented = set()
    for line in ENV_EXAMPLE.read_text().splitlines():
        s = line.strip().lstrip("#").strip()
        m = re.match(r"^([A-Z][A-Z0-9_]*)=", s)
        if m:
            documented.add(m.group(1))
    return documented


def test_env_example_exists():
    assert ENV_EXAMPLE.exists(), "operators have nothing to copy from"


def test_every_variable_the_code_reads_is_documented():
    missing = sorted(env_vars_in_code() - env_vars_documented())
    assert not missing, (
        f"undocumented environment variables: {missing}\n"
        f"Add each to .env.example with what it does and what the default is."
    )


def test_no_stale_variables_are_documented():
    stale = sorted(env_vars_documented() - env_vars_in_code())
    assert not stale, (
        f".env.example documents variables no code reads: {stale}\n"
        f"An operator will set these and believe they took effect."
    )


def test_the_example_file_carries_no_real_credentials():
    """A filled-in example is the classic accidental secret commit."""
    text = ENV_EXAMPLE.read_text()
    for name in ("BYBIT_API_KEY", "BYBIT_API_SECRET", "SUPABASE_KEY",
                 "TELEGRAM_BOT_TOKEN", "HEALTH_TOKEN"):
        for line in text.splitlines():
            if line.strip().startswith(f"{name}="):
                assert line.strip() == f"{name}=", (
                    f"{name} has a value in .env.example — that is a committed secret"
                )


def test_the_default_mode_is_not_live():
    """Live trading must be an explicit act, never a default."""
    for line in ENV_EXAMPLE.read_text().splitlines():
        if line.strip().startswith("TRADING_MODE="):
            assert line.strip().split("=", 1)[1].upper() != "LIVE"


def test_gitignore_excludes_the_filled_copy():
    gi = (REPO / ".gitignore")
    assert gi.exists(), "no .gitignore — a filled .env would be committable"
    body = gi.read_text()
    assert ".env" in body, ".env is not ignored; a filled copy could be committed"


def test_the_risk_limits_are_documented_with_their_effect():
    """A number with no explanation gets changed by someone who doesn't know."""
    text = ENV_EXAMPLE.read_text()
    for name in ("MAX_DRAWDOWN_PCT", "MAX_SINGLE_POSITION_RISK_PCT",
                 "MIN_REQUIRED_EQUITY", "NOTIONAL_DEVIATION_TOLERANCE",
                 "MAX_ORDERBOOK_AGE_SEC", "SKIP_BELOW_MIN_NOTIONAL"):
        idx = text.index(f"{name}=")
        preceding = text[:idx].rsplit("\n\n", 1)[-1]
        assert preceding.count("#") >= 1, f"{name} has no explanatory comment"
