"""P0-B7 + P0-NEW-1 + B26 — runtime configuration, mode safety, build identity."""
import pytest

from runtime_config import (
    RuntimeConfig, TradingMode, ConfigurationError, get_config,
    reset_config_for_tests, resolve_build_revision,
    is_banned_symbol, normalise_base_asset,
)

MODE_VARS = ("LIVE_TRADING", "TRADING_MODE", "TEST_MODE",
             "BYBIT_API_KEY", "BYBIT_API_SECRET")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for v in MODE_VARS:
        monkeypatch.delenv(v, raising=False)
    reset_config_for_tests()
    yield
    reset_config_for_tests()


def creds(monkeypatch):
    monkeypatch.setenv("BYBIT_API_KEY", "k" * 16)
    monkeypatch.setenv("BYBIT_API_SECRET", "s" * 16)


# --- B7: mode resolution ---------------------------------------------------

def test_default_is_paper_not_live(monkeypatch):
    """The headline B7 case: an empty environment must never trade real money."""
    creds(monkeypatch)
    cfg = RuntimeConfig.from_env()
    assert cfg.mode is TradingMode.PAPER
    assert cfg.mode.places_real_orders is False
    assert cfg.is_live is False


def test_credentials_alone_do_not_enable_live(monkeypatch):
    creds(monkeypatch)
    assert RuntimeConfig.from_env().mode is TradingMode.PAPER


def test_live_requires_explicit_flag_and_credentials(monkeypatch):
    creds(monkeypatch)
    monkeypatch.setenv("LIVE_TRADING", "1")
    cfg = RuntimeConfig.from_env()
    assert cfg.mode is TradingMode.LIVE and cfg.is_live


def test_live_flag_without_credentials_refuses_to_boot(monkeypatch):
    monkeypatch.setenv("LIVE_TRADING", "1")
    with pytest.raises(ConfigurationError, match="Refusing to boot"):
        RuntimeConfig.from_env()


def test_trading_mode_live_without_flag_refuses(monkeypatch):
    creds(monkeypatch)
    monkeypatch.setenv("TRADING_MODE", "LIVE")
    with pytest.raises(ConfigurationError, match="LIVE_TRADING=1"):
        RuntimeConfig.from_env()


def test_unknown_trading_mode_refuses(monkeypatch):
    creds(monkeypatch)
    monkeypatch.setenv("TRADING_MODE", "YOLO")
    with pytest.raises(ConfigurationError):
        RuntimeConfig.from_env()


def test_legacy_test_mode_maps_to_testnet(monkeypatch):
    creds(monkeypatch)
    monkeypatch.setenv("TEST_MODE", "true")
    cfg = RuntimeConfig.from_env()
    assert cfg.mode is TradingMode.TESTNET
    assert cfg.mode.uses_testnet_endpoints is True


def test_paper_places_no_real_orders(monkeypatch):
    creds(monkeypatch)
    assert RuntimeConfig.from_env().mode.places_real_orders is False


@pytest.mark.parametrize("mode,real", [
    (TradingMode.PAPER, False), (TradingMode.TESTNET, True), (TradingMode.LIVE, True),
])
def test_mode_order_semantics(mode, real):
    assert mode.places_real_orders is real


# --- NEW-1: build identity -------------------------------------------------

def test_build_revision_prefers_explicit_env(monkeypatch):
    monkeypatch.setenv("BUILD_SHA", "deadbeef" * 5)
    assert resolve_build_revision() == "deadbeef" * 5


def test_build_revision_reads_platform_vars(monkeypatch):
    monkeypatch.delenv("BUILD_SHA", raising=False)
    monkeypatch.setenv("RENDER_GIT_COMMIT", "abc123")
    assert resolve_build_revision() == "abc123"


def test_build_revision_falls_back_to_git(monkeypatch):
    for v in ("BUILD_SHA", "RENDER_GIT_COMMIT", "RAILWAY_GIT_COMMIT_SHA", "GITHUB_SHA"):
        monkeypatch.delenv(v, raising=False)
    rev = resolve_build_revision()
    assert rev == "unknown" or len(rev) == 40


def test_banner_flags_unknown_revision(monkeypatch):
    cfg = RuntimeConfig(mode=TradingMode.PAPER, build_revision="unknown")
    assert "unverifiable" in cfg.banner()


def test_banner_warns_on_live(monkeypatch):
    cfg = RuntimeConfig(mode=TradingMode.LIVE, build_revision="a" * 40)
    b = cfg.banner()
    assert "MODE=LIVE" in b and "REAL CAPITAL AT RISK" in b


# --- B26: exact-match bans -------------------------------------------------

@pytest.mark.parametrize("symbol,base", [
    ("BTCUSDT", "BTC"), ("1000PEPEUSDT", "PEPE"), ("ETHUSDT", "ETH"),
    ("10000SATSUSDT", "SATS"), ("SOLUSDT", "SOL"),
])
def test_base_asset_normalisation(symbol, base):
    assert normalise_base_asset(symbol) == base


@pytest.mark.parametrize("symbol", ["AAPLUSDT", "TSLAUSDT", "KOUSDT", "USDCUSDT",
                                    "PRE-FOOUSDT", "INNO-BARUSDT"])
def test_banned_symbols_are_banned(symbol):
    assert is_banned_symbol(symbol) is True


@pytest.mark.parametrize("symbol", [
    "BTCUSDT", "ETHUSDT", "SOLUSDT",
    "OKBUSDT",      # contains "KB" not "KO", must not trip
    "ARBUSDT",      # legitimate; substring matching once risked "ARM"-style hits
    "KOMAUSDT",     # starts with "KO" but base is KOMA -- previously banned
    "AMDAUSDT",     # base AMDA, not AMD
    "BANKERUSDT",   # base BANKER, not BANK
])
def test_legitimate_symbols_not_banned(symbol):
    assert is_banned_symbol(symbol) is False, (
        f"B26: substring matching false-positive on {symbol}"
    )


def test_get_config_is_cached(monkeypatch):
    creds(monkeypatch)
    assert get_config() is get_config()
