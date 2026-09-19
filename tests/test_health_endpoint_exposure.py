"""
The health server binds 0.0.0.0 — every route is public.

APEX section 25 found that `/metrics` published `METRICS.snapshot()`
unauthenticated, and main.py publishes `equity` and `wallet_balance` as gauges.
The live account balance was therefore readable by anyone who found the URL,
along with counters that leak position activity and timing.

These tests pin the fail-closed behaviour: without HEALTH_TOKEN the detailed
routes are OFF, so forgetting to configure it exposes nobody.
"""
import importlib

import pytest

flask = pytest.importorskip("flask", reason="health server requires flask")


@pytest.fixture
def client(monkeypatch):
    monkeypatch.delenv("HEALTH_TOKEN", raising=False)
    import keep_alive
    importlib.reload(keep_alive)
    keep_alive.app.config["TESTING"] = True
    return keep_alive.app.test_client()


def _seed_sensitive_gauges():
    from observability import METRICS
    METRICS.gauge("equity", 1234.56)
    METRICS.gauge("wallet_balance", 1200.00)


def test_metrics_is_closed_when_no_token_is_configured(client):
    _seed_sensitive_gauges()
    r = client.get("/metrics")
    assert r.status_code == 403
    assert b"1234.56" not in r.data


def test_health_never_leaks_balances_to_an_anonymous_caller(client):
    _seed_sensitive_gauges()
    r = client.get("/health")
    assert r.status_code == 200, "liveness must still work for uptime monitors"
    body = r.get_json()
    assert "metrics" not in body
    assert b"1234.56" not in r.data
    assert b"wallet_balance" not in r.data


def test_liveness_fields_survive_so_platform_health_checks_still_pass(client):
    body = client.get("/health").get_json()
    for k in ("status", "build_revision", "mode", "uptime_hours"):
        assert k in body


def test_a_wrong_token_is_refused(monkeypatch):
    monkeypatch.setenv("HEALTH_TOKEN", "correct-horse-battery-staple")
    import keep_alive
    importlib.reload(keep_alive)
    keep_alive.app.config["TESTING"] = True
    c = keep_alive.app.test_client()
    _seed_sensitive_gauges()
    assert c.get("/metrics", headers={"X-Health-Token": "wrong"}).status_code == 403
    assert c.get("/metrics").status_code == 403


def test_the_right_token_opens_it(monkeypatch):
    monkeypatch.setenv("HEALTH_TOKEN", "correct-horse-battery-staple")
    import keep_alive
    importlib.reload(keep_alive)
    keep_alive.app.config["TESTING"] = True
    c = keep_alive.app.test_client()
    _seed_sensitive_gauges()
    r = c.get("/metrics", headers={"X-Health-Token": "correct-horse-battery-staple"})
    assert r.status_code == 200
    assert r.get_json()["gauges"]["equity"] == pytest.approx(1234.56)


def test_token_comparison_is_constant_time():
    """A naive `==` leaks the token one byte at a time to a timing attacker."""
    import pathlib
    src = pathlib.Path(__file__).resolve().parents[1] / "src" / "keep_alive.py"
    assert "hmac.compare_digest" in src.read_text()


def test_the_landing_page_says_nothing_operational(client):
    body = client.get("/").data
    for leak in (b"equity", b"balance", b"USDT", b"position"):
        assert leak.lower() not in body.lower()
