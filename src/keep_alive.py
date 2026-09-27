"""
💎 V1.0 TITANIUM APEX: MISSION CONTROL UPTIME SERVER
-----------------------------------------------------
Dedicated daemonic web server for platform health checks (Render, Railway, etc).
Upgraded to V1.0 specifications with exact uptime tracking, UTC timestamps,
and zero-blocking threading.
"""

import os
import hmac
import time
import logging
from flask import Flask, jsonify, request
from threading import Thread
from datetime import datetime, timezone

# Suppress standard Flask startup logs to keep the terminal clean for quantitative outputs
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)
START_TIME = time.time()

@app.route('/')
def home():
    """Default landing page for external pings."""
    return "🟢 PJMASK EMPIRE | V1.0 TITANIUM APEX Quant Swarm is Online and Hunting!"

def _metrics_snapshot():
    """AUDIT B31: surface counters and reason codes rather than only logging them."""
    try:
        from observability import METRICS
        return METRICS.snapshot()
    except Exception as e:
        return {"error": f"metrics unavailable: {e}"}


def _authorised(request_obj) -> bool:
    """
    APEX/QA finding: this server binds 0.0.0.0 (required by Render et al.), so
    every route is reachable from the public internet. `METRICS.snapshot()`
    publishes the `equity` and `wallet_balance` gauges -- main.py:927-928 -- so
    the unauthenticated /metrics route was disclosing the live account balance,
    and the counters alongside it leak position activity and timing.

    FAIL CLOSED: with no HEALTH_TOKEN configured the detailed routes are simply
    off. An operator opts in by setting the variable; nobody is exposed by
    forgetting to.
    """
    token = os.environ.get("HEALTH_TOKEN", "").strip()
    if not token:
        return False
    supplied = (request_obj.headers.get("X-Health-Token", "")
                or request_obj.args.get("token", ""))
    # constant-time compare: a naive == leaks the token a byte at a time
    return hmac.compare_digest(supplied, token)


@app.route('/metrics')
def metrics_endpoint():
    if not _authorised(request):
        return jsonify({
            "error": "forbidden",
            "detail": "Set HEALTH_TOKEN and send it as X-Health-Token. "
                      "This endpoint exposes account gauges and is closed by default.",
        }), 403
    return jsonify(_metrics_snapshot()), 200


@app.route('/health')
def health_check():
    """
    🚀 V1.0 UPGRADE: Dedicated JSON Health Endpoint
    Allows external uptime monitors (e.g., UptimeRobot, Render Health Checks)
    to programmatically verify the engine's heartbeat.
    Now includes exact uptime tracking and UTC sync.
    """
    uptime_seconds = time.time() - START_TIME
    uptime_hours = uptime_seconds / 3600.0

    # AUDIT NEW-1: report the ACTUAL running revision and mode. The previous
    # hardcoded version string made it impossible to verify which commit was
    # deployed, which in turn made every production claim unfalsifiable.
    try:
        from runtime_config import get_config
        cfg = get_config()
        revision = cfg.build_revision
        mode = cfg.mode.value
        places_real_orders = cfg.mode.places_real_orders
    except Exception as e:  # configuration faults must not take the probe down
        revision = "unknown"
        mode = "UNRESOLVED"
        places_real_orders = None
        log.error(f"health: could not resolve runtime config: {e}")

    # Liveness only. An uptime monitor needs to know the process is up and which
    # build is running; it does not need the account balance. The metrics block
    # is attached only for an authorised caller.
    body = {
        "status": "online",
        "build_revision": revision,
        "revision_verified": revision != "unknown",
        "mode": mode,
        "places_real_orders": places_real_orders,
        "engine": "Distributed Quant Swarm",
        "uptime_hours": round(uptime_hours, 4),
        "started_utc": datetime.fromtimestamp(START_TIME, timezone.utc).isoformat(),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    if _authorised(request):
        body["metrics"] = _metrics_snapshot()
    return jsonify(body), 200

def run():
    # Render assigns a dynamic port. Fallback to 8080 locally.
    port = int(os.environ.get("PORT", 8080))
    # Host must be 0.0.0.0 to bind to cloud provider external network interfaces.
    # use_reloader=False prevents Flask from spinning up duplicate processes.
    app.run(host='0.0.0.0', port=port, use_reloader=False)

def keep_alive():
    """
    🚀 V1.0 UPGRADE: Daemonic Background Thread
    Spins up a background thread to keep the server awake.
    daemon=True ensures this web server does not block graceful system shutdowns
    during emergency flatten sequences.
    """
    t = Thread(target=run, name="TitaniumHealthServer", daemon=True)
    t.start()

    logger = logging.getLogger("QUANT_CORE.HEALTH")
    logger.info("🟢 TITANIUM UPTIME SERVER ONLINE: Listening for external health checks.")
