"""
💎 V58.0 TITANIUM APEX: MISSION CONTROL UPTIME SERVER
-----------------------------------------------------
Dedicated daemonic web server for platform health checks (Render, Railway, etc).
Upgraded to V58.0 specifications with exact uptime tracking, UTC timestamps, 
and zero-blocking threading.
"""

import os
import time
import logging
from flask import Flask, jsonify
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
    return "🟢 PJMASK EMPIRE | V58.0 TITANIUM APEX Quant Swarm is Online and Hunting!"

@app.route('/health')
def health_check():
    """
    🚀 V58.0 UPGRADE: Dedicated JSON Health Endpoint
    Allows external uptime monitors (e.g., UptimeRobot, Render Health Checks) 
    to programmatically verify the engine's heartbeat.
    Now includes exact uptime tracking and UTC sync.
    """
    uptime_seconds = time.time() - START_TIME
    uptime_hours = uptime_seconds / 3600.0
    
    return jsonify({
        "status": "online",
        "version": "V58.0 TITANIUM APEX",
        "engine": "Distributed Quant Swarm",
        "organization": "PJMASK EMPIRE",
        "uptime_hours": round(uptime_hours, 4),
        "timestamp_utc": datetime.now(timezone.utc).isoformat()
    }), 200

def run():
    # Render assigns a dynamic port. Fallback to 8080 locally.
    port = int(os.environ.get("PORT", 8080))
    # Host must be 0.0.0.0 to bind to cloud provider external network interfaces.
    # use_reloader=False prevents Flask from spinning up duplicate processes.
    app.run(host='0.0.0.0', port=port, use_reloader=False)

def keep_alive():
    """
    🚀 V58.0 UPGRADE: Daemonic Background Thread
    Spins up a background thread to keep the server awake.
    daemon=True ensures this web server does not block graceful system shutdowns 
    during emergency flatten sequences.
    """
    t = Thread(target=run, name="TitaniumHealthServer", daemon=True)
    t.start()
    
    logger = logging.getLogger("QUANT_CORE.HEALTH")
    logger.info("🟢 TITANIUM UPTIME SERVER ONLINE: Listening for external health checks.")