import os
import logging
from flask import Flask, jsonify
from threading import Thread

# Suppress standard Flask startup logs to keep the terminal clean for quantitative outputs
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)

@app.route('/')
def home():
    """Default landing page for external pings."""
    return "🟢 PJMASK EMPIRE | V26.0 APEX Quant Swarm is Online and Hunting!"

@app.route('/health')
def health_check():
    """
    🚀 V26 UPGRADE: Dedicated JSON Health Endpoint
    Allows external uptime monitors (e.g., UptimeRobot, Render Health Checks) 
    to programmatically verify the engine's heartbeat.
    """
    return jsonify({
        "status": "online",
        "version": "V26.0 APEX",
        "engine": "Distributed Quant Swarm"
    }), 200

def run():
    # Render assigns a dynamic port. Fallback to 8080 locally.
    port = int(os.environ.get("PORT", 8080))
    # Host must be 0.0.0.0 to bind to Render's external network interfaces
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    """
    🚀 V26 UPGRADE: Daemonic Background Thread
    Spins up a background thread to keep the server awake.
    daemon=True ensures this web server does not block graceful system shutdowns 
    during emergency flatten sequences.
    """
    t = Thread(target=run, name="ApexHealthServer", daemon=True)
    t.start()