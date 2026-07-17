import os
import logging
from flask import Flask
from threading import Thread

# Suppress standard Flask startup logs to keep the terminal clean for quantitative outputs
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)

@app.route('/')
def home():
    return "🟢 Quant Swarm Engine is Online and Hunting!"

def run():
    # Render assigns a dynamic port. Fallback to 8080 locally.
    port = int(os.environ.get("PORT", 8080))
    # Host must be 0.0.0.0 to bind to Render's external network interfaces
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    """Spins up a background thread to keep the Render server awake."""
    # 🛑 P2-4 FIX: daemon=True prevents the Flask server from blocking container shutdowns
    t = Thread(target=run, daemon=True)
    t.start()