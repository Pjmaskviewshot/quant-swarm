import os
from flask import Flask
from threading import Thread

app = Flask(__name__)

@app.route('/')
def home():
    return "🟢 Quant Swarm Engine is Online and Hunting!"

def run():
    # Render assigns a dynamic port. Fallback to 8080 locally.
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    """Spins up a background thread to keep the Render server awake."""
    t = Thread(target=run)
    t.start()