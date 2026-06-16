from flask import Flask
from threading import Thread

app = Flask(__name__)

@app.route('/')
def home():
    return "🟢 Quant Swarm Engine is Online and Hunting!"

def run():
    # Render requires web services to bind to host 0.0.0.0
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    """Spins up a background thread to keep the Render server awake."""
    t = Thread(target=run)
    t.start()