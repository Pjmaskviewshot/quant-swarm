import sqlite3
import pandas as pd

def check_bot_brain():
    try:
        # Connect to the bot's memory bank
        conn = sqlite3.connect("data/quant_memory.db")
        
        # Pull all the predictions it has made
        df = pd.read_sql_query("SELECT timestamp, predicted_direction, actual_outcome, is_correct FROM signals ORDER BY timestamp DESC LIMIT 10", conn)
        
        if df.empty:
            print("\n🤖 The database is connected, but the bot hasn't found a strong enough signal to log a prediction yet.")
            print("Keep letting it run. It is waiting for a volume anomaly.")
        else:
            print("\n🧠 BOT MEMORY BANK (Last 10 Ghost Predictions):")
            print(df.to_string(index=False))
            
    except Exception as e:
        print(f"Error reading database: {e}")

if __name__ == "__main__":
    check_bot_brain()