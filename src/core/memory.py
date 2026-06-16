import os
import sqlite3
import logging
from typing import Tuple, List, Dict, Any

logger = logging.getLogger("QUANT_CORE.MEMORY")

class MemoryBank:
    def __init__(self, db_path: str = "data/quant_memory.db"):
        self.db_path = db_path
        
        # CRITICAL FIX: Ensure the directory exists before SQLite tries to connect
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
            
        self._migrate_db()

    def _get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path, timeout=30.0)

    def _migrate_db(self):
        """Initializes database schema with optimized execution indexing."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS signals (
                    signal_id TEXT PRIMARY KEY,
                    timestamp REAL NOT NULL,
                    price_at_prediction REAL NOT NULL,
                    predicted_direction TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    resolved INTEGER DEFAULT 0,
                    actual_outcome TEXT,
                    is_correct INTEGER
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_resolved ON signals(resolved);')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON signals(timestamp DESC);')
            conn.commit()

    def commit_prediction(self, signal_id: str, timestamp: float, price: float, direction: str, confidence: float):
        """Saves a fresh prediction sequence into persistent storage."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO signals (signal_id, timestamp, price_at_prediction, predicted_direction, confidence)
                VALUES (?, ?, ?, ?, ?)
            ''', (signal_id, timestamp, price, direction, confidence))
            conn.commit()

    def resolve_historical_predictions(self, current_price: float, age_cutoff: float) -> int:
        """Compares expired, unresolved predictions against current market price."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT signal_id, price_at_prediction, predicted_direction 
                FROM signals 
                WHERE resolved = 0 AND timestamp <= ?
            ''', (age_cutoff,))
            unresolved = cursor.fetchall()

            resolved_count = 0
            for row in unresolved:
                sig_id, entry_price, prediction = row
                actual = "HOLD"
                
                if current_price > entry_price:
                    actual = "BUY"
                elif current_price < entry_price:
                    actual = "SELL"

                is_correct = 1 if prediction == actual else 0
                
                cursor.execute('''
                    UPDATE signals 
                    SET resolved = 1, actual_outcome = ?, is_correct = ? 
                    WHERE signal_id = ?
                ''', (actual, is_correct, sig_id))
                resolved_count += 1
                
            conn.commit()
            return resolved_count

    def compute_rolling_accuracy(self, window_size: int = 50) -> Tuple[float, int]:
        """Calculates rolling system accuracy metric over the target baseline sample window."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT is_correct FROM signals 
                WHERE resolved = 1 
                ORDER BY timestamp DESC 
                LIMIT ?
            ''', (window_size,))
            results = cursor.fetchall()
            
            total_resolved = len(results)
            if total_resolved == 0:
                return 0.0, 0

            correct_predictions = sum(row[0] for row in results)
            accuracy = correct_predictions / total_resolved
            return accuracy, total_resolved