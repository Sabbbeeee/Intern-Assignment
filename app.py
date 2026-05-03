from flask import Flask, request, jsonify
import sqlite3
import hashlib
import json
from datetime import datetime

app = Flask(__name__)
print("HELLO I AM RUNNING")

# -----------------------------
# DATABASE SETUP
# -----------------------------
def init_db():
    conn = sqlite3.connect('events.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT UNIQUE,
            client_id TEXT,
            metric TEXT,
            amount INTEGER,
            timestamp TEXT
        )
    ''')
    conn.commit()
    conn.close()

init_db()


# -----------------------------
# NORMALIZATION
# -----------------------------
def normalize(event):
    payload = event.get("payload", {})

    # Handle amount
    try:
        amount = int(payload.get("amount", 0))
    except:
        amount = 0

    # Handle timestamp
    raw_time = payload.get("timestamp", "")
    try:
        parsed_time = datetime.strptime(raw_time, "%Y/%m/%d")
        timestamp = parsed_time.isoformat()
    except:
        timestamp = datetime.utcnow().isoformat()

    return {
        "client_id": event.get("source", "unknown"),
        "metric": payload.get("metric", "unknown"),
        "amount": amount,
        "timestamp": timestamp
    }


# -----------------------------
# HASH FOR DEDUPLICATION
# -----------------------------
def generate_hash(data):
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()


# -----------------------------
# INGEST API
# -----------------------------
@app.route('/ingest', methods=['POST'])
def ingest():
    event = request.json

    try:
        normalized = normalize(event)
        event_hash = generate_hash(normalized)

        conn = sqlite3.connect('events.db')
        c = conn.cursor()

        # Check duplicate
        c.execute("SELECT * FROM events WHERE hash=?", (event_hash,))
        if c.fetchone():
            return jsonify({"message": "Duplicate event ignored"}), 200

        # Insert
        c.execute('''
            INSERT INTO events (hash, client_id, metric, amount, timestamp)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            event_hash,
            normalized["client_id"],
            normalized["metric"],
            normalized["amount"],
            normalized["timestamp"]
        ))

        conn.commit()
        conn.close()

        return jsonify({"message": "Event processed successfully"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -----------------------------
# AGGREGATION API
# -----------------------------
@app.route('/aggregate', methods=['GET'])
def aggregate():
    conn = sqlite3.connect('events.db')
    c = conn.cursor()

    c.execute('''
        SELECT client_id, COUNT(*), SUM(amount)
        FROM events
        GROUP BY client_id
    ''')

    data = c.fetchall()
    conn.close()

    result = []
    for row in data:
        result.append({
            "client_id": row[0],
            "count": row[1],
            "total_amount": row[2]
        })

    return jsonify(result)


# -----------------------------
# RUN APP
# -----------------------------
if __name__ == '__main__':
    print("Starting server...")
    app.run(debug=True)