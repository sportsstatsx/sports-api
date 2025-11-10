# main.py
from flask import Flask, jsonify, request
from flask_cors import CORS
from db import fetch_all, fetch_one

app = Flask(__name__)
CORS(app)

@app.route("/")
def home():
    return "Hello from SportsStatsX API!"

@app.route("/health")
def health():
    return jsonify({"ok": True, "service": "SportStatsX", "version": "0.2.1"})

@app.route("/api/test-db")
def test_db():
    try:
        result = fetch_one("SELECT 1;")
        return jsonify({"ok": True, "db": "connected", "result": result})
    except Exception as e:
        return jsonify({"ok": False, "db": "error", "detail": str(e)}), 500

@app.route("/api/fixtures")
def get_fixtures():
    try:
        league_id = request.args.get("league_id", type=int)
        match_date = request.args.get("date")  # YYYY-MM-DD optional

        sql = "SELECT id, league_id, match_date, home_team, away_team, home_score, away_score FROM fixtures WHERE 1=1"
        params = []

        if league_id:
            sql += " AND league_id = %s"
            params.append(league_id)

        if match_date:
            sql += " AND match_date = %s"
            params.append(match_date)

        sql += " ORDER BY match_date, id LIMIT 50"

        rows = fetch_all(sql, tuple(params))
        return jsonify({"ok": True, "fixtures": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
