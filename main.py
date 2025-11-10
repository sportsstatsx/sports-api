# main.py
from flask import Flask, jsonify, request
from flask_cors import CORS
from db import fetch_all, fetch_one, execute

app = Flask(__name__)
CORS(app)

@app.route("/")
def root():
    return "Hello from SportsStatsX API!"

@app.route("/health")
def health():
    return jsonify({"ok": True, "service": "SportsStatsX", "version": "0.2.0"})

@app.route("/api/test-db")
def test_db():
    try:
        result = fetch_one("SELECT 1;")
        return jsonify({"ok": True, "db": "connected", "result": result})
    except Exception as e:
        return jsonify({"ok": False, "db": "error", "error": str(e)}), 500

# ─────────────────────────────────────────────
# ✅ /api/fixtures — 최신 스키마(league_id, match_date, home_team 등) 기반
# ─────────────────────────────────────────────
@app.route("/api/fixtures")
def get_fixtures():
    try:
        league_id = request.args.get("league_id")
        date = request.args.get("date")

        sql = "SELECT id, league_id, match_date, home_team, away_team, home_score, away_score FROM fixtures WHERE 1=1"
        params = []

        if league_id:
            sql += " AND league_id = %s"
            params.append(league_id)
        if date:
            sql += " AND match_date = %s"
            params.append(date)

        sql += " ORDER BY match_date, id"

        rows = fetch_all(sql, params)
        fixtures = [
            {
                "id": r[0],
                "league_id": r[1],
                "match_date": str(r[2]),
                "home_team": r[3],
                "away_team": r[4],
                "home_score": r[5],
                "away_score": r[6],
            }
            for r in rows
        ]

        return jsonify({"ok": True, "count": len(fixtures), "fixtures": fixtures})

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": "server_error",
            "detail": str(e)
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
