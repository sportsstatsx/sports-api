from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import json
import os
import psycopg  # ✅ psycopg v3

app = Flask(__name__)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True
app.config["JSON_SORT_KEYS"] = False
CORS(app, resources={r"/api/*": {"origins": "*"}})

def json_response(payload: dict, status: int = 200) -> Response:
    return Response(
        json.dumps(payload, ensure_ascii=False, indent=2),
        status=status,
        mimetype="application/json",
    )

@app.route("/")
def home():
    return "Hello from SportsStatsX API!"

@app.route("/health")
def health():
    return json_response({"ok": True, "service": "SportsStatsX", "version": "0.1.0"})

@app.route("/api/ping")
def api_ping():
    return json_response({"pong": True})

@app.route("/api/fixtures")
def api_fixtures():
    league_id = request.args.get("league_id")
    date = request.args.get("date")

    sample = [
        {
            "fixture_id": "FX12345",
            "league_id": league_id or "39",
            "date": date or "2025-11-12",
            "kickoff_utc": "2025-11-12T19:00:00Z",
            "home": "Team A",
            "away": "Team B",
            "status": "scheduled"
        },
        {
            "fixture_id": "FX12346",
            "league_id": league_id or "39",
            "date": date or "2025-11-12",
            "kickoff_utc": "2025-11-12T21:00:00Z",
            "home": "Team C",
            "away": "Team D",
            "status": "scheduled"
        },
    ]

    payload = {
        "ok": True,
        "count": len(sample),
        "filters": {"league_id": league_id, "date": date},
        "fixtures": sample,
    }
    return json_response(payload)

# ✅ DB 연결 테스트 (psycopg v3)
@app.route("/api/test-db")
def test_db():
    dsn = os.getenv("DATABASE_URL")  # Render Environment에 넣은 값
    if not dsn:
        return json_response({"ok": False, "error": "missing_DATABASE_URL"}, 500)
    try:
        # connect_timeout(초) 옵션으로 빠른 실패 유도
        with psycopg.connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                one = cur.fetchone()[0]
        return json_response({"ok": True, "db": "connected", "result": int(one)})
    except Exception as e:
        return json_response({"ok": False, "error": "db_error", "detail": str(e)}, 500)

@app.errorhandler(404)
def not_found(_):
    return json_response({"ok": False, "error": "not_found"}, 404)

@app.errorhandler(500)
def server_error(e):
    return json_response({"ok": False, "error": "server_error", "detail": str(e)}, 500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
