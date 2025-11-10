from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import json
from db import fetch_all, fetch_one, execute

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
    return json_response({"ok": True, "service": "SportsStatsX", "version": "0.2.0"})

# -------------------------------
# DB 연결 확인
# -------------------------------
@app.route("/api/test-db")
def api_test_db():
    try:
        row = fetch_one("SELECT 1 AS ok")
        return json_response({"ok": True, "db": "connected", "result": row["ok"]})
    except Exception as e:
        return json_response({"ok": False, "error": str(e)}, 500)

# -------------------------------
# Fixtures (DB 기반)
# GET /api/fixtures?league_id=39&date=2025-11-12
# -------------------------------
@app.route("/api/fixtures")
def api_fixtures():
    league_id = request.args.get("league_id", type=int)
    date = request.args.get("date")  # YYYY-MM-DD

    where = []
    params = []

    if league_id is not None:
        where.append("league_id = %s")
        params.append(league_id)
    if date:
        where.append("date = %s")
        params.append(date)

    sql = "SELECT fixture_id, league_id, date, kickoff_utc, home, away, status FROM fixtures"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY kickoff_utc"

    rows = fetch_all(sql, tuple(params))
    return json_response({
        "ok": True,
        "count": len(rows),
        "filters": {"league_id": league_id, "date": date},
        "fixtures": rows
    })

# -------------------------------
# 새 경기 추가 (옵션)
# POST /api/fixtures  JSON body
# -------------------------------
@app.route("/api/fixtures", methods=["POST"])
def api_add_fixture():
    data = request.get_json(force=True)
    required = ["fixture_id", "league_id", "date", "kickoff_utc", "home", "away", "status"]
    missing = [k for k in required if k not in data]
    if missing:
        return json_response({"ok": False, "error": f"missing fields: {missing}"}, 400)

    execute(
        """
        INSERT INTO fixtures (fixture_id, league_id, date, kickoff_utc, home, away, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (fixture_id) DO NOTHING
        """,
        (
            data["fixture_id"],
            int(data["league_id"]),
            data["date"],
            data["kickoff_utc"],
            data["home"],
            data["away"],
            data["status"],
        ),
    )
    return json_response({"ok": True})

@app.errorhandler(404)
def not_found(_):
    return json_response({"ok": False, "error": "not_found"}, 404)

@app.errorhandler(500)
def server_error(e):
    return json_response({"ok": False, "error": "server_error", "detail": str(e)}, 500)

if __name__ == "__main__":
    # Render의 Start Command가 'python main.py'이므로 그대로.
    # Flask의 내장 서버로 충분 (Starter 플랜)
    app.run(host="0.0.0.0", port=10000)
