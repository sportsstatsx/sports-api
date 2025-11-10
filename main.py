from flask import Flask, request, Response
from flask_cors import CORS
import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# JSON 포맷(들여쓰기 & 키 순서 유지)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True
app.config["JSON_SORT_KEYS"] = False

# CORS: /api/* 전부 허용
CORS(app, resources={r"/api/*": {"origins": "*"}})


def json_response(payload: dict, status: int = 200) -> Response:
    """항상 같은 순서/들여쓰기로 응답"""
    return Response(
        json.dumps(payload, ensure_ascii=False, indent=2),  # pretty print
        status=status,
        mimetype="application/json",
    )


def get_db_conn():
    """Render Postgres 연결 (DATABASE_URL 사용, sslmode=require)"""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("missing env: DATABASE_URL")

    # Render/Heroku 스타일 URL 호환, sslmode=require 권장
    return psycopg2.connect(db_url, sslmode="require")


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
    """샘플 경기 목록 (필터: league_id, date)"""
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
            "status": "scheduled",
        },
        {
            "fixture_id": "FX12346",
            "league_id": league_id or "39",
            "date": date or "2025-11-12",
            "kickoff_utc": "2025-11-12T21:00:00Z",
            "home": "Team C",
            "away": "Team D",
            "status": "scheduled",
        },
    ]

    payload = {
        "ok": True,
        "count": len(sample),
        "filters": {"league_id": league_id, "date": date},
        "fixtures": sample,
    }
    return json_response(payload)


@app.route("/api/test-db")
def test_db():
    """DATABASE_URL로 Postgres 접속 테스트"""
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT NOW() AS server_time;")
                row = cur.fetchone()
        return json_response(
            {"ok": True, "message": "DB Connected!", "time": str(row["server_time"])}
        )
    except Exception as e:
        return json_response(
            {"ok": False, "error": "db_connect_failed", "detail": str(e)}, 500
        )


@app.errorhandler(404)
def not_found(_):
    return json_response({"ok": False, "error": "not_found"}, 404)


@app.errorhandler(500)
def server_error(e):
    return json_response({"ok": False, "error": "server_error", "detail": str(e)}, 500)


if __name__ == "__main__":
    # Render의 Start Command가 `python main.py` 라면 이 포트로 실행
    app.run(host="0.0.0.0", port=10000)
