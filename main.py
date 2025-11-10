# main.py
import os
from flask import Flask, jsonify, request
from flask_cors import CORS
from db import fetch_all, fetch_one, execute  # db.py 헬퍼들 사용

app = Flask(__name__)
CORS(app)

SERVICE_NAME = "SportsStatsX"
SERVICE_VERSION = "0.5.0"
API_KEY = os.getenv("API_KEY")  # Render 환경변수


def v(r, key, idx):
    """row가 dict/mapping이든 tuple이든 안전하게 값을 꺼낸다."""
    try:
        return r[key]
    except Exception:
        return r[idx]


def require_api_key():
    """간단한 API 키 인증 (헤더: X-API-KEY)."""
    if not API_KEY:
        # 운영 안전을 위해, API_KEY가 설정되지 않았으면 503으로 막습니다.
        return False, ("API key not configured on server", 503)
    sent = request.headers.get("X-API-KEY")
    if not sent or sent != API_KEY:
        return False, ("Unauthorized", 401)
    return True, None


@app.route("/")
def root():
    return "Hello from SportsStatsX API!"


@app.route("/health")
def health():
    return jsonify({"ok": True, "service": SERVICE_NAME, "version": SERVICE_VERSION})


@app.route("/api/test-db")
def test_db():
    try:
        result = fetch_one("SELECT 1;")
        value = result[0] if isinstance(result, (tuple, list)) else (
            result.get("1") if isinstance(result, dict) else result
        )
        return jsonify({"ok": True, "db": "connected", "result": value})
    except Exception as e:
        return jsonify({"ok": False, "db": "error", "detail": str(e)}), 500


# -------------------------------------------------------------------
# Fixtures (GET with ?league_id=39&date=YYYY-MM-DD&since=ISO8601)
# -------------------------------------------------------------------
@app.route("/api/fixtures")
def get_fixtures():
    try:
        league_id = request.args.get("league_id", type=int)
        on_date = request.args.get("date")   # YYYY-MM-DD
        since = request.args.get("since")    # ISO8601
        limit = request.args.get("limit", default=50, type=int)

        sql = """
            SELECT id, league_id, match_date, home_team, away_team,
                   home_score, away_score, updated_at
            FROM fixtures
            WHERE 1=1
        """
        params = []

        if league_id is not None:
            sql += " AND league_id = %s"
            params.append(league_id)
        if on_date:
            sql += " AND match_date = %s"
            params.append(on_date)
        if since:
            sql += " AND updated_at >= %s"
            params.append(since)

        sql += " ORDER BY match_date, id LIMIT %s"
        params.append(limit)

        rows = fetch_all(sql, tuple(params))

        fixtures = [
            {
                "id": v(r, "id", 0),
                "league_id": v(r, "league_id", 1),
                "match_date": str(v(r, "match_date", 2)),
                "home_team": v(r, "home_team", 3),
                "away_team": v(r, "away_team", 4),
                "home_score": v(r, "home_score", 5),
                "away_score": v(r, "away_score", 6),
                "updated_at": str(v(r, "updated_at", 7)),
            }
            for r in rows
        ]

        return jsonify({"ok": True, "count": len(fixtures), "fixtures": fixtures})
    except Exception as e:
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500


# -------------------------------------------------------------------
# NEW: Fixtures by Team
#   GET /api/fixtures/by-team?league_id=39&team=Arsenal&date=YYYY-MM-DD&since=ISO8601&limit=50
#   - home_team 또는 away_team 중 하나라도 team과 일치하면 반환
# -------------------------------------------------------------------
@app.route("/api/fixtures/by-team")
def get_fixtures_by_team():
    try:
        league_id = request.args.get("league_id", type=int)
        team = request.args.get("team")      # 팀 이름(정확 매칭)
        on_date = request.args.get("date")   # YYYY-MM-DD
        since = request.args.get("since")    # ISO8601
        limit = request.args.get("limit", default=50, type=int)

        if not team:
            return jsonify({"ok": False, "error": "bad_request", "detail": "team is required"}), 400

        sql = """
            SELECT id, league_id, match_date, home_team, away_team,
                   home_score, away_score, updated_at
            FROM fixtures
            WHERE (home_team = %s OR away_team = %s)
        """
        params = [team, team]

        if league_id is not None:
            sql += " AND league_id = %s"
            params.append(league_id)
        if on_date:
            sql += " AND match_date = %s"
            params.append(on_date)
        if since:
            sql += " AND updated_at >= %s"
            params.append(since)

        sql += " ORDER BY match_date, id LIMIT %s"
        params.append(limit)

        rows = fetch_all(sql, tuple(params))

        fixtures = [
            {
                "id": v(r, "id", 0),
                "league_id": v(r, "league_id", 1),
                "match_date": str(v(r, "match_date", 2)),
                "home_team": v(r, "home_team", 3),
                "away_team": v(r, "away_team", 4),
                "home_score": v(r, "home_score", 5),
                "away_score": v(r, "away_score", 6),
                "updated_at": str(v(r, "updated_at", 7)),
            }
            for r in rows
        ]
        return jsonify({"ok": True, "count": len(fixtures), "fixtures": fixtures})
    except Exception as e:
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500


# -------------------------------------------------------------------
# Teams (GET /api/teams?league_id=39&q=ars&limit=50)
# -------------------------------------------------------------------
@app.route("/api/teams")
def list_teams():
    try:
        league_id = request.args.get("league_id", type=int)
        q = request.args.get("q")
        limit = request.args.get("limit", default=50, type=int)

        sql = """
            SELECT id, league_id, name, country, short_name
            FROM teams
            WHERE 1=1
        """
        params = []

        if league_id is not None:
            sql += " AND league_id = %s"
            params.append(league_id)
        if q:
            sql += " AND LOWER(name) LIKE LOWER(%s)"
            params.append(f"%{q}%")

        sql += " ORDER BY name LIMIT %s"
        params.append(limit)

        rows = fetch_all(sql, tuple(params))

        teams = [
            {
                "id": v(r, "id", 0),
                "league_id": v(r, "league_id", 1),
                "name": v(r, "name", 2),
                "country": v(r, "country", 3),
                "short_name": v(r, "short_name", 4),
            }
            for r in rows
        ]

        return jsonify({"ok": True, "count": len(teams), "teams": teams})
    except Exception as e:
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500


# -------------------------------------------------------------------
# Standings (GET /api/standings?league_id=39&season=2025-26)
# -------------------------------------------------------------------
@app.route("/api/standings")
def list_standings():
    try:
        league_id = request.args.get("league_id", type=int)
        season = request.args.get("season")
        limit = request.args.get("limit", default=50, type=int)

        sql = """
            SELECT league_id, season, team_name, rank,
                   played, win, draw, loss, gf, ga, gd, points
            FROM standings
            WHERE 1=1
        """
        params = []

        if league_id is not None:
            sql += " AND league_id = %s"
            params.append(league_id)
        if season:
            sql += " AND season = %s"
            params.append(season)

        sql += " ORDER BY season DESC, rank ASC LIMIT %s"
        params.append(limit)

        rows = fetch_all(sql, tuple(params))

        table = [
            {
                "league_id": v(r, "league_id", 0),
                "season": v(r, "season", 1),
                "team_name": v(r, "team_name", 2),
                "rank": v(r, "rank", 3),
                "played": v(r, "played", 4),
                "win": v(r, "win", 5),
                "draw": v(r, "draw", 6),
                "loss": v(r, "loss", 7),
                "gf": v(r, "gf", 8),
                "ga": v(r, "ga", 9),
                "gd": v(r, "gd", 10),
                "points": v(r, "points", 11),
            }
            for r in rows
        ]

        return jsonify({"ok": True, "count": len(table), "standings": table})
    except Exception as e:
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500


# -------------------------------------------------------------------
# Write: PATCH /api/fixtures/<id>  (보호: X-API-KEY)
# Body(JSON): {"home_score": 1, "away_score": 2}  둘 중 일부만 보내도 됨
# -------------------------------------------------------------------
@app.route("/api/fixtures/<int:fixture_id>", methods=["PATCH"])
def update_fixture(fixture_id: int):
    ok, err = require_api_key()
    if not ok:
        msg, code = err
        return jsonify({"ok": False, "error": "unauthorized", "detail": msg}), code

    try:
        payload = request.get_json(silent=True) or {}
        fields = []
        params = []

        if "home_score" in payload:
            fields.append("home_score = %s")
            params.append(payload["home_score"])
        if "away_score" in payload:
            fields.append("away_score = %s")
            params.append(payload["away_score"])

        if not fields:
            return jsonify({"ok": False, "error": "bad_request", "detail": "no fields to update"}), 400

        sql = f"UPDATE fixtures SET {', '.join(fields)} WHERE id = %s RETURNING id;"
        params.append(fixture_id)

        row = fetch_one(sql, tuple(params))
        if not row:
            return jsonify({"ok": False, "error": "not_found"}), 404

        # 갱신된 행 반환
        fresh = fetch_one("""
            SELECT id, league_id, match_date, home_team, away_team, home_score, away_score, updated_at
            FROM fixtures WHERE id = %s
        """, (fixture_id,))

        data = {
            "id": v(fresh, "id", 0),
            "league_id": v(fresh, "league_id", 1),
            "match_date": str(v(fresh, "match_date", 2)),
            "home_team": v(fresh, "home_team", 3),
            "away_team": v(fresh, "away_team", 4),
            "home_score": v(fresh, "home_score", 5),
            "away_score": v(fresh, "away_score", 6),
            "updated_at": str(v(fresh, "updated_at", 7)),
        }
        return jsonify({"ok": True, "fixture": data})
    except Exception as e:
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
