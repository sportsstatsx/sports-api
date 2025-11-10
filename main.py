# main.py
from flask import Flask, jsonify, request
from flask_cors import CORS
from db import fetch_all, fetch_one  # db.py의 헬퍼 사용

app = Flask(__name__)
CORS(app)

SERVICE_NAME = "SportsStatsX"
SERVICE_VERSION = "0.3.0"

# -----------------------------
# 기본 엔드포인트
# -----------------------------
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
        value = result[0] if isinstance(result, (tuple, list)) else result
        return jsonify({"ok": True, "db": "connected", "result": value})
    except Exception as e:
        return jsonify({"ok": False, "db": "error", "detail": str(e)}), 500

# -----------------------------
# Fixtures
#   GET /api/fixtures?league_id=39&date=2025-11-12
# -----------------------------
@app.route("/api/fixtures")
def get_fixtures():
    try:
        league_id = request.args.get("league_id", type=int)
        on_date   = request.args.get("date")  # YYYY-MM-DD
        limit     = request.args.get("limit", default=50, type=int)

        sql = """
            SELECT id, league_id, match_date, home_team, away_team, home_score, away_score
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

        sql += " ORDER BY match_date, id LIMIT %s"
        params.append(limit)

        rows = fetch_all(sql, tuple(params))
        fixtures = [
            {
                "id": r[0],
                "league_id": r[1],
                "match_date": str(r[2]),
                "home_team": r[3],
                "away_team": r[4],
                "home_score": r[5],
                "away_score": r[6],
            } for r in rows
        ]
        return jsonify({"ok": True, "count": len(fixtures), "fixtures": fixtures})
    except Exception as e:
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500

# -----------------------------
# Teams
#   GET /api/teams?league_id=39&q=ars&limit=50
# -----------------------------
@app.route("/api/teams")
def list_teams():
    try:
        league_id = request.args.get("league_id", type=int)
        q         = request.args.get("q")      # 부분 검색(이름)
        limit     = request.args.get("limit", default=50, type=int)

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
                "id": r[0],
                "league_id": r[1],
                "name": r[2],
                "country": r[3],
                "short_name": r[4],
            } for r in rows
        ]
        return jsonify({"ok": True, "count": len(teams), "teams": teams})
    except Exception as e:
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500

# -----------------------------
# Standings
#   GET /api/standings?league_id=39&season=2025-26&limit=20
# -----------------------------
@app.route("/api/standings")
def list_standings():
    try:
        league_id = request.args.get("league_id", type=int)
        season    = request.args.get("season")  # 예: '2025-26'
        limit     = request.args.get("limit", default=50, type=int)

        sql = """
            SELECT league_id, season, team_name, rank, played, win, draw, loss, gf, ga, gd, points
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
                "league_id": r[0],
                "season": r[1],
                "team_name": r[2],
                "rank": r[3],
                "played": r[4],
                "win": r[5],
                "draw": r[6],
                "loss": r[7],
                "gf": r[8],
                "ga": r[9],
                "gd": r[10],
                "points": r[11],
            } for r in rows
        ]
        return jsonify({"ok": True, "count": len(table), "standings": table})
    except Exception as e:
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500


# -----------------------------
# 로컬 실행
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
