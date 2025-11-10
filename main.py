# main.py
import os
from flask import Flask, jsonify, request
from flask_cors import CORS
from db import fetch_all, fetch_one  # db.py 헬퍼 사용

app = Flask(__name__)
CORS(app)

SERVICE_NAME = "SportsStatsX"
SERVICE_VERSION = "0.6.0"
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
        return False, ("API key not configured on server", 503)
    sent = request.headers.get("X-API-KEY")
    if not sent or sent != API_KEY:
        return False, ("Unauthorized", 401)
    return True, None


# -----------------------------
# 공통 유틸: 페이지네이션/정렬 파싱
# -----------------------------
def parse_pagination():
    page = request.args.get("page", default=1, type=int)
    page_size = request.args.get("page_size", default=50, type=int)
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 1
    if page_size > 200:
        page_size = 200
    offset = (page - 1) * page_size
    return page, page_size, offset

def parse_sort(allowed_columns, default_sort, default_order="asc"):
    sort = request.args.get("sort", default_sort)
    order = request.args.get("order", default_order).lower()
    if sort not in allowed_columns:
        sort = default_sort
    if order not in ("asc", "desc"):
        order = default_order
    return sort, order


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
# Fixtures (GET with ?league_id=39&date=YYYY-MM-DD&since=ISO8601
#           &page=1&page_size=50&sort=match_date&order=asc)
# -------------------------------------------------------------------
@app.route("/api/fixtures")
def get_fixtures():
    try:
        league_id = request.args.get("league_id", type=int)
        on_date = request.args.get("date")   # YYYY-MM-DD
        since = request.args.get("since")    # ISO8601

        # pagination + sort
        page, page_size, offset = parse_pagination()
        sort, order = parse_sort(
            allowed_columns={"match_date", "id", "updated_at", "home_team", "away_team", "league_id"},
            default_sort="match_date",
            default_order="asc",
        )

        base_where = "WHERE 1=1"
        params = []

        if league_id is not None:
            base_where += " AND league_id = %s"
            params.append(league_id)
        if on_date:
            base_where += " AND match_date = %s"
            params.append(on_date)
        if since:
            base_where += " AND updated_at >= %s"
            params.append(since)

        # total count
        count_sql = f"SELECT COUNT(*) FROM fixtures {base_where}"
        total = fetch_one(count_sql, tuple(params))
        total_val = total[0] if isinstance(total, (tuple, list)) else (total.get("count") if isinstance(total, dict) else total)

        # page query
        data_sql = f"""
            SELECT id, league_id, match_date, home_team, away_team,
                   home_score, away_score, updated_at
            FROM fixtures
            {base_where}
            ORDER BY {sort} {order}, id ASC
            LIMIT %s OFFSET %s
        """
        data_params = params + [page_size, offset]
        rows = fetch_all(data_sql, tuple(data_params))

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

        has_next = (page * page_size) < int(total_val or 0)
        return jsonify({
            "ok": True,
            "page": page,
            "page_size": page_size,
            "total": int(total_val or 0),
            "has_next": has_next,
            "fixtures": fixtures
        })
    except Exception as e:
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500


# -------------------------------------------------------------------
# Fixtures by Team
#   GET /api/fixtures/by-team?league_id=39&team=Arsenal
#       &date=YYYY-MM-DD&since=ISO8601&page=1&page_size=50
#       &sort=match_date|updated_at|id|home_team|away_team  &order=asc|desc
# -------------------------------------------------------------------
@app.route("/api/fixtures/by-team")
def get_fixtures_by_team():
    try:
        league_id = request.args.get("league_id", type=int)
        team = request.args.get("team")      # 정확 매칭
        on_date = request.args.get("date")
        since = request.args.get("since")

        if not team:
            return jsonify({"ok": False, "error": "bad_request", "detail": "team is required"}), 400

        page, page_size, offset = parse_pagination()
        sort, order = parse_sort(
            allowed_columns={"match_date", "id", "updated_at", "home_team", "away_team", "league_id"},
            default_sort="match_date",
            default_order="asc",
        )

        base_where = "WHERE (home_team = %s OR away_team = %s)"
        params = [team, team]

        if league_id is not None:
            base_where += " AND league_id = %s"
            params.append(league_id)
        if on_date:
            base_where += " AND match_date = %s"
            params.append(on_date)
        if since:
            base_where += " AND updated_at >= %s"
            params.append(since)

        count_sql = f"SELECT COUNT(*) FROM fixtures {base_where}"
        total = fetch_one(count_sql, tuple(params))
        total_val = total[0] if isinstance(total, (tuple, list)) else (total.get("count") if isinstance(total, dict) else total)

        data_sql = f"""
            SELECT id, league_id, match_date, home_team, away_team,
                   home_score, away_score, updated_at
            FROM fixtures
            {base_where}
            ORDER BY {sort} {order}, id ASC
            LIMIT %s OFFSET %s
        """
        data_params = params + [page_size, offset]
        rows = fetch_all(data_sql, tuple(data_params))

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

        has_next = (page * page_size) < int(total_val or 0)
        return jsonify({
            "ok": True,
            "page": page,
            "page_size": page_size,
            "total": int(total_val or 0),
            "has_next": has_next,
            "fixtures": fixtures
        })
    except Exception as e:
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500


# -------------------------------------------------------------------
# Teams (GET /api/teams?league_id=39&q=ars&page=1&page_size=50
#        &sort=name|short_name|id|league_id &order=asc|desc)
# -------------------------------------------------------------------
@app.route("/api/teams")
def list_teams():
    try:
        league_id = request.args.get("league_id", type=int)
        q = request.args.get("q")

        page, page_size, offset = parse_pagination()
        sort, order = parse_sort(
            allowed_columns={"name", "short_name", "id", "league_id"},
            default_sort="name",
            default_order="asc",
        )

        base_where = "WHERE 1=1"
        params = []
        if league_id is not None:
            base_where += " AND league_id = %s"
            params.append(league_id)
        if q:
            base_where += " AND LOWER(name) LIKE LOWER(%s)"
            params.append(f"%{q}%")

        count_sql = f"SELECT COUNT(*) FROM teams {base_where}"
        total = fetch_one(count_sql, tuple(params))
        total_val = total[0] if isinstance(total, (tuple, list)) else (total.get("count") if isinstance(total, dict) else total)

        data_sql = f"""
            SELECT id, league_id, name, country, short_name
            FROM teams
            {base_where}
            ORDER BY {sort} {order}, id ASC
            LIMIT %s OFFSET %s
        """
        data_params = params + [page_size, offset]
        rows = fetch_all(data_sql, tuple(data_params))

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

        has_next = (page * page_size) < int(total_val or 0)
        return jsonify({
            "ok": True,
            "page": page,
            "page_size": page_size,
            "total": int(total_val or 0),
            "has_next": has_next,
            "teams": teams
        })
    except Exception as e:
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500


# -------------------------------------------------------------------
# Standings (GET /api/standings?league_id=39&season=2025-26
#            &page=1&page_size=50&sort=rank|points|team_name|id  &order=asc|desc)
# -------------------------------------------------------------------
@app.route("/api/standings")
def list_standings():
    try:
        league_id = request.args.get("league_id", type=int)
        season = request.args.get("season")

        page, page_size, offset = parse_pagination()
        sort, order = parse_sort(
            allowed_columns={"rank", "points", "team_name", "league_id"},
            default_sort="rank",
            default_order="asc",
        )

        base_where = "WHERE 1=1"
        params = []
        if league_id is not None:
            base_where += " AND league_id = %s"
            params.append(league_id)
        if season:
            base_where += " AND season = %s"
            params.append(season)

        count_sql = f"SELECT COUNT(*) FROM standings {base_where}"
        total = fetch_one(count_sql, tuple(params))
        total_val = total[0] if isinstance(total, (tuple, list)) else (total.get("count") if isinstance(total, dict) else total)

        data_sql = f"""
            SELECT league_id, season, team_name, rank,
                   played, win, draw, loss, gf, ga, gd, points
            FROM standings
            {base_where}
            ORDER BY {sort} {order}, rank ASC
            LIMIT %s OFFSET %s
        """
        data_params = params + [page_size, offset]
        rows = fetch_all(data_sql, tuple(data_params))

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

        has_next = (page * page_size) < int(total_val or 0)
        return jsonify({
            "ok": True,
            "page": page,
            "page_size": page_size,
            "total": int(total_val or 0),
            "has_next": has_next,
            "standings": table
        })
    except Exception as e:
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500


# -------------------------------------------------------------------
# Write: PATCH /api/fixtures/<id>  (보호: X-API-KEY)
# Body(JSON): {"home_score": 1, "away_score": 2}
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
