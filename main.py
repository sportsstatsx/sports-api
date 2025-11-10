# main.py
import os
import json
import uuid
import time
import re
import hashlib
import logging
from datetime import datetime, timezone
from email.utils import format_datetime
from flask import Flask, jsonify, request, make_response, g, Response
from flask_cors import CORS
from db import fetch_all, fetch_one  # db.py helpers

# ─────────────────────────────────────────────────────
# App / Settings
# ─────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)

SERVICE_NAME = "SportsStatsX"
SERVICE_VERSION = "1.1.0"

API_KEY = os.getenv("API_KEY")  # set in Render Environment
LOG_SAMPLE_RATE = float(os.getenv("LOG_SAMPLE_RATE", "0.25"))  # 0.0~1.0
app.config["MAX_CONTENT_LENGTH"] = 256 * 1024  # 256KB per request

MIN_SCORE, MAX_SCORE = 0, 99

logging.basicConfig(level=logging.INFO, format="%(message)s")  # JSON line logs


# ─────────────────────────────────────────────────────
# Logging / Correlation
# ─────────────────────────────────────────────────────
def _rand_float():
    return (uuid.uuid4().int % 10_000_000) / 10_000_000.0

def _should_sample() -> bool:
    try:
        return _rand_float() < LOG_SAMPLE_RATE
    except Exception:
        return True

def _client_ip() -> str:
    return request.headers.get("X-Forwarded-For", request.remote_addr or "")

def _req_id() -> str:
    rid = request.headers.get("X-Request-ID")
    return rid if rid else str(uuid.uuid4())

@app.before_request
def _before():
    g._ts = time.time()
    g._req_id = _req_id()
    cl = request.headers.get("Content-Length")
    if cl and cl.isdigit() and int(cl) > app.config["MAX_CONTENT_LENGTH"]:
        return error_response("payload_too_large", 413, "Request payload too large")
    if _should_sample():
        safe_headers = {k: v for k, v in request.headers.items() if k.lower() not in {"authorization", "x-api-key"}}
        try:
            logging.info(json.dumps({
                "t": "req",
                "request_id": g._req_id,
                "method": request.method,
                "path": request.path,
                "query": request.query_string.decode("utf-8", errors="ignore"),
                "ip": _client_ip(),
                "headers": safe_headers,
            }, ensure_ascii=False))
        except Exception:
            pass

@app.after_request
def _after(resp):
    resp.headers["X-Request-ID"] = getattr(g, "_req_id", "")
    if _should_sample():
        duration_ms = int((time.time() - getattr(g, "_ts", time.time())) * 1000)
        try:
            logging.info(json.dumps({
                "t": "resp",
                "request_id": getattr(g, "_req_id", ""),
                "status": resp.status_code,
                "method": request.method,
                "path": request.path,
                "duration_ms": duration_ms,
            }, ensure_ascii=False))
        except Exception:
            pass
    return resp


# ─────────────────────────────────────────────────────
# Common helpers / validation
# ─────────────────────────────────────────────────────
def v(r, key, idx):
    try:
        return r[key]
    except Exception:
        return r[idx]

def ok_response(payload: dict, http_status: int = 200):
    body = {"ok": True}
    body.update(payload or {})
    resp = make_response(jsonify(body), http_status)
    resp.headers["X-Request-ID"] = getattr(g, "_req_id", "")
    return resp

def error_response(code: str, http_status: int, message: str, *, detail: str | None = None, hint: str | None = None, fields: dict | None = None):
    resp = make_response(jsonify({
        "ok": False,
        "error": {
            "code": code,
            "message": message,
            **({"detail": detail} if detail else {}),
            **({"hint": hint} if hint else {}),
            **({"fields": fields} if fields else {}),
        },
    }), http_status)
    resp.headers["X-Request-ID"] = getattr(g, "_req_id", "")
    return resp

def require_api_key():
    if not API_KEY:
        return error_response("server_error", 503, "API key not configured on server")
    sent = request.headers.get("X-API-KEY")
    if not sent or sent != API_KEY:
        return error_response("unauthorized", 401, "Unauthorized")
    return None

def parse_pagination():
    page = request.args.get("page", default=1, type=int)
    page_size = request.args.get("page_size", default=50, type=int)
    if page < 1: page = 1
    if page_size < 1: page_size = 1
    if page_size > 200: page_size = 200
    offset = (page - 1) * page_size
    return page, page_size, offset

def parse_sort(allowed_columns, default_sort, default_order="asc"):
    sort = request.args.get("sort", default=default_sort)
    order = request.args.get("order", default=default_order).lower()
    if sort not in allowed_columns: sort = default_sort
    if order not in ("asc", "desc"): order = default_order
    return sort, order

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
def validate_date_str(val: str) -> bool:
    if not val or not DATE_RE.match(val): return False
    try:
        datetime.strptime(val, "%Y-%m-%d")
        return True
    except Exception:
        return False

ISO_MIN_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+\-]\d{2}:\d{2})?$")
def validate_iso8601(val: str) -> bool:
    return bool(val and ISO_MIN_RE.match(val))

SEASON_RE = re.compile(r"^\d{4}-\d{2}$")
def validate_season(val: str) -> bool:
    return bool(val and SEASON_RE.match(val))

def validate_team_name(val: str) -> bool:
    if not val or len(val) > 64:
        return False
    return bool(re.match(r"^[\w\s\-\.\']+$", val, flags=re.UNICODE))

def get_safe_json():
    raw = request.get_data(cache=False, as_text=False)
    if raw and len(raw) > 64 * 1024:
        raise ValueError("payload too large (>64KB)")
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"invalid json: {e}")


# ─────────────────────────────────────────────────────
# Cache helpers
# ─────────────────────────────────────────────────────
def to_utc_dt(dtobj) -> datetime:
    if isinstance(dtobj, datetime):
        if dtobj.tzinfo is None:
            return dtobj.replace(tzinfo=timezone.utc)
        return dtobj.astimezone(timezone.utc)
    return datetime.now(timezone.utc)

def build_cache_keys(query_fingerprint: str, last_modified: datetime, total: int):
    lm_utc = to_utc_dt(last_modified)
    etag_src = f"{query_fingerprint}|{int(lm_utc.timestamp())}|{total}"
    etag = 'W/"' + hashlib.md5(etag_src.encode("utf-8")).hexdigest() + '"'
    last_mod_http = format_datetime(lm_utc, usegmt=True)
    return etag, last_mod_http

def not_modified(if_none_match: str | None, if_modified_since: str | None, etag: str, last_mod_http: str) -> bool:
    if if_none_match and if_none_match.strip() == etag: return True
    if if_modified_since and if_modified_since.strip() == last_mod_http: return True
    return False

def with_cache_headers(resp, etag: str, last_mod_http: str, max_age: int = 30):
    resp.headers["ETag"] = etag
    resp.headers["Last-Modified"] = last_mod_http
    resp.headers["Cache-Control"] = f"public, max-age={max_age}"
    return resp


# ─────────────────────────────────────────────────────
# Base endpoints
# ─────────────────────────────────────────────────────
@app.route("/")
def root():
    return f"Hello from {SERVICE_NAME} API!"

@app.route("/health")
def health():
    return ok_response({"service": SERVICE_NAME, "version": SERVICE_VERSION})

@app.route("/api/test-db")
def test_db():
    try:
        result = fetch_one("SELECT 1;")
        value = result[0] if isinstance(result, (tuple, list)) else (result.get("1") if isinstance(result, dict) else result)
        return ok_response({"db": "connected", "result": value})
    except Exception as e:
        return error_response("server_error", 500, "Database check failed", detail=str(e))


# ─────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────
@app.route("/api/fixtures")
def get_fixtures():
    try:
        league_id = request.args.get("league_id", type=int)
        on_date = request.args.get("date")
        since = request.args.get("since")

        fields_err = {}
        if on_date and not validate_date_str(on_date):
            fields_err["date"] = "format must be YYYY-MM-DD"
        if since and not validate_iso8601(since):
            fields_err["since"] = "format must be ISO8601 (e.g., 2025-11-10T00:00:00Z)"
        if fields_err:
            return error_response("validation_error", 422, "Invalid query parameters", fields=fields_err)

        page, page_size, offset = parse_pagination()
        sort, order = parse_sort(
            allowed_columns={"match_date", "id", "updated_at", "home_team", "away_team", "league_id"},
            default_sort="match_date", default_order="asc"
        )

        base_where, params = "WHERE 1=1", []
        if league_id is not None: base_where += " AND league_id = %s"; params.append(league_id)
        if on_date: base_where += " AND match_date = %s"; params.append(on_date)
        if since: base_where += " AND updated_at >= %s"; params.append(since)

        total_row = fetch_one(f"SELECT COUNT(*) FROM fixtures {base_where}", tuple(params))
        total_val = total_row[0] if isinstance(total_row, (tuple, list)) else (total_row.get("count") if isinstance(total_row, dict) else int(total_row))
        max_row = fetch_one(f"SELECT COALESCE(MAX(updated_at), NOW() AT TIME ZONE 'UTC') FROM fixtures {base_where}", tuple(params))
        max_updated = max_row[0] if isinstance(max_row, (tuple, list)) else list(max_row.values())[0]

        etag, last_mod_http = build_cache_keys(
            f"fixtures|league={league_id}|date={on_date}|since={since}|sort={sort}|order={order}|page={page}|size={page_size}",
            max_updated, int(total_val or 0)
        )

        if not_modified(request.headers.get("If-None-Match"), request.headers.get("If-Modified-Since"), etag, last_mod_http):
            return with_cache_headers(make_response("", 304), etag, last_mod_http)

        rows = fetch_all(f"""
            SELECT id, league_id, match_date, home_team, away_team,
                   home_score, away_score, updated_at
            FROM fixtures
            {base_where}
            ORDER BY {sort} {order}, id ASC
            LIMIT %s OFFSET %s
        """, tuple(params + [page_size, offset]))

        fixtures = [{
            "id": v(r, "id", 0),
            "league_id": v(r, "league_id", 1),
            "match_date": str(v(r, "match_date", 2)),
            "home_team": v(r, "home_team", 3),
            "away_team": v(r, "away_team", 4),
            "home_score": v(r, "home_score", 5),
            "away_score": v(r, "away_score", 6),
            "updated_at": str(v(r, "updated_at", 7)),
        } for r in rows]

        has_next = (page * page_size) < int(total_val or 0)
        return with_cache_headers(ok_response({
            "page": page, "page_size": page_size, "total": int(total_val or 0),
            "has_next": has_next, "fixtures": fixtures
        }), etag, last_mod_http)
    except Exception as e:
        return error_response("server_error", 500, "Failed to fetch fixtures", detail=str(e))


@app.route("/api/fixtures/by-team")
def get_fixtures_by_team():
    try:
        league_id = request.args.get("league_id", type=int)
        team = request.args.get("team")
        on_date = request.args.get("date")
        since = request.args.get("since")

        fields_err = {}
        if not team:
            fields_err["team"] = "team parameter is required"
        elif not validate_team_name(team):
            fields_err["team"] = "invalid team name (max 64, letters/digits/space/-.')"
        if on_date and not validate_date_str(on_date):
            fields_err["date"] = "format must be YYYY-MM-DD"
        if since and not validate_iso8601(since):
            fields_err["since"] = "format must be ISO8601"
        if fields_err:
            return error_response("validation_error", 422, "Invalid query parameters", fields=fields_err)

        page, page_size, offset = parse_pagination()
        sort, order = parse_sort(
            allowed_columns={"match_date", "id", "updated_at", "home_team", "away_team", "league_id"},
            default_sort="match_date", default_order="asc"
        )

        base_where, params = "WHERE (home_team = %s OR away_team = %s)", [team, team]
        if league_id is not None: base_where += " AND league_id = %s"; params.append(league_id)
        if on_date: base_where += " AND match_date = %s"; params.append(on_date)
        if since: base_where += " AND updated_at >= %s"; params.append(since)

        total_row = fetch_one(f"SELECT COUNT(*) FROM fixtures {base_where}", tuple(params))
        total_val = total_row[0] if isinstance(total_row, (tuple, list)) else (total_row.get("count") if isinstance(total_row, dict) else int(total_row))
        max_row = fetch_one(f"SELECT COALESCE(MAX(updated_at), NOW() AT TIME ZONE 'UTC') FROM fixtures {base_where}", tuple(params))
        max_updated = max_row[0] if isinstance(max_row, (tuple, list)) else list(max_row.values())[0]

        etag, last_mod_http = build_cache_keys(
            f"fixtures_by_team|league={league_id}|team={team}|date={on_date}|since={since}|sort={sort}|order={order}|page={page}|size={page_size}",
            max_updated, int(total_val or 0)
        )

        if not_modified(request.headers.get("If-None-Match"), request.headers.get("If-Modified-Since"), etag, last_mod_http):
            return with_cache_headers(make_response("", 304), etag, last_mod_http)

        rows = fetch_all(f"""
            SELECT id, league_id, match_date, home_team, away_team,
                   home_score, away_score, updated_at
            FROM fixtures
            {base_where}
            ORDER BY {sort} {order}, id ASC
            LIMIT %s OFFSET %s
        """, tuple(params + [page_size, offset]))

        fixtures = [{
            "id": v(r, "id", 0),
            "league_id": v(r, "league_id", 1),
            "match_date": str(v(r, "match_date", 2)),
            "home_team": v(r, "home_team", 3),
            "away_team": v(r, "away_team", 4),
            "home_score": v(r, "home_score", 5),
            "away_score": v(r, "away_score", 6),
            "updated_at": str(v(r, "updated_at", 7)),
        } for r in rows]

        has_next = (page * page_size) < int(total_val or 0)
        return with_cache_headers(ok_response({
            "page": page, "page_size": page_size, "total": int(total_val or 0),
            "has_next": has_next, "fixtures": fixtures
        }), etag, last_mod_http)
    except Exception as e:
        return error_response("server_error", 500, "Failed to fetch fixtures by team", detail=str(e))


# Teams
@app.route("/api/teams")
def list_teams():
    try:
        league_id = request.args.get("league_id", type=int)
        q = request.args.get("q")

        page, page_size, offset = parse_pagination()
        sort, order = parse_sort(allowed_columns={"name", "short_name", "id", "league_id"},
                                 default_sort="name", default_order="asc")

        base_where, params = "WHERE 1=1", []
        if league_id is not None: base_where += " AND league_id = %s"; params.append(league_id)
        if q: base_where += " AND LOWER(name) LIKE LOWER(%s)"; params.append(f"%{q}%")

        total_row = fetch_one(f"SELECT COUNT(*) FROM teams {base_where}", tuple(params))
        total_val = total_row[0] if isinstance(total_row, (tuple, list)) else (total_row.get("count") if isinstance(total_row, dict) else int(total_row))

        rows = fetch_all(f"""
            SELECT id, league_id, name, country, short_name
            FROM teams
            {base_where}
            ORDER BY {sort} {order}, id ASC
            LIMIT %s OFFSET %s
        """, tuple(params + [page_size, offset]))

        teams = [{
            "id": v(r, "id", 0),
            "league_id": v(r, "league_id", 1),
            "name": v(r, "name", 2),
            "country": v(r, "country", 3),
            "short_name": v(r, "short_name", 4),
        } for r in rows]

        has_next = (page * page_size) < int(total_val or 0)
        return ok_response({
            "page": page, "page_size": page_size, "total": int(total_val or 0),
            "has_next": has_next, "teams": teams
        })
    except Exception as e:
        return error_response("server_error", 500, "Failed to list teams", detail=str(e))


# Standings
@app.route("/api/standings")
def list_standings():
    try:
        league_id = request.args.get("league_id", type=int)
        season = request.args.get("season")

        if season and not validate_season(season):
            return error_response("validation_error", 422, "Invalid season format",
                                  fields={"season": "format must be YYYY-YY (e.g., 2025-26)"})

        page, page_size, offset = parse_pagination()
        sort, order = parse_sort(allowed_columns={"rank", "points", "team_name", "league_id"},
                                 default_sort="rank", default_order="asc")

        base_where, params = "WHERE 1=1", []
        if league_id is not None: base_where += " AND league_id = %s"; params.append(league_id)
        if season: base_where += " AND season = %s"; params.append(season)

        total_row = fetch_one(f"SELECT COUNT(*) FROM standings {base_where}", tuple(params))
        total_val = total_row[0] if isinstance(total_row, (tuple, list)) else (total_row.get("count") if isinstance(total_row, dict) else int(total_row))

        rows = fetch_all(f"""
            SELECT league_id, season, team_name, rank,
                   played, win, draw, loss, gf, ga, gd, points
            FROM standings
            {base_where}
            ORDER BY {sort} {order}, rank ASC
            LIMIT %s OFFSET %s
        """, tuple(params + [page_size, offset]))

        table = [{
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
        } for r in rows]

        has_next = (page * page_size) < int(total_val or 0)
        return ok_response({
            "page": page, "page_size": page_size, "total": int(total_val or 0),
            "has_next": has_next, "standings": table
        })
    except Exception as e:
        return error_response("server_error", 500, "Failed to list standings", detail=str(e))


# PATCH (protected)
@app.route("/api/fixtures/<int:fixture_id>", methods=["PATCH"])
def update_fixture(fixture_id: int):
    auth_err = require_api_key()
    if auth_err:
        return auth_err
    try:
        try:
            payload = get_safe_json()
        except ValueError as ve:
            return error_response("validation_error", 422, "Invalid JSON body", detail=str(ve))

        fields, params, fields_err = [], [], {}

        if "home_score" in payload:
            hs = payload["home_score"]
            if not isinstance(hs, int) or not (MIN_SCORE <= hs <= MAX_SCORE):
                fields_err["home_score"] = f"must be integer {MIN_SCORE}-{MAX_SCORE}"
            else:
                fields.append("home_score = %s"); params.append(hs)

        if "away_score" in payload:
            as_ = payload["away_score"]
            if not isinstance(as_, int) or not (MIN_SCORE <= as_ <= MAX_SCORE):
                fields_err["away_score"] = f"must be integer {MIN_SCORE}-{MAX_SCORE}"
            else:
                fields.append("away_score = %s"); params.append(as_)

        if fields_err:
            return error_response("validation_error", 422, "Invalid fields", fields=fields_err)
        if not fields:
            return error_response("validation_error", 422, "No fields to update",
                                  hint='Provide at least one of ["home_score","away_score"]')

        row = fetch_one(f"UPDATE fixtures SET {', '.join(fields)} WHERE id = %s RETURNING id;", tuple(params + [fixture_id]))
        if not row:
            return error_response("not_found", 404, "Fixture not found")

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
        return ok_response({"fixture": data})
    except Exception as e:
        return error_response("server_error", 500, "Failed to update fixture", detail=str(e))


# ─────────────────────────────────────────────────────
# OpenAPI spec & Docs
# ─────────────────────────────────────────────────────
def openapi_spec() -> dict:
    return {
        "openapi": "3.0.3",
        "info": {
            "title": f"{SERVICE_NAME} API",
            "version": SERVICE_VERSION,
            "description": "Sports fixtures / teams / standings for SportsStatsX.",
        },
        "servers": [{"url": "https://sports-api-8vlh.onrender.com"}],
        "components": {
            "securitySchemes": {
                "ApiKeyHeader": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "X-API-KEY",
                }
            },
            "schemas": {
                "Fixture": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "league_id": {"type": "integer"},
                        "match_date": {"type": "string", "format": "date"},
                        "home_team": {"type": "string"},
                        "away_team": {"type": "string"},
                        "home_score": {"type": "integer", "nullable": True},
                        "away_score": {"type": "integer", "nullable": True},
                        "updated_at": {"type": "string"},
                    },
                },
                "Team": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "league_id": {"type": "integer"},
                        "name": {"type": "string"},
                        "country": {"type": "string"},
                        "short_name": {"type": "string"},
                    },
                },
                "Standing": {
                    "type": "object",
                    "properties": {
                        "league_id": {"type": "integer"},
                        "season": {"type": "string"},
                        "team_name": {"type": "string"},
                        "rank": {"type": "integer"},
                        "played": {"type": "integer"},
                        "win": {"type": "integer"},
                        "draw": {"type": "integer"},
                        "loss": {"type": "integer"},
                        "gf": {"type": "integer"},
                        "ga": {"type": "integer"},
                        "gd": {"type": "integer"},
                        "points": {"type": "integer"},
                    },
                },
            },
        },
        "paths": {
            "/health": {
                "get": {"summary": "Health check", "responses": {"200": {"description": "OK"}}}
            },
            "/api/fixtures": {
                "get": {
                    "summary": "List fixtures",
                    "parameters": [
                        {"name": "league_id", "in": "query", "schema": {"type": "integer"}},
                        {"name": "date", "in": "query", "schema": {"type": "string", "format": "date"}},
                        {"name": "since", "in": "query", "schema": {"type": "string", "format": "date-time"}},
                        {"name": "sort", "in": "query", "schema": {"type": "string"}},
                        {"name": "order", "in": "query", "schema": {"type": "string", "enum": ["asc", "desc"]}},
                        {"name": "page", "in": "query", "schema": {"type": "integer", "minimum": 1}},
                        {"name": "page_size", "in": "query", "schema": {"type": "integer", "maximum": 200}},
                    ],
                    "responses": {"200": {"description": "OK"}},
                }
            },
            "/api/fixtures/by-team": {
                "get": {
                    "summary": "List fixtures by team",
                    "parameters": [
                        {"name": "team", "in": "query", "required": True, "schema": {"type": "string"}},
                        {"name": "league_id", "in": "query", "schema": {"type": "integer"}},
                        {"name": "date", "in": "query", "schema": {"type": "string", "format": "date"}},
                        {"name": "since", "in": "query", "schema": {"type": "string", "format": "date-time"}},
                        {"name": "page", "in": "query", "schema": {"type": "integer"}},
                        {"name": "page_size", "in": "query", "schema": {"type": "integer"}},
                    ],
                    "responses": {"200": {"description": "OK"}},
                }
            },
            "/api/teams": {
                "get": {
                    "summary": "List teams",
                    "parameters": [
                        {"name": "league_id", "in": "query", "schema": {"type": "integer"}},
                        {"name": "q", "in": "query", "schema": {"type": "string"}},
                        {"name": "sort", "in": "query", "schema": {"type": "string"}},
                        {"name": "order", "in": "query", "schema": {"type": "string", "enum": ["asc", "desc"]}},
                        {"name": "page", "in": "query", "schema": {"type": "integer"}},
                        {"name": "page_size", "in": "query", "schema": {"type": "integer"}},
                    ],
                    "responses": {"200": {"description": "OK"}},
                }
            },
            "/api/standings": {
                "get": {
                    "summary": "List standings",
                    "parameters": [
                        {"name": "league_id", "in": "query", "schema": {"type": "integer"}},
                        {"name": "season", "in": "query", "schema": {"type": "string"}},
                        {"name": "sort", "in": "query", "schema": {"type": "string"}},
                        {"name": "order", "in": "query", "schema": {"type": "string", "enum": ["asc", "desc"]}},
                        {"name": "page", "in": "query", "schema": {"type": "integer"}},
                        {"name": "page_size", "in": "query", "schema": {"type": "integer"}},
                    ],
                    "responses": {"200": {"description": "OK"}},
                }
            },
            "/api/fixtures/{id}": {
                "patch": {
                    "summary": "Update a fixture score",
                    "security": [{"ApiKeyHeader": []}],
                    "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}}],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "home_score": {"type": "integer", "minimum": 0, "maximum": 99},
                                        "away_score": {"type": "integer", "minimum": 0, "maximum": 99},
                                    }
                                }
                            }
                        }
                    },
                    "responses": {"200": {"description": "Updated"}, "401": {"description": "Unauthorized"}},
                }
            },
        },
    }

@app.route("/openapi.json")
def openapi_json():
    spec = openapi_spec()
    return Response(json.dumps(spec, ensure_ascii=False), mimetype="application/json")

# 간단한 Swagger UI (CDN)
@app.route("/docs")
def docs():
    return Response(f"""
<!doctype html>
<html>
  <head>
    <meta charset="utf-8"/>
    <title>{SERVICE_NAME} API Docs</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"/>
    <style> body {{ margin:0; }} #topbar {{ display:none; }}</style>
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
      window.ui = SwaggerUIBundle({{
        url: '/openapi.json',
        dom_id: '#swagger-ui',
        presets: [SwaggerUIBundle.presets.apis],
        layout: 'BaseLayout'
      }});
    </script>
  </body>
</html>
""", mimetype="text/html")


# ─────────────────────────────────────────────────────
# Error handlers
# ─────────────────────────────────────────────────────
@app.errorhandler(400)
def handle_400(err): return error_response("bad_request", 400, "Bad request", detail=str(err))

@app.errorhandler(401)
def handle_401(err): return error_response("unauthorized", 401, "Unauthorized")

@app.errorhandler(404)
def handle_404(err): return error_response("not_found", 404, "Not found")

@app.errorhandler(405)
def handle_405(err): return error_response("method_not_allowed", 405, "Method not allowed")

@app.errorhandler(413)
def handle_413(err): return error_response("payload_too_large", 413, "Request payload too large")

@app.errorhandler(500)
def handle_500(err): return error_response("server_error", 500, "Internal server error", detail=str(err))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
