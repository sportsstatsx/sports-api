# main.py  v1.5.0 — SportsStatsX API (structured logs + metrics + rate limit + timeouts)
import os
import json
import time
import uuid
from datetime import datetime
from functools import wraps
from typing import Dict, Tuple

from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from psycopg_pool import ConnectionPool

# ─────────────────────────────────────────
# Config
# ─────────────────────────────────────────
SERVICE_NAME = os.getenv("SERVICE_NAME", "SportsStatsX")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.5.0")
APP_ENV = os.getenv("APP_ENV", "production")

API_KEY = os.getenv("API_KEY", "")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "60"))
RATE_LIMIT_BURST   = int(os.getenv("RATE_LIMIT_BURST", "30"))
LOG_SAMPLE_RATE    = float(os.getenv("LOG_SAMPLE_RATE", "0.25"))
DB_STATEMENT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "3000"))
DEFAULT_MAX_AGE = 30

# ─────────────────────────────────────────
# App / DB
# ─────────────────────────────────────────
app = Flask(__name__)
CORS(app, resources={
    r"/api/*": {"origins": "*"},
    r"/health": {"origins": "*"},
    r"/docs": {"origins": "*"},
    r"/openapi.json": {"origins": "*"},
    r"/metrics": {"origins": "*"},
})

pool = ConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=5, timeout=10)

# ─────────────────────────────────────────
# Metrics
# ─────────────────────────────────────────
metrics = {
    "start_ts": time.time(),
    "req_total": 0,
    "resp_2xx": 0,
    "resp_4xx": 0,
    "resp_5xx": 0,
    "rate_limited": 0,
    "path_counts": {},
}
def _metrics_incr_path(path: str):
    metrics["path_counts"][path] = metrics["path_counts"].get(path, 0) + 1

# ─────────────────────────────────────────
# Helpers: logging
# ─────────────────────────────────────────
def _now_iso():
    return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

def _client_ip():
    return (request.headers.get("CF-Connecting-IP")
            or request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
            or request.remote_addr
            or "")

def _maybe_log(payload: dict):
    # 샘플링 로그
    try:
        import random
        if random.random() <= LOG_SAMPLE_RATE:
            print(json.dumps(payload, ensure_ascii=False), flush=True)
    except Exception:
        pass

# ─────────────────────────────────────────
# Request lifecycle hooks
# ─────────────────────────────────────────
@app.before_request
def _before():
    rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.environ["x_request_id"] = rid
    request.environ["__t0"] = time.perf_counter()
    metrics["req_total"] += 1
    _metrics_incr_path(request.path)

    _maybe_log({
        "t": "req",
        "ts": _now_iso(),
        "service": SERVICE_NAME,
        "ver": SERVICE_VERSION,
        "env": APP_ENV,
        "request_id": rid,
        "method": request.method,
        "path": request.path,
        "query": request.query_string.decode("utf-8") if request.query_string else "",
        "ip": _client_ip(),
        "ua": request.headers.get("User-Agent", ""),
    })

@app.after_request
def _after(resp: Response):
    rid = request.environ.get("x_request_id")
    if rid:
        resp.headers["X-Request-ID"] = rid

    t0 = request.environ.get("__t0")
    if t0 is not None:
        dur_ms = int((time.perf_counter() - t0) * 1000)
        resp.headers["X-Response-Time"] = str(dur_ms)
    else:
        dur_ms = None

    if 200 <= resp.status_code < 300:
        metrics["resp_2xx"] += 1
    elif 400 <= resp.status_code < 500:
        metrics["resp_4xx"] += 1
        if resp.status_code == 429:
            metrics["rate_limited"] += 1
    else:
        metrics["resp_5xx"] += 1

    if 200 <= resp.status_code < 300 and "Cache-Control" not in resp.headers:
        resp.headers["Cache-Control"] = f"public, max-age={DEFAULT_MAX_AGE}"

    _maybe_log({
        "t": "resp",
        "ts": _now_iso(),
        "service": SERVICE_NAME,
        "ver": SERVICE_VERSION,
        "env": APP_ENV,
        "request_id": rid,
        "method": request.method,
        "path": request.path,
        "status": resp.status_code,
        "duration_ms": dur_ms,
        "ip": _client_ip(),
        "rate_limit": {
            "limit": resp.headers.get("X-RateLimit-Limit"),
            "remaining": resp.headers.get("X-RateLimit-Remaining"),
            "reset": resp.headers.get("X-RateLimit-Reset"),
        },
    })
    return resp

# ─────────────────────────────────────────
# Rate limit (token bucket: ip+path)
# ─────────────────────────────────────────
_rate_buckets: Dict[Tuple[str, str], Dict[str, float]] = {}

def _key():
    return (_client_ip(), request.path)

def _refill(bucket: Dict[str, float], per_min: int):
    now = time.time()
    last = bucket.get("last", now)
    per_sec = per_min / 60.0
    tokens = bucket.get("tokens", float(RATE_LIMIT_BURST))
    tokens = min(RATE_LIMIT_BURST, tokens + (now - last) * per_sec)
    bucket["tokens"], bucket["last"] = tokens, now

def rate_limited(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if RATE_LIMIT_PER_MIN <= 0:
            return f(*args, **kwargs)
        key = _key()
        bucket = _rate_buckets.setdefault(key, {"tokens": float(RATE_LIMIT_BURST), "last": time.time()})
        _refill(bucket, RATE_LIMIT_PER_MIN)
        if bucket["tokens"] >= 1.0:
            bucket["tokens"] -= 1.0
            resp: Response = f(*args, **kwargs)
            reset_sec = max(0, int(60 - (time.time() - bucket["last"])))
            resp.headers["X-RateLimit-Limit"] = str(RATE_LIMIT_PER_MIN)
            resp.headers["X-RateLimit-Remaining"] = str(int(bucket["tokens"]))
            resp.headers["X-RateLimit-Reset"] = str(reset_sec)
            return resp
        reset_sec = max(0, int(60 - (time.time() - bucket["last"])))
        return jsonify({
            "ok": False,
            "error": {"code": "rate_limited", "message": "Too Many Requests", "retry_after_sec": reset_sec}
        }), 429
    return wrapper

# ─────────────────────────────────────────
# Auth
# ─────────────────────────────────────────
def require_api_key(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        client_key = request.headers.get("X-API-KEY", "")
        if not API_KEY or client_key != API_KEY:
            return jsonify({"ok": False, "error": "unauthorized",
                            "detail": "API key not configured on server" if not API_KEY else "invalid api key"}), 401
        return f(*args, **kwargs)
    return wrapper

# ─────────────────────────────────────────
# DB helpers (statement_timeout)
# ─────────────────────────────────────────
def _set_statement_timeout(conn):
    try:
        conn.execute(f"SET LOCAL statement_timeout = {DB_STATEMENT_TIMEOUT_MS}")
    except Exception:
        try:
            conn.execute(f"SET statement_timeout = {DB_STATEMENT_TIMEOUT_MS}")
        except Exception:
            pass

def fetch_all(sql: str, params: tuple = ()):
    with pool.connection() as conn:
        _set_statement_timeout(conn)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

def fetch_one(sql: str, params: tuple = ()):
    with pool.connection() as conn:
        _set_statement_timeout(conn)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row:
                return None
            cols = [c[0] for c in cur.description]
            return dict(zip(cols, row))

def execute(sql: str, params: tuple = ()):
    with pool.connection() as conn:
        _set_statement_timeout(conn)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount

# ─────────────────────────────────────────
# OpenAPI / Docs
# ─────────────────────────────────────────
OPENAPI = {
    "openapi": "3.0.3",
    "info": {"title": f"{SERVICE_NAME} API", "version": SERVICE_VERSION, "description": "Sports fixtures / teams / standings for SportsStatsX."},
    "servers": [{"url": "https://sports-api-8vlh.onrender.com"}],
    "components": {"securitySchemes": {"ApiKeyHeader": {"type": "apiKey", "in": "header", "name": "X-API-KEY"}}},
    "paths": {
        "/health": {"get": {"summary": "Health check", "responses": {"200": {"description": "OK"}}}},
        "/metrics": {"get": {"summary": "Service metrics (JSON)", "responses": {"200": {"description": "OK"}}}},
        "/api/fixtures": {"get": {"summary": "List fixtures", "parameters": [
            {"name": "league_id", "in": "query", "schema": {"type": "integer"}},
            {"name": "date", "in": "query", "schema": {"type": "string", "format": "date"}},
            {"name": "page", "in": "query", "schema": {"type": "integer"}},
            {"name": "page_size", "in": "query", "schema": {"type": "integer"}},
            {"name": "since", "in": "query", "schema": {"type": "string"}}
        ], "responses": {"200": {"description": "OK"}}}},
        "/api/fixtures/by-team": {"get": {"summary": "List fixtures by team", "parameters": [
            {"name": "league_id", "in": "query", "schema": {"type": "integer"}},
            {"name": "team", "in": "query", "schema": {"type": "string"}}
        ], "responses": {"200": {"description": "OK"}}}},
        "/api/teams": {"get": {"summary": "List teams", "parameters": [
            {"name": "league_id", "in": "query", "schema": {"type": "integer"}},
            {"name": "sort", "in": "query", "schema": {"type": "string"}, "example": "short_name"},
            {"name": "order", "in": "query", "schema": {"type": "string", "enum": ["asc", "desc"]}}
        ], "responses": {"200": {"description": "OK"}}}},
        "/api/standings": {"get": {"summary": "List standings", "parameters": [
            {"name": "league_id", "in": "query", "schema": {"type": "integer"}},
            {"name": "season", "in": "query", "schema": {"type": "string"}},
            {"name": "sort", "in": "query", "schema": {"type": "string"}},
            {"name": "order", "in": "query", "schema": {"type": "string", "enum": ["asc", "desc"]}}
        ], "responses": {"200": {"description": "OK"}}}},
        "/api/fixtures/{id}": {"patch": {"summary": "Update a fixture score", "security": [{"ApiKeyHeader": []}],
            "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}}],
            "requestBody": {"required": True, "content": {"application/json": {"schema": {
                "type": "object", "properties": {"home_score": {"type": "integer", "minimum": 0, "maximum": 99}, "away_score": {"type": "integer", "minimum": 0, "maximum": 99}}
            }}}},
            "responses": {"200": {"description": "Updated"}, "401": {"description": "Unauthorized"}}}}
    }
}

@app.get("/openapi.json")
@rate_limited
def openapi_json():
    return jsonify(OPENAPI)

SWAGGER_HTML = """<!doctype html>
<html>
<head><meta charset="utf-8"/><title>SportsStatsX API Docs</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist/swagger-ui.css"></head>
<body><div id="swagger"></div>
<script src="https://unpkg.com/swagger-ui-dist/swagger-ui-bundle.js"></script>
<script>window.ui=SwaggerUIBundle({url:'/openapi.json',dom_id:'#swagger'});</script>
</body></html>"""

@app.get("/docs")
def docs():
    return Response(SWAGGER_HTML, mimetype="text/html")

# ─────────────────────────────────────────
# Validators / Errors
# ─────────────────────────────────────────
def parse_date(d: str):
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        return None

def error_400(fields=None, message="Invalid query parameters"):
    return jsonify({"ok": False, "error": {"code": "validation_error", "fields": fields or {}, "message": message}}), 422

# ─────────────────────────────────────────
# API Endpoints
# ─────────────────────────────────────────
@app.get("/")
def root():
    return "Hello from SportsStatsX API!"

@app.get("/health")
@rate_limited
def health():
    return jsonify({
        "ok": True,
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "env": APP_ENV,
        "uptime_sec": int(time.time() - ps_start_ts)
    })

@app.get("/metrics")
@rate_limited
def get_metrics():
    return jsonify({
        "ok": True,
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "env": APP_ENV,
        "since": int(metrics["start_ts"]),
        "uptime_sec": int(time.time() - metrics["start_ts"]),
        "requests": {
            "total": metrics["req_total"],
            "2xx": metrics["resp_2xx"],
            "4xx": metrics["resp_4xx"],
            "5xx": metrics["resp_5xx"],
            "rate_limited": metrics["rate_limited"],
        },
        "paths": metrics["path_counts"],
        "rate_limit": {"per_min": RATE_LIMIT_PER_MIN, "burst": RATE_LIMIT_BURST}
    })

@app.get("/api/fixtures")
@rate_limited
def list_fixtures():
    league_id = request.args.get("league_id", type=int)
    date_str  = request.args.get("date")
    page      = max(1, request.args.get("page", default=1, type=int))
    page_size = max(1, min(100, request.args.get("page_size", default=50, type=int)))
    since     = request.args.get("since")

    where, params = [], []
    if league_id: where.append("league_id = %s"); params.append(league_id)
    if date_str:
        d = parse_date(date_str)
        if not d: return error_400({"date": "format must be YYYY-MM-DD"})
        where.append("match_date = %s"); params.append(d)
    if since:
        where.append("updated_at >= %s"); params.append(since)

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    total_row = fetch_one(f"SELECT COUNT(*) AS cnt FROM fixtures{where_sql}", tuple(params))
    total = total_row["cnt"] if total_row else 0

    offset = (page - 1) * page_size
    rows = fetch_all(f"""
        SELECT id, league_id, match_date, home_team, away_team, home_score, away_score, updated_at
        FROM fixtures
        {where_sql}
        ORDER BY id ASC
        LIMIT %s OFFSET %s
    """, tuple(params + [page_size, offset]))

    etag_seed = json.dumps(rows, default=str)
    etag = f'W/"{hash(etag_seed)}"'
    if request.headers.get("If-None-Match") == etag:
        return Response(status=304)

    resp = jsonify({"ok": True, "fixtures": rows, "total": total, "page": page, "page_size": page_size,
                    "has_next": (offset + page_size) < total})
    resp.headers["ETag"] = etag
    if rows:
        lm = max(datetime.fromisoformat(str(r["updated_at"]).replace("Z", "")) for r in rows)
        resp.headers["Last-Modified"] = lm.strftime("%a, %d %b %Y %H:%M:%S GMT")
    return resp

@app.get("/api/fixtures/by-team")
@rate_limited
def fixtures_by_team():
    league_id = request.args.get("league_id", type=int)
    team      = request.args.get("team", type=str)
    if not (league_id and team):
        return error_400({"league_id": "required", "team": "required"})
    rows = fetch_all("""
        SELECT id, league_id, match_date, home_team, away_team, home_score, away_score, updated_at
        FROM fixtures
        WHERE league_id=%s AND (home_team=%s OR away_team=%s)
        ORDER BY match_date ASC, id ASC
    """, (league_id, team, team))
    return jsonify({"ok": True, "count": len(rows), "fixtures": rows})

@app.get("/api/teams")
@rate_limited
def teams():
    league_id = request.args.get("league_id", type=int)
    sort = request.args.get("sort", default="id", type=str)
    order = request.args.get("order", default="asc", type=str)
    if league_id is None:
        return error_400({"league_id": "required"})
    if order not in ("asc", "desc"):
        return error_400({"order": "must be asc|desc"})
    if sort not in ("id", "name", "short_name"):
        sort = "id"
    rows = fetch_all(f"""
        SELECT id, league_id, name, short_name, country
        FROM teams WHERE league_id=%s
        ORDER BY {sort} {order}
    """, (league_id,))
    return jsonify({"ok": True, "total": len(rows), "teams": rows})

@app.get("/api/standings")
@rate_limited
def standings():
    league_id = request.args.get("league_id", type=int)
    season    = request.args.get("season", type=str)
    sort      = request.args.get("sort", default="points", type=str)
    order     = request.args.get("order", default="desc", type=str)
    if league_id is None or not season:
        return error_400({"league_id": "required", "season": "required"})
    if order not in ("asc", "desc"):
        return error_400({"order": "must be asc|desc"})
    if sort not in ("rank", "points", "gf", "ga", "gd", "win", "draw", "loss", "played"):
        sort, order = "points", "desc"

    rows = fetch_all(f"""
        SELECT league_id, season, team_name, rank, played, win, draw, loss, gf, ga, gd, points
        FROM standings
        WHERE league_id=%s AND season=%s
        ORDER BY {sort} {order}
    """, (league_id, season))
    return jsonify({"ok": True, "total": len(rows), "standings": rows})

@app.patch("/api/fixtures/<int:fx_id>")
@rate_limited
@require_api_key
def update_fixture(fx_id: int):
    try:
        payload = request.get_json(force=True, silent=False)
    except Exception as e:
        return jsonify({"ok": False, "error": {"code": "validation_error", "message": "Invalid JSON body",
                                               "detail": f"invalid json: {e}"}},), 422
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": {"code": "validation_error", "message": "Invalid JSON body"}},), 422

    home_score = payload.get("home_score")
    away_score = payload.get("away_score")
    if home_score is None and away_score is None:
        return jsonify({"ok": False, "error": {"code": "validation_error",
                                               "message": "No fields to update",
                                               "hint": "Provide at least one of [\"home_score\",\"away_score\"]"}}), 422

    fields, params = [], []
    if home_score is not None:
        if not (isinstance(home_score, int) and 0 <= home_score <= 99):
            return jsonify({"ok": False, "error": {"code": "validation_error",
                                                   "fields": {"home_score": "must be integer 0-99"},
                                                   "message": "Invalid fields"}}), 422
        fields.append("home_score=%s"); params.append(home_score)
    if away_score is not None:
        if not (isinstance(away_score, int) and 0 <= away_score <= 99):
            return jsonify({"ok": False, "error": {"code": "validation_error",
                                                   "fields": {"away_score": "must be integer 0-99"},
                                                   "message": "Invalid fields"}}), 422
        fields.append("away_score=%s"); params.append(away_score)

    fields.append("updated_at=NOW()")
    params.append(fx_id)

    rowcount = execute(f"UPDATE fixtures SET {', '.join(fields)} WHERE id=%s", tuple(params))
    if rowcount == 0:
        return jsonify({"ok": False, "error": "not_found"}), 404

    row = fetch_one("""
        SELECT id, league_id, match_date, home_team, away_team, home_score, away_score, updated_at
        FROM fixtures WHERE id=%s
    """, (fx_id,))
    return jsonify({"ok": True, "fixture": row})

# ─────────────────────────────────────────
ps_start_ts = time.time()
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
