import os
import time
import uuid
import hashlib
from functools import wraps
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, jsonify, request, g, Response, make_response
from flask_cors import CORS
import psycopg
from psycopg.rows import dict_row

# ------------------------------------------------------------------------------
# 환경 변수 / 기본값
# ------------------------------------------------------------------------------
SERVICE_NAME = os.getenv("SERVICE_NAME", "SportsStatsX")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.5.0")
APP_ENV = os.getenv("APP_ENV", "production")

# API Key 로테이션 (무중단 교체)
API_KEY_PRIMARY = os.getenv("API_KEY_PRIMARY", os.getenv("API_KEY", ""))  # 과거 단일키도 흡수
API_KEY_BACKUP = os.getenv("API_KEY_BACKUP", "")

# 레이트리밋 (IP 기반 토큰버킷)
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "60"))
RATE_LIMIT_BURST = int(os.getenv("RATE_LIMIT_BURST", "30"))

# 관찰성 로그 샘플링
LOG_SAMPLE_RATE = float(os.getenv("LOG_SAMPLE_RATE", "0.25"))

DATABASE_URL = os.getenv("DATABASE_URL", "")

# ------------------------------------------------------------------------------
# 앱 생성
# ------------------------------------------------------------------------------
app = Flask(__name__)
CORS(app)

# ------------------------------------------------------------------------------
# 간단한 메모리 메트릭
# ------------------------------------------------------------------------------
metrics: Dict[str, Any] = {
    "start_ts": time.time(),
    "req_total": 0,
    "resp_2xx": 0,
    "resp_4xx": 0,
    "resp_5xx": 0,
    "rate_limited": 0,
    "path_counts": {}
}

# ------------------------------------------------------------------------------
# 유틸
# ------------------------------------------------------------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _should_log() -> bool:
    # 확률적으로 전체 로그 남김
    import random
    return random.random() < LOG_SAMPLE_RATE

def db_conn():
    if not hasattr(g, "_pg_conn"):
        # autocommit=True로 간단 운용
        g._pg_conn = psycopg.connect(DATABASE_URL, autocommit=True, row_factory=dict_row)
    return g._pg_conn

@app.teardown_appcontext
def teardown_db(exception):
    conn = getattr(g, "_pg_conn", None)
    if conn:
        conn.close()

def fetch_all(sql: str, params: Tuple = ()) -> List[Dict[str, Any]]:
    with db_conn().cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())

def fetch_one(sql: str, params: Tuple = ()) -> Optional[Dict[str, Any]]:
    with db_conn().cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None

def execute(sql: str, params: Tuple = ()) -> int:
    with db_conn().cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount

def strong_etag(payload_bytes: bytes) -> str:
    # 강한 ETag(W/ 접두 없는) 대신 안정적 약한 ETag로 운용
    # 아래는 약한 ETag 포맷: W/"<hash>"
    h = hashlib.md5(payload_bytes).hexdigest()
    return f'W/"{h}"'

# ------------------------------------------------------------------------------
# 미들웨어: 요청 ID / 응답 시간 / 메트릭
# ------------------------------------------------------------------------------
@app.before_request
def _before_request():
    g.req_start = time.time()
    g.request_id = str(uuid.uuid4())
    # 메트릭 집계
    metrics["req_total"] += 1
    path = request.path or "/"
    metrics["path_counts"][path] = metrics["path_counts"].get(path, 0) + 1

    if _should_log():
        app.logger.info({
            "t": "req",
            "service": SERVICE_NAME,
            "ver": SERVICE_VERSION,
            "env": APP_ENV,
            "method": request.method,
            "path": request.path,
            "remote": request.headers.get("CF-Connecting-IP") or request.remote_addr,
            "ts": _now_utc().isoformat()
        })

@app.after_request
def _after_request(resp):
    # 응답 헤더 기본 세트
    resp.headers["X-Request-ID"] = g.get("request_id", "-")
    resp.headers["X-Response-Time"] = str(int((time.time() - g.get("req_start", time.time())) * 1000))
    # 레이트리밋 현재 상태 헤더
    resp.headers["X-RateLimit-Limit"] = str(RATE_LIMIT_PER_MIN)
    resp.headers["X-RateLimit-Remaining"] = str(bucket_allowance(request) )  # 참고용(대략)
    resp.headers["X-RateLimit-Reset"] = "59"

    # 메트릭 응답 클래스
    try:
        sc = int(resp.status_code)
    except Exception:
        sc = 0
    if 200 <= sc < 300:
        metrics["resp_2xx"] += 1
    elif 400 <= sc < 500:
        metrics["resp_4xx"] += 1
    elif 500 <= sc < 600:
        metrics["resp_5xx"] += 1

    if _should_log():
        app.logger.info({
            "t": "resp",
            "service": SERVICE_NAME,
            "ver": SERVICE_VERSION,
            "env": APP_ENV,
            "status": sc,
            "duration_ms": int((time.time() - g.get("req_start", time.time())) * 1000),
            "path": request.path,
            "ts": _now_utc().isoformat(),
            "request_id": g.get("request_id", "-")
        })
    return resp

# ------------------------------------------------------------------------------
# 레이트리밋 (간단 토큰버킷: 메모리/IP 단위)
# ------------------------------------------------------------------------------
_RATE_BUCKETS: Dict[str, Dict[str, Any]] = {}

def bucket_allowance(req) -> int:
    ip = req.headers.get("CF-Connecting-IP") or req.remote_addr or "-"
    b = _RATE_BUCKETS.get(ip)
    if not b:
        return RATE_LIMIT_PER_MIN
    return int(b.get("tokens", RATE_LIMIT_PER_MIN))

def rate_limited(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        ip = request.headers.get("CF-Connecting-IP") or request.remote_addr or "-"
        now = time.time()
        bucket = _RATE_BUCKETS.get(ip)
        if not bucket:
            bucket = {"tokens": RATE_LIMIT_PER_MIN, "ts": now}
            _RATE_BUCKETS[ip] = bucket
        # 토큰 재충전
        elapsed = now - bucket["ts"]
        refill = elapsed * (RATE_LIMIT_PER_MIN / 60.0)  # 초당 토큰
        bucket["tokens"] = min(RATE_LIMIT_BURST, bucket["tokens"] + refill)
        bucket["ts"] = now

        if bucket["tokens"] < 1.0:
            metrics["rate_limited"] += 1
            return make_response(jsonify({
                "ok": False,
                "error": "rate_limited",
                "retry_after_sec": 1
            }), 429)

        bucket["tokens"] -= 1.0
        return f(*args, **kwargs)
    return wrapper

# ------------------------------------------------------------------------------
# 인증 (API Key 로테이션)
# ------------------------------------------------------------------------------
def _get_key_from_request() -> str:
    # 1) Authorization: Bearer <KEY>
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    # 2) X-API-Key: <KEY>
    h = request.headers.get("X-API-Key")
    if h:
        return h.strip()
    # 3) ?api_key=<KEY>
    q = request.args.get("api_key")
    if q:
        return q.strip()
    return ""

def _is_valid_key(key: str) -> bool:
    if not key:
        return False
    # PRIMARY/ BACKUP 동시 허용 → 무중단 교체 가능
    if API_KEY_PRIMARY and key == API_KEY_PRIMARY:
        return True
    if API_KEY_BACKUP and key == API_KEY_BACKUP:
        return True
    return False

def require_api_key(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        key = _get_key_from_request()
        if not _is_valid_key(key):
            return make_response(jsonify({"ok": False, "error": "unauthorized"}), 401)
        return f(*args, **kwargs)
    return wrapper

# ------------------------------------------------------------------------------
# 루트/헬스/메트릭
# ------------------------------------------------------------------------------
@app.get("/")
@rate_limited
def root():
    return jsonify({
        "ok": True,
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "env": APP_ENV
    })

@app.get("/health")
@rate_limited
def health():
    return jsonify({
        "ok": True,
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "env": APP_ENV,
        "uptime_sec": int(time.time() - metrics["start_ts"])
    })

@app.get("/metrics")
@rate_limited
def metrics_json():
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
        "rate_limit": {
            "per_min": RATE_LIMIT_PER_MIN,
            "burst": RATE_LIMIT_BURST
        }
    })

@app.get("/metrics_prom")
@rate_limited
def metrics_prom():
    # Prometheus Exposition Format (text/plain; version=0.0.4)
    lines = []
    lines.append("# HELP sportsstatsx_requests_total Total requests since start")
    lines.append("# TYPE sportsstatsx_requests_total counter")
    lines.append(f"sportsstatsx_requests_total {metrics['req_total']}")

    lines.append("# HELP sportsstatsx_responses_count Response counts by class")
    lines.append("# TYPE sportsstatsx_responses_count counter")
    lines.append(f"sportsstatsx_responses_count{{class=\"2xx\"}} {metrics['resp_2xx']}")
    lines.append(f"sportsstatsx_responses_count{{class=\"4xx\"}} {metrics['resp_4xx']}")
    lines.append(f"sportsstatsx_responses_count{{class=\"5xx\"}} {metrics['resp_5xx']}")

    lines.append("# HELP sportsstatsx_rate_limited Total 429 responses")
    lines.append("# TYPE sportsstatsx_rate_limited counter")
    lines.append(f"sportsstatsx_rate_limited {metrics['rate_limited']}")

    lines.append("# HELP sportsstatsx_path_requests_total Requests per path")
    lines.append("# TYPE sportsstatsx_path_requests_total counter")
    for p, c in metrics["path_counts"].items():
        sp = p.replace("\\", "\\\\").replace("\"", "\\\"")
        lines.append(f'sportsstatsx_path_requests_total{{path="{sp}"}} {c}')

    uptime = int(time.time() - metrics["start_ts"])
    lines.append("# HELP sportsstatsx_uptime_seconds Uptime in seconds")
    lines.append("# TYPE sportsstatsx_uptime_seconds gauge")
    lines.append(f"sportsstatsx_uptime_seconds {uptime}")

    txt = "\n".join(lines) + "\n"
    return Response(txt, mimetype="text/plain; version=0.0.4")

# ------------------------------------------------------------------------------
# API: Teams / Fixtures / Standings
# ------------------------------------------------------------------------------
@app.get("/api/teams")
@rate_limited
def api_teams():
    rows = fetch_all("""
        SELECT id, name, league_id, created_at, updated_at
        FROM teams
        ORDER BY id
    """)
    payload = {"ok": True, "teams": rows}
    js = jsonify(payload)
    # 캐시 헤더 (간단)
    resp = make_response(js, 200)
    body = resp.get_data()
    resp.headers["ETag"] = strong_etag(body)
    resp.headers["Last-Modified"] = _now_utc().strftime("%a, %d %b %Y %H:%M:%S GMT")
    resp.headers["Cache-Control"] = "public, max-age=30"
    return resp

@app.get("/api/standings")
@rate_limited
def api_standings():
    league_id = request.args.get("league_id", type=int)
    if not league_id:
        return make_response(jsonify({"ok": False, "error": "league_id required"}), 400)
    rows = fetch_all("""
        SELECT team_id, league_id, season, position, points, played, won, draw, lost,
               goals_for, goals_against, goal_diff, updated_at
        FROM standings
        WHERE league_id = %s
        ORDER BY position ASC
    """, (league_id,))
    payload = {"ok": True, "standings": rows}
    js = jsonify(payload)
    resp = make_response(js, 200)
    body = resp.get_data()
    resp.headers["ETag"] = strong_etag(body)
    resp.headers["Last-Modified"] = _now_utc().strftime("%a, %d %b %Y %H:%M:%S GMT")
    resp.headers["Cache-Control"] = "public, max-age=30"
    return resp

@app.get("/api/fixtures")
@rate_limited
def api_fixtures():
    league_id = request.args.get("league_id", type=int)
    page = request.args.get("page", default=1, type=int)
    page_size = request.args.get("page_size", default=20, type=int)
    if not league_id:
        return make_response(jsonify({"ok": False, "error": "league_id required"}), 400)

    offset = (page - 1) * page_size
    rows = fetch_all("""
        SELECT id, league_id, home_team, away_team, home_score, away_score,
               match_date, updated_at
        FROM fixtures
        WHERE league_id = %s
        ORDER BY match_date DESC, id DESC
        LIMIT %s OFFSET %s
    """, (league_id, page_size, offset))

    # total 개수
    total_row = fetch_one("SELECT COUNT(*) AS c FROM fixtures WHERE league_id = %s", (league_id,))
    total = total_row["c"] if total_row else 0
    has_next = (offset + page_size) < total

    payload = {
        "ok": True,
        "fixtures": rows,
        "page": page,
        "page_size": page_size,
        "total": total,
        "has_next": has_next
    }
    js = jsonify(payload)
    resp = make_response(js, 200)
    body = resp.get_data()
    resp.headers["ETag"] = strong_etag(body)
    # updated_at 컬럼 최신값을 Last-Modified로 사용(간단화)
    lm = fetch_one("""
        SELECT COALESCE(MAX(updated_at), NOW() AT TIME ZONE 'UTC') AS lm
        FROM fixtures WHERE league_id = %s
    """, (league_id,))
    last_mod = lm["lm"] if lm and lm["lm"] else _now_utc()
    if isinstance(last_mod, datetime):
        last_mod_str = last_mod.strftime("%a, %d %b %Y %H:%M:%S GMT")
    else:
        last_mod_str = _now_utc().strftime("%a, %d %b %Y %H:%M:%S GMT")
    resp.headers["Last-Modified"] = last_mod_str
    resp.headers["Cache-Control"] = "public, max-age=30"
    return resp

@app.patch("/api/fixtures/<int:fixture_id>")
@rate_limited
@require_api_key
def api_patch_fixture(fixture_id: int):
    data = request.get_json(silent=True) or {}
    home_score = data.get("home_score")
    away_score = data.get("away_score")

    if home_score is None and away_score is None:
        return make_response(jsonify({"ok": False, "error": "no fields to update"}), 400)

    sets = []
    params: List[Any] = []
    if home_score is not None:
        sets.append("home_score = %s")
        params.append(int(home_score))
    if away_score is not None:
        sets.append("away_score = %s")
        params.append(int(away_score))

    params.append(fixture_id)
    sql = f"UPDATE fixtures SET {', '.join(sets)}, updated_at = NOW() AT TIME ZONE 'UTC' WHERE id = %s"
    changed = execute(sql, tuple(params))
    if changed < 1:
        return make_response(jsonify({"ok": False, "error": "not_found"}), 404)

    row = fetch_one("""
        SELECT id, league_id, home_team, away_team, home_score, away_score,
               match_date, updated_at
        FROM fixtures WHERE id = %s
    """, (fixture_id,))
    return jsonify({"ok": True, "fixture": row})

# ------------------------------------------------------------------------------
# OpenAPI/Swagger (경량 안내)
# ------------------------------------------------------------------------------
@app.get("/openapi.json")
@rate_limited
def openapi_json():
    # 실제 OpenAPI 스키마를 전부 적기엔 장황하므로, 최소 안내만 유지
    return jsonify({
        "openapi": "3.0.0",
        "info": {"title": SERVICE_NAME, "version": SERVICE_VERSION},
        "paths": {
            "/health": {"get": {}},
            "/metrics": {"get": {}},
            "/metrics_prom": {"get": {}},
            "/api/fixtures": {"get": {}},
            "/api/fixtures/{id}": {"patch": {}},
            "/api/teams": {"get": {}},
            "/api/standings": {"get": {}},
        }
    })

@app.get("/docs")
@rate_limited
def docs_redirect():
    # 간단한 안내 페이지
    html = f"""
    <html>
      <head><title>{SERVICE_NAME} API Docs</title></head>
      <body style="font-family: ui-sans-serif, system-ui;">
        <h1>{SERVICE_NAME} API</h1>
        <p>Env: <b>{APP_ENV}</b> | Version: <b>{SERVICE_VERSION}</b></p>
        <ul>
          <li><code>GET /health</code></li>
          <li><code>GET /metrics</code> (JSON)</li>
          <li><code>GET /metrics_prom</code> (Prometheus format)</li>
          <li><code>GET /api/fixtures?league_id=39&page=1&page_size=20</code></li>
          <li><code>PATCH /api/fixtures/&lt;id&gt;</code> <small>(Authorization: Bearer &lt;API_KEY_PRIMARY/ BACKUP&gt;)</small></li>
          <li><code>GET /api/teams</code></li>
          <li><code>GET /api/standings?league_id=39</code></li>
          <li><code>GET /openapi.json</code></li>
        </ul>
      </body>
    </html>
    """
    return Response(html, mimetype="text/html")

# ------------------------------------------------------------------------------
# 실행
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # Render는 gunicorn 등을 쓰지만 로컬 테스트용
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
