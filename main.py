# main.py  v1.6.1 — SportsStatsX API
# - Prometheus 표준 /metrics 유지 (prometheus_client)
# - 기존 커스텀 텍스트 /metrics_prom 유지
# - 요청 카운트/지연/429/예외 카운트 계측
# - Grafana/Prometheus 룰과 라벨 스키마 정합성 강화

import os
import json
import time
import uuid
from datetime import datetime
from functools import wraps
from typing import Dict
from collections import defaultdict

from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from psycopg_pool import ConnectionPool

# ─────────────────────────────────────────
# Prometheus client
# ─────────────────────────────────────────
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# ─────────────────────────────────────────
# Config
# ─────────────────────────────────────────
SERVICE_NAME = os.getenv("SERVICE_NAME", "SportsStatsX")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.6.1")  # bumped
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
    r"/metrics": {"origins": "*"},       # Prometheus scrape (표준)
    r"/metrics_prom": {"origins": "*"},  # 커스텀 텍스트
    r"/metrics_json": {"origins": "*"},  # 레거시 JSON
})

pool = ConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=5, timeout=10)

# ─────────────────────────────────────────
# In-memory metrics (legacy JSON + custom text)
# ─────────────────────────────────────────
metrics = {
    "start_ts": time.time(),
    "req_total": 0,
    "resp_2xx": 0,
    "resp_4xx": 0,
    "resp_5xx": 0,
    "rate_limited": 0,
    "path_counts": {},  # path -> count
}
def _metrics_incr_path(path: str):
    metrics["path_counts"][path] = metrics["path_counts"].get(path, 0) + 1

# ─────────────────────────────────────────
# Prometheus metric objects
# ─────────────────────────────────────────
# NOTE: path는 정적 경로만 사용(동적 세그먼트가 생기면 정규화 필요)
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"]
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "Request latency (seconds)",
    ["method", "path"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 8, 13)
)

RATE_LIMITED_TOTAL = Counter(
    "http_requests_rate_limited_total",
    "Total number of 429 responses"
)

EXCEPTIONS_TOTAL = Counter(
    "exceptions_total",
    "Unhandled exceptions count",
    ["path"]
)

UPTIME_SECONDS = Gauge(
    "sportsstatsx_uptime_seconds",
    "Uptime in seconds"
)

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
    # uptime 지속 갱신
    UPTIME_SECONDS.set(int(time.time() - metrics["start_ts"]))

    rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.environ["x_request_id"] = rid
    request.environ["__t0"] = time.perf_counter()

    # 내부/Prometheus 경로도 카운트(트래픽 관찰용)
    metrics["req_total"] += 1
    _metrics_incr_path(request.path)

    _maybe_log({
        "t": "req", "ts": _now_iso(), "service": SERVICE_NAME,
        "ver": SERVICE_VERSION, "env": APP_ENV,
        "request_id": rid, "method": request.method,
        "path": request.path, "query": request.query_string.decode("utf-8") if request.query_string else "",
        "ip": _client_ip(), "ua": request.headers.get("User-Agent", ""),
    })

@app.after_request
def _after(resp: Response):
    rid = request.environ.get("x_request_id")
    if rid:
        resp.headers["X-Request-ID"] = rid

    t0 = request.environ.get("__t0")
    dur_s = (time.perf_counter() - t0) if t0 else None
    if dur_s is not None:
        resp.headers["X-Response-Time"] = str(int(dur_s * 1000))
        HTTP_REQUEST_DURATION_SECONDS.labels(request.method, request.path).observe(dur_s)

    sc = resp.status_code
    if 200 <= sc < 300:
        metrics["resp_2xx"] += 1
    elif 400 <= sc < 500:
        metrics["resp_4xx"] += 1
        if sc == 429:
            metrics["rate_limited"] += 1
            RATE_LIMITED_TOTAL.inc()
    else:
        metrics["resp_5xx"] += 1

    HTTP_REQUESTS_TOTAL.labels(request.method, request.path, str(sc)).inc()

    # 캐시 헤더 기본값(성공 응답만)
    if 200 <= sc < 300 and "Cache-Control" not in resp.headers:
        resp.headers["Cache-Control"] = f"public, max-age={DEFAULT_MAX_AGE}"

    _maybe_log({
        "t": "resp", "ts": _now_iso(), "service": SERVICE_NAME,
        "ver": SERVICE_VERSION, "env": APP_ENV,
        "status": sc, "path": request.path,
        "dur_ms": int(dur_s * 1000) if dur_s is not None else None,
        "ip": _client_ip(),
    })
    return resp

# 전역 예외 핸들러(500 발생 시 계측)
@app.errorhandler(Exception)
def _handle_exception(e):
    try:
        EXCEPTIONS_TOTAL.labels(path=(request.path or "unknown")).inc()
    except Exception:
        pass
    # 필요시 상세 로깅
    # app.logger.exception("Unhandled exception")
    return jsonify({"ok": False, "error": "internal-error"}), 500

# ─────────────────────────────────────────
# Rate Limiter
# ─────────────────────────────────────────
rate_state: Dict[str, Dict[str, float]] = defaultdict(lambda: {"tokens": RATE_LIMIT_BURST, "last": time.time()})

def rate_limited(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        # 메트릭/헬스는 무조건 통과 (Prometheus scrape 방해 금지)
        if request.path in ("/metrics", "/metrics_prom", "/health"):
            return f(*args, **kwargs)

        ip = _client_ip() or "unknown"
        bucket = rate_state[ip]
        now = time.time()
        elapsed = now - bucket["last"]
        bucket["tokens"] = min(RATE_LIMIT_BURST, bucket["tokens"] + elapsed * (RATE_LIMIT_PER_MIN / 60))
        bucket["last"] = now
        if bucket["tokens"] < 1:
            reset_sec = max(0, int(60 - (time.time() - bucket["last"])))
            return jsonify({
                "ok": False,
                "error": {"code": "rate_limited", "message": "Too Many Requests", "retry_after_sec": reset_sec}
            }), 429
        bucket["tokens"] -= 1
        return f(*args, **kwargs)
    return wrapper

# ─────────────────────────────────────────
# Auth (옵션)
# ─────────────────────────────────────────
def require_api_key(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        client_key = request.headers.get("X-API-KEY", "")
        if not API_KEY or client_key != API_KEY:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper

# ─────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────
def _set_statement_timeout(conn):
    try:
        conn.execute(f"SET LOCAL statement_timeout = {DB_STATEMENT_TIMEOUT_MS}")
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

# ─────────────────────────────────────────
# API Endpoints
# ─────────────────────────────────────────
@app.get("/")
def root():
    return "Hello from SportsStatsX API!"

@app.get("/health")
def health_plain():
    # plain text "ok" with 200 OK (Blackbox http_2xx_strict 가 본문 'ok' 검증)
    return Response("ok", status=200, mimetype="text/plain")


# 레거시 JSON 메트릭
@app.get("/metrics_json")
@rate_limited
def get_metrics_json():
    return jsonify({
        "ok": True,
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "env": APP_ENV,
        "since": int(metrics["start_ts"]),
        "uptime_sec": int(time.time() - metrics["start_ts"]),
        "requests": metrics
    })

# Prometheus 표준 텍스트 포맷 — rate limit 제외
@app.get("/metrics")
def metrics_prometheus():
    data = generate_latest()
    return Response(data, mimetype=CONTENT_TYPE_LATEST)

# 커스텀 텍스트 포맷(레거시 시각화 용)
def _line_help(name, text): return f"# HELP {name} {text}\n"
def _line_type(name, typ):  return f"# TYPE {name} {typ}\n"
def _line_sample(name, value, labels=None):
    if labels:
        pairs = ",".join(f'{k}="{v}"' for k, v in labels.items())
        return f"{name}{{{pairs}}} {value}\n"
    return f"{name} {value}\n"

@app.get("/metrics_prom")
def metrics_prom():
    out = []
    out.append(_line_help("sportsstatsx_requests_total", "Total requests since start"))
    out.append(_line_type("sportsstatsx_requests_total", "counter"))
    out.append(_line_sample("sportsstatsx_requests_total", metrics["req_total"]))

    out.append(_line_help("sportsstatsx_responses_count", "Response counts by class"))
    out.append(_line_type("sportsstatsx_responses_count", "counter"))
    out.append(_line_sample("sportsstatsx_responses_count", metrics["resp_2xx"], {"class": "2xx"}))
    out.append(_line_sample("sportsstatsx_responses_count", metrics["resp_4xx"], {"class": "4xx"}))
    out.append(_line_sample("sportsstatsx_responses_count", metrics["resp_5xx"], {"class": "5xx"}))

    out.append(_line_help("sportsstatsx_rate_limited", "Total 429 responses"))
    out.append(_line_type("sportsstatsx_rate_limited", "counter"))
    out.append(_line_sample("sportsstatsx_rate_limited", metrics["rate_limited"]))

    out.append(_line_help("sportsstatsx_path_requests_total", "Requests per path"))
    out.append(_line_type("sportsstatsx_path_requests_total", "counter"))
    for p, c in sorted(metrics["path_counts"].items()):
        out.append(_line_sample("sportsstatsx_path_requests_total", c, {"path": p}))

    out.append(_line_help("sportsstatsx_uptime_seconds", "Uptime in seconds"))
    out.append(_line_type("sportsstatsx_uptime_seconds", "gauge"))
    out.append(_line_sample("sportsstatsx_uptime_seconds", int(time.time() - metrics["start_ts"])))

    body = "".join(out)
    return Response(body, mimetype="text/plain; charset=utf-8")

@app.get("/api/fixtures")
@rate_limited
def list_fixtures():
    league_id = request.args.get("league_id", type=int)
    date_str  = request.args.get("date")
    page      = max(1, request.args.get("page", default=1, type=int))
    page_size = max(1, min(100, request.args.get("page_size", default=50, type=int)))

    where, params = [], []
    if league_id: where.append("league_id = %s"); params.append(league_id)
    if date_str:  where.append("match_date = %s"); params.append(date_str)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    rows = fetch_all(f"""
        SELECT id, league_id, match_date, home_team, away_team, home_score, away_score, updated_at
        FROM fixtures {where_sql} ORDER BY id ASC LIMIT %s OFFSET %s
    """, tuple(params + [page_size, (page - 1) * page_size]))

    return jsonify({"ok": True, "rows": rows, "count": len(rows)})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))





