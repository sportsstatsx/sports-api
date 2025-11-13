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
from werkzeug.exceptions import HTTPException

from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

from db import fetch_all, fetch_one, execute
from src.metrics_instrumentation import (
    instrument_app,
)

# ─────────────────────────────────────────
# 환경 변수 / 기본 설정
# ─────────────────────────────────────────

SERVICE_NAME = os.getenv("SERVICE_NAME", "sportsstatsx-api")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.0.0")
APP_ENV = os.getenv("APP_ENV", "prod")

LOG_SAMPLE_RATE = float(os.getenv("LOG_SAMPLE_RATE", "1.0"))  # 0.0 ~ 1.0
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "120"))

# ─────────────────────────────────────────
# Flask 앱 생성
# ─────────────────────────────────────────

app = Flask(__name__)

# ─────────────────────────────────────────
# Prometheus 지표 정의 (표준 /metrics 용)
# ─────────────────────────────────────────

HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "path"],
)

HTTP_EXCEPTIONS_TOTAL = Counter(
    "http_exceptions_total",
    "Total exceptions",
    ["type", "path"],
)

RATE_LIMITED_TOTAL = Counter(
    "http_rate_limited_total",
    "Total rate limited responses (429)",
)

UPTIME_SECONDS = Gauge(
    "process_uptime_seconds",
    "Process uptime in seconds",
)

START_TS = time.time()

# ─────────────────────────────────────────
# 간단한 레이트 리미터 (IP 기준 분당 요청 수)
# ─────────────────────────────────────────

_ip_buckets: Dict[str, Dict[str, int]] = defaultdict(lambda: {"ts": 0, "cnt": 0})


def _client_ip() -> str:
    return request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or request.remote_addr or "unknown"


def rate_limited(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        ip = _client_ip()
        now = int(time.time())
        bucket = _ip_buckets[ip]

        if now - bucket["ts"] >= 60:
            bucket["ts"] = now
            bucket["cnt"] = 0

        bucket["cnt"] += 1
        if bucket["cnt"] > RATE_LIMIT_PER_MINUTE:
            RATE_LIMITED_TOTAL.inc()
            return jsonify({"ok": False, "error": "rate_limited"}), 429

        return f(*args, **kwargs)

    return wrapper


# ─────────────────────────────────────────
# 요청/응답 로깅 (샘플링)
# ─────────────────────────────────────────

def log_json(level: str, msg: str, **kwargs):
    if LOG_SAMPLE_RATE <= 0:
        return
    if LOG_SAMPLE_RATE < 1.0:
        if uuid.uuid4().int % 1000 > int(LOG_SAMPLE_RATE * 1000):
            return

    base = {
        "ts": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
        "level": level,
        "msg": msg,
        "svc": SERVICE_NAME,
        "ver": SERVICE_VERSION,
        "env": APP_ENV,
    }
    base.update(kwargs)
    print(json.dumps(base, ensure_ascii=False))


@app.before_request
def _before():
    request.environ["__t0"] = time.perf_counter()
    rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.environ["x_request_id"] = rid

    log_json(
        "INFO",
        "request",
        rid=rid,
        method=request.method,
        path=request.path,
        query=request.query_string.decode("utf-8") if request.query_string else "",
        ip=_client_ip(),
        ua=request.headers.get("User-Agent", ""),
    )


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
        pass
    elif 400 <= sc < 500:
        if sc == 429:
            RATE_LIMITED_TOTAL.inc()
    else:
        pass

    HTTP_REQUESTS_TOTAL.labels(request.method, request.path, str(sc)).inc()

    log_json(
        "INFO",
        "response",
        rid=rid,
        method=request.method,
        path=request.path,
        status=sc,
        dur_ms=int(dur_s * 1000) if dur_s is not None else None,
    )
    return resp


@app.errorhandler(Exception)
def _handle_error(e: Exception):
    if isinstance(e, HTTPException):
        HTTP_EXCEPTIONS_TOTAL.labels(type(e).__name__, request.path).inc()
        return e

    HTTP_EXCEPTIONS_TOTAL.labels(type(e).__name__, request.path).inc()
    log_json("ERROR", "unhandled_exception", err=str(e), path=request.path)
    return jsonify({"ok": False, "error": "internal_error"}), 500


# ─────────────────────────────────────────
# Prometheus /metrics (표준 포맷)
# ─────────────────────────────────────────

@app.get("/metrics")
def metrics_endpoint():
    UPTIME_SECONDS.set(time.time() - START_TS)
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


# ─────────────────────────────────────────
# Health check
# ─────────────────────────────────────────

@app.get("/health")
def health():
    row = fetch_one("SELECT 1 AS ok")
    if not row or row["ok"] != 1:
        return jsonify({"ok": False}), 500
    return jsonify({"ok": True, "service": SERVICE_NAME, "version": SERVICE_VERSION})


# ─────────────────────────────────────────
# 예제 API: fixtures 조회 (Postgres matches/teams/leagues 기반)
# ─────────────────────────────────────────

# 레거시 커스텀 텍스트 /metrics_prom용 메트릭 집계용
metrics = {
    "start_ts": time.time(),
    "req_total": 0,
    "resp_2xx": 0,
    "resp_4xx": 0,
    "resp_5xx": 0,
    "rate_limited": 0,
    "path_counts": defaultdict(int),
}

# Flask용 계측(instrument_app)은 기존 코드 유지
instrument_app(app, service_name=SERVICE_NAME, service_version=SERVICE_VERSION)


@app.before_request
def _before_for_legacy_metrics():
    metrics["req_total"] += 1
    metrics["path_counts"][request.path] += 1


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
    """
    football.db 에서 옮겨온 matches / teams / leagues 테이블을 기준으로
    홈 화면 매치 리스트용 간단한 API.

    쿼리 파라미터:
      - league_id: 리그 ID (예: 39)
      - date: 'YYYY-MM-DD' 형식 날짜 (옵션)
      - page, page_size: 페이징
    """
    league_id = request.args.get("league_id", type=int)
    date_str = request.args.get("date")  # 'YYYY-MM-DD'
    page = max(1, request.args.get("page", default=1, type=int))
    page_size = max(1, min(100, request.args.get("page_size", default=50, type=int)))

    where_parts = []
    params = []

    if league_id is not None:
        where_parts.append("m.league_id = %s")
        params.append(league_id)

    # date_utc TEXT 에서 앞 10글자(YYYY-MM-DD)만 잘라서 비교
    if date_str:
        where_parts.append("SUBSTRING(m.date_utc FROM 1 FOR 10) = %s")
        params.append(date_str)

    where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    sql = f"""
        SELECT
            m.fixture_id,
            m.league_id,
            l.name AS league_name,
            m.season,
            m.date_utc,
            SUBSTRING(m.date_utc FROM 1 FOR 10) AS match_date,
            SUBSTRING(m.date_utc FROM 12 FOR 8) AS match_time_utc,
            m.status,
            m.status_group,
            m.home_id,
            th.name AS home_name,
            m.away_id,
            ta.name AS away_name,
            m.home_ft,
            m.away_ft
        FROM matches m
        JOIN leagues l ON l.id = m.league_id
        JOIN teams th ON th.id = m.home_id
        JOIN teams ta ON ta.id = m.away_id
        {where_sql}
        ORDER BY m.date_utc ASC
        LIMIT %s OFFSET %s
    """

    params.extend([page_size, (page - 1) * page_size])

    rows = fetch_all(sql, tuple(params))
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
