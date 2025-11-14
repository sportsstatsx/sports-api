# main.py  v1.6.2 — SportsStatsX API
# - Prometheus 표준 /metrics
# - 커스텀 텍스트 /metrics_prom (레거시용)
# - 요청 카운트/지연/429/예외 카운트 계측

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
...
    generate_latest,
    CONTENT_TYPE_LATEST,
)

from db import fetch_all, fetch_one, execute

# ─────────────────────────────────────────
# 환경 변수 / 기본 설정
# ─────────────────────────────────────────

SERVICE_NAME = os.getenv("SERVICE_NAME", "sportsstatsx-api")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.0.0")
APP_ENV = os.getenv("APP_ENV", "prod")

# 허용 리그 목록 (Render 환경변수에서 읽기)
# 예: LIVE_LEAGUES="39,40,140,141"
_RAW_LIVE_LEAGUES = (
    os.getenv("live-league")
    or os.getenv("LIVE_LEAGUES")
    or os.getenv("LIVE_LEAGUE")
    or ""
)

def _parse_allowed_league_ids(raw: str):
    ids = []
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            # 숫자로 변환 안 되는 값은 무시
            continue
    # 중복 제거 + 정렬
    return sorted(set(ids))

ALLOWED_LEAGUE_IDS = _parse_allowed_league_ids(_RAW_LIVE_LEAGUES)

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
    "HTTP request duration in seconds",
    ["method", "path"],
)

HTTP_REQUEST_EXCEPTIONS_TOTAL = Counter(
    "http_request_exceptions_total",
    "Total HTTP exceptions",
    ["type"],
)

HTTP_429_TOTAL = Counter(
    "http_429_total",
    "Total HTTP 429 responses (rate limited)",
    ["ip", "path"],
)

UPTIME_SECONDS = Gauge(
    "process_uptime_seconds",
    "Process uptime in seconds",
)

START_TS = time.time()

# ─────────────────────────────────────────
# 간단한 레이트 리미터 (IP 기준 분당 요청 수)
# ─────────────────────────────────────────

_ip_buckets: Dict[str, Dict[str, int]] = defaultdict(
    lambda: {"ts": 0, "count": 0}
)

RATE_LIMIT_BURST = int(os.getenv("RATE_LIMIT_BURST", "60"))  # 분당 허용 버스트
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "120"))


def _rate_limit_key() -> str:
    """IP 기준 rate-limit key"""
    ip = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
    if not ip:
        ip = request.remote_addr or "unknown"
    return ip


def check_rate_limit() -> bool:
    """분당 요청 수가 RATE_LIMIT_PER_MIN 이상이면 False"""
    ip = _rate_limit_key()
    now = int(time.time())
    bucket = _ip_buckets[ip]

    if now - bucket["ts"] >= 60:
        bucket["ts"] = now
        bucket["count"] = 0

    bucket["count"] += 1
    if bucket["count"] > RATE_LIMIT_PER_MIN:
        HTTP_429_TOTAL.labels(ip=ip, path=request.path).inc()
        return False
    return True


def rate_limited(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not check_rate_limit():
            return jsonify({"ok": False, "error": "rate_limited"}), 429
        return fn(*args, **kwargs)

    return wrapper


# ─────────────────────────────────────────
# 요청/응답 로깅 + metrics 계측용 미들웨어
# ─────────────────────────────────────────

def _client_ip() -> str:
    ip = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
    return ip or (request.remote_addr or "unknown")


@app.before_request
def _before():
    request.environ["__t0"] = time.perf_counter()
    # 샘플링 비율에 따라 일부 요청만 로깅
    if LOG_SAMPLE_RATE <= 0:
        return
    if LOG_SAMPLE_RATE < 1.0:
        if uuid.uuid4().int % 10_000 > int(LOG_SAMPLE_RATE * 10_000):
            return

    request_id = str(uuid.uuid4())
    request.environ["x_request_id"] = request_id

    app.logger.info(
        "[REQ] %s %s ip=%s ua=%s rid=%s",
        request.method,
        request.path,
        _client_ip(),
        request.headers.get("User-Agent", ""),
        request_id,
    )


@app.after_request
def _after(resp: Response):
    rid = request.environ.get("x_request_id")
    if rid:
        resp.headers["X-Request-ID"] = rid

    t0 = request.environ.get("__t0")
    if t0:
        dur = time.perf_counter() - t0
        HTTP_REQUEST_DURATION_SECONDS.labels(
            method=request.method,
            path=request.path,
        ).observe(dur)

    HTTP_REQUESTS_TOTAL.labels(
        method=request.method,
        path=request.path,
        status=resp.status_code,
    ).inc()

    return resp


@app.errorhandler(Exception)
def handle_exception(e):
    if isinstance(e, HTTPException):
        HTTP_REQUEST_EXCEPTIONS_TOTAL.labels(type=e.__class__.__name__).inc()
        return e

    HTTP_REQUEST_EXCEPTIONS_TOTAL.labels(type=e.__class__.__name__).inc()
    app.logger.exception("Unhandled exception: %s", e)
    return jsonify({"ok": False, "error": "internal_error"}), 500


# ─────────────────────────────────────────
# /metrics  (Prometheus 표준 포맷)
# ─────────────────────────────────────────

@app.get("/metrics")
def metrics():
    UPTIME_SECONDS.set(time.time() - START_TS)
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


# ─────────────────────────────────────────
# /metrics_prom  (텍스트 포맷, 레거시용)
# ─────────────────────────────────────────

@app.get("/metrics_prom")
def metrics_prom():
    # Prometheus 텍스트 포맷에 맞는 라인들을 구성
    # (이전 버전 호환용, 필요한 최소한만 유지)
    metrics = {
        "start_ts": START_TS,
        "uptime": time.time() - START_TS,
    }

    # HTTP 요청 카운트/429/예외/지연 등의 일부 지표를 단순 합산해 텍스트로 노출
    out = []

    def _line_help(name, desc):
        return f"# HELP {name} {desc}\n"

    def _line_type(name, typ):
        return f"# TYPE {name} {typ}\n"

    def _line_sample(name, value, labels=None):
        if labels:
            label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())
            return f'{name}{{{label_str}}} {value}\n'
        return f"{name} {value}\n"

    # http_requests_total
    out.append(_line_help("sportsstatsx_http_requests_total", "Total HTTP requests"))
    out.append(_line_type("sportsstatsx_http_requests_total", "counter"))
    for (m, p, s), c in HTTP_REQUESTS_TOTAL._metrics.items():
        out.append(
            _line_sample(
                "sportsstatsx_http_requests_total",
                c._value.get(),
                {"method": m, "path": p, "status": s},
            )
        )

    # http_429_total
    out.append(
        _line_help("sportsstatsx_http_429_total", "Total rate limited responses (429)")
    )
    out.append(_line_type("sportsstatsx_http_429_total", "counter"))
    for (ip, path), c in HTTP_429_TOTAL._metrics.items():
        out.append(
            _line_sample(
                "sportsstatsx_http_429_total",
                c._value.get(),
                {"ip": ip, "path": path},
            )
        )

    # uptime
    out.append(_line_help("sportsstatsx_uptime_seconds", "Uptime in seconds"))
    out.append(_line_type("sportsstatsx_uptime_seconds", "gauge"))
    out.append(
        _line_sample(
            "sportsstatsx_uptime_seconds", int(time.time() - metrics["start_ts"])
        )
    )

    body = "".join(out)

    return Response(body, mimetype="text/plain; version=0.0.4")


# ─────────────────────────────────────────
# 헬스 체크
# ─────────────────────────────────────────

@app.get("/health")
def health():
    try:
        row = fetch_one("SELECT 1 AS ok")
        if not row or row.get("ok") != 1:
            raise RuntimeError("DB check failed")
        return jsonify({"ok": True, "service": SERVICE_NAME, "version": SERVICE_VERSION})
    except Exception as e:
        app.logger.exception("Health check failed: %s", e)
        return jsonify({"ok": False, "error": "db_unavailable"}), 500


# ─────────────────────────────────────────
# 홈 화면: fixtures 리스트
# ─────────────────────────────────────────

@app.get("/api/fixtures")
@rate_limited
def list_fixtures():
    """
    Postgres의 matches / teams / leagues 테이블을 기반으로
    홈 화면 매치 리스트용 데이터를 반환.

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

    # 환경에서 허용된 리그만 필터링 (설정되어 있을 때만 적용)
    if ALLOWED_LEAGUE_IDS:
        placeholders = ",".join(["%s"] * len(ALLOWED_LEAGUE_IDS))
        where_parts.append(f"m.league_id IN ({placeholders})")
        params.extend(ALLOWED_LEAGUE_IDS)

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
    import time as _time  # perf_counter 충돌 피하기 위해 별칭
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
