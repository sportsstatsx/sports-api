# main.py  — SportsStatsX API

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
from services.home_service import (
    get_home_leagues,
    get_home_league_directory,
    get_next_matchday,
    get_prev_matchday,
)

# ─────────────────────────────────────────
# 환경 변수 / 기본 설정
# ─────────────────────────────────────────

SERVICE_NAME = os.getenv("SERVICE_NAME", "sportsstatsx-api")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.0.0")
APP_ENV = os.getenv("APP_ENV", "prod")

LOG_SAMPLE_RATE = float(os.getenv("LOG_SAMPLE_RATE", "1.0"))
API_RATE_LIMIT_PER_MINUTE = int(os.getenv("API_RATE_LIMIT_PER_MINUTE", "120"))

START_TS = time.time()

app = Flask(__name__)

# ─────────────────────────────────────────
# Prometheus 메트릭
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

RATE_LIMITED_TOTAL = Counter(
    "http_rate_limited_total",
    "Total rate limited responses (429)",
)

UPTIME_SECONDS = Gauge(
    "process_uptime_seconds",
    "Process uptime in seconds",
)

# ─────────────────────────────────────────
# 레이트 리미터
# ─────────────────────────────────────────

_ip_buckets: Dict[str, Dict[str, int]] = defaultdict(
    lambda: {"ts": 0, "cnt": 0}
)


def _client_ip() -> str:
    return (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.remote_addr
        or "unknown"
    )


def check_rate_limit() -> bool:
    """분당 요청 수가 API_RATE_LIMIT_PER_MINUTE 이상이면 False."""
    ip = _client_ip()
    now = int(time.time())
    bucket = _ip_buckets[ip]
    if now - bucket["ts"] >= 60:
        bucket["ts"] = now
        bucket["cnt"] = 0
    bucket["cnt"] += 1
    return bucket["cnt"] <= API_RATE_LIMIT_PER_MINUTE


def rate_limited(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not check_rate_limit():
            RATE_LIMITED_TOTAL.inc()
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "rate_limited",
                        "message": "Too many requests. Please slow down.",
                    }
                ),
                429,
            )
        return fn(*args, **kwargs)

    return wrapper


# ─────────────────────────────────────────
# JSON 로깅 / 에러 처리
# ─────────────────────────────────────────

def log_json(level: str, msg: str, **kwargs):
    if LOG_SAMPLE_RATE <= 0:
        return
    if LOG_SAMPLE_RATE < 1.0:
        if uuid.uuid4().int % 10_000 > int(LOG_SAMPLE_RATE * 10_000):
            return

    payload = {
        "level": level,
        "ts": datetime.utcnow().isoformat() + "Z",
        "msg": msg,
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "path": request.path if request else "",
        "method": request.method if request else "",
        "ip": _client_ip() if request else "",
    }
    payload.update(kwargs)
    print(json.dumps(payload, ensure_ascii=False))


@app.before_request
def before_request():
    request._start_ts = time.perf_counter()


@app.after_request
def after_request(resp: Response):
    start_ts = getattr(request, "_start_ts", None)
    if start_ts is not None:
        dur = time.perf_counter() - start_ts
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
    log_json("error", "Unhandled exception", error=str(e))
    return jsonify({"ok": False, "error": "internal_error"}), 500


# ─────────────────────────────────────────
# 루트 (브라우저 테스트용)
# ─────────────────────────────────────────

@app.get("/")
def root():
    """
    브라우저에서 바로 확인할 수 있는 간단 상태 체크.
    실제 모니터링은 /health, /metrics 를 기준으로 하고,
    이 엔드포인트는 사람/브라우저 확인용이다.
    """
    return jsonify(
        {
            "ok": True,
            "service": SERVICE_NAME,
            "version": SERVICE_VERSION,
            "time_utc": datetime.utcnow().isoformat() + "Z",
        }
    )


# ─────────────────────────────────────────
# /metrics  (Prometheus 기본 메트릭)
# ─────────────────────────────────────────

@app.get("/metrics")
def metrics():
    UPTIME_SECONDS.set(time.time() - START_TS)
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


@app.get("/metrics_prom")
def metrics_prom():
    """
    Prometheus 기본 메트릭에서 자주 보는 것만 뽑아서
    조금 더 읽기 좋은 포맷으로 내려주는 엔드포인트.
    내부 구조가 바뀌어도 전체 500을 내지 않도록, 각 섹션을 try/except 로 감싼다.
    """
    lines = []

    # http_requests_total
    try:
        lines.append("# HELP http_requests_total Total HTTP requests")
        lines.append("# TYPE http_requests_total counter")
        metrics_map = getattr(HTTP_REQUESTS_TOTAL, "_metrics", None)
        if metrics_map:
            for labels, metric in metrics_map.items():
                method, path, status = labels
                value = metric._value.get()
                lines.append(
                    f'http_requests_total{{method="{method}",path="{path}",status="{status}"}} {value}'
                )
    except Exception as e:
        log_json("error", "metrics_prom http_requests_total error", error=str(e))

    # http_request_duration_seconds (Histogram)  ← 내부 필드 없을 수도 있으니 방어적으로
    try:
        lines.append(
            "# HELP http_request_duration_seconds HTTP request duration in seconds"
        )
        lines.append("# TYPE http_request_duration_seconds histogram")
        metrics_map = getattr(HTTP_REQUEST_DURATION_SECONDS, "_metrics", None)
        if metrics_map:
            for labels, metric in metrics_map.items():
                method, path = labels
                buckets = getattr(metric, "_buckets", {}) or {}

                sum_obj = getattr(metric, "_sum", None)
                if hasattr(sum_obj, "get"):
                    sum_v = sum_obj.get()
                else:
                    sum_v = 0

                count_obj = getattr(metric, "_count", None)
                if hasattr(count_obj, "get"):
                    count_v = count_obj.get()
                else:
                    # _count 가 없으면 버킷의 마지막 값을 count 로 사용 (대략적인 fallback)
                    try:
                        count_v = float(list(buckets.values())[-1]) if buckets else 0
                    except Exception:
                        count_v = 0

                for le, v in buckets.items():
                    lines.append(
                        f'http_request_duration_seconds_bucket{{method="{method}",path="{path}",le="{le}"}} {v}'
                    )
                lines.append(
                    f'http_request_duration_seconds_sum{{method="{method}",path="{path}"}} {sum_v}'
                )
                lines.append(
                    f'http_request_duration_seconds_count{{method="{method}",path="{path}"}} {count_v}'
                )
    except Exception as e:
        log_json(
            "error",
            "metrics_prom http_request_duration_seconds error",
            error=str(e),
        )

    # http_request_exceptions_total
    try:
        lines.append("# HELP http_request_exceptions_total Total HTTP exceptions")
        lines.append("# TYPE http_request_exceptions_total counter")
        metrics_map = getattr(HTTP_REQUEST_EXCEPTIONS_TOTAL, "_metrics", None)
        if metrics_map:
            for labels, metric in metrics_map.items():
                (etype,) = labels
                value = metric._value.get()
                lines.append(
                    f'http_request_exceptions_total{{type="{etype}"}} {value}'
                )
    except Exception as e:
        log_json(
            "error",
            "metrics_prom http_request_exceptions_total error",
            error=str(e),
        )

    # http_rate_limited_total (unlabelled counter)
    try:
        lines.append(
            "# HELP http_rate_limited_total Total rate limited responses (429)"
        )
        lines.append("# TYPE http_rate_limited_total counter")
        value_obj = getattr(RATE_LIMITED_TOTAL, "_value", None)
        if value_obj is not None:
            lines.append(f"http_rate_limited_total {value_obj.get()}")
    except Exception as e:
        log_json(
            "error",
            "metrics_prom http_rate_limited_total error",
            error=str(e),
        )

    # process_uptime_seconds
    lines.append("# HELP process_uptime_seconds Process uptime in seconds")
    lines.append("# TYPE process_uptime_seconds gauge")
    lines.append(f"process_uptime_seconds {time.time() - START_TS}")

    body = "\n".join(lines) + "\n"
    return Response(body, mimetype="text/plain; version=0.0.4")


# ─────────────────────────────────────────
# 헬스 체크 (/health)
# ─────────────────────────────────────────

@app.get("/health")
def health():
    try:
        row = fetch_one("SELECT 1 AS ok")
        if not row or row.get("ok") != 1:
            raise RuntimeError("DB check failed")
        return jsonify(
            {"ok": True, "service": SERVICE_NAME, "version": SERVICE_VERSION}
        )
    except Exception as e:
        log_json("error", "Health check failed", error=str(e))
        return jsonify({"ok": False, "error": "db_unavailable"}), 500


# ─────────────────────────────────────────
# 홈 화면: fixtures 리스트 (/api/fixtures)
# ─────────────────────────────────────────

@app.get("/api/fixtures")
@rate_limited
def list_fixtures():
    """
    홈 화면 기본 경기 리스트
    기존 SQLite 쿼리를 Postgres 로 변환
    """
    league_id = request.args.get("league_id", type=int)
    date_str = request.args.get("date")  # YYYY-MM-DD
    page = request.args.get("page", 1, type=int)
    page_size = request.args.get("page_size", 50, type=int)

    if not league_id or not date_str:
        return jsonify({"ok": False, "error": "missing_params"}), 400

    offset = (page - 1) * page_size

    rows = fetch_all(
        """
        SELECT
            m.fixture_id,
            m.league_id,
            m.season,
            m.date_utc,
            m.status,
            m.status_group,
            m.home_id,
            m.away_id,
            m.home_ft,
            m.away_ft
        FROM matches m
        WHERE m.league_id = %s
          AND SUBSTRING(m.date_utc FROM 1 FOR 10) = %s
        ORDER BY m.date_utc ASC
        LIMIT %s OFFSET %s
        """,
        (league_id, date_str, page_size, offset),
    )

    return jsonify({"ok": True, "rows": rows})


# ─────────────────────────────────────────
# 홈 화면: 리그 탭용 / 디렉터리 / 매치데이 API
# ─────────────────────────────────────────

@app.get("/api/home/leagues")
@rate_limited
def api_home_leagues():
    """
    홈 탭 상단 “리그별 매치수” 리스트
      /api/home/leagues?date=2025-11-15
    """
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "missing_date"}), 400

    rows = get_home_leagues(date_str)
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})


@app.get("/api/home/league_directory")
@rate_limited
def api_home_league_directory():
    """
    리그 선택 바텀시트용: 전체 지원 리그 + 오늘 경기 수.
      /api/home/league_directory?date=2025-11-15
    """
    date_str = request.args.get("date")
    # date 없으면 서비스 쪽에서 "오늘"로 처리해도 되고, None 으로 넘겨도 됨
    rows = get_home_league_directory(date_str)
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})


@app.get("/api/home/next_matchday")
@rate_limited
def api_home_next_matchday():
    """
    지정 날짜 이후(포함) 첫 번째 매치데이.
      /api/home/next_matchday?date=2025-11-15
      /api/home/next_matchday?date=2025-11-15&league_id=39
    """
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "missing_date"}), 400

    league_id = request.args.get("league_id", type=int)
    next_date = get_next_matchday(date_str, league_id)
    return jsonify({"ok": True, "date": next_date})


@app.get("/api/home/prev_matchday")
@rate_limited
def api_home_prev_matchday():
    """
    지정 날짜 이전 마지막 매치데이.
      /api/home/prev_matchday?date=2025-11-15
      /api/home/prev_matchday?date=2025-11-15&league_id=39
    """
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "missing_date"}), 400

    league_id = request.args.get("league_id", type=int)
    prev_date = get_prev_matchday(date_str, league_id)
    return jsonify({"ok": True, "date": prev_date})


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
