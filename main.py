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

from db import get_connection, fetch_one, fetch_all
from metrics_instrumentation import (
    instrument_db_connection,
    instrument_flask_app,
)

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
    or os.getenv("LIVE_LEAGUES_HOME")
    or ""
)

ALLOWED_LEAGUES = []
for part in _RAW_LIVE_LEAGUES.replace(" ", "").split(","):
    if not part:
        continue
    try:
        ALLOWED_LEAGUES.append(int(part))
    except ValueError:
        continue

# 레이트 리밋 설정 (분당 요청수)
PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "120"))

# ─────────────────────────────────────────
# Flask 앱 / Prometheus 메트릭
# ─────────────────────────────────────────

app = Flask(__name__)

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

START_TS = time.time()

# ─────────────────────────────────────────
# 레이트 리미터
# ─────────────────────────────────────────

_ip_buckets: Dict[str, Dict[str, int]] = defaultdict(
    lambda: {"ts": 0, "count": 0}
)


def _client_ip() -> str:
    return request.headers.get("X-Real-IP") or request.remote_addr or "-"


def rate_limited(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        ip = _client_ip()
        now_ts = int(time.time())
        bucket = _ip_buckets[ip]
        if now_ts - bucket["ts"] >= 60:
            # 새 1분 버킷
            bucket["ts"] = now_ts
            bucket["count"] = 0

        if bucket["count"] >= PER_MINUTE:
            RATE_LIMITED_TOTAL.inc()
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "rate_limited",
                        "message": "Too many requests, please slow down.",
                    }
                ),
                429,
            )

        bucket["count"] += 1
        return fn(*args, **kwargs)

    return wrapper


# ─────────────────────────────────────────
# 로깅 / 에러 핸들링
# ─────────────────────────────────────────

def log_json(level: str, message: str, **kwargs):
    payload = {
        "t": "log",
        "ts": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "msg": message,
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "ip": _client_ip(),
        "path": request.path if request else "",
        "method": request.method if request else "",
    }
    payload.update(kwargs)
    print(json.dumps(payload, ensure_ascii=False))


@app.before_request
def _before():
    request.environ["__t0"] = time.perf_counter()


@app.after_request
def _after(resp: Response):
    t0 = request.environ.get("__t0")
    if t0 is not None:
        dt_s = time.perf_counter() - t0
        path = request.path
        method = request.method
        status = resp.status_code

        # 메트릭 기록
        HTTP_REQUESTS_TOTAL.labels(method=method, path=path, status=str(status)).inc()
        HTTP_REQUEST_DURATION_SECONDS.labels(method=method, path=path).observe(dt_s)

    return resp


@app.errorhandler(Exception)
def _handle_error(e: Exception):
    if isinstance(e, HTTPException):
        code = e.code or 500
        HTTP_REQUEST_EXCEPTIONS_TOTAL.labels(
            type=e.__class__.__name__
        ).inc()
        log_json("error", "HTTPException", error=str(e), status=code)
        return (
            jsonify({"ok": False, "error": e.name, "message": str(e.description)}),
            code,
        )

    HTTP_REQUEST_EXCEPTIONS_TOTAL.labels(type=e.__class__.__name__).inc()
    log_json("error", "Unhandled exception", error=str(e))
    return jsonify({"ok": False, "error": "internal_error"}), 500


# ─────────────────────────────────────────
# 루트(테스트용) 엔드포인트
# ─────────────────────────────────────────

@app.get("/")
def root():
    """
    브라우저에서 바로 확인할 수 있는 간단한 헬스 엔드포인트.
    실제 모니터링에는 /health, /metrics 를 사용하고,
    이 엔드포인트는 사람 눈으로 확인용이다.
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
# /metrics  (Prometheus)
# ─────────────────────────────────────────

@app.get("/metrics")
def metrics():
    UPTIME_SECONDS.set(time.time() - START_TS)
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


@app.get("/metrics_prom")
def metrics_prom():
    """
    내부 Prometheus 메트릭에서 자주 쓰는 것만 뽑아서
    조금 더 읽기 좋은 포맷으로 내려주는 엔드포인트.
    에러가 나더라도 전체 500을 내지 않고, 가능한 값만 반환한다.
    """
    lines = []

    # --------------------------------------------------
    # http_requests_total
    # --------------------------------------------------
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

    # --------------------------------------------------
    # http_request_duration_seconds
    # --------------------------------------------------
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
                sum_v = getattr(metric, "_sum").get()
                count_v = getattr(metric, "_count").get()

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

    # --------------------------------------------------
    # http_request_exceptions_total
    # --------------------------------------------------
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

    # --------------------------------------------------
    # http_rate_limited_total  (unlabelled counter)
    # --------------------------------------------------
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

    # --------------------------------------------------
    # process_uptime_seconds
    # --------------------------------------------------
    lines.append("# HELP process_uptime_seconds Process uptime in seconds")
    lines.append("# TYPE process_uptime_seconds gauge")
    lines.append(f"process_uptime_seconds {time.time() - START_TS}")

    body = "\n".join(lines) + "\n"
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
        return jsonify(
            {"ok": True, "service": SERVICE_NAME, "version": SERVICE_VERSION}
        )
    except Exception as e:
        log_json("error", "Health check failed", error=str(e))
        return jsonify({"ok": False, "error": "db_unavailable"}), 500


# ─────────────────────────────────────────
# 홈 화면: fixtures 리스트 등
# (여기부터는 기존에 우리가 작업한 홈 매치리스트, home_service 연동 코드가
# 그대로 들어있다고 가정)
# ─────────────────────────────────────────

# ... (여기 아래에는 네가 이미 쓰고 있는
# /api/fixtures, /api/home/leagues, /api/home/league_directory,
# /api/home/next_matchday, /api/home/prev_matchday 등
# 기존 endpoint 들이 그대로 이어진다.)
# 실제 레포에서는 그 부분까지 포함해서 main.py 전체를 사용하면 된다.

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
