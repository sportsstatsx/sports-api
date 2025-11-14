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

_ip_buckets: Dict[str, Dict[str, int]] = defaultdict(lambda: {"ts": 0, "cnt": 0})


def _client_ip() -> str:
    return (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.remote_addr
        or "unknown"
    )


def check_rate_limit() -> bool:
    """분당 요청 수가 RATE_LIMIT_PER_MINUTE 이상이면 False"""
    ip = _client_ip()
    now = int(time.time())
    bucket = _ip_buckets[ip]

    if now - bucket["ts"] >= 60:
        bucket["ts"] = now
        bucket["cnt"] = 0

    bucket["cnt"] += 1
    if bucket["cnt"] > RATE_LIMIT_PER_MINUTE:
        RATE_LIMITED_TOTAL.inc()
        return False
    return True


def rate_limited(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not check_rate_limit():
            return jsonify({"ok": False, "error": "rate_limited"}), 429
            # 429: Too Many Requests
        return f(*args, **kwargs)

    return wrapper


# ─────────────────────────────────────────
# 요청 로깅 / 에러 처리
# ─────────────────────────────────────────

def log_json(level: str, msg: str, **kwargs):
    if LOG_SAMPLE_RATE <= 0:
        return
    if LOG_SAMPLE_RATE < 1.0:
        if uuid.uuid4().int % 10_000 > int(LOG_SAMPLE_RATE * 10_000):
            return

    payload = {
        "level": level,
        "ts": datetime.utcnow().isoformat(),
        "msg": msg,
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "ip": _client_ip(),
        "path": request.path,
        "method": request.method,
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
    log_json("error", "Unhandled exception", error=str(e))
    return jsonify({"ok": False, "error": "internal_error"}), 500


# ─────────────────────────────────────────
# /metrics  (Prometheus)
# ─────────────────────────────────────────

@app.get("/metrics")
def metrics():
    UPTIME_SECONDS.set(time.time() - START_TS)
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


@app.get("/metrics_prom")
def metrics_prom():
    lines = []

    lines.append("# HELP http_requests_total Total HTTP requests")
    lines.append("# TYPE http_requests_total counter")
    for labels, metric in HTTP_REQUESTS_TOTAL._metrics.items():
        method, path, status = labels
        value = metric._value.get()
        lines.append(
            f'http_requests_total{{method="{method}",path="{path}",status="{status}"}} {value}'
        )

    lines.append("# HELP http_request_duration_seconds HTTP request duration in seconds")
    lines.append("# TYPE http_request_duration_seconds histogram")
    for labels, metric in HTTP_REQUEST_DURATION_SECONDS._metrics.items():
        method, path = labels
        buckets = metric._buckets
        sum_v = metric._sum.get()
        count_v = metric._count.get()
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

    lines.append("# HELP http_exceptions_total Total HTTP exceptions")
    lines.append("# TYPE http_exceptions_total counter")
    for labels, metric in HTTP_REQUEST_EXCEPTIONS_TOTAL._metrics.items():
        (etype,) = labels
        value = metric._value.get()
        lines.append(f'http_exceptions_total{{type="{etype}"}} {value}')

    lines.append("# HELP http_rate_limited_total Total rate limited responses (429)")
    lines.append("# TYPE http_rate_limited_total counter")
    for _, metric in RATE_LIMITED_TOTAL._metrics.items():
        value = metric._value.get()
        lines.append(f"http_rate_limited_total {value}")

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
        return jsonify({"ok": True, "service": SERVICE_NAME, "version": SERVICE_VERSION})
    except Exception as e:
        log_json("error", "Health check failed", error=str(e))
        return jsonify({"ok": False, "error": "db_unavailable"}), 500


# ─────────────────────────────────────────
# 홈 화면: fixtures 리스트 (이미 적용된 버전)
# ─────────────────────────────────────────

@app.get("/api/fixtures")
@rate_limited
def list_fixtures():
    """
    홈 화면 매치 리스트용 데이터.

    query:
      - league_id: >0 이면 해당 리그만, 0/없음이면 전체 허용 리그
      - date: yyyy-MM-dd
      - page, page_size
    """
    league_id = request.args.get("league_id", type=int)
    date_str = request.args.get("date")  # 'YYYY-MM-DD'
    page = max(1, request.args.get("page", default=1, type=int))
    page_size = max(1, min(100, request.args.get("page_size", default=50, type=int)))

    where_parts = []
    params = []

    # league_id > 0 일 때만 리그 필터 적용
    if league_id is not None and league_id > 0:
        where_parts.append("m.league_id = %s")
        params.append(league_id)

    if date_str:
        where_parts.append("SUBSTRING(m.date_utc FROM 1 FOR 10) = %s")
        params.append(date_str)

    # 허용된 리그만
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


# ─────────────────────────────────────────
# 홈: 상단 리그 탭용 API
# ─────────────────────────────────────────

@app.get("/api/home/leagues")
@rate_limited
def home_leagues():
    """
    상단 탭용: 오늘(또는 지정된 날짜)에 경기 있는 리그 목록.

    query:
      - date: yyyy-MM-dd (없으면 오늘 UTC 기준)
    """
    date_str = request.args.get("date")
    if not date_str:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")

    where_parts = ["SUBSTRING(m.date_utc FROM 1 FOR 10) = %s"]
    params = [date_str]

    if ALLOWED_LEAGUE_IDS:
        placeholders = ",".join(["%s"] * len(ALLOWED_LEAGUE_IDS))
        where_parts.append(f"m.league_id IN ({placeholders})")
        params.extend(ALLOWED_LEAGUE_IDS)

    where_sql = "WHERE " + " AND ".join(where_parts)

    sql = f"""
        SELECT
            m.league_id,
            l.name AS league_name,
            COUNT(*) AS match_count
        FROM matches m
        JOIN leagues l ON l.id = m.league_id
        {where_sql}
        GROUP BY m.league_id, l.name
        ORDER BY l.name ASC
    """

    rows = fetch_all(sql, tuple(params))
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})


# ─────────────────────────────────────────
# 홈: 리그 디렉터리(바텀시트) API
# ─────────────────────────────────────────

@app.get("/api/home/league_directory")
@rate_limited
def home_league_directory():
    """
    리그 선택 바텀시트용: 전체 지원 리그 + 오늘 경기 수.

    query:
      - date: yyyy-MM-dd (없으면 오늘)
    """
    date_str = request.args.get("date")
    if not date_str:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")

    where_parts = []
    params = []

    # 허용된 리그만 대상으로
    if ALLOWED_LEAGUE_IDS:
        placeholders = ",".join(["%s"] * len(ALLOWED_LEAGUE_IDS))
        where_parts.append(f"l.id IN ({placeholders})")
        params.extend(ALLOWED_LEAGUE_IDS)

    where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    sql = f"""
        SELECT
            l.id AS league_id,
            l.name AS league_name,
            l.country AS country,
            COALESCE(
                SUM(
                    CASE
                        WHEN SUBSTRING(m.date_utc FROM 1 FOR 10) = %s THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS today_count
        FROM leagues l
        LEFT JOIN matches m ON m.league_id = l.id
        {where_sql}
        GROUP BY l.id, l.name, l.country
        ORDER BY l.name ASC
    """

    # today_count 쪽에 날짜 파라미터 하나 더 필요
    params_with_date = list(params)
    params_with_date.insert(0, date_str)

    rows = fetch_all(sql, tuple(params_with_date))
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})


# ─────────────────────────────────────────
# 홈: 다음 / 이전 매치데이 API
# ─────────────────────────────────────────

@app.get("/api/home/next_matchday")
@rate_limited
def next_matchday():
    """
    지정 날짜 이후(포함) 첫 번째 매치데이.

    query:
      - date: yyyy-MM-dd (필수)
      - league_id: >0 이면 그 리그만, 0/없음이면 전체
    """
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "date_required"}), 400

    league_id = request.args.get("league_id", type=int)

    where_parts = ["SUBSTRING(m.date_utc FROM 1 FOR 10) >= %s"]
    params = [date_str]

    if league_id is not None and league_id > 0:
        where_parts.append("m.league_id = %s")
        params.append(league_id)

    if ALLOWED_LEAGUE_IDS:
        placeholders = ",".join(["%s"] * len(ALLOWED_LEAGUE_IDS))
        where_parts.append(f"m.league_id IN ({placeholders})")
        params.extend(ALLOWED_LEAGUE_IDS)

    where_sql = "WHERE " + " AND ".join(where_parts)

    sql = f"""
        SELECT MIN(SUBSTRING(m.date_utc FROM 1 FOR 10)) AS next_date
        FROM matches m
        {where_sql}
    """

    row = fetch_one(sql, tuple(params))
    return jsonify({"ok": True, "date": row.get("next_date") if row else None})


@app.get("/api/home/prev_matchday")
@rate_limited
def prev_matchday():
    """
    지정 날짜 이전 마지막 매치데이.

    query:
      - date: yyyy-MM-dd (필수)
      - league_id: >0 이면 그 리그만, 0/없음이면 전체
    """
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "date_required"}), 400

    league_id = request.args.get("league_id", type=int)

    where_parts = ["SUBSTRING(m.date_utc FROM 1 FOR 10) < %s"]
    params = [date_str]

    if league_id is not None and league_id > 0:
        where_parts.append("m.league_id = %s")
        params.append(league_id)

    if ALLOWED_LEAGUE_IDS:
        placeholders = ",".join(["%s"] * len(ALLOWED_LEAGUE_IDS))
        where_parts.append(f"m.league_id IN ({placeholders})")
        params.extend(ALLOWED_LEAGUE_IDS)

    where_sql = "WHERE " + " AND ".join(where_parts)

    sql = f"""
        SELECT MAX(SUBSTRING(m.date_utc FROM 1 FOR 10)) AS prev_date
        FROM matches m
        {where_sql}
    """

    row = fetch_one(sql, tuple(params))
    return jsonify({"ok": True, "date": row.get("prev_date") if row else None})


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────

if __name__ == "__main__":
    import time as _time  # perf_counter 충돌 방지용 별칭
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
