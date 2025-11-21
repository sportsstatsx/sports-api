import os
import json
import uuid
from datetime import datetime, timezone, timedelta
from functools import wraps
from typing import Dict

from flask import Flask, request, jsonify, Response
from werkzeug.exceptions import HTTPException
import pytz  # ← 타임존 계산용

from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

from db import fetch_all, fetch_one
from services.home_service import (
    get_home_leagues,
    get_home_league_directory,
    get_next_matchday,
    get_prev_matchday,
    get_team_season_stats,
    get_team_info,
)
from routers.home_router import home_bp


# ─────────────────────────────────────────
# 기본 설정
# ─────────────────────────────────────────
SERVICE_NAME = os.getenv("SERVICE_NAME", "sportsstatsx-api")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.0.0")

app = Flask(__name__)
app.register_blueprint(home_bp)


# ─────────────────────────────────────────
# 에러 핸들러
# ─────────────────────────────────────────
@app.errorhandler(Exception)
def handle_error(e):
    if isinstance(e, HTTPException):
        return jsonify({"ok": False, "error": e.description}), e.code
    return jsonify({"ok": False, "error": str(e)}), 500


# ─────────────────────────────────────────
# Prometheus 메트릭
# ─────────────────────────────────────────
REQUEST_COUNT = Counter(
    "api_request_total",
    "Total API Requests",
    ["service", "version", "endpoint", "method"],
)

REQUEST_LATENCY = Histogram(
    "api_request_latency_seconds",
    "API Request latency",
    ["service", "version", "endpoint"],
)

ACTIVE_REQUESTS = Gauge(
    "api_active_requests",
    "Active requests",
    ["service", "version"],
)


def track_metrics(endpoint_name):
    """API 호출 측정용 데코레이터"""

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            REQUEST_COUNT.labels(
                SERVICE_NAME, SERVICE_VERSION, endpoint_name, request.method
            ).inc()
            ACTIVE_REQUESTS.labels(SERVICE_NAME, SERVICE_VERSION).inc()

            with REQUEST_LATENCY.labels(
                SERVICE_NAME, SERVICE_VERSION, endpoint_name
            ).time():
                try:
                    return fn(*args, **kwargs)
                finally:
                    ACTIVE_REQUESTS.labels(SERVICE_NAME, SERVICE_VERSION).dec()

        return wrapper

    return decorator


# ─────────────────────────────────────────
# API: /health
# ─────────────────────────────────────────
@app.route("/health")
@track_metrics("/health")
def health():
    return jsonify({"ok": True, "service": SERVICE_NAME, "version": SERVICE_VERSION})


# ─────────────────────────────────────────
# API: Prometheus metrics
# ─────────────────────────────────────────
@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


# ─────────────────────────────────────────
# 핵심 API: /api/fixtures  (타임존 기반 날짜 처리)
# ─────────────────────────────────────────
@app.route("/api/fixtures")
@track_metrics("/api/fixtures")
def list_fixtures():
    """
    사용자가 있는 지역 날짜를 기반으로 경기 조회.
    - date: YYYY-MM-DD (사용자 지역 날짜)
    - timezone: 사용자 지역의 타임존 ex) Asia/Seoul, America/New_York
    """

    league_id = request.args.get("league_id", type=int)
    date_str = request.args.get("date", type=str)
    tz_str = request.args.get("timezone", "UTC")

    if not league_id or not date_str:
        return (
            jsonify({"ok": False, "error": "league_id and date are required"}),
            400,
        )

    # 1) 사용자 타임존 객체
    try:
        user_tz = pytz.timezone(tz_str)
    except Exception:
        return jsonify({"ok": False, "error": f"Invalid timezone: {tz_str}"}), 400

    # 2) 사용자 날짜 → datetime
    try:
        local_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid date format YYYY-MM-DD"}), 400

    # 3) 사용자 날짜의 시작/끝 (지역 기준)
    local_start = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 0, 0, 0))
    local_end = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 23, 59, 59))

    # 4) UTC 로 변환
    utc_start = local_start.astimezone(timezone.utc)
    utc_end = local_end.astimezone(timezone.utc)

    # 5) SQL 범위 필터 (date_utc 는 text → timestamptz 로 변환)
    sql = """
        SELECT
            m.fixture_id,
            m.league_id,
            m.season,
            m.date_utc,
            m.status_group,
            m.status,
            m.elapsed,
            m.home_id,
            m.away_id,
            m.home_ft,
            m.away_ft,
            th.name  AS home_name,
            ta.name  AS away_name,
            th.logo  AS home_logo,
            ta.logo  AS away_logo,
            (
                SELECT COUNT(*)
                FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id = m.home_id
                  AND e.type = 'Card'
                  AND e.detail = 'Red Card'
            ) AS home_red_cards,
            (
                SELECT COUNT(*)
                FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id = m.away_id
                  AND e.type = 'Card'
                  AND e.detail = 'Red Card'
            ) AS away_red_cards
        FROM matches m
        JOIN teams th ON th.id = m.home_id
        JOIN teams ta ON ta.id = m.away_id
        WHERE m.league_id = %s
          AND (m.date_utc::timestamptz BETWEEN %s AND %s)
        ORDER BY m.date_utc ASC
    """

    rows = fetch_all(sql, (league_id, utc_start, utc_end))

    fixtures = []
    for r in rows:
        fixtures.append(
            {
                "fixture_id": r["fixture_id"],
                "league_id": r["league_id"],
                "season": r["season"],
                "date_utc": r["date_utc"],
                "status_group": r["status_group"],
                "status": r["status"],
                "elapsed": r["elapsed"],
                "home": {
                    "id": r["home_id"],
                    "name": r["home_name"],
                    "logo": r["home_logo"],
                    "ft": r["home_ft"],
                    "red_cards": r["home_red_cards"],
                },
                "away": {
                    "id": r["away_id"],
                    "name": r["away_name"],
                    "logo": r["away_logo"],
                    "ft": r["away_ft"],
                    "red_cards": r["away_red_cards"],
                },
            }
        )

    return jsonify({"ok": True, "rows": fixtures})


# ─────────────────────────────────────────
# 실행
# ─────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
