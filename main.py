import os
import json
import uuid
from datetime import datetime, timezone
from functools import wraps
from typing import Dict

from flask import Flask, request, jsonify, Response
from werkzeug.exceptions import HTTPException

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
# 핵심 API: /api/fixtures  (A 방식)
# ─────────────────────────────────────────
@app.route("/api/fixtures")
@track_metrics("/api/fixtures")
def list_fixtures():
    """
    경기 리스트는 "오직 DB(matches)" 기준으로 제공한다.
    - status_group / status / elapsed / home_ft / away_ft
    - red cards
    - 팀명/로고
    - 날짜/시간 (timezone에서 변환)
    """

    league_id = request.args.get("league_id", type=int)
    date = request.args.get("date", type=str)
    tz = request.args.get("timezone", "UTC")

    if not league_id or not date:
        return (
            jsonify({"ok": False, "error": "league_id and date are required"}),
            400,
        )

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
            th.name      AS home_name,
            ta.name      AS away_name,
            th.logo      AS home_logo,
            ta.logo      AS away_logo,
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
        JOIN teams th ON th.team_id = m.home_id
        JOIN teams ta ON ta.team_id = m.away_id
        WHERE m.league_id = %s
          AND (timezone(%s, m.date_utc))::date = %s
        ORDER BY m.date_utc ASC
    """

    rows = fetch_all(sql, (league_id, tz, date))

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
