import os
import json
import uuid
from datetime import datetime, timezone, timedelta
from functools import wraps
from typing import Dict, List, Any

from flask import Flask, request, jsonify, Response, send_from_directory
from werkzeug.exceptions import HTTPException
import pytz  # 타임존 계산용

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
from routers.matchdetail_router import matchdetail_bp
from teamdetail.routes import teamdetail_bp
from leaguedetail.routes import leaguedetail_bp
from notifications.routes import notifications_bp
from routers.vip_routes import vip_bp



import traceback
import sys


# ─────────────────────────────────────────
# 기본 설정
# ─────────────────────────────────────────
SERVICE_NAME = os.getenv("SERVICE_NAME", "sportsstatsx-api")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.0.0")

app = Flask(__name__)
app.register_blueprint(home_bp)
app.register_blueprint(matchdetail_bp)
app.register_blueprint(teamdetail_bp)
app.register_blueprint(leaguedetail_bp)
app.register_blueprint(notifications_bp)
app.register_blueprint(vip_bp)


# ─────────────────────────────────────────
# 통합 에러 핸들러 (Traceback 로그 + JSON 응답)
# ─────────────────────────────────────────
@app.errorhandler(Exception)
def handle_exception(e):

    # 콘솔에 Traceback 출력
    print("\n=== SERVER EXCEPTION ===", file=sys.stderr)
    traceback.print_exc()
    print("=== END EXCEPTION ===\n", file=sys.stderr)

    # werkzeug HTTP 에러면 기존 status 유지
    if isinstance(e, HTTPException):
        return jsonify({
            "ok": False,
            "error": e.description
        }), e.code

    # 일반 파이썬 예외는 500 처리
    return jsonify({
        "ok": False,
        "error": str(e)
    }), 500


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

            ACTIVE_REQUESTS.labels(
                SERVICE_NAME, SERVICE_VERSION
            ).inc()

            with REQUEST_LATENCY.labels(
                SERVICE_NAME, SERVICE_VERSION, endpoint_name
            ).time():
                try:
                    return fn(*args, **kwargs)
                finally:
                    ACTIVE_REQUESTS.labels(
                        SERVICE_NAME, SERVICE_VERSION
                    ).dec()

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
# Policy: Privacy Policy / Terms (EN main + KO split)
# ─────────────────────────────────────────
STATIC_DIR = os.path.join(app.root_path, "static")

@app.route("/privacy")
def privacy_en():
    # EN main
    return send_from_directory(STATIC_DIR, "privacy.html")

@app.route("/privacy/ko")
def privacy_ko():
    # KO
    return send_from_directory(STATIC_DIR, "privacy_ko.html")

@app.route("/terms")
def terms_en():
    # EN main
    return send_from_directory(STATIC_DIR, "terms.html")

@app.route("/terms/ko")
def terms_ko():
    # KO
    return send_from_directory(STATIC_DIR, "terms_ko.html")



# ─────────────────────────────────────────
# API: /api/fixtures  (타임존 + 다중 리그 필터)
# ─────────────────────────────────────────
@app.route("/api/fixtures")
@track_metrics("/api/fixtures")
def list_fixtures():
    """
    사용자의 지역 날짜를 기반으로 경기 조회.
    """

    # 🔹 리그 필터
    league_id = request.args.get("league_id", type=int)
    league_ids_raw = request.args.get("league_ids", type=str)

    league_ids: List[int] = []
    if league_ids_raw:
        for part in league_ids_raw.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                league_ids.append(int(part))
            except ValueError:
                continue

    # 🔹 날짜 / 타임존
    date_str = request.args.get("date", type=str)
    tz_str = request.args.get("timezone", "UTC")

    if not date_str:
        return jsonify({"ok": False, "error": "date is required (YYYY-MM-DD)"}), 400

    try:
        user_tz = pytz.timezone(tz_str)
    except Exception:
        return jsonify({"ok": False, "error": f"Invalid timezone: {tz_str}"}), 400

    try:
        local_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid date format YYYY-MM-DD"}), 400

    # 날짜 생성
    local_start = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 0, 0, 0))
    local_end   = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 23, 59, 59))

    utc_start = local_start.astimezone(timezone.utc)
    utc_end   = local_end.astimezone(timezone.utc)

    # SQL
    params: List[Any] = [utc_start, utc_end]
    where_clauses = ["(m.date_utc::timestamptz BETWEEN %s AND %s)"]

    if league_ids:
        placeholders = ", ".join(["%s"] * len(league_ids))
        where_clauses.append(f"m.league_id IN ({placeholders})")
        params.extend(league_ids)
    elif league_id is not None and league_id > 0:
        where_clauses.append("m.league_id = %s")
        params.append(league_id)

    where_sql = " AND ".join(where_clauses)

    sql = f"""
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
            th.name AS home_name,
            ta.name AS away_name,
            th.logo AS home_logo,
            ta.logo AS away_logo,
            l.name AS league_name,
            l.logo AS league_logo,
            l.country AS league_country,
            (
                SELECT COUNT(*) FROM match_events e 
                WHERE e.fixture_id = m.fixture_id
                AND e.team_id = m.home_id
                AND e.type = 'Card'
                AND e.detail = 'Red Card'
            ) AS home_red_cards,
            (
                SELECT COUNT(*) FROM match_events e 
                WHERE e.fixture_id = m.fixture_id
                AND e.team_id = m.away_id
                AND e.type = 'Card'
                AND e.detail = 'Red Card'
            ) AS away_red_cards
        FROM matches m
        JOIN teams th ON th.id = m.home_id
        JOIN teams ta ON ta.id = m.away_id
        JOIN leagues l ON l.id = m.league_id
        WHERE {where_sql}
        ORDER BY m.date_utc ASC
    """

    rows = fetch_all(sql, tuple(params))

    fixtures = []
    for r in rows:
        fixtures.append({
            "fixture_id": r["fixture_id"],
            "league_id": r["league_id"],
            "season": r["season"],
            "date_utc": r["date_utc"],
            "status_group": r["status_group"],
            "status": r["status"],
            "elapsed": r["elapsed"],
            "league_name": r["league_name"],
            "league_logo": r["league_logo"],
            "league_country": r["league_country"],
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
        })

    return jsonify({"ok": True, "rows": fixtures})


# ─────────────────────────────────────────
# 실행
# ─────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)









