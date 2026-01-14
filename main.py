import os
import json
import uuid
from datetime import datetime, timezone, timedelta
from functools import wraps
from typing import Dict, List, Any, Optional, Tuple

from flask import Flask, request, jsonify, Response, send_from_directory, redirect
from werkzeug.exceptions import HTTPException
import pytz  # 타임존 계산용

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
    get_team_season_stats,
    get_team_info,
)
from routers.home_router import home_bp
from routers.matchdetail_router import matchdetail_bp
from teamdetail.routes import teamdetail_bp
from leaguedetail.routes import leaguedetail_bp
from notifications.routes import notifications_bp
from routers.vip_routes import vip_bp

from hockey.routers.hockey_games_router import hockey_games_bp
from hockey.routers.hockey_fixtures_router import hockey_fixtures_bp
from hockey.routers.hockey_matchdetail_router import hockey_matchdetail_bp
from hockey.routers.hockey_standings_router import hockey_standings_bp
from hockey.routers.hockey_insights_router import hockey_insights_bp
from hockey.routers.hockey_notifications_router import hockey_notifications_bp
from hockey.teamdetail.hockey_team_detail_routes import hockey_teamdetail_bp
from hockey.leaguedetail.hockey_leaguedetail_routes import hockey_leaguedetail_bp



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

app.register_blueprint(hockey_games_bp)
app.register_blueprint(hockey_fixtures_bp)
app.register_blueprint(hockey_matchdetail_bp)
app.register_blueprint(hockey_leaguedetail_bp)
app.register_blueprint(hockey_standings_bp)
app.register_blueprint(hockey_insights_bp)
app.register_blueprint(hockey_notifications_bp)
app.register_blueprint(hockey_teamdetail_bp)


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

def _deep_merge(base: Any, patch: Any) -> Any:
    """
    dict는 재귀 병합, list/primitive는 patch가 base를 대체.
    """
    if isinstance(base, dict) and isinstance(patch, dict):
        out = dict(base)
        for k, v in patch.items():
            if k in out:
                out[k] = _deep_merge(out[k], v)
            else:
                out[k] = v
        return out
    return patch


def _load_match_overrides(fixture_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not fixture_ids:
        return {}

    placeholders = ", ".join(["%s"] * len(fixture_ids))
    sql = f"""
        SELECT fixture_id, patch
        FROM match_overrides
        WHERE fixture_id IN ({placeholders})
    """
    rows = fetch_all(sql, tuple(fixture_ids))
    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        out[int(r["fixture_id"])] = r["patch"] or {}
    return out


# ─────────────────────────────────────────
# Prometheus 메트릭
# ─────────────────────────────────────────
import time
from flask import g

REQUEST_COUNT = Counter(
    "api_request_total",
    "Total API Requests",
    ["service", "version", "endpoint", "method", "status_code", "class"],
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


def _code_class(status_code: int) -> str:
    try:
        return f"{int(status_code) // 100}xx"
    except Exception:
        return "unknown"


def _should_skip_metrics(path: str) -> bool:
    # Prometheus가 /metrics를 긁을 때 그 요청까지 카운트하면 노이즈가 커져서 보통 제외
    return path in ("/metrics",)


@app.before_request
def _metrics_before_request():
    if _should_skip_metrics(request.path):
        return
    g._metrics_started = True
    g._metrics_start_time = time.time()
    ACTIVE_REQUESTS.labels(SERVICE_NAME, SERVICE_VERSION).inc()


@app.after_request
def _metrics_after_request(response):
    if _should_skip_metrics(request.path):
        return response

    started = getattr(g, "_metrics_started", False)
    if not started:
        return response

    endpoint = request.path
    method = request.method
    status_code = int(getattr(response, "status_code", 0) or 0)
    klass = _code_class(status_code)

    REQUEST_COUNT.labels(
        SERVICE_NAME,
        SERVICE_VERSION,
        endpoint,
        method,
        str(status_code),
        klass,
    ).inc()

    start_t = getattr(g, "_metrics_start_time", None)
    if start_t is not None:
        REQUEST_LATENCY.labels(SERVICE_NAME, SERVICE_VERSION, endpoint).observe(
            time.time() - start_t
        )

    ACTIVE_REQUESTS.labels(SERVICE_NAME, SERVICE_VERSION).dec()
    g._metrics_started = False
    return response


@app.teardown_request
def _metrics_teardown_request(exc):
    # 예외로 after_request가 안 타는 케이스 방어용 (대부분은 after_request가 실행됨)
    started = getattr(g, "_metrics_started", False)
    if started:
        try:
            ACTIVE_REQUESTS.labels(SERVICE_NAME, SERVICE_VERSION).dec()
        except Exception:
            pass
        g._metrics_started = False


# ─────────────────────────────────────────
# Admin (single-user) settings
# ─────────────────────────────────────────
ADMIN_PATH = (os.getenv("ADMIN_PATH", "") or "").strip().strip("/")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "") or ""


def _admin_enabled() -> bool:
    return bool(ADMIN_PATH) and bool(ADMIN_TOKEN)


def _client_ip() -> str:
    # Cloudflare / Proxy 고려
    cf_ip = request.headers.get("CF-Connecting-IP")
    if cf_ip:
        return cf_ip.strip()

    xff = request.headers.get("X-Forwarded-For")
    if xff:
        # 첫 번째가 원 IP인 경우가 대부분
        return xff.split(",")[0].strip()

    return (request.remote_addr or "").strip()


def _admin_log(
    event_type: str,
    ok: bool = True,
    status_code: int | None = None,
    fixture_id: int | None = None,
    detail: Dict[str, Any] | None = None,
) -> None:
    """
    admin_logs 테이블에 기록 (실패해도 서비스는 계속 동작해야 하므로 try/except)
    """
    try:
        payload = detail or {}
        execute(
            """
            INSERT INTO admin_logs (event_type, path, method, ip, user_agent, ok, status_code, fixture_id, detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                event_type,
                request.path,
                request.method,
                _client_ip(),
                (request.headers.get("User-Agent") or "")[:400],
                ok,
                status_code,
                fixture_id,
                json.dumps(payload, ensure_ascii=False),
            ),
        )
    except Exception:
        pass


def require_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        # 토큰/경로 미설정이면 관리자 기능 비활성(404)
        if not _admin_enabled():
            return jsonify({"ok": False, "error": "admin disabled"}), 404

        token = request.headers.get("X-Admin-Token", "") or ""
        if token != ADMIN_TOKEN:
            _admin_log(
                event_type="auth_fail",
                ok=False,
                status_code=401,
                detail={"note": "bad token"},
            )
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        return fn(*args, **kwargs)

    return wrapper


# ─────────────────────────────────────────
# Root: redirect to Google Play
# ─────────────────────────────────────────
PLAY_STORE_URL = os.getenv(
    "PLAY_STORE_URL",
    "https://play.google.com/store/apps/details?id=com.sportsstatsx.app",
)

@app.route("/")
def root_redirect():
    return redirect(PLAY_STORE_URL, code=302)  # 안정화되면 301로 바꿔도 됨



# ─────────────────────────────────────────
# API: /health
# ─────────────────────────────────────────
@app.route("/health")
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

@app.route("/app-ads.txt")
def app_ads_txt():
    # AdMob app-ads.txt verification
    return send_from_directory(STATIC_DIR, "app-ads.txt", mimetype="text/plain")


# ─────────────────────────────────────────
# Admin Page (single HTML)
# ─────────────────────────────────────────
@app.route(f"/{ADMIN_PATH}")
def admin_page():
    if not _admin_enabled():
        return jsonify({"ok": False, "error": "admin disabled"}), 404

    _admin_log("access", ok=True, status_code=200, detail={"note": "admin page loaded"})

    # ✅ HTML은 static/admin.html 파일로 분리
    # - 캐시 방지용으로 headers 추가(개발/운영 초기엔 편함)
    resp = send_from_directory(STATIC_DIR, "admin.html", mimetype="text/html")
    resp.headers["Cache-Control"] = "no-store"
    return resp




# ─────────────────────────────────────────
# Admin APIs
# ─────────────────────────────────────────
@app.route(f"/{ADMIN_PATH}/api/overrides/<int:fixture_id>", methods=["GET"])
@require_admin
def admin_get_override(fixture_id: int):
    row = fetch_one(
        "SELECT fixture_id, patch, updated_at FROM match_overrides WHERE fixture_id = %s",
        (fixture_id,),
    )
    _admin_log("override_get", ok=True, status_code=200, fixture_id=fixture_id)
    return jsonify({"ok": True, "row": row, "patch": (row["patch"] if row else None)})


@app.route(f"/{ADMIN_PATH}/api/overrides/<int:fixture_id>", methods=["PUT"])
@require_admin
def admin_upsert_override(fixture_id: int):
    patch = request.get_json(silent=True)
    if not isinstance(patch, dict):
        _admin_log("override_upsert", ok=False, status_code=400, fixture_id=fixture_id, detail={"error": "patch must be object"})
        return jsonify({"ok": False, "error": "patch must be a JSON object"}), 400

    execute(
        """
        INSERT INTO match_overrides (fixture_id, patch, updated_at)
        VALUES (%s, %s::jsonb, now())
        ON CONFLICT (fixture_id)
        DO UPDATE SET patch = EXCLUDED.patch, updated_at = now()
        """,
        (fixture_id, json.dumps(patch, ensure_ascii=False)),
    )

    _admin_log("override_upsert", ok=True, status_code=200, fixture_id=fixture_id, detail={"keys": list(patch.keys())[:50]})
    return jsonify({"ok": True, "fixture_id": fixture_id})


@app.route(f"/{ADMIN_PATH}/api/overrides/<int:fixture_id>", methods=["DELETE"])
@require_admin
def admin_delete_override(fixture_id: int):
    execute("DELETE FROM match_overrides WHERE fixture_id = %s", (fixture_id,))
    _admin_log("override_delete", ok=True, status_code=200, fixture_id=fixture_id)
    return jsonify({"ok": True, "fixture_id": fixture_id})


@app.route(f"/{ADMIN_PATH}/api/logs", methods=["GET"])
@require_admin
def admin_list_logs():
    limit = request.args.get("limit", type=int) or 200
    limit = max(1, min(limit, 500))

    event_type = request.args.get("event_type", type=str) or ""
    fixture_id = request.args.get("fixture_id", type=int)

    where = []
    params: List[Any] = []

    if event_type:
        where.append("event_type = %s")
        params.append(event_type)

    if fixture_id is not None:
        where.append("fixture_id = %s")
        params.append(fixture_id)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    rows = fetch_all(
        f"""
        SELECT ts, event_type, ok, status_code, fixture_id, detail
        FROM admin_logs
        {where_sql}
        ORDER BY ts DESC
        LIMIT %s
        """,
        tuple(params + [limit]),
    )

    _admin_log("logs_list", ok=True, status_code=200, detail={"limit": limit, "event_type": event_type, "fixture_id": fixture_id})
    return jsonify({"ok": True, "rows": rows})

# ─────────────────────────────────────────
# Admin API: fixtures (raw/merged)
# - merged 는 override 반영하지만 hidden=true도 "제외하지 않고" 포함
# - 리스트 UI에서 배지 표시를 위해 _has_override 필드 추가
# ─────────────────────────────────────────
@app.route(f"/{ADMIN_PATH}/api/fixtures_merged")
@require_admin
def admin_list_fixtures_merged():
    """
    관리자용 fixtures 조회:
    - /api/fixtures 와 동일한 필터(date/timezone/league_ids)
    - override 반영
    - hidden=true 도 제외하지 않고 포함(관리자가 다시 숨김해제 가능해야 함)
    - _has_override 플래그 추가

    ✅ 중요:
    - override patch에 timeline이 있으면, 그 timeline 기준으로
      red_cards / ft / ht 를 재계산해서 리스트에 동기화한다.
      (타임라인만 수정했는데 리스트/스코어가 안 바뀌는 문제 해결)
    """

    def _extract_timeline_list(patch_obj: Any) -> Optional[List[Any]]:
        if not isinstance(patch_obj, dict):
            return None
        tl = patch_obj.get("timeline")
        if isinstance(tl, list):
            return tl
        if isinstance(tl, dict):
            ev = tl.get("events")
            if isinstance(ev, list):
                return ev
        return None

    def _get_minute(e: Dict[str, Any]) -> Optional[int]:
        # admin.html / 서버 timeline 모두 방어적으로 지원
        for k in ("minute", "elapsed", "time", "min"):
            v = e.get(k)
            if isinstance(v, int):
                return v
            if isinstance(v, str):
                s = v.strip()
                if s.isdigit():
                    return int(s)
        # "45+2" 같은 문자열 방어
        v2 = e.get("minute")
        if isinstance(v2, str) and "+" in v2:
            base = v2.split("+", 1)[0].strip()
            if base.isdigit():
                return int(base)
        return None

    def _is_red_event(e: Dict[str, Any]) -> bool:
        t = e.get("type")
        d = e.get("detail")

        if isinstance(t, str):
            tu = t.strip().upper()
            if tu in ("RED", "RED_CARD", "REDCARD"):
                return True
            if tu == "CARD" and isinstance(d, str) and "RED" in d.upper():
                return True

        if isinstance(d, str) and "RED" in d.upper():
            return True

        l1 = e.get("line1")
        if isinstance(l1, str) and "RED" in l1.upper():
            return True

        return False

    def _is_goal_event(e: Dict[str, Any]) -> bool:
        t = e.get("type")
        d = e.get("detail")

        if isinstance(t, str):
            tu = t.strip().upper()
            if tu in ("GOAL", "GOAL_NORMAL", "GOAL_PENALTY", "PENALTY_GOAL"):
                return True

        # 서버/수집 데이터 방어
        if isinstance(d, str) and "GOAL" in d.upper():
            return True

        l1 = e.get("line1")
        if isinstance(l1, str) and "GOAL" in l1.upper():
            return True

        return False

    def _calc_from_timeline(
        timeline_list: List[Any],
        home_id: Any,
        away_id: Any,
    ) -> Tuple[int, int, int, int, int, int]:
        """
        return: (home_ft, away_ft, home_ht, away_ht, home_red, away_red)
        """
        home_ft = away_ft = 0
        home_ht = away_ht = 0
        home_red = away_red = 0

        for item in timeline_list:
            if not isinstance(item, dict):
                continue

            # side 판별
            side = item.get("side")
            side_home = item.get("side_home")
            team_id = item.get("team_id") or item.get("teamId")

            resolved_side: Optional[str] = None
            if isinstance(side, str):
                s = side.strip().lower()
                if s in ("home", "away"):
                    resolved_side = s
            elif isinstance(side_home, bool):
                resolved_side = "home" if side_home else "away"
            elif team_id is not None:
                if team_id == home_id:
                    resolved_side = "home"
                elif team_id == away_id:
                    resolved_side = "away"

            # 레드카드
            if _is_red_event(item):
                if resolved_side == "home":
                    home_red += 1
                elif resolved_side == "away":
                    away_red += 1
                continue

            # 골
            if _is_goal_event(item):
                if resolved_side == "home":
                    home_ft += 1
                elif resolved_side == "away":
                    away_ft += 1

                m = _get_minute(item)
                if m is not None and m <= 45:
                    if resolved_side == "home":
                        home_ht += 1
                    elif resolved_side == "away":
                        away_ht += 1
                continue

        return home_ft, away_ft, home_ht, away_ht, home_red, away_red

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
        _admin_log("fixtures_merged_list", ok=False, status_code=400, detail={"error": "date required"})
        return jsonify({"ok": False, "error": "date is required (YYYY-MM-DD)"}), 400

    try:
        user_tz = pytz.timezone(tz_str)
    except Exception:
        _admin_log("fixtures_merged_list", ok=False, status_code=400, detail={"error": "invalid timezone", "timezone": tz_str})
        return jsonify({"ok": False, "error": f"Invalid timezone: {tz_str}"}), 400

    try:
        local_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        _admin_log("fixtures_merged_list", ok=False, status_code=400, detail={"error": "invalid date", "date": date_str})
        return jsonify({"ok": False, "error": "Invalid date format YYYY-MM-DD"}), 400

    local_start = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 0, 0, 0))
    local_end   = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 23, 59, 59))
    utc_start = local_start.astimezone(timezone.utc)
    utc_end   = local_end.astimezone(timezone.utc)

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
            m.status_long,
            m.home_id,
            m.away_id,
            m.home_ft,
            m.away_ft,
            m.home_ht,
            m.away_ht,
            m.venue_name,
            m.league_round,
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
            "status_long": r["status_long"],
            "league_name": r["league_name"],
            "league_logo": r["league_logo"],
            "league_country": r["league_country"],
            "league_round": r["league_round"],
            "venue_name": r["venue_name"],
            "home": {
                "id": r["home_id"],
                "name": r["home_name"],
                "logo": r["home_logo"],
                "ft": r["home_ft"],
                "ht": r["home_ht"],
                "red_cards": r["home_red_cards"],
            },
            "away": {
                "id": r["away_id"],
                "name": r["away_name"],
                "logo": r["away_logo"],
                "ft": r["away_ft"],
                "ht": r["away_ht"],
                "red_cards": r["away_red_cards"],
            },
        })

    fixture_ids = [f["fixture_id"] for f in fixtures]
    override_map = _load_match_overrides(fixture_ids)

    fixture_patch_keys = {
        "fixture_id", "league_id", "season",
        "date_utc", "kickoff_utc",
        "status_group", "status", "elapsed", "minute", "status_long",
        "league_round", "venue_name",
        "league_name", "league_logo", "league_country",
        "home", "away",
        "hidden",
    }

    merged = []
    for f in fixtures:
        patch = override_map.get(f["fixture_id"])
        if patch and isinstance(patch, dict):
            # ✅ admin 목록에는 큰 블록(timeline/insights_overall 등)이 붙지 않게, 필요한 키만 추려서 merge
            if isinstance(patch.get("header"), dict):
                p2 = dict(patch.get("header") or {})
                if "hidden" in patch:
                    p2["hidden"] = patch.get("hidden")
            else:
                p2 = {k: v for k, v in patch.items() if k in fixture_patch_keys}

            # ✅ timeline이 있으면, 그 timeline 기준으로 ft/ht/red_cards를 재계산해서 p2에 주입
            tl = _extract_timeline_list(patch)
            if isinstance(tl, list):
                home_id = (f.get("home") or {}).get("id")
                away_id = (f.get("away") or {}).get("id")
                hft, aft, hht, aht, hrc, arc = _calc_from_timeline(tl, home_id, away_id)

                home_p = p2.get("home") if isinstance(p2.get("home"), dict) else {}
                away_p = p2.get("away") if isinstance(p2.get("away"), dict) else {}

                home_p = dict(home_p)
                away_p = dict(away_p)

                home_p["ft"] = hft
                away_p["ft"] = aft
                home_p["ht"] = hht
                away_p["ht"] = aht
                home_p["red_cards"] = hrc
                away_p["red_cards"] = arc

                p2["home"] = home_p
                p2["away"] = away_p

            f2 = _deep_merge(f, p2)

            # 관리자용이므로 hidden=true도 제외하지 않음
            f2["_has_override"] = True
            merged.append(f2)
        else:
            f["_has_override"] = False
            merged.append(f)

    _admin_log(
        "fixtures_merged_list",
        ok=True,
        status_code=200,
        detail={"date": date_str, "timezone": tz_str, "league_ids": league_ids_raw or "", "rows": len(merged)},
    )
    return jsonify({"ok": True, "rows": merged})





@app.route(f"/{ADMIN_PATH}/api/fixtures_raw", methods=["GET"])
@require_admin
def admin_fixtures_raw():
    """
    ✅ override 적용 전 "원본" fixtures 반환
    - /api/fixtures 와 동일한 필터(date/timezone/league_ids) 사용
    - 단, match_overrides 병합/hidden 처리 없이 그대로 반환
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
        _admin_log("fixtures_raw_list", ok=False, status_code=400, detail={"error": "date required"})
        return jsonify({"ok": False, "error": "date is required (YYYY-MM-DD)"}), 400

    try:
        user_tz = pytz.timezone(tz_str)
    except Exception:
        _admin_log("fixtures_raw_list", ok=False, status_code=400, detail={"error": "invalid timezone", "timezone": tz_str})
        return jsonify({"ok": False, "error": f"Invalid timezone: {tz_str}"}), 400

    try:
        local_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        _admin_log("fixtures_raw_list", ok=False, status_code=400, detail={"error": "invalid date", "date": date_str})
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
            m.status_long,
            m.home_id,
            m.away_id,
            m.home_ft,
            m.away_ft,
            m.home_ht,
            m.away_ht,
            m.venue_name,
            m.league_round,
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
            "status_long": r["status_long"],
            "league_name": r["league_name"],
            "league_logo": r["league_logo"],
            "league_country": r["league_country"],
            "league_round": r["league_round"],
            "venue_name": r["venue_name"],
            "home": {
                "id": r["home_id"],
                "name": r["home_name"],
                "logo": r["home_logo"],
                "ft": r["home_ft"],
                "ht": r["home_ht"],
                "red_cards": r["home_red_cards"],
            },
            "away": {
                "id": r["away_id"],
                "name": r["away_name"],
                "logo": r["away_logo"],
                "ft": r["away_ft"],
                "ht": r["away_ht"],
                "red_cards": r["away_red_cards"],
            },
        })

    _admin_log(
        "fixtures_raw_list",
        ok=True,
        status_code=200,
        detail={
            "date": date_str,
            "timezone": tz_str,
            "league_ids": league_ids_raw or "",
            "rows": len(fixtures),
        },
    )
    return jsonify({"ok": True, "rows": fixtures})





# ─────────────────────────────────────────
# API: /api/fixtures  (타임존 + 다중 리그 필터)
# ─────────────────────────────────────────
@app.route("/api/fixtures")
def list_fixtures():
    """
    사용자의 지역 날짜를 기반으로 경기 조회.
    ✅ override 반영
    ✅ override에 timeline이 있으면 timeline 기준으로 ft/ht/red_cards를 재계산해 동기화
    """

    def _extract_timeline_list(patch_obj: Any) -> Optional[List[Any]]:
        if not isinstance(patch_obj, dict):
            return None
        tl = patch_obj.get("timeline")
        if isinstance(tl, list):
            return tl
        if isinstance(tl, dict):
            ev = tl.get("events")
            if isinstance(ev, list):
                return ev
        return None

    def _get_minute(e: Dict[str, Any]) -> Optional[int]:
        for k in ("minute", "elapsed", "time", "min"):
            v = e.get(k)
            if isinstance(v, int):
                return v
            if isinstance(v, str):
                s = v.strip()
                if s.isdigit():
                    return int(s)
        v2 = e.get("minute")
        if isinstance(v2, str) and "+" in v2:
            base = v2.split("+", 1)[0].strip()
            if base.isdigit():
                return int(base)
        return None

    def _is_red_event(e: Dict[str, Any]) -> bool:
        t = e.get("type")
        d = e.get("detail")

        if isinstance(t, str):
            tu = t.strip().upper()
            if tu in ("RED", "RED_CARD", "REDCARD"):
                return True
            if tu == "CARD" and isinstance(d, str) and "RED" in d.upper():
                return True

        if isinstance(d, str) and "RED" in d.upper():
            return True

        l1 = e.get("line1")
        if isinstance(l1, str) and "RED" in l1.upper():
            return True

        return False

    def _is_goal_event(e: Dict[str, Any]) -> bool:
        t = e.get("type")
        d = e.get("detail")

        if isinstance(t, str):
            tu = t.strip().upper()
            if tu in ("GOAL", "GOAL_NORMAL", "GOAL_PENALTY", "PENALTY_GOAL"):
                return True

        if isinstance(d, str) and "GOAL" in d.upper():
            return True

        l1 = e.get("line1")
        if isinstance(l1, str) and "GOAL" in l1.upper():
            return True

        return False

    def _calc_from_timeline(
        timeline_list: List[Any],
        home_id: Any,
        away_id: Any,
    ) -> Tuple[int, int, int, int, int, int]:
        home_ft = away_ft = 0
        home_ht = away_ht = 0
        home_red = away_red = 0

        for item in timeline_list:
            if not isinstance(item, dict):
                continue

            side = item.get("side")
            side_home = item.get("side_home")
            team_id = item.get("team_id") or item.get("teamId")

            resolved_side: Optional[str] = None
            if isinstance(side, str):
                s = side.strip().lower()
                if s in ("home", "away"):
                    resolved_side = s
            elif isinstance(side_home, bool):
                resolved_side = "home" if side_home else "away"
            elif team_id is not None:
                if team_id == home_id:
                    resolved_side = "home"
                elif team_id == away_id:
                    resolved_side = "away"

            if _is_red_event(item):
                if resolved_side == "home":
                    home_red += 1
                elif resolved_side == "away":
                    away_red += 1
                continue

            if _is_goal_event(item):
                if resolved_side == "home":
                    home_ft += 1
                elif resolved_side == "away":
                    away_ft += 1

                m = _get_minute(item)
                if m is not None and m <= 45:
                    if resolved_side == "home":
                        home_ht += 1
                    elif resolved_side == "away":
                        away_ht += 1
                continue

        return home_ft, away_ft, home_ht, away_ht, home_red, away_red

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

    local_start = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 0, 0, 0))
    local_end   = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 23, 59, 59))

    utc_start = local_start.astimezone(timezone.utc)
    utc_end   = local_end.astimezone(timezone.utc)

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
            m.status_long,
            m.home_id,
            m.away_id,
            m.home_ft,
            m.away_ft,
            m.home_ht,
            m.away_ht,
            m.venue_name,
            m.league_round,
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
            "status_long": r["status_long"],
            "league_name": r["league_name"],
            "league_logo": r["league_logo"],
            "league_country": r["league_country"],
            "league_round": r["league_round"],
            "venue_name": r["venue_name"],
            "home": {
                "id": r["home_id"],
                "name": r["home_name"],
                "logo": r["home_logo"],
                "ft": r["home_ft"],
                "ht": r["home_ht"],
                "red_cards": r["home_red_cards"],
            },
            "away": {
                "id": r["away_id"],
                "name": r["away_name"],
                "logo": r["away_logo"],
                "ft": r["away_ft"],
                "ht": r["away_ht"],
                "red_cards": r["away_red_cards"],
            },
        })

    fixture_ids = [f["fixture_id"] for f in fixtures]
    override_map = _load_match_overrides(fixture_ids)

    fixture_patch_keys = {
        "fixture_id", "league_id", "season",
        "date_utc", "kickoff_utc",
        "status_group", "status", "elapsed", "minute", "status_long",
        "league_round", "venue_name",
        "league_name", "league_logo", "league_country",
        "home", "away",
        "hidden",
    }

    merged = []
    for f in fixtures:
        patch = override_map.get(f["fixture_id"])
        if patch and isinstance(patch, dict):
            # 목록에는 필요한 키만 merge
            if isinstance(patch.get("header"), dict):
                p2 = dict(patch.get("header") or {})
                if "hidden" in patch:
                    p2["hidden"] = patch.get("hidden")
            else:
                p2 = {k: v for k, v in patch.items() if k in fixture_patch_keys}

            # ✅ timeline이 있으면 timeline 기준으로 ft/ht/red_cards 동기화해서 p2에 주입
            tl = _extract_timeline_list(patch)
            if isinstance(tl, list):
                home_id = (f.get("home") or {}).get("id")
                away_id = (f.get("away") or {}).get("id")
                hft, aft, hht, aht, hrc, arc = _calc_from_timeline(tl, home_id, away_id)

                home_p = p2.get("home") if isinstance(p2.get("home"), dict) else {}
                away_p = p2.get("away") if isinstance(p2.get("away"), dict) else {}

                home_p = dict(home_p)
                away_p = dict(away_p)

                home_p["ft"] = hft
                away_p["ft"] = aft
                home_p["ht"] = hht
                away_p["ht"] = aht
                home_p["red_cards"] = hrc
                away_p["red_cards"] = arc

                p2["home"] = home_p
                p2["away"] = away_p

            f2 = _deep_merge(f, p2)

            # hidden=true면 노출 제외
            if f2.get("hidden") is True:
                continue

            merged.append(f2)
        else:
            merged.append(f)

    return jsonify({"ok": True, "rows": merged})



# ─────────────────────────────────────────
# 실행
# ─────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)

































