import os
import json
import uuid
from datetime import datetime, timezone, timedelta
from functools import wraps
from typing import Dict, List, Any

from flask import Flask, request, jsonify, Response, send_from_directory, redirect
from werkzeug.exceptions import HTTPException
import pytz  # 타임존 계산용

# ─────────────────────────────────────
# Board DB (separate database)
#  - Render env: BOARD_DATABASE_URL
# ─────────────────────────────────────
BOARD_DATABASE_URL = os.getenv("BOARD_DATABASE_URL", "").strip()

try:
    import psycopg  # psycopg v3
    from psycopg.rows import dict_row as _psycopg_dict_row

    def _board_connect():
        if not BOARD_DATABASE_URL:
            raise RuntimeError("BOARD_DATABASE_URL is not set")
        return psycopg.connect(BOARD_DATABASE_URL, row_factory=_psycopg_dict_row)

except Exception:
    psycopg = None
    _psycopg_dict_row = None

    import psycopg2
    import psycopg2.extras

    def _board_connect():
        if not BOARD_DATABASE_URL:
            raise RuntimeError("BOARD_DATABASE_URL is not set")
        # ✅ psycopg2에서도 dict 형태로 받기 (jsonify 안전)
        conn = psycopg2.connect(BOARD_DATABASE_URL)
        return conn



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
from routers.version_router import version_bp
from search.routes import search_bp

from hockey.routers.hockey_games_router import hockey_games_bp
from hockey.routers.hockey_fixtures_router import hockey_fixtures_bp
from hockey.routers.hockey_matchdetail_router import hockey_matchdetail_bp
from hockey.routers.hockey_standings_router import hockey_standings_bp
from hockey.routers.hockey_insights_router import hockey_insights_bp
from hockey.routers.hockey_notifications_router import hockey_notifications_bp
from hockey.teamdetail.hockey_team_detail_routes import hockey_teamdetail_bp
from hockey.leaguedetail.hockey_leaguedetail_routes import hockey_leaguedetail_bp

from basketball.nba.routers.nba_fixtures_router import nba_fixtures_bp
from basketball.nba.routers.nba_matchdetail_router import nba_matchdetail_bp
from basketball.nba.routers.nba_standings_router import nba_standings_bp
from basketball.nba.routers.nba_games_router import nba_games_bp
from basketball.nba.routers.nba_notifications_router import nba_notifications_bp
from basketball.nba.routers.nba_insights_router import nba_insights_bp


import traceback
import sys

import logging
log = logging.getLogger("sportsstatsx-api")



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
app.register_blueprint(version_bp)
app.register_blueprint(search_bp)

app.register_blueprint(hockey_games_bp)
app.register_blueprint(hockey_fixtures_bp)
app.register_blueprint(hockey_matchdetail_bp)
app.register_blueprint(hockey_leaguedetail_bp)
app.register_blueprint(hockey_standings_bp)
app.register_blueprint(hockey_insights_bp)
app.register_blueprint(hockey_notifications_bp)
app.register_blueprint(hockey_teamdetail_bp)app.register_blueprint(hockey_teamdetail_bp)

app.register_blueprint(nba_fixtures_bp)
app.register_blueprint(nba_matchdetail_bp)
app.register_blueprint(nba_standings_bp)
app.register_blueprint(nba_games_bp)
app.register_blueprint(nba_notifications_bp)
app.register_blueprint(nba_insights_bp)



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
# Optional table: match_live_state (있으면 INPLAY 빨간카드/등 표시를 더 싸게 처리)
# - 테이블이 없으면 JOIN이 즉시 터지므로 존재 확인 후 fallback 한다.
# ─────────────────────────────────────────
_MATCH_LIVE_STATE_OK: bool | None = None

def _match_live_state_available() -> bool:
    global _MATCH_LIVE_STATE_OK
    if _MATCH_LIVE_STATE_OK is not None:
        return _MATCH_LIVE_STATE_OK

    try:
        row = fetch_one("SELECT to_regclass('public.match_live_state') AS t", ())
        ok = bool(row and row.get("t"))
        if ok:
            _MATCH_LIVE_STATE_OK = True
            return True
    except Exception:
        pass

    # 없으면 생성 시도(권한/환경에 따라 실패할 수 있으니 try/except)
    try:
        execute(
            """
            CREATE TABLE IF NOT EXISTS match_live_state (
                fixture_id BIGINT PRIMARY KEY,
                home_red   INTEGER DEFAULT 0,
                away_red   INTEGER DEFAULT 0,
                updated_utc TIMESTAMPTZ DEFAULT now()
            )
            """,
            (),
        )
        row2 = fetch_one("SELECT to_regclass('public.match_live_state') AS t", ())
        _MATCH_LIVE_STATE_OK = bool(row2 and row2.get("t"))
        return _MATCH_LIVE_STATE_OK
    except Exception:
        _MATCH_LIVE_STATE_OK = False
        return False



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

    # ✅ 카디널리티 폭발 방지:
    # - request.path 는 /api/x/12345 처럼 값이 무한히 늘어날 수 있음
    # - url_rule.rule 은 /api/x/<int:id> 형태로 고정 라벨이 됨
    if getattr(request, "url_rule", None) is not None and getattr(request.url_rule, "rule", None):
        endpoint = request.url_rule.rule
    else:
        # fallback (정적 라우트/일부 상황)
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

# ADMIN_PATH가 비어있으면 "/{ADMIN_PATH}" == "/" 라우트가 되어 root("/")와 충돌할 수 있으므로
# 비활성 상태에서도 충돌만은 피하도록 안전한 기본값을 부여한다.
if not ADMIN_PATH:
    ADMIN_PATH = "__admin__"



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
# API: fixtures by ids (favorites refresh)
# ─────────────────────────────────────────
@app.route("/api/fixtures_by_ids", methods=["GET"])
def fixtures_by_ids():
    ids_raw = request.args.get("ids", type=str) or ""
    live_only = (request.args.get("live", type=int) or 0) == 1
    apply_override = (request.args.get("apply_override", type=int) or 1) == 1
    include_hidden = (request.args.get("include_hidden", type=int) or 0) == 1

    if not ids_raw.strip():
        return jsonify({"ok": False, "error": "ids is required (comma-separated)"}), 400

    # 입력 순서 유지 + 중복 제거
    ordered_ids: List[int] = []
    seen: set[int] = set()
    for part in ids_raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            fid = int(part)
        except Exception:
            continue
        if fid in seen:
            continue
        seen.add(fid)
        ordered_ids.append(fid)

    if not ordered_ids:
        return jsonify({"ok": False, "error": "no valid ids"}), 400

    # 과도한 IN 방지 (필요하면 조정)
    if len(ordered_ids) > 200:
        return jsonify({"ok": False, "error": "too many ids (max 200)"}), 400

    placeholders = ", ".join(["%s"] * len(ordered_ids))
    params: List[Any] = list(ordered_ids)

    where_clauses = [f"m.fixture_id IN ({placeholders})"]
    if live_only:
        where_clauses.append("m.status_group = 'INPLAY'")
    where_sql = " AND ".join(where_clauses)

    use_mls = _match_live_state_available()
    red_detail_sql = "('Red Card','Second Yellow card','Second Yellow Card')"

    if use_mls:
        home_red_sql = f"""
            COALESCE(
                mls.home_red,
                (
                    SELECT COUNT(*) FROM match_events e
                    WHERE e.fixture_id = m.fixture_id
                      AND e.team_id = m.home_id
                      AND e.type = 'Card'
                      AND e.detail IN {red_detail_sql}
                )
            ) AS home_red_cards
        """
        away_red_sql = f"""
            COALESCE(
                mls.away_red,
                (
                    SELECT COUNT(*) FROM match_events e
                    WHERE e.fixture_id = m.fixture_id
                      AND e.team_id = m.away_id
                      AND e.type = 'Card'
                      AND e.detail IN {red_detail_sql}
                )
            ) AS away_red_cards
        """
        mls_join = "LEFT JOIN match_live_state mls ON mls.fixture_id = m.fixture_id"
    else:
        home_red_sql = f"""
            (
                SELECT COUNT(*) FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id = m.home_id
                  AND e.type = 'Card'
                  AND e.detail IN {red_detail_sql}
            ) AS home_red_cards
        """
        away_red_sql = f"""
            (
                SELECT COUNT(*) FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id = m.away_id
                  AND e.type = 'Card'
                  AND e.detail IN {red_detail_sql}
            ) AS away_red_cards
        """
        mls_join = ""

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
            c.flag AS league_country_flag,
            (rf.data_json::jsonb->'score'->'extratime'->>'home') AS home_et,
            (rf.data_json::jsonb->'score'->'extratime'->>'away') AS away_et,
            (rf.data_json::jsonb->'score'->'penalty'->>'home') AS home_pen,
            (rf.data_json::jsonb->'score'->'penalty'->>'away') AS away_pen,
            {home_red_sql},
            {away_red_sql}
        FROM matches m
        JOIN teams th ON th.id = m.home_id
        JOIN teams ta ON ta.id = m.away_id
        JOIN leagues l ON l.id = m.league_id
        LEFT JOIN countries c
          ON LOWER(TRIM(c.name)) = LOWER(TRIM(l.country))
        LEFT JOIN match_fixtures_raw rf ON rf.fixture_id = m.fixture_id
        {mls_join}
        WHERE {where_sql}
        ORDER BY m.date_utc ASC
    """

    rows = fetch_all(sql, tuple(params))

    base_map: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        fid = int(r["fixture_id"])
        base_map[fid] = {
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
            "league_country_flag": r["league_country_flag"],
            "league_round": r["league_round"],
            "venue_name": r["venue_name"],
            "home": {
                "id": r["home_id"],
                "name": r["home_name"],
                "logo": r["home_logo"],
                "ft": r["home_ft"],
                "ht": r["home_ht"],
                "et": int(r["home_et"]) if r.get("home_et") not in (None, "") else None,
                "pen": int(r["home_pen"]) if r.get("home_pen") not in (None, "") else None,
                "red_cards": r["home_red_cards"],
            },
            "away": {
                "id": r["away_id"],
                "name": r["away_name"],
                "logo": r["away_logo"],
                "ft": r["away_ft"],
                "ht": r["away_ht"],
                "et": int(r["away_et"]) if r.get("away_et") not in (None, "") else None,
                "pen": int(r["away_pen"]) if r.get("away_pen") not in (None, "") else None,
                "red_cards": r["away_red_cards"],
            },
        }

    fixture_patch_keys = {
        "fixture_id", "league_id", "season",
        "date_utc", "kickoff_utc",
        "status_group", "status", "elapsed", "minute", "status_long",
        "league_round", "venue_name",
        "league_name", "league_logo", "league_country",
        "home", "away",
        "hidden",
    }

    merged_map: Dict[int, Dict[str, Any]] = dict(base_map)

    if apply_override and base_map:
        override_map = _load_match_overrides(list(base_map.keys()))
        for fid, f in list(merged_map.items()):
            patch = override_map.get(fid)
            if patch and isinstance(patch, dict):
                if isinstance(patch.get("header"), dict):
                    p2 = dict(patch.get("header") or {})
                    if "hidden" in patch:
                        p2["hidden"] = patch.get("hidden")
                else:
                    p2 = {k: v for k, v in patch.items() if k in fixture_patch_keys}

                f2 = _deep_merge(f, p2)
                f2["_has_override"] = True
                merged_map[fid] = f2
            else:
                f["_has_override"] = False
                merged_map[fid] = f

    out_rows: List[Dict[str, Any]] = []
    for fid in ordered_ids:
        f = merged_map.get(fid)
        if not f:
            continue
        if (not include_hidden) and bool(f.get("hidden")):
            continue
        out_rows.append(f)

    return jsonify({"ok": True, "count": len(out_rows), "rows": out_rows})



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

# ─────────────────────────────────────────
# NBA static assets (league logo etc.)
# - serve files under basketball/nba/static
#   URL: /static/nba/<filename>
# ─────────────────────────────────────────
NBA_STATIC_DIR = os.path.join(app.root_path, "basketball", "nba", "static")

@app.route("/static/nba/<path:filename>")
def nba_static(filename: str):
    return send_from_directory(NBA_STATIC_DIR, filename)


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
# Admin Pages (split HTML)
# - /{ADMIN_PATH}/pages/*.html 로 분리된 페이지 제공
# ─────────────────────────────────────────
ADMIN_PAGES_DIR = os.path.join(STATIC_DIR, "admin_pages")

@app.route(f"/{ADMIN_PATH}/pages/<path:filename>")
def admin_pages(filename: str):
    if not _admin_enabled():
        return jsonify({"ok": False, "error": "admin disabled"}), 404

    # pages는 HTML만 제공 (보안은 ADMIN_PATH 난수 + API 토큰으로 보장)
    resp = send_from_directory(ADMIN_PAGES_DIR, filename)
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

def _admin_sync_header_from_timeline_patch(patch: Dict[str, Any]) -> None:
    """
    Admin 이벤트(timeline) 수정 시 표시 레이어(매치리스트/스코어블럭/헤더)도 같이 동기화.
    - timeline(list)에서 GOAL/PEN_GOAL/OWN_GOAL, RED를 집계하여
      patch.header.home/away 의 ft/ht/score/red_cards 를 자동 갱신한다.
    - DB 원본(matches/match_events)은 건드리지 않음(옵션1).
    """
    if not isinstance(patch, dict):
        return

    timeline = patch.get("timeline")
    if not isinstance(timeline, list):
        return

    def _as_int(v: Any) -> int:
        try:
            if v is None or v == "":
                return 0
            return int(v)
        except Exception:
            return 0

    def _pick(o: Dict[str, Any], *keys: str) -> Any:
        for k in keys:
            if k in o and o.get(k) is not None:
                return o.get(k)
        return None

    def _canon_side(raw: Any) -> str:
        s = str(raw or "").strip().lower()
        if s in ("home", "h"):
            return "home"
        if s in ("away", "a"):
            return "away"
        return ""

    def _canon_type(raw_type: Any, raw_detail: Any) -> str:
        t = str(raw_type or "").strip().upper()
        d = str(raw_detail or "").strip().upper()

        # 카드류: type이 "CARD"로 오고 detail에 RED/YELLOW가 들어올 수도 있음
        if t in ("CARD", "CARDS"):
            if "RED" in d:
                return "RED"
            if "YELLOW" in d:
                return "YELLOW"
            return "CARD"

        # 이미 정규화된 형태도 허용
        if t in ("RED CARD", "RED_CARD"):
            return "RED"
        if t in ("YELLOW CARD", "YELLOW_CARD"):
            return "YELLOW"

        return t

    home_ft = 0
    away_ft = 0
    home_ht = 0
    away_ht = 0
    home_red = 0
    away_red = 0

    for e in timeline:
        if not isinstance(e, dict):
            continue

        minute = _as_int(_pick(e, "minute", "min", "elapsed"))
        side = _canon_side(_pick(e, "side", "team", "teamSide", "team_side"))
        typ = _canon_type(_pick(e, "type", "event_type"), _pick(e, "detail", "reason", "note"))

        # 득점: timeline의 side가 득점 팀이라는 전제(현재 admin 이벤트 에디터 구조와 동일)
        if typ in ("GOAL", "PEN_GOAL", "OWN_GOAL"):
            if side == "home":
                home_ft += 1
                if minute <= 45:
                    home_ht += 1
            elif side == "away":
                away_ft += 1
                if minute <= 45:
                    away_ht += 1

        # 레드카드(표시용): timeline에 RED가 들어있으면 집계
        if typ == "RED":
            if side == "home":
                home_red += 1
            elif side == "away":
                away_red += 1

    # header 생성/갱신
    header = patch.get("header")
    if not isinstance(header, dict):
        header = {}
        patch["header"] = header

    home = header.get("home")
    if not isinstance(home, dict):
        home = {}
        header["home"] = home

    away = header.get("away")
    if not isinstance(away, dict):
        away = {}
        header["away"] = away

    home["ft"] = home_ft
    home["ht"] = home_ht
    home["score"] = home_ft
    home["red_cards"] = home_red

    away["ft"] = away_ft
    away["ht"] = away_ht
    away["score"] = away_ft
    away["red_cards"] = away_red



@app.route(f"/{ADMIN_PATH}/api/overrides/<int:fixture_id>", methods=["PUT"])
@require_admin
def admin_upsert_override(fixture_id: int):
    patch = request.get_json(silent=True)
    if not isinstance(patch, dict):
        _admin_log(
            "override_upsert",
            ok=False,
            status_code=400,
            fixture_id=fixture_id,
            detail={"error": "patch must be object"},
        )
        return jsonify({"ok": False, "error": "patch must be a JSON object"}), 400

    # ✅ 옵션1(표시 레이어 동기화): timeline -> header(ft/ht/score/red_cards) 자동 생성/갱신
    _admin_sync_header_from_timeline_patch(patch)

    execute(
        """
        INSERT INTO match_overrides (fixture_id, patch, updated_at)
        VALUES (%s, %s::jsonb, now())
        ON CONFLICT (fixture_id)
        DO UPDATE SET patch = EXCLUDED.patch, updated_at = now()
        """,
        (fixture_id, json.dumps(patch, ensure_ascii=False)),
    )

    _admin_log(
        "override_upsert",
        ok=True,
        status_code=200,
        fixture_id=fixture_id,
        detail={"keys": list(patch.keys())[:50]},
    )
    return jsonify({"ok": True, "fixture_id": fixture_id})



@app.route(f"/{ADMIN_PATH}/api/overrides/<int:fixture_id>", methods=["DELETE"])
@require_admin
def admin_delete_override(fixture_id: int):
    execute("DELETE FROM match_overrides WHERE fixture_id = %s", (fixture_id,))
    _admin_log("override_delete", ok=True, status_code=200, fixture_id=fixture_id)
    return jsonify({"ok": True, "fixture_id": fixture_id})

# ─────────────────────────────────────────
# Admin API: Hockey overrides + fixtures(raw/merged) + bundle
# - DB: hockey_match_overrides
# - key column: game_id (기본 가정)
# ─────────────────────────────────────────

def _hockey_load_overrides(game_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """
    hockey_match_overrides에서 patch 로드
    - DB key column: fixture_id (PK)
    - UI/서비스 레이어에서는 game_id를 쓰지만, 현재 구조는
      fixture_id == game_id 로 매핑해서 사용한다.
    """
    if not game_ids:
        return {}

    try:
        from hockey.hockey_db import hockey_fetch_all
    except Exception:
        return {}

    ids = list(dict.fromkeys([int(x) for x in game_ids if x is not None]))
    if not ids:
        return {}

    placeholders = ",".join(["%s"] * len(ids))
    sql = f"SELECT fixture_id, patch FROM hockey_match_overrides WHERE fixture_id IN ({placeholders})"
    rows = hockey_fetch_all(sql, tuple(ids)) or []

    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        try:
            fid = int(r.get("fixture_id"))
        except Exception:
            continue

        p = r.get("patch")
        if isinstance(p, dict):
            out[fid] = p
        else:
            # patch가 json string으로 들어오는 케이스도 방어
            try:
                import json as _json
                pp = _json.loads(p) if isinstance(p, str) else None
                if isinstance(pp, dict):
                    out[fid] = pp
            except Exception:
                pass

    return out



def _hockey_game_to_fixture_row(g: Dict[str, Any]) -> Dict[str, Any]:
    """
    /api/hockey/fixtures 의 game 객체를 football_override UI가 쓰는 row 형태로 변환
    - fixture_id == game_id 로 맞춤(프론트 수정 최소화)
    """
    league = g.get("league") or {}
    home = g.get("home") or {}
    away = g.get("away") or {}

    game_id = g.get("game_id")
    league_id = g.get("league_id") or league.get("id")
    season = g.get("season")
    date_utc = g.get("date_utc")

    status = (g.get("status") or "").strip().upper()
    status_long = g.get("status_long") or ""

    # hockey status_group 대충 3그룹만: NS / LIVE / FINISHED
    if status in ("NS",):
        status_group = "NS"
    elif status in ("FT", "AET", "PEN"):
        status_group = "FINISHED"
    elif status in ("P1", "P2", "P3", "BT", "OT", "SO"):
        status_group = "LIVE"
    else:
        status_group = status or ""

    # score는 UI quick-edit가 ft를 쓰므로 ft/score 둘 다 세팅
    hscore = home.get("score")
    ascore = away.get("score")

    row = {
        "fixture_id": int(game_id) if game_id is not None else None,
        "league_id": int(league_id) if league_id is not None else None,
        "season": int(season) if season is not None else None,
        "date_utc": date_utc,
        "kickoff_utc": date_utc,   # UI에서 비교용으로만 쓰이니 동일값
        "status_group": status_group,
        "status": status,
        "elapsed": None,
        "minute": None,
        "status_long": status_long,
        "league_round": "",
        "venue_name": "",
        "league_name": league.get("name") or "",
        "league_logo": league.get("logo"),
        "league_country": league.get("country") or "",
        "home": {
            "id": home.get("id"),
            "name": home.get("name") or "",
            "logo": home.get("logo"),
            "ft": hscore,
            "ht": None,
            "score": hscore,
            "red_cards": 0,
        },
        "away": {
            "id": away.get("id"),
            "name": away.get("name") or "",
            "logo": away.get("logo"),
            "ft": ascore,
            "ht": None,
            "score": ascore,
            "red_cards": 0,
        },
        "hidden": False,
    }
    return row


@app.route(f"/{ADMIN_PATH}/api/hockey/fixtures_raw", methods=["GET"])
@require_admin
def admin_hockey_fixtures_raw():
    """
    ✅ 하키 원본(raw) fixtures (override/hidden 미적용)
    - hockey_fixtures_router와 동일한 date/timezone/league_ids 해석
    """
    from hockey.services.hockey_fixtures_service import hockey_get_fixtures_by_utc_range

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

    # ✅ 하키 정식과 동일: [local_start, next_day_start)
    local_start = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 0, 0, 0))
    local_next_day_start = local_start + timedelta(days=1)
    utc_start = local_start.astimezone(timezone.utc)
    utc_end = local_next_day_start.astimezone(timezone.utc)

    games = hockey_get_fixtures_by_utc_range(
        utc_start=utc_start,
        utc_end=utc_end,
        league_ids=league_ids,
        league_id=league_id,
    )

    rows = [_hockey_game_to_fixture_row(g) for g in (games or [])]
    return jsonify({"ok": True, "rows": rows})


@app.route(f"/{ADMIN_PATH}/api/hockey/fixtures_merged", methods=["GET"])
@require_admin
def admin_hockey_fixtures_merged():
    """
    ✅ 하키 merged fixtures (override 반영 + hidden 포함)
    - UI 배지용: _has_override 포함
    """
    from hockey.services.hockey_fixtures_service import hockey_get_fixtures_by_utc_range

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
    local_next_day_start = local_start + timedelta(days=1)
    utc_start = local_start.astimezone(timezone.utc)
    utc_end = local_next_day_start.astimezone(timezone.utc)

    games = hockey_get_fixtures_by_utc_range(
        utc_start=utc_start,
        utc_end=utc_end,
        league_ids=league_ids,
        league_id=league_id,
    )

    rows = [_hockey_game_to_fixture_row(g) for g in (games or [])]
    ids = [r["fixture_id"] for r in rows if r.get("fixture_id") is not None]
    override_map = _hockey_load_overrides(ids)

    fixture_patch_keys = {
        "fixture_id", "league_id", "season",
        "date_utc", "kickoff_utc",
        "status_group", "status", "elapsed", "minute", "status_long",
        "league_round", "venue_name",
        "league_name", "league_logo", "league_country",
        "home", "away",
        "hidden",
    }

    merged: List[Dict[str, Any]] = []
    for f in rows:
        fid = f.get("fixture_id")
        patch = override_map.get(fid) if fid is not None else None

        if patch and isinstance(patch, dict):
            if isinstance(patch.get("header"), dict):
                p2 = dict(patch.get("header") or {})
                if "hidden" in patch:
                    p2["hidden"] = patch.get("hidden")
            else:
                p2 = {k: v for k, v in patch.items() if k in fixture_patch_keys}

            f2 = _deep_merge(f, p2)
            f2["_has_override"] = True
            merged.append(f2)
        else:
            f["_has_override"] = False
            merged.append(f)

    return jsonify({"ok": True, "rows": merged})


@app.route(f"/{ADMIN_PATH}/api/hockey/overrides/<int:game_id>", methods=["GET"])
@require_admin
def admin_hockey_get_override(game_id: int):
    try:
        from hockey.hockey_db import hockey_fetch_one
        row = hockey_fetch_one(
            "SELECT patch, updated_at FROM hockey_match_overrides WHERE fixture_id = %s",
            (game_id,),
        )
    except Exception:
        row = None

    if not row:
        return jsonify({"ok": True, "game_id": game_id, "patch": None})

    return jsonify(
        {
            "ok": True,
            "game_id": game_id,
            "patch": row.get("patch"),
            "updated_at": row.get("updated_at"),
        }
    )



@app.route(f"/{ADMIN_PATH}/api/hockey/overrides/<int:game_id>", methods=["PUT"])
@require_admin
def admin_hockey_upsert_override(game_id: int):
    patch = request.get_json(silent=True)
    if not isinstance(patch, dict):
        return jsonify({"ok": False, "error": "patch must be a JSON object"}), 400

    # football과 동일: timeline -> header(ft/ht/score/red_cards) 동기화 시도(없으면 그냥 통과)
    try:
        _admin_sync_header_from_timeline_patch(patch)
    except Exception:
        pass

    try:
        from hockey.hockey_db import hockey_execute
    except Exception:
        hockey_execute = None

    if not hockey_execute:
        from hockey.hockey_db import hockey_fetch_all  # noqa
        return jsonify({"ok": False, "error": "hockey_execute not available"}), 500

    hockey_execute(
        """
        INSERT INTO hockey_match_overrides (fixture_id, patch, updated_at)
        VALUES (%s, %s::jsonb, now())
        ON CONFLICT (fixture_id)
        DO UPDATE SET patch = EXCLUDED.patch, updated_at = now()
        """,
        (game_id, json.dumps(patch, ensure_ascii=False)),
    )
    return jsonify({"ok": True, "game_id": game_id})



@app.route(f"/{ADMIN_PATH}/api/hockey/overrides/<int:game_id>", methods=["DELETE"])
@require_admin
def admin_hockey_delete_override(game_id: int):
    try:
        from hockey.hockey_db import hockey_execute
        hockey_execute("DELETE FROM hockey_match_overrides WHERE fixture_id = %s", (game_id,))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({"ok": True, "game_id": game_id})



@app.route(f"/{ADMIN_PATH}/api/hockey/game_detail_bundle", methods=["GET"])
def admin_hockey_game_detail_bundle():
    """
    ✅ football_override의 match_detail_bundle와 동일 컨셉(하지만 하키용)
    - public endpoint (UI에서 publicApi로 호출)
    - apply_override=0/1 지원
    """
    game_id = request.args.get("fixture_id", type=int) or request.args.get("game_id", type=int)
    apply_override = request.args.get("apply_override", default="1")
    apply_override = str(apply_override).strip() != "0"

    if not game_id:
        return jsonify({"ok": False, "error": "fixture_id(game_id) required"}), 400

    from hockey.services.hockey_matchdetail_service import hockey_get_game_detail
    base = hockey_get_game_detail(int(game_id))

    # base가 {ok:true, data:{...}} 형태면 data만 쓰고, 아니면 통째로
    data = base.get("data") if isinstance(base, dict) and "data" in base else base

    if apply_override:
        patch = _hockey_load_overrides([int(game_id)]).get(int(game_id))
        if isinstance(patch, dict):
            if isinstance(patch.get("header"), dict) and isinstance(data, dict):
                # header가 있으면 header 우선 병합
                if isinstance(data.get("header"), dict):
                    data["header"] = _deep_merge(data["header"], patch["header"])
                else:
                    data["header"] = patch["header"]
                if "hidden" in patch:
                    data["hidden"] = patch.get("hidden")
            elif isinstance(data, dict):
                data = _deep_merge(data, patch)

    return jsonify({"ok": True, "data": data})


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
    local_end = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 23, 59, 59))
    utc_start = local_start.astimezone(timezone.utc)
    utc_end = local_end.astimezone(timezone.utc)

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

    use_mls = _match_live_state_available()

    red_detail_sql = "('Red Card','Second Yellow card','Second Yellow Card')"

    if use_mls:
        home_red_sql = f"""
            CASE
                WHEN m.status_group = 'INPLAY' THEN COALESCE(mls.home_red, 0)
                ELSE (
                    SELECT COUNT(*) FROM match_events e
                    WHERE e.fixture_id = m.fixture_id
                      AND e.team_id = m.home_id
                      AND e.type = 'Card'
                      AND e.detail IN {red_detail_sql}
                )
            END AS home_red_cards
        """
        away_red_sql = f"""
            CASE
                WHEN m.status_group = 'INPLAY' THEN COALESCE(mls.away_red, 0)
                ELSE (
                    SELECT COUNT(*) FROM match_events e
                    WHERE e.fixture_id = m.fixture_id
                      AND e.team_id = m.away_id
                      AND e.type = 'Card'
                      AND e.detail IN {red_detail_sql}
                )
            END AS away_red_cards
        """
        mls_join = "LEFT JOIN match_live_state mls ON mls.fixture_id = m.fixture_id"
    else:
        home_red_sql = f"""
            (
                SELECT COUNT(*) FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id = m.home_id
                  AND e.type = 'Card'
                  AND e.detail IN {red_detail_sql}
            ) AS home_red_cards
        """
        away_red_sql = f"""
            (
                SELECT COUNT(*) FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id = m.away_id
                  AND e.type = 'Card'
                  AND e.detail IN {red_detail_sql}
            ) AS away_red_cards
        """
        mls_join = ""

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
            {home_red_sql},
            {away_red_sql}
        FROM matches m
        JOIN teams th ON th.id = m.home_id
        JOIN teams ta ON ta.id = m.away_id
        JOIN leagues l ON l.id = m.league_id
        {mls_join}
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
            if isinstance(patch.get("header"), dict):
                p2 = dict(patch.get("header") or {})
                if "hidden" in patch:
                    p2["hidden"] = patch.get("hidden")
            else:
                p2 = {k: v for k, v in patch.items() if k in fixture_patch_keys}

            f2 = _deep_merge(f, p2)
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

    local_start = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 0, 0, 0))
    local_end = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 23, 59, 59))

    utc_start = local_start.astimezone(timezone.utc)
    utc_end = local_end.astimezone(timezone.utc)

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

    use_mls = _match_live_state_available()
    red_detail_sql = "('Red Card','Second Yellow card','Second Yellow Card')"

    if use_mls:
        home_red_sql = f"""
            CASE
                WHEN m.status_group = 'INPLAY' THEN COALESCE(mls.home_red, 0)
                ELSE (
                    SELECT COUNT(*) FROM match_events e
                    WHERE e.fixture_id = m.fixture_id
                      AND e.team_id = m.home_id
                      AND e.type = 'Card'
                      AND e.detail IN {red_detail_sql}
                )
            END AS home_red_cards
        """
        away_red_sql = f"""
            CASE
                WHEN m.status_group = 'INPLAY' THEN COALESCE(mls.away_red, 0)
                ELSE (
                    SELECT COUNT(*) FROM match_events e
                    WHERE e.fixture_id = m.fixture_id
                      AND e.team_id = m.away_id
                      AND e.type = 'Card'
                      AND e.detail IN {red_detail_sql}
                )
            END AS away_red_cards
        """
        mls_join = "LEFT JOIN match_live_state mls ON mls.fixture_id = m.fixture_id"
    else:
        home_red_sql = f"""
            (
                SELECT COUNT(*) FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id = m.home_id
                  AND e.type = 'Card'
                  AND e.detail IN {red_detail_sql}
            ) AS home_red_cards
        """
        away_red_sql = f"""
            (
                SELECT COUNT(*) FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id = m.away_id
                  AND e.type = 'Card'
                  AND e.detail IN {red_detail_sql}
            ) AS away_red_cards
        """
        mls_join = ""

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
            {home_red_sql},
            {away_red_sql}
        FROM matches m
        JOIN teams th ON th.id = m.home_id
        JOIN teams ta ON ta.id = m.away_id
        JOIN leagues l ON l.id = m.league_id
        {mls_join}
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
            "use_mls": use_mls,
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
    """
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
    local_next_day_start = local_start + timedelta(days=1)

    utc_start = local_start.astimezone(timezone.utc)
    utc_end = local_next_day_start.astimezone(timezone.utc)

    params: List[Any] = [utc_start, utc_end]
    where_clauses = ["m.date_utc::timestamptz >= %s AND m.date_utc::timestamptz < %s"]

    if league_ids:
        placeholders = ", ".join(["%s"] * len(league_ids))
        where_clauses.append(f"m.league_id IN ({placeholders})")
        params.extend(league_ids)
    elif league_id is not None and league_id > 0:
        where_clauses.append("m.league_id = %s")
        params.append(league_id)

    where_sql = " AND ".join(where_clauses)

    use_mls = _match_live_state_available()
    red_detail_sql = "('Red Card','Second Yellow card','Second Yellow Card')"

    if use_mls:
        home_red_sql = f"""
            COALESCE(
                mls.home_red,
                (
                    SELECT COUNT(*) FROM match_events e
                    WHERE e.fixture_id = m.fixture_id
                      AND e.team_id = m.home_id
                      AND e.type = 'Card'
                      AND e.detail IN {red_detail_sql}
                )
            ) AS home_red_cards
        """
        away_red_sql = f"""
            COALESCE(
                mls.away_red,
                (
                    SELECT COUNT(*) FROM match_events e
                    WHERE e.fixture_id = m.fixture_id
                      AND e.team_id = m.away_id
                      AND e.type = 'Card'
                      AND e.detail IN {red_detail_sql}
                )
            ) AS away_red_cards
        """
        mls_join = "LEFT JOIN match_live_state mls ON mls.fixture_id = m.fixture_id"
    else:
        home_red_sql = f"""
            (
                SELECT COUNT(*) FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id = m.home_id
                  AND e.type = 'Card'
                  AND e.detail IN {red_detail_sql}
            ) AS home_red_cards
        """
        away_red_sql = f"""
            (
                SELECT COUNT(*) FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id = m.away_id
                  AND e.type = 'Card'
                  AND e.detail IN {red_detail_sql}
            ) AS away_red_cards
        """
        mls_join = ""

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
            c.flag AS league_country_flag,
            (rf.data_json::jsonb->'score'->'extratime'->>'home') AS home_et,
            (rf.data_json::jsonb->'score'->'extratime'->>'away') AS away_et,
            (rf.data_json::jsonb->'score'->'penalty'->>'home') AS home_pen,
            (rf.data_json::jsonb->'score'->'penalty'->>'away') AS away_pen,
            {home_red_sql},
            {away_red_sql}
        FROM matches m
        JOIN teams th ON th.id = m.home_id
        JOIN teams ta ON ta.id = m.away_id
        JOIN leagues l ON l.id = m.league_id
        LEFT JOIN countries c
          ON LOWER(TRIM(c.name)) = LOWER(TRIM(l.country))
        LEFT JOIN match_fixtures_raw rf ON rf.fixture_id = m.fixture_id
        {mls_join}
        WHERE {where_sql}
        ORDER BY m.date_utc::timestamptz ASC
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
            "league_country_flag": r["league_country_flag"],
            "league_round": r["league_round"],
            "venue_name": r["venue_name"],
            "home": {
                "id": r["home_id"],
                "name": r["home_name"],
                "logo": r["home_logo"],
                "ft": r["home_ft"],
                "ht": r["home_ht"],
                "et": int(r["home_et"]) if r.get("home_et") not in (None, "") else None,
                "pen": int(r["home_pen"]) if r.get("home_pen") not in (None, "") else None,
                "red_cards": r["home_red_cards"],
            },
            "away": {
                "id": r["away_id"],
                "name": r["away_name"],
                "logo": r["away_logo"],
                "ft": r["away_ft"],
                "ht": r["away_ht"],
                "et": int(r["away_et"]) if r.get("away_et") not in (None, "") else None,
                "pen": int(r["away_pen"]) if r.get("away_pen") not in (None, "") else None,
                "red_cards": r["away_red_cards"],
            },
        })

    fixture_ids = [f["fixture_id"] for f in fixtures]
    override_map = _load_match_overrides(fixture_ids)

    # ✅ 리스트 API에서는 큰 블록이 붙지 않게 필요한 키만 허용
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
            if isinstance(patch.get("header"), dict):
                p2 = dict(patch.get("header") or {})
                if "hidden" in patch:
                    p2["hidden"] = patch.get("hidden")
            else:
                # ✅ 전체 patch merge 금지: allowed keys만 merge
                p2 = {k: v for k, v in patch.items() if k in fixture_patch_keys}

            f2 = _deep_merge(f, p2)

            if f2.get("hidden") is True:
                continue

            merged.append(f2)
        else:
            merged.append(f)

    return jsonify({"ok": True, "rows": merged})




# ─────────────────────────────────────
# Board APIs (public)
# ─────────────────────────────────────

def _lang_base(lang: str) -> str:
    s = (lang or "").strip()
    if not s:
        return ""
    s = s.replace("_", "-")
    return (s.split("-")[0] or "").lower().strip()

def _country_up(country: str) -> str:
    return (country or "").strip().upper()

def _arr_nonempty(arr) -> bool:
    return isinstance(arr, list) and len(arr) > 0

def _parse_text_array_csv(v: str) -> List[str]:
    # "KR, JP ,us" -> ["KR","JP","US"] (upper)
    s = (v or "").strip()
    if not s:
        return []
    out = []
    for x in s.split(","):
        t = x.strip()
        if t:
            out.append(t)
    return out

@app.get("/api/board/feed")
def board_feed():
    """
    Public feed (언어권 전용):
      /api/board/feed?lang=ko-KR&limit=20&offset=0&category=...&sport=...&fixture_key=...
    - lang 파라미터가 없으면 Accept-Language 헤더에서 첫 언어를 사용
    - target_langs 비어있으면 전세계 노출
    - lang이 비어있으면(파싱 실패) target_langs가 비어있는 글만 노출
    """
    # 1) lang: query 우선, 없으면 Accept-Language 첫 토큰 사용
    q_lang = (request.args.get("lang", "") or "").strip()
    if not q_lang:
        al = (request.headers.get("Accept-Language") or "").strip()
        # 예: "en-US,en;q=0.9,ko;q=0.8" -> "en-US"
        q_lang = (al.split(",")[0].strip() if al else "")
    lang = _lang_base(q_lang)

    category = (request.args.get("category") or "").strip()
    sport = (request.args.get("sport") or "").strip()
    fixture_key = (request.args.get("fixture_key") or "").strip()

    limit = int(request.args.get("limit", "20") or "20")
    offset = int(request.args.get("offset", "0") or "0")
    limit = max(1, min(limit, 50))
    offset = max(0, offset)

    lang_missing = (lang == "")

    where = ["status='published'"]

    # optional filters
    if category:
        where.append("category = %(category)s")
    if sport:
        where.append("sport = %(sport)s")
    if fixture_key:
        where.append("fixture_key = %(fixture_key)s")

    # ✅ 언어권 필터만 적용
    # - 레거시로 target_langs='{}'(빈 배열)로 저장된 글도 전세계 노출로 취급해야 함
    if lang_missing:
        # lang이 없으면 target_langs 지정 글은 숨기고, 전세계 글만 노출
        where.append("(array_length(target_langs, 1) IS NULL OR array_length(target_langs, 1) = 0)")
    else:
        where.append("""(
            array_length(target_langs, 1) IS NULL
            OR array_length(target_langs, 1) = 0
            OR %(lang)s = ANY (SELECT LOWER(x) FROM unnest(target_langs) x)
        )""")

    where_sql = " AND ".join(where)

    # pin 만료되면 정렬에서만 pin_level=0 취급(글은 계속 노출)
    sql = f"""
    SELECT
      id, sport, fixture_key, category, title, summary, status,
      pin_level, pin_until, publish_at, created_at,
      target_langs,
      CASE
        WHEN pin_level > 0 AND (pin_until IS NULL OR pin_until > NOW()) THEN pin_level
        ELSE 0
      END AS effective_pin
    FROM board_posts
    WHERE {where_sql}
    ORDER BY effective_pin DESC, publish_at DESC NULLS LAST, created_at DESC
    LIMIT %(limit)s OFFSET %(offset)s
    """

    params = {
        "lang": lang,
        "category": category,
        "sport": sport,
        "fixture_key": fixture_key,
        "limit": limit,
        "offset": offset,
    }

    try:
        conn = _board_connect()
        try:
            # psycopg3
            if psycopg is not None:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, params)
                        rows = cur.fetchall()
            else:
                # psycopg2: dict cursor 사용
                with conn:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute(sql, params)
                        rows = cur.fetchall()
        finally:
            try:
                conn.close()
            except Exception:
                pass

        return jsonify({"ok": True, "lang": lang, "rows": rows})
    except Exception as e:
        # ✅ log 미정의 이슈 회피: print로만 남김(전역 exception handler도 있음)
        print(f"[/api/board/feed] failed: {e}", file=sys.stderr)
        return jsonify({"ok": False, "error": str(e)}), 500




@app.get("/api/board/posts/<int:post_id>")
def board_post_detail(post_id: int):
    # lang: query 우선, 없으면 Accept-Language 첫 토큰
    q_lang = (request.args.get("lang", "") or "").strip()
    if not q_lang:
        al = (request.headers.get("Accept-Language") or "").strip()
        q_lang = (al.split(",")[0].strip() if al else "")
    lang = _lang_base(q_lang)

    lang_missing = (lang == "")

    where = ["status='published'", "id=%(id)s"]

    # ✅ 언어권만 적용
    # - 레거시로 target_langs='{}'(빈 배열)로 저장된 글도 전세계 노출로 취급해야 함
    if lang_missing:
        where.append("(array_length(target_langs, 1) IS NULL OR array_length(target_langs, 1) = 0)")
    else:
        where.append("""(
            array_length(target_langs, 1) IS NULL
            OR array_length(target_langs, 1) = 0
            OR %(lang)s = ANY (SELECT LOWER(x) FROM unnest(target_langs) x)
        )""")

    where_sql = " AND ".join(where)

    sql = f"""
    SELECT
      id, sport, fixture_key, category, title, summary, content_md, status,
      pin_level, pin_until, filters_json, snapshot_json,
      publish_at, created_at, updated_at,
      target_langs
    FROM board_posts
    WHERE {where_sql}
    LIMIT 1
    """

    try:
        conn = _board_connect()
        try:
            if psycopg is not None:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, {"id": post_id, "lang": lang})
                        row = cur.fetchone()
            else:
                with conn:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute(sql, {"id": post_id, "lang": lang})
                        row = cur.fetchone()
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if not row:
            return jsonify({"ok": False, "error": "not_found"}), 404
        return jsonify({"ok": True, "lang": lang, "row": row})
    except Exception as e:
        print(f"[/api/board/posts/<id>] failed: {e}", file=sys.stderr)
        return jsonify({"ok": False, "error": str(e)}), 500



@app.get(f"/{ADMIN_PATH}/api/fixture_meta")
@require_admin
def admin_fixture_meta():
    """
    A안: admin UI에서 analysis 글 작성 시, (sport, fixture_key)로 팀/리그 메타를 받아
    snapshot_json에 자동 저장하기 위한 최소 메타 API

    GET /{ADMIN_PATH}/api/fixture_meta?sport=football&fixture_key=1379184
    """
    sport = (request.args.get("sport") or "").strip().lower()
    fixture_key = (request.args.get("fixture_key") or "").strip()

    if not sport or not fixture_key:
        return jsonify({"ok": False, "error": "sport_and_fixture_key_required"}), 400

    # 현재는 football만 지원 (hockey는 필요 시 같은 방식으로 확장)
    if sport != "football":
        return jsonify({"ok": False, "error": "sport_not_supported_yet"}), 400

    try:
        fixture_id = int(fixture_key)
    except Exception:
        return jsonify({"ok": False, "error": "fixture_key_must_be_int_for_football"}), 400

    sql = """
    SELECT
      m.fixture_id,
      m.league_id,
      m.season,
      m.date_utc,

      ht.id   AS home_team_id,
      ht.name AS home_name,
      ht.logo AS home_logo,

      at.id   AS away_team_id,
      at.name AS away_name,
      at.logo AS away_logo,

      l.name  AS league_name,
      l.logo  AS league_logo
    FROM matches m
    JOIN teams ht ON ht.id = m.home_id
    JOIN teams at ON at.id = m.away_id
    LEFT JOIN leagues l ON l.id = m.league_id
    WHERE m.fixture_id = %s
    LIMIT 1
    """

    row = fetch_one(sql, (fixture_id,))
    if not row:
        return jsonify({"ok": False, "error": "not_found"}), 404

    def _to_iso(v):
        if v is None:
            return None
        if isinstance(v, str):
            return v
        try:
            # timestamptz -> UTC ISO
            vv = v
            if getattr(vv, "tzinfo", None) is None:
                vv = vv.replace(tzinfo=timezone.utc)
            return vv.astimezone(timezone.utc).isoformat()
        except Exception:
            return str(v)

    fixture = {
        "league": {
            "id": row.get("league_id"),
            "name": row.get("league_name") or "",
            "logo": row.get("league_logo") or "",
        },
        "season": row.get("season"),
        "kickoff_utc": _to_iso(row.get("date_utc")),
        "home": {
            "id": row.get("home_team_id"),
            "name": row.get("home_name") or "",
            "logo": row.get("home_logo") or "",
        },
        "away": {
            "id": row.get("away_team_id"),
            "name": row.get("away_name") or "",
            "logo": row.get("away_logo") or "",
        },
    }

    return jsonify({"ok": True, "fixture": fixture})


@app.route(f"/{ADMIN_PATH}/api/match_snapshot", methods=["GET", "POST"])
@require_admin
def admin_match_snapshot():
    """
    Admin UI에서 analysis 글 작성 시,
    (sport, fixture_key) 기반으로 insights_overall / ai_predictions 블록을 생성해 내려준다.

    ✅ 지원:
    - POST JSON (프론트 최신): { sport, fixture_key, filters: { scope:"league", last_n:"3|5|7|10|season_current|season_prev", league_id, season, comp } }
    - GET query (레거시): ?sport=football&fixture_key=...&comp=All&last_n=10

    ✅ 핵심 정책:
    - scope는 무조건 "league"로 강제 (선택한 경기 리그 기준)
    - last_n: 3/5/7/10 또는 season_current/season_prev 지원
      * season_current: 해당 경기 season으로 계산
      * season_prev: (해당 경기 season - 1)로 계산
    """
    # ── 1) 입력 파싱: POST 우선, 없으면 GET
    body = request.get_json(silent=True) if request.method == "POST" else None
    if not isinstance(body, dict):
        body = {}

    if request.method == "POST":
        sport = (body.get("sport") or "").strip().lower()
        fixture_key = (body.get("fixture_key") or "").strip()
        filters_in = body.get("filters") or {}
    else:
        sport = (request.args.get("sport") or "").strip().lower()
        fixture_key = (request.args.get("fixture_key") or "").strip()
        filters_in = {}

    if not sport or not fixture_key:
        return jsonify({"ok": False, "error": "sport_and_fixture_key_required"}), 400

    if sport != "football":
        return jsonify({"ok": False, "error": "sport_not_supported_yet"}), 400

    try:
        fixture_id = int(fixture_key)
    except Exception:
        return jsonify({"ok": False, "error": "fixture_key_must_be_int_for_football"}), 400

    if not isinstance(filters_in, dict):
        filters_in = {}

    # ── 2) matches에서 기본 메타 확보 (league_id/season/home/away)
    sql = """
    SELECT
      m.fixture_id,
      m.league_id,
      m.season,
      m.date_utc,
      m.home_id,
      m.away_id
    FROM matches m
    WHERE m.fixture_id = %s
    LIMIT 1
    """
    row = fetch_one(sql, (fixture_id,))
    if not row:
        return jsonify({"ok": False, "error": "not_found"}), 404

    def _to_iso(v):
        if v is None:
            return None
        if isinstance(v, str):
            return v
        try:
            vv = v
            if getattr(vv, "tzinfo", None) is None:
                vv = vv.replace(tzinfo=timezone.utc)
            return vv.astimezone(timezone.utc).isoformat()
        except Exception:
            return str(v)

    base_league_id = int(row.get("league_id") or 0)
    base_season = int(row.get("season") or 0)

    # ── 3) filters 정규화: 리그 기준 강제 + last_n(숫자/시즌모드) 처리
    # comp
    if request.method == "POST":
        comp = str(filters_in.get("comp") or "All").strip() or "All"
    else:
        comp = (request.args.get("comp") or "All").strip() or "All"

    # scope: 무조건 league 강제
    scope = "league"

    # league_id/season: filters가 오면 우선, 없으면 match 기준
    try:
        league_id = int(filters_in.get("league_id")) if filters_in.get("league_id") is not None else base_league_id
    except Exception:
        league_id = base_league_id

    # last_n raw (POST는 filters, GET은 query)
    if request.method == "POST":
        last_n_raw = filters_in.get("last_n")
    else:
        last_n_raw = request.args.get("last_n")

    last_n_mode = ""
    last_n_val: int | None = None

    # season_current / season_prev / n3/n5... / "3"
    s = (str(last_n_raw).strip() if last_n_raw is not None else "")
    s_low = s.lower()

    if s_low in ("season_current", "current_season", "this_season"):
        last_n_mode = "season_current"
    elif s_low in ("season_prev", "prev_season", "previous_season", "last_season"):
        last_n_mode = "season_prev"
    else:
        # "n5" 같은 형태도 허용
        if s_low.startswith("n") and s_low[1:].isdigit():
            s_low = s_low[1:]
        try:
            v = int(s_low) if s_low else 10
        except Exception:
            v = 10
        v = max(3, min(v, 50))
        last_n_val = v

    # season 결정:
    # - filters.season 있으면 우선
    # - 없으면 base_season
    # - last_n_mode가 season_prev면 season-1 적용
    try:
        season_in = int(filters_in.get("season")) if filters_in.get("season") is not None else base_season
    except Exception:
        season_in = base_season

    if last_n_mode == "season_prev":
        season = max(0, season_in - 1)
    else:
        season = season_in

    # header.filters.last_n 형태:
    # - 숫자 모드: int
    # - 시즌 모드: "season_current" / "season_prev" 문자열 그대로 전달 (블록 쪽에서 해석 가능하게)
    header_last_n: Any
    if last_n_mode:
        header_last_n = last_n_mode
    else:
        header_last_n = int(last_n_val or 10)

    header = {
        "fixture_id": int(row.get("fixture_id") or fixture_id),
        "league_id": int(league_id or 0),
        "season": int(season or 0),
        "kickoff_utc": _to_iso(row.get("date_utc")),
        "home": {"id": int(row.get("home_id") or 0)},
        "away": {"id": int(row.get("away_id") or 0)},
        "filters": {
            "scope": scope,
            "comp": comp,
            "last_n": header_last_n,
        },
    }

    # ── 4) 블록 생성 (지연 import)
    try:
        from matchdetail.insights_block import build_insights_overall_block
        from matchdetail.ai_predictions_block import build_ai_predictions_block
    except Exception as e:
        _admin_log(
            event_type="match_snapshot_import_fail",
            ok=False,
            status_code=500,
            fixture_id=fixture_id,
            detail={"error": str(e)},
        )
        return jsonify({"ok": False, "error": f"import_failed: {e}"}), 500

    try:
        insights_overall = build_insights_overall_block(header)
        ai_predictions = build_ai_predictions_block(header, insights_overall)

        _admin_log(
            event_type="match_snapshot_ok",
            ok=True,
            status_code=200,
            fixture_id=fixture_id,
            detail={
                "scope": scope,
                "comp": comp,
                "last_n": header_last_n,
                "league_id": header.get("league_id"),
                "season": header.get("season"),
                "method": request.method,
            },
        )
        return jsonify(
            {
                "ok": True,
                "blocks": {
                    "insights_overall": insights_overall,
                    "ai_predictions": ai_predictions,
                },
            }
        )
    except Exception as e:
        _admin_log(
            event_type="match_snapshot_fail",
            ok=False,
            status_code=500,
            fixture_id=fixture_id,
            detail={"error": str(e)},
        )
        raise




# ─────────────────────────────────────
# Board APIs (admin)
# ─────────────────────────────────────

@app.get(f"/{ADMIN_PATH}/api/board/posts")
@require_admin
def admin_board_list_posts():
    status = (request.args.get("status") or "").strip()  # draft/published/hidden
    q = (request.args.get("q") or "").strip()
    category = (request.args.get("category") or "").strip()
    sport = (request.args.get("sport") or "").strip()
    fixture_key = (request.args.get("fixture_key") or "").strip()

    limit = int(request.args.get("limit", "50") or "50")
    offset = int(request.args.get("offset", "0") or "0")
    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    where = ["1=1"]
    params = {"limit": limit, "offset": offset}

    if status:
        where.append("status=%(status)s")
        params["status"] = status
    if category:
        where.append("category=%(category)s")
        params["category"] = category
    if sport:
        where.append("sport=%(sport)s")
        params["sport"] = sport
    if fixture_key:
        where.append("fixture_key=%(fixture_key)s")
        params["fixture_key"] = fixture_key
    if q:
        where.append("(title ILIKE %(q)s OR summary ILIKE %(q)s)")
        params["q"] = f"%{q}%"

    where_sql = " AND ".join(where)

    # ✅ 선택A: country 컬럼은 admin에서도 끊는다(언어권만 관리)
    sql = f"""
    SELECT
      id, sport, fixture_key, category, title, summary, status,
      pin_level, pin_until, publish_at, created_at, updated_at,
      target_langs
    FROM board_posts
    WHERE {where_sql}
    ORDER BY updated_at DESC
    LIMIT %(limit)s OFFSET %(offset)s
    """

    try:
        conn = _board_connect()
        try:
            if psycopg is not None:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, params)
                        rows = cur.fetchall()
            else:
                with conn:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute(sql, params)
                        rows = cur.fetchall()
        finally:
            try:
                conn.close()
            except Exception:
                pass

        return jsonify({"ok": True, "rows": rows})
    except Exception as e:
        print(f"[admin board list] failed: {e}", file=sys.stderr)
        return jsonify({"ok": False, "error": str(e)}), 500



@app.get(f"/{ADMIN_PATH}/api/board/posts/<int:post_id>")
@require_admin
def admin_board_get_post(post_id: int):
    sql = """
    SELECT *
    FROM board_posts
    WHERE id=%(id)s
    LIMIT 1
    """
    try:
        conn = _board_connect()
        try:
            if psycopg is not None:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, {"id": post_id})
                        row = cur.fetchone()
            else:
                with conn:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute(sql, {"id": post_id})
                        row = cur.fetchone()
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if not row:
            return jsonify({"ok": False, "error": "not_found"}), 404
        return jsonify({"ok": True, "row": row})
    except Exception as e:
        print(f"[admin board get] failed: {e}", file=sys.stderr)
        return jsonify({"ok": False, "error": str(e)}), 500



@app.post(f"/{ADMIN_PATH}/api/board/posts")
@require_admin
def admin_board_create_post():
    body = request.get_json(force=True) or {}

    def arr_lower(xs):
        xs = xs or []
        out = []
        for x in xs:
            b = _lang_base(str(x))
            if b:
                out.append(b)
        # 중복 제거(순서 유지)
        seen = set()
        uniq = []
        for v in out:
            if v in seen:
                continue
            seen.add(v)
            uniq.append(v)
        # ✅ 비어있으면 빈 배열로 저장(전세계 노출, NOT NULL 만족)
        return uniq  # uniq는 비어있으면 []

    filters_obj = body.get("filters_json") or {}
    snapshot_obj = body.get("snapshot_json") or {}

    row = {
        "sport": (body.get("sport") or None),
        "fixture_key": (body.get("fixture_key") or None),
        "category": (body.get("category") or "analysis"),
        "title": (body.get("title") or "").strip(),
        "summary": (body.get("summary") or "").strip(),
        "content_md": (body.get("content_md") or "").strip(),
        "status": (body.get("status") or "draft"),
        "pin_level": int(body.get("pin_level") or 0),
        "pin_until": body.get("pin_until") or None,
        "filters_json": json.dumps(filters_obj, ensure_ascii=False),
        "snapshot_json": json.dumps(snapshot_obj, ensure_ascii=False),
        "publish_at": body.get("publish_at") or None,
        "target_langs": arr_lower(body.get("target_langs")),
    }

    if not row["title"]:
        return jsonify({"ok": False, "error": "title_required"}), 400
    if not row["content_md"]:
        return jsonify({"ok": False, "error": "content_required"}), 400

    sql = """
    INSERT INTO board_posts
      (sport, fixture_key, category, title, summary, content_md, status,
       pin_level, pin_until, filters_json, snapshot_json, publish_at,
       target_langs)
    VALUES
      (%(sport)s, %(fixture_key)s, %(category)s, %(title)s, %(summary)s, %(content_md)s, %(status)s,
       %(pin_level)s, %(pin_until)s, %(filters_json)s::jsonb, %(snapshot_json)s::jsonb, %(publish_at)s,
       %(target_langs)s)
    RETURNING id
    """

    try:
        conn = _board_connect()
        try:
            if psycopg is not None:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, row)
                        r = cur.fetchone()
                        new_id = (r.get("id") if isinstance(r, dict) else (r[0] if r else None))
            else:
                with conn:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute(sql, row)
                        r = cur.fetchone()
                        new_id = (r.get("id") if isinstance(r, dict) else None)
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if not new_id:
            return jsonify({"ok": False, "error": "insert_failed"}), 500
        return jsonify({"ok": True, "id": int(new_id)})
    except Exception as e:
        print(f"[admin board create] failed: {e}", file=sys.stderr)
        return jsonify({"ok": False, "error": str(e)}), 500







@app.put(f"/{ADMIN_PATH}/api/board/posts/<int:post_id>")
@require_admin
def admin_board_update_post(post_id: int):
    body = request.get_json(force=True) or {}

    def arr_lower(xs):
        xs = xs or []
        out = []
        for x in xs:
            b = _lang_base(str(x))
            if b:
                out.append(b)
        seen = set()
        uniq = []
        for v in out:
            if v in seen:
                continue
            seen.add(v)
            uniq.append(v)
        # ✅ 비어있으면 빈 배열로 저장(전세계 노출, NOT NULL 만족)
        return uniq  # uniq는 비어있으면 []

    filters_obj = body.get("filters_json") or {}
    snapshot_obj = body.get("snapshot_json") or {}

    row = {
        "id": post_id,
        "sport": (body.get("sport") or None),
        "fixture_key": (body.get("fixture_key") or None),
        "category": (body.get("category") or "analysis"),
        "title": (body.get("title") or "").strip(),
        "summary": (body.get("summary") or "").strip(),
        "content_md": (body.get("content_md") or "").strip(),
        "status": (body.get("status") or "draft"),
        "pin_level": int(body.get("pin_level") or 0),
        "pin_until": body.get("pin_until") or None,
        "filters_json": json.dumps(filters_obj, ensure_ascii=False),
        "snapshot_json": json.dumps(snapshot_obj, ensure_ascii=False),
        "publish_at": body.get("publish_at") or None,
        "target_langs": arr_lower(body.get("target_langs")),
    }

    if not row["title"]:
        return jsonify({"ok": False, "error": "title_required"}), 400
    if not row["content_md"]:
        return jsonify({"ok": False, "error": "content_required"}), 400

    sql = """
    UPDATE board_posts
    SET
      sport=%(sport)s,
      fixture_key=%(fixture_key)s,
      category=%(category)s,
      title=%(title)s,
      summary=%(summary)s,
      content_md=%(content_md)s,
      status=%(status)s,
      pin_level=%(pin_level)s,
      pin_until=%(pin_until)s,
      filters_json=%(filters_json)s::jsonb,
      snapshot_json=%(snapshot_json)s::jsonb,
      publish_at=%(publish_at)s,
      target_langs=%(target_langs)s
    WHERE id=%(id)s
    """

    try:
        conn = _board_connect()
        try:
            if psycopg is not None:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, row)
            else:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, row)
        finally:
            try:
                conn.close()
            except Exception:
                pass

        return jsonify({"ok": True})
    except Exception as e:
        print(f"[admin board update] failed: {e}", file=sys.stderr)
        return jsonify({"ok": False, "error": str(e)}), 500






@app.delete(f"/{ADMIN_PATH}/api/board/posts/<int:post_id>")
@require_admin
def admin_board_delete_post(post_id: int):
    try:
        conn = _board_connect()
        try:
            if psycopg is not None:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM board_posts WHERE id=%(id)s", {"id": post_id})
            else:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM board_posts WHERE id=%(id)s", {"id": post_id})
        finally:
            try:
                conn.close()
            except Exception:
                pass

        return jsonify({"ok": True})
    except Exception as e:
        print(f"[admin board delete] failed: {e}", file=sys.stderr)
        return jsonify({"ok": False, "error": str(e)}), 500



# ─────────────────────────────────────────
# 실행
# ─────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)




