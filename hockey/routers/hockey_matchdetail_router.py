# hockey/routers/hockey_matchdetail_router.py
from __future__ import annotations

import os
import json
from typing import Any, Dict, Optional, Tuple

from flask import Blueprint, jsonify, request

from hockey.services.hockey_game_detail_service import hockey_get_game_detail

hockey_matchdetail_bp = Blueprint("hockey_matchdetail", __name__)

# ─────────────────────────────────────────────────────────────
# Hockey overrides storage (HOCKEY_DATABASE_URL)
#  - table: hockey_match_overrides(fixture_id BIGINT PRIMARY KEY, patch JSONB, updated_at TIMESTAMPTZ)
#  - patch에 events: [...] 포함 가능 (이벤트 전체 교체)
# ─────────────────────────────────────────────────────────────

HOCKEY_DATABASE_URL = (os.getenv("HOCKEY_DATABASE_URL") or "").strip()

_psycopg3 = None
_dict_row = None
_psycopg2 = None
_psycopg2_extras = None

try:
    import psycopg  # psycopg v3
    from psycopg.rows import dict_row as _dict_row
    _psycopg3 = psycopg
except Exception:
    psycopg = None  # type: ignore
    try:
        import psycopg2 as _psycopg2  # type: ignore
        import psycopg2.extras as _psycopg2_extras  # type: ignore
    except Exception:
        _psycopg2 = None
        _psycopg2_extras = None


_OVR_TABLE_READY = False


def _hockey_connect():
    if not HOCKEY_DATABASE_URL:
        raise RuntimeError("HOCKEY_DATABASE_URL is not set")
    if _psycopg3 is not None:
        return _psycopg3.connect(HOCKEY_DATABASE_URL, row_factory=_dict_row)
    if _psycopg2 is None:
        raise RuntimeError("psycopg/psycopg2 not available")
    conn = _psycopg2.connect(HOCKEY_DATABASE_URL)
    return conn


def _ensure_overrides_table() -> None:
    global _OVR_TABLE_READY
    if _OVR_TABLE_READY:
        return

    sql = """
    CREATE TABLE IF NOT EXISTS hockey_match_overrides (
      fixture_id   BIGINT PRIMARY KEY,
      patch        JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    conn = _hockey_connect()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        _OVR_TABLE_READY = True
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _hockey_fetch_one(sql: str, params: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    conn = _hockey_connect()
    try:
        if _psycopg3 is not None:
            cur = conn.cursor()
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None
        # psycopg2 fallback
        cur = conn.cursor(cursor_factory=_psycopg2_extras.RealDictCursor)  # type: ignore
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _load_hockey_override(fixture_id: int) -> Dict[str, Any]:
    _ensure_overrides_table()
    row = _hockey_fetch_one("SELECT patch FROM hockey_match_overrides WHERE fixture_id=%s", (fixture_id,))
    if not row:
        return {}
    patch = row.get("patch") or {}
    if isinstance(patch, str):
        try:
            patch = json.loads(patch)
        except Exception:
            patch = {}
    return patch if isinstance(patch, dict) else {}


def _deep_merge(base: Any, patch: Any) -> Any:
    if patch is None:
        return base
    if isinstance(base, dict) and isinstance(patch, dict):
        out = dict(base)
        for k, v in patch.items():
            if k in out:
                out[k] = _deep_merge(out[k], v)
            else:
                out[k] = v
        return out
    # list 등은 통째로 교체
    return patch


def _build_header(detail: Dict[str, Any]) -> Dict[str, Any]:
    """
    ✅ 축구 override UI 호환용 header(평평한 필드) 생성
    - public 응답에 header를 추가해도 앱은 무시 가능
    """
    g = (detail.get("game") or {}) if isinstance(detail.get("game"), dict) else {}
    league = (g.get("league") or {}) if isinstance(g.get("league"), dict) else {}
    home = (g.get("home") or {}) if isinstance(g.get("home"), dict) else {}
    away = (g.get("away") or {}) if isinstance(g.get("away"), dict) else {}

    # alias scores
    h = dict(home)
    a = dict(away)
    if "ft" not in h:
        h["ft"] = h.get("score")
    if "ft" not in a:
        a["ft"] = a.get("score")
    if "ht" not in h:
        h["ht"] = None
    if "ht" not in a:
        a["ht"] = None

    header = {
        "fixture_id": g.get("game_id"),
        "league_id": g.get("league_id") or league.get("id"),
        "season": g.get("season"),
        "date_utc": g.get("date_utc"),
        "status": g.get("status"),
        "status_long": g.get("status_long"),
        "status_group": None,  # fixtures에서만 쓰는 키 (여기선 굳이 계산 안함)
        "elapsed": None,
        "league_name": league.get("name"),
        "league_logo": league.get("logo"),
        "league_country": league.get("country"),
        "league_round": g.get("stage") or g.get("group_name"),
        "venue_name": None,
        "home": h,
        "away": a,
    }
    return header


def _apply_override(detail: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    if not patch:
        return detail

    out = dict(detail)

    # ✅ header 지원 (축구 override UI 호환)
    header = out.get("header")
    if not isinstance(header, dict):
        header = _build_header(out)
    if isinstance(patch.get("header"), dict):
        header = _deep_merge(header, patch.get("header"))
    else:
        # header에 들어갈만한 keys는 top-level에 남겨둘 수도 있어서, 여기선 건드리지 않음
        pass
    out["header"] = header

    # ✅ events/h2h/game 등은 patch가 있으면 그대로 merge (events는 list라 통째로 교체됨)
    out = _deep_merge(out, {k: v for k, v in patch.items() if k != "header"})
    return out


@hockey_matchdetail_bp.route("/api/hockey/games/<int:game_id>", methods=["GET"])
def hockey_game_detail(game_id: int):
    """
    ✅ Hockey game detail (public)
    - 기본 apply_override=1
    - apply_override=0 으로 raw 확인 가능
    """
    apply_override = request.args.get("apply_override", default=1, type=int)

    detail = hockey_get_game_detail(game_id=game_id)  # {"ok":True,"game":...,"events":...,"h2h":...}

    if not isinstance(detail, dict):
        return jsonify({"ok": False, "error": "invalid response"}), 500

    # ✅ header 항상 포함(디버깅/override UI용)
    if "header" not in detail:
        detail["header"] = _build_header(detail)

    if apply_override != 1:
        return jsonify(detail)

    patch = _load_hockey_override(game_id)
    merged = _apply_override(detail, patch)
    return jsonify(merged)


@hockey_matchdetail_bp.route("/api/hockey/match_detail_bundle", methods=["GET"])
def hockey_match_detail_bundle():
    """
    ✅ override UI에서 raw/merged 비교용
    - fixture_id(or game_id) 필수
    - apply_override=0/1 지원
    """
    fixture_id = request.args.get("fixture_id", type=int) or request.args.get("game_id", type=int)
    if not fixture_id:
        return jsonify({"ok": False, "error": "fixture_id is required"}), 400

    apply_override = request.args.get("apply_override", default=1, type=int)

    detail = hockey_get_game_detail(game_id=fixture_id)
    if not isinstance(detail, dict):
        return jsonify({"ok": False, "error": "invalid response"}), 500

    if "header" not in detail:
        detail["header"] = _build_header(detail)

    if apply_override != 1:
        return jsonify(detail)

    patch = _load_hockey_override(fixture_id)
    merged = _apply_override(detail, patch)
    return jsonify(merged)
