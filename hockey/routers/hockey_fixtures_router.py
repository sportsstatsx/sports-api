# hockey/routers/hockey_fixtures_router.py
from __future__ import annotations

import os
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pytz
from flask import Blueprint, jsonify, request

from hockey.services.hockey_fixtures_service import hockey_get_fixtures_by_utc_range

hockey_fixtures_bp = Blueprint("hockey_fixtures", __name__)

# ─────────────────────────────────────────────────────────────
# Hockey overrides storage (HOCKEY_DATABASE_URL)
#  - table: hockey_match_overrides(fixture_id BIGINT PRIMARY KEY, patch JSONB, updated_at TIMESTAMPTZ)
#  - patch에 hidden:true 포함 가능 (public fixtures에서 숨김 처리)
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


def _hockey_fetch_all(sql: str, params: Tuple[Any, ...]) -> List[Dict[str, Any]]:
    conn = _hockey_connect()
    try:
        if _psycopg3 is not None:
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            return list(rows)
        # psycopg2 fallback
        cur = conn.cursor(cursor_factory=_psycopg2_extras.RealDictCursor)  # type: ignore
        cur.execute(sql, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _load_hockey_overrides(fixture_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not fixture_ids:
        return {}

    _ensure_overrides_table()

    placeholders = ", ".join(["%s"] * len(fixture_ids))
    sql = f"SELECT fixture_id, patch FROM hockey_match_overrides WHERE fixture_id IN ({placeholders})"
    rows = _hockey_fetch_all(sql, tuple(fixture_ids))

    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        fid = int(r["fixture_id"])
        patch = r.get("patch") or {}
        if isinstance(patch, str):
            try:
                patch = json.loads(patch)
            except Exception:
                patch = {}
        if isinstance(patch, dict):
            out[fid] = patch
    return out


def _deep_merge(base: Any, patch: Any) -> Any:
    # list는 통째로 교체, dict는 재귀 merge
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
    return patch


def _status_group_from_status(status: str) -> str:
    s = (status or "").upper().strip()
    if s in {"NS", "TBD"}:
        return "NS"
    if s in {"FT", "AOT", "AP"}:
        return "FT"
    if s in {"P1", "P2", "P3", "OT", "SO", "BT"}:
        return "INPLAY"
    if s in {"PST", "CANC", "ABD", "SUSP"}:
        return "PST"
    return s or "?"


def _enrich_game_for_admin(g: Dict[str, Any]) -> Dict[str, Any]:
    """
    ✅ 기존 hockey fixtures 응답 구조는 유지하면서,
    축구 override UI가 기대하는 flat 필드(alias)를 추가한다.
    """
    league = (g.get("league") or {}) if isinstance(g.get("league"), dict) else {}
    home = (g.get("home") or {}) if isinstance(g.get("home"), dict) else {}
    away = (g.get("away") or {}) if isinstance(g.get("away"), dict) else {}

    # alias
    game_id = g.get("game_id")
    league_id = g.get("league_id") or league.get("id")

    # score alias (축구 UI 호환)
    if "ft" not in home:
        home["ft"] = home.get("score")
    if "ft" not in away:
        away["ft"] = away.get("score")
    if "ht" not in home:
        home["ht"] = None
    if "ht" not in away:
        away["ht"] = None

    g2 = dict(g)
    g2["fixture_id"] = game_id  # ✅ UI 호환 키
    g2["league_id"] = league_id
    g2["league_name"] = league.get("name")
    g2["league_logo"] = league.get("logo")
    g2["league_country"] = league.get("country")
    g2["league_round"] = g.get("stage") or g.get("group_name")  # 하키에 round 개념이 없어서 stage/group_name 활용
    g2["venue_name"] = None

    g2["status_group"] = _status_group_from_status(str(g.get("status") or ""))
    g2["elapsed"] = None

    g2["home"] = home
    g2["away"] = away
    return g2


@hockey_fixtures_bp.route("/api/hockey/fixtures", methods=["GET"])
def hockey_list_fixtures():
    """
    ✅ Hockey fixtures (public)
    - 기존 응답 유지 + 축구 override UI 호환 alias 필드 추가
    - 기본 apply_override=1 (hockey_match_overrides 적용 + hidden:true는 목록에서 제외)
    - 디버깅용 apply_override=0 지원
    """
    league_id = request.args.get("league_id", type=int)
    league_ids_raw = request.args.get("league_ids", type=str)
    apply_override = request.args.get("apply_override", default=1, type=int)

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
    local_end = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 23, 59, 59))

    utc_start = local_start.astimezone(timezone.utc)
    utc_end = local_end.astimezone(timezone.utc)

    games = hockey_get_fixtures_by_utc_range(
        utc_start=utc_start,
        utc_end=utc_end,
        league_id=league_id,
        league_ids=league_ids,
    )

    # ✅ UI 호환 alias 추가
    enriched: List[Dict[str, Any]] = []
    ids: List[int] = []
    for g in games:
        if not isinstance(g, dict):
            continue
        g2 = _enrich_game_for_admin(g)
        enriched.append(g2)
        if isinstance(g2.get("fixture_id"), (int, float, str)):
            try:
                ids.append(int(g2["fixture_id"]))
            except Exception:
                pass

    if apply_override != 1:
        return jsonify({"ok": True, "games": enriched})

    # ✅ overrides 적용
    ovr_map = _load_hockey_overrides(ids)

    fixture_patch_keys = {
        "fixture_id", "league_id", "season",
        "date_utc",
        "status_group", "status", "elapsed", "status_long",
        "league_round", "venue_name",
        "league_name", "league_logo", "league_country",
        "home", "away",
        "hidden",
    }

    merged: List[Dict[str, Any]] = []
    for g in enriched:
        fid = int(g.get("fixture_id") or 0) if str(g.get("fixture_id") or "").isdigit() else None
        patch = ovr_map.get(fid) if fid is not None else None

        if patch and isinstance(patch, dict):
            # hidden 처리 (public)
            if patch.get("hidden") is True:
                continue

            # header 우선 지원 (축구 override UI 호환)
            if isinstance(patch.get("header"), dict):
                p2 = dict(patch.get("header") or {})
                if "hidden" in patch:
                    p2["hidden"] = patch.get("hidden")
            else:
                p2 = {k: v for k, v in patch.items() if k in fixture_patch_keys}

            g2 = _deep_merge(g, p2)
            g2["_has_override"] = True
            merged.append(g2)
        else:
            g["_has_override"] = False
            merged.append(g)

    return jsonify({"ok": True, "games": merged})
