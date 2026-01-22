from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, List, Optional

import pytz
from flask import Blueprint, jsonify, request

from hockey.services.hockey_fixtures_service import hockey_get_fixtures_by_utc_range


hockey_fixtures_bp = Blueprint("hockey_fixtures", __name__, url_prefix="/api/hockey")


@hockey_fixtures_bp.route("/fixtures")
def hockey_list_fixtures():
    """
    정식 하키 매치리스트 API (+ override/hidden 적용)

    Query:
      - date: YYYY-MM-DD (필수)
      - timezone: 예) Asia/Seoul (기본 UTC)
      - league_id: int (선택)
      - league_ids: "57,58,..." (선택)  -> 있으면 league_id보다 우선
      - include_hidden: 0/1 (선택, 기본 0)  -> hidden도 포함해서 보고 싶을 때(관리자/디버그)
    """
    # 리그 필터
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

    # 날짜/타임존
    date_str = request.args.get("date", type=str)
    tz_str = request.args.get("timezone", "UTC")
    include_hidden = str(request.args.get("include_hidden", "0")).strip() == "1"

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

    # ✅ 정식: [local_start, next_day_start)
    local_start = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 0, 0, 0))
    local_next_day_start = local_start + timedelta(days=1)

    utc_start = local_start.astimezone(timezone.utc)
    utc_end = local_next_day_start.astimezone(timezone.utc)

    games = hockey_get_fixtures_by_utc_range(
        utc_start=utc_start,
        utc_end=utc_end,
        league_ids=league_ids,
        league_id=league_id,
    ) or []

    # ── overrides 로드
    # DB: hockey_match_overrides(fixture_id PK) 이므로 fixture_id로 조회
    # 현재 하키 game_id를 fixture_id로 동일 매핑해서 사용한다.
    override_map: dict[int, dict] = {}
    try:
        from hockey.hockey_db import hockey_fetch_all
        ids = [int(g.get("game_id")) for g in games if g.get("game_id") is not None]
        ids = list(dict.fromkeys(ids))
        if ids:
            placeholders = ",".join(["%s"] * len(ids))
            rows = hockey_fetch_all(
                f"SELECT fixture_id, patch FROM hockey_match_overrides WHERE fixture_id IN ({placeholders})",
                tuple(ids),
            ) or []
            for r in rows:
                fid = r.get("fixture_id")
                p = r.get("patch")
                if isinstance(fid, int) and isinstance(p, dict):
                    override_map[fid] = p
    except Exception:
        override_map = {}

    # deep merge (간단)
    def _deep_merge_local(a: Any, b: Any) -> Any:
        if isinstance(a, dict) and isinstance(b, dict):
            out = dict(a)
            for k, v in b.items():
                out[k] = _deep_merge_local(out.get(k), v)
            return out
        return b

    out_games = []
    for g in games:
        gid = g.get("game_id")
        p = override_map.get(int(gid)) if gid is not None else None

        g2 = g
        hidden = False

        if isinstance(p, dict):
            # header 기반이면 header만 병합
            if isinstance(p.get("header"), dict):
                g2 = _deep_merge_local(g, p["header"])
                hidden = bool(p.get("hidden", False))
            else:
                g2 = _deep_merge_local(g, p)
                hidden = bool(p.get("hidden", False))

            g2["_has_override"] = True
        else:
            g2["_has_override"] = False

        if hidden and not include_hidden:
            continue

        if hidden:
            try:
                g2["hidden"] = True
            except Exception:
                pass

        out_games.append(g2)

    return jsonify({"ok": True, "games": out_games})


