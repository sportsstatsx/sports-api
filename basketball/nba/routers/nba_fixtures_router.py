from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import List, Optional

import pytz
from flask import Blueprint, jsonify, request

from basketball.nba.services.nba_fixtures_service import nba_get_fixtures_by_utc_range


nba_fixtures_bp = Blueprint("nba_fixtures", __name__, url_prefix="/api/nba")


@nba_fixtures_bp.route("/fixtures")
def nba_list_fixtures():
    """
    정식 NBA 매치리스트 API

    Query:
      - date: YYYY-MM-DD (필수)
      - timezone: 예) Asia/Seoul (기본 UTC)
      - league: text (선택, 기본 'standard')
      - leagues: "standard,..." (선택) -> 있으면 league보다 우선
      - include_hidden: 0/1 (선택, 기본 0)  -> (현재 NBA override/hidden 테이블 없음: 무시)
    """
    league = request.args.get("league", default="standard", type=str)
    leagues_raw = request.args.get("leagues", type=str)

    leagues: List[str] = []
    if leagues_raw:
        for part in leagues_raw.split(","):
            part = (part or "").strip()
            if part:
                leagues.append(part)

    date_str = request.args.get("date", type=str)
    tz_str = request.args.get("timezone", "UTC")

    # 유지용 파라미터(현재 unused)
    _include_hidden = str(request.args.get("include_hidden", "0")).strip() == "1"

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

    # [local_start, next_day_start)
    local_start = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 0, 0, 0))
    local_next_day_start = local_start + timedelta(days=1)

    utc_start = local_start.astimezone(timezone.utc)
    utc_end = local_next_day_start.astimezone(timezone.utc)

    games = nba_get_fixtures_by_utc_range(
        utc_start=utc_start,
        utc_end=utc_end,
        leagues=leagues,
        league=league if not leagues else None,
    ) or []

    # ✅ league_logo fallback: DB(nba_leagues)에 로고가 없으면 서버 static 경로로 고정
    # - request.url_root 도 프록시 환경에서 http 로 잡힐 수 있어서 https 로 고정
    fallback_logo = f"https://{request.host}/static/nba/Basketball_Nba_League_logo.svg"


    for g in games:
        li = g.get("league_info") or {}
        if not li.get("logo"):
            li["logo"] = fallback_logo
            g["league_info"] = li

    return jsonify({"ok": True, "games": games})

