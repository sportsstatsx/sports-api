# hockey/routers/hockey_fixtures_router.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional

import pytz
from flask import Blueprint, jsonify, request

from hockey.services.hockey_fixtures_service import hockey_get_fixtures_by_utc_range


hockey_fixtures_bp = Blueprint("hockey_fixtures", __name__, url_prefix="/api/hockey")


@hockey_fixtures_bp.route("/fixtures")
def hockey_list_fixtures():
    """
    정식 하키 매치리스트 API

    Query:
      - date: YYYY-MM-DD (필수)
      - timezone: 예) Asia/Seoul (기본 UTC)
      - league_id: int (선택)
      - league_ids: "57,58,..." (선택)  -> 있으면 league_id보다 우선
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

    # local date range -> utc range
    local_start = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 0, 0, 0))
    local_end = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 23, 59, 59))

    utc_start = local_start.astimezone(timezone.utc)
    utc_end = local_end.astimezone(timezone.utc)

    rows = hockey_get_fixtures_by_utc_range(
        utc_start=utc_start,
        utc_end=utc_end,
        league_ids=league_ids,
        league_id=league_id,
    )

    return jsonify({"ok": True, "rows": rows})
