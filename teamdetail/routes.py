# src/teamdetail/routes.py

from __future__ import annotations

from flask import Blueprint, request, jsonify, current_app

from teamdetail.bundle_service import get_team_detail_bundle
from db import fetch_all

teamdetail_bp = Blueprint("teamdetail", __name__)


def _resolve_season_for_teamdetail(league_id: int, season: int | None) -> int | None:
    """
    팀디테일 전용 시즌 보정(캘린더-이어 리그 문제 해결용)

    우선순위:
    1) 요청 season이 실제로 matches에 존재하면 그대로 사용
    2) (가장 중요) league_id 기준 "가장 가까운 미래 경기"의 season
    3) 없으면 "가장 최근 경기"의 season
    4) 그래도 없으면 standings/fixtures의 MAX(season) (가능하면)
    """

    # 1) 요청 season 검증: 해당 시즌 데이터가 matches에 있으면 그대로
    if season is not None:
        try:
            rows = fetch_all(
                """
                SELECT 1
                FROM matches
                WHERE league_id = %s
                  AND season = %s
                LIMIT 1
                """,
                (league_id, season),
            )
            if rows:
                return int(season)
        except Exception:
            pass

    # 2) "가장 가까운 미래 경기"의 season 우선 (캘린더-이어 리그의 핵심)
    try:
        rows = fetch_all(
            """
            SELECT season
            FROM matches
            WHERE league_id = %s
              AND date_utc >= NOW()
            ORDER BY date_utc ASC
            LIMIT 1
            """,
            (league_id,),
        )
        if rows:
            s = rows[0].get("season")
            if s is not None:
                return int(s)
    except Exception:
        pass

    # 3) 미래 경기가 없으면 "가장 최근 경기" season
    try:
        rows = fetch_all(
            """
            SELECT season
            FROM matches
            WHERE league_id = %s
            ORDER BY date_utc DESC
            LIMIT 1
            """,
            (league_id,),
        )
        if rows:
            s = rows[0].get("season")
            if s is not None:
                return int(s)
    except Exception:
        pass

    # 4) 마지막 안전망: standings / fixtures MAX(season)
    try:
        rows = fetch_all(
            """
            SELECT MAX(season) AS season
            FROM standings
            WHERE league_id = %s
            """,
            (league_id,),
        )
        if rows:
            s = rows[0].get("season")
            if s is not None:
                si = int(s)
                if si > 0:
                    return si
    except Exception:
        pass

    try:
        rows = fetch_all(
            """
            SELECT MAX(season) AS season
            FROM fixtures
            WHERE league_id = %s
            """,
            (league_id,),
        )
        if rows:
            s = rows[0].get("season")
            if s is not None:
                si = int(s)
                if si > 0:
                    return si
    except Exception:
        pass

    return None


@teamdetail_bp.route("/api/team_detail_bundle", methods=["GET"])
def team_detail_bundle():
    """
    ✅ 완전무결 팀디테일 번들:
    - team_id, league_id 는 필수
    - season 은 optional (없거나/틀려도 서버가 DB 기준으로 보정)
    """
    try:
        team_id = request.args.get("team_id", type=int)
        league_id = request.args.get("league_id", type=int)
        season = request.args.get("season", type=int)  # optional

        if team_id is None or league_id is None:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "team_id, league_id 는 필수입니다.",
                    }
                ),
                400,
            )

        resolved_season = _resolve_season_for_teamdetail(league_id=league_id, season=season)
        if resolved_season is None:
            return jsonify({"ok": False, "error": "season_not_resolvable"}), 400

        bundle = get_team_detail_bundle(
            team_id=team_id,
            league_id=league_id,
            season=resolved_season,
        )

        return jsonify({"ok": True, "data": bundle})

    except Exception as e:  # noqa: BLE001
        current_app.logger.exception("team_detail_bundle error")
        return jsonify({"ok": False, "error": str(e)}), 500
