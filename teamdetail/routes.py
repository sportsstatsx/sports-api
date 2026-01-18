# src/teamdetail/routes.py

from __future__ import annotations

from flask import Blueprint, request, jsonify, current_app

from db import fetch_all
from teamdetail.bundle_service import get_team_detail_bundle

teamdetail_bp = Blueprint("teamdetail", __name__)


def _coalesce_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _fetch_one(query: str, params: tuple):
    rows = fetch_all(query, params)
    return rows[0] if rows else None


def _resolve_season_for_team_league(
    team_id: int,
    league_id: int,
    season: int | None,
) -> int | None:
    """
    ✅ 팀디테일 시즌 보정(완전무결)
    우선순위:
      0) season 파라미터가 유효하면 그대로 사용
      1) matches에서 (team_id, league_id) 기준 MAX(season)
      2) fixtures에서 league_id 기준 MAX(season)
      3) standings에서 league_id 기준 MAX(season)
    """

    # 0) season이 들어왔으면 "해당 팀+리그+시즌" 데이터 존재 여부 체크 후 사용
    if season is not None:
        row = _fetch_one(
            """
            SELECT 1 AS ok
            FROM matches
            WHERE league_id = %s
              AND season = %s
              AND (home_id = %s OR away_id = %s)
            LIMIT 1
            """,
            (league_id, season, team_id, team_id),
        )
        if row is not None:
            return season

    # 1) matches 기준 (팀+리그) 최신 시즌
    row = _fetch_one(
        """
        SELECT MAX(season) AS season
        FROM matches
        WHERE league_id = %s
          AND (home_id = %s OR away_id = %s)
        """,
        (league_id, team_id, team_id),
    )
    if row is not None:
        s = _coalesce_int(row.get("season"), 0)
        if s > 0:
            return s

    # 2) fixtures 기준 (리그) 최신 시즌
    row = _fetch_one(
        """
        SELECT MAX(season) AS season
        FROM fixtures
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if row is not None:
        s = _coalesce_int(row.get("season"), 0)
        if s > 0:
            return s

    # 3) standings 기준 (리그) 최신 시즌
    row = _fetch_one(
        """
        SELECT MAX(season) AS season
        FROM standings
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if row is not None:
        s = _coalesce_int(row.get("season"), 0)
        if s > 0:
            return s

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

        resolved_season = _resolve_season_for_team_league(
            team_id=team_id,
            league_id=league_id,
            season=season,
        )
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
