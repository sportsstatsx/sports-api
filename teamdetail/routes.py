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
    ✅ 팀디테일 시즌 보정(하이브리드 완전체)

    목표:
      - 앱이 season=이전시즌(예:2025)을 보내도,
        DB에 더 최신 시즌(예:2026)이 있으면 자동으로 최신 시즌을 사용.
      - season 파라미터는 "사용자 힌트"로만 취급.

    우선순위:
      1) matches에서 (team_id, league_id) 기준 MAX(season) = 최신 시즌 (가장 신뢰)
      2) season 파라미터가 있고, 그것이 최신 시즌보다 크거나(미래) 유효하면 그걸 사용
      3) fixtures MAX(season)
      4) standings MAX(season)
    """

    # 1) (팀+리그) 최신 시즌을 먼저 구한다
    max_row = _fetch_one(
        """
        SELECT MAX(season) AS season
        FROM matches
        WHERE league_id = %s
          AND (home_id = %s OR away_id = %s)
        """,
        (league_id, team_id, team_id),
    )
    max_season = _coalesce_int((max_row or {}).get("season"), 0)
    if max_season > 0:
        # ✅ 앱이 season=2025를 보내도 최신이 2026이면 2026으로 강제
        if season is None:
            return max_season

        req_season = _coalesce_int(season, 0)
        if req_season <= 0:
            return max_season

        # 요청 season이 최신보다 "크면"(사용자가 미래/특정 시즌을 의도) 존재하면 허용
        if req_season > max_season:
            ok = _fetch_one(
                """
                SELECT 1 AS ok
                FROM matches
                WHERE league_id = %s
                  AND season = %s
                  AND (home_id = %s OR away_id = %s)
                LIMIT 1
                """,
                (league_id, req_season, team_id, team_id),
            )
            return req_season if ok is not None else max_season

        # 요청 season이 최신보다 같거나 작으면 → 최신 시즌 우선
        return max_season

    # 2) matches에서 최신 시즌을 못 구한 경우에만, 요청 season을 유효성 체크 후 사용
    if season is not None:
        ok = _fetch_one(
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
        if ok is not None:
            return int(season)

    # 3) fixtures 기준 (리그) 최신 시즌
    row = _fetch_one(
        """
        SELECT MAX(season) AS season
        FROM fixtures
        WHERE league_id = %s
        """,
        (league_id,),
    )
    s = _coalesce_int((row or {}).get("season"), 0)
    if s > 0:
        return s

    # 4) standings 기준 (리그) 최신 시즌
    row = _fetch_one(
        """
        SELECT MAX(season) AS season
        FROM standings
        WHERE league_id = %s
        """,
        (league_id,),
    )
    s = _coalesce_int((row or {}).get("season"), 0)
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

        # ✅ 디버그 로그(한 번만 보고 지워도 됨)
        current_app.logger.info(
            "[team_detail_bundle] req team_id=%s league_id=%s season=%s -> resolved=%s",
            team_id, league_id, season, resolved_season
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

