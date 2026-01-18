# src/teamdetail/standing_block.py

from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple

from db import fetch_all


def _coalesce_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _fetch_one(query: str, params: tuple) -> Optional[Dict[str, Any]]:
    rows = fetch_all(query, params)
    return rows[0] if rows else None


def _resolve_season(league_id: int, season: Optional[int]) -> Optional[int]:
    """
    season이 없거나 0/None으로 들어오는 경우 방어:
      1) standings에서 MAX(season)
      2) 없으면 fixtures에서 MAX(season)
    """
    if season is not None and _coalesce_int(season, 0) > 0:
        return int(season)

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

    return None


def _cols_of(table_name: str) -> set[str]:
    try:
        cols = fetch_all(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            """,
            (table_name,),
        )
        return {str(r.get("column_name") or "") for r in cols if r.get("column_name")}
    except Exception:
        return set()


def _pick_pair(cols: set[str], pairs: List[Tuple[str, str]]) -> Optional[Tuple[str, str]]:
    for a, b in pairs:
        if a in cols and b in cols:
            return (a, b)
    return None


def build_standing_block(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    Team Detail - Standing(이 팀이 포함된 순위표) 블록 (하이브리드 완전체)

    ✅ 규칙:
    1) standings 테이블에 (league_id, season) rows가 있으면 그걸 우선 사용
    2) standings가 비어 있어도,
       - 해당 시즌에 "완료된 경기"가 1개라도 있으면(matches 기준)
       - 즉시 standings를 계산해서 내려준다
    3) 완료된 경기 자체가 0이면 table=[] (+ message)

    + 기존 규칙 유지:
      - 팀당 중복 row(스플릿 라운드 등)는 played 최대 row만 남김
      - group_name 여러 개인 리그는 "이 팀이 속한 group"의 테이블만 사용
    """

    team_id_i = _coalesce_int(team_id, 0)
    league_id_i = _coalesce_int(league_id, 0)
    season_i = _coalesce_int(season, 0)

    if league_id_i <= 0 or team_id_i <= 0:
        return {
            "league_id": league_id_i,
            "season": season_i if season_i > 0 else None,
            "team_id": team_id_i,
            "table": [],
            "message": "Standings are not available yet.\nPlease check back later.",
            "source": "none",
        }

    season_resolved = _resolve_season(league_id_i, season_i if season_i > 0 else None)
    if season_resolved is None:
        return {
            "league_id": league_id_i,
            "season": None,
            "team_id": team_id_i,
            "table": [],
            "message": "Standings are not available yet.\nPlease check back later.",
            "source": "none",
        }

    # ─────────────────────────────────────────────
    # 1) standings 테이블 우선
    # ─────────────────────────────────────────────
    try:
        rows_raw: List[Dict[str, Any]] = fetch_all(
            """
            SELECT
                s.rank,
                s.team_id,
                t.name       AS team_name,
                s.played,
                s.win        AS wins,
                s.draw       AS draws,
                s.lose       AS losses,
                s.goals_for,
                s.goals_against,
                s.goals_diff AS goal_diff,
                s.points,
                s.group_name
            FROM standings AS s
            JOIN teams     AS t ON t.id = s.team_id
            WHERE s.league_id = %s
              AND s.season    = %s
            ORDER BY s.rank ASC, t.name ASC
            """,
            (league_id_i, season_resolved),
        )
    except Exception:
        rows_raw = []

    source = "standings_table" if rows_raw else "computed_from_matches"

    # ─────────────────────────────────────────────
    # 2) standings가 비면 → matches에서 계산
    # ─────────────────────────────────────────────
    if not rows_raw:
        mcols = _cols_of("matches")

        team_pair = _pick_pair(
            mcols,
            [
                ("home_team_id", "away_team_id"),
                ("home_id", "away_id"),
            ],
        )
        goal_pair = _pick_pair(
            mcols,
            [
                ("home_goals", "away_goals"),
                ("home_ft", "away_ft"),
                ("goals_home", "goals_away"),
                ("home_score", "away_score"),
            ],
        )

        if not team_pair or not goal_pair:
            return {
                "league_id": league_id_i,
                "season": season_resolved,
                "team_id": team_id_i,
                "table": [],
                "message": "Standings are not available yet.\nPlease check back later.",
                "source": "computed_unavailable",
            }

        ht, at = team_pair
        hg, ag = goal_pair

        # 완료된 경기 수 확인 (0이면 시즌 시작 전/데이터 없음)
        try:
            cnt_row = _fetch_one(
                f"""
                SELECT COUNT(*) AS cnt
                FROM matches
                WHERE league_id = %s
                  AND season = %s
                  AND (
                    lower(coalesce(status_group,'')) = 'finished'
                    OR coalesce(status,'') IN ('FT','AET','PEN')
                    OR coalesce(status_short,'') IN ('FT','AET','PEN')
                  )
                  AND {ht} IS NOT NULL AND {at} IS NOT NULL
                  AND {hg} IS NOT NULL AND {ag} IS NOT NULL
                """,
                (league_id_i, season_resolved),
            )
            finished_cnt = int((cnt_row or {}).get("cnt") or 0)
        except Exception:
            finished_cnt = 0

        if finished_cnt <= 0:
            return {
                "league_id": league_id_i,
                "season": season_resolved,
                "team_id": team_id_i,
                "table": [],
                "message": "Standings are not available yet.\nPlease check back later.",
                "source": "computed_no_finished",
            }

        # matches 기반 standings 계산 (포인트/득실/다득점 기본 정렬)
        try:
            rows_raw = fetch_all(
                f"""
                WITH finished AS (
                  SELECT
                    {ht} AS home_team_id,
                    {at} AS away_team_id,
                    {hg} AS home_goals,
                    {ag} AS away_goals
                  FROM matches
                  WHERE league_id = %s
                    AND season = %s
                    AND (
                      lower(coalesce(status_group,'')) = 'finished'
                      OR coalesce(status,'') IN ('FT','AET','PEN')
                      OR coalesce(status_short,'') IN ('FT','AET','PEN')
                    )
                    AND {ht} IS NOT NULL AND {at} IS NOT NULL
                    AND {hg} IS NOT NULL AND {ag} IS NOT NULL
                ),
                per_team AS (
                  SELECT
                    home_team_id AS team_id,
                    COUNT(*) AS played,
                    SUM(CASE WHEN home_goals > away_goals THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN home_goals = away_goals THEN 1 ELSE 0 END) AS draws,
                    SUM(CASE WHEN home_goals < away_goals THEN 1 ELSE 0 END) AS losses,
                    SUM(home_goals) AS goals_for,
                    SUM(away_goals) AS goals_against,
                    SUM(CASE WHEN home_goals > away_goals THEN 3 WHEN home_goals = away_goals THEN 1 ELSE 0 END) AS points
                  FROM finished
                  GROUP BY home_team_id

                  UNION ALL

                  SELECT
                    away_team_id AS team_id,
                    COUNT(*) AS played,
                    SUM(CASE WHEN away_goals > home_goals THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN away_goals = home_goals THEN 1 ELSE 0 END) AS draws,
                    SUM(CASE WHEN away_goals < home_goals THEN 1 ELSE 0 END) AS losses,
                    SUM(away_goals) AS goals_for,
                    SUM(home_goals) AS goals_against,
                    SUM(CASE WHEN away_goals > home_goals THEN 3 WHEN away_goals = home_goals THEN 1 ELSE 0 END) AS points
                  FROM finished
                  GROUP BY away_team_id
                ),
                agg AS (
                  SELECT
                    team_id,
                    SUM(played) AS played,
                    SUM(wins) AS wins,
                    SUM(draws) AS draws,
                    SUM(losses) AS losses,
                    SUM(goals_for) AS goals_for,
                    SUM(goals_against) AS goals_against,
                    (SUM(goals_for) - SUM(goals_against)) AS goal_diff,
                    SUM(points) AS points
                  FROM per_team
                  GROUP BY team_id
                ),
                ranked AS (
                  SELECT
                    ROW_NUMBER() OVER (
                      ORDER BY points DESC, goal_diff DESC, goals_for DESC, team_id ASC
                    ) AS rank,
                    *
                  FROM agg
                )
                SELECT
                  r.rank,
                  r.team_id,
                  COALESCE(t.name, '') AS team_name,
                  r.played,
                  r.wins,
                  r.draws,
                  r.losses,
                  r.goals_for,
                  r.goals_against,
                  r.goal_diff,
                  r.points,
                  NULL::text AS group_name
                FROM ranked r
                LEFT JOIN teams t ON t.id = r.team_id
                ORDER BY r.rank ASC, team_name ASC
                """,
                (league_id_i, season_resolved),
            )
        except Exception:
            rows_raw = []

        if not rows_raw:
            return {
                "league_id": league_id_i,
                "season": season_resolved,
                "team_id": team_id_i,
                "table": [],
                "message": "Standings are not available yet.\nPlease check back later.",
                "source": "computed_failed",
            }

    # ─────────────────────────────────────────────
    # 공통 후처리: 중복 row 제거(played 최대)
    # ─────────────────────────────────────────────
    rows_by_team: Dict[int, Dict[str, Any]] = {}
    for r in rows_raw:
        tid = _coalesce_int(r.get("team_id"), 0)
        if tid == 0:
            continue
        prev = rows_by_team.get(tid)
        if prev is None:
            rows_by_team[tid] = r
        else:
            if _coalesce_int(r.get("played"), 0) > _coalesce_int(prev.get("played"), 0):
                rows_by_team[tid] = r

    dedup_rows: List[Dict[str, Any]] = list(rows_by_team.values())

    # ─────────────────────────────────────────────
    # group_name 여러 개인 경우 → 이 팀이 속한 그룹만
    # (computed_from_matches는 group_name이 NULL이라 영향 거의 없음)
    # ─────────────────────────────────────────────
    group_names = {
        (r.get("group_name") or "").strip()
        for r in dedup_rows
        if r.get("group_name") is not None
    }
    if len(group_names) > 1:
        main_group = None
        for r in dedup_rows:
            if _coalesce_int(r.get("team_id"), 0) == team_id_i:
                main_group = (r.get("group_name") or "").strip()
                break
        if main_group:
            dedup_rows = [
                r for r in dedup_rows
                if (r.get("group_name") or "").strip() == main_group
            ]

    dedup_rows.sort(key=lambda r: _coalesce_int(r.get("rank"), 0) or 999999)

    # ─────────────────────────────────────────────
    # table 매핑
    # ─────────────────────────────────────────────
    table: List[Dict[str, Any]] = []
    for r in dedup_rows:
        table.append(
            {
                "position": _coalesce_int(r.get("rank"), 0),
                "team_id": _coalesce_int(r.get("team_id"), 0),
                "team_name": r.get("team_name") or "",
                "played": _coalesce_int(r.get("played"), 0),
                "wins": _coalesce_int(r.get("wins"), 0),
                "draws": _coalesce_int(r.get("draws"), 0),
                "losses": _coalesce_int(r.get("losses"), 0),
                "goals_for": _coalesce_int(r.get("goals_for"), 0),
                "goals_against": _coalesce_int(r.get("goals_against"), 0),
                "goal_diff": _coalesce_int(r.get("goal_diff"), 0),
                "points": _coalesce_int(r.get("points"), 0),
            }
        )

    out: Dict[str, Any] = {
        "league_id": league_id_i,
        "season": int(season_resolved),
        "team_id": team_id_i,
        "table": table,
        "source": source,  # 앱은 안 써도 됨(디버깅용)
    }

    if not table:
        out["message"] = "Standings are not available yet.\nPlease check back later."

    return out
