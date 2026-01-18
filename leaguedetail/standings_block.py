# leaguedetail/standings_block.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
import re

from db import fetch_all


def _coalesce_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _fetch_one(query: str, params: tuple) -> Optional[Dict[str, Any]]:
    """
    fetch_all 래핑해서 첫 번째 row만 돌려주는 헬퍼.
    """
    rows = fetch_all(query, params)
    return rows[0] if rows else None


def _resolve_season(league_id: int, season: Optional[int]) -> Optional[int]:
    """
    season 이 None 이면:
      1) standings 에서 해당 리그의 MAX(season)
      2) 없으면 fixtures 에서 MAX(season)
    순서대로 시도해서 하나라도 찾으면 그 값 리턴.
    """
    if season is not None:
        return season

    # 1) standings 기준
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

    # 2) fixtures 기준
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


# ────────────────────────────────────────────────────────────
#  컨퍼런스 / 그룹 / 스플릿 정보 추출 (context_options)
# ────────────────────────────────────────────────────────────

_RX_GROUP = re.compile(r"group\s+[A-Z]", re.IGNORECASE)
_RX_CONF = re.compile(r"conference", re.IGNORECASE)
_RX_EAST = re.compile(r"east", re.IGNORECASE)
_RX_WEST = re.compile(r"west", re.IGNORECASE)
_RX_CHAMP = re.compile(r"championship", re.IGNORECASE)
_RX_RELEG = re.compile(r"relegation", re.IGNORECASE)
_RX_PLAYOFF = re.compile(r"play[- ]?off", re.IGNORECASE)


def _build_context_options_from_rows(rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """
    standings row 들을 보고:
      - MLS East/West 같은 컨퍼런스
      - K리그 Championship / Relegation / Playoff 스플릿
      - Group A / Group B 등 그룹
    을 추출해서 context_options 로 내려준다.

    클라이언트에서는:
      conferences → StandingsContext.conferences
      groups      → StandingsContext.groups
    로 맵핑해서 칩 필터로 사용.
    """

    conferences: List[str] = []
    groups: List[str] = []

    for r in rows:
        g = (r.get("group_name") or "").strip()
        desc = (r.get("description") or "").strip()
        text = f"{g} {desc}".strip()
        if not text:
            continue

        # 1) 컨퍼런스/East/West (MLS 류)
        if _RX_CONF.search(text) or _RX_EAST.search(text) or _RX_WEST.search(text):
            label = g or desc
            if label and label not in conferences:
                conferences.append(label)
            continue

        # 2) 챔피언십/강등/플레이오프/그룹 → groups 로
        if (
            _RX_CHAMP.search(text)
            or _RX_RELEG.search(text)
            or _RX_PLAYOFF.search(text)
            or _RX_GROUP.search(text)
        ):
            label = g or desc
            if label and label not in groups:
                groups.append(label)

    return {
        "conferences": conferences,
        "groups": groups,
    }


def build_standings_block(league_id: int, season: Optional[int]) -> Dict[str, Any]:
    """
    League Detail 화면의 'Standings' 탭 데이터.

    ✅ 완전무결(절대 흔들리지 않게):
    1) standings 테이블에 (league_id, season) rows가 있으면 그걸 우선 사용
    2) standings가 비어 있어도,
       - 해당 시즌에 "완료된 경기"가 1개라도 있으면(matches 기준)
       - 즉시 standings를 계산해서 내려준다 (현재 시즌 Standings 보장)
    3) 완료된 경기 자체가 0이면 rows=[] (아직 시즌 시작 전)
    """

    if not league_id:
        return {
            "league_id": None,
            "season": None,
            "rows": [],
            "context_options": {"conferences": [], "groups": []},
        }

    season_resolved = _resolve_season(league_id, season)
    if season_resolved is None:
        print(f"[build_standings_block] WARN: no season found for league_id={league_id}")
        return {
            "league_id": league_id,
            "season": None,
            "rows": [],
            "context_options": {"conferences": [], "groups": []},
        }

    league_name: Optional[str] = None
    try:
        league_row = _fetch_one(
            """
            SELECT name
            FROM leagues
            WHERE id = %s
            """,
            (league_id,),
        )
        if league_row is not None:
            league_name = (league_row.get("name") or "").strip() or None
    except Exception as e:
        print(f"[build_standings_block] WARN: failed to load league name league_id={league_id}: {e}")

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

    def _pick_pair(cols: set[str], pairs: List[tuple[str, str]]) -> Optional[tuple[str, str]]:
        for a, b in pairs:
            if a in cols and b in cols:
                return (a, b)
        return None

    # 1) standings 테이블 우선
    try:
        rows_raw: List[Dict[str, Any]] = fetch_all(
            """
            SELECT
                s.rank,
                s.team_id,
                t.name       AS team_name,
                t.logo       AS team_logo,
                s.played,
                s.win,
                s.draw,
                s.lose,
                s.goals_for,
                s.goals_against,
                s.goals_diff,
                s.points,
                s.description,
                s.group_name,
                s.form
            FROM standings AS s
            JOIN teams     AS t ON t.id = s.team_id
            WHERE s.league_id = %s
              AND s.season    = %s
            ORDER BY
                s.group_name NULLS FIRST,
                s.rank       NULLS LAST,
                t.name       ASC
            """,
            (league_id, season_resolved),
        )
    except Exception as e:
        print(f"[build_standings_block] ERROR standings query league_id={league_id}, season={season_resolved}: {e}")
        rows_raw = []

    # 2) standings가 비어 있으면 → matches에서 즉시 계산
    if not rows_raw:
        # 2-1) matches 컬럼 자동 탐지(환경/스키마 흔들림 방어)
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
            # matches로도 계산 불가 → 그냥 빈 값
            return {
                "league_id": league_id,
                "season": season_resolved,
                "league_name": league_name,
                "rows": [],
                "context_options": {"conferences": [], "groups": []},
            }

        ht, at = team_pair
        hg, ag = goal_pair

        # 2-2) 완료된 경기 수 확인 (0이면 시즌 시작 전)
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
                (league_id, season_resolved),
            )
            finished_cnt = int((cnt_row or {}).get("cnt") or 0)
        except Exception:
            finished_cnt = 0

        if finished_cnt <= 0:
            return {
                "league_id": league_id,
                "season": season_resolved,
                "league_name": league_name,
                "rows": [],
                "context_options": {"conferences": [], "groups": []},
            }

        # 2-3) matches 기반 standings 계산 (기본 포인트/득실/다득점 정렬)
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
                    SUM(CASE WHEN home_goals > away_goals THEN 1 ELSE 0 END) AS win,
                    SUM(CASE WHEN home_goals = away_goals THEN 1 ELSE 0 END) AS draw,
                    SUM(CASE WHEN home_goals < away_goals THEN 1 ELSE 0 END) AS lose,
                    SUM(home_goals) AS goals_for,
                    SUM(away_goals) AS goals_against,
                    SUM(CASE WHEN home_goals > away_goals THEN 3 WHEN home_goals = away_goals THEN 1 ELSE 0 END) AS points
                  FROM finished
                  GROUP BY home_team_id

                  UNION ALL

                  SELECT
                    away_team_id AS team_id,
                    COUNT(*) AS played,
                    SUM(CASE WHEN away_goals > home_goals THEN 1 ELSE 0 END) AS win,
                    SUM(CASE WHEN away_goals = home_goals THEN 1 ELSE 0 END) AS draw,
                    SUM(CASE WHEN away_goals < home_goals THEN 1 ELSE 0 END) AS lose,
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
                    SUM(win) AS win,
                    SUM(draw) AS draw,
                    SUM(lose) AS lose,
                    SUM(goals_for) AS goals_for,
                    SUM(goals_against) AS goals_against,
                    (SUM(goals_for) - SUM(goals_against)) AS goals_diff,
                    SUM(points) AS points
                  FROM per_team
                  GROUP BY team_id
                ),
                ranked AS (
                  SELECT
                    ROW_NUMBER() OVER (
                      ORDER BY points DESC, goals_diff DESC, goals_for DESC, team_id ASC
                    ) AS rank,
                    *
                  FROM agg
                )
                SELECT
                  r.rank,
                  r.team_id,
                  COALESCE(t.name, '') AS team_name,
                  t.logo AS team_logo,
                  r.played,
                  r.win,
                  r.draw,
                  r.lose,
                  r.goals_for,
                  r.goals_against,
                  r.goals_diff,
                  r.points,
                  NULL::text AS description,
                  NULL::text AS group_name,
                  NULL::text AS form
                FROM ranked r
                LEFT JOIN teams t ON t.id = r.team_id
                ORDER BY r.rank ASC, team_name ASC
                """,
                (league_id, season_resolved),
            )
        except Exception as e:
            print(f"[build_standings_block] ERROR computed standings league_id={league_id}, season={season_resolved}: {e}")
            rows_raw = []

        if not rows_raw:
            return {
                "league_id": league_id,
                "season": season_resolved,
                "league_name": league_name,
                "rows": [],
                "context_options": {"conferences": [], "groups": []},
            }

    # ── 1) 팀당 중복 row 정리 (played 가장 큰 row만 사용) ─────────────────
    rows_by_team: Dict[int, Dict[str, Any]] = {}
    for r in rows_raw:
        team_id = _coalesce_int(r.get("team_id"), 0)
        if team_id == 0:
            continue

        prev = rows_by_team.get(team_id)
        if prev is None:
            rows_by_team[team_id] = r
        else:
            prev_played = _coalesce_int(prev.get("played"), 0)
            cur_played = _coalesce_int(r.get("played"), 0)
            if cur_played > prev_played:
                rows_by_team[team_id] = r

    dedup_rows: List[Dict[str, Any]] = list(rows_by_team.values())
    dedup_rows.sort(key=lambda r: _coalesce_int(r.get("rank"), 0) or 999999)

    out_rows: List[Dict[str, Any]] = []
    for r in dedup_rows:
        out_rows.append(
            {
                "position": _coalesce_int(r.get("rank"), 0),
                "team_id": _coalesce_int(r.get("team_id"), 0),
                "team_name": r.get("team_name") or "",
                "team_logo": r.get("team_logo"),
                "played": _coalesce_int(r.get("played"), 0),
                "win": _coalesce_int(r.get("win"), 0),
                "draw": _coalesce_int(r.get("draw"), 0),
                "loss": _coalesce_int(r.get("lose"), 0),
                "goals_for": _coalesce_int(r.get("goals_for"), 0),
                "goals_against": _coalesce_int(r.get("goals_against"), 0),
                "goal_diff": _coalesce_int(r.get("goals_diff"), 0),
                "points": _coalesce_int(r.get("points"), 0),
                "description": r.get("description"),
                "group_name": r.get("group_name"),
                "form": r.get("form"),
            }
        )

    context_options = _build_context_options_from_rows(dedup_rows)

    return {
        "league_id": league_id,
        "season": season_resolved,
        "league_name": league_name,
        "rows": out_rows,
        "context_options": context_options,
    }

