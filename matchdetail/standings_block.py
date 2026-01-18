# services/matchdetail/standings_block.py
from __future__ import annotations

from typing import Any, Dict, Optional, List, Tuple
import re

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
    matchdetail header에서 season이 비어오는 경우 방어:
      1) standings에서 MAX(season)
      2) 없으면 fixtures에서 MAX(season)
    """
    if season is not None:
        return season

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


def build_standings_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Match Detail용 Standings 블록 (하이브리드 완전체)

    ✅ 규칙:
    1) standings 테이블에 (league_id, season) rows가 있으면 그걸 우선 사용
    2) standings가 비어 있어도,
       - 해당 시즌에 "완료된 경기"가 1개라도 있으면(matches 기준)
       - 즉시 standings를 계산해서 내려준다
    3) 완료된 경기 자체가 0이면 rows=[] + 안내 문구(message)

    + 기존 matchdetail 특징 유지:
      - team당 중복 row(스플릿 라운드 등)는 played 최대 row만 남김
      - group_name 여러 개면 home/away 팀이 속한 group 하나만 사용(단 East/West split은 유지)
      - is_home / is_away 플래그 포함
      - context_options(conferences/groups) 포함
    """

    league_id = header.get("league_id")
    season = header.get("season")

    league_name = None
    league_info = header.get("league") or {}
    if isinstance(league_info, dict):
        league_name = league_info.get("name")

    def _extract_team_id(side_key: str) -> Optional[int]:
        side = header.get(side_key) or {}
        if not isinstance(side, dict):
            return None
        tid = side.get("id")
        try:
            return int(tid) if tid is not None else None
        except (TypeError, ValueError):
            return None

    home_team_id = _extract_team_id("home")
    away_team_id = _extract_team_id("away")

    if not league_id:
        return None

    try:
        league_id_int = int(league_id)
    except (TypeError, ValueError):
        return None

    season_resolved = _resolve_season(league_id_int, season if isinstance(season, int) else None)

    # 시즌 자체를 못 찾으면: 빈 블록 + 안내
    if season_resolved is None:
        return {
            "league": {
                "league_id": league_id_int,
                "season": None,
                "name": league_name,
            },
            "rows": [],
            "context_options": {"conferences": [], "groups": []},
            "message": "Standings are not available yet.\nPlease check back later.",
        }

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
            ORDER BY s.group_name NULLS FIRST, s.rank NULLS LAST, t.name ASC
            """,
            (league_id_int, season_resolved),
        )
    except Exception:
        rows_raw = []

    source = "standings_table" if rows_raw else "computed_from_matches"

    # 2) standings가 비어 있으면 → matches에서 즉시 계산
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
                "league": {
                    "league_id": league_id_int,
                    "season": season_resolved,
                    "name": league_name,
                },
                "rows": [],
                "context_options": {"conferences": [], "groups": []},
                "message": "Standings are not available yet.\nPlease check back later.",
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
                (league_id_int, season_resolved),
            )
            finished_cnt = int((cnt_row or {}).get("cnt") or 0)
        except Exception:
            finished_cnt = 0

        if finished_cnt <= 0:
            return {
                "league": {
                    "league_id": league_id_int,
                    "season": season_resolved,
                    "name": league_name,
                },
                "rows": [],
                "context_options": {"conferences": [], "groups": []},
                "message": "Standings are not available yet.\nPlease check back later.",
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
                (league_id_int, season_resolved),
            )
        except Exception:
            rows_raw = []

        if not rows_raw:
            return {
                "league": {
                    "league_id": league_id_int,
                    "season": season_resolved,
                    "name": league_name,
                },
                "rows": [],
                "context_options": {"conferences": [], "groups": []},
                "message": "Standings are not available yet.\nPlease check back later.",
            }

    # ── 공통 후처리: 팀당 중복 row 정리 (played 최대 row만) ─────────────────
    rows_by_team: Dict[int, Dict[str, Any]] = {}
    for r in rows_raw:
        tid = _coalesce_int(r.get("team_id"), 0)
        if tid == 0:
            continue
        prev = rows_by_team.get(tid)
        if prev is None:
            rows_by_team[tid] = r
        else:
            prev_played = _coalesce_int(prev.get("played"), 0)
            cur_played = _coalesce_int(r.get("played"), 0)
            if cur_played > prev_played:
                rows_by_team[tid] = r

    dedup_rows: List[Dict[str, Any]] = list(rows_by_team.values())

    # ── group_name 여러 개면 home/away가 속한 group 하나만 사용 (East/West split 제외) ──
    group_names = {
        (r.get("group_name") or "").strip()
        for r in dedup_rows
        if r.get("group_name") is not None
    }

    def _is_east_west_split(names) -> bool:
        lower = {g.lower() for g in names if g}
        has_east = any("east" in g for g in lower)
        has_west = any("west" in g for g in lower)
        return has_east and has_west

    if len(group_names) > 1 and not _is_east_west_split(group_names):
        main_group = None

        if home_team_id is not None:
            for r in dedup_rows:
                if _coalesce_int(r.get("team_id"), 0) == _coalesce_int(home_team_id, 0):
                    main_group = (r.get("group_name") or "").strip()
                    break

        if main_group is None and away_team_id is not None:
            for r in dedup_rows:
                if _coalesce_int(r.get("team_id"), 0) == _coalesce_int(away_team_id, 0):
                    main_group = (r.get("group_name") or "").strip()
                    break

        if main_group:
            dedup_rows = [
                r
                for r in dedup_rows
                if (r.get("group_name") or "").strip() == main_group
            ]

    # ── rank 기준 정렬 후 JSON 매핑 ───────────────────────────────────────
    dedup_rows.sort(key=lambda r: _coalesce_int(r.get("rank"), 0) or 999999)

    table: List[Dict[str, Any]] = []
    for r in dedup_rows:
        team_id = _coalesce_int(r.get("team_id"), 0)
        table.append(
            {
                "position": _coalesce_int(r.get("rank"), 0),
                "team_id": team_id,
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
                "is_home": (home_team_id is not None and team_id == home_team_id),
                "is_away": (away_team_id is not None and team_id == away_team_id),
            }
        )

    context_options = _build_context_options_from_rows(dedup_rows)

    out: Dict[str, Any] = {
        "league": {
            "league_id": league_id_int,
            "season": season_resolved,
            "name": league_name,
        },
        "rows": table,
        "context_options": context_options,
        # 디버깅/검증 편의(앱에서 안 써도 무방)
        "source": source,
    }

    # rows가 비면 안내문구(혹시라도 안전망)
    if not table:
        out["message"] = "Standings are not available yet.\nPlease check back later."

    return out


def _build_context_options_from_rows(
    rows: List[Dict[str, Any]]
) -> Dict[str, List[str]]:
    """
    StandingsDao.buildContext(...) 에서 하던 컨퍼런스/그룹 인식 로직을
    서버쪽으로 옮긴 버전 (순수 A방식 준비).

    - conferences: ["East", "West"] 등
    - groups: ["Group A", "Group B", "Championship Round", "Relegation Round"] 등
    """
    if not rows:
        return {"conferences": [], "groups": []}

    group_raw: List[str] = []
    desc_raw: List[str] = []
    for r in rows:
        g = r.get("group_name")
        d = r.get("description")
        if isinstance(g, str):
            g = g.strip()
            if g:
                group_raw.append(re.sub(r"\s+", " ", g))
        if isinstance(d, str):
            desc_raw.append(d.lower())

    group_raw = list(dict.fromkeys(group_raw))  # distinct, 순서 유지

    rx_has_split_round = re.compile(
        r"(champ(ion)?ship\s+.*(round|rnd))|(releg(ation)?\s+.*(round|rnd))",
        re.IGNORECASE,
    )
    rx_group = re.compile(r"group\s*([A-Z])", re.IGNORECASE)

    def derive_from_description() -> List[str]:
        if not desc_raw:
            return []
        has_champ_round = any(
            rx_has_split_round.search(d) and "champ" in d for d in desc_raw
        )
        has_releg_round = any(
            rx_has_split_round.search(d) and "releg" in d for d in desc_raw
        )
        out: List[str] = []
        if has_champ_round:
            out.append("Championship Round")
        if has_releg_round:
            out.append("Relegation Round")
        return out

    has_east = any("east" in g.lower() for g in group_raw)
    has_west = any("west" in g.lower() for g in group_raw)
    has_grp = any(rx_group.search(g) for g in group_raw)
    has_rnd = any(rx_has_split_round.search(g) for g in group_raw)

    conferences: List[str] = []
    if has_east:
        conferences.append("East")
    if has_west:
        conferences.append("West")

    groups: List[str] = []
    for g in group_raw:
        gl = g.lower()
        if "east" in gl or "west" in gl:
            continue
        m = rx_group.search(g)
        if m:
            groups.append(f"Group {m.group(1).upper()}")
        elif rx_has_split_round.search(g) and "champ" in gl:
            groups.append("Championship Round")
        elif rx_has_split_round.search(g) and "releg" in gl:
            groups.append("Relegation Round")

    has_meaningful = has_east or has_west or has_grp or has_rnd or bool(groups)
    if not has_meaningful:
        groups = derive_from_description()

    def _dedup_case_insensitive(items: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for x in items:
            key = x.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(x)
        return out

    conferences = _dedup_case_insensitive(conferences)
    groups = _dedup_case_insensitive(groups)

    return {"conferences": conferences, "groups": groups}
