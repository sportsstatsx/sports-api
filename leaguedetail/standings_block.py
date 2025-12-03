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

    - league_id / season 기반으로 standings 조회
    - season 이 None 이면 standings → fixtures 순서대로 최신 시즌 추론
    - 팀당 여러 row(스플릿 라운드 등)가 있으면, played 가 가장 큰 row만 남김
    - context_options 에 MLS/K리그 스플릿/그룹 정보까지 내려줌
    """

    if not league_id:
        # league_id 가 없으면 아예 빈 값 리턴
        return {
            "league_id": None,
            "season": None,
            "rows": [],
            "context_options": {
                "conferences": [],
                "groups": [],
            },
        }

    # season 자동 추론
    season_resolved = _resolve_season(league_id, season)
    if season_resolved is None:
        # season 을 끝까지 못 찾은 경우
        print(
            f"[build_standings_block] WARN: no season found for league_id={league_id}"
        )
        return {
            "league_id": league_id,
            "season": None,
            "rows": [],
            "context_options": {
                "conferences": [],
                "groups": [],
            },
        }

    # 리그 이름도 있으면 같이 내려주기 (선택 사항)
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
        print(
            f"[build_standings_block] WARN: failed to load league name "
            f"league_id={league_id}: {e}"
        )

    # standings 원본 조회
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
        print(
            f"[build_standings_block] ERROR league_id={league_id}, "
            f"season={season_resolved}: {e}"
        )
        rows_raw = []

    if not rows_raw:
        return {
            "league_id": league_id,
            "season": season_resolved,
            "league_name": league_name,
            "rows": [],
            "context_options": {
                "conferences": [],
                "groups": [],
            },
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

    # ── 2) rank 기준 정렬 ────────────────────────────────────────────────
    dedup_rows.sort(key=lambda r: _coalesce_int(r.get("rank"), 0) or 999999)

    # ── 3) JSON 매핑 (matchdetail standings 와 필드 구조 최대한 맞춤) ──────
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

    # ── 4) context_options 생성 ─────────────────────────────────────────
    context_options = _build_context_options_from_rows(dedup_rows)

    return {
        "league_id": league_id,
        "season": season_resolved,
        "league_name": league_name,
        "rows": out_rows,
        "context_options": context_options,
    }
