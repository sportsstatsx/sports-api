from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all


def build_seasons_block(league_id: int) -> Dict[str, Any]:
    """
    League Detail 화면의 'Seasons' 탭 + 기본 시즌 선택에 사용할 시즌 목록.

    반환 예시:
    {
        "league_id": 188,
        "seasons": [2025, 2024],
        "season_champions": [
            {"season": 2025, "team_id": 943, "team_name": "Some Club", "points": 12},
            {"season": 2024, "team_id": 24608, "team_name": "Another Club", "points": 53}
        ]
    }
    """
    seasons: List[int] = []
    season_champions: List[Dict[str, Any]] = []

    # 1) 사용 가능한 시즌 목록 (matches 기준)
    try:
        rows = fetch_all(
            """
            SELECT DISTINCT season
            FROM matches
            WHERE league_id = %s
            ORDER BY season DESC
            """,
            (league_id,),
        )
        seasons = [int(r["season"]) for r in rows if r.get("season") is not None]
    except Exception as e:
        print(f"[build_seasons_block] ERROR league_id={league_id}: {e}")
        seasons = []

    # 2) 시즌별 우승 팀 (standings 기준)
    #    - league_id = X
    #    - rank = 1
    #    - 같은 시즌에 여러 group_name 이 있을 수 있으니
    #      → DISTINCT ON (season) 으로 시즌당 한 팀만 선택
    try:
        champ_rows = fetch_all(
            """
            SELECT DISTINCT ON (s.season)
                s.season,
                s.team_id,
                COALESCE(t.name, '') AS team_name,
                t.logo AS team_logo,
                s.points
            FROM standings AS s
            LEFT JOIN teams AS t
              ON t.id = s.team_id
            WHERE s.league_id = %s
              AND s.rank = 1
            ORDER BY s.season DESC, s.rank ASC;
            """,
            (league_id,),
        )

        season_champions = []
        for r in champ_rows:
            season_val = r.get("season")
            if season_val is None:
                continue
            season_champions.append(
                {
                    "season": int(season_val),
                    "team_id": r.get("team_id"),
                    "team_name": r.get("team_name") or "",
                    "team_logo": r.get("team_logo"),
                    "points": r.get("points"),
                }
            )
    except Exception as e:
        print(f"[build_seasons_block] CHAMPIONS ERROR league_id={league_id}: {e}")
        season_champions = []

    # 3) 현재 진행 중인 시즌(가장 최신 시즌)은 챔피언 목록에서 제외
    try:
        latest_season = resolve_season_for_league(league_id, None)
    except Exception as e:
        latest_season = None
        print(
            f"[build_seasons_block] resolve_season_for_league ERROR league_id={league_id}: {e}"
        )

    if latest_season is not None and len(season_champions) > 1:
        season_champions = [
            c for c in season_champions
            if c.get("season") != latest_season
        ]

    return {
        "league_id": league_id,
        "seasons": seasons,
        "season_champions": season_champions,
    }


def resolve_season_for_league(league_id: int, season: Optional[int]) -> Optional[int]:
    """
    ✅ 완전무결(절대 흔들리지 않게) 시즌 결정 규칙

    1) season 파라미터가 들어온 경우:
       - (league_id, season) 이 DB에 존재하면 그대로 사용
       - 존재하지 않으면 "해당 리그의 최신 시즌(MAX(season))" 으로 강제 보정
         (예: 2027, 3000, 기타 어떤 값이 와도 절대 흔들리지 않게)

    2) season 파라미터가 없는 경우(None):
       - "해당 리그의 최신 시즌(MAX(season))" 을 기본으로 사용

    ⚠️ 이유:
    - finished_cnt 기반 폴백은 캘린더 시즌 리그(MLS/K리그/J리그/브라질/아르헨 등)에서
      시즌 시작 전/초기에 오히려 최신 시즌(일정/경기)을 못 보게 만드는 역효과가 큼.
    - “우승팀/통계는 이전 시즌을 보여주자” 같은 정책은
      기본 season 결정이 아니라 각 블록에서 별도 정책으로 처리하는 게 안전함.
    """

    def _safe_int(v: Any) -> int:
        try:
            return int(v)
        except Exception:
            return 0

    def _season_exists(s: int) -> bool:
        if s <= 0:
            return False
        try:
            hit = fetch_all(
                """
                SELECT 1
                FROM matches
                WHERE league_id = %s
                  AND season = %s
                LIMIT 1
                """,
                (league_id, s),
            )
            return bool(hit)
        except Exception:
            return False

    def _latest_season() -> Optional[int]:
        try:
            rows = fetch_all(
                """
                SELECT MAX(season) AS max_season
                FROM matches
                WHERE league_id = %s
                """,
                (league_id,),
            )
            if not rows:
                return None
            ms = _safe_int(rows[0].get("max_season"))
            return ms if ms > 0 else None
        except Exception as e:
            print(f"[resolve_season_for_league] latest season query ERROR league_id={league_id}: {e}")
            return None

    # 1) season 파라미터가 오면: 존재 검증 → 없으면 최신 시즌으로 보정
    if season is not None:
        s = _safe_int(season)

        # 비정상 값 방어 (예: 20262027 같은 값)
        if 0 < s <= 3000 and _season_exists(s):
            return s

        # ✅ 핵심: 존재하지 않으면 무조건 "최신 시즌"으로 고정 보정
        return _latest_season()

    # 2) season 파라미터가 없으면: 최신 시즌
    return _latest_season()



