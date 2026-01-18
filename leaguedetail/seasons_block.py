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
    ✅ 근본해결(절대 흔들리지 않게):
    - season 쿼리가 와도 "그대로 신뢰"하지 않는다.
      → DB에 (league_id, season) 조합이 실제로 존재할 때만 사용.
      → 없다면 season-1 이 존재하면 자동 보정(예: '2026-2027'에서 2027로 넘어온 경우 2026으로 보정).
      → 그마저도 없으면 자동선택 로직으로 폴백.

    - season 쿼리가 없으면:
      기존과 동일하게 "완료(FINISHED) 경기 수가 충분한 시즌" 중 최신 시즌을 기본으로 선택
      → 시즌이 막 시작해서 완료 경기가 0~몇 경기면, 자동으로 이전 시즌으로 폴백
    """

    # ✅ 임계치: 이 값 미만이면 "시즌이 아직 제대로 시작 안함"으로 보고 이전 시즌을 우선
    MIN_FINISHED = 5

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
            # 존재 확인 실패 시에는 보수적으로 "없다"로 처리하고 아래 폴백 로직으로 넘긴다.
            return False

    # 0) season 쿼리 값이 들어온 경우: 무조건 검증/보정
    if season is not None:
        try:
            s = int(season)
        except (TypeError, ValueError):
            s = 0

        # 비정상적으로 큰 값(예: 20262027) 방어
        if 0 < s <= 3000:
            # (league_id, s) 존재하면 그대로 사용
            if _season_exists(s):
                return s

            # 없으면 season-1 존재 시 자동 보정 (예: 2027 -> 2026)
            if _season_exists(s - 1):
                return s - 1

        # 여기까지 왔으면 "유효하지 않은 season"이므로 자동선택 로직으로 폴백

    # 1) season 미지정(or 유효하지 않음) → 기존 로직: finished_cnt 임계치 기반 자동선택
    try:
        rows = fetch_all(
            """
            SELECT
                season,
                COUNT(*) AS total_cnt,
                SUM(
                    CASE
                        WHEN COALESCE(status_group, '') = 'FINISHED'
                          OR COALESCE(status, '') IN ('FT', 'AET', 'PEN')
                          OR COALESCE(status_short, '') IN ('FT', 'AET', 'PEN')
                        THEN 1 ELSE 0
                    END
                ) AS finished_cnt,
                MAX(date_utc::timestamptz) AS max_dt
            FROM matches
            WHERE league_id = %s
            GROUP BY season
            ORDER BY season DESC
            """,
            (league_id,),
        )

        if not rows:
            return None

        # 1) "finished_cnt >= MIN_FINISHED" 인 시즌 중 최신 season 선택
        for r in rows:
            s = r.get("season")
            if s is None:
                continue
            try:
                finished_cnt = int(r.get("finished_cnt") or 0)
            except (TypeError, ValueError):
                finished_cnt = 0

            if finished_cnt >= MIN_FINISHED:
                return int(s)

        # 2) 그런 시즌이 하나도 없으면 최신 season으로 폴백
        max_season = rows[0].get("season")
        return int(max_season) if max_season is not None else None

    except Exception as e:
        print(f"[resolve_season_for_league] ERROR league_id={league_id}: {e}")
        return None


