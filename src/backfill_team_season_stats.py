# backfill_team_season_stats.py
"""
각 리그/시즌에 대해 team_season_stats(full_json)을 한 번에 채워 넣는 백필 스크립트.

- 대상 리그: LIVE_LEAGUES env 에 들어있는 리그들만 처리
- 대상 시즌: matches 테이블에 실제로 존재하는 시즌 전체 (예: 2024, 2025 등)

사용 예시 (Render Web Shell):

    cd /opt/render/project/src
    python backfill_team_season_stats.py          # 모든 시즌 백필
    python backfill_team_season_stats.py 2024     # 2024 시즌만 백필
    python backfill_team_season_stats.py 2024,2025  # 2024, 2025만 백필
"""

import sys
from typing import List, Set

from db import fetch_all
from live_fixtures_common import LIVE_LEAGUES_ENV, parse_live_leagues
from live_fixtures_b_group import update_team_season_stats_for_league


def _get_seasons_for_league(league_id: int, allowed_seasons: Set[int] | None) -> List[int]:
    """
    matches 테이블에서 해당 리그의 실제 시즌 목록을 가져온다.
    allowed_seasons 가 지정되어 있으면 그 안에 포함된 시즌만 사용.
    """
    rows = fetch_all(
        """
        SELECT DISTINCT season
        FROM matches
        WHERE league_id = %s
          AND season IS NOT NULL
        ORDER BY season
        """,
        (league_id,),
    )

    seasons: List[int] = []
    for r in rows:
        s = r.get("season")
        if s is None:
            continue
        try:
            s_int = int(s)
        except (TypeError, ValueError):
            continue

        if allowed_seasons is not None and s_int not in allowed_seasons:
            continue

        seasons.append(s_int)

    return seasons


def _parse_allowed_seasons_from_argv() -> Set[int] | None:
    """
    CLI 인자에서 시즌 필터를 추출한다.

    예)
      python backfill_team_season_stats.py          → None (전체 시즌)
      python backfill_team_season_stats.py 2024     → {2024}
      python backfill_team_season_stats.py 2024,2025 → {2024, 2025}
    """
    if len(sys.argv) < 2:
        return None

    raw = sys.argv[1].strip()
    if not raw:
        return None

    parts = [p.strip() for p in raw.split(",") if p.strip()]
    seasons: Set[int] = set()
    for p in parts:
        try:
            seasons.add(int(p))
        except ValueError:
            print(f"[WARN] 무시된 시즌 인자: {p!r} (정수 아님)")
    return seasons or None


def main() -> None:
    # 1) LIVE_LEAGUES 에서 리그 목록 가져오기
    live_leagues = parse_live_leagues(LIVE_LEAGUES_ENV)
    if not live_leagues:
        print("[ERROR] LIVE_LEAGUES env 에 리그 ID 가 없습니다. 종료.")
        return

    # 2) CLI 인자로 시즌 필터(선택) 파싱
    allowed_seasons = _parse_allowed_seasons_from_argv()
    if allowed_seasons:
        print(f"[INFO] 지정된 시즌만 백필: {sorted(allowed_seasons)}")
    else:
        print("[INFO] 시즌 필터 없음 → matches 에 있는 모든 시즌을 대상으로 백필")

    total_jobs = 0
    for lid in live_leagues:
        try:
            seasons = _get_seasons_for_league(lid, allowed_seasons)
            if not seasons:
                print(
                    f"  - league {lid}: 조건에 맞는 시즌이 matches 테이블에 없음 → 스킵"
                )
                continue

            print(
                f"  - league {lid}: 백필 대상 시즌 목록 = {seasons}"
            )

            for season in seasons:
                print(
                    f"    [BACKFILL] league={lid}, season={season} → "
                    f"update_team_season_stats_for_league 호출"
                )
                try:
                    # phase 는 로그에만 쓰이므로 'BACKFILL' 로 구분
                    update_team_season_stats_for_league(
                        league_id=lid,
                        season=season,
                        phase="BACKFILL",
                    )
                    total_jobs += 1
                except Exception as e:
                    print(
                        f"    ! [BACKFILL] league={lid}, season={season} 처리 중 에러: {e}",
                        file=sys.stderr,
                    )
        except Exception as e:
            print(
                f"  ! league {lid} 시즌 목록 조회 중 에러: {e}",
                file=sys.stderr,
            )

    print(f"[BACKFILL DONE] 총 처리된 (league, season) 개수 = {total_jobs}")


if __name__ == "__main__":
    main()
