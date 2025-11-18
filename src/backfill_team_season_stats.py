# backfill_team_season_stats.py
#
# DB 안의 league_id / season 조합을 찾아서
# team_season_stats(teams/statistics) 를 한 번에 채우는 스크립트.
#
# 사용 예시:
#   1) 전체 DB에 있는 리그+시즌 모두 백필
#      python backfill_team_season_stats.py
#
#   2) 특정 시즌만 백필 (예: 2024만)
#      python backfill_team_season_stats.py 2024
#
#   3) 여러 시즌만 백필 (예: 2024, 2025)
#      python backfill_team_season_stats.py 2024 2025
#      또는
#      python backfill_team_season_stats.py 2024,2025
#

import sys
from typing import List, Tuple

from db import fetch_all
from live_fixtures_b_group import update_team_season_stats_for_league


def parse_seasons_from_argv(argv: List[str]) -> List[int]:
    """
    sys.argv[1:] 로 들어온 값들에서 시즌(정수) 목록만 추출.

    예:
      ["2024"]            -> [2024]
      ["2024,2025"]       -> [2024, 2025]
      ["2024", "2025"]    -> [2024, 2025]
      ["2024,2025", "23"] -> [23, 2024, 2025] (정렬은 나중에)
    """
    season_tokens: List[str] = []
    for arg in argv:
        for token in arg.split(","):
            token = token.strip()
            if not token:
                continue
            season_tokens.append(token)

    seasons: List[int] = []
    for t in season_tokens:
        try:
            seasons.append(int(t))
        except ValueError:
            print(f"[WARN] 시즌 값으로 해석할 수 없음: {t!r} → 무시", file=sys.stderr)

    # 중복 제거 + 정렬
    return sorted(set(seasons))


def load_league_seasons_from_db(seasons_filter: List[int]) -> List[Tuple[int, int]]:
    """
    matches 테이블에서 DISTINCT (league_id, season) 조합을 가져온다.
    seasons_filter 가 비어 있으면 전체, 비어 있지 않으면 해당 시즌만.

    반환: [(league_id, season), ...]
    """
    params: List[object] = []
    where_clause = ""

    if seasons_filter:
        placeholders = ", ".join(["%s"] * len(seasons_filter))
        where_clause = f"WHERE season IN ({placeholders})"
        params.extend(seasons_filter)

    rows = fetch_all(
        f"""
        SELECT DISTINCT league_id, season
        FROM matches
        {where_clause}
        ORDER BY league_id ASC, season ASC
        """,
        tuple(params),
    )

    result: List[Tuple[int, int]] = []
    for r in rows:
        lid = r.get("league_id")
        season = r.get("season")
        if lid is None or season is None:
            continue
        result.append((int(lid), int(season)))

    return result


def main() -> None:
    # 1) CLI 인자로 시즌 필터 파싱
    seasons_filter = parse_seasons_from_argv(sys.argv[1:])

    if seasons_filter:
        print(f"[INFO] 지정된 시즌만 백필: {seasons_filter}")
    else:
        print("[INFO] 시즌 필터 없음 → DB에 있는 모든 league_id / season 조합 대상")

    # 2) DB에서 (league_id, season) 목록 조회
    pairs = load_league_seasons_from_db(seasons_filter)
    if not pairs:
        print("[INFO] matches 테이블에서 대상 league_id/season 을 찾지 못했습니다.")
        return

    print(f"[INFO] 대상 league/season 개수 = {len(pairs)}")
    for (league_id, season) in pairs:
        print(
            f"[BACKFILL] league_id={league_id}, season={season} "
            f"→ team_season_stats 갱신 시작"
        )
        try:
            # phase 는 로그용 태그일 뿐, PREMATCH/POSTMATCH 와는 별개로 사용
            update_team_season_stats_for_league(
                league_id=league_id,
                season=season,
                phase="BACKFILL",
            )
        except Exception as e:
            print(
                f"[ERROR] league_id={league_id}, season={season} 처리 중 에러: {e}",
                file=sys.stderr,
            )

    print("[DONE] backfill_team_season_stats 전체 완료")


if __name__ == "__main__":
    main()
