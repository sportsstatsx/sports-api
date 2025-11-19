"""
reconcile_fixtures_season.py

Api-Football /fixtures (league + season 전체)와
우리 DB(matches, fixtures)를 비교해서:

  - API에는 있는데 DB에 없는 fixture_id  → INSERT/UPSERT
  - 둘 다 있는데 date/status 등이 달라진 fixture_id → UPSERT(갱신)
  - DB에는 있는데 API에 없는 fixture_id → 유령 경기로 보고 삭제

를 수행하는 리컨실리에이션 스크립트.

사용 예시:

  # ① 시즌을 직접 지정 (2025만)
  python reconcile_fixtures_season.py 2025

  # ② 여러 시즌 지정
  python reconcile_fixtures_season.py 2024 2025
  python reconcile_fixtures_season.py 2024,2025

  # ③ 인자를 하나도 안 주면:
  #    matches 테이블에 존재하는 모든 season 을 자동으로 찾아서 실행
  python reconcile_fixtures_season.py
"""

import os
import sys
from typing import Any, Dict, List, Tuple

import requests

from db import fetch_all, execute
from live_fixtures_common import parse_live_leagues
from live_fixtures_a_group import (
    _get_headers,          # Api-Football 헤더
    upsert_fixture_row,    # fixtures 테이블 upsert
    upsert_match_row,      # matches 테이블 upsert
)

BASE_URL = "https://v3.football.api-sports.io/fixtures"


# ─────────────────────────────────────
#  CLI 유틸
# ─────────────────────────────────────

def parse_seasons_from_argv(argv: List[str]) -> List[int]:
    """
    sys.argv[1:] 로 들어온 값들에서 시즌(정수) 목록만 추출.

    예:
      ["2024"]            -> [2024]
      ["2024,2025"]       -> [2024, 2025]
      ["2024", "2025"]    -> [2024, 2025]
      ["2024,2025", "23"] -> [23, 2024, 2025]
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


def load_all_seasons_from_db() -> List[int]:
    """
    matches 테이블에서 DISTINCT season 목록을 가져온다.
    인자를 하나도 안 줬을 때 사용.
    """
    rows = fetch_all(
        """
        SELECT DISTINCT season
        FROM matches
        WHERE season IS NOT NULL
        ORDER BY season ASC
        """,
        (),
    )
    seasons: List[int] = []
    for r in rows:
        s = r.get("season")
        if s is None:
            continue
        try:
            seasons.append(int(s))
        except (TypeError, ValueError):
            continue
    return seasons


# ─────────────────────────────────────
#  삭제 유틸 (유령 경기 정리용)
# ─────────────────────────────────────

def delete_fixture_everywhere(fixture_id: int) -> None:
    """
    하나의 fixture_id 에 대해, 관련된 모든 A그룹 테이블 + fixtures/matches 에서 삭제.
    """
    print(f"    [DEL] fixture_id={fixture_id} → 관련 테이블에서 삭제")

    # 디테일 테이블 먼저
    execute("DELETE FROM match_events       WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM match_events_raw   WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM match_lineups      WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM match_team_stats   WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM match_player_stats WHERE fixture_id = %s", (fixture_id,))

    # 메인 테이블
    execute("DELETE FROM fixtures           WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM matches            WHERE fixture_id = %s", (fixture_id,))


# ─────────────────────────────────────
#  Api-Football /fixtures (league+season 전체)
# ─────────────────────────────────────

def fetch_league_season_from_api(league_id: int, season: int) -> List[Dict[str, Any]]:
    """
    /fixtures?league=XXX&season=YYYY 호출해서
    해당 리그+시즌 전체 경기 리스트를 가져온다.
    """
    headers = _get_headers()
    params = {
        "league": league_id,
        "season": season,
    }

    resp = requests.get(BASE_URL, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    results = data.get("results", 0) or 0
    if results == 0:
        errors = data.get("errors")
        print(f"[WARN] league={league_id}, season={season} → results=0, errors={errors}")
        return []

    rows = data.get("response", []) or []
    fixtures: List[Dict[str, Any]] = []
    for item in rows:
        if isinstance(item, dict):
            fixtures.append(item)
    return fixtures


# ─────────────────────────────────────
#  DB 조회
# ─────────────────────────────────────

def load_db_fixtures(league_id: int, season: int) -> Dict[int, Dict[str, Any]]:
    """
    우리 DB(matches)에서 league+season 에 해당하는 fixture 들을 로드.

    반환:
      { fixture_id: { "fixture_id": ..., "date_utc": ..., "status": ..., "status_group": ... }, ... }
    """
    rows = fetch_all(
        """
        SELECT
            fixture_id,
            date_utc,
            status,
            status_group
        FROM matches
        WHERE league_id = %s
          AND season     = %s
        """,
        (league_id, season),
    )

    result: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        fid = r.get("fixture_id")
        if fid is None:
            continue
        result[int(fid)] = {
            "fixture_id": int(fid),
            "date_utc": r.get("date_utc"),
            "status": r.get("status"),
            "status_group": r.get("status_group"),
        }
    return result


# ─────────────────────────────────────
#  메인 리컨실리에이션 로직
# ─────────────────────────────────────

def reconcile_league_season(league_id: int, season: int) -> None:
    print(f"[RUN] league_id={league_id}, season={season} 리컨실리에이션 시작")

    api_fixtures = fetch_league_season_from_api(league_id, season)
    if not api_fixtures:
        print(f"[INFO] league={league_id}, season={season} → API 쪽 경기 없음 (건너뜀)")
        return

    db_fixtures = load_db_fixtures(league_id, season)

    api_by_id: Dict[int, Dict[str, Any]] = {}
    for f in api_fixtures:
        fixture_block = f.get("fixture") or {}
        fid = fixture_block.get("id")
        if fid is None:
            continue
        api_by_id[int(fid)] = f

    api_ids = set(api_by_id.keys())
    db_ids = set(db_fixtures.keys())

    # 1) API에만 있는 fixture → 신규 or 복구 → UPSERT
    only_api = api_ids - db_ids

    # 2) 둘 다 있는 fixture → 항상 UPSERT로 최신화 (날짜/상태가 변했을 수 있음)
    common = api_ids & db_ids

    # 3) DB에만 있는 fixture → 유령 경기 → 삭제
    only_db = db_ids - api_ids

    print(f"    API={len(api_ids)}, DB={len(db_ids)}, "
          f"only_api={len(only_api)}, common={len(common)}, only_db={len(only_db)}")

    # 1) API 전용 → UPSERT
    for fid in sorted(only_api):
        fixture = api_by_id[fid]
        print(f"    [UPSERT new] fixture_id={fid}")
        upsert_match_row(fixture, league_id=league_id, season=season)
        upsert_fixture_row(fixture, league_id=league_id, season=season)

    # 2) 공통 → UPSERT (날짜/상태가 변했어도 알아서 덮어씌움)
    for fid in sorted(common):
        fixture = api_by_id[fid]
        print(f"    [UPSERT sync] fixture_id={fid}")
        upsert_match_row(fixture, league_id=league_id, season=season)
        upsert_fixture_row(fixture, league_id=league_id, season=season)

    # 3) DB 전용(유령 경기) → 삭제
    for fid in sorted(only_db):
        delete_fixture_everywhere(fid)

    print(f"[DONE] league_id={league_id}, season={season} 리컨실리에이션 완료")


# ─────────────────────────────────────
#  엔트리 포인트
# ─────────────────────────────────────

def main() -> None:
    # 1) CLI 인자로 들어온 시즌들 먼저 파싱
    seasons = parse_seasons_from_argv(sys.argv[1:])

    # 2) 인자가 하나도 없으면 DB에서 시즌 목록 자동으로 가져오기
    if not seasons:
        seasons = load_all_seasons_from_db()
        if not seasons:
            print("[ERROR] matches 테이블에서 season 정보를 찾지 못했습니다. "
                  "최소 한 개 시즌을 인자로 넘기거나, DB에 데이터를 채워 주세요.",
                  file=sys.stderr)
            sys.exit(1)
        print(f"[INFO] 인자로 시즌이 지정되지 않아, DB에서 자동으로 찾은 시즌들: {seasons}")
    else:
        print(f"[INFO] CLI 인자로 지정된 시즌만 리컨실리에이션: {seasons}")

    live_leagues_env = os.environ.get("LIVE_LEAGUES", "")
    league_ids = parse_live_leagues(live_leagues_env)

    if not league_ids:
        print("[ERROR] LIVE_LEAGUES 환경변수가 비어 있어서, 어떤 리그를 리컨실리에이션할지 알 수 없습니다.",
              file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] LIVE_LEAGUES 에서 읽은 리그들: {league_ids}")

    for season in seasons:
        for lid in league_ids:
            reconcile_league_season(league_id=lid, season=season)


if __name__ == "__main__":
    main()
