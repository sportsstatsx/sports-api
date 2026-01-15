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
# 환경변수:
#   - APIFOOTBALL_KEY (또는 API_FOOTBALL_KEY / API_KEY / FOOTBALL_API_KEY)

import os
import sys
import time
import json
from typing import Any, Dict, List, Optional, Tuple

import requests

from db import fetch_all, execute


# ─────────────────────────────────────
#  ENV / HTTP
# ─────────────────────────────────────

def _get_api_key() -> str:
    key = (
        os.environ.get("APIFOOTBALL_KEY")
        or os.environ.get("API_FOOTBALL_KEY")
        or os.environ.get("API_KEY")
        or os.environ.get("FOOTBALL_API_KEY")
        or ""
    )
    if not key:
        raise RuntimeError("API key missing: set APIFOOTBALL_KEY (or API_FOOTBALL_KEY / API_KEY)")
    return key


def _headers() -> Dict[str, str]:
    return {"x-apisports-key": _get_api_key()}


def _safe_get(url: str, *, params: Dict[str, Any], timeout: int = 20, max_retry: int = 4) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for i in range(max_retry):
        try:
            resp = requests.get(url, headers=_headers(), params=params, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.7 * (i + 1))
                continue
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError("API response is not a dict")
            return data
        except Exception as e:
            last_err = e
            time.sleep(0.7 * (i + 1))
            continue
    raise RuntimeError(f"API request failed after retries: {last_err}")


# ─────────────────────────────────────
#  CLI 유틸
# ─────────────────────────────────────

def parse_seasons_from_argv(argv: List[str]) -> List[int]:
    """
    sys.argv[1:] 로 들어온 값들에서 시즌(정수) 목록만 추출.
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

    return sorted(set(seasons))


def load_league_seasons_from_db(seasons_filter: List[int]) -> List[Tuple[int, int]]:
    """
    matches 테이블에서 DISTINCT (league_id, season) 조합을 가져온다.
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


# ─────────────────────────────────────
#  team_ids 구하기 (standings → matches fallback)
# ─────────────────────────────────────

def _get_team_ids_for_league_season(league_id: int, season: int) -> List[int]:
    # 1) standings 기준
    rows = fetch_all(
        """
        SELECT DISTINCT team_id
        FROM standings
        WHERE league_id = %s
          AND season = %s
        """,
        (league_id, season),
    )
    if rows:
        return [int(r["team_id"]) for r in rows if r.get("team_id") is not None]

    # 2) standings 가 비었으면 matches 기준
    rows = fetch_all(
        """
        SELECT DISTINCT team_id FROM (
            SELECT home_id AS team_id
            FROM matches
            WHERE league_id = %s AND season = %s
            UNION
            SELECT away_id AS team_id
            FROM matches
            WHERE league_id = %s AND season = %s
        ) t
        """,
        (league_id, season, league_id, season),
    )
    return [int(r["team_id"]) for r in rows if r.get("team_id") is not None]


# ─────────────────────────────────────
#  /teams/statistics → team_season_stats(full_json)
# ─────────────────────────────────────

def fetch_team_statistics_from_api(league_id: int, season: int, team_id: int) -> Optional[Dict[str, Any]]:
    """
    Api-Football /teams/statistics 호출.
    response(dict 1개)를 그대로 반환.
    """
    url = "https://v3.football.api-sports.io/teams/statistics"
    params = {"league": league_id, "season": season, "team": team_id}
    data = _safe_get(url, params=params, timeout=20)

    resp_obj = data.get("response")
    if not resp_obj:
        return None
    return resp_obj if isinstance(resp_obj, dict) else None


def upsert_team_season_stats(league_id: int, season: int, team_id: int, stats_data: Dict[str, Any]) -> None:
    """
    team_season_stats (
        league_id INTEGER NOT NULL,
        season    INTEGER NOT NULL,
        team_id   INTEGER NOT NULL,
        name      TEXT    NOT NULL,
        value     TEXT,
        PRIMARY KEY (league_id, season, team_id, name)
    )

    전체 statistics JSON을 name='full_json' 한 줄로 저장.
    """
    json_str = json.dumps(stats_data, ensure_ascii=False)

    execute(
        """
        INSERT INTO team_season_stats (
            league_id,
            season,
            team_id,
            name,
            value
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (league_id, season, team_id, name) DO UPDATE SET
            value = EXCLUDED.value
        """,
        (league_id, season, team_id, "full_json", json_str),
    )


def update_team_season_stats_for_league(league_id: int, season: int, phase: str) -> None:
    team_ids = _get_team_ids_for_league_season(league_id, season)
    if not team_ids:
        print(
            f"    [team_season_stats {phase}] league={league_id}, season={season}: "
            f"team_ids 비어있음 → 스킵"
        )
        return

    print(
        f"    [team_season_stats {phase}] league={league_id}, season={season}: "
        f"{len(team_ids)}개 팀 갱신"
    )

    ok = 0
    fail = 0
    for idx, team_id in enumerate(team_ids, start=1):
        try:
            stats = fetch_team_statistics_from_api(league_id, season, team_id)
            if not stats:
                fail += 1
                continue
            upsert_team_season_stats(league_id, season, team_id, stats)
            ok += 1
            if idx % 50 == 0:
                print(f"      [progress] {idx}/{len(team_ids)} ok={ok} fail={fail}")
            time.sleep(0.09)
        except Exception as e:
            fail += 1
            print(
                f"      [ERR] league={league_id} season={season} team={team_id} err={e}",
                file=sys.stderr,
            )
            time.sleep(0.25)

    print(f"    [team_season_stats {phase}] done ok={ok} fail={fail}")


# ─────────────────────────────────────
#  MAIN
# ─────────────────────────────────────

def main() -> None:
    seasons_filter = parse_seasons_from_argv(sys.argv[1:])

    if seasons_filter:
        print(f"[INFO] 지정된 시즌만 백필: {seasons_filter}")
    else:
        print("[INFO] 시즌 필터 없음 → DB에 있는 모든 league_id / season 조합 대상")

    pairs = load_league_seasons_from_db(seasons_filter)
    if not pairs:
        print("[INFO] matches 테이블에서 대상 league_id/season 을 찾지 못했습니다.")
        return

    print(f"[INFO] 대상 league/season 개수 = {len(pairs)}")
    for (league_id, season) in pairs:
        print(f"[BACKFILL] league_id={league_id}, season={season} → team_season_stats 갱신 시작")
        try:
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
