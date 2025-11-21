import sys
import datetime as dt
from typing import Optional

from db import execute
from live_fixtures_common import (
    LIVE_LEAGUES_ENV,
    parse_live_leagues,
    get_target_date,
    now_utc,
    should_call_league_today,
)
from live_fixtures_a_group import (
    fetch_fixtures_from_api,
    upsert_fixture_row,
    _extract_fixture_basic,
)


def upsert_match_status_only(
    fixture,
    league_id: Optional[int],
    season: Optional[int],
) -> None:
    """
    matches 테이블에 '상태 전용' upsert.
    - 시작 / 하프타임 / 종료 (status, status_group, elapsed)
    - 기본 정보 (league_id, season, date_utc, home_id, away_id)
    - ❌ 스코어(home_ft, away_ft)는 절대 수정하지 않는다.
    """
    basic = _extract_fixture_basic(fixture)
    if basic is None:
        return

    fixture_id = basic["fixture_id"]

    # 상위에서 전달한 league_id / season 이 우선
    league_id = league_id or basic["league_id"]
    if season is None:
        season = basic["season"]

    date_utc = basic["date_utc"]
    status_short = basic["status"]
    status_group = basic["status_group"]
    # elapsed 없으면 None → DB에서는 NULL
    elapsed = basic.get("elapsed")

    teams_block = fixture.get("teams") or {}
    home_team = teams_block.get("home") or {}
    away_team = teams_block.get("away") or {}

    home_team_id = home_team.get("id")
    away_team_id = away_team.get("id")

    if home_team_id is None or away_team_id is None:
        # 팀 ID 없으면 matches 에 넣지 않음
        return

    # ❗ home_ft / away_ft 는 여기서 다루지 않는다.
    #    (postmatch_backfill.py 에서만 세팅)
    execute(
        """
        INSERT INTO matches (
            fixture_id,
            league_id,
            season,
            date_utc,
            status,
            status_group,
            elapsed,
            home_id,
            away_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id    = EXCLUDED.league_id,
            season       = EXCLUDED.season,
            date_utc     = EXCLUDED.date_utc,
            status       = EXCLUDED.status,
            status_group = EXCLUDED.status_group,
            elapsed      = EXCLUDED.elapsed,
            home_id      = EXCLUDED.home_id,
            away_id      = EXCLUDED.away_id
        """,
        (
            fixture_id,
            league_id,
            season,
            date_utc,
            status_short,
            status_group,
            elapsed,
            home_team_id,
            away_team_id,
        ),
    )


def main() -> None:
    """
    경량 라이브 워커.

    - 역할: 경기 상태(시작 / 하프타임 / 종료), elapsed, 기본 팀/리그 정보만 업데이트
    - Api-Football 호출: /fixtures 만 사용
    - DB 작업:
        * fixtures 테이블: upsert_fixture_row
        * matches 테이블: upsert_match_status_only
    - 절대 하지 않는 것:
        * 스코어(home_ft, away_ft)
        * events / lineups / team stats / player stats
        * standings / squads / players / transfers / toplists 등 정적 데이터
          → 전부 postmatch_backfill.py 에서 처리
    """
    target_date = get_target_date()
    live_leagues = parse_live_leagues(LIVE_LEAGUES_ENV)

    if not live_leagues:
        print(
            "[update_live_fixtures] LIVE_LEAGUES env 가 비어있습니다. 종료.",
            file=sys.stderr,
        )
        return

    today_str = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    is_today = target_date == today_str
    now = now_utc()

    print(
        f"[update_live_fixtures] date={target_date}, today={today_str}, "
        f"is_today={is_today}, leagues={live_leagues}"
    )

    total_updated = 0

    for lid in live_leagues:
        try:
            if is_today:
                # 오늘 날짜 → should_call_league_today 로 호출 시간대인지 체크
                if not should_call_league_today(lid, target_date, now):
                    print(
                        f"  - league {lid}: 지금은 라이브 호출 시간대가 아님 → 스킵"
                    )
                    continue
                else:
                    print(
                        f"  - league {lid}: 라이브 호출 시간대 → /fixtures 호출"
                    )
            else:
                # 과거/미래 날짜 → 수동 백필용으로 전체 호출 허용
                print(
                    f"  - league {lid}: date={target_date} (today 아님) → /fixtures 전체 호출"
                )

            fixtures = fetch_fixtures_from_api(lid, target_date)
            print(f"    응답 경기 수: {len(fixtures)}")

            for fx in fixtures:
                # fixtures 테이블 기본 정보(upsert)
                upsert_fixture_row(fx, lid, None)
                # matches 테이블에는 상태만 upsert (스코어 제외)
                upsert_match_status_only(fx, lid, None)
                total_updated += 1

        except Exception as e:
            print(f"  ! league {lid} 처리 중 에러: {e}", file=sys.stderr)

    print(
        f"[update_live_fixtures] 완료. 총 상태 업데이트 경기 수 = {total_updated}"
    )


if __name__ == "__main__":
    main()
