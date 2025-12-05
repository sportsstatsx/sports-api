import sys
import time
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
    upsert_match_row,
    fetch_events_from_api,
    upsert_match_events,
    upsert_match_events_raw,
    fetch_team_stats_from_api,
    upsert_match_team_stats,
)

# 스탯 라이브 호출 쿨다운 (초 단위: 60초 = 1분)
STATS_INTERVAL_SEC = 60.0
# fixture_id 별 마지막 스탯 갱신 시각 (UNIX timestamp)
LAST_STATS_SYNC = {}


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

    - 역할:
        * 경기 상태(시작 / 하프타임 / 종료), elapsed, 기본 팀/리그 정보 업데이트
        * INPLAY 경기의 실시간 스코어 및 이벤트(골/카드/교체) 인입
        * INPLAY 경기의 팀 스탯을 최대 1분에 1번만 인입
    - Api-Football 호출: /fixtures (+ /fixtures/events, /fixtures/statistics)
    - DB 작업:
        * fixtures 테이블: upsert_fixture_row
        * matches 테이블: upsert_match_row (스코어 + 상태 + elapsed)
        * match_events / match_events_raw: INPLAY 경기만 갱신
        * match_team_stats: INPLAY 경기만 60초 쿨다운으로 갱신
    - 절대 하지 않는 것:
        * FINISHED 경기의 이벤트/스코어/스탯 재갱신 (postmatch_backfill.py 에서 최종 정리)
        * standings / squads / players / transfers / toplists 등 정적 데이터
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
                # 1) fixtures 테이블 기본 정보(upsert) → 모든 경기 공통
                upsert_fixture_row(fx, lid, None)

                # 2) 기본 필드 추출 (fixture_id, status_group 등)
                basic = _extract_fixture_basic(fx)
                if basic is None:
                    continue

                fixture_id = basic["fixture_id"]
                status_group = basic["status_group"]

                # 3) FINISHED 경기는 여기서 스킵 (불필요한 라이브 처리 방지)
                if status_group == "FINISHED":
                    continue

                # 4) INPLAY 외(NS, POSTPONED 등)도 matches row 자체는 업데이트해도 무방
                #    (status / elapsed / 팀 정보 최신화 용도)
                upsert_match_row(fx, lid, None)

                # 5) INPLAY 경기만 이벤트(골/카드/교체) + 스탯 갱신
                if status_group == "INPLAY":
                    # ───────── EVENTS (10초마다)
                    try:
                        events = fetch_events_from_api(fixture_id)
                        upsert_match_events(fixture_id, events)
                        upsert_match_events_raw(fixture_id, events)
                    except Exception as ev_err:
                        print(
                            f"      [events] fixture_id={fixture_id} 처리 중 에러: {ev_err}",
                            file=sys.stderr,
                        )

                    # ───────── STATS (60초 쿨다운)
                    now_ts = time.time()
                    last_ts = LAST_STATS_SYNC.get(fixture_id)
                    should_sync_stats = False

                    if last_ts is None:
                        should_sync_stats = True
                    elif (now_ts - last_ts) >= STATS_INTERVAL_SEC:
                        should_sync_stats = True

                    if should_sync_stats:
                        try:
                            stats = fetch_team_stats_from_api(fixture_id)
                            upsert_match_team_stats(fixture_id, stats)
                            LAST_STATS_SYNC[fixture_id] = now_ts
                            print(
                                f"      [stats] fixture_id={fixture_id} updated"
                            )
                        except Exception as st_err:
                            print(
                                f"      [stats] fixture_id={fixture_id} 처리 중 에러: {st_err}",
                                file=sys.stderr,
                            )

                total_updated += 1

        except Exception as e:
            print(f"  ! league {lid} 처리 중 에러: {e}", file=sys.stderr)

    print(
        f"[update_live_fixtures] 완료. 총 상태 업데이트 경기 수 = {total_updated}"
    )


if __name__ == "__main__":
    main()
