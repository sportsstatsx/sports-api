# postmatch_backfill.py
#
# 역할:
#   - 경기 종료(FT)된 경기들에 대해 "한 번만" 무거운 데이터 전체 백필
#     * /fixtures → 스코어 포함 기본 정보 upsert (fixtures, matches)
#     * /fixtures/events → match_events / match_events_raw
#     * /fixtures/lineups → match_lineups
#     * /fixtures/statistics → match_team_stats
#     * /fixtures/players → match_player_stats
#   - B그룹 정적 데이터:
#     * PREMATCH 타이밍: standings, squads, players, transfers, rounds, toplists 등 준비
#     * POSTMATCH 타이밍: standings / toplists 등 후처리
#
# 특징:
#   - "이미 백필된 경기"는 다시 호출하지 않음
#     -> match_events 에 row 가 있는 fixture 는 스킵
#   - LIVE_LEAGUES 에 포함된 리그만 대상
#   - get_target_date() 기준 날짜만 처리

import sys
import datetime as dt
from typing import Optional, Dict, Any, List

from db import fetch_one
from live_fixtures_common import (
    LIVE_LEAGUES_ENV,
    parse_live_leagues,
    get_target_date,
    now_utc,
    detect_static_phase_for_league,
)
from live_fixtures_a_group import (
    fetch_fixtures_from_api,
    fetch_events_from_api,
    fetch_lineups_from_api,
    fetch_team_stats_from_api,
    fetch_player_stats_from_api,
    upsert_fixture_row,
    upsert_match_row,
    upsert_match_events,
    upsert_match_events_raw,
    upsert_match_lineups,
    upsert_match_team_stats,
    upsert_match_player_stats,
    _extract_fixture_basic,
)
from live_fixtures_b_group import (
    update_static_data_prematch_for_league,
    update_static_data_postmatch_for_league,
)


# ─────────────────────────────────────
#  유틸: 이미 백필된 경기인지 체크
# ─────────────────────────────────────

def is_fixture_already_backfilled(fixture_id: int) -> bool:
    """
    이 fixture_id 에 대해 postmatch 백필이 이미 수행되었는지 여부를 판단.

    여기서는 간단히:
      - match_events 테이블에 해당 fixture_id 로 row 가 하나라도 있으면
        "이미 postmatch 데이터가 들어간 경기"로 간주하고 스킵한다.

    필요하면:
      - match_lineups, match_team_stats, match_player_stats 도 함께 체크하는 방식으로 확장 가능.
    """
    row = fetch_one(
        """
        SELECT 1
        FROM match_events
        WHERE fixture_id = %s
        LIMIT 1
        """,
        (fixture_id,),
    )
    return row is not None


# ─────────────────────────────────────
#  A그룹: 한 경기의 상세 데이터 백필
# ─────────────────────────────────────

def backfill_postmatch_for_fixture(fixture_id: int) -> None:
    """
    한 경기(fixture_id)에 대해:
      - events
      - lineups
      - team stats
      - player stats
    를 Api-Football 에서 가져와 각각 upsert.

    ※ 이 함수는 "이미 백필 여부"를 체크하지 않는다.
       -> 호출 전에 is_fixture_already_backfilled(...) 으로 필터링할 것.
    """
    # 이벤트
    try:
        events = fetch_events_from_api(fixture_id)
    except Exception as e:
        print(f"    ! fixture {fixture_id}: events 호출 중 에러: {e}", file=sys.stderr)
        events = []

    if events:
        upsert_match_events(fixture_id, events)
        upsert_match_events_raw(fixture_id, events)

    # 라인업
    try:
        lineups = fetch_lineups_from_api(fixture_id)
    except Exception as e:
        print(f"    ! fixture {fixture_id}: lineups 호출 중 에러: {e}", file=sys.stderr)
        lineups = []

    if lineups:
        upsert_match_lineups(fixture_id, lineups)

    # 팀 통계
    try:
        stats = fetch_team_stats_from_api(fixture_id)
    except Exception as e:
        print(f"    ! fixture {fixture_id}: statistics 호출 중 에러: {e}", file=sys.stderr)
        stats = []

    if stats:
        upsert_match_team_stats(fixture_id, stats)

    # 선수 통계
    try:
        players_stats = fetch_player_stats_from_api(fixture_id)
    except Exception as e:
        print(f"    ! fixture {fixture_id}: players 호출 중 에러: {e}", file=sys.stderr)
        players_stats = []

    if players_stats:
        upsert_match_player_stats(fixture_id, players_stats)


# ─────────────────────────────────────
#  메인 로직
# ─────────────────────────────────────

def main() -> None:
    """
    경기 종료(FT) 이후 한 번에 전체 데이터를 백필하는 워커.

    - 대상:
        * LIVE_LEAGUES 에 포함된 리그
        * get_target_date() 가 가리키는 날짜(target_date)의 경기들
    - 수행 작업:
        * FINISHED 상태인 경기들에 대해
            1) /fixtures
               → upsert_fixture_row / upsert_match_row (스코어 포함)
            2) 이미 백필되었는지(match_events 존재 여부) 확인
               → 이미 있으면 스킵
               → 없으면 /events, /lineups, /statistics, /players 백필
        * B그룹 정적 데이터:
            - PREMATCH 타이밍: standings 등 프리매치 데이터 준비
            - POSTMATCH 타이밍: standings / toplists 등 후처리
    """
    target_date = get_target_date()
    live_leagues = parse_live_leagues(LIVE_LEAGUES_ENV)

    if not live_leagues:
        print(
            "[postmatch_backfill] LIVE_LEAGUES env 가 비어있습니다. 종료.",
            file=sys.stderr,
        )
        return

    today_str = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    is_today = target_date == today_str
    now = now_utc()

    print(
        f"[postmatch_backfill] date={target_date}, today={today_str}, "
        f"is_today={is_today}, leagues={live_leagues}"
    )

    total_fixtures_processed = 0
    total_fixtures_skipped = 0

    for lid in live_leagues:
        try:
            print(f"  - league {lid}: /fixtures 호출 → FINISHED 경기 후처리 진입")

            fixtures = fetch_fixtures_from_api(lid, target_date)
            print(f"    응답 경기 수: {len(fixtures)}")

            for fx in fixtures:
                basic = _extract_fixture_basic(fx)
                if basic is None:
                    continue

                if basic.get("status_group") != "FINISHED":
                    # 아직 진행 중이거나 시작 전인 경기는 postmatch 백필 대상이 아님
                    continue

                fixture_id = basic["fixture_id"]
                league_id = lid  # 또는 basic["league_id"]
                season = basic["season"]

                # 0) 이미 백필된 경기인지 먼저 체크
                if is_fixture_already_backfilled(fixture_id):
                    print(
                        f"    - fixture {fixture_id}: 이미 match_events 존재 → "
                        f"postmatch 백필 스킵"
                    )
                    total_fixtures_skipped += 1
                    # 그래도 /fixtures 기반 기본 정보/스코어는 최신으로 동기화할 수 있음
                    upsert_fixture_row(fx, league_id, season)
                    upsert_match_row(fx, league_id, season)
                    continue

                print(
                    f"    * fixture {fixture_id}: FINISHED → 스코어 + 상세 데이터 첫 백필"
                )

                # 1) /fixtures 기반 기본 정보 + 스코어 업데이트
                upsert_fixture_row(fx, league_id, season)
                upsert_match_row(fx, league_id, season)

                # 2) /events, /lineups, /statistics, /players 백필
                backfill_postmatch_for_fixture(fixture_id)
                total_fixtures_processed += 1

            # 3) B그룹: standings 등 정적 데이터
            if is_today:
                static_phase: Optional[str] = detect_static_phase_for_league(
                    lid, target_date, now
                )
                if static_phase == "PREMATCH":
                    print(
                        f"  - league {lid}: static_phase=PREMATCH → "
                        f"update_static_data_prematch_for_league 호출"
                    )
                    update_static_data_prematch_for_league(lid, target_date)
                elif static_phase == "POSTMATCH":
                    print(
                        f"  - league {lid}: static_phase=POSTMATCH → "
                        f"update_static_data_postmatch_for_league 호출"
                    )
                    update_static_data_postmatch_for_league(lid, target_date)

        except Exception as e:
            print(f"  ! league {lid} 처리 중 에러: {e}", file=sys.stderr)

    print(
        f"[postmatch_backfill] 완료. "
        f"신규 postmatch 백필 경기 수 = {total_fixtures_processed}, "
        f"이미 백필되어 스킵된 경기 수 = {total_fixtures_skipped}"
    )


if __name__ == "__main__":
    main()
