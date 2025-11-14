import sys
import datetime as dt
from typing import Optional

from live_fixtures_common import (
    LIVE_LEAGUES_ENV,
    parse_live_leagues,
    get_target_date,
    now_utc,
    should_call_league_today,
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
)
from live_fixtures_b_group import (
    update_static_data_prematch_for_league,
    update_static_data_postmatch_for_league,
)


def main() -> None:
    target_date = get_target_date()
    live_leagues = parse_live_leagues(LIVE_LEAGUES_ENV)

    if not live_leagues:
        print("LIVE_LEAGUES env 에 리그 ID 가 없습니다. 종료.", file=sys.stderr)
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
            static_phase: Optional[str] = None
            a_group_active: bool = False

            if is_today:
                # A그룹(라이브) 필요 여부
                if not should_call_league_today(lid, target_date, now):
                    # A는 필요 없지만 B(PREMATCH/POSTMATCH)만 필요할 수도 있음
                    static_phase = detect_static_phase_for_league(lid, target_date, now)
                    if static_phase is None:
                        print(
                            f"  - league {lid}: 지금 업데이트가 필요한 경기가 없어 "
                            f"Api 호출 스킵 (A/B 모두 해당 없음)"
                        )
                        continue
                    else:
                        print(
                            f"  - league {lid}: A그룹은 필요 없지만 "
                            f"static_phase={static_phase} → B그룹만 처리"
                        )
                        a_group_active = False
                else:
                    print(
                        f"  - league {lid}: 시간 창 조건 만족 → Api-Football 호출 (A그룹)"
                    )
                    static_phase = detect_static_phase_for_league(lid, target_date, now)
                    a_group_active = True
            else:
                print(
                    f"  - league {lid}: date={target_date} (today 아님) → 전체 백필 호출"
                )
                # 과거/미래 날짜 전체 백필 시에는 A그룹 데이터도 같이 채움
                a_group_active = True

            # A/B 그룹 중 하나라도 필요하면 fixtures 호출
            fixtures = fetch_fixtures_from_api(lid, target_date)
            print(f"    응답 경기 수: {len(fixtures)}")

            for row in fixtures:
                # A그룹: 라이브 핵심( fixtures / matches )
                # season 은 None 으로 넘기면, 내부에서 Api-Football 응답의 season 을 fallback 으로 사용
                upsert_fixture_row(row, lid, None)
                upsert_match_row(row, lid, None)
                total_updated += 1

                if not a_group_active:
                    continue

                fixture_block = row.get("fixture") or {}
                fid = fixture_block.get("id")
                if not fid:
                    continue

                # 이벤트
                try:
                    events = fetch_events_from_api(fid)
                except Exception as e:
                    print(
                        f"    ! fixture {fid}: events 호출 중 에러: {e}",
                        file=sys.stderr,
                    )
                    events = []

                if events:
                    upsert_match_events(fid, events)
                    upsert_match_events_raw(fid, events)

                # 라인업
                try:
                    lineups = fetch_lineups_from_api(fid)
                except Exception as e:
                    print(
                        f"    ! fixture {fid}: lineups 호출 중 에러: {e}",
                        file=sys.stderr,
                    )
                    lineups = []

                if lineups:
                    upsert_match_lineups(fid, lineups)

                # 팀 통계
                try:
                    stats = fetch_team_stats_from_api(fid)
                except Exception as e:
                    print(
                        f"    ! fixture {fid}: statistics 호출 중 에러: {e}",
                        file=sys.stderr,
                    )
                    stats = []

                if stats:
                    upsert_match_team_stats(fid, stats)

                # 선수 통계
                try:
                    players_stats = fetch_player_stats_from_api(fid)
                except Exception as e:
                    print(
                        f"    ! fixture {fid}: players 호출 중 에러: {e}",
                        file=sys.stderr,
                    )
                    players_stats = []

                if players_stats:
                    upsert_match_player_stats(fid, players_stats)

            # B그룹: standings 등 정적 데이터 (지금은 standings만, 나중에 확장)
            if is_today and static_phase == "PREMATCH":
                update_static_data_prematch_for_league(lid, target_date)
            elif is_today and static_phase == "POSTMATCH":
                update_static_data_postmatch_for_league(lid, target_date)

        except Exception as e:
            print(f"  ! league {lid} 처리 중 에러: {e}", file=sys.stderr)

    print(f"[update_live_fixtures] 완료. 총 업데이트/삽입 경기 수 = {total_updated}")


if __name__ == "__main__":
    main()
