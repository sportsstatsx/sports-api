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
    upsert_match_fixtures_raw,
    fetch_team_stats_from_api,
    upsert_match_team_stats,
    fetch_lineups_from_api,
    upsert_match_lineups,
)


# ───────────────────────────────
# 스탯 라이브 호출 쿨다운 (초 단위)
# ───────────────────────────────
STATS_INTERVAL_SEC = 60.0           # 팀 스탯: 1분에 한 번
LAST_STATS_SYNC = {}                # fixture_id -> 마지막 스탯 갱신시각(UNIX ts)

# ───────────────────────────────
# 라인업 호출 여부 메모리 캐시
# ───────────────────────────────
# 라인업은 "킥오프 전/직후에 한 번만" 받으면 되므로,
# 워커 프로세스 살아 있는 동안 fixture_id 를 기록해두고 중복 호출 방지.
LINEUPS_FETCHED = set()


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


def _maybe_fetch_lineups_once(
    fixture_id: int,
    date_utc: Optional[str],
    status_group: str,
    elapsed: Optional[int],
    now: dt.datetime,
) -> None:
    """
    라인업 인입 정책:

    - ❗ 경기당 1회만 호출 (LINEUPS_FETCHED 로 관리)
    - 프리매치:
        * status_group == 'NS' 이고
        * 킥오프까지 남은 시간이 0~60분 사이면 (1시간 전~직전)
          → 라인업 호출
    - 킥오프 직후:
        * status_group == 'INPLAY' 이고
        * elapsed 가 5분 이하일 때
          → 라인업 호출 (프리매치 타이밍을 놓쳤을 경우 대비)
    - API 응답이 비어 있으면 (라인업 아직 안 풀린 상태)
        → FETCHED 마킹 하지 않고 나중에 다시 시도
    """
    if fixture_id in LINEUPS_FETCHED:
        return

    if not date_utc:
        # 킥오프 시간을 모르면 프리매치 윈도우 계산 불가 → INPLAY fallback만 사용
        pass

    should_call = False

    # 1) 프리매치 윈도우 (NS + 킥오프 0~60분 전)
    if date_utc and status_group == "NS":
        try:
            kickoff = dt.datetime.fromisoformat(date_utc)
            # now, kickoff 는 둘 다 timezone-aware(UTC) 라고 가정
            minutes_to_kickoff = (kickoff - now).total_seconds() / 60.0
            # 예: 60분 전 ~ 직후 5분(-5분) 정도까지 허용해도 됨
            if -5.0 <= minutes_to_kickoff <= 60.0:
                should_call = True
        except Exception as e:
            print(
                f"      [lineups] fixture_id={fixture_id} date_utc 파싱 에러: {e}",
                file=sys.stderr,
            )

    # 2) 킥오프 직후 fallback (INPLAY + elapsed ≤ 5분)
    if status_group == "INPLAY":
        if elapsed is None or elapsed <= 5:
            # elapsed 가 없으면, 그냥 초반으로 보고 한 번은 시도해준다
            should_call = True

    if not should_call:
        return

    try:
        lineups = fetch_lineups_from_api(fixture_id)
        if lineups:
            upsert_match_lineups(fixture_id, lineups)
            LINEUPS_FETCHED.add(fixture_id)
            print(
                f"      [lineups] fixture_id={fixture_id} fetched and saved"
            )
        else:
            # 응답 비어 있음 → 아직 라인업이 안 풀린 상태. 나중에 다시 시도.
            print(
                f"      [lineups] fixture_id={fixture_id} response empty, will retry later"
            )
    except Exception as lu_err:
        print(
            f"      [lineups] fixture_id={fixture_id} 처리 중 에러: {lu_err}",
            file=sys.stderr,
        )


def main() -> None:
    """
    경량 라이브 워커.

    - 역할:
        * 경기 상태(시작 / 하프타임 / 종료), elapsed, 기본 팀/리그 정보 업데이트
        * INPLAY 경기의 실시간 스코어 및 이벤트(골/카드/교체) 인입
        * INPLAY 경기의 팀 스탯을 최대 1분에 1번만 인입
        * 라인업:
            - 킥오프 1시간 전 ~ 직전(프리매치)에 한 번
            - 킥오프 직후(INPLAY 초반)에 한 번 (프리매치 못 받았을 때 대비)
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
                date_utc = basic["date_utc"]
                elapsed = basic.get("elapsed")

                # 3) matches row 상태/스코어/elapsed 갱신 (NS / INPLAY / FINISHED 공통)
                upsert_match_row(fx, lid, None)

                # 4) FINISHED 경기는 여기서 라이브 처리만 스킵
                #    (라인업 / 이벤트 / 스탯 같은 추가 작업만 막고, matches 갱신은 이미 위에서 한 번 수행)
                if status_group == "FINISHED":
                    continue

                # 5) 라인업: 프리매치/직후 정책

                _maybe_fetch_lineups_once(
                    fixture_id=fixture_id,
                    date_utc=date_utc,
                    status_group=status_group,
                    elapsed=elapsed,
                    now=now,
                )

                # 6) INPLAY 경기만 이벤트/스탯 라이브 처리
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
