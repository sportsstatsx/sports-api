import os
import sys
import datetime as dt
from typing import List, Any, Dict, Optional

import requests

from db import fetch_all, execute


API_KEY = os.environ.get("APIFOOTBALL_KEY")
LIVE_LEAGUES_ENV = os.environ.get("LIVE_LEAGUES", "")


# ─────────────────────────────────────
#  공통 유틸
# ─────────────────────────────────────

def parse_live_leagues(env_val: str) -> List[int]:
    """
    LIVE_LEAGUES 환경변수("39,140,141") 등을 정수 리스트로 파싱.
    """
    ids: List[int] = []
    for part in env_val.replace(" ", "").split(","):
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            continue
    return ids


def get_target_date() -> str:
    """
    CLI 인자에 YYYY-MM-DD 가 들어오면 그 날짜,
    없으면 오늘(UTC)의 날짜 문자열을 반환.
    """
    if len(sys.argv) >= 2:
        return sys.argv[1]
    # timezone-aware UTC now
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


def now_utc() -> dt.datetime:
    """항상 timezone-aware UTC now."""
    return dt.datetime.now(dt.timezone.utc)


def map_status_group(short_code: str) -> str:
    """
    Api-Football status.short 코드를 우리 DB의 status_group 으로 변환.
    """
    s = (short_code or "").upper()

    inplay_codes = {
        "1H",
        "2H",
        "ET",
        "BT",
        "P",
        "LIVE",
        "INPLAY",
        "HT",
    }
    finished_codes = {
        "FT",
        "AET",
        "PEN",
    }
    upcoming_codes = {
        "NS",
        "TBD",
        "PST",
        "CANC",
        "SUSP",
        "INT",
    }

    if s in inplay_codes:
        return "INPLAY"
    if s in finished_codes:
        return "FINISHED"
    if s in upcoming_codes:
        return "UPCOMING"

    # 모르는 건 일단 UPCOMING 으로
    return "UPCOMING"


# ─────────────────────────────────────
#  Api-Football: fixtures (A그룹 - 라이브 핵심)
# ─────────────────────────────────────

def fetch_fixtures_from_api(league_id: int, date_str: str):
    """
    Api-Football v3 에서 특정 리그 + 날짜 경기를 가져온다.
    /fixtures?league={league_id}&date={YYYY-MM-DD}
    """
    if not API_KEY:
        raise RuntimeError("APIFOOTBALL_KEY env 가 설정되어 있지 않습니다.")

    url = "https://v3.football.api-sports.io/fixtures"
    headers = {
        "x-apisports-key": API_KEY,
    }
    params = {
        "league": league_id,
        "date": date_str,  # YYYY-MM-DD
        "timezone": "UTC",
    }

    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    # Api-Football 응답 형식: {"response": [ ... ]}
    return data.get("response", [])


def upsert_fixture_row(row: Dict[str, Any]):
    """
    Api-Football 한 경기 정보를 Postgres matches/fixtures 테이블에 upsert.
    (A그룹: 라이브 핵심 - 스코어/상태/킥오프 시간)
    """
    fixture = row.get("fixture", {})
    league = row.get("league", {})
    teams = row.get("teams", {})
    goals = row.get("goals", {})

    fixture_id = fixture.get("id")
    if fixture_id is None:
        return

    league_id = league.get("id")
    season = league.get("season")
    date_utc = fixture.get("date")  # ISO8601, TIMESTAMPTZ 로 캐스팅됨

    status_short = (fixture.get("status") or {}).get("short", "")
    status_group = map_status_group(status_short)

    home_team = teams.get("home") or {}
    away_team = teams.get("away") or {}

    home_id = home_team.get("id")
    away_id = away_team.get("id")

    home_ft = goals.get("home")
    away_ft = goals.get("away")

    # matches 테이블 upsert
    execute(
        """
        INSERT INTO matches (
            fixture_id, league_id, season, date_utc,
            status, status_group,
            home_id, away_id,
            home_ft, away_ft
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id    = EXCLUDED.league_id,
            season       = EXCLUDED.season,
            date_utc     = EXCLUDED.date_utc,
            status       = EXCLUDED.status,
            status_group = EXCLUDED.status_group,
            home_id      = EXCLUDED.home_id,
            away_id      = EXCLUDED.away_id,
            home_ft      = EXCLUDED.home_ft,
            away_ft      = EXCLUDED.away_ft
        """,
        (
            fixture_id,
            league_id,
            season,
            date_utc,
            status_short,
            status_group,
            home_id,
            away_id,
            home_ft,
            away_ft,
        ),
    )

    # fixtures 테이블 upsert (요약용)
    execute(
        """
        INSERT INTO fixtures (
            fixture_id, league_id, season, date_utc,
            status, status_group
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id    = EXCLUDED.league_id,
            season       = EXCLUDED.season,
            date_utc     = EXCLUDED.date_utc,
            status       = EXCLUDED.status,
            status_group = EXCLUDED.status_group
        """,
        (
            fixture_id,
            league_id,
            season,
            date_utc,
            status_short,
            status_group,
        ),
    )


# ─────────────────────────────────────
#  시간 창 기반 호출 여부 판단 (A그룹 용)
# ─────────────────────────────────────

def _parse_kickoff_to_utc(value: Any) -> dt.datetime | None:
    """
    Postgres 에서 넘어온 date_utc 를 UTC datetime 으로 변환.
    """
    if value is None:
        return None

    if isinstance(value, dt.datetime):
        # tz 정보 없으면 UTC 로 가정
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc)

    if isinstance(value, str):
        s = value.strip()
        # ISO8601 'Z' → '+00:00'
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            d = dt.datetime.fromisoformat(s)
        except ValueError:
            return None
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc)

    return None


def _match_needs_update(row: Dict[str, Any], now: dt.datetime) -> bool:
    """
    한 경기(row)가 지금 시점에서 Api-Football 업데이트가 필요한지 여부.

    🔵 A그룹(라이브 중심: matches/fixtures, 나중에 events/lineups/stats/odds 등)의
       '언제'를 정의하는 핵심 규칙.

    규칙(분 단위 Δt = kickoff - now):

      - UPCOMING:
          * 59~61분 전에 1번  (≈ 킥오프 1시간 전)
          * 29~31분 전에 1번  (≈ 킥오프 30분 전)
          *  -1~+1분 사이 1번 (≈ 킥오프 시점)

      - INPLAY:
          * 경기 중에는 항상 True (크론이 1분마다 돌기 때문에
            결과적으로 '경기 중 1분에 한 번' 호출)

      - FINISHED:
          * 킥오프 기준 ±10분 안쪽(대략 경기 직후/전후)만 한 번 더 보정
    """
    kickoff = _parse_kickoff_to_utc(row.get("date_utc"))
    if kickoff is None:
        return False

    sg = (row.get("status_group") or "").upper()
    diff_minutes = (kickoff - now).total_seconds() / 60.0

    if sg == "UPCOMING":
        if 59 <= diff_minutes <= 61:
            return True
        if 29 <= diff_minutes <= 31:
            return True
        if -1 <= diff_minutes <= 1:
            return True
        return False

    if sg == "INPLAY":
        # 경기 중이면 크론이 1분마다 돌면서 항상 True → 1분당 1번 호출
        return True

    if sg == "FINISHED":
        # 킥오프 기준으로 너무 오래된 경기는 굳이 다시 안 부름
        # (대략 10분 이내만 한 번 더 보정)
        if -10 <= diff_minutes <= 10:
            return True
        return False

    # 그 외 상태는 보수적으로 안 부름
    return False


def should_call_league_today(league_id: int, date_str: str, now: dt.datetime) -> bool:
    """
    오늘(date_str) 기준으로, 해당 리그에
    '지금 A그룹(라이브 데이터) 업데이트가 필요한 경기'가 하나라도 있으면 True.
    """
    rows = fetch_all(
        """
        SELECT
            fixture_id,
            date_utc,
            status_group
        FROM matches
        WHERE league_id = %s
          AND SUBSTRING(date_utc FROM 1 FOR 10) = %s
        """,
        (league_id, date_str),
    )

    if not rows:
        # 이 날짜에 등록된 경기가 없으면 굳이 API 호출 안 함
        return False

    for r in rows:
        if _match_needs_update(r, now):
            return True

    return False


# ─────────────────────────────────────
#  B그룹(느리게 바뀌는 애들) - 언제 호출할지 판단
#   - 킥오프 1시간 전 (PREMATCH) 1회
#   - 경기 종료 직후 (POSTMATCH) 1회
# ─────────────────────────────────────

def _detect_static_phase_for_league(
    league_id: int,
    date_str: str,
    now: dt.datetime,
) -> Optional[str]:
    """
    B그룹(standings, team_season_stats, squads, players, injuries, transfers,
    toplists, venues 등)을 언제 호출할지 결정.

    반환값:
      - "PREMATCH"  : 킥오프 59~61분 구간에 해당하는 UPCOMING 경기 존재
      - "POSTMATCH" : 킥오프 기준 -10 ~ +10분 구간에 해당하는 FINISHED 경기 존재
      - None        : 아직/더 이상 B그룹 호출할 타이밍 아님
    """
    rows = fetch_all(
        """
        SELECT
            fixture_id,
            date_utc,
            status_group
        FROM matches
        WHERE league_id = %s
          AND SUBSTRING(date_utc FROM 1 FOR 10) = %s
        """,
        (league_id, date_str),
    )

    if not rows:
        return None

    for r in rows:
        kickoff = _parse_kickoff_to_utc(r.get("date_utc"))
        if kickoff is None:
            continue

        sg = (r.get("status_group") or "").upper()
        diff_minutes = (kickoff - now).total_seconds() / 60.0

        # PREMATCH: 킥오프 59~61분 전
        if sg == "UPCOMING" and 59 <= diff_minutes <= 61:
            return "PREMATCH"

        # POSTMATCH: 킥오프 기준 -10~+10분 (경기 종료 직후 근처)
        if sg == "FINISHED" and -10 <= diff_minutes <= 10:
            return "POSTMATCH"

    return None


# ─────────────────────────────────────
#  standings (B그룹 첫 번째 테이블) 구현
# ─────────────────────────────────────

def _resolve_league_season_for_date(league_id: int, date_str: str) -> Optional[int]:
    """
    standings 호출에 사용할 season 을 matches 테이블에서 유추.
    - 해당 리그 + 해당 날짜의 경기 중 season 이 가장 큰 값 사용.
    - 없으면 None 반환.
    """
    rows = fetch_all(
        """
        SELECT DISTINCT season
        FROM matches
        WHERE league_id = %s
          AND SUBSTRING(date_utc FROM 1 FOR 10) = %s
        ORDER BY season DESC
        LIMIT 1
        """,
        (league_id, date_str),
    )
    if not rows:
        return None
    return rows[0]["season"]


def fetch_standings_from_api(league_id: int, season: int) -> List[Dict[str, Any]]:
    """
    Api-Football /standings 엔드포인트 호출.
    응답 형식 (대략):

    {
      "response": [
        {
          "league": {
            "id": 39,
            "season": 2024,
            "standings": [
              [ { ... 팀1 ... }, { ... 팀2 ... }, ... ],  # 그룹 1
              [ { ... }, ... ]                           # 그룹 2 (있을 수도)
            ]
          }
        }
      ]
    }

    우리는 league.standings 의 2중 리스트를 평탄화해서 사용.
    """
    if not API_KEY:
        raise RuntimeError("APIFOOTBALL_KEY env 가 설정되어 있지 않습니다.")

    url = "https://v3.football.api-sports.io/standings"
    headers = {
        "x-apisports-key": API_KEY,
    }
    params = {
        "league": league_id,
        "season": season,
    }

    resp = requests.get(url, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    resp_list = data.get("response") or []
    if not resp_list:
        return []

    league_obj = resp_list[0].get("league") or {}
    raw_standings = league_obj.get("standings") or []

    flat_rows: List[Dict[str, Any]] = []
    for group_table in raw_standings:
        # group_table: 한 그룹(예: Overall / Group A)의 팀 리스트
        for team_row in group_table:
            flat_rows.append(team_row)

    return flat_rows


def upsert_standings(league_id: int, season: int, rows: List[Dict[str, Any]]):
    """
    standings 테이블(upsert).
    스키마 (user 제공):

      standings (
        league_id     integer not null,
        season        integer not null,
        group_name    text    not null default 'Overall',
        rank          integer not null,
        team_id       integer not null,
        points        integer,
        goals_diff    integer,
        played        integer,
        win           integer,
        draw          integer,
        lose          integer,
        goals_for     integer,
        goals_against integer,
        form          text,
        updated_utc   text,
        description   text,
        PRIMARY KEY (league_id, season, group_name, team_id)
      )
    """
    if not rows:
        print(f"    [standings] league={league_id}, season={season}: 응답 0 rows → 스킵")
        return

    now_iso = now_utc().isoformat()

    for row in rows:
        team = row.get("team") or {}
        stats_all = (row.get("all") or {})  # all: { played, win, draw, lose, goals: { for, against } }
        goals = stats_all.get("goals") or {}

        team_id = team.get("id")
        if team_id is None:
            continue

        group_name = row.get("group") or "Overall"
        rank = row.get("rank")
        points = row.get("points")
        goals_diff = row.get("goalsDiff")
        played = stats_all.get("played")
        win = stats_all.get("win")
        draw = stats_all.get("draw")
        lose = stats_all.get("lose")
        goals_for = goals.get("for")
        goals_against = goals.get("against")
        form = row.get("form")
        description = row.get("description")

        execute(
            """
            INSERT INTO standings (
                league_id,
                season,
                group_name,
                rank,
                team_id,
                points,
                goals_diff,
                played,
                win,
                draw,
                lose,
                goals_for,
                goals_against,
                form,
                updated_utc,
                description
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (league_id, season, group_name, team_id) DO UPDATE SET
                rank          = EXCLUDED.rank,
                points        = EXCLUDED.points,
                goals_diff    = EXCLUDED.goals_diff,
                played        = EXCLUDED.played,
                win           = EXCLUDED.win,
                draw          = EXCLUDED.draw,
                lose          = EXCLUDED.lose,
                goals_for     = EXCLUDED.goals_for,
                goals_against = EXCLUDED.goals_against,
                form          = EXCLUDED.form,
                updated_utc   = EXCLUDED.updated_utc,
                description   = EXCLUDED.description
            """,
            (
                league_id,
                season,
                group_name,
                rank,
                team_id,
                points,
                goals_diff,
                played,
                win,
                draw,
                lose,
                goals_for,
                goals_against,
                form,
                now_iso,
                description,
            ),
        )


def update_standings_for_league(league_id: int, date_str: str, phase: str):
    """
    PREMATCH / POSTMATCH 타이밍에서 standings 를 갱신.
    phase: "PREMATCH" 또는 "POSTMATCH"
    """
    season = _resolve_league_season_for_date(league_id, date_str)
    if season is None:
        print(
            f"    [standings {phase}] league={league_id}, date={date_str}: "
            f"matches 에서 season 을 찾지 못해 스킵"
        )
        return

    print(
        f"    [standings {phase}] league={league_id}, season={season}, date={date_str} → Api-Football 호출"
    )
    try:
        rows = fetch_standings_from_api(league_id, season)
        print(f"    [standings {phase}] 응답 팀 수={len(rows)}")
        upsert_standings(league_id, season, rows)
    except Exception as e:
        print(
            f"    [standings {phase}] league={league_id}, season={season} 처리 중 에러: {e}",
            file=sys.stderr,
        )


# ─────────────────────────────────────
#  B그룹 실제 갱신 함수 (현재는 standings만 구현, 나머지는 차례로 추가 예정)
# ─────────────────────────────────────

def update_static_data_prematch_for_league(league_id: int, date_str: str):
    """
    B그룹 데이터(standings, team_season_stats, squads, players, injuries, transfers,
    toplists, venues 등)를 '킥오프 1시간 전' 타이밍에 갱신하는 자리.

    지금 단계에서는 standings 만 실제 구현.
    추후 team_season_stats, squads, players, injuries, transfers, toplists, venues 등을
    이 함수 내부에 추가해 나갈 예정.
    """
    print(f"    [STATIC PREMATCH] league={league_id}, date={date_str}")
    update_standings_for_league(league_id, date_str, phase="PREMATCH")
    # TODO: 여기 아래에 team_season_stats, squads, players, injuries, transfers, toplists, venues 등
    #       순서대로 추가 예정.


def update_static_data_postmatch_for_league(league_id: int, date_str: str):
    """
    B그룹 데이터(standings, team_season_stats, toplists 등)를
    '경기 종료 직후(킥오프 기준 ±10분)' 타이밍에 갱신하는 자리.

    지금 단계에서는 standings 만 실제 구현.
    추후 team_season_stats, toplists 등을 이 함수 내부에 추가해 나갈 예정.
    """
    print(f"    [STATIC POSTMATCH] league={league_id}, date={date_str}")
    update_standings_for_league(league_id, date_str, phase="POSTMATCH")
    # TODO: 여기 아래에 team_season_stats, toplists 등 순서대로 추가 예정.


# ─────────────────────────────────────
#  메인 루프
# ─────────────────────────────────────

def main():
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

            # 오늘 날짜일 때만 "시간 창" 로직 적용 (라이브 + B그룹 스케줄)
            if is_today:
                # A그룹(라이브 데이터) 필요 여부 체크
                if not should_call_league_today(lid, target_date, now):
                    # B그룹(정적 데이터) 타이밍도 동시에 오는 경우가 있을 수 있으므로,
                    # 먼저 스케줄을 한번 확인해본다.
                    static_phase = _detect_static_phase_for_league(lid, target_date, now)
                    if static_phase is None:
                        print(
                            f"  - league {lid}: 지금 업데이트가 필요한 경기가 없어 "
                            f"Api 호출 스킵 (A/B 모두 해당 없음)"
                        )
                        continue
                    else:
                        # A그룹 스킵이더라도, B그룹(프리매치/포스트매치)만 호출할 수도 있음
                        print(
                            f"  - league {lid}: A그룹은 필요 없지만 "
                            f"static_phase={static_phase} → B그룹만 처리"
                        )
                else:
                    print(
                        f"  - league {lid}: 시간 창 조건 만족 → Api-Football 호출 (A그룹)"
                    )
                    # A그룹 호출과 별개로 B그룹 스케줄도 같이 확인
                    static_phase = _detect_static_phase_for_league(lid, target_date, now)
            else:
                # 과거/미래 특정 날짜 수동 실행 시에는 항상 호출 (백필용)
                print(
                    f"  - league {lid}: date={target_date} (today 아님) → 전체 백필 호출"
                )

            # A/B 그룹 중 어느 쪽이든 작업할 필요가 있는 상태에서만 fixtures 호출
            fixtures = fetch_fixtures_from_api(lid, target_date)
            print(f"    응답 경기 수: {len(fixtures)}")

            for row in fixtures:
                # A그룹: 라이브 핵심 fixtures/matches upsert
                upsert_fixture_row(row)
                total_updated += 1

            # B그룹: 느리게 바뀌는 데이터 - PRE/POST 두 타이밍만 1회씩
            if is_today and static_phase == "PREMATCH":
                update_static_data_prematch_for_league(lid, target_date)
            elif is_today and static_phase == "POSTMATCH":
                update_static_data_postmatch_for_league(lid, target_date)

        except Exception as e:
            print(f"  ! league {lid} 처리 중 에러: {e}", file=sys.stderr)

    print(f"[update_live_fixtures] 완료. 총 업데이트/삽입 경기 수 = {total_updated}")


if __name__ == "__main__":
    main()
