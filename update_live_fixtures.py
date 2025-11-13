import os
import sys
import time
import datetime as dt
from typing import List, Any, Dict

import requests

from db import fetch_all, execute


API_KEY = os.environ.get("APIFOOTBALL_KEY")
LIVE_LEAGUES_ENV = os.environ.get("LIVE_LEAGUES", "")


def parse_live_leagues(env_val: str) -> List[int]:
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
    # utcnow() 경고 제거: timezone-aware 로 바꿈
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


def fetch_fixtures_from_api(league_id: int, date_str: str):
    """
    Api-Football v3 에서 특정 리그 + 날짜 경기를 가져온다.
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
#  시간 창 기반 호출 여부 판단 로직
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
            (너가 말한 '종료 1번'을 대충 맞추는 용도)
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
        # (여기선 대략 10분 이내만 한 번 더 보정)
        if -10 <= diff_minutes <= 10:
            return True
        return False

    # 그 외 상태는 보수적으로 안 부름
    return False


def should_call_league_today(league_id: int, date_str: str, now: dt.datetime) -> bool:
    """
    오늘(date_str) 기준으로, 해당 리그에
    '지금 업데이트가 필요한 경기'가 하나라도 있으면 True.
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
            # 오늘 날짜일 때만 '시간 창' 로직 적용
            if is_today:
                if not should_call_league_today(lid, target_date, now):
                    print(
                        f"  - league {lid}: 지금 업데이트가 필요한 경기가 없어 Api 호출 스킵"
                    )
                    continue
                else:
                    print(
                        f"  - league {lid}: 시간 창 조건 만족 → Api-Football 호출"
                    )
            else:
                # 과거/미래 특정 날짜 수동 실행 시에는 항상 호출 (백필용)
                print(
                    f"  - league {lid}: date={target_date} (today 아님) → 전체 백필 호출"
                )

            fixtures = fetch_fixtures_from_api(lid, target_date)
            print(f"    응답 경기 수: {len(fixtures)}")

            for row in fixtures:
                upsert_fixture_row(row)
                total_updated += 1

        except Exception as e:
            print(f"  ! league {lid} 처리 중 에러: {e}", file=sys.stderr)

    print(f"[update_live_fixtures] 완료. 총 업데이트/삽입 경기 수 = {total_updated}")


if __name__ == "__main__":
    main()
