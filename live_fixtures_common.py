import os
import sys
import datetime as dt
from typing import List, Any, Dict, Optional

from db import fetch_all

# 공통 ENV
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
    LIVE_TARGET_DATE 환경변수 또는 CLI 인자가 있으면 그 값을 사용하고,
    없으면 오늘(UTC 기준) 날짜 문자열 "YYYY-MM-DD" 반환.
    """
    env = os.environ.get("LIVE_TARGET_DATE")
    if env:
        return env.strip()

    if len(sys.argv) >= 2:
        return sys.argv[1]

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

    return "UPCOMING"


def _parse_kickoff_to_utc(value: Any) -> dt.datetime | None:
    """
    Postgres 에서 넘어온 date_utc 를 UTC datetime 으로 변환.

    - DB에는 보통 "YYYY-MM-DD HH:MM:SS" (timezone 없는 naive 문자열)로 저장되어 있다고 가정.
    - 여기서는 그것을 'UTC 시각'이라고 보고, timezone-aware UTC datetime 으로 변환.
    """
    if value is None:
        return None

    if isinstance(value, dt.datetime):
        # timezone 이 없으면 UTC 로 가정해서 붙여준다.
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc)

    if isinstance(value, str):
        try:
            # "YYYY-MM-DD HH:MM:SS" 같은 포맷 가정
            dt_naive = dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            return dt_naive.replace(tzinfo=dt.timezone.utc)
        except ValueError:
            return None

    return None


# ─────────────────────────────────────
#  A그룹(라이브) 호출 타이밍 판단
# ─────────────────────────────────────

def match_needs_live_update(row: Dict[str, Any], now: dt.datetime) -> bool:
    """
    A그룹(라이브 데이터: matches/fixtures, 나중에 events/lineups/stats/odds 등)의
    '언제'를 정의하는 핵심 규칙.

    Δt = kickoff - now (분 단위)

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

    # ✅ 구버전 데이터에서 status_group 이 'NS', 'FT' 같은 short 코드로 들어가 있어도
    #    항상 map_status_group 으로 UPCOMING/INPLAY/FINISHED 로 정규화해서 사용
    raw_status = (row.get("status_group") or row.get("status") or "").upper()
    sg = map_status_group(raw_status)
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
        return True

    if sg == "FINISHED":
        if -10 <= diff_minutes <= 10:
            return True
        return False

    return False


def should_call_league_today(league_id: int, date_str: str, now: dt.datetime) -> bool:
    """
    오늘(date_str) 기준으로, 해당 리그에
    '지금 A그룹(라이브 데이터) 업데이트가 필요한 경기'가 하나라도 있으면 True.

    - matches 테이블 기준으로만 판단.
    """
    rows = fetch_all(
        """
        SELECT
            fixture_id,
            date_utc,
            status_group,
            status
        FROM matches
        WHERE league_id = %s
          AND SUBSTRING(date_utc FROM 1 FOR 10) = %s
        """,
        (league_id, date_str),
    )

    if not rows:
        return False

    for r in rows:
        if match_needs_live_update(r, now):
            return True

    return False


# ─────────────────────────────────────
#  B그룹(standings, team_season_stats 등) 호출 타이밍 판단
# ─────────────────────────────────────

def detect_static_phase_for_league(
    league_id: int,
    date_str: str,
    now: dt.datetime,
) -> Optional[str]:
    """
    standings, team_season_stats, squads, players, injuries, transfers,
    toplists, venues 등의 호출 타이밍 판단.

    반환값:
      - "PREMATCH"  : 킥오프 59~61분 구간에 해당하는 UPCOMING 경기 존재
      - "POSTMATCH" : 킥오프 기준 -10~+10분 구간에 해당하는 FINISHED 경기 존재
      - None        : 아직/더 이상 B그룹 호출할 타이밍 아님
    """
    rows = fetch_all(
        """
        SELECT
            fixture_id,
            date_utc,
            status_group,
            status
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

        # ✅ 여기서도 구버전 status_group/status 를 모두 정규화해서 사용
        raw_status = (r.get("status_group") or r.get("status") or "").upper()
        sg = map_status_group(raw_status)
        diff_minutes = (kickoff - now).total_seconds() / 60.0

        if sg == "UPCOMING" and 59 <= diff_minutes <= 61:
            return "PREMATCH"

        if sg == "FINISHED" and -10 <= diff_minutes <= 10:
            return "POSTMATCH"

    return None


# ─────────────────────────────────────
#  season 유추 (B그룹 공통)
# ─────────────────────────────────────

def resolve_league_season_for_date(league_id: int, date_str: str) -> Optional[int]:
    """
    standings, team_season_stats 등에서 사용할 season 을 matches 테이블에서 유추.
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
