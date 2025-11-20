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
    if not env_val:
        return ids

    for part in env_val.replace(" ", "").split(","):
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            continue
    return ids


# ─────────────────────────────────────
#  날짜/시간 유틸
# ─────────────────────────────────────


def parse_date(date_str: str) -> dt.date:
    """
    "2025-11-01" 형태의 문자열을 date 객체로 변환.
    """
    return dt.date.fromisoformat(date_str)


def parse_datetime_utc(val: Any) -> Optional[dt.datetime]:
    """
    DB 에서 읽은 date_utc / created_at 등의 값을 UTC aware datetime 으로 변환.
    문자열 / datetime / None 모두 처리.
    """
    if val is None:
        return None

    if isinstance(val, dt.datetime):
        if val.tzinfo is None:
            # tz 정보 없으면 UTC 로 간주
            return val.replace(tzinfo=dt.timezone.utc)
        return val.astimezone(dt.timezone.utc)

    s = str(val)
    try:
        # ISO8601 ("2025-11-01T15:00:00+00:00") 우선
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(
            dt.timezone.utc
        )
    except Exception:
        try:
            # "2025-11-01 15:00:00" 같은 형태
            return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=dt.timezone.utc
            )
        except Exception:
            try:
                # 날짜만 있는 경우("2025-11-01")
                d = dt.date.fromisoformat(s[:10])
                return dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)
            except Exception:
                return None


def now_utc() -> dt.datetime:
    """
    현재 UTC 시각.
    """
    return dt.datetime.now(dt.timezone.utc)


def _parse_kickoff_to_utc(val: Any) -> Optional[dt.datetime]:
    """
    matches/fixtures.date_utc 값(문자열 또는 datetime)을 UTC aware datetime 으로 변환.
    """
    if val is None:
        return None

    if isinstance(val, dt.datetime):
        if val.tzinfo is None:
            # tz 정보 없으면 UTC 로 간주
            return val.replace(tzinfo=dt.timezone.utc)
        return val.astimezone(dt.timezone.utc)

    s = str(val)
    try:
        # ISO8601 형태("2025-11-01T15:00:00+00:00" 등) 우선
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(
            dt.timezone.utc
        )
    except Exception:
        try:
            # "2025-11-01 15:00:00" 같은 형태
            return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=dt.timezone.utc
            )
        except Exception:
            try:
                # DATE 만 있을 수도 있음("2025-11-01")
                d = dt.date.fromisoformat(s[:10])
                return dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)
            except Exception:
                return None


# ─────────────────────────────────────
#  A/B 그룹 공통 쿼리
# ─────────────────────────────────────


def load_live_leagues_from_env() -> List[int]:
    """
    LIVE_LEAGUES 환경변수로부터 monitoring 대상 리그 목록을 읽어온다.
    """
    return parse_live_leagues(LIVE_LEAGUES_ENV)


def infer_season_for_league_and_date(
    league_id: int,
    date_str: str,
) -> int:
    """
    standings / fixtures 에서 시즌을 추론하기 위한 helper.

    기본 전략:
      1) matches 테이블에서 해당 리그/날짜의 시즌을 우선 찾는다.
      2) 없으면 fixtures 에서 시즌을 찾는다.
      3) 그래도 없으면 단순히 date_str 의 연도를 사용한다.
    """
    rows = fetch_all(
        """
        SELECT DISTINCT season
        FROM matches
        WHERE league_id = %s
          AND DATE(date_utc) = %s
        ORDER BY season DESC
        LIMIT 1
        """,
        (league_id, date_str),
    )
    if rows:
        return int(rows[0]["season"])

    rows = fetch_all(
        """
        SELECT DISTINCT season
        FROM fixtures
        WHERE league_id = %s
          AND DATE(date_utc) = %s
        ORDER BY season DESC
        LIMIT 1
        """,
        (league_id, date_str),
    )
    if rows:
        return int(rows[0]["season"])

    # 마지막 fallback: 날짜의 연도 사용
    d = parse_date(date_str)
    return d.year


# ─────────────────────────────────────
#  A그룹 호출 여부 (시간대 기반)
# ─────────────────────────────────────


def should_call_league_today(
    league_id: int,
    date_str: str,
    now: dt.datetime,
) -> bool:
    """
    오늘 리그의 A그룹(Api-Football 라이브) 호출 여부를 결정한다.

    ✅ 설계 목표
    - "오늘 이 리그에 경기가 있는지"는 fixtures 기준으로 판단한다.
    - 각 경기의 킥오프 기준으로,
        kickoff - 60분  ~  kickoff + 150분
      사이에 현재 시간이 들어오면 A그룹을 활성화한다.
    - 이렇게 하면:
        * 킥오프 1시간 전부터 경기 종료 후 어느 정도까지는
          크론이 도는 매 분마다 라이브를 갱신하고,
        * 그 외 시간대에는 불필요한 호출을 줄일 수 있다.
    """

    # 1) 오늘 날짜의 해당 리그 경기 일정(fixtures) 조회
    rows = fetch_all(
        """
        SELECT date_utc
        FROM fixtures
        WHERE league_id = %s
          AND DATE(date_utc) = %s
        """,
        (league_id, date_str),
    )
    if not rows:
        # 오늘 이 리그에 아예 경기가 없으면 A그룹을 돌릴 필요가 없다.
        return False

    # 2) 각 경기 킥오프 기준으로 "동적 구간" 안에 있는지 확인
    #    - kickoff - 60분  ~  kickoff + 150분
    #    이 구간 안에 현재(now)가 하나라도 걸리면 True.
    for (date_utc,) in rows:
        ko = _parse_kickoff_to_utc(date_utc)
        if ko is None:
            continue

        start = ko - dt.timedelta(minutes=60)
        end = ko + dt.timedelta(minutes=150)

        if start <= now <= end:
            return True

    # 3) 어떤 경기 기준으로도 "A그룹 구간"에 들지 않으면 False
    return False


# ─────────────────────────────────────
#  B그룹(정적 데이터) 호출 
# ─────────────────────────────────────


def detect_static_phase_for_league(
    league_id: int,
    date_str: str,
) -> Optional[str]:
    """
    B그룹(standings, team_season_stats 등)을 어느 시즌 기준으로 호출할지
    결정하기 위한 "정적 페이즈" 탐지기.

    예시:
      - 시즌 중에는 현재 시즌만 보면 되지만,
      - 시즌이 끝난 뒤에는 마지막 시즌을 기준으로 한동안 B그룹을 유지하고 싶을 수 있다.

    지금은 단순하게:
      - 주어진 date_str 에 매치가 하나라도 있으면 "in_season"
      - 없으면 None
    """
    rows = fetch_all(
        """
        SELECT 1
        FROM matches
        WHERE league_id = %s
          AND DATE(date_utc) = %s
        LIMIT 1
        """,
        (league_id, date_str),
    )
    if rows:
        return "in_season"
    return None


# ─────────────────────────────────────
#  리그/시즌 해석
# ─────────────────────────────────────


def resolve_league_season_for_date(
    league_id: int,
    date_str: str,
) -> int:
    """
    B그룹(standings 등)에서 사용하는 시즌 해석기.

    기본적으로 infer_season_for_league_and_date 와 동일하게 동작하지만,
    혹시라도 나중에 standings 기준으로 season 을 조정하고 싶을 때
    이 함수 하나만 수정하면 되도록 분리해 둔다.
    """
    return infer_season_for_league_and_date(league_id, date_str)
