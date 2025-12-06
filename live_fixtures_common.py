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

def now_utc() -> dt.datetime:
    """
    항상 timezone-aware UTC datetime 을 반환.
    """
    return dt.datetime.now(dt.timezone.utc)


def get_target_date() -> str:
    """
    update_live_fixtures.py 등에서 사용하는 대상 날짜 문자열(YYYY-MM-DD).
    """
    if len(sys.argv) >= 2:
        return sys.argv[1]
    return dt.date.today().isoformat()


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

# ─────────────────────────────────────
#  status 정규화
# ─────────────────────────────────────

def map_status_group(code: str) -> str:
    """
    Api-Football 의 status.short 등을 크게 3그룹으로 정규화.
    """
    if not code:
        return "UPCOMING"

    c = code.strip().upper()

    if c in {"UPCOMING", "INPLAY", "FINISHED"}:
        return c

    upcoming_codes = {"NS", "TBD", "PST", "CANC", "ABD", "AWD", "WO"}
    inplay_codes = {"1H", "2H", "ET", "P", "LIVE", "BT", "HT"}
    finished_codes = {"FT", "AET", "PEN", "SUSP", "INT"}

    if c in upcoming_codes:
        return "UPCOMING"
    if c in inplay_codes:
        return "INPLAY"
    if c in finished_codes:
        return "FINISHED"

    return "UPCOMING"

# ─────────────────────────────────────
#  킥오프 시각 파싱
# ─────────────────────────────────────

def _parse_kickoff_to_utc(val: Any) -> Optional[dt.datetime]:
    """
    matches.date_utc 값을 UTC datetime 으로 변환.
    """
    if isinstance(val, dt.datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=dt.timezone.utc)
        return val.astimezone(dt.timezone.utc)

    if isinstance(val, str):
        try:
            dt_val = dt.datetime.fromisoformat(val)
        except ValueError:
            return None
        if dt_val.tzinfo is None:
            return dt_val.replace(tzinfo=dt.timezone.utc)
        return dt_val.astimezone(dt.timezone.utc)

    return None

# ─────────────────────────────────────
#  ✅ A그룹(라이브 호출) 여부 판단
# ─────────────────────────────────────

def should_call_league_today(
    league_id: int,
    date_str: str,
    now: dt.datetime,
) -> bool:
    """
    ✅ 핵심 보강 포인트
    - DATE(date_utc) 필터를 사용하지 않음
    - '지연 시작 / 재개 경기'를 위해 넉넉한 시간 범위를 허용
    - elapsed 가 멈췄다 다시 뛰는 구조에서도 A그룹 호출 유지

    활성 조건:
      kickoff 기준
      -60분 ≤ diff_min ≤ 240분
    """

    rows = fetch_all(
        """
        SELECT fixture_id, date_utc
        FROM matches
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if not rows:
        return False

    # now 를 UTC aware 로 정규화
    now_utc_val = now
    if now_utc_val.tzinfo is None:
        now_utc_val = now_utc_val.replace(tzinfo=dt.timezone.utc)
    else:
        now_utc_val = now_utc_val.astimezone(dt.timezone.utc)

    for r in rows:
        kickoff_raw = r.get("date_utc")
        kickoff_utc = _parse_kickoff_to_utc(kickoff_raw)
        if kickoff_utc is None:
            continue

        diff_min = (now_utc_val - kickoff_utc).total_seconds() / 60.0

        # ✅ 지연 / 재개 경기까지 커버
        if -60.0 <= diff_min <= 240.0:
            return True

    return False

# ─────────────────────────────────────
#  B그룹(정적 데이터) 호출 타이밍
# ─────────────────────────────────────

def detect_static_phase_for_league(
    league_id: int,
    date_str: str,
    now: dt.datetime,
) -> Optional[str]:
    """
    B그룹(STANDINGS 등) 호출 타이밍 판단.
    (기존 로직 그대로 유지)
    """

    rows = fetch_all(
        """
        SELECT status_group, status, date_utc
        FROM matches
        WHERE league_id = %s
          AND DATE(date_utc) = %s
        """,
        (league_id, date_str),
    )
    if not rows:
        return None

    now_utc_val = now
    if now_utc_val.tzinfo is None:
        now_utc_val = now_utc_val.replace(tzinfo=dt.timezone.utc)
    else:
        now_utc_val = now_utc_val.astimezone(dt.timezone.utc)

    # PREMATCH
    min_future_diff: Optional[float] = None
    for r in rows:
        kickoff_utc = _parse_kickoff_to_utc(r.get("date_utc"))
        if kickoff_utc is None:
            continue

        diff_future = (kickoff_utc - now_utc_val).total_seconds() / 60.0
        if diff_future < 0:
            continue

        if min_future_diff is None or diff_future < min_future_diff:
            min_future_diff = diff_future

    if min_future_diff is not None:
        if 30.0 <= min_future_diff <= 60.0 and now_utc_val.minute in (0, 30):
            return "PREMATCH"

    # POSTMATCH
    has_finished = False
    for r in rows:
        sg = map_status_group(r.get("status_group") or r.get("status") or "")
        if sg == "FINISHED":
            has_finished = True

    if has_finished and now_utc_val.hour >= 21:
        return "POSTMATCH"

    return None

# ─────────────────────────────────────
#  시즌 추론
# ─────────────────────────────────────

def _fetch_season_range_for_league(league_id: int) -> List[Dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT DISTINCT season
        FROM matches
        WHERE league_id = %s
        ORDER BY season
        """,
        (league_id,),
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        s = r.get("season")
        if s is not None:
            out.append({"season": int(s)})
    return out


def infer_season_for_league_and_date(
    league_id: int,
    date_str: str,
) -> int:
    rows = fetch_all(
        """
        SELECT season
        FROM matches
        WHERE league_id = %s
          AND DATE(date_utc) = %s
        ORDER BY season DESC
        LIMIT 1
        """,
        (league_id, date_str),
    )
    if rows and rows[0].get("season") is not None:
        return int(rows[0]["season"])

    seasons = _fetch_season_range_for_league(league_id)
    if not seasons:
        raise ValueError(f"no season info found for league_id={league_id}")

    return int(seasons[-1]["season"])


def resolve_league_season_for_date(
    league_id: int,
    date_str: str,
) -> int:
    return infer_season_for_league_and_date(league_id, date_str)
