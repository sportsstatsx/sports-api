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

    - 인자로 날짜가 들어오면 그 값을 사용
    - 없으면 오늘 날짜를 YYYY-MM-DD 로 반환
    """
    if len(sys.argv) >= 2:
        return sys.argv[1]
    return dt.date.today().isoformat()


def parse_live_leagues(env_val: str) -> List[int]:
    """
    LIVE_LEAGUES 환경변수("39,140,141") 등을 정수 리스트로 파싱.
    공백이나 잘못된 값은 무시한다.
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
    Api-Football 의 status.short / status_long / 기존 status_group 값을
    크게 세 그룹으로 정규화.

      - "UPCOMING"
      - "INPLAY"
      - "FINISHED"

    이미 "UPCOMING" / "INPLAY" / "FINISHED" 가 들어오면 그대로 사용.
    """
    if not code:
        return "UPCOMING"

    c = code.strip().upper()

    # 이미 정규화된 값이면 그대로 반환
    if c in {"UPCOMING", "INPLAY", "FINISHED"}:
        return c

    # Api-Football 의 short 코드 기준 대략적인 매핑
    upcoming_codes = {
        "NS", "TBD", "PST", "CANC", "ABD", "AWD", "WO",
    }
    inplay_codes = {
        "1H", "2H", "ET", "P", "LIVE", "BT", "HT",
    }
    finished_codes = {
        "FT", "AET", "PEN", "SUSP", "INT",
    }

    if c in upcoming_codes:
        return "UPCOMING"
    if c in inplay_codes:
        return "INPLAY"
    if c in finished_codes:
        return "FINISHED"

    # 알 수 없는 값이면 일단 UPCOMING 으로 본다.
    return "UPCOMING"


# ─────────────────────────────────────
#  킥오프 시각 파싱
# ─────────────────────────────────────


def _parse_kickoff_to_utc(val: Any) -> Optional[dt.datetime]:
    """
    matches.date_utc 등에 들어있는 값을 UTC datetime 으로 변환.

    - timezone-aware datetime  → UTC 로 변환
    - naive datetime          → UTC 로 가정
    - ISO 문자열              → fromisoformat 으로 파싱 후 위와 동일
    """
    if val is None:
        return None

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
#  A그룹(라이브 호출) 여부 판단
# ─────────────────────────────────────


def should_call_league_today(
    league_id: int,
    date_str: str,
    now: dt.datetime,
) -> bool:
    """
    경기 일정(matches.date_utc)을 기준으로
    "지금 이 리그에 대해 A그룹(Api-Football 라이브 호출)을 해야 하는지"
    를 최소 호출 + 안정성 위주로 판단한다.

    ✅ 변경 1: DATE(date_utc) = %s 필터 제거
        - 리그 전체 시즌을 대상으로, 지금 시각(now)과의 시간 차이로만 판단.
        - DB 에서 날짜가 하루 정도 밀려 있어도, 실제 시간 차이만 맞으면 잡을 수 있다.

    로직:
      diff_min = (now_utc - kickoff_utc) [분 단위]

      -30분  ≤ diff_min ≤ 120분   → A그룹 활성 (라이브/전후 포함)

    이 구간 밖이면:
      - 이 리그에 대해서는 지금 Api-Football 라이브 호출을 하지 않는다.

    ※ date_str 는 B그룹 등 다른 용도 호환을 위해 인자로만 유지,
       여기 로직은 오로지 DB 일정 + now(UTC)만 사용.
    """

    # 이 리그의 전체 경기를 한 번에 읽어온다.
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

        # 경기 직전/중/직후 (라이브 구간: 1분마다 호출)
        if -30.0 <= diff_min <= 120.0:
            return True

    # 어느 경기에도 해당 안 되면, 지금은 이 리그에 대해 라이브 호출할 필요 없음
    return False


# ─────────────────────────────────────
#  B그룹(정적 데이터) 호출 타이밍 감지
#   - PREMATCH: 킥오프 60분 전 ~ 30분 전 사이, 단
#               현재 시각의 분이 0 또는 30일 때만 1번씩(최대 2회)
#   - POSTMATCH: 경기 종료 후, 밤 21시 이후
# ─────────────────────────────────────


def detect_static_phase_for_league(
    league_id: int,
    date_str: str,
    now: dt.datetime,
) -> Optional[str]:
    """
    B그룹(standings / squads / players / transfers 등)을
    언제 호출할지 결정하는 헬퍼.

    ✅ 변경 2:
        PREMATCH 는 "킥오프 60분 전 ~ 30분 전" 구간이면서
        현재 시각의 분이 0 또는 30 인 경우에만 True.
        → 1분 크론 기준, 각 리그/날짜당 최대 2번만 PREMATCH 호출.

    POSTMATCH 는:
      - 오늘(date_str)에 FINISHED 경기들이 있고
      - now.hour >= 21 (UTC 기준) 인 경우만 "POSTMATCH" 반환.
    """

    # 오늘 날짜의 경기만 본다 (static 은 날짜 기반으로 충분)
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

    # now 를 UTC aware 로 정규화
    now_utc_val = now
    if now_utc_val.tzinfo is None:
        now_utc_val = now_utc_val.replace(tzinfo=dt.timezone.utc)
    else:
        now_utc_val = now_utc_val.astimezone(dt.timezone.utc)

    # ── 1) PREMATCH: 킥오프 60분 전 ~ 30분 전, 분이 0 또는 30일 때만 ──
    min_future_diff: Optional[float] = None

    for r in rows:
        kickoff_raw = r.get("date_utc")
        kickoff_utc = _parse_kickoff_to_utc(kickoff_raw)
        if kickoff_utc is None:
            continue

        diff_future_min = (kickoff_utc - now_utc_val).total_seconds() / 60.0

        # 미래 경기만 고려 (이미 시작/끝난 경기는 PREMATCH 대상 아님)
        if diff_future_min < 0:
            continue

        if min_future_diff is None or diff_future_min < min_future_diff:
            min_future_diff = diff_future_min

    # 가장 가까운 미래 경기까지 남은 시간이 60~30분 사이인 경우만 PREMATCH 후보
    if min_future_diff is not None and 30.0 <= min_future_diff <= 60.0:
        # 매분이 아니라, 시각의 분이 0 or 30 인 시점에만 PREMATCH 실행
        minute = now_utc_val.minute
        if minute in (0, 30):
            return "PREMATCH"

    # ── 2) POSTMATCH: 경기들이 전부 끝난 뒤, 밤 21시 이후 ──
    has_finished = False

    for r in rows:
        sg_raw = r.get("status_group") or r.get("status") or ""
        sg = map_status_group(sg_raw)
        if sg == "FINISHED":
            has_finished = True

    if has_finished:
        hour = now_utc_val.hour
        if hour >= 21:
            return "POSTMATCH"

    return None


# ─────────────────────────────────────
#  시즌 추론 / 해석 (A/B 공통)
# ─────────────────────────────────────


def _fetch_season_range_for_league(league_id: int) -> List[Dict[str, Any]]:
    """
    해당 리그에 대해 DB 에 저장된 시즌 목록을 가져온다.
    가장 단순하게 matches 테이블 기준 DISTINCT season 리스트를 사용.
    """
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
        season = r.get("season")
        if season is None:
            continue
        out.append({"season": int(season)})
    return out


def infer_season_for_league_and_date(
    league_id: int,
    date_str: str,
) -> int:
    """
    주어진 league_id + date_str(YYYY-MM-DD)에 대해 적절한 시즌을 추론.

    우선순위:
      1) matches 테이블에서 해당 날짜에 실제로 존재하는 season 이 있으면 그 값을 사용
      2) 없으면 해당 리그의 시즌 목록 중 가장 최신 시즌(max season)을 사용
    """
    # 1) 해당 날짜에 실제 경기 있는지 확인
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
    if rows:
        season = rows[0].get("season")
        if season is not None:
            return int(season)

    # 2) 없으면 시즌 범위에서 가장 최신 시즌 사용
    seasons = _fetch_season_range_for_league(league_id)
    if not seasons:
        raise ValueError(f"no season info found for league_id={league_id}")

    return int(seasons[-1]["season"])


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
