import os
import sys
import datetime as dt
from typing import List, Any, Dict, Optional

from db import fetch_all

# 공통 ENV
API_KEY = os.environ.get("APIFOOTBALL_KEY")
LIVE_LEAGUES_ENV = os.environ.get("LIVE_LEAGUES", "")


# ─────────────────────────────────────
#  기본 유틸
# ─────────────────────────────────────

def now_utc() -> dt.datetime:
    """UTC 기준 현재 시각 (aware datetime)."""
    return dt.datetime.now(dt.timezone.utc)


def get_target_date() -> str:
    """update_live_fixtures.py 에서 사용하는 대상 날짜.

    - 인자가 있으면: python update_live_fixtures.py 2025-11-20  → 그 날짜
    - 인자가 없으면: 오늘(UTC 날짜)
    """
    if len(sys.argv) >= 2:
        return sys.argv[1]
    return now_utc().strftime("%Y-%m-%d")


def parse_live_leagues(env_val: str) -> List[int]:
    """LIVE_LEAGUES 환경변수("39,140,141") 등을 정수 리스트로 파싱."""
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


def map_status_group(code: str) -> str:
    """Api-Football status.short 코드를 status_group 으로 매핑.

    앱 쪽 MatchRepository.kt 에서
      "INPLAY", "LIVE", "1H", "HT", "2H", "ET", "BT", "FT", ...
    등을 사용하므로, 여기서 최대한 맞춰준다.
    """
    c = (code or "").upper()

    if c in {"1H", "2H", "ET", "P", "IP", "LIVE", "INPLAY"}:
        return "INPLAY"
    if c in {"HT", "BT"}:
        return "BT"  # 하프타임/브레이크
    if c in {"FT", "AET", "PEN"}:
        return "FINISHED"
    if c in {"NS", "TBD"}:
        return "NOT_STARTED"
    if c in {"PST", "CANC", "ABD", "AWD", "WO", "SUSP", "INT"}:
        return "POSTPONED"

    # 그 외 알 수 없는 값은 일단 NOT_STARTED 로 본다.
    return "NOT_STARTED"


def _parse_kickoff_to_utc(val: Any) -> Optional[dt.datetime]:
    """fixtures/matches.date_utc 값을 UTC aware datetime 으로 변환."""
    if val is None:
        return None

    if isinstance(val, dt.datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=dt.timezone.utc)
        return val.astimezone(dt.timezone.utc)

    s = str(val)
    # ISO8601 + 'Z' 형태 먼저 처리
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(
            dt.timezone.utc
        )
    except Exception:
        pass

    # "YYYY-MM-DD HH:MM:SS" 형태
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=dt.timezone.utc
        )
    except Exception:
        pass

    # 날짜만 있을 경우
    try:
        d = dt.date.fromisoformat(s[:10])
        return dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)
    except Exception:
        return None


# ─────────────────────────────────────
#  시즌/리그 유틸
# ─────────────────────────────────────

def _fetch_season_range_for_league(league_id: int) -> List[Dict[str, Any]]:
    """matches 테이블에서 리그별 season / 최소일 / 최대일 을 가져온다."""
    rows = fetch_all(
        """
        SELECT
            season,
            MIN(date_utc) AS min_date_utc,
            MAX(date_utc) AS max_date_utc
        FROM matches
        WHERE league_id = %s
        GROUP BY season
        ORDER BY season
        """,
        (league_id,),
    )
    return rows or []


def infer_season_for_league_and_date(league_id: int, date_str: str) -> int:
    """리그+날짜 기준으로 사용할 season 을 추론.

    우선순위:
      1) matches 에서 해당 날짜에 실제로 존재하는 season
      2) fixtures 에서 해당 날짜의 season
      3) 마지막 fallback: date_str 의 연도
    """
    rows = fetch_all(
        """
        SELECT DISTINCT season
        FROM matches
        WHERE league_id = %s
          AND DATE(date_utc) = %s
        ORDER BY season DESC
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
        """,
        (league_id, date_str),
    )
    if rows:
        return int(rows[0]["season"])

    d = dt.date.fromisoformat(date_str)
    return d.year


def resolve_league_season_for_date(league_id: int, date_str: str) -> int:
    """B그룹(standings, team_season_stats) 에서 사용하는 시즌 해석기."""
    return infer_season_for_league_and_date(league_id, date_str)


# ─────────────────────────────────────
#  A그룹(라이브) 호출 여부
# ─────────────────────────────────────

def should_call_league_today(
    league_id: int,
    date_str: str,
    now: dt.datetime,
) -> bool:
    """오늘 해당 리그에 대해 A그룹(Api-Football 라이브)을 호출할지 여부.

    설계 목표:
      - "라이브 중"에는 1분마다 호출 (지금처럼 cron 1분 단위).
      - 킥오프 전에는 전체 1시간 내내 돌리지 말고,
        -60분, -30분, -5분 근처의 짧은 구간에서만 호출해 호출 수를 줄인다.
      - 경기와 완전히 무관한 시간대에는 호출하지 않는다.
    """
    # 1) 오늘 날짜의 fixtures (일정) 조회
    fixtures = fetch_all(
        """
        SELECT fixture_id, date_utc
        FROM fixtures
        WHERE league_id = %s
          AND DATE(date_utc) = %s
        """,
        (league_id, date_str),
    )
    if not fixtures:
        return False

    # 2) matches 에서 현재 상태 확인 (이미 한 번이라도 A그룹이 돈 경우)
    matches = fetch_all(
        """
        SELECT fixture_id, date_utc, status_group
        FROM matches
        WHERE league_id = %s
          AND DATE(date_utc) = %s
        """,
        (league_id, date_str),
    )

    # 2-1) 라이브 경기 여부 먼저 체크
    live_codes = {"INPLAY", "LIVE", "1H", "HT", "2H", "ET", "BT", "PEN"}
    for row in matches:
        sg = (row.get("status_group") or "").upper()
        if sg in live_codes:
            # 라이브가 하나라도 있으면 매 분 호출
            return True

    # 여기까지 왔으면 아직 라이브는 아님 (경기 전/완료 후)

    # 3) 킥오프 기준으로 가장 가까운 경기까지의 시간 차이 계산
    nearest_diff_min: Optional[float] = None

    for row in fixtures:
        ko = _parse_kickoff_to_utc(row.get("date_utc"))
        if ko is None:
            continue
        diff_min = (now - ko).total_seconds() / 60.0  # now - kickoff (분)
        if nearest_diff_min is None or abs(diff_min) < abs(nearest_diff_min):
            nearest_diff_min = diff_min

    if nearest_diff_min is None:
        return False

    # 4) 프리매치/초기 구간: -70분 ~ +10분 사이만 봄
    #    - -70 ~ -50 : "킥오프 60분 전" 근처 → 5분 간격으로만 호출 (분 % 5 == 0)
    #    - -40 ~ -20 : "킥오프 30분 전" 근처 → 5분 간격
    #    - -5  ~ +10 : 킥오프 직전/직후   → 1분 간격
    dm = nearest_diff_min

    # 킥오프 60분 전 근처 (대략 -70 ~ -50분)
    if -70 <= dm <= -50:
        return (now.minute % 5) == 0

    # 킥오프 30분 전 근처 (대략 -40 ~ -20분)
    if -40 <= dm <= -20:
        return (now.minute % 5) == 0

    # 킥오프 직전/직후 (대략 -5 ~ +10분): 1분마다 호출
    if -5 <= dm <= 10:
        return True

    # 5) 경기 종료 후: 오늘 날짜이면서, 이미 대부분 경기가 끝난 늦은 시간대(예: 21시 이후)에만
    #    15분 간격으로 한 번씩만 호출하도록 한다.
    try:
        target_date = dt.date.fromisoformat(date_str)
    except Exception:
        target_date = now.date()

    if now.date() == target_date and now.hour >= 21:
        # matches 가 있고, 전부 FINISHED 계열이면
        finished_codes = {"FINISHED", "FT", "AET", "PEN"}
        if matches:
            all_sg = {(m.get("status_group") or "").upper() for m in matches}
            if all_sg and all(s in finished_codes for s in all_sg):
                return (now.minute % 15) == 0

    return False


# ─────────────────────────────────────
#  B그룹(정적 데이터) 호출 타이밍
# ─────────────────────────────────────

def detect_static_phase_for_league(
    league_id: int,
    date_str: str,
    now: dt.datetime,
) -> Optional[str]:
    """standings / team_season_stats 같은 정적 데이터 업데이트 타이밍 판단.

    반환값:
      - "PREMATCH" : 오늘 날짜에 이 리그 경기 있고, 아직 하루가 많이 남은 경우
      - "POSTMATCH": 오늘 날짜에 이 리그 경기 있고, 하루가 거의 끝나가는 경우
      - None       : 오늘은 이 리그에 대해 B그룹 업데이트 필요 없음

    너무 복잡하게 가지 않고,
    - fixtures 에 오늘 일정이 하나라도 있으면 대상
    - 같은 날짜라면, 단순히 현재 시각(hour) 기준으로 PRE/POST 를 나눈다.
    """
    # 오늘 날짜에 이 리그 경기 있는지 확인
    rows = fetch_all(
        """
        SELECT 1
        FROM fixtures
        WHERE league_id = %s
          AND DATE(date_utc) = %s
        LIMIT 1
        """,
        (league_id, date_str),
    )
    if not rows:
        return None

    # 날짜 비교
    try:
        target_date = dt.date.fromisoformat(date_str)
    except Exception:
        target_date = now.date()

    if now.date() < target_date:
        # 아직 해당 날짜가 오지 않았다면 정적 데이터 업데이트 X
        return None
    if now.date() > target_date:
        # 이미 지난 날짜라면 POSTMATCH 로 간주
        return "POSTMATCH"

    # now.date() == target_date 인 경우:
    # - 너무 이른 시간(예: 0~5시)에는 굳이 돌리지 않는다.
    if now.hour < 6:
        return None
    # - 낮/저녁(6~21시)에는 PREMATCH 모드
    if now.hour < 21:
        return "PREMATCH"
    # - 21시 이후에는 POSTMATCH 모드
    return "POSTMATCH"
