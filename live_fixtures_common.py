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
    항상 timezone 이 붙은 UTC 현재시각으로 반환.
    """
    return dt.datetime.now(dt.timezone.utc)


def get_target_date() -> str:
    """
    update_live_fixtures.py 에서 사용하는 대상 날짜 결정.

    - 인자가 주어지지 않으면: 오늘(UTC 기준) YYYY-MM-DD
    - 인자가 1개 이상이면: 첫 번째 인자를 날짜로 사용
      (YYYY-MM-DD 형식이 아니면 그대로 사용하지만, 일반적으로 YYYY-MM-DD 만 사용)
    """
    if len(sys.argv) >= 2:
        return str(sys.argv[1])
    return now_utc().strftime("%Y-%m-%d")


def parse_live_leagues(env_val: str) -> List[int]:
    """
    LIVE_LEAGUES 환경변수("39, 40, 140") 등을 정수 리스트로 파싱.

    잘못된 값은 조용히 무시.
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
#  상태 코드 정규화
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

    # Api-Football status.short 기준 대략적인 매핑
    if c in {"FT", "AET", "PEN", "FT_PEN", "AWD", "WO"}:
        return "FINISHED"

    if c in {
        "1H",
        "2H",
        "ET",
        "P",
        "LIVE",
        "INT",  # 하프타임(전반 종료)
        "BT",   # 브레이크
    }:
        return "INPLAY"

    # 그 외 대부분은 킥오프 전/취소 등을 UPCOMING 으로 처리
    return "UPCOMING"


def _parse_kickoff_to_utc(val: Any) -> Optional[dt.datetime]:
    """
    matches.date_utc 값(문자열 또는 datetime)을 UTC aware datetime 으로 변환.
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
            # DATE 만 있을 수도 있음("2025-11-01")
            d = dt.date.fromisoformat(s[:10])
            return dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)
        except Exception:
            return None


# ─────────────────────────────────────
#  A그룹 호출 여부 (킥오프 기반 2번 방식)
# ─────────────────────────────────────


def should_call_league_today(
    league_id: int,
    date_str: str,
    now: dt.datetime,
) -> bool:
    """
    ✅ 2번 방식: "해당 리그의 경기 킥오프 시간" 기준으로
       호출해야 할 시간 창인지 판단한다.

    - matches 테이블에서 league_id 에 해당하는 모든 경기의 date_utc 를 가져온 뒤,
      각 경기의 킥오프(kickoff_utc) 와 현재 시각(now)의 차이(diff_min)를 계산한다.

    - diff_min = (kickoff_utc - now) [분 단위]

    아래 구간에 한 번이라도 걸리면 → 이 리그에 대해 지금 A그룹(Api-Football)을 호출:

      1) -75 ~ -45분 : 킥오프 60분 전 근처 (PRE60)
      2) -45 ~ -15분 : 킥오프 30분 전 근처 (PRE30)
      3) -15 ~ 130분 : 경기 직전 ~ 경기 중/직후 (LIVE 구간, 1분마다 호출)
      4) 130 ~ 180분 : 경기 종료 후 조금 지난 구간 (POST)

    이렇게 하면:
      - 경기 없으면 호출 안 함.
      - 경기와 상관없는 시간대(오전/한밤중 등)에는 Api-Football 호출 안 함.
      - 경기 전후 3~4시간 정도에만 크론(1분마다) + Api 호출이 돌아간다.

    ※ date_str 는 과거/미래 백필 및 B그룹용으로 유지하지만,
       여기서는 "오늘 날짜" 여부와 상관없이 오로지 킥오프 시간 차이만 사용한다.
    """
    # 리그 전체 경기의 킥오프 시간 목록
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

    # 현재 시각을 UTC 기준으로 정규화
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    else:
        now = now.astimezone(dt.timezone.utc)

    for r in rows:
        kickoff_raw = r.get("date_utc")
        kickoff_utc = _parse_kickoff_to_utc(kickoff_raw)
        if not kickoff_utc:
            continue

        diff_min = (kickoff_utc - now).total_seconds() / 60.0

        # 1) 킥오프 60분 전 근처
        if -75.0 <= diff_min <= -45.0:
            return True

        # 2) 킥오프 30분 전 근처
        if -45.0 < diff_min <= -15.0:
            return True

        # 3) 경기 직전/중/직후 (여기서는 1분마다 호출 유지)
        if -15.0 < diff_min <= 130.0:
            return True

        # 4) 경기 종료 후 살짝 지난 구간
        if 130.0 < diff_min <= 180.0:
            return True

    return False


# ─────────────────────────────────────
#  B그룹(정적 데이터) 호출 타이밍 감지
# ─────────────────────────────────────


def detect_static_phase_for_league(
    league_id: int,
    date_str: str,
    now: dt.datetime,
) -> Optional[str]:
    """
    standings / team_season_stats 같은 "정적" 데이터 업데이트 타이밍을 대략 판단.

    반환값:
      - "PREMATCH" : 오늘 날짜에 예정/진행/종료 경기가 있고,
                     아직 당일이 많이 지나지 않은 시점 (대략 킥오프 전/중/직후)
      - "POSTMATCH": 오늘 경기가 있고, 대부분 종료된 뒤 (하루 거의 끝난 시점)
      - None       : 오늘은 이 리그에 업데이트할 필요 없음

    너무 복잡하게 가지 말고,
    단순히 status_group + 현재 시각(hour) 기준으로만 판단한다.
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

    has_inplay_or_upcoming = False
    has_finished = False

    for r in rows:
        sg_raw = r.get("status_group") or r.get("status") or ""
        sg = map_status_group(sg_raw)
        if sg == "INPLAY" or sg == "UPCOMING":
            has_inplay_or_upcoming = True
        elif sg == "FINISHED":
            has_finished = True

    # UTC 기준 오늘 날짜의 "현재 시간" 을 사용
    hour = now.hour

    if has_inplay_or_upcoming:
        # 경기 전/중
        return "PREMATCH"

    if has_finished:
        # 경기들이 전부 끝나고, 하루가 꽤 지난 시점이면 POSTMATCH 로 본다.
        if hour >= 21:
            return "POSTMATCH"

    return None


# ─────────────────────────────────────
#  시즌 추론 / 해석 (A/B 공통)
# ─────────────────────────────────────


def _fetch_season_range_for_league(league_id: int) -> List[Dict[str, Any]]:
    """
    matches 테이블에서 리그별 season / 최소일 / 최대일 을 가져온다.
    """
    rows = fetch_all(
        """
        SELECT
            season,
            MIN(date_utc) AS min_date_utc,
            MAX(date_utc) AS max_date_utc
        FROM matches
        WHERE league_id = %s
        GROUP BY season
        """,
        (league_id,),
    )
    return rows or []


def infer_season_for_league_and_date(
    league_id: int,
    date_str: str,
) -> int:
    """
    Api-Football /fixtures 호출에서 사용할 season 값을 추론.

    1) matches 테이블에 이 리그의 season 별로 date_utc 범위가 들어있다면:
         - 각 season 의 [시즌 시작-30일, 시즌 종료+60일] 범위 안에
           date_str 가 들어가는 season 을 우선 사용.
    2) 적당한 시즌을 못 찾으면:
         - date_str 의 연도를 그대로 season 으로 사용.
    """
    try:
        d = dt.date.fromisoformat(date_str[:10])
    except Exception:
        d = now_utc().date()

    year = d.year

    season_rows = _fetch_season_range_for_league(league_id)
    best_season: Optional[int] = None

    for row in season_rows:
        s = row.get("season")
        if s is None:
            continue
        try:
            s_int = int(s)
        except (TypeError, ValueError):
            continue

        min_raw = row.get("min_date_utc")
        max_raw = row.get("max_date_utc")
        if not min_raw or not max_raw:
            continue

        min_dt = _parse_kickoff_to_utc(min_raw)
        max_dt = _parse_kickoff_to_utc(max_raw)
        if not min_dt or not max_dt:
            continue

        min_d = min_dt.date()
        max_d = max_dt.date()

        # 시즌 시작 30일 전 ~ 시즌 종료 60일 후까지를 같은 시즌으로 본다.
        before = min_d - dt.timedelta(days=30)
        after = max_d + dt.timedelta(days=60)

        if before <= d <= after:
            best_season = s_int
            break

    if best_season is not None:
        return best_season

    # 아직 이 리그에 대한 matches 데이터가 거의 없으면 → 날짜 연도 기준 season
    return year


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
