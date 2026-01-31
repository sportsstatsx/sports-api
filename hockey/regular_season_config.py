# hockey/regular_season_config.py
from __future__ import annotations

from datetime import datetime, date, timezone, timedelta
from typing import Dict, Tuple, Optional

KST = timezone(timedelta(hours=9))

# (league_id, season) -> (regular_season_start_date_in_kst)
REGULAR_SEASON_START_KST: Dict[Tuple[int, int], date] = {
    # NHL(57): 정규시즌 시작 2025-10-08 (KST)
    (57, 2025): date(2025, 10, 8),

    # AHL(58): 정규시즌 시작 2025-10-11 (KST)
    (58, 2025): date(2025, 10, 11),
}


def get_regular_season_start_utc(league_id: int, season: int) -> Optional[datetime]:
    """
    정규시즌 시작일을 'KST 자정(00:00)' 기준으로 UTC datetime으로 변환해서 반환.
    - 예) 2025-10-08 00:00 KST -> 2025-10-07 15:00 UTC
    """
    d = REGULAR_SEASON_START_KST.get((int(league_id), int(season)))
    if not d:
        return None

    dt_kst = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=KST)
    return dt_kst.astimezone(timezone.utc)
