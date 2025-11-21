# compare_stale_live_vs_api.py
#
# 1) DB에서 "킥오프 후 2시간이 지났는데 아직 INPLAY인 경기"를 찾고
# 2) 해당 경기들을 DB에서 강제로 FT(종료) 상태로 업데이트하는 스크립트.
#
# ※ Api-Football 을 호출하지 않고, 우리 DB 값만 직접 고치는 "안전장치" 역할.
#    - 나중에 라이브 워커나 다른 스크립트가 FT/상세 상태를 다시 써도 무방함
#    - 조건에 맞는 경기만 건드리기 때문에 중복/충돌 문제 없음

import sys
from typing import Any, Dict, List

from db import fetch_all, execute


def load_stale_inplay_rows() -> List[Dict[str, Any]]:
    """
    킥오프 후 2시간이 지났는데 아직 INPLAY 인 경기들만 가져오기.
    필요하면 interval '2 hours' 부분을 3시간 등으로 조정해서 사용.
    """
    sql = """
        SELECT
            fixture_id,
            league_id,
            season,
            date_utc,
            status,
            status_group,
            elapsed
        FROM matches
        WHERE date_utc::timestamptz < now() - interval '2 hours'
          AND status_group = 'INPLAY'
        ORDER BY date_utc;
    """
    return fetch_all(sql, ())


def force_close_fixture(fixture_id: int) -> None:
    """
    주어진 fixture_id 에 대해
    - status       = 'FT'
    - status_group = 'FINISHED'
    - elapsed      = 최소 90분(기존 값이 90보다 작거나 NULL 이면 90으로)
    로 강제 종료 처리.

    이미 다른 곳에서 FT 로 바꿔놓았으면, 이 스크립트의 WHERE 조건
    (status_group = 'INPLAY') 에 걸리지 않기 때문에 변경하지 않음.
    """
    sql = """
        UPDATE matches
        SET
            status       = 'FT',
            status_group = 'FINISHED',
            elapsed      = CASE
                             WHEN elapsed IS NULL OR elapsed < 90 THEN 90
                             ELSE elapsed
                           END
        WHERE fixture_id = %s
          AND status_group = 'INPLAY';
    """
    execute(sql, (fixture_id,))


def main() -> None:
    rows = load_stale_inplay_rows()
    if not rows:
        print("[INFO] 현재 '킥오프 +2h 이상인데 INPLAY' 인 경기가 없습니다.")
        return

    print(f"[INFO] 강제 종료 대상 경기 수 = {len(rows)}\n")

    for r in rows:
        fid = int(r["fixture_id"])
        print("=" * 60)
        print(
            f"fixture_id={fid}, league_id={r['league_id']}, "
            f"season={r['season']}, date_utc={r['date_utc']}"
        )
        print(
            f"  BEFORE → status={r['status']}, "
            f"status_group={r['status_group']}, elapsed={r['elapsed']}"
        )

        try:
            force_close_fixture(fid)
            print("  AFTER  → status=FT, status_group=FINISHED, elapsed>=90 로 강제 종료 처리 시도.")
        except Exception as e:
            print(f"  [ERROR] fixture_id={fid} 업데이트 중 오류 발생: {e}", file=sys.stderr)

        print()

    print("=" * 60)
    print("[DONE] 오래된 INPLAY 경기 강제 종료 작업 완료.")


if __name__ == "__main__":
    main()
