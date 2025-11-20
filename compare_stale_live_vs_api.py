# compare_stale_live_vs_api.py
#
# 1) DB에서 "킥오프 후 2시간이 지났는데 아직 INPLAY인 경기"를 찾고
# 2) 각 fixture_id 에 대해 Api-Football /fixtures?id=... 를 호출해서
# 3) DB 값과 API 값을 나란히 출력해주는 디버그용 스크립트.

import os
import sys
from typing import Any, Dict, List, Optional

import requests

from db import fetch_all

API_BASE = "https://v3.football.api-sports.io/fixtures"


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


def fetch_fixture_from_api(fixture_id: int) -> Optional[Dict[str, Any]]:
    """
    Api-Football /fixtures?id=... 호출해서
    fixture.status.* 정보만 추려서 반환.
    """
    api_key = os.environ.get("APIFOOTBALL_KEY")
    if not api_key:
        print("[ERROR] APIFOOTBALL_KEY 환경변수가 설정되어 있지 않습니다.", file=sys.stderr)
        return None

    headers = {
        "x-apisports-key": api_key,
    }
    params = {"id": fixture_id}

    try:
        resp = requests.get(API_BASE, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"[ERROR] fixture_id={fixture_id} API 호출 실패: {e}", file=sys.stderr)
        return None

    data = resp.json()
    if not data.get("response"):
        print(f"[WARN] fixture_id={fixture_id} API response 가 비어 있습니다: {data}", file=sys.stderr)
        return None

    fixture = data["response"][0].get("fixture") or {}
    status = fixture.get("status") or {}

    return {
        "date": fixture.get("date"),
        "status_short": status.get("short"),
        "status_long": status.get("long"),
        "elapsed": status.get("elapsed"),
        "extra": status.get("extra"),
    }


def main() -> None:
    rows = load_stale_inplay_rows()
    if not rows:
        print("[INFO] 현재 '킥오프 +2h 이상인데 INPLAY' 인 경기가 없습니다.")
        return

    print(f"[INFO] 이상하게 남아 있는 경기 수 = {len(rows)}\n")

    for r in rows:
        fid = int(r["fixture_id"])
        print("=" * 60)
        print(
            f"fixture_id={fid}, league_id={r['league_id']}, "
            f"season={r['season']}, date_utc={r['date_utc']}"
        )
        print(
            f"  DB  → status={r['status']}, "
            f"status_group={r['status_group']}, elapsed={r['elapsed']}"
        )

        api_info = fetch_fixture_from_api(fid)
        if api_info is None:
            print("  API → (호출 실패 또는 응답 없음)")
            continue

        print(
            "  API → "
            f"status_short={api_info['status_short']}, "
            f"status_long={api_info['status_long']}, "
            f"elapsed={api_info['elapsed']}, "
            f"extra={api_info['extra']}, "
            f"date={api_info['date']}"
        )
        print()

    print("=" * 60)
    print("[DONE] DB vs Api-Football 상태 비교 완료.")


if __name__ == "__main__":
    main()
