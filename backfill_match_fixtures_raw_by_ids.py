# backfill_match_fixtures_raw_by_ids.py
import sys
import time
import requests

from live_fixtures_common import API_KEY
from live_fixtures_a_group import upsert_match_fixtures_raw, upsert_match_row

BASE_URL = "https://v3.football.api-sports.io/fixtures"

def _headers():
    if not API_KEY:
        raise RuntimeError("APIFOOTBALL_KEY 환경변수가 설정되어 있지 않습니다.")
    return {"x-apisports-key": API_KEY}

def fetch_fixture_by_id(fixture_id: int):
    resp = requests.get(
        BASE_URL,
        headers=_headers(),
        params={"id": fixture_id},
        timeout=25,
    )
    resp.raise_for_status()
    data = resp.json()
    rows = data.get("response") or []
    if not rows:
        return None
    return rows[0]

def main():
    if len(sys.argv) < 2:
        print("Usage: python backfill_match_fixtures_raw_by_ids.py /path/to/ids.txt [start_idx]")
        sys.exit(1)

    path = sys.argv[1]
    start_idx = int(sys.argv[2]) if len(sys.argv) >= 3 else 0

    with open(path, "r", encoding="utf-8") as f:
        ids = [line.strip() for line in f if line.strip()]

    total = len(ids)
    ok = 0
    fail = 0

    for i in range(start_idx, total):
        s = ids[i]
        try:
            fid = int(s)
            fx = fetch_fixture_by_id(fid)
            if not fx:
                fail += 1
                continue

            # raw 저장 + matches 미러링(HT/venue/round/status_long 등)
            upsert_match_fixtures_raw(fid, fx)
            upsert_match_row(fx, league_id=None, season=None)

            ok += 1
            if (i + 1) % 100 == 0:
                print(f"[progress] idx={i+1}/{total} ok={ok} fail={fail} (start={start_idx})")
            time.sleep(0.09)  # 레이트리밋 완화
        except Exception as e:
            fail += 1
            print(f"[ERR] idx={i+1}/{total} fixture_id={s} err={e}", file=sys.stderr)
            time.sleep(0.25)

    print(f"[done] total={total} start={start_idx} ok={ok} fail={fail}")

if __name__ == "__main__":
    main()
