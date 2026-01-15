# backfill_match_fixtures_raw_by_ids.py
#
# 목적:
# - fixture_id 리스트(ids.txt)를 받아서
#   1) /fixtures?id= 로 원본을 다시 받아 match_fixtures_raw 저장
#   2) matches + fixtures 테이블도 최신화(스코어/라운드/venue/status_long 등 포함)
#
# 사용:
#   python backfill_match_fixtures_raw_by_ids.py /path/to/ids.txt [start_idx]
#
# 환경변수:
#   - APIFOOTBALL_KEY (또는 API_FOOTBALL_KEY / API_KEY / FOOTBALL_API_KEY)

import os
import sys
import time
import json
from typing import Any, Dict, Optional

import requests

from db import execute

BASE_URL = "https://v3.football.api-sports.io/fixtures"


# ─────────────────────────────────────
#  ENV / HTTP
# ─────────────────────────────────────

def _get_api_key() -> str:
    key = (
        os.environ.get("APIFOOTBALL_KEY")
        or os.environ.get("API_FOOTBALL_KEY")
        or os.environ.get("API_KEY")
        or os.environ.get("FOOTBALL_API_KEY")
        or ""
    )
    if not key:
        raise RuntimeError("API key missing: set APIFOOTBALL_KEY (or API_FOOTBALL_KEY / API_KEY)")
    return key


def _headers() -> Dict[str, str]:
    return {"x-apisports-key": _get_api_key()}


def _safe_get(url: str, *, params: Dict[str, Any], timeout: int = 25, max_retry: int = 4) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for i in range(max_retry):
        try:
            resp = requests.get(url, headers=_headers(), params=params, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.7 * (i + 1))
                continue
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError("API response is not a dict")
            return data
        except Exception as e:
            last_err = e
            time.sleep(0.7 * (i + 1))
            continue
    raise RuntimeError(f"API request failed after retries: {last_err}")


def fetch_fixture_by_id(fixture_id: int) -> Optional[Dict[str, Any]]:
    data = _safe_get(BASE_URL, params={"id": fixture_id}, timeout=25)
    rows = data.get("response") or []
    if not rows:
        return None
    first = rows[0]
    return first if isinstance(first, dict) else None


# ─────────────────────────────────────
#  DB UPSERTS
# ─────────────────────────────────────

def _status_group_from_short(short: Optional[str]) -> str:
    s = (short or "").upper()
    if s in ("FT", "AET", "PEN"):
        return "FINISHED"
    if s in ("NS", "TBD"):
        return "SCHEDULED"
    if s in ("PST", "CANC", "ABD", "AWD", "WO"):
        return "CANCELLED"
    return "LIVE"


def upsert_match_fixtures_raw(fixture_id: int, fixture_obj: Dict[str, Any]) -> None:
    raw = json.dumps(fixture_obj, ensure_ascii=False)
    execute(
        """
        INSERT INTO match_fixtures_raw (fixture_id, data_json, fetched_at, updated_at)
        VALUES (%s, %s, now(), now())
        ON CONFLICT (fixture_id) DO UPDATE
        SET data_json = EXCLUDED.data_json,
            fetched_at = now(),
            updated_at = now()
        """,
        (fixture_id, raw),
    )


def upsert_fixture_row(fx: Dict[str, Any]) -> None:
    fixture_block = fx.get("fixture") or {}
    league_block = fx.get("league") or {}

    fid = fixture_block.get("id")
    league_id = league_block.get("id")
    season = league_block.get("season")
    if fid is None or league_id is None or season is None:
        # fixtures 테이블 스키마상 league_id/season이 필요할 확률이 높음
        return

    date_utc = fixture_block.get("date")
    status_short = (fixture_block.get("status") or {}).get("short")
    status_group = _status_group_from_short(status_short)

    execute(
        """
        INSERT INTO fixtures (fixture_id, league_id, season, date_utc, status, status_group)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id     = EXCLUDED.league_id,
            season        = EXCLUDED.season,
            date_utc      = EXCLUDED.date_utc,
            status        = EXCLUDED.status,
            status_group  = EXCLUDED.status_group
        """,
        (int(fid), int(league_id), int(season), date_utc, status_short, status_group),
    )


def upsert_match_row(fx: Dict[str, Any]) -> None:
    fixture_block = fx.get("fixture") or {}
    league_block = fx.get("league") or {}
    teams_block = fx.get("teams") or {}
    goals_block = fx.get("goals") or {}
    score_block = fx.get("score") or {}

    fid = fixture_block.get("id")
    league_id = league_block.get("id")
    season = league_block.get("season")

    if fid is None or league_id is None or season is None:
        return

    date_utc = fixture_block.get("date")
    st = fixture_block.get("status") or {}
    status_short = st.get("short")
    status_long = st.get("long")
    status_elapsed = st.get("elapsed")
    status_extra = st.get("extra")
    status_group = _status_group_from_short(status_short)

    home_id = (teams_block.get("home") or {}).get("id")
    away_id = (teams_block.get("away") or {}).get("id")
    if home_id is None or away_id is None:
        return

    home_ft = goals_block.get("home")
    away_ft = goals_block.get("away")

    ht = score_block.get("halftime") or {}
    home_ht = ht.get("home")
    away_ht = ht.get("away")

    elapsed = status_elapsed

    referee = fixture_block.get("referee")
    fixture_timezone = fixture_block.get("timezone")
    fixture_timestamp = fixture_block.get("timestamp")
    venue = fixture_block.get("venue") or {}
    venue_id = venue.get("id")
    venue_name = venue.get("name")
    venue_city = venue.get("city")

    league_round = league_block.get("round")

    execute(
        """
        INSERT INTO matches (
            fixture_id, league_id, season, date_utc, status, status_group,
            home_id, away_id,
            home_ft, away_ft, elapsed,
            home_ht, away_ht,
            referee, fixture_timezone, fixture_timestamp,
            status_short, status_long, status_elapsed, status_extra,
            venue_id, venue_name, venue_city, league_round
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id = EXCLUDED.league_id,
            season = EXCLUDED.season,
            date_utc = EXCLUDED.date_utc,
            status = EXCLUDED.status,
            status_group = EXCLUDED.status_group,
            home_id = EXCLUDED.home_id,
            away_id = EXCLUDED.away_id,
            home_ft = EXCLUDED.home_ft,
            away_ft = EXCLUDED.away_ft,
            elapsed = EXCLUDED.elapsed,
            home_ht = EXCLUDED.home_ht,
            away_ht = EXCLUDED.away_ht,
            referee = EXCLUDED.referee,
            fixture_timezone = EXCLUDED.fixture_timezone,
            fixture_timestamp = EXCLUDED.fixture_timestamp,
            status_short = EXCLUDED.status_short,
            status_long = EXCLUDED.status_long,
            status_elapsed = EXCLUDED.status_elapsed,
            status_extra = EXCLUDED.status_extra,
            venue_id = EXCLUDED.venue_id,
            venue_name = EXCLUDED.venue_name,
            venue_city = EXCLUDED.venue_city,
            league_round = EXCLUDED.league_round
        """,
        (
            int(fid), int(league_id), int(season), date_utc, status_short, status_group,
            int(home_id), int(away_id),
            home_ft, away_ft, elapsed,
            home_ht, away_ht,
            referee, fixture_timezone, fixture_timestamp,
            status_short, status_long, status_elapsed, status_extra,
            venue_id, venue_name, venue_city, league_round,
        ),
    )


# ─────────────────────────────────────
#  MAIN
# ─────────────────────────────────────

def main() -> None:
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

            # raw 저장 + matches/fixtures 미러링
            upsert_match_fixtures_raw(fid, fx)
            upsert_match_row(fx)
            upsert_fixture_row(fx)

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
