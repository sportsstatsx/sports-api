import os
import sys
import requests
import datetime as dt
from typing import Dict, Any, List

from db import fetch_all, execute


# ─────────────────────────────────────
# API-Sports 설정
# ─────────────────────────────────────
API_KEY = os.environ.get("APIFOOTBALL_KEY")
API_HOST = "v3.football.api-sports.io"

HEADERS = {
    "x-apisports-key": API_KEY
}


# ─────────────────────────────────────
# 상태 매핑
# ─────────────────────────────────────
STATUS_MAP = {
    "NS": ("UPCOMING", "NS"),
    "PST": ("UPCOMING", "PST"),
    "CANC": ("UPCOMING", "CANC"),
    "AWD": ("UPCOMING", "AWD"),

    "1H": ("INPLAY", "1H"),
    "HT": ("INPLAY", "HT"),
    "2H": ("INPLAY", "2H"),

    "FT": ("FINISHED", "FT"),
    "AET": ("FINISHED", "AET"),
    "PEN": ("FINISHED", "PEN"),
}


# ─────────────────────────────────────
# API 호출
# ─────────────────────────────────────
def fetch_fixture_detail(fixture_id: int) -> Dict[str, Any]:
    url = f"https://{API_HOST}/fixtures"
    params = {"id": fixture_id}

    r = requests.get(url, headers=HEADERS, params=params, timeout=12)
    r.raise_for_status()
    data = r.json()

    if "response" not in data or len(data["response"]) == 0:
        return {}

    return data["response"][0]


# ─────────────────────────────────────
# 오늘+어제 중 FINISHED 아닌 경기들 조회
# ─────────────────────────────────────
def get_target_matches() -> List[Dict[str, Any]]:
    now = dt.datetime.utcnow()
    today = now.date()
    yesterday = today - dt.timedelta(days=1)

    start_dt = f"{yesterday} 00:00:00"
    end_dt = f"{today} 23:59:59"

    sql = """
        SELECT fixture_id, date_utc, status, status_group
        FROM matches
        WHERE date_utc BETWEEN %s AND %s
          AND status_group != 'FINISHED'
        ORDER BY date_utc ASC
    """

    return fetch_all(sql, (start_dt, end_dt))


# ─────────────────────────────────────
# 이벤트 업데이트
# ─────────────────────────────────────
def upsert_match_events(fixture_id: int, events: List[Dict[str, Any]]):
    # ⚠️ 기존 "DELETE 후 재삽입" 방식은 match_events.id가 매번 바뀌어서
    # match_event_worker가 같은 이벤트를 계속 "새 이벤트"로 오인 → 푸시 중복 발생.
    # 여기서는 (fixture_id, type, minute, extra, team_id, player_id, player_in_id) 조합으로
    # 기존 행을 UPDATE 하고, 없을 때만 INSERT 해서 id를 안정화한다.

    for ev in events:
        minute = ev["time"]["elapsed"]
        extra = ev["time"].get("extra") or 0
        team_id = ev.get("team", {}).get("id")
        player_id = ev.get("player", {}).get("id")
        assist_id = ev.get("assist", {}).get("id")
        assist_name = ev.get("assist", {}).get("name")
        p_in = ev["player"].get("id_in") if "player" in ev else None
        p_in_name = None

        sql = """
            WITH upd AS (
                UPDATE match_events
                   SET detail = %s,
                       assist_player_id = %s,
                       assist_name = %s,
                       player_in_name = %s
                 WHERE fixture_id = %s
                   AND type = %s
                   AND minute = %s
                   AND extra = %s
                   AND team_id IS NOT DISTINCT FROM %s
                   AND player_id IS NOT DISTINCT FROM %s
                   AND player_in_id IS NOT DISTINCT FROM %s
                 RETURNING id
            )
            INSERT INTO match_events (
                fixture_id, team_id, player_id,
                type, detail, minute, extra,
                assist_player_id, assist_name,
                player_in_id, player_in_name
            )
            SELECT
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM upd);
        """

        execute(sql, (
            # upd
            ev.get("detail"),
            assist_id, assist_name,
            p_in_name,
            fixture_id,
            ev.get("type"),
            minute, extra,
            team_id, player_id, p_in,
            # insert
            fixture_id, team_id, player_id,
            ev.get("type"), ev.get("detail"),
            minute, extra,
            assist_id, assist_name,
            p_in, p_in_name,
        ))



# ─────────────────────────────────────
# 팀 스탯 업데이트
# ─────────────────────────────────────
def upsert_match_team_stats(fixture_id: int, stats: List[Dict[str, Any]]):
    execute("DELETE FROM match_team_stats WHERE fixture_id = %s", (fixture_id,))

    for team_stat in stats:
        team_id = team_stat["team"]["id"]
        stats_list = team_stat.get("statistics", [])

        for s in stats_list:
            name = s.get("type")
            value = s.get("value")

            sql = """
                INSERT INTO match_team_stats (fixture_id, team_id, name, value)
                VALUES (%s,%s,%s,%s)
            """
            execute(sql, (fixture_id, team_id, name, str(value)))


# ─────────────────────────────────────
# 논리적 종료 (kickoff + 150분)
# ─────────────────────────────────────
def is_logical_finished(date_utc: str) -> bool:
    try:
        kickoff = dt.datetime.strptime(date_utc, "%Y-%m-%d %H:%M:%S")
    except:
        return False

    cutoff = kickoff + dt.timedelta(minutes=150)
    return dt.datetime.utcnow() > cutoff


# ─────────────────────────────────────
# 메인 처리
# ─────────────────────────────────────
def update_matches():
    rows = get_target_matches()

    if not rows:
        print("No matches to update.")
        return

    print(f"[INFO] Target matches: {len(rows)}")

    updated = 0
    finished = 0

    for row in rows:
        fid = row["fixture_id"]
        date_utc = row["date_utc"]

        # 논리 종료 판단
        if is_logical_finished(date_utc):
            sql = """
                UPDATE matches
                SET status_group='FINISHED', status='FT', elapsed=90
                WHERE fixture_id=%s
            """
            execute(sql, (fid,))
            print(f"[LOGICAL FINISH] Fixture {fid} → FINISHED")
            finished += 1
            continue

        # API에서 최신 상태 가져오기
        data = fetch_fixture_detail(fid)
        if not data:
            print(f"[WARN] No API data for fixture {fid}")
            continue

        s = data["fixture"]["status"]["short"]

        if s not in STATUS_MAP:
            print(f"[WARN] Unknown status {s} for fixture {fid}")
            continue

        group, status = STATUS_MAP[s]

        goals = data.get("goals", {})
        home_ft = goals.get("home")
        away_ft = goals.get("away")
        elapsed = data["fixture"]["status"].get("elapsed")

        sql = """
            UPDATE matches
            SET status_group=%s,
                status=%s,
                elapsed=%s,
                home_ft=%s,
                away_ft=%s
            WHERE fixture_id=%s
        """
        execute(sql, (
            group, status, elapsed,
            home_ft, away_ft, fid
        ))
        updated += 1

        # FINISHED면 이벤트/스탯도 저장
        if group == "FINISHED":
            finished += 1
            upsert_match_events(fid, data.get("events", []))
            upsert_match_team_stats(fid, data.get("statistics", []))

        print(f"[OK] Fixture {fid} → {group}/{status}")

    print(f"\n[RESULT] Updated: {updated}, Finished: {finished}")


# ─────────────────────────────────────
# 엔트리포인트
# ─────────────────────────────────────
if __name__ == "__main__":
    try:
        update_matches()
    except Exception as e:
        print("[ERROR]", e)
        sys.exit(1)
