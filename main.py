# main.py
from flask import Flask, jsonify, request
from flask_cors import CORS
from db import fetch_all, fetch_one, execute  # db.py에 이미 구현된 헬퍼 사용

app = Flask(__name__)
CORS(app)

SERVICE_NAME = "SportsStatsX"
SERVICE_VERSION = "0.2.1"

# --------------------------------------------------------------------
# 기본 엔드포인트
# --------------------------------------------------------------------
@app.route("/")
def root():
    return "Hello from SportsStatsX API!"

@app.route("/health")
def health():
    return jsonify({"ok": True, "service": SERVICE_NAME, "version": SERVICE_VERSION})

@app.route("/api/test-db")
def test_db():
    try:
        result = fetch_one("SELECT 1;")
        # fetch_one이 (1,) 또는 1을 반환해도 아래 표현은 안전
        value = result[0] if isinstance(result, (tuple, list)) else result
        return jsonify({"ok": True, "db": "connected", "result": value})
    except Exception as e:
        return jsonify({"ok": False, "db": "error", "error": str(e)}), 500

# --------------------------------------------------------------------
# Fixtures API (스키마: id, league_id, match_date, home_team, away_team, home_score, away_score)
#   - 쿼리 파라미터:
#       league_id (옵션)
#       date      (옵션, 예: 2025-11-12)
# --------------------------------------------------------------------
@app.route("/api/fixtures")
def get_fixtures():
    try:
        league_id = request.args.get("league_id")
        on_date   = request.args.get("date")  # YYYY-MM-DD

        sql = """
            SELECT id, league_id, match_date, home_team, away_team, home_score, away_score
            FROM fixtures
            WHERE 1=1
        """
        params = []

        if league_id:
            sql += " AND league_id = %s"
            params.append(league_id)

        if on_date:
            sql += " AND match_date = %s"
            params.append(on_date)

        sql += " ORDER BY match_date, id"

        rows = fetch_all(sql, params)

        fixtures = [
            {
                "id": r[0],
                "league_id": r[1],
                "match_date": str(r[2]),
                "home_team": r[3],
                "away_team": r[4],
                "home_score": r[5],
                "away_score": r[6],
            }
            for r in rows
        ]

        return jsonify({"ok": True, "count": len(fixtures), "fixtures": fixtures})

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": "server_error",
            "detail": str(e)
        }), 500


# --------------------------------------------------------------------
# 로컬 실행용 (Render에서는 gunicorn/entrypoint로 실행)
# --------------------------------------------------------------------
if __name__ == "__main__":
    # 로컬 테스트 시 편의
    app.run(host="0.0.0.0", port=5000)
