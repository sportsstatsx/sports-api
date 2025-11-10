from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
# CORS: 앱에서 호출할 수 있게 /api/* 경로 전체 허용 (필요 시 도메인 제한 예정)
CORS(app, resources={r"/api/*": {"origins": "*"}})

@app.route("/")
def home():
    return "Hello from SportsStatsX API!"

@app.route("/health")
def health():
    return jsonify(ok=True, service="SportsStatsX", version="0.1.0")

@app.route("/api/ping")
def api_ping():
    return jsonify(pong=True)

# ---- 새로 추가: /api/fixtures 스텁 ----
# 사용법: /api/fixtures?league_id=39&date=2025-11-12 (ISO yyyy-mm-dd 권장)
@app.route("/api/fixtures")
def api_fixtures():
    league_id = request.args.get("league_id")
    date = request.args.get("date")

    # 가짜 데이터(스텁): 앱 연동 용도. DB 붙인 뒤 실제 데이터로 교체 예정.
    sample = [
        {
            "fixture_id": "FX12345",
            "league_id": league_id or "39",
            "date": date or "2025-11-12",
            "kickoff_utc": "2025-11-12T19:00:00Z",
            "home": "Team A",
            "away": "Team B",
            "status": "scheduled"
        },
        {
            "fixture_id": "FX12346",
            "league_id": league_id or "39",
            "date": date or "2025-11-12",
            "kickoff_utc": "2025-11-12T21:00:00Z",
            "home": "Team C",
            "away": "Team D",
            "status": "scheduled"
        },
    ]

    return jsonify(
        ok=True,
        count=len(sample),
        filters={"league_id": league_id, "date": date},
        fixtures=sample
    )

# 404/500도 JSON으로
@app.errorhandler(404)
def not_found(_):
    return jsonify(ok=False, error="not_found"), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify(ok=False, error="server_error", detail=str(e)), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
