from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import json

app = Flask(__name__)
# JSON 포맷 설정
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True   # 들여쓰기
app.config["JSON_SORT_KEYS"] = False               # 키 순서 유지
CORS(app, resources={r"/api/*": {"origins": "*"}})

def json_response(payload: dict, status: int = 200) -> Response:
    """항상 같은 순서/들여쓰기로 응답"""
    return Response(
        json.dumps(payload, ensure_ascii=False, indent=2),  # pretty print
        status=status,
        mimetype="application/json",
    )

@app.route("/")
def home():
    return "Hello from SportsStatsX API!"

@app.route("/health")
def health():
    return json_response({"ok": True, "service": "SportsStatsX", "version": "0.1.0"})

@app.route("/api/ping")
def api_ping():
    return json_response({"pong": True})

@app.route("/api/fixtures")
def api_fixtures():
    league_id = request.args.get("league_id")
    date = request.args.get("date")

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

    # ✅ 항상 동일한 키 순서( ok → count → filters → fixtures )
    payload = {
        "ok": True,
        "count": len(sample),
        "filters": {"league_id": league_id, "date": date},
        "fixtures": sample,
    }
    return json_response(payload)

@app.errorhandler(404)
def not_found(_):
    return json_response({"ok": False, "error": "not_found"}, 404)

@app.errorhandler(500)
def server_error(e):
    return json_response({"ok": False, "error": "server_error", "detail": str(e)}, 500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
