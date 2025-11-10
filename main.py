from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/")
def home():
    return "Hello from SportsStatsX API!"

@app.route("/health")
def health():
    return jsonify(ok=True, service="SportsStatsX", version="0.1.0")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
