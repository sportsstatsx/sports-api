from fastapi import FastAPI

app = FastAPI(title="SportsStatsX Staging API")

@app.get("/health")
def health():
    return {"ok": True, "service": "SportsStatsX", "version": "0.1.0"}
