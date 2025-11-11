# metrics_instrumentation.py
from time import perf_counter
from flask import g, request
from prometheus_client import (
    Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
)
import re
import os
import time

SERVICE_NAME = os.getenv("SERVICE_NAME", "SportsStatsX")

# ---- 기존 카운터/게이지 (있어도 중복 등록 없이 재사용되도록 이름 동일 유지) ----
REQUESTS_TOTAL = Counter(
    "sportsstatsx_requests_total", "Total requests since start"
)
RESPONSES_COUNT = Counter(
    "sportsstatsx_responses_count", "Response counts by class", ["class"]
)
RATE_LIMITED = Counter(
    "sportsstatsx_rate_limited", "Total 429 responses"
)
PATH_REQUESTS_TOTAL = Counter(
    "sportsstatsx_path_requests_total", "Requests per path", ["path"]
)
UPTIME_SECONDS = Gauge(
    "sportsstatsx_uptime_seconds", "Uptime in seconds"
)

# ---- 신규: 요청 지연 시간 히스토그램 (초 단위) ----
# 버킷은 웹 API에 적합한 구간(50ms ~ 5s)
REQUEST_DURATION = Histogram(
    "sportsstatsx_request_duration_seconds",
    "Request duration in seconds",
    ["path"],
    buckets=(0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0)
)

# 경로 라벨 폭발 방지: 숫자 ID 등은 :id 로 정규화
_path_param_re = re.compile(r"/\d+(/|$)")

def _normalize_path(path: str) -> str:
    # /api/fixtures/123 -> /api/fixtures/:id
    return _path_param_re.sub(r"/:id\1", path or "/")

def before_request_hook():
    # 요청 시작
    g.__start_ts = perf_counter()

def after_request_hook(response):
    try:
        # Uptime 갱신
        UPTIME_SECONDS.set(time.time() - START_EPOCH)

        # 요청 카운트
        REQUESTS_TOTAL.inc()

        # 경로 정규화 후 카운트/히스토그램 기록
        raw_path = request.path or "/"
        norm_path = _normalize_path(raw_path)

        PATH_REQUESTS_TOTAL.labels(path=norm_path).inc()

        # 상태코드 클래스(2xx/4xx/5xx) 집계
        status = response.status_code
        if 200 <= status < 300:
            RESPONSES_COUNT.labels("2xx").inc()
        elif 400 <= status < 500:
            RESPONSES_COUNT.labels("4xx").inc()
            if status == 429:
                RATE_LIMITED.inc()
        elif 500 <= status < 600:
            RESPONSES_COUNT.labels("5xx").inc()
        else:
            RESPONSES_COUNT.labels(f"{status//100}xx").inc()

        # 지연 시간 측정 (초)
        start_ts = getattr(g, "__start_ts", None)
        if start_ts is not None:
            duration = perf_counter() - start_ts
            REQUEST_DURATION.labels(path=norm_path).observe(duration)
    finally:
        return response

# /metrics_prom 뷰 유틸리티 (기존에 라우팅만 연결해 쓰면 됨)
def prom_exposition():
    data = generate_latest()
    headers = {"Content-Type": CONTENT_TYPE_LATEST}
    return data, 200, headers

# 프로세스 시작 시각 (uptime 계산용)
START_EPOCH = time.time()
