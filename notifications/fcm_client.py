# notifications/fcm_client.py

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

import requests


FCM_LEGACY_URL = "https://fcm.googleapis.com/fcm/send"


class FCMClient:
    def __init__(self, server_key: str | None = None) -> None:
        self.server_key = server_key or os.getenv("FCM_SERVER_KEY")
        if not self.server_key:
            raise RuntimeError("FCM_SERVER_KEY environment variable is not set")

    def send_to_tokens(
        self,
        tokens: List[str],
        title: str,
        body: str,
        data: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """
        여러 디바이스 토큰에 동일한 알림 전송.
        tokens 길이가 많으면, 바깥에서 적당히 나눠서 호출해도 됨.
        """
        if not tokens:
            return {"success": 0, "failure": 0}

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"key={self.server_key}",
        }

        payload: Dict[str, Any] = {
            "registration_ids": tokens,
            "notification": {
                "title": title,
                "body": body,
            },
            "data": data or {},
        }

        resp = requests.post(
            FCM_LEGACY_URL,
            headers=headers,
            data=json.dumps(payload),
            timeout=5,
        )
        resp.raise_for_status()
        return resp.json()
