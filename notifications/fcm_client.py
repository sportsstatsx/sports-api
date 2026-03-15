# notifications/fcm_client.py

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

import firebase_admin
from firebase_admin import credentials, messaging

SERVICE_ENV_VAR = "FIREBASE_SERVICE_ACCOUNT_JSON"


def _init_firebase_app() -> firebase_admin.App:
    """
    Firebase Admin SDK 초기화.
    Render 환경변수 FIREBASE_SERVICE_ACCOUNT_JSON 에 저장된
    서비스 계정 JSON 을 사용한다.
    """

    # 이미 초기화된 앱 있으면 그대로 사용
    if firebase_admin._apps:
        return list(firebase_admin._apps.values())[0]

    raw = os.environ.get(SERVICE_ENV_VAR)
    if not raw:
        raise RuntimeError(
            f"{SERVICE_ENV_VAR} 환경변수가 없습니다. "
            "Render 환경변수에 서비스 계정 JSON 전체를 넣어야 합니다."
        )

    try:
        service_account_info = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"{SERVICE_ENV_VAR} 값이 JSON 형식이 아닙니다."
        ) from e

    cred = credentials.Certificate(service_account_info)
    return firebase_admin.initialize_app(cred)


class FCMClient:
    """
    Firebase Admin SDK 기반 FCM 알림 전송 클라이언트.
    """

    def __init__(self) -> None:
        _init_firebase_app()

    @staticmethod
    def _build_data(
        title: str,
        body: str,
        data: Optional[Dict[str, Any]],
    ) -> Dict[str, str]:
        """
        Android 앱이 항상 onMessageReceived() 에서 직접 노티를 만들 수 있게
        title/body 도 data payload 로 넣는다.
        data payload 는 모두 문자열이어야 하므로 str() 변환.
        """
        payload: Dict[str, Any] = dict(data or {})
        payload["title"] = title
        payload["body"] = body
        return {str(k): str(v) for k, v in payload.items()}

    def send_to_tokens(
        self,
        tokens: List[str],
        title: str,
        body: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        여러 FCM 토큰으로 알림을 전송한다.

        핵심:
        - Android 에서는 notification payload 를 제거하고 data-only 로 보낸다.
        - 이렇게 해야 백그라운드에서도 앱의 FirebaseMessagingService 가
          일관되게 직접 알림을 생성할 수 있다.
        """

        if not tokens:
            return {"success_count": 0, "failure_count": 0, "results": []}

        _init_firebase_app()
        payload_data = self._build_data(title=title, body=body, data=data)

        success_count = 0
        failure_count = 0
        results: List[Dict[str, Any]] = []

        for token in tokens:
            msg = messaging.Message(
                token=token,
                data=payload_data,
                android=messaging.AndroidConfig(
                    priority="high",
                ),
            )

            try:
                message_id = messaging.send(msg)
                results.append({
                    "token": token,
                    "success": True,
                    "message_id": message_id,
                    "error": None,
                })
                success_count += 1
            except Exception as e:
                results.append({
                    "token": token,
                    "success": False,
                    "message_id": None,
                    "error": str(e),
                })
                failure_count += 1

        return {
            "success_count": success_count,
            "failure_count": failure_count,
            "results": results,
        }

    def send_to_token(
        self,
        token: str,
        title: str,
        body: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        단일 토큰용 헬퍼.
        """
        return self.send_to_tokens([token], title, body, data)


if __name__ == "__main__":
    # Render Shell manual test
    import sys

    print("FCMClient self-test 시작")

    try:
        _init_firebase_app()
    except Exception as e:
        print("Firebase 초기화 실패:", e, file=sys.stderr)
        sys.exit(1)

    print("Firebase 초기화 성공 (푸시 전송은 테스트하지 않음)")
