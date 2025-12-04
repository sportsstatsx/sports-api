# notifications/fcm_client.py

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

import firebase_admin
from firebase_admin import credentials, messaging

"""
FCM 클라이언트 (Firebase Admin SDK 사용 버전)

- 환경변수 FIREBASE_SERVICE_ACCOUNT_JSON 에
  Firebase 서비스 계정 JSON 전체 내용을 문자열로 넣어두고 사용.
- 더 이상 FCM_SERVER_KEY, 레거시 HTTP 엔드포인트는 사용하지 않음.

사용 예시 (기존 match_event_worker 코드와 호환):

    from notifications.fcm_client import FCMClient

    fcm = FCMClient()
    result = fcm.send_to_tokens(
        tokens=["token1", "token2"],
        title="Kickoff!",
        body="경기가 곧 시작합니다.",
        data={"match_id": "12345"}
    )
"""

_FIREBASE_APP: Optional[firebase_admin.App] = None


def _ensure_firebase_app() -> firebase_admin.App:
    """
    전역 Firebase App 싱글톤 초기화.
    FIREBASE_SERVICE_ACCOUNT_JSON 이 없으면 예외 발생.
    """
    global _FIREBASE_APP
    if _FIREBASE_APP is not None:
        return _FIREBASE_APP

    raw_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
    if not raw_json:
        raise RuntimeError(
            "FIREBASE_SERVICE_ACCOUNT_JSON 환경변수가 설정되어 있지 않습니다. "
            "Firebase 콘솔에서 받은 서비스 계정 JSON 전체를 넣어주세요."
        )

    try:
        info = json.loads(raw_json)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            "FIREBASE_SERVICE_ACCOUNT_JSON 값이 유효한 JSON 이 아닙니다."
        ) from e

    cred = credentials.Certificate(info)
    _FIREBASE_APP = firebase_admin.initialize_app(cred)
    return _FIREBASE_APP


class FCMClient:
    """
    Firebase Admin SDK 를 이용해 FCM 메시지를 보내는 클라이언트.

    기존 코드와의 호환을 위해 __init__ 에 인자를 받지 않지만,
    내부적으로 FIREBASE_SERVICE_ACCOUNT_JSON 만 사용합니다.
    """

    def __init__(self) -> None:
        # 앱이 없으면 여기서 한 번 초기화
        _ensure_firebase_app()

    # ------------------------------------------------------------------
    # 내부 헬퍼
    # ------------------------------------------------------------------
    @staticmethod
    def _build_data_payload(data: Optional[Dict[str, Any]]) -> Dict[str, str]:
        """
        data 필드는 모두 문자열이어야 해서, 들어온 값을 str 로 캐스팅.
        """
        if not data:
            return {}
        return {str(k): str(v) for k, v in data.items()}

    # ------------------------------------------------------------------
    # 외부에서 쓰는 메인 메서드
    # ------------------------------------------------------------------
    def send_to_tokens(
        self,
        tokens: List[str],
        title: str,
        body: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        여러 기기 토큰으로 알림 전송.

        :param tokens: FCM registration token 리스트
        :param title: 알림 제목
        :param body: 알림 내용
        :param data: data payload (선택)
        :return: 전송 결과 요약 딕셔너리
        """
        if not tokens:
            return {
                "success": 0,
                "failure": 0,
                "results": [],
            }

        app = _ensure_firebase_app()

        message = messaging.MulticastMessage(
            tokens=tokens,
            notification=messaging.Notification(title=title, body=body),
            data=self._build_data_payload(data),
        )

        response = messaging.send_multicast(message, app=app)

        results: List[Dict[str, Any]] = []
        for idx, resp in enumerate(response.responses):
            results.append(
                {
                    "token": tokens[idx],
                    "success": resp.success,
                    "message_id": getattr(resp, "message_id", None),
                    "exception": str(resp.exception) if resp.exception else None,
                }
            )

        return {
            "success": response.success_count,
            "failure": response.failure_count,
            "results": results,
        }

    # 필요하면 단일 토큰용 헬퍼도 제공 (기존 코드에서 쓸 수도 있음)
    def send_to_token(
        self,
        token: str,
        title: str,
        body: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self.send_to_tokens([token], title, body, data)
