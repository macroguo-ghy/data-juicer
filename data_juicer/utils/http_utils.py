from __future__ import annotations

from typing import Any

import httpx


class HttpClient:
    SUPPORTED_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}

    def __init__(
        self,
        endpoint: str,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
    ):
        if not endpoint:
            raise ValueError("endpoint must be provided")
        method = method.upper()
        if method not in self.SUPPORTED_METHODS:
            raise ValueError(f"Unsupported HTTP method: {method}")

        self.endpoint = endpoint
        self.method = method
        self.headers = dict(headers or {})
        self.timeout = timeout

    def request(
        self,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | list[Any] | None = None,
    ) -> dict[str, Any]:
        try:
            response = httpx.request(
                method=self.method,
                url=self.endpoint,
                headers=self.headers,
                timeout=self.timeout,
                params=params,
                json=json_body,
            )
            response.raise_for_status()
            return self._success_result(response)
        except httpx.HTTPStatusError as exc:
            return self._error_result(exc, response=exc.response)
        except httpx.HTTPError as exc:
            return self._error_result(exc)

    @classmethod
    def _success_result(cls, response: httpx.Response) -> dict[str, Any]:
        try:
            data = response.json()
        except ValueError:
            data = None
            text = response.text
        else:
            text = None
        return {
            "ok": True,
            "status_code": response.status_code,
            "data": data,
            "text": text,
            "error": None,
        }

    @classmethod
    def _error_result(
        cls,
        exc: Exception,
        response: httpx.Response | None = None,
    ) -> dict[str, Any]:
        return {
            "ok": False,
            "status_code": response.status_code if response is not None else None,
            "data": None,
            "text": response.text if response is not None else None,
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
            },
        }
