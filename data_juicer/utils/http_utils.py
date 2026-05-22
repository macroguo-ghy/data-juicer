from __future__ import annotations

import time
from typing import Any

import httpx


class HttpClient:
    SUPPORTED_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}
    DEFAULT_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)

    def __init__(
        self,
        endpoint: str,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
        retry_attempts: int = 0,
        retry_backoff_seconds: float = 0.5,
        retry_backoff_multiplier: float = 2.0,
        retry_status_codes: tuple[int, ...] | None = None,
        retry_on_timeout: bool = True,
        retry_on_connection_error: bool = True,
    ):
        if not endpoint:
            raise ValueError("endpoint must be provided")
        method = method.upper()
        if method not in self.SUPPORTED_METHODS:
            raise ValueError(f"Unsupported HTTP method: {method}")
        if retry_attempts < 0:
            raise ValueError("retry_attempts must be non-negative")
        if retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds must be non-negative")
        if retry_backoff_multiplier < 0:
            raise ValueError("retry_backoff_multiplier must be non-negative")

        self.endpoint = endpoint
        self.method = method
        self.headers = dict(headers or {})
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.retry_backoff_multiplier = retry_backoff_multiplier
        self.retry_status_codes = tuple(
            retry_status_codes
            if retry_status_codes is not None
            else self.DEFAULT_RETRY_STATUS_CODES
        )
        self.retry_on_timeout = retry_on_timeout
        self.retry_on_connection_error = retry_on_connection_error

    def request(
        self,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | list[Any] | None = None,
    ) -> dict[str, Any]:
        max_attempts = self.retry_attempts + 1
        for attempt in range(max_attempts):
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
                if self._should_retry_status(exc.response.status_code, attempt, max_attempts):
                    self._sleep_before_retry(attempt)
                    continue
                return self._error_result(exc, response=exc.response)
            except httpx.HTTPError as exc:
                if self._should_retry_error(exc, attempt, max_attempts):
                    self._sleep_before_retry(attempt)
                    continue
                return self._error_result(exc)

        raise RuntimeError("unreachable HTTP retry state")

    def _should_retry_status(
        self,
        status_code: int,
        attempt: int,
        max_attempts: int,
    ) -> bool:
        return (
            attempt < max_attempts - 1
            and status_code in self.retry_status_codes
        )

    def _should_retry_error(
        self,
        exc: httpx.HTTPError,
        attempt: int,
        max_attempts: int,
    ) -> bool:
        if attempt >= max_attempts - 1:
            return False
        if isinstance(exc, httpx.TimeoutException):
            return self.retry_on_timeout
        if isinstance(exc, (httpx.NetworkError, httpx.RemoteProtocolError)):
            return self.retry_on_connection_error
        return False

    def _sleep_before_retry(self, attempt: int) -> None:
        wait_seconds = self.retry_backoff_seconds * (
            self.retry_backoff_multiplier ** attempt
        )
        if wait_seconds > 0:
            time.sleep(wait_seconds)

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
