# Ad Test HttpClient Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a lightweight project-level `HttpClient` wrapper that standardizes HTTP calls for future `ad_test` HTTP, LLM, and Code operators.

**Architecture:** Implement `HttpClient` in `data_juicer/utils/http_utils.py` as a thin wrapper around `httpx.request`. It will validate HTTP methods, pass through headers/query params/JSON body/timeout, parse JSON responses when possible, fall back to text for non-JSON responses, and return a stable result dictionary.

**Critical Assumptions & Early Checks:** `httpx` is already a core dependency and should be used rather than Python standard-library HTTP modules. The first implementation must remain generic and not depend on Data-Juicer sample structures. The earliest check is a test using mocked `httpx.request` to confirm no real network calls are needed.

**Tech Stack:** Python, `httpx`, `unittest`, `unittest.mock`.

---

### Task 1: Add `HttpClient` Success-Path Tests

**Files:**
- Create: `tests/utils/test_http_utils.py`
- Later create: `data_juicer/utils/http_utils.py`

**Step 1: Write the failing tests**

Create `tests/utils/test_http_utils.py` with success-path tests:

```python
import unittest
from unittest.mock import Mock, patch

import httpx

from data_juicer.utils.http_utils import HttpClient


class HttpClientTest(unittest.TestCase):

    @patch("data_juicer.utils.http_utils.httpx.request")
    def test_get_with_params_returns_json_data(self, mock_request):
        response = Mock(spec=httpx.Response)
        response.status_code = 200
        response.text = '{"ok": true}'
        response.json.return_value = {"ok": True}
        response.raise_for_status.return_value = None
        mock_request.return_value = response

        client = HttpClient(endpoint="http://example.test/items", method="GET", headers={"X-Test": "1"}, timeout=3.0)
        result = client.request(params={"id": 1})

        mock_request.assert_called_once_with(
            method="GET",
            url="http://example.test/items",
            headers={"X-Test": "1"},
            timeout=3.0,
            params={"id": 1},
            json=None,
        )
        self.assertEqual(result, {
            "ok": True,
            "status_code": 200,
            "data": {"ok": True},
            "text": None,
            "error": None,
        })

    @patch("data_juicer.utils.http_utils.httpx.request")
    def test_post_with_json_body_returns_json_data(self, mock_request):
        response = Mock(spec=httpx.Response)
        response.status_code = 201
        response.text = '{"answer": "hello"}'
        response.json.return_value = {"answer": "hello"}
        response.raise_for_status.return_value = None
        mock_request.return_value = response

        client = HttpClient(endpoint="http://example.test/invoke", method="POST")
        result = client.request(json_body={"inputs": {"prompt": "hi"}})

        mock_request.assert_called_once_with(
            method="POST",
            url="http://example.test/invoke",
            headers={},
            timeout=30.0,
            params=None,
            json={"inputs": {"prompt": "hi"}},
        )
        self.assertEqual(result["ok"], True)
        self.assertEqual(result["status_code"], 201)
        self.assertEqual(result["data"], {"answer": "hello"})
        self.assertIsNone(result["text"])
        self.assertIsNone(result["error"])

    @patch("data_juicer.utils.http_utils.httpx.request")
    def test_non_json_success_returns_text(self, mock_request):
        response = Mock(spec=httpx.Response)
        response.status_code = 200
        response.text = "plain text"
        response.json.side_effect = ValueError("not json")
        response.raise_for_status.return_value = None
        mock_request.return_value = response

        client = HttpClient(endpoint="http://example.test/plain")
        result = client.request()

        self.assertEqual(result, {
            "ok": True,
            "status_code": 200,
            "data": None,
            "text": "plain text",
            "error": None,
        })
```

**Step 2: Run tests to verify they fail**

Run:

```bash
./.venv/bin/python -m unittest tests.utils.test_http_utils
```

Expected: FAIL with `ModuleNotFoundError: No module named 'data_juicer.utils.http_utils'`.

**Step 3: Commit**

```bash
git add tests/utils/test_http_utils.py
git commit -m "test: add HttpClient success-path tests"
```

### Task 2: Implement Minimal `HttpClient`

**Files:**
- Create: `data_juicer/utils/http_utils.py`
- Test: `tests/utils/test_http_utils.py`

**Step 1: Write minimal implementation**

Create `data_juicer/utils/http_utils.py`:

```python
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
    def _error_result(cls, exc: Exception, response: httpx.Response | None = None) -> dict[str, Any]:
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
```

**Step 2: Run success-path tests**

Run:

```bash
./.venv/bin/python -m unittest tests.utils.test_http_utils
```

Expected: PASS for the first three tests.

**Step 3: Commit**

```bash
git add data_juicer/utils/http_utils.py tests/utils/test_http_utils.py
git commit -m "feat: add reusable HttpClient utility"
```

### Task 3: Add Error-Handling Tests

**Files:**
- Modify: `tests/utils/test_http_utils.py`
- Modify if needed: `data_juicer/utils/http_utils.py`

**Step 1: Add failing tests for errors and validation**

Append tests:

```python
    @patch("data_juicer.utils.http_utils.httpx.request")
    def test_http_status_error_returns_error_result(self, mock_request):
        request = httpx.Request("GET", "http://example.test/error")
        response = httpx.Response(500, request=request, text="server error")
        mock_request.return_value = response

        client = HttpClient(endpoint="http://example.test/error", method="GET")
        result = client.request()

        self.assertEqual(result["ok"], False)
        self.assertEqual(result["status_code"], 500)
        self.assertIsNone(result["data"])
        self.assertEqual(result["text"], "server error")
        self.assertEqual(result["error"]["type"], "HTTPStatusError")

    @patch("data_juicer.utils.http_utils.httpx.request")
    def test_request_error_returns_error_result(self, mock_request):
        mock_request.side_effect = httpx.TimeoutException("timed out")

        client = HttpClient(endpoint="http://example.test/timeout")
        result = client.request()

        self.assertEqual(result["ok"], False)
        self.assertIsNone(result["status_code"])
        self.assertIsNone(result["data"])
        self.assertIsNone(result["text"])
        self.assertEqual(result["error"]["type"], "TimeoutException")

    def test_rejects_unsupported_method(self):
        with self.assertRaises(ValueError):
            HttpClient(endpoint="http://example.test", method="TRACE")

    def test_rejects_empty_endpoint(self):
        with self.assertRaises(ValueError):
            HttpClient(endpoint="")
```

**Step 2: Run tests**

Run:

```bash
./.venv/bin/python -m unittest tests.utils.test_http_utils
```

Expected: PASS. If any error-shape detail fails, adjust `HttpClient` only enough to match the documented result structure.

**Step 3: Commit**

```bash
git add data_juicer/utils/http_utils.py tests/utils/test_http_utils.py
git commit -m "test: cover HttpClient error handling"
```

### Task 4: Final Verification

**Files:**
- Verify: `data_juicer/utils/http_utils.py`
- Verify: `tests/utils/test_http_utils.py`

**Step 1: Run syntax checks**

Run:

```bash
python3 -m py_compile data_juicer/utils/http_utils.py tests/utils/test_http_utils.py
```

Expected: no output, exit code 0.

**Step 2: Run focused unit tests**

Run:

```bash
./.venv/bin/python -m unittest tests.utils.test_http_utils
```

Expected: all tests pass.

**Step 3: Run diff hygiene check**

Run:

```bash
git diff --check
```

Expected: no output, exit code 0.

**Step 4: Review changed files**

Run:

```bash
git status --short
git diff -- data_juicer/utils/http_utils.py tests/utils/test_http_utils.py
```

Expected: only the intended utility and test files changed for this implementation phase.

**Step 5: Commit final verification if needed**

If previous commits were skipped, commit the final implementation:

```bash
git add data_juicer/utils/http_utils.py tests/utils/test_http_utils.py
git commit -m "feat: add reusable HttpClient utility"
```
