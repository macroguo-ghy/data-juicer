from __future__ import annotations

import copy
import hashlib
from typing import Any

from loguru import logger

from data_juicer.utils.http_utils import HttpClient

RESULT_SYNC_SUBMIT_PATH = "/openapi/synthesis/result-sync/submit"
DEFAULT_TIMEOUT_SECONDS = 300.0
DEFAULT_RETRY_ATTEMPTS = 5
DEFAULT_RETRY_STATUS_CODES = (420, 429, 500, 502, 503, 504)


class AdcResultSyncHook:
    """Sync final Magnus Lance export results to ADC external targets."""

    def __init__(self, export_cfg: dict[str, Any], hook_cfg: dict[str, Any]):
        self.export_cfg = export_cfg
        self.hook_cfg = hook_cfg
        self.ctx = self._require_dict(hook_cfg.get("ctx"), "after_export_hook.ctx")
        self.table_name = self._require_value(export_cfg.get("table_name"), "export.table_name")
        self.api_base = str(self._require_value(self.ctx.get("apiBase"), "ctx.apiBase")).rstrip("/")
        self.user_account = str(self._require_value(self.ctx.get("userAccount"), "ctx.userAccount"))
        self.timeout = float(hook_cfg.get("timeout", DEFAULT_TIMEOUT_SECONDS))
        self.retry_attempts = int(hook_cfg.get("retry_attempts", DEFAULT_RETRY_ATTEMPTS))
        if self.retry_attempts < 0:
            raise ValueError("after_export_hook.retry_attempts must be non-negative")

    def run(self) -> None:
        if self.export_cfg.get("target") != "magnus":
            raise ValueError("adc_result_sync after_export_hook only supports export.target=magnus")

        sync_cfg = self._require_dict(self.hook_cfg.get("sync"), "after_export_hook.sync")
        targets = sync_cfg.get("targets")
        if not isinstance(targets, list):
            raise ValueError("after_export_hook.sync.targets must be a list")

        result_sync_targets = []
        for target in targets:
            if not isinstance(target, dict):
                raise ValueError("after_export_hook.sync.targets item must be a dictionary")
            if target.get("enabled", True) is False:
                continue
            result_sync_targets.append(self._build_result_sync_target(target))

        if not result_sync_targets:
            logger.info("ADC result sync after_export_hook has no enabled targets; skip submit")
            return

        self._post(
            RESULT_SYNC_SUBMIT_PATH,
            self._build_submit_payload(result_sync_targets),
            headers=self._base_headers(),
        )

    def _build_result_sync_target(self, target: dict[str, Any]) -> dict[str, Any]:
        target_type = target.get("type")
        if target_type == "sheet":
            payload = {"targetType": "sheet"}
            if target.get("sheetTitle") is not None:
                payload["sheetTitle"] = target.get("sheetTitle")
            return payload
        if target_type == "eval_set":
            target_cfg = copy.deepcopy(self._require_dict(target.get("target"), "eval_set.target"))
            target_cfg.setdefault("spaceId", self._require_value(self.ctx.get("spaceId"), "ctx.spaceId"))
            payload = {
                "targetType": "eval_set",
                "target": target_cfg,
            }
            if target.get("selectedFields") is not None:
                payload["selectedFields"] = target.get("selectedFields")
            if target.get("fieldMapping") is not None:
                payload["fieldMapping"] = target.get("fieldMapping")
            return payload
        raise ValueError(f"Unsupported after_export_hook sync target type: {target_type}")

    def _build_submit_payload(self, targets: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "idempotencyKey": self._build_idempotency_key(),
            "source": {
                "sourceType": "lance",
                "datasourceName": self.table_name,
            },
            "targets": targets,
        }

    def _build_idempotency_key(self) -> str:
        instance_id = self.ctx.get("flowInstanceId") or self.ctx.get("synthesisInstanceId")
        instance_id = self._require_value(instance_id, "ctx.flowInstanceId or ctx.synthesisInstanceId")
        flow_node_id = self._require_value(self.ctx.get("flowNodeId"), "ctx.flowNodeId")
        source_hash = hashlib.sha256(str(self.table_name).encode("utf-8")).hexdigest()[:16]
        return f"synthesis:{instance_id}:node:{flow_node_id}:result_sync:lance:{source_hash}"

    def _post(self, path: str, payload: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
        client = HttpClient(
            endpoint=f"{self.api_base}/{path.lstrip('/')}",
            method="POST",
            headers=headers,
            timeout=self.timeout,
            retry_attempts=self.retry_attempts,
            retry_status_codes=DEFAULT_RETRY_STATUS_CODES,
            retry_on_timeout=False,
        )
        result = client.request(json_body=payload)
        if not result["ok"]:
            raise ValueError(f"ADC result sync request failed: {result['error']}")
        self._validate_openapi_result(result.get("data"))
        return result

    def _base_headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "User-Account": self.user_account,
            "space-id": str(self._require_value(self.ctx.get("spaceId"), "ctx.spaceId")),
        }
        for key in ("x-tt-env", "x-use-ppe"):
            value = self.ctx.get(key)
            if value:
                headers[key] = str(value)
        return headers

    @staticmethod
    def _validate_openapi_result(data: Any) -> None:
        if not isinstance(data, dict) or "code" not in data:
            return
        if data.get("code") != 0:
            message = data.get("message") or data.get("msg") or ""
            raise ValueError(f"ADC result sync business failed: code={data.get('code')}, message={message}")

    @staticmethod
    def _require_dict(value: Any, name: str) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise ValueError(f"{name} must be a dictionary")
        return value

    @staticmethod
    def _require_value(value: Any, name: str) -> Any:
        if value in (None, ""):
            raise ValueError(f"{name} must be provided")
        return value
