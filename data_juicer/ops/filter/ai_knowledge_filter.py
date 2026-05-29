from __future__ import annotations

import json
import os
import time
from functools import lru_cache
from pathlib import Path
from typing import Any

from loguru import logger

from ..base_op import NON_STATS_FILTERS, OPERATORS, Filter
from data_juicer.utils.metrics_utils import emit_rpc_qps

OP_NAME = "ai_knowledge_filter"

DEFAULT_SOURCE_PSM = "ad.ai.data_forge"
DEFAULT_SOURCE_CLUSTER = "default"
DEFAULT_TARGET_PSM = "ad.stats.ai_knowledge_center_admin"
DEFAULT_TARGET_CLUSTER = "default"
DEFAULT_USER_ID = 0
DEFAULT_TIMEOUT = 60.0
MAX_RPC_IDENTIFIERS_PER_REQUEST = 500
CODE_FIELD = "code"
IDENTIFIER_FIELD = "identifier"
SOURCE_FIELD = "source"


def _build_target(psm: str, cluster: str) -> str:
    return f"sd://{psm}?cluster={cluster}"


def _ensure_requester_env(source_psm: str, source_cluster: str) -> None:
    os.environ["LOAD_SERVICE_PSM"] = source_psm
    os.environ["PSM"] = source_psm
    os.environ["TCE_PSM"] = source_psm
    os.environ["TCE_CLUSTER"] = source_cluster
    os.environ["SERVICE_CLUSTER"] = source_cluster


@lru_cache(maxsize=1)
def _load_akc_admin_thrift():
    import thriftpy2

    idl_dir = Path(__file__).resolve().parent / "idl" / "akc"
    return thriftpy2.load(
        str(idl_dir / "admin" / "akc_admin.thrift"),
        module_name="data_juicer_akc_admin_thrift",
        include_dirs=[str(idl_dir)],
    )


@NON_STATS_FILTERS.register_module(OP_NAME)
@OPERATORS.register_module(OP_NAME)
class AiKnowledgeFilter(Filter):
    """Filter knowledge samples through the AKC admin thrift RPC."""

    _batched_op = True

    def __init__(
        self,
        condition: dict[str, Any] | str | None = None,
        keyword: str = "",
        env: str | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param condition: Sirius AckSearchCondition as a JSON object or JSON string.
        :param keyword: comma-separated keywords string.
        :param env: optional PPE env for Euler routing and request Base TrafficEnv.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        self.condition = self._normalize_condition(condition)
        self.keywords = self._split_keyword(keyword)
        self.env = env
        self._client = None
        self._api_thrift = None

    def __getstate__(self):
        state = dict(self.__dict__)
        state["_client"] = None
        state["_api_thrift"] = None
        return state

    def compute_stats_batched(self, samples):
        return samples

    def process_batched(self, samples):
        if not samples:
            return []

        code_values = self._get_batch_values(samples, CODE_FIELD)
        identifiers = self._build_identifier_payloads(samples, code_values)
        keep_bools = [False] * len(code_values)
        if not identifiers:
            return keep_bools

        matched = set()
        for start in range(0, len(identifiers), MAX_RPC_IDENTIFIERS_PER_REQUEST):
            identifier_batch = identifiers[start : start + MAX_RPC_IDENTIFIERS_PER_REQUEST]
            matched.update(self._filter_identifiers([item["payload"] for item in identifier_batch]))
        for item in identifiers:
            keep = self._identifier_key(item["payload"]) in matched
            if self.reversed_range:
                keep = not keep
            keep_bools[item["index"]] = keep
        return keep_bools

    def _filter_identifiers(self, identifiers: list[dict[str, str]]) -> set[tuple[str, str]]:
        client, api_thrift = self._get_client_and_thrift()
        req = self._build_filter_request(api_thrift, identifiers)
        target = _build_target(DEFAULT_TARGET_PSM, DEFAULT_TARGET_CLUSTER)
        start_time = time.monotonic()
        logger.info(
            f"ai_knowledge_filter rpc start: target={target}, method=filter, "
            f"env={self.env or ''}, identifiers={len(identifiers)}"
        )
        try:
            resp = client.filter(req)
            self._raise_if_rpc_failed(resp)
        except Exception as exc:
            elapsed_ms = (time.monotonic() - start_time) * 1000
            logger.warning(
                f"ai_knowledge_filter rpc finish: target={target}, method=filter, "
                f"status=error, env={self.env or ''}, identifiers={len(identifiers)}, "
                f"elapsed_ms={elapsed_ms:.2f}, error={exc}"
            )
            emit_rpc_qps(op_name=self._name, target=target, method="filter", status="error")
            raise
        matched = {self._identifier_key(item) for item in getattr(resp, "identifiers", None) or []}
        elapsed_ms = (time.monotonic() - start_time) * 1000
        logger.info(
            f"ai_knowledge_filter rpc finish: target={target}, method=filter, "
            f"status=success, env={self.env or ''}, identifiers={len(identifiers)}, "
            f"matched={len(matched)}, elapsed_ms={elapsed_ms:.2f}"
        )
        emit_rpc_qps(op_name=self._name, target=target, method="filter", status="success")
        return matched

    def _build_identifier_payload(self, sample: dict[str, Any]) -> dict[str, str]:
        if IDENTIFIER_FIELD not in sample:
            raise KeyError(f"`{IDENTIFIER_FIELD}` not found")
        if SOURCE_FIELD not in sample:
            raise KeyError(f"`{SOURCE_FIELD}` not found")

        return {
            "identifier": self._to_string(sample[IDENTIFIER_FIELD]),
            "source": self._to_string(sample[SOURCE_FIELD]),
        }

    def _build_identifier_payloads(self, samples: dict[str, list[Any]], code_values: list[Any]) -> list[dict[str, Any]]:
        identifiers = self._get_batch_values(samples, IDENTIFIER_FIELD)
        sources = self._get_batch_values(samples, SOURCE_FIELD)
        if len(identifiers) != len(sources) or len(identifiers) != len(code_values):
            raise ValueError(f"`{IDENTIFIER_FIELD}`, `{SOURCE_FIELD}` and `{CODE_FIELD}` must have the same length")

        payloads = []
        for index, code in enumerate(code_values):
            if code != 0:
                continue
            payloads.append(
                {
                    "index": index,
                    "payload": {
                        "identifier": self._to_string(identifiers[index]),
                        "source": self._to_string(sources[index]),
                    },
                }
            )
        return payloads

    @staticmethod
    def _get_batch_values(samples: dict[str, list[Any]], field: str) -> list[Any]:
        if field not in samples:
            raise KeyError(f"`{field}` not found")
        return samples[field]

    def _get_client_and_thrift(self):
        if self._client is None or self._api_thrift is None:
            self._client, self._api_thrift = self._create_client_and_thrift()
        return self._client, self._api_thrift

    def _create_client_and_thrift(self):
        try:
            import euler
            from euler import base_compat_middleware
        except ImportError as exc:
            raise RuntimeError(
                "Euler RPC runtime is required. Install the internal package `bytedeuler`."
            ) from exc

        api_thrift = _load_akc_admin_thrift()
        _ensure_requester_env(DEFAULT_SOURCE_PSM, DEFAULT_SOURCE_CLUSTER)
        client = euler.Client(
            api_thrift.AkcAdminService,
            _build_target(DEFAULT_TARGET_PSM, DEFAULT_TARGET_CLUSTER),
            timeout=DEFAULT_TIMEOUT,
            transport="ttheader",
            protocol="binary",
        )

        def env_middleware(ctx, *middleware_args, **middleware_kwargs):
            ctx.persistent["cluster"] = DEFAULT_TARGET_CLUSTER
            if self.env:
                ctx.persistent["env"] = self.env
            return ctx.next(*middleware_args, **middleware_kwargs)

        client.use(env_middleware)
        client.use(base_compat_middleware.client_middleware)
        return client, api_thrift

    def _build_filter_request(self, api_thrift, identifiers: list[dict[str, str]]):
        return api_thrift.AckSearchFilterRequest(
            condition=self._build_condition(api_thrift, self.condition),
            keywords=list(self.keywords),
            identifiers=[
                api_thrift.Identifier(
                    identifier=item["identifier"],
                    source=item["source"],
                )
                for item in identifiers
            ],
            BizReq=self._build_biz_req(api_thrift),
            Base=self._build_base(api_thrift),
        )

    def _build_biz_req(self, api_thrift):
        return self._base_thrift(api_thrift).BizReq(UserId=DEFAULT_USER_ID)

    def _build_base(self, api_thrift):
        base_thrift = self._base_thrift(api_thrift)
        extra = {"cluster": DEFAULT_SOURCE_CLUSTER}
        traffic_env = None
        if self.env:
            extra["env"] = self.env
            traffic_env = base_thrift.TrafficEnv(Open=True, Env=self.env)
        return base_thrift.Base(
            Caller=DEFAULT_SOURCE_PSM,
            TrafficEnv=traffic_env,
            Extra=extra,
        )

    @staticmethod
    def _base_thrift(api_thrift):
        base_thrift = getattr(api_thrift, "base_thrift", None) or getattr(api_thrift, "base", None)
        if base_thrift is None:
            raise RuntimeError("AKC admin thrift module does not expose base thrift definitions.")
        return base_thrift

    def _build_condition(self, api_thrift, condition: dict[str, Any] | None):
        if condition is None:
            return None

        predicate = condition.get("predicate")
        children = condition.get("children")
        kwargs = {"op": condition.get("op")}
        if children:
            kwargs["children"] = [
                self._build_condition(api_thrift, child)
                for child in children
                if child is not None
            ]
        if predicate:
            kwargs["predicate"] = self._build_predicate(api_thrift, predicate)
        return api_thrift.AckSearchCondition(**kwargs)

    def _build_predicate(self, api_thrift, predicate: dict[str, Any]):
        return api_thrift.AckSearchPredicate(
            field=predicate["field"],
            operator=predicate["operator"],
            value=self._build_value(api_thrift, predicate.get("value")),
        )

    def _build_value(self, api_thrift, value: Any):
        if value is None:
            return None
        if isinstance(value, dict):
            union_keys = {"stringValue", "longValue", "boolValue", "stringListValue"}
            union_values = {key: value[key] for key in union_keys if key in value}
            if len(union_values) == 1:
                return api_thrift.AckSearchValue(**union_values)
        if isinstance(value, bool):
            return api_thrift.AckSearchValue(boolValue=value)
        if isinstance(value, int) and not isinstance(value, bool):
            return api_thrift.AckSearchValue(longValue=value)
        if isinstance(value, (list, tuple, set)):
            return api_thrift.AckSearchValue(stringListValue=[self._to_string(item) for item in value])
        return api_thrift.AckSearchValue(stringValue=self._to_string(value))

    @staticmethod
    def _normalize_condition(condition: dict[str, Any] | str | None) -> dict[str, Any] | None:
        if condition is None or condition == "":
            return None
        if isinstance(condition, str):
            condition = json.loads(condition)
        if not isinstance(condition, dict):
            raise ValueError("condition must be a JSON object or JSON object string")
        return condition

    @staticmethod
    def _split_keyword(keyword: str | None) -> list[str]:
        if keyword is None:
            return []
        if not isinstance(keyword, str):
            raise ValueError("keyword must be a string")
        return [item.strip() for item in keyword.split(",") if item.strip()]

    @staticmethod
    def _raise_if_rpc_failed(resp) -> None:
        biz_resp = getattr(resp, "BizResp", None)
        biz_code = getattr(biz_resp, "Code", 0) or 0
        if biz_code:
            raise RuntimeError(
                f"ai knowledge filter rpc failed: biz_code={biz_code}, "
                f"msg={getattr(biz_resp, 'Msg', '')}"
            )

        base_resp = getattr(resp, "BaseResp", None)
        status_code = getattr(base_resp, "StatusCode", 0) or 0
        if status_code:
            raise RuntimeError(
                f"ai knowledge filter rpc failed: status_code={status_code}, "
                f"status_message={getattr(base_resp, 'StatusMessage', '')}"
            )

    @classmethod
    def _identifier_key(cls, item: Any) -> tuple[str, str]:
        if isinstance(item, dict):
            identifier = item["identifier"]
            source = item["source"]
        else:
            identifier = getattr(item, "identifier")
            source = getattr(item, "source")
        return cls._to_string(identifier), cls._to_string(source)

    @staticmethod
    def _to_string(value: Any) -> str:
        if hasattr(value, "as_py"):
            value = value.as_py()
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="ignore")
        return str(value)
