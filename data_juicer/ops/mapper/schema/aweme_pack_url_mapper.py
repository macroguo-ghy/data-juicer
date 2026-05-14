from __future__ import annotations

import ast
import itertools
import json
import os
from pathlib import Path
from typing import Any

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.metrics_utils import emit_rpc_qps

OP_NAME = "aweme_pack_url_mapper"

DEFAULT_SOURCE_PSM = "ad.ai.data_forge"
DEFAULT_SOURCE_CLUSTER = "default"
DEFAULT_TARGET_PSM = "aweme.pack.url"
DEFAULT_TARGET_CLUSTER = "default"
DEFAULT_IMAGE_EXPIRE_SECOND = 10 * 365 * 24 * 60 * 60
GDPR_TOKEN_ENV = "INJECTED_SEC_TOKEN_STRING"
GDPR_TOKEN_EXTRA_KEY = "gdpr-token"


def _ensure_requester_env(source_psm: str, source_cluster: str) -> None:
    os.environ["LOAD_SERVICE_PSM"] = source_psm
    os.environ["PSM"] = source_psm
    os.environ["TCE_PSM"] = source_psm
    os.environ["TCE_CLUSTER"] = source_cluster
    os.environ["SERVICE_CLUSTER"] = source_cluster


def _build_target(psm: str, cluster: str) -> str:
    return f"sd://{psm}?cluster={cluster}"


def _build_override_gdpr_auth_middleware(base_compat_middleware, gdpr_token: str):
    base_thrift = getattr(base_compat_middleware, "base_thrift", None)
    if base_thrift is None:
        raise RuntimeError("Current euler package does not expose base_thrift for GDPR override.")

    def middleware(ctx, *middleware_args, **middleware_kwargs):
        for req in itertools.chain(middleware_args, middleware_kwargs.values()):
            if not hasattr(req, "Base"):
                continue
            if not req.Base:
                req.Base = base_thrift.Base()
            if not getattr(req.Base, "Extra", None):
                req.Base.Extra = {}
            req.Base.Extra[GDPR_TOKEN_EXTRA_KEY] = gdpr_token
            ctx.local["gdpr_token"] = gdpr_token
        return ctx.next(*middleware_args, **middleware_kwargs)

    return middleware


def _load_aweme_pack_url_thrift():
    import thriftpy2

    idl_dir = Path(__file__).resolve().parents[1] / "idl"
    return thriftpy2.load(
        str(idl_dir / "aweme_pack_url.thrift"),
        module_name="data_juicer_aweme_pack_url_thrift",
        include_dirs=[str(idl_dir)],
    )


@OPERATORS.register_module(OP_NAME)
class AwemePackUrlMapper(Mapper):
    """Resolve image URIs through aweme.pack.url PackImage Euler RPC."""

    _batched_op = True

    def __init__(
        self,
        uri_field: str = "image_uris",
        url_field: str = "image_urls",
        image_expire_second: int = DEFAULT_IMAGE_EXPIRE_SECOND,
        source_psm: str = DEFAULT_SOURCE_PSM,
        source_cluster: str = DEFAULT_SOURCE_CLUSTER,
        target_psm: str = DEFAULT_TARGET_PSM,
        target_cluster: str = DEFAULT_TARGET_CLUSTER,
        timeout: float = 5.0,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param uri_field: field containing one or more image URIs.
        :param url_field: field to store resolved URLs.
        :param image_expire_second: image URL expiration seconds.
        :param source_psm: caller/source PSM for Euler Base and env.
        :param source_cluster: caller/source cluster for Euler Base and env.
        :param target_psm: target RPC service PSM.
        :param target_cluster: target RPC service cluster.
        :param timeout: Euler RPC timeout in seconds.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not uri_field:
            raise ValueError("uri_field must be provided")
        if not url_field:
            raise ValueError("url_field must be provided")
        self.uri_field = uri_field
        self.url_field = url_field
        self.image_expire_second = image_expire_second
        self.source_psm = source_psm
        self.source_cluster = source_cluster
        self.target_psm = target_psm
        self.target_cluster = target_cluster
        self.timeout = timeout
        self._client = None
        self._api_thrift = None

    def __getstate__(self):
        state = dict(self.__dict__)
        state["_client"] = None
        state["_api_thrift"] = None
        return state

    def process_single(self, sample):
        sample[self.url_field] = self._resolve_urls(sample.get(self.uri_field))
        return sample

    def _resolve_urls(self, uri_value: Any) -> list[str]:
        uris = self._uri_items(uri_value)
        if not uris:
            return []

        urls = []
        for uri in uris:
            try:
                urls.extend(self._pack_image(uri))
            except Exception:
                continue
        return urls

    def _pack_image(self, uri: str) -> list[str]:
        client, api_thrift = self._get_client_and_thrift()
        req = api_thrift.PackImageUrlRequest(
            uri=uri,
            image_expire_second=self.image_expire_second,
            Base=self._build_base(api_thrift),
        )
        target = _build_target(self.target_psm, self.target_cluster)
        try:
            resp = client.PackImage(req)
            status_code = self._base_resp_status_code(resp)
        except Exception:
            emit_rpc_qps(op_name=self._name, target=target, method="PackImage", status="error")
            raise
        if status_code != 0:
            emit_rpc_qps(op_name=self._name, target=target, method="PackImage", status="error")
            return []
        emit_rpc_qps(op_name=self._name, target=target, method="PackImage", status="success")
        return self._url_items(getattr(resp, "url_list", None))

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

        gdpr_token = os.environ.get(GDPR_TOKEN_ENV, "")
        if not gdpr_token:
            raise RuntimeError(f"{GDPR_TOKEN_ENV} must be set for aweme.pack.url GDPR auth.")

        api_thrift = _load_aweme_pack_url_thrift()
        _ensure_requester_env(self.source_psm, self.source_cluster)
        client = euler.Client(
            api_thrift.PackUrlService,
            _build_target(self.target_psm, self.target_cluster),
            timeout=self.timeout,
            transport="ttheader",
            protocol="binary",
        )

        def env_middleware(ctx, *middleware_args, **middleware_kwargs):
            ctx.persistent["cluster"] = self.target_cluster
            return ctx.next(*middleware_args, **middleware_kwargs)

        client.use(env_middleware)
        client.use(base_compat_middleware.client_middleware)
        client.use(_build_override_gdpr_auth_middleware(base_compat_middleware, gdpr_token))
        return client, api_thrift

    def _build_base(self, api_thrift):
        return api_thrift.base_thrift.Base(
            Caller=self.source_psm,
            Extra={"cluster": self.source_cluster},
        )

    @staticmethod
    def _base_resp_status_code(resp) -> int:
        base_resp = getattr(resp, "BaseResp", None)
        if base_resp is None:
            return 0
        return getattr(base_resp, "StatusCode", 0) or 0

    @classmethod
    def _uri_items(cls, value: Any) -> list[str]:
        value = cls._unwrap(value)
        if value is None:
            return []
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="ignore")
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            parsed = cls._parse_structured_string(text)
            if parsed is not None:
                return cls._uri_items(parsed)
            return [text]
        if isinstance(value, dict):
            for key in ("uri", "url", "src", "image_uri", "image"):
                if key in value:
                    return cls._uri_items(value[key])
            return []
        if isinstance(value, (list, tuple, set)):
            items = []
            for item in value:
                items.extend(cls._uri_items(item))
            return items
        return [str(value)]

    @classmethod
    def _url_items(cls, value: Any) -> list[str]:
        urls = []
        for item in cls._uri_items(value):
            cleaned = item.strip()
            if cleaned:
                urls.append(cleaned)
        return urls

    @staticmethod
    def _unwrap(value: Any) -> Any:
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray)):
            value = value.tolist()
        return value

    @staticmethod
    def _parse_structured_string(text: str) -> Any | None:
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(text)
            except (ValueError, SyntaxError, TypeError, json.JSONDecodeError):
                continue
            if isinstance(parsed, str) and parsed == text:
                return None
            return parsed
        return None
