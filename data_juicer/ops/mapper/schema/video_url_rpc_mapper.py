from __future__ import annotations

import base64
import codecs
import hmac
import hashlib
import os
import random
import time
from collections import Counter
from pathlib import Path
from typing import Any, Sequence

import pyarrow as pa
from loguru import logger

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.ops.condition_utils import RowCondition
from data_juicer.ops.mapper.rpc_rate_limiter import RpcQpsRateLimiter, validate_qps
from data_juicer.utils.metrics_utils import emit_rpc_qps

OP_NAME = "video_url_rpc_mapper"

AUTH_PREFIX_V1 = "VARCH1-HMAC-SHA1"
DEFAULT_SIGN_TTL = 3600
DEFAULT_EULER_CALLER = "ad.ai.data_forge_merlin"
DEFAULT_MAX_VIDS_PER_REQUEST = 20
MAX_VIDS_PER_REQUEST = 60
ALLOWED_URL_TYPES = {6, 7, 8, 9, 10, 11}
EMPTY_RESULT_RPC_DEBUG_SAMPLE_RATE = 0.001
DEBUG_PAYLOAD_MAX_DEPTH = 6
DEBUG_PAYLOAD_MAX_ITEMS = 80
DEBUG_PAYLOAD_MAX_STRING_LENGTH = 4096
SENSITIVE_DEBUG_FIELD_NAMES = {
    "ak",
    "authorization",
    "cookie",
    "identityinfo",
    "secret",
    "signature",
    "sk",
    "token",
}


@OPERATORS.register_module(OP_NAME)
class VideoUrlRpcMapper(Mapper):
    """Resolve VideoArch play URLs from vid through SmartPlayer MGetPlayInfosV2."""

    _batched_op = True

    def __init__(
        self,
        vid_key: str = "vid",
        output_key: str = "urls",
        condition: str = "",
        quality_preference: str = "720p",
        ak: str = "",
        sk: str = "",
        psm: str = "toutiao.videoarch.smart_player",
        cluster: str = "aweme",
        caller: str = DEFAULT_EULER_CALLER,
        outside_url: bool = False,
        url_type: int | None = None,
        ssl: bool | None = None,
        cdn_type: int | None = None,
        ttl: int | None = None,
        indate: int | None = None,
        need_ori: bool = True,
        max_vids_per_request: int = DEFAULT_MAX_VIDS_PER_REQUEST,
        qps: int | None = None,
        timeout: float = 5.0,
        retry_times: int = 3,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if max_vids_per_request <= 0 or max_vids_per_request > MAX_VIDS_PER_REQUEST:
            raise ValueError(f"max_vids_per_request must be between 1 and {MAX_VIDS_PER_REQUEST}")
        resolved_url_type = url_type if url_type is not None else (6 if outside_url else 9)
        if resolved_url_type not in ALLOWED_URL_TYPES:
            raise ValueError(f"url_type must be one of {sorted(ALLOWED_URL_TYPES)}")
        if ttl is not None and indate is not None and ttl != indate:
            raise ValueError("ttl and indate cannot both be set to different values; use indate")
        resolved_indate = indate if indate is not None else ttl
        if resolved_indate is not None and resolved_indate <= 0:
            raise ValueError("indate must be positive when set")
        validate_qps(qps)
        self.vid_key = vid_key
        self.output_key = output_key
        self.condition = condition
        self._condition = RowCondition(condition)
        self.quality_preference = str(quality_preference)
        self.ak = ak
        self.sk = sk
        self.psm = psm
        self.cluster = cluster
        self.caller = caller
        self.outside_url = outside_url
        self.url_type = resolved_url_type
        self.ssl = ssl if ssl is not None else resolved_url_type in (6, 7, 11)
        self.cdn_type = cdn_type
        self.ttl = ttl
        self.indate = resolved_indate
        self.need_ori = need_ori
        self.max_vids_per_request = max_vids_per_request
        self.qps = qps
        self.timeout = timeout
        self.retry_times = retry_times
        self.method = "MGetPlayInfosV2"
        self._rpc_qps_limiter = RpcQpsRateLimiter(qps, self._rate_limiter_key())
        self._client = None
        self._api_thrift = None
        self._logged_first_batch = False
        self._rpc_error_log_count = 0

    def __getstate__(self):
        state = dict(self.__dict__)
        state["_client"] = None
        state["_api_thrift"] = None
        return state

    def prepare_backend_for_ray_tasks(self):
        self._rpc_qps_limiter.setup_ray_actor()

    def run(self, dataset, *, exporter=None, tracer=None):
        self.prepare_backend_for_ray_tasks()
        return super().run(dataset, exporter=exporter, tracer=tracer)

    def process_single(self, sample):
        if not self._condition.matches(sample):
            sample[self.output_key] = []
            return sample
        vid = sample.get(self.vid_key)
        sample[self.output_key] = self._resolve_urls(vid)
        return sample

    def process_batched(self, samples):
        input_schema = samples.schema if isinstance(samples, pa.Table) else None
        return_arrow = isinstance(samples, pa.Table)
        if return_arrow:
            samples = samples.to_pydict()
        rows = self._dict_to_rows(samples)
        self._log_first_batch(rows)
        started_at = time.monotonic()
        stats = Counter()
        output_rows = [dict(row) for row in rows]
        pending: list[tuple[int, str]] = []
        for idx, row in enumerate(output_rows):
            if not self._condition.matches(row):
                row[self.output_key] = []
                stats.update({"condition_skipped": 1})
                continue
            stats.update({"condition_matched": 1})
            vid = row.get(self.vid_key)
            if vid is None or str(vid).strip() == "":
                row[self.output_key] = []
                stats.update({"status_empty_vid": 1})
                continue
            pending.append((idx, str(vid)))

        for start in range(0, len(pending), self.max_vids_per_request):
            chunk = pending[start : start + self.max_vids_per_request]
            url_map, status_map, attempts = self._resolve_urls_batch_with_status([vid for _, vid in chunk])
            stats["rpc_attempts"] += attempts
            for row_idx, vid in chunk:
                urls = url_map.get(vid, [])
                output_rows[row_idx][self.output_key] = urls
                status = status_map.get(vid, "error")
                stats[f"status_{status}"] += 1
                stats["urls"] += len(urls)
        self._log_batch_summary(stats, time.monotonic() - started_at)
        if return_arrow:
            return self._rows_to_table(output_rows, input_schema)
        return self._rows_to_dict(output_rows, samples.keys())

    def _process_single_with_stats(self, sample):
        if not self._condition.matches(sample):
            sample[self.output_key] = []
            return sample, Counter({"condition_skipped": 1})
        vid = sample.get(self.vid_key)
        urls, status, attempts = self._resolve_urls_with_status(vid)
        sample[self.output_key] = urls
        stats = Counter({"condition_matched": 1, f"status_{status}": 1})
        stats["rpc_attempts"] = attempts
        stats["urls"] = len(urls)
        return sample, stats

    def _resolve_urls(self, vid: Any) -> list[str]:
        urls, _status, _attempts = self._resolve_urls_with_status(vid)
        return urls

    def _resolve_urls_with_status(self, vid: Any) -> tuple[list[str], str, int]:
        if vid is None or str(vid).strip() == "":
            return [], "empty_vid", 0
        vid = str(vid)
        url_map, status_map, attempts = self._resolve_urls_batch_with_status([vid])
        return url_map.get(vid, []), status_map.get(vid, "error"), attempts

    def _resolve_urls_batch_with_status(self, vids: Sequence[str]) -> tuple[dict[str, list[str]], dict[str, str], int]:
        vids = [str(vid) for vid in vids if str(vid).strip()]
        if not vids:
            return {}, {}, 0
        attempts = max(1, self.retry_times)
        last_err = None
        for attempt in range(1, attempts + 1):
            try:
                url_map = self._resolve_urls_batch_once(vids)
                status_map = {vid: "success" if url_map.get(vid) else "empty_result" for vid in vids}
                return url_map, status_map, attempt
            except Exception as err:
                last_err = err
                continue
        self._log_rpc_failure(last_err, attempts)
        return {vid: [] for vid in vids}, {vid: "error" for vid in vids}, attempts

    def _resolve_url_once(self, vid: str) -> list[str]:
        return self._resolve_urls_batch_once([vid]).get(vid, [])

    def _resolve_urls_batch_once(self, vids: Sequence[str]) -> dict[str, list[str]]:
        client, api_thrift = self._get_client_and_thrift()
        req = self._build_request(api_thrift, list(vids))
        try:
            self._rpc_qps_limiter.acquire()
            resp = client.MGetPlayInfosV2(req)
            urls_by_vid = {}
            for vid in vids:
                video_info = self._get_video_info(resp, vid)
                if video_info is None:
                    urls_by_vid[vid] = []
                    continue
                url = getattr(video_info, "MainUrl", None)
                urls_by_vid[vid] = [url] if url else []
            empty_vids = [vid for vid in vids if not urls_by_vid.get(vid)]
            self._maybe_log_empty_result_rpc_payload(req, resp, empty_vids)
        except Exception:
            emit_rpc_qps(op_name=self._name, target=self._target(), method=self.method, status="error")
            raise
        emit_rpc_qps(op_name=self._name, target=self._target(), method=self.method, status="success")
        return urls_by_vid

    def _get_client_and_thrift(self):
        if self._client is None or self._api_thrift is None:
            self._client, self._api_thrift = self._create_client_and_thrift()
        return self._client, self._api_thrift

    def _create_client_and_thrift(self):
        try:
            import euler
        except ImportError as exc:
            raise RuntimeError("Euler RPC runtime is required. Install the internal package `bytedeuler`.") from exc
        api_thrift = _load_smart_player_thrift()
        target = f"sd://{self.psm}?cluster={self.cluster}"
        client = euler.Client(
            api_thrift.SmartPlayerService,
            target,
            timeout=self.timeout,
            transport="ttheader",
            protocol="binary",
        )

        def env_middleware(ctx, *middleware_args, **middleware_kwargs):
            ctx.persistent["cluster"] = self.cluster
            return ctx.next(*middleware_args, **middleware_kwargs)

        if hasattr(client, "use"):
            client.use(env_middleware)
        return client, api_thrift

    def _build_request(self, api_thrift, vids: str | Sequence[str]):
        req = api_thrift.MGetPlayInfosV2Request()
        req.VIDs = [vids] if isinstance(vids, str) else list(vids)
        req.FilterParams = api_thrift.FilterParams(
            NeedDefinition=self._definition_value(api_thrift, self.quality_preference)
        )
        req.UrlParams = api_thrift.UrlParams(UrlType=self.url_type)
        self._set_optional_thrift_field(req.UrlParams, "SSL", self.ssl)
        self._set_optional_thrift_field(req.UrlParams, "CdnType", self.cdn_type)
        self._set_optional_thrift_field(req.UrlParams, "Indate", self.indate)
        req.NeedOriginalVideoInfo = self.need_ori
        req.Identity = api_thrift.Identity(
            IdentityInfo=sign_rpc_request(
                self._expand_env(self.ak),
                self._expand_env(self.sk),
                self.method,
                self.caller,
            )
        )
        return req

    @staticmethod
    def _expand_env(value: str) -> str:
        return os.path.expandvars(value) if isinstance(value, str) else value

    @staticmethod
    def _set_optional_thrift_field(obj, name: str, value: Any) -> None:
        if value is None:
            return
        thrift_spec = getattr(obj, "thrift_spec", None)
        if thrift_spec is not None:
            field_names = {spec[1] for spec in thrift_spec.values() if spec is not None}
            if name not in field_names:
                return
        setattr(obj, name, value)

    @staticmethod
    def _definition_value(api_thrift, quality_preference: str):
        if quality_preference in ("high", "low", "ori"):
            return 0
        return getattr(api_thrift.VideoDefinition, f"V{quality_preference.upper()}")

    def _get_video_info(self, resp, vid: str):
        video_infos = getattr(resp, "VideoInfos", None) or {}
        if vid not in video_infos:
            return None
        play_info = video_infos[vid]
        if getattr(play_info, "Status", 10) != 10:
            return None
        original = getattr(play_info, "OriginalVideoInfo", None)
        if original is not None and self.quality_preference in ("high", "ori"):
            return original
        videos = [
            item
            for item in (getattr(play_info, "VideoInfos", None) or [])
            if self.need_ori or getattr(getattr(item, "VideoMeta", None), "EncodedType", None) != "original"
        ]
        if not videos and original is None:
            return None
        if original is not None:
            videos.append(original)
        videos.sort(key=lambda item: getattr(getattr(item, "VideoMeta", None), "Width", 0) or 0)
        if self.quality_preference == "high":
            return videos[-1]
        if self.quality_preference == "low":
            return videos[0]
        for item in videos:
            if getattr(getattr(item, "VideoMeta", None), "Definition", None) == self.quality_preference:
                return item
        return videos[-1]

    def _target(self) -> str:
        return f"sd://{self.psm}?cluster={self.cluster}"

    def _rate_limiter_key(self) -> str:
        return f"{self._target()}:{self.method}"

    def _log_first_batch(self, rows: list[dict[str, Any]]) -> None:
        if self._logged_first_batch:
            return
        matched = sum(1 for row in rows if self._condition.matches(row))
        empty_vid = sum(1 for row in rows if row.get(self.vid_key) is None or str(row.get(self.vid_key)).strip() == "")
        logger.info(
            "VideoUrlRpcMapper first worker batch: pid={}, rows={}, condition_matched={}, empty_vid_rows={}, "
            "psm={}, cluster={}, method={}, quality_preference={}, max_vids_per_request={}, qps={}, "
            "url_type={}, retry_times={}, timeout={}",
            os.getpid(),
            len(rows),
            matched,
            empty_vid,
            self.psm,
            self.cluster,
            self.method,
            self.quality_preference,
            self.max_vids_per_request,
            self.qps,
            self.url_type,
            self.retry_times,
            self.timeout,
        )
        self._logged_first_batch = True

    def _log_batch_summary(self, stats: Counter, elapsed_seconds: float) -> None:
        logger.info(
            "VideoUrlRpcMapper batch summary: pid={}, rows={}, condition_matched={}, condition_skipped={}, "
            "empty_vid_rows={}, empty_result_rows={}, error_rows={}, rpc_attempts={}, output_url_count={}, "
            "elapsed_seconds={:.3f}",
            os.getpid(),
            stats["condition_matched"] + stats["condition_skipped"],
            stats["condition_matched"],
            stats["condition_skipped"],
            stats["status_empty_vid"],
            stats["status_empty_result"],
            stats["status_error"],
            stats["rpc_attempts"],
            stats["urls"],
            elapsed_seconds,
        )

    def _log_rpc_failure(self, err: BaseException | None, attempts: int) -> None:
        if self._rpc_error_log_count >= 5:
            return
        logger.error(
            "VideoUrlRpcMapper RPC failed after retries: pid={}, attempts={}, psm={}, cluster={}, method={}, error={}",
            os.getpid(),
            attempts,
            self.psm,
            self.cluster,
            self.method,
            self._format_error_for_log(err),
        )
        self._rpc_error_log_count += 1

    def _maybe_log_empty_result_rpc_payload(self, req, resp, empty_vids: list[str]) -> None:
        if not empty_vids or random.random() >= EMPTY_RESULT_RPC_DEBUG_SAMPLE_RATE:
            return
        logger.warning(
            "VideoUrlRpcMapper empty-result RPC sampled: pid={}, empty_vids={}, psm={}, cluster={}, method={}, "
            "request={}, response={}",
            os.getpid(),
            empty_vids,
            self.psm,
            self.cluster,
            self.method,
            self._safe_debug_payload(req),
            self._safe_debug_payload(resp),
        )

    @staticmethod
    def _format_error_for_log(err: BaseException | None) -> str:
        if err is None:
            return "unknown"
        message = str(err).replace("\n", "\\n")
        if not message:
            return err.__class__.__name__
        return f"{err.__class__.__name__}: {message}"

    @classmethod
    def _safe_debug_payload(cls, value: Any, field_name: str | None = None, depth: int = 0):
        if cls._is_sensitive_debug_field(field_name):
            return "<redacted>"
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, str):
            return cls._truncate_debug_string(value)
        if isinstance(value, bytes):
            return f"<bytes len={len(value)}>"
        if depth >= DEBUG_PAYLOAD_MAX_DEPTH:
            return cls._truncate_debug_string(repr(value))
        if isinstance(value, dict):
            return cls._safe_debug_mapping(value, depth)
        if isinstance(value, (list, tuple, set)):
            return [
                cls._safe_debug_payload(item, depth=depth + 1)
                for item in list(value)[:DEBUG_PAYLOAD_MAX_ITEMS]
            ]

        fields = cls._debug_payload_fields(value)
        if fields:
            return cls._safe_debug_mapping(fields, depth)
        return cls._truncate_debug_string(repr(value))

    @classmethod
    def _safe_debug_mapping(cls, mapping: dict, depth: int):
        items = list(mapping.items())
        payload = {
            str(key): cls._safe_debug_payload(value, field_name=str(key), depth=depth + 1)
            for key, value in items[:DEBUG_PAYLOAD_MAX_ITEMS]
        }
        if len(items) > DEBUG_PAYLOAD_MAX_ITEMS:
            payload["..."] = f"truncated {len(items) - DEBUG_PAYLOAD_MAX_ITEMS} fields"
        return payload

    @staticmethod
    def _debug_payload_fields(value: Any) -> dict[str, Any]:
        fields = {}
        thrift_spec = getattr(value, "thrift_spec", None)
        if isinstance(thrift_spec, dict):
            for spec in thrift_spec.values():
                if spec is None or len(spec) < 2:
                    continue
                name = spec[1]
                if isinstance(name, str) and hasattr(value, name):
                    fields[name] = getattr(value, name)

        try:
            attrs = vars(value)
        except TypeError:
            attrs = {}
        for key, attr_value in attrs.items():
            if not key.startswith("_") and not callable(attr_value):
                fields.setdefault(key, attr_value)
        return fields

    @staticmethod
    def _is_sensitive_debug_field(field_name: str | None) -> bool:
        if not field_name:
            return False
        lowered = field_name.lower()
        sensitive_markers = (
            "authorization",
            "cookie",
            "identityinfo",
            "secret",
            "signature",
            "token",
        )
        return lowered in SENSITIVE_DEBUG_FIELD_NAMES or any(
            marker in lowered for marker in sensitive_markers
        )

    @staticmethod
    def _truncate_debug_string(value: str) -> str:
        if len(value) <= DEBUG_PAYLOAD_MAX_STRING_LENGTH:
            return value
        return value[:DEBUG_PAYLOAD_MAX_STRING_LENGTH] + "...<truncated>"

    @staticmethod
    def _dict_to_rows(samples: dict[str, list[Any]]) -> list[dict[str, Any]]:
        keys = list(samples.keys())
        if not keys:
            return []
        return [{key: samples[key][i] for key in keys} for i in range(len(samples[keys[0]]))]

    def _rows_to_dict(self, rows: list[dict[str, Any]], original_keys) -> dict[str, list[Any]]:
        keys = list(original_keys)
        if self.output_key not in keys:
            keys.append(self.output_key)
        if not rows:
            return {key: [] for key in keys}
        return {key: [row.get(key) for row in rows] for key in keys}

    def _rows_to_table(self, rows: list[dict[str, Any]], input_schema: pa.Schema | None) -> pa.Table:
        keys = list(input_schema.names if input_schema is not None else [])
        if self.output_key not in keys:
            keys.append(self.output_key)
        arrays = []
        fields = []
        for key in keys:
            values = [row.get(key) for row in rows]
            arrow_type = (
                pa.list_(pa.string())
                if key == self.output_key
                else self._input_or_inferred_type(key, values, input_schema)
            )
            arrays.append(pa.array(values, type=arrow_type))
            fields.append(pa.field(key, arrow_type))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    @staticmethod
    def _input_or_inferred_type(key: str, values: list[Any], input_schema: pa.Schema | None) -> pa.DataType:
        if input_schema is not None:
            field_index = input_schema.get_field_index(key)
            if field_index >= 0:
                return input_schema.field(field_index).type
        inferred = pa.array(values)
        if pa.types.is_null(inferred.type):
            return pa.string()
        return inferred.type


def sign_rpc_request(ak: str, sk: str, method: str = "", caller: str = "", extra=None, ttl: int = 0, now=None) -> str:
    if extra is None:
        extra = {}
    if ttl <= 0:
        ttl = DEFAULT_SIGN_TTL
    now_value = time.time() if now is None else now
    deadline = str(int(now_value) + ttl)
    items = [f"method={method}", f"caller={caller}", f"deadline={deadline}"]
    items.extend(f"{key}={extra[key]}" for key in sorted(extra))
    raw = "&".join(items)
    digest = hmac.new(codecs.encode(sk), codecs.encode(raw), hashlib.sha1).digest()
    ciphertext = base64.standard_b64encode(digest)
    return ":".join([AUTH_PREFIX_V1, ak, deadline, codecs.decode(ciphertext)])


def _load_smart_player_thrift():
    import thriftpy2

    idl_dir = Path(__file__).resolve().parents[1] / "idl"
    return thriftpy2.load(
        str(idl_dir / "smart_player.thrift"),
        module_name="data_juicer_smart_player_thrift",
        include_dirs=[str(idl_dir)],
    )
