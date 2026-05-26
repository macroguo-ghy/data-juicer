from __future__ import annotations

import base64
import codecs
import hmac
import hashlib
import os
import time
from collections import Counter
from pathlib import Path
from typing import Any

import pyarrow as pa
from loguru import logger

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.ops.condition_utils import RowCondition
from data_juicer.utils.metrics_utils import emit_rpc_qps

OP_NAME = "video_url_rpc_mapper"

AUTH_PREFIX_V1 = "VARCH1-HMAC-SHA1"
DEFAULT_SIGN_TTL = 3600
DEFAULT_EULER_CALLER = "ad.ai.data_forge_merlin"


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
        ttl: int | None = None,
        need_ori: bool = True,
        timeout: float = 5.0,
        retry_times: int = 3,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
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
        self.ttl = ttl
        self.need_ori = need_ori
        self.timeout = timeout
        self.retry_times = retry_times
        self.method = "MGetPlayInfosV2"
        self._client = None
        self._api_thrift = None
        self._logged_first_batch = False
        self._rpc_error_log_count = 0

    def __getstate__(self):
        state = dict(self.__dict__)
        state["_client"] = None
        state["_api_thrift"] = None
        return state

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
        output_rows = []
        for row in rows:
            output_row, row_stats = self._process_single_with_stats(row)
            output_rows.append(output_row)
            stats.update(row_stats)
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
        attempts = max(1, self.retry_times)
        last_err = None
        for attempt in range(1, attempts + 1):
            try:
                urls = self._resolve_url_once(str(vid))
                return urls, "success" if urls else "empty_result", attempt
            except Exception as err:
                last_err = err
                continue
        self._log_rpc_failure(last_err, attempts)
        return [], "error", attempts

    def _resolve_url_once(self, vid: str) -> list[str]:
        client, api_thrift = self._get_client_and_thrift()
        req = self._build_request(api_thrift, vid)
        try:
            resp = client.MGetPlayInfosV2(req)
            video_info = self._get_video_info(resp, vid)
            if video_info is None:
                urls = []
            else:
                url = getattr(video_info, "MainUrl", None)
                urls = [url] if url else []
        except Exception:
            emit_rpc_qps(op_name=self._name, target=self._target(), method=self.method, status="error")
            raise
        emit_rpc_qps(op_name=self._name, target=self._target(), method=self.method, status="success")
        return urls

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

    def _build_request(self, api_thrift, vid: str):
        req = api_thrift.MGetPlayInfosV2Request()
        req.VIDs = [vid]
        req.FilterParams = api_thrift.FilterParams(
            NeedDefinition=self._definition_value(api_thrift, self.quality_preference)
        )
        req.UrlParams = api_thrift.UrlParams(UrlType=6 if self.outside_url else 9)
        if self.ttl is not None:
            req.UrlParams.Indate = self.ttl
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

    def _log_first_batch(self, rows: list[dict[str, Any]]) -> None:
        if self._logged_first_batch:
            return
        matched = sum(1 for row in rows if self._condition.matches(row))
        empty_vid = sum(1 for row in rows if row.get(self.vid_key) is None or str(row.get(self.vid_key)).strip() == "")
        logger.info(
            "VideoUrlRpcMapper first worker batch: pid={}, rows={}, condition_matched={}, empty_vid_rows={}, "
            "psm={}, cluster={}, method={}, quality_preference={}, retry_times={}, timeout={}",
            os.getpid(),
            len(rows),
            matched,
            empty_vid,
            self.psm,
            self.cluster,
            self.method,
            self.quality_preference,
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

    @staticmethod
    def _format_error_for_log(err: BaseException | None) -> str:
        if err is None:
            return "unknown"
        message = str(err).replace("\n", "\\n")
        if not message:
            return err.__class__.__name__
        return f"{err.__class__.__name__}: {message}"

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
