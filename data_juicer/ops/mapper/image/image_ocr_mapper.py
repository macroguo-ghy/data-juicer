from __future__ import annotations

import asyncio
from collections import defaultdict, deque
import hashlib
import json
import os
import itertools
from functools import lru_cache
from pathlib import Path
import re
import time
from datetime import datetime
from typing import Any, Sequence

from loguru import logger
import pyarrow as pa

from data_juicer.core.data import NestedDataset
from data_juicer.ops.base_op import OPERATORS, Pipeline
from data_juicer.utils.metrics_utils import emit_rpc_qps

OP_NAME = "image_ocr_mapper"
OCR_ARROW_TYPE = pa.list_(pa.string())
DEFAULT_SOURCE_CLUSTER = "default"
OCR_RPC_CONNECT_RETRY_SLEEP_SECONDS = 0.1
OCR_RATE_LIMIT_WINDOW_SECONDS = 1.0
MAX_OCR_IMAGES_PER_REQUEST = 16
GDPR_TOKEN_ENV = "INJECTED_SEC_TOKEN_STRING"
GDPR_TOKEN_PATH_ENV = "SEC_TOKEN_PATH"
GDPR_TOKEN_FALLBACK_ENV = "SEC_TOKEN_STRING"
GDPR_TOKEN_EXTRA_KEY = "gdpr-token"
ImageInput = bytes | bytearray | memoryview | str | dict[str, Any]


class OcrResponseStatusError(RuntimeError):
    """Raised when OCR RPC returns a non-success response status."""


def _ensure_requester_env(source_psm: str, source_cluster: str) -> None:
    os.environ["LOAD_SERVICE_PSM"] = source_psm
    os.environ["PSM"] = source_psm
    os.environ["TCE_PSM"] = source_psm
    os.environ["TCE_CLUSTER"] = source_cluster
    os.environ["SERVICE_CLUSTER"] = source_cluster


def _build_target(psm: str, cluster: str) -> str:
    return f"sd://{psm}?cluster={cluster}"


class _RayJobOcrRateLimiter:
    def __init__(self):
        self._request_events = defaultdict(deque)
        self._next_available_at = defaultdict(float)
        self._limits = {}

    def register(self, key: str, qps: int | None) -> None:
        self._update_limit(key, qps)

    async def acquire(self, key: str, qps: int | None) -> None:
        self._update_limit(key, qps)
        while True:
            limit = self._limits.get(key)
            if limit is None:
                return
            now = time.monotonic()
            self._prune(key, now)
            wait_seconds = 0.0
            if now < self._next_available_at[key]:
                wait_seconds = max(wait_seconds, self._next_available_at[key] - now)
            if len(self._request_events[key]) >= limit:
                wait_seconds = max(
                    wait_seconds,
                    OCR_RATE_LIMIT_WINDOW_SECONDS - (now - self._request_events[key][0]),
                )
            if wait_seconds <= 0:
                self._request_events[key].append(now)
                self._next_available_at[key] = now + (OCR_RATE_LIMIT_WINDOW_SECONDS / limit)
                return
            await asyncio.sleep(wait_seconds)

    def _update_limit(self, key: str, qps: int | None) -> None:
        if qps is None:
            return
        self._limits[key] = qps if key not in self._limits else min(self._limits[key], qps)

    def _prune(self, key: str, now: float) -> None:
        while self._request_events[key] and now - self._request_events[key][0] >= OCR_RATE_LIMIT_WINDOW_SECONDS:
            self._request_events[key].popleft()


def _try_import_ray():
    try:
        import ray
    except ImportError:
        return None
    return ray


def _remote_ocr_rate_limiter_actor(ray_module):
    return ray_module.remote(_RayJobOcrRateLimiter)


def _ocr_rate_limiter_actor_name(job_id: str, limiter_key: str) -> str:
    normalized_job = re.sub(r"[^0-9A-Za-z_]", "_", job_id or "default")
    key_digest = hashlib.sha1(limiter_key.encode("utf-8")).hexdigest()[:12]
    return f"dj_image_ocr_rate_limiter_{normalized_job}_{key_digest}"


def _read_token_file(path: str) -> str:
    if not path:
        return ""
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _build_gdpr_auth_middleware(gdpr_token: str, extra_key: str = GDPR_TOKEN_EXTRA_KEY):
    def middleware(ctx, *middleware_args, **middleware_kwargs):
        for req in itertools.chain(middleware_args, middleware_kwargs.values()):
            base = getattr(req, "Base", None)
            if base is None:
                continue
            extra_name = "extra" if hasattr(base, "extra") else "Extra"
            extra = getattr(base, extra_name, None)
            if not extra:
                extra = {}
                setattr(base, extra_name, extra)
            extra[extra_key] = gdpr_token
            ctx.local["gdpr_token"] = gdpr_token
        return ctx.next(*middleware_args, **middleware_kwargs)

    return middleware


@lru_cache(maxsize=1)
def _load_lab_ocr_thrift():
    import thriftpy2

    idl_dir = Path(__file__).resolve().parents[1] / "idl" / "lab_ocr_general"
    return thriftpy2.load(
        str(idl_dir / "ocr.thrift"),
        module_name="data_juicer_lab_ocr_general_thrift",
        include_dirs=[str(idl_dir)],
    )


def _thrift_obj_to_dict(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, (int, float, bool, str)):
        return obj
    if isinstance(obj, (list, tuple)):
        return [_thrift_obj_to_dict(item) for item in obj]
    if isinstance(obj, dict):
        return {key: _thrift_obj_to_dict(value) for key, value in obj.items()}

    result = {}
    if hasattr(obj, "thrift_spec"):
        for spec in obj.thrift_spec.values():
            if spec is None:
                continue
            _, field_name, *_ = spec
            value = getattr(obj, field_name, None)
            if value is not None:
                result[field_name] = _thrift_obj_to_dict(value)
        return result

    if hasattr(obj, "__dict__"):
        return {
            key: _thrift_obj_to_dict(value)
            for key, value in vars(obj).items()
            if not key.startswith("_")
        }
    return obj


def _calculate_ocr_area(words: Sequence[Any] | None) -> float:
    ocr_area = 0.0
    for word in words or []:
        min_x, min_y = float("inf"), float("inf")
        max_x, max_y = 0.0, 0.0
        for point in getattr(word, "det_points_abs", None) or []:
            min_x = min(min_x, getattr(point, "x", 0.0))
            max_x = max(max_x, getattr(point, "x", 0.0))
            min_y = min(min_y, getattr(point, "y", 0.0))
            max_y = max(max_y, getattr(point, "y", 0.0))
        if min_x != float("inf") and min_y != float("inf") and max_x and max_y:
            ocr_area += (max_x - min_x) * (max_y - min_y)
    return ocr_area


def _serialize_ocr_response(resp: Any) -> list[str]:
    results = list(getattr(resp, "results", None) or [])
    serialized = _thrift_obj_to_dict(results)
    if not isinstance(serialized, list):
        serialized = []

    for index, result in enumerate(results):
        if index >= len(serialized):
            serialized.append({})
        elif not isinstance(serialized[index], dict):
            serialized[index] = {}

        extra = getattr(result, "extra", None) or {}
        try:
            image_width = float(extra.get("width", 0.0))
            image_height = float(extra.get("height", 0.0))
        except (TypeError, ValueError):
            image_width = 0.0
            image_height = 0.0
        image_area = image_width * image_height
        serialized[index]["ocr_area_ratio"] = (
            _calculate_ocr_area(getattr(result, "words", None)) / image_area
            if image_area
            else 0.0
        )

    return [json.dumps(item, ensure_ascii=False, default=str) for item in serialized]


@OPERATORS.register_module(OP_NAME)
class ImageOcrMapper(Pipeline):
    """Add OCR results to image rows."""

    def __init__(
        self,
        image_key: str = "images",
        ocr_result_key: str = "ocr_result",
        psm: str = "lab.ocrx.fusion_general",
        cluster: str = "default",
        timeout: int = 30,
        caller: str = "ad.ai.data_forge_merlin",
        source_cluster: str = DEFAULT_SOURCE_CLUSTER,
        expected_caller_psm: str | None = "ad.ai.data_forge_merlin",
        enable_gdpr_auth: bool = True,
        gdpr_token_env: str = GDPR_TOKEN_ENV,
        gdpr_token_path_env: str = GDPR_TOKEN_PATH_ENV,
        gdpr_token_fallback_env: str = GDPR_TOKEN_FALLBACK_ENV,
        gdpr_token_extra_key: str = GDPR_TOKEN_EXTRA_KEY,
        split_size: int = 10,
        dag: str = "text_tag",
        rpc_method: str = "PredictImages",
        day_interval_seconds: float = 4.0,
        night_interval_seconds: float = 1.0,
        qps: int | None = None,
        repartition_num_blocks: int | None = None,
        status_retry_attempts: int = 2,
        status_retry_backoff_seconds: float = 0.5,
        *args,
        **kwargs,
    ):
        """
        Initialization method.
        :param image_key: input field containing a list of image bytes.
        :param ocr_result_key: output field for OCR JSON strings.
        :param psm: OCR service PSM.
        :param cluster: OCR service cluster.
        :param timeout: OCR RPC timeout in seconds.
        :param caller: caller identity passed to OCR RPC.
        :param source_cluster: caller/source cluster for Euler Base and env.
        :param expected_caller_psm: expected internal CALLER_PSM value.
        :param enable_gdpr_auth: whether to inject GDPR token into request Base.
        :param gdpr_token_env: env var that stores the GDPR token.
        :param gdpr_token_path_env: env var that stores a GDPR token file path.
        :param gdpr_token_fallback_env: fallback env var for the GDPR token.
        :param gdpr_token_extra_key: Base extra key used for the GDPR token.
        :param split_size: max images per OCR RPC request. Must be no more
            than 16.
        :param dag: OCR DAG name.
        :param rpc_method: Euler OCR method name. Defaults to PredictImages.
        :param day_interval_seconds: throttle interval outside 01:00-08:00.
        :param night_interval_seconds: throttle interval from 01:00 to 08:00.
        :param qps: Ray job-level OCR RPC request QPS limit. When set, this
            replaces the per-worker interval throttle.
        :param repartition_num_blocks: if set in Ray mode, repartition input
            into this many blocks before OCR to avoid long-running oversized
            OCR tasks.
        :param status_retry_attempts: extra retry attempts for OCR response
            status errors.
        :param status_retry_backoff_seconds: base backoff seconds for OCR
            response status retries.
        """
        super().__init__(*args, **kwargs)
        if split_size <= 0:
            raise ValueError("split_size must be positive")
        if split_size > MAX_OCR_IMAGES_PER_REQUEST:
            raise ValueError(f"split_size must be no more than {MAX_OCR_IMAGES_PER_REQUEST}")
        if qps is not None and qps <= 0:
            raise ValueError("qps must be positive when set")
        if repartition_num_blocks is not None and repartition_num_blocks <= 0:
            raise ValueError("repartition_num_blocks must be positive when set")
        if status_retry_attempts < 0:
            raise ValueError("status_retry_attempts must be non-negative")
        if status_retry_backoff_seconds < 0:
            raise ValueError("status_retry_backoff_seconds must be non-negative")
        self.image_key = image_key
        self.ocr_result_key = ocr_result_key
        self.psm = psm
        self.cluster = cluster
        self.timeout = timeout
        self.caller = caller
        self.source_cluster = source_cluster
        self.expected_caller_psm = expected_caller_psm
        self.enable_gdpr_auth = enable_gdpr_auth
        self.gdpr_token_env = gdpr_token_env
        self.gdpr_token_path_env = gdpr_token_path_env
        self.gdpr_token_fallback_env = gdpr_token_fallback_env
        self.gdpr_token_extra_key = gdpr_token_extra_key
        self.split_size = split_size
        self.dag = dag
        self.rpc_method = rpc_method
        self.day_interval_seconds = day_interval_seconds
        self.night_interval_seconds = night_interval_seconds
        self.qps = qps
        self.repartition_num_blocks = repartition_num_blocks
        self.status_retry_attempts = status_retry_attempts
        self.status_retry_backoff_seconds = status_retry_backoff_seconds
        self._request_events = deque()
        self._rate_limit_next_available_at = 0.0
        self._rate_limiter_actor = None
        self._rate_limiter_actor_name = None
        self._client = None
        self._api_thrift = None
        self._logged_first_batch = False
        self._ocr_error_log_count = 0

    def __getstate__(self):
        state = dict(self.__dict__)
        state["_client"] = None
        state["_api_thrift"] = None
        return state

    def run(self, dataset, *, exporter=None, tracer=None):
        if isinstance(dataset, NestedDataset):
            self._rate_limiter_actor = None
            self._rate_limiter_actor_name = None
            return dataset.map(
                self.process_batched,
                batched=True,
                batch_size=self.batch_size,
                num_proc=self.runtime_np(),
                desc=self._name + "_process",
            )

        self._setup_ray_rate_limiter()
        interval = None if self.qps is not None else self.current_ocr_interval()
        logger.info(
            "ImageOcrMapper Ray run: batch_size={}, concurrency={}, num_cpus={}, "
            "repartition_num_blocks={}, split_size={}, qps={}, interval_seconds={}, rate_limiter_actor={}",
            self.batch_size,
            self.runtime_np(),
            self.num_cpus,
            self.repartition_num_blocks,
            self.split_size,
            self.qps,
            interval,
            self._rate_limiter_actor_name or "local",
        )
        if self.repartition_num_blocks is not None:
            logger.info("ImageOcrMapper repartition input to {} blocks before OCR", self.repartition_num_blocks)
            dataset = dataset.repartition(num_blocks=self.repartition_num_blocks, shuffle=False)

        map_kwargs = {
            "batch_format": "pyarrow",
            "batch_size": self.batch_size,
        }
        concurrency = self.runtime_np()
        if concurrency and concurrency > 0:
            map_kwargs["concurrency"] = concurrency
        if self.num_cpus is not None:
            map_kwargs["num_cpus"] = self.num_cpus
        if self.num_gpus is not None:
            map_kwargs["num_gpus"] = self.num_gpus
        if self.memory is not None:
            map_kwargs["memory"] = self.memory
        if self.runtime_env is not None:
            map_kwargs["runtime_env"] = self.runtime_env

        return dataset.map_batches(self.process_batched, **map_kwargs)

    def process_single(self, sample: dict[str, Any]) -> dict[str, Any]:
        row = dict(sample)
        row[self.ocr_result_key] = self.batch_get_ocr(sample.get(self.image_key))
        return row

    def process_batched(self, samples):
        input_schema = None
        return_arrow = isinstance(samples, pa.Table)
        if return_arrow:
            input_schema = samples.schema
            rows = samples.to_pylist()
            keys = input_schema.names
        else:
            keys = list(samples.keys())
            if not keys:
                return {self.ocr_result_key: []}
            rows = [{key: samples[key][idx] for key in keys} for idx in range(len(samples[keys[0]]))]

        self._log_first_batch(rows)
        output_rows = [self.process_single(row) for row in rows]

        if return_arrow:
            return self._rows_to_arrow_table(output_rows, input_schema)
        if not output_rows:
            return {key: [] for key in self._output_keys(keys)}
        return {key: [row.get(key) for row in output_rows] for key in self._output_keys(keys)}

    def batch_get_ocr(self, image_values: Any) -> list[str]:
        image_inputs = self._as_image_input_list(image_values)
        if not image_inputs:
            return []

        all_ocr_result = []
        for start in range(0, len(image_inputs), self.split_size):
            ocr_result = self.get_ocr(image_inputs[start : start + self.split_size])
            if ocr_result is None:
                return []
            all_ocr_result.extend(ocr_result)
        return all_ocr_result

    def get_ocr(self, image_inputs: Sequence[ImageInput]) -> list[str] | None:
        start_time = time.time()
        try:
            return self._call_ocr_rpc_with_status_retries(image_inputs)
        finally:
            if self.qps is None:
                elapsed = time.time() - start_time
                interval = self.current_ocr_interval()
                if elapsed < interval:
                    time.sleep(interval - elapsed)

    def _call_ocr_rpc_with_status_retries(self, image_inputs: Sequence[ImageInput]) -> list[str] | None:
        max_attempts = self.status_retry_attempts + 1
        for attempt in range(1, max_attempts + 1):
            try:
                return self._call_ocr_rpc_with_connection_retry(image_inputs)
            except OcrResponseStatusError as err:
                if attempt >= max_attempts:
                    return self._log_ocr_rpc_failure(image_inputs, err, attempts=attempt)
                backoff_seconds = self.status_retry_backoff_seconds * (2 ** (attempt - 1))
                if backoff_seconds > 0:
                    time.sleep(backoff_seconds)
            except Exception as err:  # noqa: BLE001
                return self._log_ocr_rpc_failure(image_inputs, err, attempts=attempt)
        return None

    def _call_ocr_rpc_with_connection_retry(self, image_inputs: Sequence[ImageInput]) -> list[str]:
        try:
            return self._call_ocr_rpc_with_rate_limit(image_inputs)
        except Exception as err:
            if not self._is_rpc_connection_error(err):
                raise
            self._client = None
            self._api_thrift = None
            logger.warning(
                "ImageOcrMapper OCR RPC connection failed, retrying once after {}s: pid={}, image_count={}, error={}",
                OCR_RPC_CONNECT_RETRY_SLEEP_SECONDS,
                os.getpid(),
                len(image_inputs),
                err,
            )
            time.sleep(OCR_RPC_CONNECT_RETRY_SLEEP_SECONDS)
            return self._call_ocr_rpc_with_rate_limit(image_inputs)

    def _call_ocr_rpc_with_rate_limit(self, image_inputs: Sequence[ImageInput]) -> list[str]:
        self._apply_rate_limit()
        return self._call_ocr_rpc(image_inputs)

    def _apply_rate_limit(self) -> None:
        if self.qps is None:
            return
        limiter_key = self._rate_limiter_key()
        if self._rate_limiter_actor is not None:
            ray = _try_import_ray()
            if ray is not None:
                ray.get(self._rate_limiter_actor.acquire.remote(limiter_key, self.qps))
                return
        while True:
            now = time.monotonic()
            self._prune_rate_limit_events(now)
            wait_seconds = 0.0
            if self._rate_limit_next_available_at > now:
                wait_seconds = max(wait_seconds, self._rate_limit_next_available_at - now)
            if len(self._request_events) >= self.qps:
                wait_seconds = max(
                    wait_seconds,
                    OCR_RATE_LIMIT_WINDOW_SECONDS - (now - self._request_events[0]),
                )
            if wait_seconds <= 0:
                self._request_events.append(now)
                self._rate_limit_next_available_at = now + (OCR_RATE_LIMIT_WINDOW_SECONDS / self.qps)
                return
            time.sleep(wait_seconds)

    def _setup_ray_rate_limiter(self) -> None:
        self._rate_limiter_actor = None
        self._rate_limiter_actor_name = None
        if self.qps is None:
            return
        ray = _try_import_ray()
        if ray is None or not ray.is_initialized():
            return
        limiter_key = self._rate_limiter_key()
        actor_name = _ocr_rate_limiter_actor_name(ray.get_runtime_context().get_job_id(), limiter_key)
        try:
            actor = ray.get_actor(actor_name)
        except ValueError:
            actor = _remote_ocr_rate_limiter_actor(ray).options(name=actor_name, num_cpus=0).remote()
        ray.get(actor.register.remote(limiter_key, self.qps))
        self._rate_limiter_actor = actor
        self._rate_limiter_actor_name = actor_name

    def _prune_rate_limit_events(self, now: float) -> None:
        while self._request_events and now - self._request_events[0] >= OCR_RATE_LIMIT_WINDOW_SECONDS:
            self._request_events.popleft()

    def _rate_limiter_key(self) -> str:
        return f"{self.psm}:{self.cluster}:{self.rpc_method}"

    def _call_ocr_rpc(self, image_inputs: Sequence[ImageInput]) -> list[str]:
        client, api_thrift = self._get_client_and_thrift()
        req = self._build_req(api_thrift, image_inputs)
        target = _build_target(self.psm, self.cluster)
        try:
            resp = getattr(client, self.rpc_method)(req)
            self._check_ocr_response(resp)
            result = _serialize_ocr_response(resp)
        except Exception:
            emit_rpc_qps(op_name=self._name, target=target, method=self.rpc_method, status="error")
            raise
        emit_rpc_qps(op_name=self._name, target=target, method=self.rpc_method, status="success")
        return result

    def _log_ocr_rpc_failure(
        self,
        image_inputs: Sequence[ImageInput],
        err: BaseException | None = None,
        attempts: int = 1,
    ) -> list[str] | None:
        if self._ocr_error_log_count < 5:
            logger.error(
                "ImageOcrMapper OCR RPC failed: pid={}, attempts={}, image_count={}, error={}",
                os.getpid(),
                attempts,
                len(image_inputs),
                self._format_error_for_log(err),
            )
            self._ocr_error_log_count += 1
        return None

    @staticmethod
    def _format_error_for_log(err: BaseException | None) -> str:
        if err is None:
            return "unknown"
        message = str(err).replace("\n", "\\n")
        if not message:
            return err.__class__.__name__
        return f"{err.__class__.__name__}: {message}"

    @staticmethod
    def _is_rpc_connection_error(err: Exception) -> bool:
        seen = set()
        current: BaseException | None = err
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            if isinstance(current, ConnectionError):
                return True
            message = str(current).lower()
            if "could not connect" in message or "connection refused" in message:
                return True
            current = current.__cause__ or current.__context__
        return False

    def current_ocr_interval(self) -> float:
        try:
            import pytz

            hour = datetime.now(pytz.timezone("Asia/Shanghai")).hour
        except Exception:  # noqa: BLE001
            hour = datetime.now().astimezone().hour
        if 1 <= hour < 8:
            return self.night_interval_seconds
        return self.day_interval_seconds

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

        api_thrift = _load_lab_ocr_thrift()
        gdpr_token = os.environ.get(self.gdpr_token_env, "").strip()
        if not gdpr_token and self.gdpr_token_path_env:
            gdpr_token = _read_token_file(os.environ.get(self.gdpr_token_path_env, "").strip())
        if not gdpr_token and self.gdpr_token_fallback_env:
            gdpr_token = os.environ.get(self.gdpr_token_fallback_env, "").strip()
        if self.enable_gdpr_auth and not gdpr_token:
            token_env_names = (
                f"{self.gdpr_token_env} or {self.gdpr_token_fallback_env}"
                if self.gdpr_token_fallback_env
                else self.gdpr_token_env
            )
            raise RuntimeError(f"{token_env_names} must be set for image OCR GDPR auth.")

        _ensure_requester_env(self.caller, self.source_cluster)
        client = euler.Client(
            api_thrift.OcrService,
            _build_target(self.psm, self.cluster),
            timeout=self.timeout,
            transport="ttheader",
            protocol="binary",
        )

        def env_middleware(ctx, *middleware_args, **middleware_kwargs):
            ctx.persistent["cluster"] = self.cluster
            return ctx.next(*middleware_args, **middleware_kwargs)

        client.use(env_middleware)
        client.use(base_compat_middleware.client_middleware)
        if self.enable_gdpr_auth:
            client.use(_build_gdpr_auth_middleware(gdpr_token, self.gdpr_token_extra_key))
        return client, api_thrift

    def _build_req(self, api_thrift, image_inputs: Sequence[ImageInput]):
        if len(image_inputs) > MAX_OCR_IMAGES_PER_REQUEST:
            raise ValueError(f"OCR request images size must be no more than {MAX_OCR_IMAGES_PER_REQUEST}")
        return api_thrift.ImagesOcrRequest(
            images=[self._build_image_info(api_thrift, image_input) for image_input in image_inputs],
            extra={"dag": self.dag},
            Base=self._build_base(api_thrift),
        )

    def _build_base(self, api_thrift):
        return api_thrift.base.Base(
            Caller=self.caller,
            extra={"cluster": self.source_cluster},
        )

    def _check_ocr_response(self, resp) -> None:
        status_code = self._base_resp_status_code(resp)
        if status_code:
            base_resp = getattr(resp, "BaseResp", None)
            status_message = getattr(base_resp, "StatusMessage", "")
            raise OcrResponseStatusError(
                f"{self.__class__.__name__} failed, status_code={status_code}, "
                f"status_message={status_message}"
            )
        for result in getattr(resp, "results", None) or []:
            status = getattr(result, "status", None)
            if status:
                raise OcrResponseStatusError(f"{self.__class__.__name__} failed, status: {status}")

    @staticmethod
    def _base_resp_status_code(resp) -> int:
        base_resp = getattr(resp, "BaseResp", None)
        if base_resp is None:
            return 0
        return getattr(base_resp, "StatusCode", 0) or 0

    @staticmethod
    def _as_bytes_list(value: Any) -> list[bytes]:
        if value is None:
            return []
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (bytes, bytearray, memoryview, str)):
            value = value.tolist()
        if isinstance(value, (bytes, bytearray, memoryview)):
            return [bytes(value)]
        if isinstance(value, (list, tuple)):
            values = []
            for item in value:
                if item is None:
                    continue
                values.extend(ImageOcrMapper._as_bytes_list(item))
            return values
        return []

    @staticmethod
    def _as_image_input_list(value: Any) -> list[ImageInput]:
        if value is None:
            return []
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (bytes, bytearray, memoryview, str, dict)):
            value = value.tolist()
        if isinstance(value, (bytes, bytearray, memoryview)):
            return [bytes(value)]
        if isinstance(value, str):
            return [value] if value.startswith(("http://", "https://")) else []
        if isinstance(value, dict):
            data = value.get("data") or value.get("binary")
            if isinstance(data, (bytes, bytearray, memoryview)):
                return [{"data": bytes(data)}]
            url = value.get("url")
            if isinstance(url, str) and url.startswith(("http://", "https://")):
                return [{"url": url}]
            tos_bucket = value.get("tos_bucket")
            tos_obj = value.get("tos_obj")
            if isinstance(tos_bucket, str) and isinstance(tos_obj, str):
                return [{"tos_bucket": tos_bucket, "tos_obj": tos_obj}]
            return []
        if isinstance(value, (list, tuple)):
            values = []
            for item in value:
                if item is None:
                    continue
                values.extend(ImageOcrMapper._as_image_input_list(item))
            return values
        return []

    @staticmethod
    def _build_image_info(ocr_thrift, image_input: ImageInput):
        if isinstance(image_input, (bytes, bytearray, memoryview)):
            return ocr_thrift.ImageInfo(data=bytes(image_input))
        if isinstance(image_input, str):
            return ocr_thrift.ImageInfo(url=image_input)
        return ocr_thrift.ImageInfo(**image_input)

    def _log_first_batch(self, rows: list[dict[str, Any]]) -> None:
        if self._logged_first_batch:
            return
        image_counts = [len(self._as_image_input_list(row.get(self.image_key))) for row in rows]
        logger.info(
            "ImageOcrMapper first worker batch: pid={}, rows={}, image_count_min={}, "
            "image_count_max={}, empty_image_rows={}",
            os.getpid(),
            len(rows),
            min(image_counts) if image_counts else 0,
            max(image_counts) if image_counts else 0,
            sum(1 for count in image_counts if count == 0),
        )
        self._logged_first_batch = True

    def _output_keys(self, input_keys: Sequence[str]) -> list[str]:
        keys = [key for key in input_keys if key != self.ocr_result_key]
        keys.append(self.ocr_result_key)
        return keys

    def _rows_to_arrow_table(self, rows: list[dict[str, Any]], input_schema: pa.Schema | None) -> pa.Table:
        fields = []
        arrays = []
        if input_schema is not None:
            for field in input_schema:
                if field.name == self.ocr_result_key:
                    continue
                values = [row.get(field.name) for row in rows]
                arrays.append(pa.array(values, type=field.type))
                fields.append(field)

        values = [row.get(self.ocr_result_key) for row in rows]
        arrays.append(pa.array(values, type=OCR_ARROW_TYPE))
        fields.append(pa.field(self.ocr_result_key, OCR_ARROW_TYPE))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))
