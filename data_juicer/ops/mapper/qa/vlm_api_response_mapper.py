from __future__ import annotations

import asyncio
import base64
from collections import defaultdict, deque
import io
import json
import math
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from loguru import logger
import pyarrow as pa

from data_juicer.core.data import NestedDataset
from data_juicer.ops.base_op import OPERATORS, Pipeline
from data_juicer.utils.metrics_utils import emit_vlm_qps

OP_NAME = "vlm_api_response_mapper"
CHAT_COMPLETIONS_RESPONSE_PATH = "choices.0.message.content"
RESPONSES_RESPONSE_PATH = "output.0.content.0.text"
PROMPT_TEMPLATE_FIELD_PATTERN = re.compile(r"\$\{([^}]+)\}")
PROMPT_TEMPLATE_FIELD_ONLY_PATTERN = re.compile(r"^\$\{([^}]+)\}$")
RATE_LIMIT_WINDOW_SECONDS = 60.0


class _RayJobVlmRateLimiter:
    def __init__(self):
        self._request_events = defaultdict(deque)
        self._token_events = defaultdict(deque)
        self._next_available_at = defaultdict(float)
        self._limits = {}

    def register(self, model: str, rpm: int | None, tpm: int | None) -> None:
        self._update_limits(model, rpm, tpm)

    async def acquire(self, model: str, rpm: int | None, tpm: int | None, estimated_tokens: int) -> None:
        self._update_limits(model, rpm, tpm)
        while True:
            limits = self._limits.get(model, {})
            rpm_limit = limits.get("rpm")
            tpm_limit = limits.get("tpm")
            if rpm_limit is None and tpm_limit is None:
                return
            now = time.monotonic()
            self._prune(model, now)
            wait_seconds = 0.0
            if now < self._next_available_at[model]:
                wait_seconds = max(wait_seconds, self._next_available_at[model] - now)
            if rpm_limit is not None and len(self._request_events[model]) >= rpm_limit:
                wait_seconds = max(wait_seconds, RATE_LIMIT_WINDOW_SECONDS - (now - self._request_events[model][0]))
            if tpm_limit is not None:
                used_tokens = sum(tokens for _, tokens in self._token_events[model])
                if self._token_events[model] and used_tokens + estimated_tokens > tpm_limit:
                    wait_seconds = max(
                        wait_seconds,
                        RATE_LIMIT_WINDOW_SECONDS - (now - self._token_events[model][0][0]),
                    )
            if wait_seconds <= 0:
                self._request_events[model].append(now)
                if tpm_limit is not None:
                    self._token_events[model].append((now, estimated_tokens))
                self._next_available_at[model] = now + self._smooth_interval(
                    rpm_limit,
                    tpm_limit,
                    estimated_tokens,
                )
                return
            await asyncio.sleep(wait_seconds)

    def _update_limits(self, model: str, rpm: int | None, tpm: int | None) -> None:
        limits = self._limits.setdefault(model, {"rpm": None, "tpm": None})
        if rpm is not None:
            limits["rpm"] = rpm if limits["rpm"] is None else min(limits["rpm"], rpm)
        if tpm is not None:
            limits["tpm"] = tpm if limits["tpm"] is None else min(limits["tpm"], tpm)

    def _prune(self, model: str, now: float) -> None:
        while self._request_events[model] and now - self._request_events[model][0] >= RATE_LIMIT_WINDOW_SECONDS:
            self._request_events[model].popleft()
        while self._token_events[model] and now - self._token_events[model][0][0] >= RATE_LIMIT_WINDOW_SECONDS:
            self._token_events[model].popleft()

    @staticmethod
    def _smooth_interval(rpm_limit: int | None, tpm_limit: int | None, estimated_tokens: int) -> float:
        intervals = []
        if rpm_limit is not None:
            intervals.append(RATE_LIMIT_WINDOW_SECONDS / rpm_limit)
        if tpm_limit is not None and estimated_tokens > 0:
            intervals.append(RATE_LIMIT_WINDOW_SECONDS * estimated_tokens / tpm_limit)
        return max(intervals, default=0.0)


def _try_import_ray():
    try:
        import ray
    except ImportError:
        return None
    return ray


def _remote_vlm_rate_limiter_actor(ray_module):
    return ray_module.remote(_RayJobVlmRateLimiter)


def _rate_limiter_actor_name(job_id: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z_]", "_", job_id or "default")
    return f"dj_vlm_rate_limiter_{normalized}"


@OPERATORS.register_module(OP_NAME)
class VlmApiResponseMapper(Pipeline):
    """按行调用 OpenAI 兼容的多模态 API。

    请求内容有两种互斥配置方式：

    1. 简单模式：使用 prompt_template/system_prompt 搭配 image_key 或
       image_keys。mapper 会自动构造单轮 user 请求，并自动追加图片。
       这是 OCR、图片问答、多图理解等常见批处理任务最短的配置方式。

       Chat 示例：
       - vlm_api_response_mapper:
           model: "ep-xxx"
           base_url: "https://ark-cn-beijing.bytedance.net/api/v3/chat/completions"
           api_key: "<YOUR_API_KEY>"
           image_key: "image_url"
           system_prompt: "你是一个严谨的图片理解助手。"
           prompt_template: "请回答这个问题：${question}"
           temperature: 0.0
           top_p: 0.7
           thinking:
             type: "disabled"

       Responses 示例：
       - vlm_api_response_mapper:
           model: "ep-xxx"
           base_url: "https://ark-cn-beijing.bytedance.net/api/v3/responses"
           api_key: "<YOUR_API_KEY>"
           api_format: "responses"
           image_keys: ["main_image", "detail_images"]
           prompt_template: "提取所有可见文字，并用 JSON 返回。"
           text:
             format:
               type: "json_object"
           store: false

    2. 模板模式：Chat Completions API 使用 messages_template，Responses API
       使用 input_template。mapper 会递归渲染完整的原生 messages/input
       结构，不再自动追加 prompt/system/images。需要多轮历史、精确控制
       文本和图片顺序、配置 image_pixel_limit、file_id 或其他厂商特有
       content 字段时，使用这种模式。

       Chat 示例：
       - vlm_api_response_mapper:
           model: "ep-xxx"
           base_url: "https://ark-cn-beijing.bytedance.net/api/v3/chat/completions"
           api_key: "<YOUR_API_KEY>"
           messages_template:
             - role: "system"
               content: "你是 OCR 质量评估助手。"
             - role: "user"
               content:
                 - type: "text"
                   text: "问题：${question}；元数据：${metadata}"
                 - type: "image_url"
                   image_url:
                     url: "${image_bytes}"
                     detail: "high"

       Responses 示例：
       - vlm_api_response_mapper:
           model: "ep-xxx"
           base_url: "https://ark-cn-beijing.bytedance.net/api/v3/responses"
           api_key: "<YOUR_API_KEY>"
           api_format: "responses"
           input_template:
             - role: "user"
               content:
                 - type: "input_image"
                   image_url: "${image_url}"
                   detail: "high"
                   image_pixel_limit:
                     min_pixels: 3136
                     max_pixels: 9031680
                 - type: "input_text"
                   text: "请回答：${question}"

    模板渲染规则：
    - 只支持样本顶层字段。${user.name} 会查找字面字段名 "user.name"，
      不会当成嵌套路径。
    - 缺字段会抛 KeyError，然后按 fail_on_error/error_key 处理。
    - dict/list/tuple 字段值会用 json.dumps(..., ensure_ascii=False)
      序列化为字符串。
    - bytes/bytearray/memoryview 仅在整个模板字符串正好是 ${field} 时
      转为 data URL；二进制值不能嵌入更长的字符串中。
    - messages_template/input_template 不能和 prompt_template、
      system_prompt 或 image_keys 混用。
    """

    def __init__(
        self,
        image_key: str = "images",
        image_keys: list[str] | None = None,
        output_key: str = "ocr_answer",
        prompt_template: str | None = None,
        messages_template: list[dict[str, Any]] | None = None,
        input_template: str | list[dict[str, Any]] | None = None,
        system_prompt: str | None = None,
        model: str | None = None,
        base_url: str | None = None,
        api_key: str | None = None,
        endpoint: str = "/chat/completions",
        api_format: str | None = None,
        timeout: int = 120,
        temperature: float = 0.0,
        top_p: float | None = None,
        max_tokens: int | None = None,
        text: dict[str, Any] | None = None,
        store: bool | None = None,
        reasoning: dict[str, Any] | None = None,
        thinking: dict[str, Any] | None = None,
        previous_response_id: str | None = None,
        response_format: dict[str, Any] | None = None,
        stop: str | list[str] | None = None,
        frequency_penalty: float | None = None,
        presence_penalty: float | None = None,
        image_mime_type: str = "image/png",
        image_detail: str | None = None,
        image_content_template: dict[str, Any] | None = None,
        extra_body: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        response_path: str = CHAT_COMPLETIONS_RESPONSE_PATH,
        raw_response_key: str | None = None,
        error_key: str | None = None,
        fail_on_error: bool = False,
        rpm: int | None = None,
        tpm: int | None = None,
        estimated_tokens_per_request: int | None = None,
        image_tokens_per_image: int = 0,
        image_token_divisor: float | None = None,
        max_image_tokens: int | None = None,
        repartition_num_blocks: int | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.image_key = image_key
        self.image_keys = image_keys
        self.output_key = output_key
        self.prompt_template = prompt_template
        self.messages_template = messages_template
        self.input_template = input_template
        self.system_prompt = system_prompt
        self.model = model
        self.base_url = base_url
        self.api_key = api_key
        self.endpoint = endpoint
        if api_format not in {None, "chat", "responses"}:
            raise ValueError("api_format must be one of None, 'chat', or 'responses'")
        self.api_format = api_format
        self.timeout = timeout
        self.temperature = temperature
        self.top_p = top_p
        self.max_tokens = max_tokens
        self.text = text
        self.store = store
        self.reasoning = reasoning
        self.thinking = thinking
        self.previous_response_id = previous_response_id
        self.response_format = response_format
        self.stop = stop
        self.frequency_penalty = frequency_penalty
        self.presence_penalty = presence_penalty
        self.image_mime_type = image_mime_type
        self.image_detail = image_detail
        self.image_content_template = image_content_template
        self.extra_body = extra_body or {}
        self.extra_headers = extra_headers or {}
        self.response_path = response_path
        self.raw_response_key = raw_response_key
        self.error_key = error_key
        self.fail_on_error = fail_on_error
        self.rpm = rpm
        self.tpm = tpm
        self.estimated_tokens_per_request = estimated_tokens_per_request
        self.image_tokens_per_image = image_tokens_per_image
        self.image_token_divisor = image_token_divisor
        self.max_image_tokens = max_image_tokens
        if repartition_num_blocks is not None and repartition_num_blocks <= 0:
            raise ValueError("repartition_num_blocks must be positive when set")
        self.repartition_num_blocks = repartition_num_blocks
        self._validate_rate_limits()
        self._validate_templates()
        self._request_events = deque()
        self._token_events = deque()
        self._rate_limit_next_available_at = 0.0
        self._rate_limiter_actor = None
        self._rate_limiter_actor_name = None
        self._logged_first_batch = False
        self._api_error_log_count = 0

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
        logger.info(
            "VlmApiResponseMapper Ray run: batch_size={}, concurrency={}, num_cpus={}, "
            "repartition_num_blocks={}, model={}, api_format={}, rpm={}, tpm={}, rate_limiter_actor={}",
            self.batch_size,
            self.runtime_np(),
            self.num_cpus,
            self.repartition_num_blocks,
            self.model or "<unset>",
            "responses" if self._is_responses_api() else "chat",
            self.rpm,
            self.tpm,
            self._rate_limiter_actor_name or "local",
        )
        if self.repartition_num_blocks is not None:
            logger.info("VlmApiResponseMapper repartition input to {} blocks before API calls", self.repartition_num_blocks)
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

    def process_batched(self, samples):
        input_schema = samples.schema if isinstance(samples, pa.Table) else None
        rows = samples.to_pylist() if isinstance(samples, pa.Table) else self._dict_batch_to_rows(samples)
        self._log_first_batch(rows)
        output_rows = [self.process_single(row) for row in rows]
        if isinstance(samples, pa.Table):
            return self._rows_to_arrow_table(output_rows, input_schema)
        if not output_rows:
            return {key: [] for key in self._output_keys(samples.keys() if samples else [])}
        return {key: [row.get(key) for row in output_rows] for key in self._output_keys(samples.keys())}

    def process_single(self, sample: dict[str, Any]) -> dict[str, Any]:
        row = dict(sample)
        try:
            response = self._api_response(row)
            if self.raw_response_key:
                row[self.raw_response_key] = json.dumps(response, ensure_ascii=False)
            row[self.output_key] = self._response_text(response)
            if self.error_key:
                row[self.error_key] = ""
        except Exception as err:
            if self.fail_on_error:
                raise
            if self._api_error_log_count < 5:
                logger.warning(
                    "VlmApiResponseMapper API call failed: pid={}, images={}, error={}",
                    os.getpid(),
                    self._image_count(row),
                    err,
                )
                self._api_error_log_count += 1
            row[self.output_key] = ""
            if self.raw_response_key and self.raw_response_key not in row:
                row[self.raw_response_key] = None
            if self.error_key:
                row[self.error_key] = str(err)
        return row

    def call_api(self, images: Any) -> str:
        return self._response_text(self._api_response({self.image_key: images}))

    def _api_response(self, sample: dict[str, Any]) -> dict[str, Any]:
        payload = self._request_payload(sample)
        self._apply_rate_limit(payload, sample)
        url = self._api_url()
        target = self._metrics_target(url)
        method = self._metrics_method(url)
        try:
            response = self._post_json(url, payload, self._headers())
        except Exception:
            emit_vlm_qps(op_name=self._name, target=target, method=method, status="error")
            raise
        emit_vlm_qps(op_name=self._name, target=target, method=method, status="success")
        return response

    def _validate_rate_limits(self) -> None:
        for name in ["rpm", "tpm", "estimated_tokens_per_request", "max_image_tokens"]:
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive when set")
        if self.image_tokens_per_image < 0:
            raise ValueError("image_tokens_per_image must be non-negative")
        if self.image_token_divisor is not None and self.image_token_divisor <= 0:
            raise ValueError("image_token_divisor must be positive when set")

    def _apply_rate_limit(self, payload: dict[str, Any], sample: dict[str, Any]) -> None:
        if self.rpm is None and self.tpm is None:
            return
        if self._rate_limiter_actor is not None:
            ray = _try_import_ray()
            if ray is not None:
                token_count = self._estimate_request_tokens(payload, sample)
                ray.get(self._rate_limiter_actor.acquire.remote(self._model(), self.rpm, self.tpm, token_count))
                return
        token_count = self._estimate_request_tokens(payload, sample) if self.tpm is not None else 0
        while True:
            now = time.monotonic()
            self._prune_rate_limit_events(now)
            wait_seconds = 0.0
            if self._rate_limit_next_available_at > now:
                wait_seconds = max(wait_seconds, self._rate_limit_next_available_at - now)
            if self.rpm is not None and len(self._request_events) >= self.rpm:
                wait_seconds = max(wait_seconds, RATE_LIMIT_WINDOW_SECONDS - (now - self._request_events[0]))
            if self.tpm is not None:
                used_tokens = sum(tokens for _, tokens in self._token_events)
                if self._token_events and used_tokens + token_count > self.tpm:
                    wait_seconds = max(wait_seconds, RATE_LIMIT_WINDOW_SECONDS - (now - self._token_events[0][0]))
            if wait_seconds <= 0:
                self._request_events.append(now)
                if self.tpm is not None:
                    self._token_events.append((now, token_count))
                self._rate_limit_next_available_at = now + _RayJobVlmRateLimiter._smooth_interval(
                    self.rpm,
                    self.tpm,
                    token_count,
                )
                return
            time.sleep(wait_seconds)

    def _setup_ray_rate_limiter(self) -> None:
        self._rate_limiter_actor = None
        self._rate_limiter_actor_name = None
        if self.rpm is None and self.tpm is None:
            return
        ray = _try_import_ray()
        if ray is None or not ray.is_initialized():
            return
        job_id = ray.get_runtime_context().get_job_id()
        actor_name = _rate_limiter_actor_name(job_id)
        try:
            actor = ray.get_actor(actor_name)
        except ValueError:
            actor = _remote_vlm_rate_limiter_actor(ray).options(name=actor_name, num_cpus=0).remote()
        ray.get(actor.register.remote(self._model(), self.rpm, self.tpm))
        self._rate_limiter_actor = actor
        self._rate_limiter_actor_name = actor_name

    def _prune_rate_limit_events(self, now: float) -> None:
        while self._request_events and now - self._request_events[0] >= RATE_LIMIT_WINDOW_SECONDS:
            self._request_events.popleft()
        while self._token_events and now - self._token_events[0][0] >= RATE_LIMIT_WINDOW_SECONDS:
            self._token_events.popleft()

    def _estimate_request_tokens(self, payload: dict[str, Any], sample: dict[str, Any]) -> int:
        if self.estimated_tokens_per_request is not None:
            return self.estimated_tokens_per_request
        text = self._payload_text_for_token_estimate(payload)
        text_tokens = max(1, (len(text) + 3) // 4)
        output_tokens = self.max_tokens or self.extra_body.get("max_tokens") or self.extra_body.get("max_output_tokens") or 0
        try:
            output_tokens = int(output_tokens)
        except (TypeError, ValueError):
            output_tokens = 0
        return text_tokens + output_tokens + self._estimate_image_tokens(sample)

    def _payload_text_for_token_estimate(self, value: Any, key: str | None = None) -> str:
        if isinstance(value, str):
            if value.startswith(("data:", "http://", "https://")):
                return ""
            return value if key in {"content", "instructions", "input_text", "text"} else ""
        if isinstance(value, dict):
            parts = [self._payload_text_for_token_estimate(item, str(item_key)) for item_key, item in value.items()]
            return "\n".join(part for part in parts if part)
        if isinstance(value, list):
            parts = [self._payload_text_for_token_estimate(item, key) for item in value]
            return "\n".join(part for part in parts if part)
        return ""

    def _image_count(self, sample: dict[str, Any]) -> int:
        return sum(len(self._image_values(sample.get(key))) for key in self.image_keys or [self.image_key])

    def _estimate_image_tokens(self, sample: dict[str, Any]) -> int:
        if self.image_token_divisor is None:
            return self.image_tokens_per_image * self._image_count(sample)
        total = 0
        for key in self.image_keys or [self.image_key]:
            total += sum(self._estimate_one_image_tokens(image) for image in self._iter_raw_images(sample.get(key)))
        return total

    def _estimate_one_image_tokens(self, image: Any) -> int:
        size = self._image_size(image)
        if size is None:
            return self.image_tokens_per_image
        width, height = size
        tokens = math.ceil(width * height / self.image_token_divisor)
        if self.max_image_tokens is not None:
            tokens = min(tokens, self.max_image_tokens)
        return tokens

    def _iter_raw_images(self, value: Any):
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray, memoryview)):
            value = value.tolist()
        if isinstance(value, (bytes, bytearray, memoryview, str)):
            if self._image_values(value):
                yield value
            return
        if isinstance(value, dict):
            if self._image_size(value) is not None:
                yield value
                return
            for item in value.values():
                yield from self._iter_raw_images(item)
            return
        if isinstance(value, (list, tuple)):
            for item in value:
                yield from self._iter_raw_images(item)

    @staticmethod
    def _image_size(value: Any) -> tuple[int, int] | None:
        if isinstance(value, dict):
            width = value.get("width") or value.get("image_width")
            height = value.get("height") or value.get("image_height")
            if width and height:
                try:
                    return int(width), int(height)
                except (TypeError, ValueError):
                    return None
            return None
        if isinstance(value, str):
            if not value.startswith("data:") or "," not in value:
                return None
            try:
                value = base64.b64decode(value.split(",", 1)[1])
            except (ValueError, TypeError):
                return None
        if not isinstance(value, (bytes, bytearray, memoryview)):
            return None
        try:
            from PIL import Image

            with Image.open(io.BytesIO(bytes(value))) as image:
                return image.size
        except Exception:
            return None

    def _response_text(self, response: dict[str, Any]) -> str:
        if self._is_responses_api() and self.response_path == CHAT_COMPLETIONS_RESPONSE_PATH:
            return self._responses_output_text(response)
        content = self._value_at_path(response, self._response_path())
        if isinstance(content, list):
            return "".join(part.get("text", "") for part in content if isinstance(part, dict))
        return str(content)

    def _responses_output_text(self, response: dict[str, Any]) -> str:
        texts = []
        for item in response.get("output") or []:
            if not isinstance(item, dict):
                continue
            for part in item.get("content") or []:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    texts.append(part["text"])
        if texts:
            return "".join(texts)
        return str(self._value_at_path(response, RESPONSES_RESPONSE_PATH))

    def _request_payload(self, sample: dict[str, Any] | Any) -> dict[str, Any]:
        if not isinstance(sample, dict):
            sample = {self.image_key: sample}
        if self._is_responses_api():
            return self._responses_payload(sample)
        return self._chat_completions_payload(sample)

    def _chat_completions_payload(self, sample: dict[str, Any]) -> dict[str, Any]:
        if self.messages_template is not None:
            messages = self._render_messages_template(sample)
        else:
            messages = []
            if self.system_prompt is not None:
                messages.append({"role": "system", "content": self.system_prompt})
            messages.append(
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": self._prompt(sample)},
                        *self._image_content_parts(sample),
                    ],
                }
            )
        payload = {
            "model": self._model(),
            "temperature": self.temperature,
            "messages": messages,
        }
        if self.max_tokens is not None:
            payload["max_tokens"] = self.max_tokens
        if self.top_p is not None:
            payload["top_p"] = self.top_p
        if self.thinking is not None:
            payload["thinking"] = self.thinking
        if self.response_format is not None:
            payload["response_format"] = self.response_format
        if self.stop is not None:
            payload["stop"] = self.stop
        if self.frequency_penalty is not None:
            payload["frequency_penalty"] = self.frequency_penalty
        if self.presence_penalty is not None:
            payload["presence_penalty"] = self.presence_penalty
        payload.update(self.extra_body)
        return payload

    def _responses_payload(self, sample: dict[str, Any]) -> dict[str, Any]:
        if self.input_template is not None:
            input_value = self._render_input_template(sample)
        else:
            input_value = [
                {
                    "role": "user",
                    "content": [
                        *self._image_content_parts(sample),
                        {"type": "input_text", "text": self._prompt(sample)},
                    ],
                }
            ]
        payload = {
            "model": self._model(),
            "temperature": self.temperature,
            "input": input_value,
        }
        if self.system_prompt is not None:
            payload["instructions"] = self.system_prompt
        if self.max_tokens is not None:
            payload["max_output_tokens"] = self.max_tokens
        if self.top_p is not None:
            payload["top_p"] = self.top_p
        if self.text is not None:
            payload["text"] = self.text
        if self.store is not None:
            payload["store"] = self.store
        if self.reasoning is not None:
            payload["reasoning"] = self.reasoning
        if self.thinking is not None:
            payload["thinking"] = self.thinking
        if self.previous_response_id is not None:
            payload["previous_response_id"] = self.previous_response_id
        payload.update(self.extra_body)
        return payload

    def _post_json(self, url: str, payload: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as err:
            body = err.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"VLM API request failed with HTTP {err.code}: {body}") from err

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        headers.update(self.extra_headers)
        return headers

    def _api_url(self) -> str:
        base_url = (self.base_url or "").rstrip("/")
        if not base_url:
            raise RuntimeError("base_url must be set")
        if base_url.endswith(("/chat/completions", "/responses")):
            return base_url
        endpoint = self.endpoint if self.endpoint.startswith("/") else f"/{self.endpoint}"
        if base_url.endswith(endpoint):
            return base_url
        return f"{base_url}{endpoint}"

    @staticmethod
    def _metrics_target(url: str) -> str:
        parsed = urllib.parse.urlsplit(url)
        return parsed.netloc or parsed.path or "unknown"

    @staticmethod
    def _metrics_method(url: str) -> str:
        parsed = urllib.parse.urlsplit(url)
        return parsed.path or "POST"

    def _is_responses_api(self) -> bool:
        if self.api_format is not None:
            return self.api_format == "responses"
        endpoint = self.endpoint.rstrip("/")
        base_url = (self.base_url or "").rstrip("/")
        return endpoint.endswith("/responses") or base_url.endswith("/responses")

    def _response_path(self) -> str:
        if self.response_path != CHAT_COMPLETIONS_RESPONSE_PATH:
            return self.response_path
        if self._is_responses_api():
            return RESPONSES_RESPONSE_PATH
        return self.response_path

    def _model(self) -> str:
        if not self.model:
            raise RuntimeError("model must be set")
        return self.model

    def _prompt(self, sample: dict[str, Any] | None = None) -> str:
        if self.prompt_template is not None:
            return self._render_prompt_template(self.prompt_template, sample or {})
        return ""

    def _validate_templates(self) -> None:
        if self.messages_template is not None and self.input_template is not None:
            raise ValueError("messages_template and input_template cannot be used together")
        if self.messages_template is not None:
            if self._is_responses_api():
                raise ValueError("messages_template can only be used with Chat Completions API")
            if not isinstance(self.messages_template, list) or not self.messages_template:
                raise ValueError("messages_template must be a non-empty list")
            self._validate_template_conflicts("messages_template")
        if self.input_template is not None:
            if not self._is_responses_api():
                raise ValueError("input_template can only be used with Responses API")
            if isinstance(self.input_template, str):
                if not self.input_template:
                    raise ValueError("input_template must be a non-empty string or list")
            elif not isinstance(self.input_template, list) or not self.input_template:
                raise ValueError("input_template must be a non-empty string or list")
            self._validate_template_conflicts("input_template")

    def _validate_template_conflicts(self, template_name: str) -> None:
        conflicts = []
        if self.prompt_template is not None:
            conflicts.append("prompt_template")
        if self.system_prompt is not None:
            conflicts.append("system_prompt")
        if self.image_keys is not None:
            conflicts.append("image_keys")
        if conflicts:
            raise ValueError(f"{template_name} cannot be combined with: {', '.join(conflicts)}")

    def _render_messages_template(self, sample: dict[str, Any]) -> list[dict[str, Any]]:
        messages = self._render_template_value(self.messages_template, sample)
        if not isinstance(messages, list) or not messages:
            raise ValueError("messages_template must render to a non-empty list")
        return messages

    def _render_input_template(self, sample: dict[str, Any]) -> str | list[dict[str, Any]]:
        input_value = self._render_template_value(self.input_template, sample)
        if isinstance(input_value, str):
            if not input_value:
                raise ValueError("input_template must render to a non-empty string or list")
            return input_value
        if not isinstance(input_value, list) or not input_value:
            raise ValueError("input_template must render to a non-empty string or list")
        return input_value

    def _render_template_value(self, template: Any, sample: dict[str, Any]) -> Any:
        if isinstance(template, dict):
            return {key: self._render_template_value(value, sample) for key, value in template.items()}
        if isinstance(template, list):
            return [self._render_template_value(value, sample) for value in template]
        if isinstance(template, tuple):
            return [self._render_template_value(value, sample) for value in template]
        if isinstance(template, str):
            return self._render_prompt_template(template, sample)
        return template

    def _render_prompt_template(self, template: str, sample: dict[str, Any]) -> Any:
        only_match = PROMPT_TEMPLATE_FIELD_ONLY_PATTERN.fullmatch(template)
        if only_match is not None:
            return self._template_field_value(only_match.group(1).strip(), sample, embedded=False)

        def replace(match: re.Match[str]) -> str:
            field_name = match.group(1).strip()
            return self._template_field_string(field_name, sample)

        return PROMPT_TEMPLATE_FIELD_PATTERN.sub(replace, template)

    def _template_field_value(self, field_name: str, sample: dict[str, Any], embedded: bool) -> Any:
        if field_name not in sample:
            raise KeyError(field_name)
        value = sample[field_name]
        if isinstance(value, (bytes, bytearray, memoryview)):
            if embedded:
                raise TypeError(f"binary field {field_name!r} cannot be embedded in a template string")
            return self._data_url_from_bytes(value)
        if isinstance(value, tuple):
            return json.dumps(list(value), ensure_ascii=False)
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)
        return value

    def _template_field_string(self, field_name: str, sample: dict[str, Any]) -> str:
        value = self._template_field_value(field_name, sample, embedded=True)
        return str(value)

    def _data_url_from_bytes(self, value: bytes | bytearray | memoryview) -> str:
        encoded = base64.b64encode(bytes(value)).decode("ascii")
        return f"data:{self.image_mime_type};base64,{encoded}"

    def _image_content_parts(self, sample: dict[str, Any]) -> list[dict[str, Any]]:
        parts = []
        for key in self.image_keys or [self.image_key]:
            for image in self._image_values(sample.get(key)):
                if self.image_content_template is not None:
                    parts.append(self._format_image_template(self.image_content_template, image))
                elif self._is_responses_api():
                    image_part = {"type": "input_image", "image_url": image["url"]}
                    if self.image_detail is not None:
                        image_part["detail"] = self.image_detail
                    parts.append(image_part)
                else:
                    image_url = {"url": image["url"]}
                    if self.image_detail is not None:
                        image_url["detail"] = self.image_detail
                    parts.append({"type": "image_url", "image_url": image_url})
        if not parts:
            raise ValueError("image bytes are empty")
        return parts

    def _image_values(self, value: Any) -> list[dict[str, str]]:
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray, memoryview)):
            value = value.tolist()
        if isinstance(value, (bytes, bytearray, memoryview)):
            encoded = base64.b64encode(bytes(value)).decode("ascii")
            data_url = f"data:{self.image_mime_type};base64,{encoded}"
            return [
                {
                    "url": data_url,
                    "data_url": data_url,
                    "base64": encoded,
                    "mime_type": self.image_mime_type,
                    "detail": self.image_detail or "",
                }
            ]
        if isinstance(value, str):
            if value.startswith(("http://", "https://", "data:")):
                return [
                    {
                        "url": value,
                        "data_url": value,
                        "base64": self._base64_from_data_url(value),
                        "mime_type": self._mime_type_from_data_url(value),
                        "detail": self.image_detail or "",
                    }
                ]
            return []
        if isinstance(value, dict):
            images = []
            for item in value.values():
                images.extend(self._image_values(item))
            return images
        if isinstance(value, (list, tuple)):
            images = []
            for item in value:
                images.extend(self._image_values(item))
            return images
        return []

    def _format_image_template(self, value: Any, image: dict[str, str]) -> Any:
        if isinstance(value, str):
            return value.format_map(image)
        if isinstance(value, dict):
            return {key: self._format_image_template(item, image) for key, item in value.items()}
        if isinstance(value, list):
            return [self._format_image_template(item, image) for item in value]
        return value

    @staticmethod
    def _base64_from_data_url(value: str) -> str:
        if value.startswith("data:") and "," in value:
            return value.split(",", 1)[1]
        return ""

    @staticmethod
    def _mime_type_from_data_url(value: str) -> str:
        if value.startswith("data:") and ";base64," in value:
            return value[len("data:") : value.index(";base64,")]
        return ""

    @staticmethod
    def _value_at_path(value: Any, path: str) -> Any:
        cursor = value
        for part in path.split("."):
            if isinstance(cursor, list):
                try:
                    cursor = cursor[int(part)]
                except (ValueError, IndexError) as err:
                    raise KeyError(f"response path {path!r} is missing at {part!r}") from err
            elif isinstance(cursor, dict) and part in cursor:
                cursor = cursor[part]
            else:
                raise KeyError(f"response path {path!r} is missing at {part!r}")
        return cursor

    @staticmethod
    def _dict_batch_to_rows(samples) -> list[dict[str, Any]]:
        keys = list(samples.keys())
        if not keys:
            return []
        return [{key: samples[key][idx] for key in keys} for idx in range(len(samples[keys[0]]))]

    def _log_first_batch(self, rows: list[dict[str, Any]]) -> None:
        if self._logged_first_batch:
            return
        image_counts = [self._image_count(row) for row in rows]
        logger.info(
            "VlmApiResponseMapper first worker batch: pid={}, rows={}, image_count_min={}, "
            "image_count_max={}, empty_image_rows={}",
            os.getpid(),
            len(rows),
            min(image_counts) if image_counts else 0,
            max(image_counts) if image_counts else 0,
            sum(1 for count in image_counts if count == 0),
        )
        self._logged_first_batch = True

    def _output_keys(self, input_keys) -> list[str]:
        keys = list(input_keys)
        if self.output_key not in keys:
            keys.append(self.output_key)
        if self.raw_response_key and self.raw_response_key not in keys:
            keys.append(self.raw_response_key)
        if self.error_key and self.error_key not in keys:
            keys.append(self.error_key)
        return keys

    def _rows_to_arrow_table(self, rows: list[dict[str, Any]], input_schema: pa.Schema | None) -> pa.Table:
        input_names = input_schema.names if input_schema is not None else (list(rows[0].keys()) if rows else [])
        arrays = []
        fields = []
        for key in self._output_keys(input_names):
            values = [row.get(key) for row in rows]
            arrow_type = self._arrow_type_for_key(key, values, input_schema)
            arrays.append(pa.array(values, type=arrow_type))
            fields.append(pa.field(key, arrow_type))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    def _arrow_type_for_key(self, key: str, values: list[Any], input_schema: pa.Schema | None) -> pa.DataType:
        if key == self.output_key or key == self.error_key:
            return pa.string()
        if key == self.raw_response_key:
            return pa.string()
        if input_schema is not None:
            field_index = input_schema.get_field_index(key)
            if field_index >= 0 and not pa.types.is_null(input_schema.field(field_index).type):
                return input_schema.field(field_index).type
        inferred = pa.array(values)
        if pa.types.is_null(inferred.type):
            return pa.string()
        return inferred.type
